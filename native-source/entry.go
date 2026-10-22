//go:build windows

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const maxRetiredWorkers = 3

type entryWorker struct {
	process                          *process
	record                           record
	statePath, revision, fingerprint string
	connections                      int // protected by entry.mu, includes in-progress backend dial
	retiring                         bool
	exitObserved                     bool
}
type entry struct {
	toClient, toBackend socketWriteStats
	a                   *app
	mu                  sync.Mutex
	upgradeMu           sync.Mutex
	active              *entryWorker
	retired             []*entryWorker
	history             *historyJournal
	dialFailures        uint64
	stop                chan struct{}
	monitorDone         chan struct{}
}

type entryReader struct{ io.Reader }
type entryWriter struct{ io.Writer }

type entryMeasuredWriter struct {
	io.Writer
	stats *socketWriteStats
}

func (w entryMeasuredWriter) Write(p []byte) (int, error) {
	start := w.stats.begin()
	n, e := w.Writer.Write(p)
	w.stats.end(start)
	return n, e
}

func entryCopy(dst io.Writer, src io.Reader) error {
	buf := buffers.Get().([]byte)
	defer buffers.Put(buf)
	_, e := io.CopyBuffer(entryWriter{dst}, entryReader{src}, buf)
	return e
}

func (b *entry) spawn(image string, requireReady bool) (*entryWorker, error) {
	id := nonce()
	cmd := exec.Command(image, "--serve")
	cmd.Env = b.a.childEnvironment(id)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if e := cmd.Start(); e != nil {
		return nil, e
	}
	p, e := openOwned(cmd.Process.Pid)
	if e != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, e
	}
	_ = cmd.Process.Release()
	w := &entryWorker{process: p, statePath: filepath.Join(b.a.root, "worker-private-"+id+".json")}
	ok := false
	defer func() {
		if !ok {
			p.kill()
			p.wait()
			p.close()
			os.Remove(w.statePath)
		}
	}()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) && p.alive() {
		data, err := os.ReadFile(w.statePath)
		var r record
		if err == nil && json.Unmarshal(data, &r) == nil && r.PID == p.pid && r.Created == p.created && r.DataPort > 0 && r.DataPort <= 65535 {
			if state, err := b.a.callRecord(r, "GET", "/status", nil, time.Second); err == nil {
				if state["entry_protocol"] != float64(1) {
					return nil, errors.New("candidate entry protocol incompatible")
				}
				if !requireReady {
					err = nil
				} else {
					_, err = b.a.callRecord(r, "GET", "/ready-json", nil, time.Second)
				}
				if err == nil {
					w.record = r
					w.revision, _ = state["revision"].(string)
					w.fingerprint, _ = state["runtime_fingerprint"].(string)
					ok = true
					b.history.emit(historyEvent{Kind: "worker_spawned", Reason: "private_backend_ready", Subject: processSession(p.pid, p.created)})
					return w, nil
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return nil, errors.New("candidate did not become ready")
}

func (b *entry) forward(client *net.TCPConn) {
	defer client.Close()
	b.mu.Lock()
	w := b.active
	if w != nil {
		w.connections++
	}
	b.mu.Unlock()
	if w == nil {
		return
	}
	defer func() { b.mu.Lock(); w.connections--; b.mu.Unlock() }()
	upstream, e := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", w.record.DataPort), 2*time.Second)
	if e != nil {
		b.mu.Lock()
		b.dialFailures++
		b.mu.Unlock()
		return
	}
	backend := upstream.(*net.TCPConn)
	defer backend.Close()
	// No request parsing, buffering of whole bodies, retries, or idle timeout.
	// Preserve half-close: a client that finished uploading may still receive a
	// long response. Each connection stays pinned to exactly one generation.
	var clientWriter io.Writer = client
	var backendWriter io.Writer = backend
	if b.a != nil && b.a.cfg.SocketWriteMetrics {
		clientWriter = entryMeasuredWriter{client, &b.toClient}
		backendWriter = entryMeasuredWriter{backend, &b.toBackend}
	}
	done := make(chan struct{})
	go func() {
		if e := entryCopy(backendWriter, client); e != nil {
			backend.Close()
			client.Close()
		} else {
			backend.CloseWrite()
		}
		close(done)
	}()
	if e := entryCopy(clientWriter, backend); e != nil {
		backend.Close()
		client.Close()
	}
	client.CloseWrite()
	// A backend EOF means it will not consume more input. Unblock a still-idle
	// client read without truncating the response already copied above.
	client.CloseRead()
	<-done
}

func (b *entry) status() map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	retired := []map[string]any{}
	for _, w := range b.retired {
		retired = append(retired, map[string]any{"pid": w.record.PID, "revision": w.revision, "connections": w.connections})
	}
	result := map[string]any{"socket_write": map[string]any{"enabled": b.a != nil && b.a.cfg.SocketWriteMetrics, "to_client": b.toClient.snapshot(), "to_backend": b.toBackend.snapshot()}, "available": true, "protocol": 1, "seamless_upgrade": true, "entry_revision": revision, "retired_workers": retired, "max_retired_workers": maxRetiredWorkers, "backend_dial_failures": b.dialFailures}
	if b.active != nil {
		result["active_pid"] = b.active.record.PID
		result["active_revision"] = b.active.revision
		result["active_connections"] = b.active.connections
	}
	return result
}

func (b *entry) install(image string) (map[string]any, error) {
	if !b.upgradeMu.TryLock() {
		return nil, errors.New("another upgrade is in progress")
	}
	defer b.upgradeMu.Unlock()
	select {
	case <-b.stop:
		return nil, errors.New("entry is stopping")
	default:
	}
	defer b.a.pruneReleases()
	image = canonical(image)
	hash := filepath.Base(filepath.Dir(image))
	if !validHash(hash) || !b.a.trustedImage(image) {
		return nil, errors.New("candidate must be an immutable staged release")
	}
	actual, e := imageHash(image)
	if e != nil || actual != hash {
		return nil, errors.New("candidate checksum mismatch")
	}
	b.mu.Lock()
	alreadyCurrent := b.active != nil && strings.EqualFold(image, b.active.record.Script)
	b.mu.Unlock()
	if alreadyCurrent {
		result := b.status()
		result["already_current"] = true
		return result, nil
	}
	b.reap()
	b.mu.Lock()
	full := len(b.retired) >= maxRetiredWorkers
	b.mu.Unlock()
	if full {
		return nil, errors.New("retired generation budget reached; existing connections preserved")
	}
	w, e := b.spawn(image, true)
	if e != nil {
		b.history.emit(historyEvent{Kind: "upgrade_failed", Reason: "candidate_not_ready"})
		return nil, e
	}
	committed := false
	oldSelection, selectionErr := os.ReadFile(filepath.Join(b.a.root, "active-release.json"))
	defer func() {
		if !committed {
			w.process.kill()
			w.process.wait()
			w.process.close()
			os.Remove(w.statePath)
			if selectionErr == nil {
				var ptr releasePointer
				if json.Unmarshal(oldSelection, &ptr) == nil {
					_ = writeRecord(filepath.Join(b.a.root, "active-release.json"), ptr)
				}
			}
		}
	}()
	b.mu.Lock()
	oldFingerprint := b.active.fingerprint
	b.mu.Unlock()
	if w.fingerprint == "" || w.fingerprint != oldFingerprint {
		return nil, errors.New("runtime configuration changed; use a planned drained upgrade")
	}
	// Write the durable selection before publishing routing. A crash on either
	// side of this point starts a ready validated version on the next launch.
	if e = b.a.selectRelease(image); e != nil {
		return nil, e
	}
	if e = writeRecord(b.a.statePath(false), w.record); e != nil {
		return nil, e
	}
	b.mu.Lock()
	old := b.active
	b.active = w
	if old != nil {
		old.retiring = true
		b.retired = append(b.retired, old)
	}
	b.mu.Unlock()
	committed = true
	b.history.emit(historyEvent{Kind: "upgrade_committed", Reason: "new_connections_switched", Subject: processSession(w.process.pid, w.process.created)})
	if old != nil {
		_, _ = b.a.callRecord(old.record, "POST", "/retire", nil, time.Second)
	}
	return b.status(), nil
}

func (b *entry) reap() {
	b.mu.Lock()
	list := append([]*entryWorker(nil), b.retired...)
	b.mu.Unlock()
	for _, w := range list {
		b.mu.Lock()
		unused := w.connections == 0
		b.mu.Unlock()
		if !unused {
			continue
		}
		if w.process.alive() {
			s, e := b.a.callRecord(w.record, "GET", "/status", nil, time.Second)
			if e != nil || s["active_requests"] != float64(0) || s["queued_requests"] != float64(0) {
				continue
			}
			if w.process.kill() != nil || w.process.wait() != nil {
				continue
			}
			b.history.emit(historyEvent{Kind: "retired_worker_stopped", Reason: "connections_finished", Subject: processSession(w.process.pid, w.process.created), ExitCode: w.process.exitCode()})
		}
		b.mu.Lock()
		for i, x := range b.retired {
			if x == w {
				b.retired = append(b.retired[:i], b.retired[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		w.process.close()
		os.Remove(w.statePath)
	}
}

func (a *app) entryGuard() (syscall.Handle, bool, error) {
	hash := sha256.Sum256([]byte(strings.ToLower(a.root) + strconv.Itoa(a.cfg.Port)))
	name := "Local\\AntigravitySystemProxy-native-" + hex.EncodeToString(hash[:8])
	h, _, e := createMutex.Call(0, 0, uintptr(unsafe.Pointer(wide(name))))
	if h == 0 {
		return 0, false, e
	}
	return syscall.Handle(h), errors.Is(e, syscall.Errno(183)), nil
}

func (a *app) supervise() (serveErr error) {
	h, exists, e := a.entryGuard()
	if e != nil {
		return e
	}
	defer syscall.CloseHandle(h)
	if exists {
		return nil
	}
	data, e := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: a.cfg.Port})
	if e != nil {
		return e
	}
	defer data.Close()
	control, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return e
	}
	defer control.Close()
	history := newHistory(a, "supervisor")
	defer history.close()
	stage := "entry_starting"
	defer func() {
		recovered := recover()
		reason := stage
		if recovered != nil {
			reason = "panic_in_entry"
		} else if errors.Is(serveErr, net.ErrClosed) {
			reason = "listener_closed"
		} else if serveErr == nil {
			reason = "entry_returned_without_error"
		}
		history.emit(historyEvent{Kind: "supervisor_exit", Reason: reason, ErrorCode: socketErrorCode(serveErr)})
		if recovered != nil {
			history.close()
			panic(recovered)
		}
	}()
	b := &entry{a: a, history: history, stop: make(chan struct{}), monitorDone: make(chan struct{})}
	monitorStarted := false
	// A restarted entry has lost its client sockets, but can adopt the active
	// backend. All other private records are tracked until their work finishes.
	paths, _ := filepath.Glob(filepath.Join(a.root, "worker-private-*.json"))
	current, _ := a.readRecord(false)
	for _, path := range paths {
		var r record
		d, _ := os.ReadFile(path)
		if json.Unmarshal(d, &r) != nil || r.ProxyPort != a.cfg.Port {
			continue
		}
		p, err := a.ownedRecord(r)
		if err != nil {
			os.Remove(path)
			continue
		}
		w := &entryWorker{record: r, process: p, statePath: path}
		if s, err := a.callRecord(r, "GET", "/status", nil, time.Second); err == nil {
			w.revision, _ = s["revision"].(string)
			w.fingerprint, _ = s["runtime_fingerprint"].(string)
		}
		if r.PID == current.PID && r.Created == current.Created {
			b.active = w
		} else {
			w.retiring = true
			b.retired = append(b.retired, w)
		}
	}
	if b.active == nil {
		image, err := a.activeImage()
		if err != nil {
			return err
		}
		b.active, e = b.spawn(image, false)
		if e != nil {
			return e
		}
		if e = writeRecord(a.statePath(false), b.active.record); e != nil {
			return e
		}
	}
	defer func() {
		close(b.stop)
		if monitorStarted {
			<-b.monitorDone
		}
		b.upgradeMu.Lock()
		defer b.upgradeMu.Unlock()
		b.mu.Lock()
		all := append(append([]*entryWorker{}, b.retired...), b.active)
		b.mu.Unlock()
		for _, w := range all {
			if w != nil {
				w.process.kill()
				w.process.wait()
				w.process.close()
				os.Remove(w.statePath)
			}
		}
	}()
	r, e := a.publish(true, control.Addr().(*net.TCPAddr).Port)
	if e != nil {
		return e
	}
	defer os.Remove(a.statePath(true))
	mux := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("X-Worker-Pid", strconv.Itoa(r.PID))
		w.Header().Set("X-Worker-Generation", r.Generation)
		if req.URL.Path == "/status" {
			json.NewEncoder(w).Encode(b.status())
			return
		}
		if req.URL.Path != "/upgrade" || req.Method != "POST" || req.Header.Get("X-Worker-Generation") != r.Generation {
			http.Error(w, "Invalid control request", 403)
			return
		}
		var in struct {
			Image string `json:"image"`
		}
		if decodeControlJSON(w, req, 8192, &in) != nil {
			http.Error(w, "Invalid input", 400)
			return
		}
		result, err := b.install(in.Image)
		if err != nil {
			http.Error(w, "Upgrade failed: "+err.Error(), 409)
			return
		}
		json.NewEncoder(w).Encode(result)
	})
	srv := newControlServer(mux)
	defer srv.Close()
	go srv.Serve(control)
	history.emit(historyEvent{Kind: "supervisor_started", Reason: "stable_entry"})
	monitorStarted = true
	go b.monitor()
	// Bound accepted front connections independently of worker admission. When
	// full, leave clients in the OS backlog instead of spawning unbounded copies.
	limit := make(chan struct{}, (a.cfg.MaxConnections+a.cfg.MaxPending)*(maxRetiredWorkers+2))
	stage = "entry_accept_failed"
	for {
		limit <- struct{}{}
		c, err := acceptWithRetry(data.AcceptTCP, func(delay time.Duration) bool {
			return waitAcceptRetry(b.stop, delay)
		}, func(kind string, err error) {
			history.emit(historyEvent{Kind: kind, Reason: "listener_accept", ErrorCode: socketErrorCode(err)})
		})
		if err != nil {
			<-limit
			return err
		}
		go func() { defer func() { <-limit }(); b.forward(c) }()
	}
}

func (b *entry) monitor() {
	defer close(b.monitorDone)
	failures := 0
	for {
		// Start the rest interval after the preceding probe has completed. A
		// ticker accumulates a ready tick during slow probes and shortens the
		// recovery grace period, causing transient stalls to lose live tunnels.
		rest := time.NewTimer(time.Second)
		select {
		case <-b.stop:
			rest.Stop()
			return
		case <-rest.C:
		}
		b.upgradeMu.Lock()
		select {
		case <-b.stop:
			b.upgradeMu.Unlock()
			return
		default:
		}
		b.reap()
		b.mu.Lock()
		w := b.active
		b.mu.Unlock()
		_, e := b.a.callRecord(w.record, "GET", "/health-json", nil, time.Second)
		if e == nil {
			if failures > 0 {
				b.history.emit(historyEvent{Kind: "worker_health_recovered", Reason: "probe_recovered", Subject: processSession(w.process.pid, w.process.created), Failures: failures})
			}
			failures = 0
			b.upgradeMu.Unlock()
			continue
		}
		failures++
		if failures == 1 {
			b.history.emit(historyEvent{Kind: "worker_health_failed", Reason: "probe_or_identity_failure", Subject: processSession(w.process.pid, w.process.created), Failures: failures})
		}
		if !w.process.alive() || failures >= b.a.cfg.HealthFailures {
			reason := "exit_observed"
			if w.process.alive() {
				reason = "health_threshold"
			}
			if !w.exitObserved {
				b.history.emit(historyEvent{Kind: "worker_restart_requested", Reason: reason, Subject: processSession(w.process.pid, w.process.created), Failures: failures})
			}
			if w.process.alive() {
				if w.process.kill() != nil || w.process.wait() != nil {
					b.history.emit(historyEvent{Kind: "worker_restart_failed", Reason: "termination_failed", Subject: processSession(w.process.pid, w.process.created)})
					b.upgradeMu.Unlock()
					continue
				}
			}
			if !w.exitObserved {
				b.history.emit(historyEvent{Kind: "worker_exit_observed", Reason: reason, Subject: processSession(w.process.pid, w.process.created), ExitCode: w.process.exitCode()})
				w.exitObserved = true
			}
			image, err := b.a.activeImage()
			if err == nil {
				var next *entryWorker
				next, err = b.spawn(image, false)
				if err == nil {
					err = writeRecord(b.a.statePath(false), next.record)
					if err == nil {
						b.mu.Lock()
						b.active = next
						w.retiring = true
						b.retired = append(b.retired, w)
						b.mu.Unlock()
						failures = 0
					} else {
						next.process.kill()
						next.process.wait()
						next.process.close()
					}
				}
			}
		}
		b.upgradeMu.Unlock()
	}
}
