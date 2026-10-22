//go:build windows

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

type record struct {
	PID        int    `json:"pid"`
	Created    uint64 `json:"created"`
	Executable string `json:"executable"`
	Script     string `json:"script"`
	ProxyPort  int    `json:"proxy_port"`
	HealthPort int    `json:"health_port,omitempty"`
	Generation string `json:"generation"`
	Version    string `json:"version"`
	DataPort   int    `json:"data_port,omitempty"`
}

func (a *app) publish(super bool, healthPort int) (record, error) {
	p, e := openOwned(os.Getpid())
	if e != nil {
		return record{}, e
	}
	defer p.close()
	r := record{PID: p.pid, Created: p.created, Executable: p.image, Script: a.exe, ProxyPort: a.cfg.Port, HealthPort: healthPort, Generation: nonce(), Version: marker}
	if !super && a.workerID != "" {
		r.DataPort = a.dataPort
	}
	return r, writeRecord(a.statePath(super), r)
}

func (a *app) readRecord(super bool) (record, error) {
	var r record
	data, e := os.ReadFile(a.statePath(super))
	if e != nil {
		return r, e
	}
	e = json.Unmarshal(data, &r)
	if e != nil {
		return r, e
	}
	if r.Version != marker || !a.trustedImage(r.Script) || r.ProxyPort != a.cfg.Port || r.PID <= 0 || len(r.Generation) != 32 {
		return r, errors.New("worker record mismatch")
	}
	return r, nil
}

func (a *app) adopt(super bool) (*process, error) {
	r, e := a.readRecord(super)
	if e != nil {
		return nil, e
	}
	return a.ownedRecord(r)
}

func (a *app) healthy() bool {
	r, e := a.readRecord(false)
	if e != nil || r.HealthPort < 1 || r.HealthPort > 65535 {
		return false
	}
	client := http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	resp, e := client.Get(fmt.Sprintf("http://127.0.0.1:%d%s", r.HealthPort, healthPath))
	if e != nil {
		return false
	}
	defer resp.Body.Close()
	body, e := io.ReadAll(io.LimitReader(resp.Body, 100))
	return e == nil && resp.StatusCode == 200 && string(body) == marker && resp.Header.Get("X-Worker-Pid") == strconv.Itoa(r.PID) && resp.Header.Get("X-Worker-Generation") == r.Generation
}

func (a *app) serve() (serveErr error) {
	listenPort := a.cfg.Port
	if a.workerID != "" {
		listenPort = 0
	}
	data, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", listenPort))
	if e != nil {
		return e
	}
	defer data.Close()
	a.dataPort = data.Addr().(*net.TCPAddr).Port
	h := newHistory(a, "worker")
	h.emit(historyEvent{Kind: "worker_starting"})
	var core *core
	var stopHistory func()
	stage := "control_listener"
	defer func() {
		if stopHistory != nil {
			stopHistory()
		}
		recovered := recover()
		reason := stage
		if recovered != nil {
			reason = "panic_in_serve"
		} else if serveErr == nil {
			reason = "serve_returned_without_error"
		} else if errors.Is(serveErr, http.ErrServerClosed) {
			reason = "server_closed"
		}
		event := historyEvent{Kind: "worker_exit", Reason: reason}
		if core != nil {
			snapshot := core.statusSnapshot(os.Getpid())
			faults := core.historyFaults()
			event.Snapshot = &snapshot
			event.Faults = &faults
		}
		h.emit(event)
		if core != nil {
			core.close()
		}
		h.close()
		if recovered != nil {
			panic(recovered)
		}
	}()
	control, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return e
	}
	defer control.Close()
	a.controlPort = control.Addr().(*net.TCPAddr).Port
	stage = "core_initialization"
	core, e = newCore(a)
	if e != nil {
		return e
	}
	core.history = h
	stage = "state_publication"
	r, e := a.publish(false, control.Addr().(*net.TCPAddr).Port)
	if e != nil {
		return e
	}

	defer func() {
		if current, e := a.readRecord(false); e == nil && current.PID == os.Getpid() {
			os.Remove(a.statePath(false))
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc(healthPath, func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("X-Worker-Pid", strconv.Itoa(r.PID))
		w.Header().Set("X-Worker-Generation", r.Generation)
		io.WriteString(w, marker)
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, req *http.Request) {
		if core.draining() {
			http.Error(w, "Proxy is draining", http.StatusServiceUnavailable)
			return
		}
		if e := validateSettings(*core.state.Load(), a.cfg.Port, a.dataPort, a.controlPort); e != nil {
			http.Error(w, "Routing configuration is invalid", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ready")
	})
	mux.HandleFunc("/ready-json", func(w http.ResponseWriter, req *http.Request) {
		if core.draining() || validateSettings(*core.state.Load(), a.cfg.Port, a.dataPort, a.controlPort) != nil {
			http.Error(w, "Not ready", 503)
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"ready": true})
	})
	mux.HandleFunc("/health-json", func(w http.ResponseWriter, req *http.Request) {
		json.NewEncoder(w).Encode(map[string]bool{"alive": true})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
		json.NewEncoder(w).Encode(core.statusSnapshot(r.PID))
	})

	for _, path := range []string{"/drain", "/resume", "/drain-timeout"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, req *http.Request) {
			if req.Method != "POST" {
				http.Error(w, "POST required", 405)
				return
			}
			if req.Header.Get("X-Worker-Generation") != r.Generation {
				http.Error(w, "Control authentication required", 403)
				return
			}
			switch req.URL.Path {
			case "/drain":
				var input struct {
					Seconds int `json:"lease_seconds"`
				}
				if decodeControlJSON(w, req, 1024, &input) != nil || input.Seconds < 1 || input.Seconds > 3610 {
					http.Error(w, "Invalid lease", 400)
					return
				}
				core.beginDrain(time.Duration(input.Seconds) * time.Second)
			case "/resume":
				core.resume()
			case "/drain-timeout":
				core.metrics.drainTimeouts.Add(1)
				core.resume()
			}
			json.NewEncoder(w).Encode(map[string]any{"draining": core.draining()})
		})
	}
	dataServer := &http.Server{Handler: core, ReadHeaderTimeout: 20 * time.Second, IdleTimeout: a.idle(), MaxHeaderBytes: 65536}
	controlHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("X-Worker-Pid", strconv.Itoa(r.PID))
		w.Header().Set("X-Worker-Generation", r.Generation)
		if a.workerID != "" && req.URL.Path == "/retire" {
			if req.Method != "POST" || req.Header.Get("X-Worker-Generation") != r.Generation {
				http.Error(w, "Control authentication required", 403)
				return
			}
			dataServer.SetKeepAlivesEnabled(false)
			json.NewEncoder(w).Encode(map[string]any{"retiring": true})
			return
		}
		mux.ServeHTTP(w, req)
	})
	healthServer := newControlServer(controlHandler)
	defer healthServer.Close()
	go healthServer.Serve(control)
	initial := core.statusSnapshot(r.PID)
	h.emit(historyEvent{Kind: "worker_started", Snapshot: &initial})
	stopHistory = h.periodic(core, r.PID, historyInterval)
	stage = "data_listener_return"
	log.Printf("Worker started, pid=%d", os.Getpid())
	return dataServer.Serve(data)
}

func (a *app) start(role string) (*process, error) {
	image, e := a.activeImage()
	if e != nil {
		return nil, e
	}
	cmd := exec.Command(image, role)
	cmd.Env = a.childEnvironment("")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if e := cmd.Start(); e != nil {
		return nil, e
	}
	p, e := openOwned(cmd.Process.Pid)
	_ = cmd.Process.Release()
	return p, e
}

func (a *app) ensure() error {
	if e := a.initializeRelease(); e != nil {
		return e
	}
	p, e := a.start("--supervise")
	if e != nil {
		return e
	}
	p.close()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if a.healthy() && a.entryReady() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("proxy unavailable; inspect supervisor.log and port configuration")
}

func (a *app) stopAll() error {
	if err := a.stopFollower(); err != nil {
		return err
	}
	return a.stopRuntime()
}

func (a *app) stopRuntime() error {
	history := newHistory(a, "command")
	defer history.close()
	for _, super := range []bool{true, false} {
		p, e := a.adopt(super)
		if e == nil {
			history.emit(historyEvent{Kind: "stop_requested", Reason: "requested_stop", Subject: processSession(p.pid, p.created)})
			history.flush(250 * time.Millisecond)
			e = p.kill()
			if e == nil {
				e = p.wait()
			}
			if e == nil {
				history.emit(historyEvent{Kind: "process_stopped", Reason: "requested_stop", Subject: processSession(p.pid, p.created), ExitCode: p.exitCode()})
			} else {
				history.emit(historyEvent{Kind: "process_stop_failed", Reason: "stop_or_wait_failed", Subject: processSession(p.pid, p.created)})
			}
			p.close()
			if e != nil {
				return fmt.Errorf("could not stop process: %w", e)
			}
		}
	}
	paths, _ := filepath.Glob(filepath.Join(a.root, "worker-private-*.json"))
	for _, path := range paths {
		var r record
		data, _ := os.ReadFile(path)
		if json.Unmarshal(data, &r) != nil || r.ProxyPort != a.cfg.Port {
			continue
		}
		p, e := a.ownedRecord(r)
		if e != nil {
			continue
		}
		history.emit(historyEvent{Kind: "stop_requested", Reason: "requested_stop", Subject: processSession(p.pid, p.created)})
		e = p.kill()
		if e == nil {
			e = p.wait()
		}
		p.close()
		if e != nil {
			return e
		}
		os.Remove(path)
	}
	return nil
}

func (a *app) entryReady() bool {
	r, e := a.readRecord(false)
	if e != nil {
		return false
	}
	if r.DataPort == 0 || r.DataPort == a.cfg.Port {
		return false
	}
	super, e := a.readRecord(true)
	if e != nil || super.HealthPort == 0 {
		return false
	}
	_, e = a.callRecord(super, "GET", "/status", nil, time.Second)
	return e == nil
}
