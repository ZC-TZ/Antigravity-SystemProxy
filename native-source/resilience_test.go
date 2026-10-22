//go:build windows

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func await(t *testing.T, condition func() bool) {
	t.Helper()
	limit := time.Now().Add(2 * time.Second)
	for time.Now().Before(limit) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func admissionFixture(t *testing.T, wait float64) (*core, *httptest.Server) {
	t.Helper()
	root := t.TempDir()
	settings := filepath.Join(root, "settings.json")
	os.WriteFile(settings, []byte(`{}`), 0600)
	a := &app{root: root, settingsFile: settings, cfg: Config{Port: 17891, Idle: 5, MaxConnections: 1, MaxPending: 1, QueueWait: wait}}
	c, e := newCore(a)
	if e != nil {
		t.Fatal(e)
	}
	p := httptest.NewServer(c)
	t.Cleanup(func() { c.close(); p.Close() })
	return c, p
}

func TestAdmissionQueueRecoversWithoutPOSTReplay(t *testing.T) {
	c, p := admissionFixture(t, 1)
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	var commands atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			commands.Add(1)
			io.WriteString(w, "confirmed")
			return
		}
		io.WriteString(w, "held")
		w.(http.Flusher).Flush()
		select {
		case <-gate:
		case <-r.Context().Done():
		}
	}))
	defer origin.Close()
	client := streamingClient(t, p, origin)
	first, e := client.Get(origin.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer first.Body.Close()
	done := make(chan error, 1)
	go func() {
		response, e := client.Post(origin.URL, "application/json", strings.NewReader(`{"result":"done"}`))
		if e == nil {
			var b []byte
			b, e = io.ReadAll(response.Body)
			response.Body.Close()
			if string(b) != "confirmed" {
				e = errors.New("feedback missing")
			}
		}
		done <- e
	}()
	await(t, func() bool { return len(c.pending) == 1 })
	if commands.Load() != 0 {
		t.Fatal("queued command was forwarded before admission")
	}
	release()
	io.Copy(io.Discard, first.Body)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if commands.Load() != 1 {
		t.Fatalf("command repeated %d times", commands.Load())
	}
	await(t, func() bool { return len(c.pending) == 0 && len(c.slots) == 0 })
}

func TestAdmissionBoundsCancellationAndShutdown(t *testing.T) {
	c, _ := admissionFixture(t, .05)
	if e := c.acquire(context.Background()); e != nil {
		t.Fatal(e)
	}
	defer func() { <-c.slots }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.acquire(ctx) }()
	await(t, func() bool { return len(c.pending) == 1 })
	if e := c.acquire(context.Background()); !errors.Is(e, errCapacity) {
		t.Fatal("queue bound not enforced", e)
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal("queue did not honor cancellation", e)
	}
	if len(c.pending) != 0 {
		t.Fatal("canceled waiter leaked")
	}
	if e := c.acquire(context.Background()); !errors.Is(e, errCapacity) {
		t.Fatal("queue timeout missing", e)
	}
	if c.queueTimeouts.Load() != 1 || c.queueRejected.Load() != 1 {
		t.Fatal("overload counters incorrect")
	}
	go func() { done <- c.acquire(context.Background()) }()
	await(t, func() bool { return len(c.pending) == 1 })
	c.close()
	if e := <-done; e == nil || errors.Is(e, errCapacity) {
		t.Fatal("shutdown did not unblock waiter", e)
	}
	if len(c.pending) != 0 {
		t.Fatal("shutdown leaked waiter")
	}
}

func TestDirectHTTPFeedbackStreamSurvivesProxyToggle(t *testing.T) {
	c, p, _ := fixture(t, Settings{})
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "running\n")
		w.(http.Flusher).Flush()
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "command-confirmed\n")
	}))
	defer origin.Close()
	client := streamingClient(t, p, origin)
	response, e := client.Get(origin.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	buf := make([]byte, len("running\n"))
	if _, e = io.ReadFull(response.Body, buf); e != nil {
		t.Fatal(e)
	}
	c.updateSettings(Settings{true, "127.0.0.1:9", ""})
	release()
	remaining, e := io.ReadAll(response.Body)
	if e != nil || string(remaining) != "command-confirmed\n" {
		t.Fatalf("unchanged local route was interrupted: %q %v", remaining, e)
	}
}

func TestChangedDirectHTTPRouteIsStillClosed(t *testing.T) {
	c, _, _ := fixture(t, Settings{})
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	var closed atomic.Bool
	tracked := &idleConn{Conn: left, onClose: func() { closed.Store(true) }}
	c.mu.Lock()
	c.httpConns[tracked] = &connectionRoute{destination{"example.test", "http"}, "DIRECT"}
	c.mu.Unlock()
	c.updateSettings(Settings{true, "127.0.0.1:9", ""})
	if !closed.Load() {
		t.Fatal("route changed to proxy but old direct connection survived")
	}
}

func TestStabilityConfigDefaultsAndOverrides(t *testing.T) {
	root := t.TempDir()
	t.Setenv("AG_SYSTEM_PROXY_STATE_DIR", root)
	t.Setenv("AG_SYSTEM_PROXY_PORT", "")
	a, e := loadApp()
	if e != nil {
		t.Fatal(e)
	}
	if a.cfg.Idle != 3600 || a.cfg.MaxConnections != 128 || a.cfg.MaxPending != 64 || a.cfg.QueueWait != 2 || a.cfg.HealthFailures != 5 || a.cfg.WSWrite != 60 {
		t.Fatalf("unexpected defaults: %+v", a.cfg)
	}
	os.WriteFile(filepath.Join(root, "config.json"), []byte(`{"idle_timeout_seconds":900,"max_connections":8,"max_pending_connections":2,"admission_wait_seconds":0.1,"health_failures_before_restart":3,"websocket_write_timeout_seconds":120}`), 0600)
	a, e = loadApp()
	if e != nil {
		t.Fatal(e)
	}
	if a.cfg.Idle != 900 || a.cfg.MaxConnections != 8 || a.cfg.MaxPending != 2 || a.cfg.QueueWait != .1 || a.cfg.HealthFailures != 3 || a.cfg.WSWrite != 120 {
		t.Fatal("explicit settings were overwritten")
	}
}
