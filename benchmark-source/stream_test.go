package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSlowInlineObserverCreatesTailWithoutProxy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 8; i++ {
			fmt.Fprintf(w, "data: %d %d\n\n", i, tickNS())
		}
		http.NewResponseController(w).Flush()
	}))
	defer server.Close()
	var inline []streamEvent
	err := measureEvents(context.Background(), server.Client(), server.URL, 8, func(e streamEvent) {
		inline = append(inline, e)
		if e.Sequence == 0 {
			time.Sleep(40 * time.Millisecond)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if inline[1].DeliveryMS < 30 {
		t.Fatal("slow observer did not produce the expected false tail", inline[1])
	}
	deferred, err := collectStreamEvents(context.Background(), server.Client(), server.URL, 8)
	if err != nil {
		t.Fatal(err)
	}
	loggingStarted := tickNS()
	time.Sleep(40 * time.Millisecond)
	for _, e := range deferred {
		if e.ArrivalQPCNS > loggingStarted {
			t.Fatal("receiver overlapped deferred logging")
		}
	}
	maxLatency := 0.0
	for _, e := range deferred {
		maxLatency = max(maxLatency, e.DeliveryMS)
	}
	t.Logf("no proxy: injected observer sleep=40ms, inline event1=%.3fms; collected max=%.3fms", inline[1].DeliveryMS, maxLatency)
}

func TestInterruptedStreamSuitePreservesPartialReport(t *testing.T) {
	exe, err := filepath.Abs(filepath.Join("..", "AntigravitySystemProxy.exe"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(exe); err != nil {
		t.Skip("bridge executable absent")
	}
	f, err := newFixture()
	if err != nil {
		t.Fatal(err)
	}
	defer f.close()
	w, err := startBridge(context.Background(), exe, f.proxy.URL, "entry")
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	out := t.TempDir()
	raw, err := newJSON(filepath.Join(out, "resources.jsonl"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mon := startMonitor(ctx, w, raw)
	defer mon.stop()
	// Wait for evidence of a received event instead of assuming setup finishes
	// within a fixed timer. Both logging modes share the suite's report path.
	ctx = context.WithValue(ctx, streamLogModeKey{}, "inline")
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if info, e := os.Stat(filepath.Join(out, "stream-events.jsonl")); e == nil && info.Size() > 0 {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-watchDone }()
	err = runStreamSuite(ctx, f, w, mon, out, "test", "entry", 2000, 2, 4, time.Millisecond)
	if err == nil {
		t.Fatal("interrupted benchmark reported success")
	}
	data, err := os.ReadFile(filepath.Join(out, "stream-summary.json"))
	if err != nil {
		t.Fatal("interruption discarded the partial report:", err)
	}
	var report struct {
		Metadata struct {
			Interrupted bool `json:"interrupted"`
		} `json:"metadata"`
		Scenarios map[string]struct {
			Events int `json:"events"`
		} `json:"scenarios"`
	}
	if err = json.Unmarshal(data, &report); err != nil || !report.Metadata.Interrupted {
		t.Fatal("missing interruption marker", err)
	}
	count := 0
	for _, s := range report.Scenarios {
		count += s.Events
	}
	if count == 0 {
		t.Fatal("partial events discarded")
	}
	w.close()
	if _, err = os.Stat(w.root); !os.IsNotExist(err) {
		t.Fatal("interruption left private backend state", err)
	}
}

func TestStreamWarmupFailuresAreReported(t *testing.T) {
	exe, err := filepath.Abs(filepath.Join("..", "AntigravitySystemProxy.exe"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(exe); err != nil {
		t.Skip("bridge executable absent")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	defer server.Close()
	f := &fixture{proxy: server}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := startBridge(ctx, exe, server.URL, "entry")
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	out := t.TempDir()
	raw, err := newJSON(filepath.Join(out, "resources.jsonl"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.close()
	mon := startMonitor(ctx, w, raw)
	defer mon.stop()
	if runStreamSuite(ctx, f, w, mon, out, "test", "entry", 2, 1, 4, time.Millisecond) == nil {
		t.Fatal("warmup failures reported success")
	}
	data, err := os.ReadFile(filepath.Join(out, "stream-summary.json"))
	if err != nil {
		t.Fatal("warmup error discarded report", err)
	}
	var report struct {
		Metadata struct {
			WarmupErrors int `json:"warmup_errors"`
			StreamErrors int `json:"stream_errors"`
		} `json:"metadata"`
	}
	if err = json.Unmarshal(data, &report); err != nil || report.Metadata.WarmupErrors != 8 || report.Metadata.StreamErrors != 8 {
		t.Fatal("missing warmup failures", string(data), err)
	}
}

// Invoke explicitly with go test -trace to inspect source flushes, client
// scheduling, GC and syscall waits in the synthetic runner. Never traces model traffic.
func TestStreamTailTraceProbe(t *testing.T) {
	exe := os.Getenv("AG_STREAM_TRACE_EXE")
	if exe == "" {
		t.Skip("explicit synthetic trace probe only")
	}
	out := os.Getenv("AG_STREAM_TRACE_OUT")
	if out == "" {
		out = t.TempDir()
	}
	if e := os.MkdirAll(out, 0700); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mode := os.Getenv("AG_STREAM_LOG_MODE")
	if mode == "" {
		mode = "deferred"
	}
	ctx = context.WithValue(ctx, streamLogModeKey{}, mode)
	f, e := newFixture()
	if e != nil {
		t.Fatal(e)
	}
	defer f.close()
	w, e := startBridge(ctx, exe, f.proxy.URL, "entry")
	if e != nil {
		t.Fatal(e)
	}
	defer w.close()
	raw, e := newJSON(filepath.Join(out, "resources.jsonl"), 64<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer raw.close()
	mon := startMonitor(ctx, w, raw)
	defer mon.stop()
	binary, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(binary))
	if e = runStreamSuite(ctx, f, w, mon, out, hash, "entry", 600, 2, 4, 2*time.Millisecond); e != nil {
		t.Fatal(e)
	}
	// A trace is a diagnostic perturbation; store its environment, not a speed claim.
	data, _ := json.Marshal(map[string]any{"trace_probe": true, "mode": mode, "source": "synthetic only"})
	os.WriteFile(filepath.Join(out, "probe.json"), data, 0600)
}

func TestTimedStreamMeasuresAllEventsOverHTTPAndTLS(t *testing.T) {
	f, e := newFixture()
	if e != nil {
		t.Fatal(e)
	}
	defer f.close()
	for _, scheme := range []string{"http", "https"} {
		c, tr, e := makeClient(f.proxy.URL, f.roots, false, true)
		if e != nil {
			t.Fatal(e)
		}
		count := 0
		e = measureEvents(context.Background(), c, scheme+"://bench.invalid/timed-events?n=8&interval=2ms", 8, func(s streamEvent) {
			if s.Sequence != count || s.DeliveryMS < 0 || s.FirstEventMS < 0 {
				t.Error(s)
			}
			if count > 0 && s.SourceGapMS <= 0 {
				t.Error("missing source interval", s)
			}
			count++
		})
		tr.close()
		if e != nil || count != 8 {
			t.Fatal(scheme, count, e)
		}
	}
}
func TestTimedStreamRejectsTruncationSequenceAndClockCorruption(t *testing.T) {
	for _, mode := range []string{"truncated", "sequence", "clock", "delimiter", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "truncated":
					fmt.Fprintf(w, "data: 0 %d\n\n", tickNS())
				case "sequence":
					fmt.Fprintf(w, "data: 1 %d\n\n", tickNS())
				case "clock":
					fmt.Fprintf(w, "data: 0 %d\n\n", tickNS()+int64(time.Hour))
				case "delimiter":
					fmt.Fprintf(w, "data: 0 %d\n", tickNS())
				case "oversize":
					for i := 0; i < 20000; i++ {
						io.WriteString(w, "x")
					}
				}
			}))
			defer server.Close()
			expected := 2
			if mode == "delimiter" {
				expected = 1
			}
			if e := measureEvents(context.Background(), server.Client(), server.URL, expected, func(streamEvent) {}); e == nil {
				t.Fatal("corrupt stream accepted")
			}
		})
	}
}
func TestTimedStreamDetectsBackpressureAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { streamFixture(w, r) }))
	defer server.Close()
	e := measureEvents(ctx, server.Client(), server.URL+"/timed-events?n=100&interval=1ms", 100, func(s streamEvent) {
		if s.Sequence == 2 {
			cancel()
		}
	})
	if e == nil {
		t.Fatal("canceled stream completed")
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if streamWait(canceled, time.Hour) {
		t.Fatal("canceled pacing wait completed")
	}
}
