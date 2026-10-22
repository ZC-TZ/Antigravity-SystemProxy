package main

import (
	"context"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestNearestRankAndBoundedReservoir(t *testing.T) {
	if percentile([]float64{4, 1, 3, 2}, .95) != 4 {
		t.Fatal("nearest rank")
	}
	g := &group{rng: rand.New(rand.NewSource(1))}
	for i := 0; i < 20000; i++ {
		g.add(sample{TTFBMS: float64(i), TotalMS: 1})
	}
	if len(g.Total) != reservoirLimit || g.Count != 20000 {
		t.Fatal("unbounded reservoir")
	}
	g.add(sample{Error: "io"})
	if g.Errors != 1 || len(g.Total) != reservoirLimit {
		t.Fatal("error accounting")
	}
}
func TestFixtureHashAndTLSReuse(t *testing.T) {
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
		sc := scenario{name: "check", method: "GET", url: scheme + "://bench.invalid/bytes?n=65536", expectedBytes: 65536, expectedDigest: digest(65536), status: 200}
		for i := 0; i < 2; i++ {
			s := measure(context.Background(), c, "direct", sc)
			if s.Error != "" || s.TTFBMS <= 0 || s.TotalMS < s.TTFBMS {
				t.Fatalf("invalid measurement: %+v", s)
			}
			if i == 1 && !s.Reused {
				t.Fatal("warm connection not reused")
			}
		}
		sc.expectedDigest = "bad"
		if measure(context.Background(), c, "direct", sc).Error != "payload_hash" {
			t.Fatal("hash corruption undetected")
		}
		tr.close()
	}
}
func TestWebSocketCancellation(t *testing.T) {
	f, e := newFixture()
	if e != nil {
		t.Fatal(e)
	}
	defer f.close()
	ctx, cancel := context.WithTimeout(context.Background(), 2300*time.Millisecond)
	defer cancel()
	count := 0
	e = wsPing(ctx, f.proxy.URL, f.roots, "direct", func(s sample) {
		if s.Error != "" {
			t.Error(s.Error)
		}
		count++
	})
	if e != nil || count != 1 {
		t.Fatalf("ping/cancellation %v %d", e, count)
	}
}
func TestWindowsResourcesAndClock(t *testing.T) {
	s, e := resourceSample(currentProcessHandle())
	if e != nil || s.WorkingSet == 0 || s.Private == 0 || s.Handles == 0 {
		t.Fatalf("invalid resources %+v %v", s, e)
	}
	before := tickNS()
	for tickNS() == before {
	}
	if tickNS() < before {
		t.Fatal("nonmonotonic performance counter")
	}
}
func TestRawCapAndNoOverwrite(t *testing.T) {
	p := filepath.Join(t.TempDir(), "raw.jsonl")
	l, e := newJSON(p, 10)
	if e != nil {
		t.Fatal(e)
	}
	l.write(map[string]string{"long": "value"})
	l.close()
	data, _ := os.ReadFile(p)
	if !l.truncated || len(data) > 10 {
		t.Fatal("raw cap")
	}
	if _, e = newJSON(p, 10); e == nil {
		t.Fatal("raw overwritten")
	}
}
func TestResourceTrend(t *testing.T) {
	var tr trend
	for i := 0; i < 100; i++ {
		tr.add(float64(i), 2*float64(i)+7)
	}
	if tr.slope() != 2 {
		t.Fatal("slope")
	}
}

func TestResourceSteadyTrendAndBoundedWindows(t *testing.T) {
	g := &resourceGroup{}
	for i := 0; i < 1000; i++ {
		g.add(resourceRow{Elapsed: float64(i), PhaseSeconds: float64(i), Worker: processSample{CPUSeconds: float64(i) / 10, Private: uint64(i) * (1 << 20), Handles: uint32(i), PeakWorkingSet: 12345}, Goroutines: float64(i)})
	}
	s := g.summary()
	if len(g.First) != 300 || len(g.Last) != 300 || g.Trend.N != 940 || s["private_mib_per_min_after_first_60s"] != float64(60) || s["handles_per_min_after_first_60s"] != float64(60) || s["cpu_percent_one_core"] != float64(10) {
		t.Fatalf("invalid bounded trends %+v", s)
	}
}
func TestDiskWriteFailureDistinctFromCap(t *testing.T) {
	l, e := newJSON(filepath.Join(t.TempDir(), "log"), 1000)
	if e != nil {
		t.Fatal(e)
	}
	l.close()
	l.write(map[string]int{"a": 1})
	if !l.writeError || !l.truncated {
		t.Fatal("disk write failure hidden")
	}
}
func TestStartupFailureCleansPrivateScratch(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMP", tmp)
	t.Setenv("TEMP", tmp)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := startWorker(ctx, filepath.Join(tmp, "missing-worker.exe"), "http://127.0.0.1:1"); e == nil {
		t.Fatal("missing worker accepted")
	}
	entries, e := os.ReadDir(tmp)
	if e != nil || len(entries) != 0 {
		t.Fatalf("private scratch leaked: %v %v", entries, e)
	}
}

func TestWorkerHistoryScratchCleanup(t *testing.T) {
	exe, e := filepath.Abs(filepath.Join("..", "AntigravitySystemProxy.exe"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(exe); e != nil {
		t.Skip("candidate bridge executable is not present")
	}
	f, e := newFixture()
	if e != nil {
		t.Fatal(e)
	}
	defer f.close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, e := startWorker(ctx, exe, f.proxy.URL)
	if e != nil {
		t.Fatal(e)
	}
	root := w.root
	w.close()
	if _, e = os.Stat(root); !os.IsNotExist(e) {
		t.Fatalf("owned scratch/history not cleaned: %v", e)
	}
}

func TestEntryArchitectureResourcesIsolationAndCleanup(t *testing.T) {
	exe, err := filepath.Abs(filepath.Join("..", "AntigravitySystemProxy.exe"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(exe); err != nil {
		t.Skip("bridge executable absent")
	}
	// A benchmark launched from a private worker must not inherit its identity.
	t.Setenv("AG_SYSTEM_PROXY_WORKER_ID", "unrelated-parent")
	f, err := newFixture()
	if err != nil {
		t.Fatal(err)
	}
	defer f.close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	w, err := startBridge(ctx, exe, f.proxy.URL, "entry")
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	if w.pid == w.entryPID || w.entryHandle == 0 {
		t.Fatal("entry process missing")
	}
	for _, scheme := range []string{"http", "https"} {
		client, tr, err := makeClient(w.endpoint(), f.roots, false, true)
		if err != nil {
			t.Fatal(err)
		}
		s := measure(ctx, client, "bridge", scenario{method: "GET", url: scheme + "://bench.invalid/bytes?n=65536", expectedBytes: 65536, expectedDigest: digest(65536), status: 200})
		tr.close()
		if s.Error != "" {
			t.Fatal(s.Error)
		}
	}
	client, tr, err := makeClient(w.endpoint(), f.roots, false, true)
	if err != nil {
		t.Fatal(err)
	}
	short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	sample := measure(short, client, "bridge", scenario{method: "GET", url: "https://bench.invalid/events?n=100", expectedBytes: -1, status: 200})
	stop()
	tr.close()
	if sample.Error == "" {
		t.Fatal("canceled stream unexpectedly completed")
	}
	quiescent := false
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		state, e := w.status()
		if e == nil && state["active_requests"] == float64(0) {
			quiescent = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !quiescent {
		t.Fatal("phase cancellation left test connections")
	}
	combined, err := w.resources()
	if err != nil {
		t.Fatal(err)
	}
	backend, err := resourceSample(w.handle)
	if err != nil || combined.Private <= backend.Private || combined.Handles <= backend.Handles {
		t.Fatalf("entry resources omitted: %+v %+v %v", combined, backend, err)
	}
	if _, err = w.status(); err != nil {
		t.Fatal(err)
	}
	w.entryGeneration = "wrong"
	if _, err = w.status(); err == nil {
		t.Fatal("entry identity change hidden")
	}
	root := w.root
	w.close()
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("entry scratch leaked: %v", err)
	}
}

func TestJobStartupHelper(t *testing.T) {
	mode := os.Getenv("PROXY_BENCH_JOB_TEST_HELPER")
	if mode == "" {
		return
	}
	if mode == "parent" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestJobStartupHelper$")
		for _, v := range os.Environ() {
			if !strings.HasPrefix(v, "PROXY_BENCH_JOB_TEST_HELPER=") {
				cmd.Env = append(cmd.Env, v)
			}
		}
		cmd.Env = append(cmd.Env, "PROXY_BENCH_JOB_TEST_HELPER=child")
		cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
		if err := cmd.Start(); err != nil {
			os.Exit(2)
		}
		if os.WriteFile(os.Getenv("PROXY_BENCH_JOB_TEST_MARKER"), []byte(strconv.Itoa(cmd.Process.Pid)), 0600) != nil {
			os.Exit(3)
		}
	}
	for {
		time.Sleep(time.Second)
	}
}

func TestJobClosesUnpublishedDescendants(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "child.pid")
	cmd := exec.Command(os.Args[0], "-test.run=^TestJobStartupHelper$")
	cmd.Env = append(os.Environ(), "PROXY_BENCH_JOB_TEST_HELPER=parent", "PROXY_BENCH_JOB_TEST_MARKER="+marker)
	job, process, _, err := launchInJob(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(process)
	defer func() {
		if job != 0 {
			syscall.CloseHandle(job)
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	var childPID int
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(marker); err == nil {
			childPID, _ = strconv.Atoi(string(data))
			if childPID > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if childPID == 0 {
		t.Fatal("helper descendant not started")
	}
	child, err := syscall.OpenProcess(0x100400, false, uint32(childPID))
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(child)
	syscall.CloseHandle(job)
	job = 0
	for _, h := range []syscall.Handle{process, child} {
		if event, err := syscall.WaitForSingleObject(h, 5000); err != nil || event != syscall.WAIT_OBJECT_0 {
			t.Fatalf("unpublished descendant survived job close: %v %v", event, err)
		}
	}
}
