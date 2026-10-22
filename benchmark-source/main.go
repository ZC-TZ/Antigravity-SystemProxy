package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	executable, _ := os.Executable()
	bridge := flag.String("bridge-exe", filepath.Join(filepath.Dir(executable), "..", "AntigravitySystemProxy.exe"), "isolated bridge executable")
	out := flag.String("out", "benchmark-results-"+time.Now().Format("20060102-150405"), "new output directory")
	samples := flag.Int("samples", 200, "paired samples per latency case")
	bulk := flag.Int("bulk-samples", 8, "paired 16 MiB upload/download samples")
	window := flag.Duration("resource-window", 10*time.Second, "each fixed-rate resource window (four windows)")
	duration := flag.Duration("soak-duration", 10*time.Minute, "mixed-load duration, up to 72h")
	cooldown := flag.Duration("cooldown-duration", 12*time.Second, "standard suite idle observation after load, 12s to 1h")
	qps := flag.Int("qps", 4, "total offered soak requests/second")
	concurrency := flag.Int("concurrency", 4, "bounded soak workers")
	soakScheme := flag.String("soak-scheme", "https", "http or https for soak requests; WebSockets remain WSS")
	upstream := flag.String("wan-upstream", "", "optional real upstream URL; never changes system settings")
	wanN := flag.Int("wan-samples", 12, "paired public HTTPS samples per cold/warm case")
	mode := flag.String("bridge-mode", "entry", "entry: full architecture; worker: isolated worker for development comparison")
	streamEvents := flag.Int("stream-events", 0, "dedicated per-event streaming suite; 0 runs standard benchmark")
	streamRounds := flag.Int("stream-rounds", 4, "paired rounds per streaming case")
	streamInterval := flag.Duration("stream-interval", 10*time.Millisecond, "source delay between stream events")
	streamLoad := flag.Int("stream-load", 4, "paced upload/download workers in mixed streaming case")
	observeWrites := flag.Bool("observe-writes", false, "enable detailed socket write timing in isolated candidate config")
	streamLogMode := flag.String("stream-log-mode", "deferred", "deferred: log after receive; inline: diagnostic observer-effect control")
	flag.Parse()
	if *streamLogMode != "deferred" && *streamLogMode != "inline" {
		return fmt.Errorf("invalid stream log mode")
	}
	if *streamEvents < 0 || *streamEvents > 65536 || *streamRounds < 1 || *streamRounds > 100 || *streamInterval < time.Millisecond || *streamInterval > time.Second || *streamLoad < 2 || *streamLoad > 16 {
		return fmt.Errorf("invalid stream limits")
	}

	if *streamEvents > 0 && time.Duration(*streamEvents-1)*(*streamInterval) > 20*time.Second {
		return fmt.Errorf("event stream must fit within 20 seconds")
	}
	if *mode != "entry" && *mode != "worker" {
		return fmt.Errorf("invalid bridge mode")
	}
	if *soakScheme != "http" && *soakScheme != "https" {
		return fmt.Errorf("invalid soak scheme")
	}
	if *samples < 1 || *samples > 100000 || *bulk < 1 || *bulk > 1000 || *duration < 0 || *duration > 72*time.Hour || *window < 0 || *window > time.Hour || *cooldown < 12*time.Second || *cooldown > time.Hour || *qps < 1 || *qps > 20 || *concurrency < 1 || *concurrency > 32 || *wanN < 1 || *wanN > 200 {
		return fmt.Errorf("invalid benchmark limits")
	}
	exe, e := filepath.Abs(*bridge)
	if e != nil {
		return e
	}
	binary, e := os.ReadFile(exe)
	if e != nil {
		return e
	}
	binaryHash := sha256.Sum256(binary)
	if e = os.Mkdir(*out, 0700); e != nil {
		return fmt.Errorf("output directory must be new: %w", e)
	}
	raw, e := newJSON(filepath.Join(*out, "samples.jsonl"), 64<<20)
	if e != nil {
		return e
	}
	defer raw.close()
	resourceRaw, e := newJSON(filepath.Join(*out, "resources.jsonl"), 64<<20)
	if e != nil {
		return e
	}
	defer resourceRaw.close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	f, e := newFixture()
	if e != nil {
		return e
	}
	defer f.close()
	ctx = context.WithValue(ctx, observeWritesKey{}, *observeWrites)
	ctx = context.WithValue(ctx, streamLogModeKey{}, *streamLogMode)
	w, e := startBridge(ctx, exe, f.proxy.URL, *mode)
	if e != nil {
		return e
	}
	defer w.close()
	if *observeWrites {
		status, err := w.status()
		if err != nil {
			return err
		}
		d, _ := status["diagnostics"].(map[string]any)
		sw, _ := d["socket_write"].(map[string]any)
		if sw["enabled"] != true {
			return fmt.Errorf("candidate does not support socket write observation")
		}
	}

	rec := &recorder{groups: map[string]*group{}, raw: raw}
	mon := startMonitor(ctx, w, resourceRaw)
	defer mon.stop()
	started := time.Now()
	if *streamEvents > 0 {
		return runStreamSuite(ctx, f, w, mon, *out, hex.EncodeToString(binaryHash[:]), *mode, *streamEvents, *streamRounds, *streamLoad, *streamInterval)
	}
	rng := rand.New(rand.NewSource(20261018))
	routes := []string{"direct", "bridge"}
	proxies := []string{f.proxy.URL, w.endpoint()}
	pairs := func(pairCtx context.Context, sc scenario, cold bool, n int, local bool, proxyURLs []string) {
		clients := make([]*http.Client, 2)
		transports := make([]*benchmarkTransport, 2)
		for i := range clients {
			var err error
			if local {
				clients[i], transports[i], err = makeClient(proxyURLs[i], f.roots, cold, local)
			} else {
				clients[i], transports[i], err = makeClient(proxyURLs[i], nil, cold, local)
			}
			if err != nil {
				rec.add(sample{Route: routes[i], Scenario: sc.name, Error: "client_setup"})
				return
			}
			defer transports[i].close()
			if !cold {
				_ = measure(pairCtx, clients[i], routes[i], sc)
			}
		}
		for j := 0; j < n && pairCtx.Err() == nil; j++ {
			order := rng.Perm(2)
			for _, i := range order {
				rec.add(measure(pairCtx, clients[i], routes[i], sc))
			}
		}
	}
	mon.set("local_latency")
	for _, scheme := range []string{"http", "https"} {
		for _, cold := range []bool{true, false} {
			mode := "warm"
			if cold {
				mode = "cold"
			}
			sc := scenario{name: "local_" + scheme + "_" + mode, method: "GET", url: scheme + "://bench.invalid/bytes?n=1024", expectedBytes: 1024, expectedDigest: digest(1024), status: 200}
			pairs(ctx, sc, cold, *samples, true, proxies)
		}
	}
	mon.set("local_bulk")
	n := int64(16 << 20)
	responseHash := sha256.Sum256([]byte(digest(n)))
	for _, scheme := range []string{"http", "https"} {
		pairs(ctx, scenario{name: "bulk_" + scheme + "_download", method: "GET", url: fmt.Sprintf("%s://bench.invalid/bytes?n=%d", scheme, n), expectedBytes: n, expectedDigest: digest(n), status: 200}, false, *bulk, true, proxies)
		pairs(ctx, scenario{name: "bulk_" + scheme + "_upload", method: "POST", url: fmt.Sprintf("%s://bench.invalid/upload?n=%d", scheme, n), uploadBytes: n, expectedBytes: 64, expectedDigest: hex.EncodeToString(responseHash[:]), status: 200}, false, *bulk, true, proxies)
	}
	windows := []map[string]any{}
	for repeat := 0; repeat < 2 && ctx.Err() == nil && *window > 0; repeat++ {
		for _, routeIndex := range rng.Perm(2) {
			phase := fmt.Sprintf("resource_%s_%d", routes[routeIndex], repeat+1)
			mon.set(phase)
			client, tr, _ := makeClient(proxies[routeIndex], f.roots, false, true)
			before, beforeError := w.resources()
			runnerBefore, runnerBeforeError := resourceSample(currentProcessHandle())
			begin := time.Now()
			windowCtx, stop := context.WithTimeout(ctx, *window)
			var count int64
			var bytes int64
			ticker := time.NewTicker(50 * time.Millisecond)
		loop:
			for {
				select {
				case <-windowCtx.Done():
					break loop
				case <-ticker.C:
					s := measure(windowCtx, client, routes[routeIndex], scenario{name: "fixed_rate_download", method: "GET", url: "https://bench.invalid/bytes?n=1048576", expectedBytes: 1 << 20, expectedDigest: digest(1 << 20), status: 200})
					if windowCtx.Err() != nil {
						break loop
					}
					s.Phase = phase
					rec.add(s)
					count++
					bytes += s.Bytes
				}
			}
			ticker.Stop()
			stop()
			elapsed := time.Since(begin).Seconds()
			after, afterError := w.resources()
			if beforeError != nil || afterError != nil {
				return fmt.Errorf("fixed-rate bridge resource sampling failed")
			}
			runnerAfter, runnerAfterError := resourceSample(currentProcessHandle())
			if runnerBeforeError != nil || runnerAfterError != nil {
				return fmt.Errorf("fixed-rate runner resource sampling failed")
			}
			tr.close()
			windows = append(windows, map[string]any{"phase": phase, "seconds": elapsed, "requests": count, "payload_mib_per_wall_second": float64(bytes) / (1 << 20) / elapsed, "worker_cpu_percent_one_core": 100 * (after.CPUSeconds - before.CPUSeconds) / elapsed, "runner_cpu_percent_one_core": 100 * (runnerAfter.CPUSeconds - runnerBefore.CPUSeconds) / elapsed})
		}
	}
	mon.set("soak")
	soakStart := time.Now()
	soakCtx, stopSoak := context.WithTimeout(ctx, *duration)
	var wg sync.WaitGroup
	var dropped atomic.Int64
	if *duration > 0 {
		for i := 0; i < 2; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := wsPing(soakCtx, proxies[i], f.roots, routes[i], rec.add); err != nil && soakCtx.Err() == nil {
					rec.add(sample{Route: routes[i], Scenario: "soak_websocket_session", Error: "websocket_session"})
				}
			}()
		}
		jobs := make(chan int, *concurrency)
		uploadN := int64(256 << 10)
		streamText := ""
		for event := 0; event < 8; event++ {
			streamText += fmt.Sprintf("data: %d\n\n", event)
		}
		streamHash := sha256.Sum256([]byte(streamText))
		uh := sha256.Sum256([]byte(digest(uploadN)))
		scenarios := []scenario{{name: "soak_small", method: "GET", url: "https://bench.invalid/bytes?n=1024", expectedBytes: 1024, expectedDigest: digest(1024), status: 200}, {name: "soak_upload", method: "POST", url: "https://bench.invalid/upload?n=262144", uploadBytes: uploadN, expectedBytes: 64, expectedDigest: hex.EncodeToString(uh[:]), status: 200}, {name: "soak_download", method: "GET", url: "https://bench.invalid/bytes?n=1048576", expectedBytes: 1 << 20, expectedDigest: digest(1 << 20), status: 200}, {name: "soak_stream", method: "GET", url: "https://bench.invalid/events?n=8", expectedBytes: int64(len(streamText)), expectedDigest: hex.EncodeToString(streamHash[:]), status: 200}}
		for i := range scenarios {
			scenarios[i].url = strings.Replace(scenarios[i].url, "https://", *soakScheme+"://", 1)
		}
		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				clients := make([]*http.Client, 2)
				for k := range clients {
					c, tr, _ := makeClient(proxies[k], f.roots, false, true)
					clients[k] = c
					defer tr.close()
				}
				for job := range jobs {
					if soakCtx.Err() != nil {
						return
					}
					routeIndex := (job / len(scenarios)) % 2
					s := measure(soakCtx, clients[routeIndex], routes[routeIndex], scenarios[job%len(scenarios)])
					if soakCtx.Err() == nil {
						rec.add(s)
					}
				}
			}()
		}
		ticker := time.NewTicker(time.Second / time.Duration(*qps))
		index := 0
	offer:
		for {
			select {
			case <-soakCtx.Done():
				break offer
			case <-ticker.C:
				select {
				case jobs <- index:
				default:
					dropped.Add(1)
				}
				index++
			}
		}
		ticker.Stop()
		close(jobs)
	}
	stopSoak()
	wg.Wait()
	soakSeconds := time.Since(soakStart).Seconds()
	mon.set("cooldown")
	cooldownStart := time.Now()
	select {
	case <-ctx.Done():
	case <-time.After(*cooldown):
	}
	cooldownSeconds := time.Since(cooldownStart).Seconds()
	finalStatus, statusError := w.status()
	quiescent := statusError == nil
	for _, key := range []string{"active_requests", "active_tunnels", "active_websockets", "queued_requests"} {
		if finalStatus[key] != float64(0) {
			quiescent = false
		}
	}
	if w.entryMode {
		entry, ok := finalStatus["benchmark_entry"].(map[string]any)
		if !ok || entry["active_connections"] != float64(0) {
			quiescent = false
		}
	}
	if *upstream != "" && ctx.Err() == nil {
		mon.set("wan")
		wanCtx, stopWAN := context.WithTimeout(ctx, 3*time.Minute)
		defer stopWAN()
		wanWorker, err := startBridge(wanCtx, exe, *upstream, *mode)
		if err != nil {
			rec.add(sample{Route: "bridge", Scenario: "wan_startup", Error: "worker_startup"})
		} else {
			for _, cold := range []bool{true, false} {
				mode := "warm"
				if cold {
					mode = "cold"
				}
				pairs(wanCtx, scenario{name: "wan_https_" + mode, method: "GET", url: "https://oauth2.googleapis.com/", expectedBytes: -1, status: 404}, cold, *wanN, false, []string{*upstream, wanWorker.endpoint()})
			}
			wanWorker.close()
		}
	}
	mon.stop()
	total, failures := rec.counts()
	safeUpstream := ""
	if *upstream != "" {
		if u, err := url.Parse(*upstream); err == nil {
			safeUpstream = u.Scheme + "://" + u.Host
		}
	}
	metadata := map[string]any{"started_utc": started.UTC(), "elapsed_seconds": time.Since(started).Seconds(), "os": runtime.GOOS, "arch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "benchmark_go": runtime.Version(), "bridge_revision": w.revision, "bridge_sha256": hex.EncodeToString(binaryHash[:]), "samples_per_latency_case": *samples, "bulk_samples": *bulk, "soak_requested_seconds": duration.Seconds(), "soak_actual_seconds": soakSeconds, "soak_offered_qps": *qps, "concurrency": *concurrency, "soak_dropped": dropped.Load(), "wan_upstream": safeUpstream, "wan_samples": *wanN, "interrupted": ctx.Err() != nil, "requests": total, "errors": failures, "raw_samples_truncated": raw.truncated, "raw_samples_write_error": raw.writeError, "raw_resources_write_error": resourceRaw.writeError, "raw_resources_truncated": resourceRaw.truncated, "monitor_errors": mon.failures, "resource_scope": "controlled_local_worker_only", "latency_clock": "Windows_QueryPerformanceCounter", "fixture_http2": true, "sample_seed": 20261018, "resource_sampling_ms": 200, "status_sampling_ms": 1000}
	metadata["bridge_mode"] = *mode
	metadata["soak_scheme"] = *soakScheme
	metadata["final_quiescent"] = quiescent
	metadata["benchmark_revision"] = "4-idle-recovery"
	metadata["cooldown_requested_seconds"] = cooldown.Seconds()
	metadata["cooldown_actual_seconds"] = cooldownSeconds
	if self, err := os.ReadFile(executable); err == nil {
		hash := sha256.Sum256(self)
		metadata["benchmark_sha256"] = hex.EncodeToString(hash[:])
	}
	metadata["go_runtime_resource_scope"] = "backend_worker_only"
	metadata["peak_working_set_scope"] = "sum_of_process_lifetime_peaks_not_simultaneous"
	if w.entryMode {
		metadata["resource_scope"] = "controlled_local_entry_plus_worker"
	}
	summary := map[string]any{"metadata": metadata, "scenarios": rec.summary(), "resource_phases": mon.summary(), "fixed_rate_windows": windows, "final_worker_status": finalStatus, "final_status_ok": statusError == nil}
	data, _ := json.MarshalIndent(summary, "", "  ")
	if e = os.WriteFile(filepath.Join(*out, "summary.json"), data, 0600); e != nil {
		return e
	}
	report := renderReport(summary)
	if e = os.WriteFile(filepath.Join(*out, "report.md"), []byte(report), 0600); e != nil {
		return e
	}
	fmt.Printf("complete: %d measured requests, %d errors; %s\n", total, failures, *out)
	if failures > 0 || statusError != nil || !quiescent || mon.failures > 0 || ctx.Err() != nil || dropped.Load() > 0 || raw.writeError || resourceRaw.writeError {
		return fmt.Errorf("benchmark detected request/status errors; inspect report")
	}
	return nil
}
func renderReport(result map[string]any) string {
	var b strings.Builder
	m := result["metadata"].(map[string]any)
	fmt.Fprintf(&b, "Bridge mode: %v; OS CPU/memory/handles scope: %v. Go heap/goroutines are backend-only. Lifetime peak working sets sum per-process peaks; sampled aggregate peaks are simultaneous. Legacy JSON Worker/worker_cpu field names refer to this selected scope.\n\n", m["bridge_mode"], m["resource_scope"])
	fmt.Fprintf(&b, "# Proxy benchmark\n\nIsolated bridge revision: %v. Requests: %v; errors: %v. Soak: %.1f seconds.\n\n", m["bridge_revision"], m["requests"], m["errors"], m["soak_actual_seconds"])
	fmt.Fprintf(&b, "Interrupted: %v; monitor errors: %v; raw logs capped: samples %v, resources %v.\n\n", m["interrupted"], m["monitor_errors"], m["raw_samples_truncated"], m["raw_resources_truncated"])
	b.WriteString("| Scenario / route | Requests | Errors | TTFB P50 ms | TTFB P95 ms | Total P99 ms | Payload MiB/s * |\n|---|---:|---:|---:|---:|---:|---:|\n")
	scenarios := result["scenarios"].(map[string]any)
	keys := make([]string, 0, len(scenarios))
	for k := range scenarios {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := scenarios[k].(map[string]any)
		fmt.Fprintf(&b, "| %s | %v | %v | %.3f | %.3f | %.3f | %.2f |\n", k, s["requests"], s["errors"], s["ttfb_p50_ms"], s["ttfb_p95_ms"], s["total_p99_ms"], s["payload_mib_per_sum_request_second"])
	}
	b.WriteString("\n* Payload divided by summed successful request durations, not parallel aggregate throughput. Failed requests excluded from latency. Percentiles use nearest rank and a bounded 8192-entry reservoir above 8192 successes. P99 with fewer than 100 samples is descriptive only.\n\n## Fixed-rate CPU comparison\n\n100% CPU = one logical core. Both routes use the same upstream; the bridge process remains idle but monitored in direct windows. Runner includes fixtures, hashing and measurement.\n\n| Window | Requests | MiB/s wall | Worker CPU % | Runner CPU % |\n|---|---:|---:|---:|---:|\n")
	for _, v := range result["fixed_rate_windows"].([]map[string]any) {
		fmt.Fprintf(&b, "| %v | %v | %.2f | %.2f | %.2f |\n", v["phase"], v["requests"], v["payload_mib_per_wall_second"], v["worker_cpu_percent_one_core"], v["runner_cpu_percent_one_core"])
	}
	phases := result["resource_phases"].(map[string]any)
	if phase, ok := phases["soak"]; ok {
		s := phase.(map[string]any)
		fmt.Fprintf(&b, "\n## Soak resources\n\nSampled peak working set %.2f MiB; peak private commit %.2f MiB; first/last (up to 60s) mean private commit %.2f / %.2f MiB; private-commit trend after first 60s %.4f MiB/min (%v points). Peak goroutines %v; handles %v. Offered-load drops %v.\n", s["sampled_peak_working_set_mib"], s["sampled_peak_private_mib"], s["first_window_private_mib"], s["last_window_private_mib"], s["private_mib_per_min_after_first_60s"], s["steady_samples"], s["peak_goroutines"], s["peak_handles"], m["soak_dropped"])
	}
	b.WriteString("\n## Interpretation limits\n\nLoopback results measure this host and synthetic fixture, not Internet bandwidth or model generation speed. Cold means a fresh client connection; the bridge can still reuse its own HTTP upstream pool. Warm clients are primed once outside statistics. Alternating randomized A/B order reduces drift but does not remove scheduler, GC or shared-host contention. Memory retention alone does not prove a leak. A 10-minute soak cannot establish 24-hour stability. No application tokens, business requests, system proxy configuration or installed worker were modified.\n\nConnection-acquire time includes pooling/dialing; it is not a pure queue metric. CPU is cumulative kernel+user time. OS resources sampled every 200ms, control status every second; observer overhead is included. Raw logs are capped at 64 MiB each; reaching that cap stops raw logging but summary aggregation continues with bounded memory. Soak request latency starts at dispatch, excludes benchmark scheduling delay, and is not an offered-load end-to-end percentile. End-of-soak in-flight requests canceled by the planned deadline are excluded; dropped offered jobs are counted separately. HTTPS uses end-to-end TLS through CONNECT; only the test origin certificate is trusted inside the benchmark client.\n\nFull metadata, final connection counters and phase resources are in summary.json. Raw observations are in samples.jsonl and resources.jsonl.\n")
	return b.String()
}
