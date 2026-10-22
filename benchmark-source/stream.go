package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type observeWritesKey struct{}
type streamLogModeKey struct{}

func streamTrace(category, id string, sequence int, qpc int64) {
	if trace.IsEnabled() {
		trace.Log(context.Background(), category, fmt.Sprintf("id=%s seq=%d qpc=%d", id, sequence, qpc))
	}
}

// Test events carry QPC timestamps from this same process. Neither the bridge
// nor a real encrypted model connection needs payload inspection.
func streamFixture(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != "/timed-events" && r.URL.Path != "/paced-bytes" {
		return false
	}
	n, e := strconv.Atoi(r.URL.Query().Get("n"))
	if e != nil || n < 1 || n > 65536 {
		http.Error(w, "invalid event count", 400)
		return true
	}
	if r.URL.Path == "/paced-bytes" {
		w.Header().Set("Content-Length", strconv.Itoa(n*32768))
		buf := make([]byte, 32768)
		repeating{}.Read(buf)
		for i := 0; i < n; i++ {
			if _, e = w.Write(buf); e != nil {
				return true
			}
			if http.NewResponseController(w).Flush() != nil {
				return true
			}
			if !streamWait(r.Context(), time.Millisecond) {
				return true
			}
		}
		return true
	}
	interval, e := time.ParseDuration(r.URL.Query().Get("interval"))
	if e != nil || interval < time.Millisecond || interval > time.Second {
		http.Error(w, "invalid interval", 400)
		return true
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	// Wait after each actual emission; never catch up missed timer ticks by
	// generating an artificial burst. Actual source timestamps reveal scheduler jitter.
	id := r.URL.Query().Get("id")
	for i := 0; i < n; i++ {
		sent := tickNS()
		streamTrace("stream.source.begin", id, i, sent)
		if _, e = fmt.Fprintf(w, "data: %d %d\n\n", i, sent); e != nil {
			return true
		}
		if http.NewResponseController(w).Flush() != nil {
			return true
		}
		if trace.IsEnabled() {
			streamTrace("stream.source.flushed", id, i, tickNS())
		}
		if i+1 < n && !streamWait(r.Context(), interval) {
			return true
		}
	}
	return true
}
func streamWait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type pacedReader struct {
	ctx    context.Context
	reader io.Reader
}

func (p pacedReader) Read(b []byte) (int, error) {
	if !streamWait(p.ctx, time.Millisecond) {
		return 0, p.ctx.Err()
	}
	if len(b) > 32768 {
		b = b[:32768]
	}
	return p.reader.Read(b)
}

type streamEvent struct {
	SourceQPCNS  int64   `json:"source_qpc_ns"`
	ArrivalQPCNS int64   `json:"arrival_qpc_ns"`
	Route        string  `json:"route"`
	Scheme       string  `json:"scheme"`
	Load         string  `json:"load"`
	Round        int     `json:"round"`
	Sequence     int     `json:"sequence"`
	DeliveryMS   float64 `json:"source_to_client_ms"`
	ArrivalGapMS float64 `json:"arrival_gap_ms"`
	SourceGapMS  float64 `json:"source_gap_ms"`
	FirstEventMS float64 `json:"first_event_ms,omitempty"`
}
type streamStats struct {
	count                                                  int64
	delivery, gaps, excess, first                          []float64
	rng                                                    *rand.Rand
	maxDelivery, maxArrivalGap, maxSourceGap, maxExcessGap float64
}

func (s *streamStats) add(e streamEvent) {
	s.maxDelivery = max(s.maxDelivery, e.DeliveryMS)
	s.maxArrivalGap = max(s.maxArrivalGap, e.ArrivalGapMS)
	s.maxSourceGap = max(s.maxSourceGap, e.SourceGapMS)
	s.maxExcessGap = max(s.maxExcessGap, max(0, e.ArrivalGapMS-e.SourceGapMS))
	s.count++
	gap := e.ArrivalGapMS
	extra := max(0, gap-e.SourceGapMS)
	i := s.count - 1
	if s.count > reservoirLimit {
		i = s.rng.Int63n(s.count)
	}
	if i < reservoirLimit {
		if len(s.delivery) < reservoirLimit {
			s.delivery = append(s.delivery, e.DeliveryMS)
			s.gaps = append(s.gaps, gap)
			s.excess = append(s.excess, extra)
		} else {
			s.delivery[i] = e.DeliveryMS
			s.gaps[i] = gap
			s.excess[i] = extra
		}
	}
	if e.Sequence == 0 {
		s.first = append(s.first, e.FirstEventMS)
	}
}
func (s *streamStats) summary() map[string]any {
	return map[string]any{"max_delivery_ms": s.maxDelivery, "max_arrival_gap_ms": s.maxArrivalGap, "max_source_gap_ms": s.maxSourceGap, "max_excess_gap_ms": s.maxExcessGap, "events": s.count, "delivery_p50_ms": percentile(s.delivery, .5), "delivery_p95_ms": percentile(s.delivery, .95), "delivery_p99_ms": percentile(s.delivery, .99), "arrival_gap_p99_ms": percentile(s.gaps, .99), "excess_gap_p99_ms": percentile(s.excess, .99), "first_event_p95_ms": percentile(s.first, .95), "event_percentiles_approximate": s.count > reservoirLimit}
}

func measureEvents(ctx context.Context, client *http.Client, url string, n int, emit func(streamEvent)) error {
	started := tickNS()
	req, e := http.NewRequestWithContext(ctx, "GET", url, nil)
	if e != nil {
		return e
	}
	traceID := ""
	if trace.IsEnabled() {
		traceID = req.URL.Query().Get("id")
	}
	response, e := client.Do(req)
	if e != nil {
		return fmt.Errorf("stream request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("stream status %d", response.StatusCode)
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 4096), 16384)
	sequence := 0
	pending := ""
	var previousArrival, previousSent int64
	for scanner.Scan() {
		arrival := tickNS()
		line := scanner.Text()
		if line != "" {
			if pending != "" {
				return fmt.Errorf("missing event delimiter")
			}
			pending = line
			continue
		}
		if pending == "" {
			continue
		}
		fields := strings.Fields(pending)
		pending = ""
		if len(fields) != 3 || fields[0] != "data:" {
			return fmt.Errorf("invalid event")
		}
		seq, e := strconv.Atoi(fields[1])
		if e != nil || seq != sequence || seq >= n {
			return fmt.Errorf("event sequence mismatch")
		}
		sent, e := strconv.ParseInt(fields[2], 10, 64)
		if e != nil || sent <= 0 || sent > arrival || (sequence > 0 && sent < previousSent) {
			return fmt.Errorf("invalid event clock")
		}
		event := streamEvent{SourceQPCNS: sent, ArrivalQPCNS: arrival, Sequence: sequence, DeliveryMS: float64(arrival-sent) / 1e6}
		if sequence == 0 {
			event.FirstEventMS = float64(arrival-started) / 1e6
		} else {
			event.ArrivalGapMS = float64(arrival-previousArrival) / 1e6
			event.SourceGapMS = float64(sent-previousSent) / 1e6
		}
		streamTrace("stream.client", traceID, sequence, arrival)
		emit(event)
		previousArrival, previousSent = arrival, sent
		sequence++
	}
	if scanner.Err() != nil || sequence != n || pending != "" {
		return fmt.Errorf("incomplete event stream: %d/%d", sequence, n)
	}
	return nil
}

// Keep formatting, aggregation and disk IO off the receive loop. This is a
// bounded per-stream buffer, not an unbounded logger goroutine or request body.
func collectStreamEvents(ctx context.Context, client *http.Client, url string, n int) ([]streamEvent, error) {
	if n < 1 || n > 65536 {
		return nil, fmt.Errorf("invalid collector capacity")
	}
	events := make([]streamEvent, 0, n)
	err := measureEvents(ctx, client, url, n, func(event streamEvent) { events = append(events, event) })
	return events, err
}

func runStreamSuite(ctx context.Context, f *fixture, w *worker, mon *monitor, out, hash, mode string, n, rounds, load int, interval time.Duration) error {
	logMode, _ := ctx.Value(streamLogModeKey{}).(string)
	if logMode == "" {
		logMode = "deferred"
	}
	if logMode != "deferred" && logMode != "inline" {
		return fmt.Errorf("invalid stream logging mode")
	}
	raw, e := newJSON(filepath.Join(out, "stream-events.jsonl"), 64<<20)
	if e != nil {
		return e
	}
	defer raw.close()
	summaries := map[string]any{}
	failures := 0
	warmupErrors := 0
	var loadComplete, loadErrors atomic.Int64
	rng := rand.New(rand.NewSource(20261019))
	for _, scheme := range []string{"http", "https"} {
		for _, mixed := range []bool{false, true} {
			label := "idle"
			if mixed {
				label = "mixed"
			}
			mon.set("stream_" + scheme + "_" + label)

			stats := []*streamStats{{rng: rand.New(rand.NewSource(1))}, {rng: rand.New(rand.NewSource(2))}}
			for round := 0; round < rounds && ctx.Err() == nil; round++ {
				for _, route := range rng.Perm(2) {
					if ctx.Err() != nil {
						break
					}
					proxy := f.proxy.URL
					name := "direct"
					if route == 1 {
						proxy = w.endpoint()
						name = "bridge"
					}
					client, tr, e := makeClient(proxy, f.roots, false, true)
					if e != nil {
						return e
					}
					// Warm the transport separately; setup delay is reported for the timed request,
					// while handshake comparisons belong to the existing cold latency suite.
					prime := scenario{name: "stream_prime", method: "GET", url: scheme + "://bench.invalid/bytes?n=1", expectedBytes: 1, expectedDigest: digest(1), status: 200}
					if measure(ctx, client, name, prime).Error != "" {
						tr.close()
						failures++
						warmupErrors++
						fmt.Printf("stream warmup failed: %s/%s/%s\n", scheme, label, name)
						continue
					}
					loadCtx, cancel := context.WithCancel(ctx)
					var wg sync.WaitGroup
					if mixed {
						for j := 0; j < load; j++ {
							wg.Add(1)
							go func(j int) {
								defer wg.Done()
								c, t, err := makeClient(proxy, f.roots, false, true)
								if err != nil {
									loadErrors.Add(1)
									return
								}
								defer t.close()
								const size = 16 << 20
								sc := scenario{name: "stream_load_download", method: "GET", url: scheme + "://bench.invalid/paced-bytes?n=512", expectedBytes: size, expectedDigest: digest(size), status: 200}
								if j%2 == 0 {
									sum := sha256.Sum256([]byte(digest(size)))
									sc = scenario{name: "stream_load_upload", method: "POST", url: scheme + "://bench.invalid/upload?n=16777216", uploadBytes: size, expectedBytes: 64, expectedDigest: hex.EncodeToString(sum[:]), status: 200, uploadPaced: true}
								}
								for loadCtx.Err() == nil {
									result := measure(loadCtx, c, name, sc)
									if loadCtx.Err() != nil {
										return
									}
									if result.Error != "" {
										loadErrors.Add(1)
										if !streamWait(loadCtx, 10*time.Millisecond) {
											return
										}
									} else {
										loadComplete.Add(1)
									}
								}
							}(j)
						}
						streamWait(ctx, 150*time.Millisecond)
					}

					id := fmt.Sprintf("%s-%s-%s-%d", scheme, label, name, round)
					url := fmt.Sprintf("%s://bench.invalid/timed-events?n=%d&interval=%s&id=%s", scheme, n, interval, id)
					emit := func(event streamEvent) {
						event.Route = name
						event.Scheme = scheme
						event.Load = label
						event.Round = round
						stats[route].add(event)
						raw.write(event)
					}
					var events []streamEvent
					if logMode == "inline" {
						e = measureEvents(ctx, client, url, n, emit)
					} else {
						events, e = collectStreamEvents(ctx, client, url, n)
					}
					cancel()
					wg.Wait()
					tr.close()
					if logMode != "inline" {
						for _, event := range events {
							emit(event)
						}
					}

					if e != nil {
						failures++
						fmt.Printf("stream error: %s/%s/%s: %v\n", scheme, label, name, e)
					}
				}
			}
			summaries[scheme+"/"+label+"/direct"] = stats[0].summary()
			summaries[scheme+"/"+label+"/bridge"] = stats[1].summary()
		}
	}
	mon.set("cooldown")
	deadline := time.Now().Add(12 * time.Second)
	var status map[string]any
	quiescent := false
	for time.Now().Before(deadline) && ctx.Err() == nil {
		status, e = w.status()
		quiescent = e == nil
		for _, k := range []string{"active_requests", "active_tunnels", "active_websockets", "queued_requests"} {
			if status[k] != float64(0) {
				quiescent = false
			}
		}
		if w.entryMode {
			entry, ok := status["benchmark_entry"].(map[string]any)
			if !ok || entry["active_connections"] != float64(0) {
				quiescent = false
			}
		}
		if quiescent {
			break
		}
		streamWait(ctx, 100*time.Millisecond)
	}
	mon.stop()
	metadata := map[string]any{"benchmark_revision": "6-stream-recovery", "stream_log_mode": logMode, "collector_capacity_events": n, "socket_write_metrics": ctx.Value(observeWritesKey{}) == true, "bridge_sha256": hash, "bridge_revision": w.revision, "bridge_mode": mode, "events_per_stream": n, "rounds": rounds, "interval_ms": float64(interval) / 1e6, "mixed_load_workers": load, "load_completed": loadComplete.Load(), "load_errors": loadErrors.Load(), "stream_errors": failures, "warmup_errors": warmupErrors, "final_quiescent": quiescent, "monitor_errors": mon.failures, "raw_truncated": raw.truncated, "raw_write_error": raw.writeError, "canceled_load_policy": "in-flight loads canceled at phase end are excluded; completed loads validate length and SHA256", "interrupted": ctx.Err() != nil, "clock": "same-process Windows QPC at source formatting and client line receipt; includes fixture/TLS/parser overhead; not model tokens"}
	result := map[string]any{"metadata": metadata, "scenarios": summaries, "final_worker_status": status, "resource_phases": mon.summary()}
	data, e := json.MarshalIndent(result, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(out, "stream-summary.json"), data, 0600); e != nil {
		return e
	}
	fmt.Printf("complete: streaming errors=%d, load errors=%d; %s\n", failures, loadErrors.Load(), out)
	if failures > 0 || loadErrors.Load() > 0 || !quiescent || mon.failures > 0 || ctx.Err() != nil || raw.writeError {
		return fmt.Errorf("stream benchmark failed")
	}
	return nil
}
