//go:build windows

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime/metrics"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func otherRuntimePauses() uint64 {
	samples := []metrics.Sample{{Name: "/sched/pauses/total/other:seconds"}}
	metrics.Read(samples)
	var count uint64
	for _, n := range samples[0].Value.Float64Histogram().Counts {
		count += n
	}
	return count
}
func snapshotTestCore() *core {
	return &core{a: &app{cfg: Config{Port: 17891, MaxConnections: 128}}, transport: &http.Transport{}}
}

func TestStatusSnapshotDoesNotForceGlobalRuntimePause(t *testing.T) {
	c := snapshotTestCore()
	before := otherRuntimePauses()
	for i := 0; i < 20; i++ {
		snapshot := c.statusSnapshot(1)
		if snapshot.HeapBytes == 0 {
			t.Fatal("heap gauge missing")
		}
	}
	delta := otherRuntimePauses() - before
	t.Logf("20 diagnostic snapshots forced %d non-GC global pauses", delta)
	if delta != 0 {
		t.Fatal("status polling stopped data goroutines", delta)
	}
}
func BenchmarkStatusSnapshot(b *testing.B) {
	c := snapshotTestCore()
	b.ReportAllocs()
	b.ResetTimer()
	before := otherRuntimePauses()
	for i := 0; i < b.N; i++ {
		c.statusSnapshot(1)
	}
	delta := otherRuntimePauses() - before
	b.StopTimer()
	b.ReportMetric(float64(delta)/float64(b.N), "forced-STW/op")
}

// Exercise the actual forwarding path while status readers contend on the
// runtime gauge and connection maps. The source waits for each event's receipt,
// so completing this test requires incremental delivery, not an EOF buffer.
func TestDiagnosticStormPreservesStreamAndCommandConfirmation(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("TLS=%v", encrypted), func(t *testing.T) {
			ack := make(chan struct{}, 1)
			var posts atomic.Int64
			origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					body, err := io.ReadAll(r.Body)
					if err != nil || string(body) != "tool-result" {
						http.Error(w, "bad body", 400)
						return
					}
					posts.Add(1)
					io.WriteString(w, "command-confirmed")
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				for i := 0; i < 64; i++ {
					if _, err := fmt.Fprintf(w, "data: %d\n\n", i); err != nil {
						return
					}
					if http.NewResponseController(w).Flush() != nil {
						return
					}
					select {
					case <-ack:
					case <-r.Context().Done():
						return
					}
				}
			}))
			if encrypted {
				origin.EnableHTTP2 = true
				origin.StartTLS()
			} else {
				origin.Start()
			}
			defer origin.Close()
			c, proxy, _ := fixture(t, Settings{})
			client := streamingClient(t, proxy, origin)
			resp, err := client.Get(origin.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			var wg sync.WaitGroup
			var snapshots atomic.Int64
			for j := 0; j < 4; j++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 200; i++ {
						s := c.statusSnapshot(1)
						if _, err := json.Marshal(s); err != nil || s.HeapBytes == 0 {
							t.Errorf("invalid concurrent snapshot: %v", err)
							return
						}
						snapshots.Add(1)
					}
				}()
			}
			defer wg.Wait()
			reader := bufio.NewReader(resp.Body)
			for i := 0; i < 64; i++ {
				line, err := reader.ReadString('\n')
				if err != nil || line != fmt.Sprintf("data: %d\n", i) {
					t.Fatalf("event lost: %q %v", line, err)
				}
				if line, err = reader.ReadString('\n'); err != nil || line != "\n" {
					t.Fatal("event delimiter lost", err)
				}
				if i == 32 {
					confirmation, err := client.Post(origin.URL, "text/plain", strings.NewReader("tool-result"))
					if err != nil {
						t.Fatal("command confirmation stalled beside active stream", err)
					}
					body, err := io.ReadAll(confirmation.Body)
					confirmation.Body.Close()
					if err != nil || string(body) != "command-confirmed" {
						t.Fatal("confirmation lost", string(body), err)
					}
				}
				ack <- struct{}{}
			}
			if rest, err := io.ReadAll(reader); err != nil || len(rest) != 0 {
				t.Fatal("unexpected stream tail", err)
			}
			wg.Wait()
			if snapshots.Load() != 800 || posts.Load() != 1 {
				t.Fatal("incomplete snapshots or replayed command", snapshots.Load(), posts.Load())
			}
		})
	}
}
