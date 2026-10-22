//go:build windows

package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func fairnessRecord(t *testing.T, name string, result map[string]any) {
	t.Helper()
	result["fixture_scope"] = "isolated Go core and loopback origins; not Internet/model latency"
	data, e := json.MarshalIndent(result, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	t.Log(string(data))
	if dir := os.Getenv("AG_FAIRNESS_REPORT_DIR"); dir != "" {
		if e = os.MkdirAll(dir, 0700); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, name+".json"), data, 0600); e != nil {
			t.Fatal(e)
		}
	}
}
func fairnessPercentile(v []float64, p float64) float64 {
	sort.Float64s(v)
	if len(v) == 0 {
		return 0
	}
	i := int(math.Ceil(float64(len(v))*p)) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(v) {
		i = len(v) - 1
	}
	return v[i]
}
func TestMixedLoadFairnessWithCapacity(t *testing.T) {
	for _, perLimit := range []int{0, 64} {
		for _, tlsMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("limit=%d/TLS=%v", perLimit, tlsMode), func(t *testing.T) {
				const size = int64(16 << 20)
				expected := uploadDigest(size)
				var started atomic.Int64
				gate := make(chan struct{})
				finish := make(chan struct{})
				var finishOnce sync.Once
				releaseFinish := func() { finishOnce.Do(func() { close(finish) }) }
				var once sync.Once
				release := func() { once.Do(func() { close(gate) }) }
				defer release()
				origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/upload":
						started.Add(1)
						<-gate
						h := sha256.New()
						b := make([]byte, 64<<10)
						var n int64
						for {
							got, e := r.Body.Read(b)
							if got > 0 {
								h.Write(b[:got])
								n += int64(got)
								time.Sleep(time.Millisecond)
							}
							if e != nil {
								if e != io.EOF {
									http.Error(w, "read error", 400)
									return
								}
								break
							}
						}
						if n != size {
							http.Error(w, "wrong size", 400)
							return
						}
						<-finish
						io.WriteString(w, hex.EncodeToString(h.Sum(nil)))
					case "/download":
						started.Add(1)
						<-gate
						w.Header().Set("Content-Length", fmt.Sprint(size))
						b := make([]byte, 64<<10)
						for i := range b {
							b[i] = 'x'
						}
						for left := size; left > 0; left -= int64(len(b)) {
							if left == int64(len(b)) {
								<-finish
							}
							if _, e := w.Write(b); e != nil {
								return
							}
							time.Sleep(time.Millisecond)
						}
					default:
						io.WriteString(w, "short-ok")
					}
				}))
				origin.EnableHTTP2 = true
				if tlsMode {
					origin.StartTLS()
				} else {
					origin.Start()
				}
				defer func() { release(); releaseFinish(); origin.Close() }()
				c, p, _ := fixture(t, Settings{})
				c.a.cfg.MaxPerDestination = perLimit
				client := streamingClient(t, p, origin)
				echo := echoListener(t, func(conn net.Conn) { io.Copy(conn, conn) })
				var held []net.Conn
				defer func() {
					for _, conn := range held {
						conn.Close()
					}
				}()
				for i := 0; i < 8; i++ {
					conn, _ := connectTest(t, p, echo.Addr().String())
					held = append(held, conn)
				}
				ws := websocketOrigin(t, false, func(conn net.Conn, rw *bufio.ReadWriter) { io.Copy(conn, rw) })
				for i := 0; i < 8; i++ {
					conn, _ := websocketClient(t, p, ws.URL)
					held = append(held, conn)
				}
				done := make(chan error, 4)
				for i := 0; i < 2; i++ {
					go func() {
						response, e := client.Post(origin.URL+"/upload", "application/octet-stream", io.LimitReader(repeatedUploadReader{'x'}, size))
						if e != nil {
							done <- e
							return
						}
						defer response.Body.Close()
						b, e := io.ReadAll(response.Body)
						if e == nil && (response.StatusCode != 200 || string(b) != expected) {
							e = fmt.Errorf("upload hash/status mismatch")
						}
						done <- e
					}()
					go func() {
						response, e := client.Get(origin.URL + "/download")
						if e != nil {
							done <- e
							return
						}
						defer response.Body.Close()
						h := sha256.New()
						n, e := io.Copy(h, response.Body)
						if e == nil && (n != size || hex.EncodeToString(h.Sum(nil)) != expected) {
							e = fmt.Errorf("download hash mismatch")
						}
						done <- e
					}()
				}
				await(t, func() bool { return started.Load() == 4 })
				release()
				latencies := []float64{}
				peakActive := len(c.slots)
				errors := 0
				for i := 0; i < 200; i++ {
					start := fairnessNowNS()
					response, e := client.Get(origin.URL + "/short")
					if e != nil {
						errors++
						continue
					}
					b, e := io.ReadAll(response.Body)
					response.Body.Close()
					if e != nil || response.StatusCode != 200 || string(b) != "short-ok" {
						errors++
					}
					latencies = append(latencies, float64(fairnessNowNS()-start)/1e6)
					peakActive = max(peakActive, len(c.slots))
					time.Sleep(3 * time.Millisecond)
				}
				releaseFinish()
				for i := 0; i < 4; i++ {
					if e := <-done; e != nil {
						t.Fatal(e)
					}
				}
				if errors != 0 || c.queueTimeouts.Load() != 0 || c.queueRejected.Load() != 0 {
					t.Fatal("mixed load failed", errors, c.metrics.snapshot())
				}
				for _, conn := range held {
					payload := maskedFrame(0x89, "alive")
					conn.Write(payload)
					conn.SetReadDeadline(time.Now().Add(time.Second))
					b := make([]byte, len(payload))
					if _, e := io.ReadFull(conn, b); e != nil || !bytes.Equal(b, payload) {
						t.Fatal("long connection disrupted", e)
					}
				}
				fairnessRecord(t, fmt.Sprintf("normal-limit-%d-tls-%v", perLimit, tlsMode), map[string]any{"per_destination_limit": perLimit, "tls": tlsMode, "short_requests": 200, "short_errors": errors, "short_complete_p50_ms": fairnessPercentile(latencies, .5), "short_complete_p95_ms": fairnessPercentile(latencies, .95), "short_complete_p99_ms": fairnessPercentile(latencies, .99), "bulk_transfers": 4, "bulk_bytes_each": size, "bulk_sha256_ok": true, "held_connect": 8, "held_websocket": 8, "peak_admission_sampled": peakActive, "max_connections": cap(c.slots), "queue_timeouts": c.queueTimeouts.Load(), "queue_rejections": c.queueRejected.Load()})
			})
		}
	}
}
func TestMixedLoadSaturationBaseline(t *testing.T) {
	c, p, _ := fixture(t, Settings{})
	echo := echoListener(t, func(conn net.Conn) { io.Copy(conn, conn) })
	var held []net.Conn
	defer func() {
		for _, conn := range held {
			conn.Close()
		}
	}()
	for i := 0; i < cap(c.slots); i++ {
		conn, _ := connectTest(t, p, echo.Addr().String())
		held = append(held, conn)
	}
	var commands atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { commands.Add(1); io.WriteString(w, "confirmed") }))
	defer origin.Close()
	client := streamingClient(t, p, origin)
	start := fairnessNowNS()
	response, e := client.Post(origin.URL, "text/plain", strings.NewReader("command"))
	if e != nil {
		t.Fatal(e)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	waitMS := float64(fairnessNowNS()-start) / 1e6
	if response.StatusCode != 503 || commands.Load() != 0 || c.queueTimeouts.Load() != 1 {
		t.Fatal("baseline saturation not reproduced", response.StatusCode, commands.Load())
	}
	held[0].Close()
	await(t, func() bool { return len(c.slots) < cap(c.slots) })
	response, e = client.Post(origin.URL, "text/plain", strings.NewReader("command"))
	if e != nil {
		t.Fatal(e)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || commands.Load() != 1 {
		t.Fatal("short request did not recover exactly once")
	}
	fairnessRecord(t, "saturation-baseline", map[string]any{"held_connect": cap(c.slots), "short_other_target_status": 503, "short_wait_ms": waitMS, "origin_executions_before_retry": 0, "origin_executions_after_explicit_retry": commands.Load(), "queue_timeouts": c.queueTimeouts.Load()})
}

var fairnessCounter = kernel.NewProc("QueryPerformanceCounter")
var fairnessFrequency = func() int64 {
	var value int64
	ok, _, _ := kernel.NewProc("QueryPerformanceFrequency").Call(uintptr(unsafe.Pointer(&value)))
	if ok == 0 || value <= 0 {
		panic("clock unavailable")
	}
	return value
}()

func fairnessNowNS() int64 {
	var value int64
	ok, _, _ := fairnessCounter.Call(uintptr(unsafe.Pointer(&value)))
	if ok == 0 {
		panic("clock unavailable")
	}
	return value/fairnessFrequency*1e9 + (value%fairnessFrequency)*1e9/fairnessFrequency
}
