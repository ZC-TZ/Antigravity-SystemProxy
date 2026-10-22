package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type sample struct {
	Route         string  `json:"route"`
	Scenario      string  `json:"scenario"`
	Phase         string  `json:"phase,omitempty"`
	TTFBMS        float64 `json:"ttfb_ms"`
	TotalMS       float64 `json:"total_ms"`
	AcquireMS     float64 `json:"connection_acquire_ms"`
	TCPMS         float64 `json:"tcp_ms"`
	TLSMS         float64 `json:"tls_ms"`
	Bytes         int64   `json:"payload_bytes"`
	Reused        bool    `json:"reused"`
	Status        int     `json:"status"`
	Error         string  `json:"error,omitempty"`
	WriteAttempts int64   `json:"write_attempts"`
}
type scenario struct {
	uploadPaced                bool
	name, method, url          string
	uploadBytes, expectedBytes int64
	expectedDigest             string
	status                     int
}

type benchmarkTransport struct {
	*http.Transport
	mu          sync.Mutex
	closed      bool
	connections map[*benchmarkConn]struct{}
}
type benchmarkConn struct {
	net.Conn
	owner *benchmarkTransport
}

func (c *benchmarkConn) Close() error {
	c.owner.mu.Lock()
	delete(c.owner.connections, c)
	c.owner.mu.Unlock()
	return c.Conn.Close()
}
func (t *benchmarkTransport) dial(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		c.Close()
		return nil, net.ErrClosed
	}
	tracked := &benchmarkConn{Conn: c, owner: t}
	t.connections[tracked] = struct{}{}
	return tracked, nil
}

// A completed phase owns no further requests. Close its actual sockets as well
// as idle pools: a canceled HTTP/2 stream can become idle after pool cleanup.
func (t *benchmarkTransport) close() {
	t.mu.Lock()
	t.closed = true
	all := make([]*benchmarkConn, 0, len(t.connections))
	for c := range t.connections {
		all = append(all, c)
	}
	t.mu.Unlock()
	t.Transport.CloseIdleConnections()
	for _, c := range all {
		c.Close()
	}
}
func makeClient(proxy string, roots *x509.CertPool, cold, local bool) (*http.Client, *benchmarkTransport, error) {
	u, e := url.Parse(proxy)
	if e != nil || u.Host == "" {
		return nil, nil, errors.New("invalid benchmark proxy URL")
	}
	tr := &http.Transport{Proxy: http.ProxyURL(u), TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true, DisableCompression: true, DisableKeepAlives: cold, MaxIdleConns: 128, MaxIdleConnsPerHost: 32, MaxConnsPerHost: 64, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 20 * time.Second}
	_ = local
	owned := &benchmarkTransport{Transport: tr, connections: map[*benchmarkConn]struct{}{}}
	tr.DialContext = owned.dial
	return &http.Client{Transport: owned, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, owned, nil
}
func measure(ctx context.Context, client *http.Client, route string, sc scenario) sample {
	s := sample{Route: route, Scenario: sc.name}
	start := tickNS()
	var first, acquire, tcp, tlsTime, getStart, connectStart, tlsStart, attempts atomic.Int64
	var reused atomic.Bool
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { getStart.Store(tickNS()) }, GotConn: func(info httptrace.GotConnInfo) {
			reused.Store(info.Reused)
			if t := getStart.Load(); t > 0 {
				acquire.Store(tickNS() - t)
			}
		},
		ConnectStart: func(string, string) { connectStart.Store(tickNS()) }, ConnectDone: func(_, _ string, e error) {
			if t := connectStart.Load(); t > 0 && e == nil {
				tcp.Add(tickNS() - t)
			}
		},
		TLSHandshakeStart: func() { tlsStart.Store(tickNS()) }, TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			if t := tlsStart.Load(); t > 0 {
				tlsTime.Add(tickNS() - t)
			}
		},
		GotFirstResponseByte: func() { first.CompareAndSwap(0, (tickNS() - start)) }, WroteRequest: func(httptrace.WroteRequestInfo) { attempts.Add(1) },
	}
	var body io.Reader
	if sc.method == "POST" {
		body = io.LimitReader(repeating{}, sc.uploadBytes)
		if sc.uploadPaced {
			body = pacedReader{ctx: ctx, reader: body}
		}
	}
	request, e := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), sc.method, sc.url, body)
	if e != nil {
		s.Error = "request_setup"
		return s
	}
	if sc.method == "POST" {
		request.ContentLength = sc.uploadBytes
		request.Header.Set("Content-Type", "application/octet-stream")
	}
	response, e := client.Do(request)
	if e != nil {
		s.Error = "request_io"
	} else {
		s.Status = response.StatusCode
		h := sha256.New()
		n, readErr := io.Copy(h, io.LimitReader(response.Body, 65<<20))
		response.Body.Close()
		s.Bytes = n
		if sc.uploadBytes > 0 {
			s.Bytes += sc.uploadBytes
		}
		if readErr != nil {
			s.Error = "response_io"
		} else if response.StatusCode != sc.status {
			s.Error = "http_status"
		} else if sc.expectedBytes >= 0 && n != sc.expectedBytes {
			s.Error = "payload_length"
		} else if sc.expectedDigest != "" && hex.EncodeToString(h.Sum(nil)) != sc.expectedDigest {
			s.Error = "payload_hash"
		}
	}
	s.TotalMS = float64(tickNS()-start) / float64(time.Millisecond)
	s.TTFBMS = float64(first.Load()) / 1e6
	s.AcquireMS = float64(acquire.Load()) / 1e6
	s.TCPMS = float64(tcp.Load()) / 1e6
	s.TLSMS = float64(tlsTime.Load()) / 1e6
	s.Reused = reused.Load()
	s.WriteAttempts = attempts.Load()
	return s
}

type limitedJSON struct {
	file        *os.File
	used, limit int64
	truncated   bool
	writeError  bool
}

func newJSON(path string, limit int64) (*limitedJSON, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	return &limitedJSON{file: f, limit: limit}, nil
}
func (l *limitedJSON) write(v any) {
	if l == nil || l.truncated {
		return
	}
	b, e := json.Marshal(v)
	if e != nil {
		return
	}
	if l.used+int64(len(b)+1) > l.limit {
		l.truncated = true
		return
	}
	n, e := l.file.Write(append(b, '\n'))
	l.used += int64(n)
	if e != nil {
		l.truncated = true
		l.writeError = true
	}
}
func (l *limitedJSON) close() {
	if l != nil {
		l.file.Close()
	}
}

const reservoirLimit = 8192

type group struct {
	Count, Errors, Reused          int64
	Bytes                          int64
	TotalSeconds                   float64
	TTFB, Total, Acquire, TCP, TLS []float64
	rng                            *rand.Rand
}

func (g *group) add(s sample) {
	g.Count++
	if s.Error != "" {
		g.Errors++
		return
	}
	if s.Reused {
		g.Reused++
	}
	g.Bytes += s.Bytes
	g.TotalSeconds += s.TotalMS / 1000
	n := g.Count - g.Errors
	if n <= reservoirLimit {
		g.TTFB = append(g.TTFB, s.TTFBMS)
		g.Total = append(g.Total, s.TotalMS)
		g.Acquire = append(g.Acquire, s.AcquireMS)
		g.TCP = append(g.TCP, s.TCPMS)
		g.TLS = append(g.TLS, s.TLSMS)
	} else if index := g.rng.Int63n(n); index < reservoirLimit {
		g.TTFB[index] = s.TTFBMS
		g.Total[index] = s.TotalMS
		g.Acquire[index] = s.AcquireMS
		g.TCP[index] = s.TCPMS
		g.TLS[index] = s.TLSMS
	}
}
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	v := append([]float64(nil), values...)
	sort.Float64s(v)
	i := int(math.Ceil(p*float64(len(v)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(v) {
		i = len(v) - 1
	}
	return v[i]
}
func (g *group) summary() map[string]any {
	mibps := 0.0
	if g.TotalSeconds > 0 {
		mibps = float64(g.Bytes) / (1 << 20) / g.TotalSeconds
	}
	return map[string]any{"requests": g.Count, "errors": g.Errors, "reused": g.Reused, "payload_bytes": g.Bytes, "ttfb_p50_ms": percentile(g.TTFB, .5), "ttfb_p95_ms": percentile(g.TTFB, .95), "ttfb_p99_ms": percentile(g.TTFB, .99), "total_p50_ms": percentile(g.Total, .5), "total_p95_ms": percentile(g.Total, .95), "total_p99_ms": percentile(g.Total, .99), "connection_acquire_p95_ms": percentile(g.Acquire, .95), "tcp_p50_ms": percentile(g.TCP, .5), "tls_p50_ms": percentile(g.TLS, .5), "payload_mib_per_sum_request_second": mibps, "percentiles_approximate": g.Count-g.Errors > reservoirLimit, "low_tail_sample_count": g.Count-g.Errors < 100}
}

type recorder struct {
	mu                  sync.Mutex
	groups              map[string]*group
	raw                 *limitedJSON
	failures, completed int64
}

func (r *recorder) add(s sample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := s.Scenario + "/" + s.Route
	g := r.groups[key]
	if g == nil {
		g = &group{rng: rand.New(rand.NewSource(1))}
		r.groups[key] = g
	}
	g.add(s)
	r.completed++
	if s.Error != "" {
		r.failures++
	}
	r.raw.write(s)
}
func (r *recorder) counts() (int64, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completed, r.failures
}
func (r *recorder) summary() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := map[string]any{}
	for k, g := range r.groups {
		result[k] = g.summary()
	}
	return result
}

type resourceRow struct {
	QPCNS                                                              int64 `json:"qpc_ns"`
	StatusStartNS                                                      int64 `json:"status_start_qpc_ns,omitempty"`
	StatusEndNS                                                        int64 `json:"status_end_qpc_ns,omitempty"`
	Elapsed, PhaseSeconds                                              float64
	Phase                                                              string
	Worker, Runner                                                     processSample
	Goroutines, HeapBytes, Active, Queued, HTTPConnections, WebSockets float64
	Error                                                              string `json:",omitempty"`
}
type trend struct{ N, SX, SY, SXX, SXY float64 }

func (t *trend) add(x, y float64) { t.N++; t.SX += x; t.SY += y; t.SXX += x * x; t.SXY += x * y }
func (t trend) slope() float64 {
	den := t.N*t.SXX - t.SX*t.SX
	if den == 0 {
		return 0
	}
	return (t.N*t.SXY - t.SX*t.SY) / den
}

type resourceGroup struct {
	LifetimePeakWS                                             float64
	CPUStart, CPUEnd, TimeStart, TimeEnd                       float64
	HandleTrend, GoroutineTrend                                trend
	Count                                                      int
	PeakWS, PeakPrivate, PeakHeap, PeakGoroutines, PeakHandles float64
	First, Last                                                []resourceRow
	Trend                                                      trend
}

func (g *resourceGroup) add(s resourceRow) {
	if g.Count == 0 {
		g.CPUStart = s.Worker.CPUSeconds
		g.TimeStart = s.Elapsed
	}
	g.CPUEnd = s.Worker.CPUSeconds
	g.TimeEnd = s.Elapsed
	g.LifetimePeakWS = max(g.LifetimePeakWS, float64(s.Worker.PeakWorkingSet))
	g.Count++
	g.PeakWS = max(g.PeakWS, float64(s.Worker.WorkingSet))
	g.PeakPrivate = max(g.PeakPrivate, float64(s.Worker.Private))
	g.PeakHeap = max(g.PeakHeap, s.HeapBytes)
	g.PeakGoroutines = max(g.PeakGoroutines, s.Goroutines)
	g.PeakHandles = max(g.PeakHandles, float64(s.Worker.Handles))
	if len(g.First) < 300 {
		g.First = append(g.First, s)
	}
	if len(g.Last) == 300 {
		copy(g.Last, g.Last[1:])
		g.Last = g.Last[:299]
	}
	g.Last = append(g.Last, s)
	if s.PhaseSeconds >= 60 {
		g.Trend.add(s.PhaseSeconds, float64(s.Worker.Private)/(1<<20))
		g.HandleTrend.add(s.PhaseSeconds, float64(s.Worker.Handles))
		g.GoroutineTrend.add(s.PhaseSeconds, s.Goroutines)
	}
}
func (g *resourceGroup) summary() map[string]any {
	mean := func(v []resourceRow) float64 {
		var sum float64
		for _, s := range v {
			sum += float64(s.Worker.Private) / (1 << 20)
		}
		if len(v) == 0 {
			return 0
		}
		return sum / float64(len(v))
	}
	cpu := 0.0
	if g.TimeEnd > g.TimeStart {
		cpu = 100 * (g.CPUEnd - g.CPUStart) / (g.TimeEnd - g.TimeStart)
	}
	return map[string]any{"cpu_percent_one_core": cpu, "os_lifetime_peak_working_set_mib": g.LifetimePeakWS / (1 << 20), "handles_per_min_after_first_60s": g.HandleTrend.slope() * 60, "goroutines_per_min_after_first_60s": g.GoroutineTrend.slope() * 60, "samples": g.Count, "sampled_peak_working_set_mib": g.PeakWS / (1 << 20), "sampled_peak_private_mib": g.PeakPrivate / (1 << 20), "sampled_peak_heap_mib": g.PeakHeap / (1 << 20), "peak_goroutines": g.PeakGoroutines, "peak_handles": g.PeakHandles, "first_window_private_mib": mean(g.First), "last_window_private_mib": mean(g.Last), "private_mib_per_min_after_first_60s": g.Trend.slope() * 60, "steady_samples": g.Trend.N}
}

type monitor struct {
	stopOnce          sync.Once
	mu                sync.Mutex
	phase             string
	phaseStart, start time.Time
	groups            map[string]*resourceGroup
	raw               *limitedJSON
	worker            *worker
	failures          int
	cancel            context.CancelFunc
	done              chan struct{}
}

func startMonitor(ctx context.Context, w *worker, raw *limitedJSON) *monitor {
	ctx, cancel := context.WithCancel(ctx)
	m := &monitor{phase: "startup", start: time.Now(), phaseStart: time.Now(), groups: map[string]*resourceGroup{}, raw: raw, worker: w, cancel: cancel, done: make(chan struct{})}
	go m.run(ctx)
	return m
}
func (m *monitor) set(phase string) {
	m.mu.Lock()
	m.phase = phase
	m.phaseStart = time.Now()
	m.mu.Unlock()
	fmt.Println("phase:", phase)
}
func (m *monitor) run(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var state map[string]any
	var lastStatus time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			m.mu.Lock()
			row := resourceRow{QPCNS: tickNS(), Elapsed: now.Sub(m.start).Seconds(), PhaseSeconds: now.Sub(m.phaseStart).Seconds(), Phase: m.phase}
			m.mu.Unlock()
			var e error
			row.Worker, e = m.worker.resources()
			if e != nil {
				row.Error = "worker_resource"
			}
			row.Runner, e = resourceSample(currentProcessHandle())
			if e != nil {
				row.Error = "runner_resource"
			}
			if time.Since(lastStatus) >= time.Second {
				row.StatusStartNS = tickNS()
				state, e = m.worker.status()
				row.StatusEndNS = tickNS()
				lastStatus = time.Now()
				if e != nil {
					row.Error = "worker_status_identity_or_io"
				}
			}
			number := func(key string) float64 { n, _ := state[key].(float64); return n }
			row.Goroutines = number("goroutines")
			row.HeapBytes = number("heap_bytes")
			row.Active = number("active_requests")
			row.Queued = number("queued_requests")
			row.HTTPConnections = number("http_connections")
			row.WebSockets = number("active_websockets")
			m.mu.Lock()
			if row.Error != "" {
				m.failures++
			}
			g := m.groups[row.Phase]
			if g == nil {
				g = &resourceGroup{}
				m.groups[row.Phase] = g
			}
			g.add(row)
			m.raw.write(row)
			m.mu.Unlock()
		}
	}
}
func (m *monitor) stop() { m.stopOnce.Do(func() { m.cancel(); <-m.done }) }
func (m *monitor) summary() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := map[string]any{}
	for key, g := range m.groups {
		result[key] = g.summary()
	}
	return result
}
