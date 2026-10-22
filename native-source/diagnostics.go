//go:build windows

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Fixed buckets and reason codes keep memory bounded and contain no request data.
type durationStats struct {
	count, total, max atomic.Int64
	buckets           [7]atomic.Int64
}

func (d *durationStats) observe(v time.Duration) {
	n := int64(v)
	if n < 0 {
		return
	}
	d.count.Add(1)
	d.total.Add(n)
	for old := d.max.Load(); n > old; old = d.max.Load() {
		if d.max.CompareAndSwap(old, n) {
			break
		}
	}
	limits := [...]time.Duration{time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, time.Second, 10 * time.Second, time.Minute}
	i := 0
	for i < len(limits) && v > limits[i] {
		i++
	}
	d.buckets[i].Add(1)
}
func (d *durationStats) snapshot() map[string]any {
	n := d.count.Load()
	avg := float64(0)
	if n > 0 {
		avg = float64(d.total.Load()) / float64(n) / 1e6
	}
	b := make([]int64, 7)
	for i := range b {
		b[i] = d.buckets[i].Load()
	}
	return map[string]any{"count": n, "mean_ms": avg, "max_ms": float64(d.max.Load()) / 1e6, "bucket_counts": b}
}

const (
	closeLocal uint32 = iota
	closeClientEOF
	closeUpstreamEOF
	closeClientRead
	closeUpstreamRead
	closeClientWrite
	closeUpstreamWrite
	closeIdle
	closeWriteTimeout
	closeRoute
	closeShutdown
	closeReasonCount
)

var reasonNames = [closeReasonCount]string{"local_close", "client_eof", "upstream_eof", "client_read_error", "upstream_read_error", "client_write_error", "upstream_write_error", "idle_timeout", "write_timeout", "route_change", "shutdown"}

// Measures a socket Write call, including local scheduling and TLS work. A
// successful return means kernel acceptance, never delivery or a business ACK.
type socketWriteStats struct {
	duration durationStats
	active   atomic.Int64
}

func (s *socketWriteStats) begin() time.Time { s.active.Add(1); return time.Now() }
func (s *socketWriteStats) end(start time.Time) {
	s.duration.observe(time.Since(start))
	s.active.Add(-1)
}
func (s *socketWriteStats) snapshot() map[string]any {
	m := s.duration.snapshot()
	m["in_flight"] = s.active.Load()
	return m
}

type diagnostics struct {
	writeEnabled                                             bool
	clientWrites, upstreamWrites                             socketWriteStats
	httpAcquire                                              httpAcquireMetrics
	queue, tcp, handshake, firstByte, responseWait, lifetime durationStats
	closes                                                   [closeReasonCount]atomic.Int64
	authFailures, dialFailures, httpFailures, drainTimeouts  atomic.Int64
	upstreamRead, upstreamWritten, clientRead, clientWritten atomic.Int64
}

func (d *diagnostics) snapshot() map[string]any {
	reasons := map[string]int64{}
	for i, n := range reasonNames {
		reasons[n] = d.closes[i].Load()
	}
	return map[string]any{"socket_write": map[string]any{"enabled": d.writeEnabled, "to_client": d.clientWrites.snapshot(), "to_upstream": d.upstreamWrites.snapshot(), "scope": "completed socket Write attempts and current calls; includes TLS/scheduling; excludes write-lock wait; kernel acceptance, not remote delivery"}, "scope": "process lifetime; aggregate socket observations, not business acknowledgements", "duration_bucket_upper_ms": []any{1, 10, 100, 1000, 10000, 60000, "+Inf"}, "queue_wait": d.queue.snapshot(), "http_connection_acquire": d.httpAcquire.snapshot(), "tcp_connect": d.tcp.snapshot(), "proxy_handshake": d.handshake.snapshot(), "request_to_first_response_byte": d.firstByte.snapshot(), "request_written_to_first_response_byte": d.responseWait.snapshot(), "connection_lifetime": d.lifetime.snapshot(), "connection_closes": reasons, "dial_failures": d.dialFailures.Load(), "http_forward_failures": d.httpFailures.Load(), "proxy_auth_failures": d.authFailures.Load(), "drain_timeouts": d.drainTimeouts.Load(), "upstream_bytes_read": d.upstreamRead.Load(), "upstream_bytes_written": d.upstreamWritten.Load(), "client_bytes_read": d.clientRead.Load(), "client_bytes_written": d.clientWritten.Load()}
}

type metricsContext struct{}
type connectionObservation struct {
	d      *diagnostics
	start  time.Time
	reason atomic.Uint32
	once   sync.Once
}

func (o *connectionObservation) note(r uint32) {
	if o != nil {
		o.reason.CompareAndSwap(0, r)
	}
}
func (o *connectionObservation) finish() {
	if o != nil {
		o.once.Do(func() { o.d.closes[o.reason.Load()].Add(1); o.d.lifetime.observe(time.Since(o.start)) })
	}
}
func (c *idleConn) observeIO(n int, e error, write bool) {
	if c.observation == nil {
		return
	}
	d := c.observation.d
	if n > 0 {
		if c.upstream {
			if write {
				d.upstreamWritten.Add(int64(n))
			} else {
				d.upstreamRead.Add(int64(n))
			}
		} else {
			if write {
				d.clientWritten.Add(int64(n))
			} else {
				d.clientRead.Add(int64(n))
			}
		}
	}
	if e == nil || errors.Is(e, net.ErrClosed) {
		return
	}
	r := closeClientRead
	if c.upstream {
		r = closeUpstreamRead
	}
	if write {
		r = closeClientWrite
		if c.upstream {
			r = closeUpstreamWrite
		}
	}
	if errors.Is(e, io.EOF) {
		r = closeClientEOF
		if c.upstream {
			r = closeUpstreamEOF
		}
	}
	if ne, ok := e.(net.Error); ok && ne.Timeout() {
		r = closeIdle
		if write {
			r = closeWriteTimeout
		}
	}
	c.observation.note(r)
}
func noteClose(c net.Conn, reason uint32) {
	if tracked := findIdleConn(c); tracked != nil {
		tracked.observation.note(reason)
	}
}
func timedTCP(ctx context.Context, network, address string) (net.Conn, error) {
	start := time.Now()
	c, e := (&net.Dialer{Timeout: 12 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, address)
	if d, ok := ctx.Value(metricsContext{}).(*diagnostics); ok {
		d.tcp.observe(time.Since(start))
	}
	return c, e
}
