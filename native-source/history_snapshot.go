//go:build windows

package main

import (
	"runtime"
	"runtime/metrics"
)

type workerStatusSnapshot struct {
	HTTPTransport         map[string]any `json:"http_transport"`
	EntryProtocol         int            `json:"entry_protocol"`
	RuntimeFingerprint    string         `json:"runtime_fingerprint"`
	DestinationAdmission  map[string]any `json:"destination_admission"`
	PID                   int            `json:"pid"`
	Revision              string         `json:"revision"`
	Draining              bool           `json:"draining"`
	Diagnostics           map[string]any `json:"diagnostics"`
	Goroutines            int            `json:"goroutines"`
	HeapBytes             uint64         `json:"heap_bytes"`
	ActiveTunnels         int            `json:"active_tunnels"`
	ActiveWebSockets      int            `json:"active_websockets"`
	HTTPConnections       int            `json:"http_connections"`
	HTTPDials             int64          `json:"http_dials"`
	ActiveRequests        int            `json:"active_requests"`
	QueuedRequests        int            `json:"queued_requests"`
	MaxConnections        int            `json:"max_connections"`
	MaxPendingConnections int            `json:"max_pending_connections"`
	QueueTimeouts         int64          `json:"queue_timeouts"`
	QueueRejections       int64          `json:"queue_rejections"`
	IdleTimeoutSeconds    float64        `json:"idle_timeout_seconds"`
	History               map[string]any `json:"history,omitempty"`
}

// ReadMemStats stops the whole runtime. A control-plane gauge must not
// deliberately pause the data-plane goroutines on every status poll.
func currentHeapBytes() uint64 {
	samples := [1]metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(samples[:])
	if samples[0].Value.Kind() == metrics.KindUint64 {
		return samples[0].Value.Uint64()
	}
	return 0
}
func (c *core) statusSnapshot(pid int) workerStatusSnapshot {
	heap := currentHeapBytes()
	c.mu.Lock()
	active, httpConns, upgrades := len(c.tunnels), len(c.httpConns), len(c.upgrades)
	c.mu.Unlock()
	s := workerStatusSnapshot{DestinationAdmission: c.destinationAdmissionSnapshot(), PID: pid, Revision: revision, Draining: c.draining(), Diagnostics: c.metrics.snapshot(), Goroutines: runtime.NumGoroutine(), HeapBytes: heap, ActiveTunnels: active, ActiveWebSockets: upgrades, HTTPConnections: httpConns, HTTPDials: c.dials.Load(), ActiveRequests: len(c.slots), QueuedRequests: len(c.pending), MaxConnections: cap(c.slots), MaxPendingConnections: cap(c.pending), QueueTimeouts: c.queueTimeouts.Load(), QueueRejections: c.queueRejected.Load(), IdleTimeoutSeconds: c.a.cfg.Idle}
	if c.history != nil {
		s.History = c.history.health()
	}
	s.RuntimeFingerprint = c.a.runtimeFingerprint()
	s.HTTPTransport = map[string]any{"max_connections_per_host": c.transport.MaxConnsPerHost, "max_idle_connections": c.transport.MaxIdleConns, "max_idle_connections_per_host": c.transport.MaxIdleConnsPerHost, "write_buffer_bytes": c.transport.WriteBufferSize, "response_flush": "immediate", "connection_limit_scope": "Transport connection key; HTTP proxy keys may be shared across destinations; admission remains global"}
	s.EntryProtocol = 1
	s.HTTPTransport["fixed_length_copy_buffer_bytes"] = 64 * 1024
	return s
}
func (c *core) historyFaults() historyFaults {
	d := &c.metrics
	return historyFaults{Dial: d.dialFailures.Load(), HTTP: d.httpFailures.Load(), Auth: d.authFailures.Load(), AcquireEnded: d.httpAcquire.failed.count.Load(), QueueTimeout: c.queueTimeouts.Load(), QueueRejected: c.queueRejected.Load(), UpstreamRead: d.closes[closeUpstreamRead].Load(), UpstreamWrite: d.closes[closeUpstreamWrite].Load(), Idle: d.closes[closeIdle].Load(), WriteTimeout: d.closes[closeWriteTimeout].Load()}
}
