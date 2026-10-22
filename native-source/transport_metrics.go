//go:build windows

package main

import (
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"
)

// Only pending acquisitions retain an entry, bounded by admitted HTTP work.
// Entries contain no host, URL, credentials, or connection references.
type httpAcquireMetrics struct {
	mu                sync.Mutex
	waiting           map[*httpAcquireObservation]time.Time
	completed, failed durationStats
	reused, fresh     int64
}

type httpAcquireObservation struct {
	metrics *httpAcquireMetrics
	closed  bool // Protected by metrics.mu, including callbacks after RoundTrip.
}

func (o *httpAcquireObservation) getConn(string) {
	m := o.metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	if o.closed {
		return
	}
	if m.waiting == nil {
		m.waiting = make(map[*httpAcquireObservation]time.Time)
	}
	if started, ok := m.waiting[o]; ok {
		// An abandoned attempt must not disappear if Transport tries again.
		m.failed.observe(time.Since(started))
	}
	m.waiting[o] = time.Now()
}

func (o *httpAcquireObservation) gotConn(info httptrace.GotConnInfo) {
	m := o.metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	if o.closed {
		return
	}
	if started, ok := m.waiting[o]; ok {
		m.completed.observe(time.Since(started))
		delete(m.waiting, o)
		if info.Reused {
			m.reused++
		} else {
			m.fresh++
		}
	}
}

func (o *httpAcquireObservation) finish() {
	m := o.metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	if o.closed {
		return
	}
	o.closed = true
	if started, ok := m.waiting[o]; ok {
		// GetConn has no matching failure callback. RoundTrip completion covers
		// cancellation, dialing errors, and other failures before GotConn.
		m.failed.observe(time.Since(started))
		delete(m.waiting, o)
	}
}

func (m *httpAcquireMetrics) snapshot() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	var oldest time.Time
	for _, started := range m.waiting {
		if oldest.IsZero() || started.Before(oldest) {
			oldest = started
		}
	}
	age := float64(0)
	if !oldest.IsZero() {
		age = float64(time.Since(oldest)) / float64(time.Millisecond)
	}
	result := m.completed.snapshot()
	result["scope"] = "HTTP Transport GetConn to GotConn; includes pool wait and connection setup, not pure queue time; attempts, not requests; excludes opaque CONNECT"
	result["failed"] = m.failed.snapshot()
	result["in_flight"] = len(m.waiting)
	result["oldest_in_flight_ms"] = age
	result["reused"] = m.reused
	result["new"] = m.fresh
	return result
}

type observedHTTPTransport struct {
	base    http.RoundTripper
	metrics *httpAcquireMetrics
}

func (t observedHTTPTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	o := &httpAcquireObservation{metrics: t.metrics}
	defer o.finish()
	trace := &httptrace.ClientTrace{GetConn: o.getConn, GotConn: o.gotConn}
	// WithClientTrace composes existing hooks, including WebSocket tracking.
	r = r.WithContext(httptrace.WithClientTrace(r.Context(), trace))
	return t.base.RoundTrip(r)
}
