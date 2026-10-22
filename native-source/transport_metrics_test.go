//go:build windows

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func awaitAcquire(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("connection acquisition condition not reached")
}
func poolWaitFixture(t *testing.T) (*core, *http.Client, string, func()) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(200)
		http.NewResponseController(w).Flush()
		if r.URL.Path == "/held" {
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		io.WriteString(w, "x")
	}))
	c, p, _ := fixture(t, Settings{})
	c.transport.MaxConnsPerHost = 1
	u, _ := url.Parse(p.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(u), DisableKeepAlives: true}
	client := &http.Client{Transport: tr, Timeout: 4 * time.Second}
	t.Cleanup(func() { unblock(); tr.CloseIdleConnections(); origin.Close() })
	return c, client, origin.URL, unblock
}
func readAcquireResponse(client *http.Client, ctx context.Context, target string) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
	response, e := client.Do(req)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	b, e := io.ReadAll(response.Body)
	if e != nil {
		return e
	}
	if response.StatusCode != 200 || string(b) != "x" {
		return errors.New("unexpected response")
	}
	return nil
}
func TestTransportAcquirePoolWaitAndReuse(t *testing.T) {
	c, client, target, release := poolWaitFixture(t)
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- readAcquireResponse(client, context.Background(), target+"/held") }()
	awaitAcquire(t, func() bool { return c.metrics.httpAcquire.completed.count.Load() == 1 })
	go func() { second <- readAcquireResponse(client, context.Background(), target+"/next") }()
	awaitAcquire(t, func() bool {
		m := c.metrics.httpAcquire.snapshot()
		return m["in_flight"].(int) == 1 && m["oldest_in_flight_ms"].(float64) >= 30
	})
	if len(c.pending) != 0 || c.metrics.queue.count.Load() != 0 {
		t.Fatal("Transport wait was confused with admission queue")
	}
	release()
	if e := <-first; e != nil {
		t.Fatal(e)
	}
	if e := <-second; e != nil {
		t.Fatal(e)
	}
	m := c.metrics.httpAcquire.snapshot()
	if m["count"] != int64(2) || m["new"] != int64(1) || m["reused"] != int64(1) || m["in_flight"] != 0 || m["oldest_in_flight_ms"] != float64(0) || c.metrics.httpAcquire.completed.max.Load() < int64(20*time.Millisecond) {
		t.Fatal(m)
	}
}
func TestTransportAcquireCanceledPoolWait(t *testing.T) {
	c, client, target, release := poolWaitFixture(t)
	first := make(chan error, 1)
	go func() { first <- readAcquireResponse(client, context.Background(), target+"/held") }()
	awaitAcquire(t, func() bool { return c.metrics.httpAcquire.completed.count.Load() == 1 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- readAcquireResponse(client, ctx, target+"/next") }()
	awaitAcquire(t, func() bool { return c.metrics.httpAcquire.snapshot()["oldest_in_flight_ms"].(float64) >= 25 })
	cancel()
	if e := <-second; e == nil {
		t.Fatal("canceled request succeeded")
	}
	awaitAcquire(t, func() bool {
		return c.metrics.httpAcquire.failed.count.Load() == 1 && c.metrics.httpAcquire.snapshot()["in_flight"].(int) == 0
	})
	if c.metrics.httpAcquire.completed.count.Load() != 1 || c.metrics.httpAcquire.failed.max.Load() < int64(20*time.Millisecond) || c.metrics.queue.count.Load() != 0 {
		t.Fatal(c.metrics.snapshot())
	}
	release()
	if e := <-first; e != nil {
		t.Fatal(e)
	}
}
func TestTransportAcquireDialFailurePrivacy(t *testing.T) {
	c, p, _ := fixture(t, Settings{})
	c.transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
		time.Sleep(25 * time.Millisecond)
		return nil, errors.New("secret-user:secret-password@secret-host")
	}
	u, _ := url.Parse(p.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(u)}
	defer tr.CloseIdleConnections()
	response, e := (&http.Client{Transport: tr, Timeout: 2 * time.Second}).Get("http://private.invalid/secret-query")
	if e != nil {
		t.Fatal(e)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 502 {
		t.Fatal(response.Status)
	}
	m := c.metrics.httpAcquire.snapshot()
	if m["in_flight"] != 0 || c.metrics.httpAcquire.failed.count.Load() != 1 || c.metrics.httpAcquire.completed.count.Load() != 0 || c.metrics.httpAcquire.failed.max.Load() < int64(20*time.Millisecond) {
		t.Fatal(m)
	}
	data, _ := json.Marshal(c.metrics.snapshot())
	if strings.Contains(string(data), "secret") || strings.Contains(string(data), "private.invalid") {
		t.Fatal("acquisition metrics exposed request details")
	}
}

type acquireRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f acquireRoundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestTransportAcquireRetriesTraceCompositionAndLateHooks(t *testing.T) {
	var m httpAcquireMetrics
	var inherited atomic.Int64
	var captured *httptrace.ClientTrace
	base := acquireRoundTripperFunc(func(r *http.Request) (*http.Response, error) {
		captured = httptrace.ContextClientTrace(r.Context())
		captured.GetConn("secret-host")
		captured.GotConn(httptrace.GotConnInfo{})
		captured.GetConn("secret-host")
		return nil, context.Canceled
	})
	req, _ := http.NewRequest("GET", "http://private.invalid", nil)
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { inherited.Add(1) }}))
	_, e := (observedHTTPTransport{base: base, metrics: &m}).RoundTrip(req)
	if !errors.Is(e, context.Canceled) || inherited.Load() != 1 {
		t.Fatal("existing trace lost", e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				captured.GetConn("late")
				captured.GotConn(httptrace.GotConnInfo{Reused: true})
			}
		}()
	}
	wg.Wait()
	s := m.snapshot()
	if m.completed.count.Load() != 1 || m.failed.count.Load() != 1 || s["in_flight"] != 0 || s["new"] != int64(1) || s["reused"] != int64(0) {
		t.Fatal(s)
	}
}
func TestTransportAcquireDoesNotCountResponseFailure(t *testing.T) {
	var m httpAcquireMetrics
	base := acquireRoundTripperFunc(func(r *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(r.Context())
		trace.GetConn("host")
		trace.GotConn(httptrace.GotConnInfo{Reused: true})
		return nil, io.ErrUnexpectedEOF
	})
	req, _ := http.NewRequest("GET", "http://host", nil)
	_, _ = (observedHTTPTransport{base: base, metrics: &m}).RoundTrip(req)
	s := m.snapshot()
	if m.completed.count.Load() != 1 || m.failed.count.Load() != 0 || s["in_flight"] != 0 || s["reused"] != int64(1) {
		t.Fatal("response failure incorrectly counted as acquisition failure", s)
	}
}
func TestTransportAcquireExcludesOpaqueCONNECT(t *testing.T) {
	origin := echoListener(t, func(c net.Conn) { io.Copy(c, c) })
	c, p, _ := fixture(t, Settings{})
	client, reader := connectTest(t, p, origin.Addr().String())
	defer client.Close()
	client.Write([]byte("ok"))
	b := make([]byte, 2)
	if _, e := io.ReadFull(reader, b); e != nil || string(b) != "ok" {
		t.Fatal(e)
	}
	if c.metrics.httpAcquire.completed.count.Load() != 0 || c.metrics.httpAcquire.failed.count.Load() != 0 || c.metrics.httpAcquire.snapshot()["in_flight"] != 0 {
		t.Fatal("opaque CONNECT counted as HTTP Transport", c.metrics.snapshot())
	}
}
