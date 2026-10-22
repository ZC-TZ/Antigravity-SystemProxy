//go:build windows

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestDurationStatsBoundaries(t *testing.T) {
	var d durationStats
	for _, n := range []time.Duration{0, time.Millisecond, 2 * time.Millisecond, time.Second, 2 * time.Minute} {
		d.observe(n)
	}
	if d.count.Load() != 5 || d.max.Load() != int64(2*time.Minute) || d.buckets[0].Load() != 2 || d.buckets[6].Load() != 1 {
		t.Fatal(d.snapshot())
	}
}

func TestSocketWriteStatsObserveBlockingAndRelease(t *testing.T) {
	for _, upstream := range []bool{false, true} {
		left, right := net.Pipe()
		defer left.Close()
		defer right.Close()
		d := diagnostics{writeEnabled: true}
		stats := &d.clientWrites
		if upstream {
			stats = &d.upstreamWrites
		}
		c := &idleConn{Conn: left, timeout: time.Second, activity: &atomic.Int64{}, upstream: upstream, observation: &connectionObservation{d: &d}}
		done := make(chan error, 1)
		go func() { _, e := c.Write([]byte("confirmation")); done <- e }()
		deadline := time.Now().Add(time.Second)
		for stats.active.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if stats.active.Load() != 1 {
			t.Fatal("blocked write not observable")
		}
		time.Sleep(20 * time.Millisecond)
		got := make([]byte, 12)
		if _, e := io.ReadFull(right, got); e != nil {
			t.Fatal(e)
		}
		select {
		case e := <-done:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(time.Second):
			t.Fatal("write retained after receiver resumed")
		}
		if string(got) != "confirmation" || stats.active.Load() != 0 || stats.duration.count.Load() != 1 || stats.duration.max.Load() < int64(20*time.Millisecond) {
			t.Fatal("incorrect write observation", stats.snapshot())
		}
	}
}
func TestDiagnosticConnectionCloseExactlyOnce(t *testing.T) {
	var d diagnostics
	o := &connectionObservation{d: &d, start: time.Now()}
	o.note(closeRoute)
	o.note(closeUpstreamRead)
	o.finish()
	o.finish()
	if d.closes[closeRoute].Load() != 1 || d.lifetime.count.Load() != 1 {
		t.Fatal(d.snapshot())
	}
}
func TestDiagnosticsHTTPWaitAndPrivacy(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(30 * time.Millisecond)
		io.WriteString(w, "secret-body-token")
	}))
	defer origin.Close()
	c, p, _ := fixture(t, Settings{})
	proxyURL, _ := url.Parse(p.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	defer tr.CloseIdleConnections()
	resp, e := (&http.Client{Transport: tr, Timeout: 2 * time.Second}).Get(origin.URL + "/?secret-query")
	if e != nil {
		t.Fatal(e)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if c.metrics.firstByte.count.Load() != 1 || c.metrics.responseWait.count.Load() != 1 || c.metrics.responseWait.max.Load() < int64(20*time.Millisecond) || c.metrics.tcp.count.Load() != 1 {
		t.Fatal(c.metrics.snapshot())
	}
	data, _ := json.Marshal(c.metrics.snapshot())
	if bytes.Contains(data, []byte("secret")) {
		t.Fatal("diagnostics contains request data")
	}
}
func TestDiagnosticsQueueWait(t *testing.T) {
	c, _, _ := fixture(t, Settings{})
	for i := 0; i < cap(c.slots); i++ {
		c.slots <- struct{}{}
	}
	done := make(chan error, 1)
	go func() { done <- c.acquire(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	<-c.slots
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if c.metrics.queue.count.Load() != 1 || c.metrics.queue.max.Load() < int64(10*time.Millisecond) {
		t.Fatal(c.metrics.queue.snapshot())
	}
	for len(c.slots) > 0 {
		<-c.slots
	}
}
func TestDrainPreservesCONNECTAndRejectsNewWork(t *testing.T) {
	origin := echoListener(t, func(c net.Conn) { io.Copy(c, c) })
	c, p, _ := fixture(t, Settings{})
	client, reader := connectTest(t, p, origin.Addr().String())
	defer client.Close()
	c.beginDrain(time.Second)
	if !errors.Is(c.acquire(context.Background()), errDraining) {
		t.Fatal("drain admitted request")
	}
	client.Write([]byte("alive"))
	b := make([]byte, 5)
	if _, e := io.ReadFull(reader, b); e != nil || string(b) != "alive" {
		t.Fatal("drain broke active tunnel", e)
	}
	if len(c.slots) != 1 {
		t.Fatal("tunnel not included in drain count")
	}
	c.resume()
	if e := c.acquire(context.Background()); e != nil {
		t.Fatal(e)
	}
	<-c.slots
}
func TestDrainPreservesWebSocket(t *testing.T) {
	origin := websocketOrigin(t, false, func(c net.Conn, rw *bufio.ReadWriter) { io.Copy(c, rw) })
	c, p, _ := fixture(t, Settings{})
	client, reader := websocketClient(t, p, origin.URL)
	defer client.Close()
	c.beginDrain(time.Second)
	want := maskedFrame(0x89, "alive")
	client.Write(want)
	b := make([]byte, len(want))
	if _, e := io.ReadFull(reader, b); e != nil || !bytes.Equal(b, want) {
		t.Fatal("drain broke WebSocket", e)
	}
	if len(c.slots) != 1 {
		t.Fatal("WebSocket not counted")
	}
	c.resume()
}
func TestDrainLeaseExpires(t *testing.T) {
	c, _, _ := fixture(t, Settings{})
	c.beginDrain(20 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if e := c.acquire(context.Background()); e != nil {
		t.Fatal("expired drain lease blocks service", e)
	}
	<-c.slots
}
func TestDiagnosticsRouteCloseAndIdleTimeout(t *testing.T) {
	t.Run("route", func(t *testing.T) {
		origin := echoListener(t, func(c net.Conn) { io.Copy(c, c) })
		var calls atomic.Int64
		proxy := socksRelay(t, origin.Addr().String(), false, &calls)
		c, p, _ := fixture(t, Settings{true, "socks5://" + proxy.Addr().String(), ""})
		client, reader := connectTest(t, p, "remote.invalid:443")
		defer client.Close()
		c.updateSettings(Settings{})
		reader.ReadByte()
		deadline := time.Now().Add(time.Second)
		for c.metrics.closes[closeRoute].Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if c.metrics.closes[closeRoute].Load() != 1 {
			t.Fatal(c.metrics.snapshot())
		}
	})
	t.Run("idle", func(t *testing.T) {
		origin := echoListener(t, func(c net.Conn) { io.Copy(io.Discard, c) })
		c, p, _ := fixture(t, Settings{})
		c.a.cfg.Idle = .05
		client, reader := connectTest(t, p, origin.Addr().String())
		defer client.Close()
		reader.ReadByte()
		deadline := time.Now().Add(time.Second)
		for c.metrics.closes[closeIdle].Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if c.metrics.closes[closeIdle].Load() != 1 {
			t.Fatal(c.metrics.snapshot())
		}
	})
}
