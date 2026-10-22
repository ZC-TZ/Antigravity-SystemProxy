//go:build windows

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func destinationFixture(t *testing.T, total, per, pending int) (*core, *httptest.Server) {
	t.Helper()
	root := t.TempDir()
	settings := filepath.Join(root, "settings.json")
	os.WriteFile(settings, []byte(`{}`), 0600)
	a := &app{root: root, settingsFile: settings, cfg: Config{Port: 17891, Idle: 5, MaxConnections: total, MaxPerDestination: per, MaxPending: pending, QueueWait: .15}}
	c, e := newCore(a)
	if e != nil {
		t.Fatal(e)
	}
	p := httptest.NewServer(c)
	t.Cleanup(func() { c.close(); p.Close() })
	return c, p
}
func destinationAcquire(t *testing.T, c *core, key string) func() {
	t.Helper()
	release, e := c.requestAdmission(context.Background(), key)
	if e != nil {
		t.Fatal(e)
	}
	return release
}
func TestDestinationFairnessBypassesFullTargetQueue(t *testing.T) {
	c, _ := destinationFixture(t, 4, 1, 1)
	hold := destinationAcquire(t, c, "A")
	defer hold()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := c.requestAdmission(ctx, "A"); done <- e }()
	await(t, func() bool { return len(c.pending) == 1 })
	if _, e := c.requestAdmission(context.Background(), "A"); !errors.Is(e, errCapacity) {
		t.Fatal("full target backlog not bounded", e)
	}
	release := destinationAcquire(t, c, "B")
	release()
	cancel()
	if !errors.Is(<-done, context.Canceled) {
		t.Fatal("cancellation lost")
	}
	hold()
	if len(c.pending) != 0 || len(c.slots) != 0 || c.destinationAdmissionSnapshot()["active_destination_count"] != 0 {
		t.Fatal("destination state leaked")
	}
}
func TestDestinationFairnessSkipsIneligibleHead(t *testing.T) {
	c, _ := destinationFixture(t, 3, 2, 3)
	a1 := destinationAcquire(t, c, "A")
	a2 := destinationAcquire(t, c, "A")
	b := destinationAcquire(t, c, "B")
	defer a1()
	defer a2()
	defer b()
	aDone, cDone := make(chan func(), 1), make(chan func(), 1)
	errCh := make(chan error, 2)
	go func() {
		r, e := c.requestAdmission(context.Background(), "A")
		if e != nil {
			errCh <- e
		} else {
			aDone <- r
		}
	}()
	await(t, func() bool { return len(c.pending) == 1 })
	go func() {
		r, e := c.requestAdmission(context.Background(), "C")
		if e != nil {
			errCh <- e
		} else {
			cDone <- r
		}
	}()
	await(t, func() bool { return len(c.pending) == 2 })
	b()
	select {
	case release := <-cDone:
		defer release()
	case e := <-errCh:
		t.Fatal(e)
	case <-time.After(time.Second):
		t.Fatal("saturated head blocked another destination")
	}
	select {
	case release := <-aDone:
		release()
		t.Fatal("destination quota exceeded")
	default:
	}
	a1()
	select {
	case release := <-aDone:
		release()
	case e := <-errCh:
		t.Fatal(e)
	case <-time.After(time.Second):
		t.Fatal("old eligible waiter starved")
	}
}
func TestDestinationCancellationAdmissionRaceCleansPermits(t *testing.T) {
	c, _ := destinationFixture(t, 2, 1, 2)
	for i := 0; i < 100; i++ {
		hold := destinationAcquire(t, c, "A")
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			release, e := c.requestAdmission(ctx, "A")
			if e == nil {
				release()
			}
			done <- e
		}()
		await(t, func() bool { return len(c.pending) == 1 })
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cancel() }()
		go func() { defer wg.Done(); hold() }()
		wg.Wait()
		if e := <-done; e != nil && !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
		if len(c.pending) != 0 || len(c.slots) != 0 || c.destinationAdmissionSnapshot()["active_destination_count"] != 0 {
			t.Fatal("cancel/grant race leaked permits")
		}
	}
}
func TestDestinationDrainAndStopDoNotLeak(t *testing.T) {
	for _, drain := range []bool{false, true} {
		t.Run(fmt.Sprint(drain), func(t *testing.T) {
			c, _ := destinationFixture(t, 2, 1, 2)
			hold := destinationAcquire(t, c, "A")
			defer hold()
			done := make(chan error, 1)
			go func() { _, e := c.requestAdmission(context.Background(), "A"); done <- e }()
			await(t, func() bool { return len(c.pending) == 1 })
			if drain {
				c.beginDrain(time.Second)
				hold()
			} else {
				c.close()
			}
			e := <-done
			if e == nil || drain && !errors.Is(e, errDraining) {
				t.Fatal("maintenance admitted queued work", e)
			}
			hold()
			if len(c.pending) != 0 || len(c.slots) != 0 || c.queueTimeouts.Load() != 0 {
				t.Fatal("maintenance leak/false timeout")
			}
		})
	}
}
func TestDestinationKeyNormalization(t *testing.T) {
	for _, tc := range []struct{ method, target, host, want string }{{"GET", "http://Example.COM./x", "", "example.com:80"}, {"GET", "https://example.com/x", "", "example.com:443"}, {"CONNECT", "http://unused", "EXAMPLE.com.:00443", "example.com:443"}, {"GET", "http://[::ffff:127.0.0.1]/x", "", "127.0.0.1:80"}} {
		r, _ := http.NewRequest(tc.method, tc.target, nil)
		if tc.host != "" {
			r.Host = tc.host
		}
		if got := destinationAdmissionKey(r); got != tc.want {
			t.Fatal(got, tc.want)
		}
	}
}
func TestDestinationConfigValidation(t *testing.T) {
	root := t.TempDir()
	t.Setenv("AG_SYSTEM_PROXY_STATE_DIR", root)
	t.Setenv("AG_SYSTEM_PROXY_PORT", "")
	for _, value := range []int{-1, 0, 2, 5} {
		os.WriteFile(filepath.Join(root, "config.json"), []byte(fmt.Sprintf(`{"max_connections":4,"max_connections_per_destination":%d}`, value)), 0600)
		_, e := loadApp()
		if (e == nil) != (value >= 0 && value <= 4) {
			t.Fatal("quota config validation", value, e)
		}
	}
}
func TestMixedLoadSaturationProtected(t *testing.T) {
	c, p := destinationFixture(t, 128, 64, 64)
	c.queueWait = 2 * time.Second
	echo := echoListener(t, func(conn net.Conn) { io.Copy(conn, conn) })
	var held []net.Conn
	defer func() {
		for _, conn := range held {
			conn.Close()
		}
	}()
	for i := 0; i < 64; i++ {
		conn, _ := connectTest(t, p, echo.Addr().String())
		held = append(held, conn)
	}
	var extras []net.Conn
	defer func() {
		for _, conn := range extras {
			conn.Close()
		}
	}()
	for i := 0; i < 64; i++ {
		extra, e := net.Dial("tcp", strings.TrimPrefix(p.URL, "http://"))
		if e != nil {
			t.Fatal(e)
		}
		extra.SetDeadline(time.Now().Add(5 * time.Second))
		fmt.Fprintf(extra, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", echo.Addr(), echo.Addr())
		extras = append(extras, extra)
	}
	await(t, func() bool { return len(c.pending) == 64 })
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "confirmed") }))
	defer origin.Close()
	client := streamingClient(t, p, origin)
	var latencies []float64
	for i := 0; i < 60; i++ {
		start := fairnessNowNS()
		response, e := client.Post(origin.URL, "text/plain", strings.NewReader("command"))
		if e != nil {
			t.Fatal(e)
		}
		b, e := io.ReadAll(response.Body)
		response.Body.Close()
		if e != nil || response.StatusCode != 200 || string(b) != "confirmed" {
			t.Fatal("other destination lost feedback")
		}
		latencies = append(latencies, float64(fairnessNowNS()-start)/1e6)
	}
	for _, extra := range extras {
		response, e := http.ReadResponse(bufio.NewReader(extra), &http.Request{Method: "CONNECT"})
		if e != nil {
			t.Fatal(e)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != 503 {
			t.Fatal("busy destination exceeded limit")
		}
	}
	fairnessRecord(t, "saturation-protected", map[string]any{"per_destination_limit": 64, "held_connect": 64, "same_target_extra_status": 503, "same_target_queued_requests": 64, "offered_connect": 128, "other_target_short_requests": 60, "other_target_short_errors": 0, "other_target_complete_p50_ms": fairnessPercentile(latencies, .5), "other_target_complete_p95_ms": fairnessPercentile(latencies, .95), "queue_timeouts": c.queueTimeouts.Load(), "destination_admission": c.destinationAdmissionSnapshot()})
}
