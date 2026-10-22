//go:build windows

package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func streamingClient(t *testing.T, proxy, origin *httptest.Server) *http.Client {
	t.Helper()
	u, _ := url.Parse(proxy.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(u), ForceAttemptHTTP2: true}
	if origin.TLS != nil {
		roots := x509.NewCertPool()
		roots.AddCert(origin.Certificate())
		tr.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 8 * time.Second}
}

func TestStreamingFirstEventAndCompleteTail(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("TLS=%v", encrypted), func(t *testing.T) {
			gate := make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(gate) }) }
			defer release()
			first := []byte("data: first\n\n")
			var tail bytes.Buffer
			for i := 0; i < 2000; i++ {
				fmt.Fprintf(&tail, "data: %d 中文 delta\n\n", i)
			}
			tail.WriteString("data: [DONE]\n\n")
			origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Write(first)
				w.(http.Flusher).Flush()
				select {
				case <-gate:
				case <-r.Context().Done():
					return
				}
				w.Write(tail.Bytes())
			}))
			if encrypted {
				origin.EnableHTTP2 = true
				origin.StartTLS()
			} else {
				origin.Start()
			}
			defer origin.Close()
			_, proxy, _ := fixture(t, Settings{})
			client := streamingClient(t, proxy, origin)
			started := time.Now()
			response, e := client.Get(origin.URL)
			if e != nil {
				t.Fatal(e)
			}
			defer response.Body.Close()
			arrived := make(chan error, 1)
			got := make([]byte, len(first))
			go func() { _, e := io.ReadFull(response.Body, got); arrived <- e }()
			select {
			case e = <-arrived:
				if e != nil || !bytes.Equal(got, first) {
					t.Fatal("first event missing", e)
				}
			case <-time.After(time.Second):
				release()
				t.Fatal("first event held until response completion")
			}
			t.Logf("first event arrived in %s, before origin completion", time.Since(started))
			release()
			gotTail, e := io.ReadAll(response.Body)
			if e != nil || !bytes.Equal(gotTail, tail.Bytes()) {
				t.Fatalf("stream tail changed: bytes=%d error=%v", len(gotTail), e)
			}
		})
	}
}

func TestHTTP2FeedbackWhileStreamIsOpen(t *testing.T) {
	gate := make(chan struct{})
	defer close(gate)
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stream" {
			io.WriteString(w, "running\n")
			w.(http.Flusher).Flush()
			select {
			case <-gate:
			case <-r.Context().Done():
			}
			return
		}
		io.WriteString(w, "command-confirmed")
	}))
	origin.EnableHTTP2 = true
	origin.StartTLS()
	defer origin.Close()
	core, proxy, _ := fixture(t, Settings{})
	client := streamingClient(t, proxy, origin)
	response, e := client.Get(origin.URL + "/stream")
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 2 {
		t.Fatal("fixture did not negotiate HTTP/2")
	}
	if _, e = bufio.NewReader(response.Body).ReadString('\n'); e != nil {
		t.Fatal(e)
	}
	started := time.Now()
	for i := 0; i < 30; i++ {
		ack, e := client.Post(origin.URL+"/feedback", "application/json", strings.NewReader(`{"result":"done"}`))
		if e != nil {
			t.Fatal(e)
		}
		body, e := io.ReadAll(ack.Body)
		ack.Body.Close()
		if e != nil || string(body) != "command-confirmed" {
			t.Fatal("feedback lost", e)
		}
	}
	core.mu.Lock()
	tunnels := len(core.tunnels)
	core.mu.Unlock()
	if tunnels != 1 {
		t.Fatalf("HTTP/2 did not share one CONNECT tunnel: %d", tunnels)
	}
	t.Logf("30 confirmations while first stream stayed open: %s", time.Since(started))
}

func TestCommandPOSTNotReplayedAfterDisconnect(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("TLS=%v", encrypted), func(t *testing.T) {
			var executions atomic.Int64
			origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					io.WriteString(w, "ready")
					return
				}
				io.Copy(io.Discard, r.Body)
				executions.Add(1)
				c, _, e := w.(http.Hijacker).Hijack()
				if e == nil {
					c.Close()
				}
			}))
			if encrypted {
				origin.StartTLS()
			} else {
				origin.Start()
			}
			defer origin.Close()
			_, proxy, _ := fixture(t, Settings{})
			client := streamingClient(t, proxy, origin)
			warm, e := client.Get(origin.URL)
			if e != nil {
				t.Fatal(e)
			}
			io.Copy(io.Discard, warm.Body)
			warm.Body.Close()
			response, e := client.Post(origin.URL, "application/json", strings.NewReader(`{"command":"run-once"}`))
			if response != nil {
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
			}
			if e == nil && response.StatusCode < 400 {
				t.Fatal("disconnect reported as success")
			}
			if executions.Load() != 1 {
				t.Fatalf("command executed %d times", executions.Load())
			}
		})
	}
}
