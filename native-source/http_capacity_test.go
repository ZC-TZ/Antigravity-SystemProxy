//go:build windows

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestHTTPTransportCapacityTracksAdmission(t *testing.T) {
	for _, limit := range []int{1, 8, 128, 2048} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			root := t.TempDir()
			settings := filepath.Join(root, "settings.json")
			os.WriteFile(settings, []byte(`{"enabled":false}`), 0600)
			a := &app{root: root, settingsFile: settings, cfg: Config{Port: 17891, Idle: 3600, MaxConnections: limit, MaxPending: 64, QueueWait: 2}}
			c, e := newCore(a)
			if e != nil {
				t.Fatal(e)
			}
			defer c.close()
			if c.transport.MaxConnsPerHost != cap(c.slots) || c.transport.MaxIdleConns != min(limit, 128) || c.transport.MaxIdleConnsPerHost != min(limit, 128) {
				t.Fatal("pool and admission capacity diverged")
			}
			state := c.statusSnapshot(os.Getpid())
			if state.HTTPTransport["write_buffer_bytes"] != 65536 || state.HTTPTransport["response_flush"] != "immediate" {
				t.Fatal(state.HTTPTransport)
			}
			if _, e = json.Marshal(state); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestHTTP65thSharedProxyRequestDoesNotWaitBehind64Bodies(t *testing.T) {
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	arrived := make(chan struct{}, 65)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(200)
		http.NewResponseController(w).Flush()
		arrived <- struct{}{}
		select {
		case <-gate:
			io.WriteString(w, "x")
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	defer release()
	c, proxy, _ := fixture(t, Settings{Enabled: true, Server: upstream.URL})
	u, _ := url.Parse(proxy.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(u), DisableKeepAlives: true, MaxConnsPerHost: 128}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 8 * time.Second}
	results := make(chan error, 65)
	for i := 0; i < 65; i++ {
		go func(i int) {
			resp, e := client.Get(fmt.Sprintf("http://target-%d.invalid/held", i))
			if e == nil {
				data, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				e = err
				if e == nil && string(data) != "x" {
					e = fmt.Errorf("confirmation changed: %q", data)
				}
			}
			results <- e
		}(i)
	}
	for i := 0; i < 65; i++ {
		select {
		case <-arrived:
		case <-time.After(3 * time.Second):
			t.Fatal("65th request remained hidden in the Transport pool")
		}
	}
	if len(c.pending) != 0 || len(c.slots) != 65 {
		t.Fatalf("admission active=%d pending=%d", len(c.slots), len(c.pending))
	}
	awaitAcquire(t, func() bool { return c.metrics.httpAcquire.snapshot()["in_flight"] == 0 })
	release()
	for i := 0; i < 65; i++ {
		if e := <-results; e != nil {
			t.Fatal(e)
		}
	}
}

func TestKnownLengthSSEIsFlushedBeforeCompletion(t *testing.T) {
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Content-Length", "14")
		io.WriteString(w, "data: first\n\n")
		http.NewResponseController(w).Flush()
		select {
		case <-gate:
			io.WriteString(w, "!")
		case <-r.Context().Done():
		}
	}))
	defer origin.Close()
	defer release()
	_, proxy, _ := fixture(t, Settings{})
	client := streamingClient(t, proxy, origin)
	response, e := client.Get(origin.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	first := make([]byte, 13)
	arrived := make(chan error, 1)
	go func() { _, e := io.ReadFull(response.Body, first); arrived <- e }()
	select {
	case e = <-arrived:
		if e != nil || string(first) != "data: first\n\n" {
			t.Fatal(string(first), e)
		}
	case <-time.After(time.Second):
		t.Fatal("known-length event held until completion")
	}
	release()
	tail, e := io.ReadAll(response.Body)
	if e != nil || string(tail) != "!" {
		t.Fatal(string(tail), e)
	}
}

func TestSmallUploadPrefixBeforeProducerCompletes(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunked=%v", chunked), func(t *testing.T) {
			prefix := make(chan struct{})
			const size = 16 * 1024
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if n, e := io.CopyN(io.Discard, r.Body, size); e == nil && n == size {
					close(prefix)
				}
				io.Copy(io.Discard, r.Body)
			}))
			defer origin.Close()
			_, proxy, _ := fixture(t, Settings{})
			c, e := net.DialTimeout("tcp", proxy.Listener.Addr().String(), time.Second)
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			if chunked {
				fmt.Fprintf(c, "POST %s/upload HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n", origin.URL, size)
			} else {
				fmt.Fprintf(c, "POST %s/upload HTTP/1.1\r\nHost: example\r\nContent-Length: %d\r\n\r\n", origin.URL, size*2)
			}
			if _, e = c.Write(make([]byte, size)); e != nil {
				t.Fatal(e)
			}
			if chunked {
				io.WriteString(c, "\r\n")
			}
			select {
			case <-prefix:
			case <-time.After(time.Second):
				t.Fatal("upload prefix buffered while producer waits for feedback")
			}
		})
	}
}

func TestHTTPBurstReusesRetainedUpstreamConnections(t *testing.T) {
	for _, oldIdleLimit := range []bool{true, false} {
		t.Run(fmt.Sprintf("old_idle_limit=%v", oldIdleLimit), func(t *testing.T) {
			var gates [2]chan struct{}
			var arrivals [2]chan struct{}
			var releases [2]sync.Once
			for i := range gates {
				gates[i] = make(chan struct{})
				arrivals[i] = make(chan struct{}, 64)
			}
			release := func(i int) { releases[i].Do(func() { close(gates[i]) }) }
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := 0
				if r.URL.Path == "/second" {
					i = 1
				}
				w.Header().Set("Content-Length", "1")
				http.NewResponseController(w).Flush()
				arrivals[i] <- struct{}{}
				select {
				case <-gates[i]:
					io.WriteString(w, "x")
				case <-r.Context().Done():
				}
			}))
			defer origin.Close()
			defer func() { release(0); release(1) }()
			c, proxy, _ := fixture(t, Settings{})
			if oldIdleLimit {
				c.transport.MaxIdleConnsPerHost = 16
			}
			u, _ := url.Parse(proxy.URL)
			tr := &http.Transport{Proxy: http.ProxyURL(u), DisableKeepAlives: true, MaxConnsPerHost: 128}
			defer tr.CloseIdleConnections()
			client := &http.Client{Transport: tr, Timeout: 8 * time.Second}
			for round := 0; round < 2; round++ {
				errors := make(chan error, 64)
				path := "/first"
				if round == 1 {
					path = "/second"
				}
				for i := 0; i < 64; i++ {
					go func() {
						resp, e := client.Get(origin.URL + path)
						if e == nil {
							data, err := io.ReadAll(resp.Body)
							resp.Body.Close()
							e = err
							if e == nil && string(data) != "x" {
								e = fmt.Errorf("wrong payload %q", data)
							}
						}
						errors <- e
					}()
				}
				for i := 0; i < 64; i++ {
					select {
					case <-arrivals[round]:
					case <-time.After(3 * time.Second):
						t.Fatal("burst did not reach origin")
					}
				}
				release(round)
				for i := 0; i < 64; i++ {
					if e := <-errors; e != nil {
						t.Fatal(e)
					}
				}
				if round == 0 {
					want := 64
					if oldIdleLimit {
						want = 16
					}
					awaitAcquire(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.httpConns) == want })
				}
			}
			want := int64(64)
			if oldIdleLimit {
				want = 112
			}
			if c.dials.Load() != want {
				t.Fatalf("dials=%d want %d", c.dials.Load(), want)
			}
			t.Logf("two 64-request bursts: upstream dials=%d reused=%v", c.dials.Load(), c.metrics.httpAcquire.snapshot()["reused"])
		})
	}
}
