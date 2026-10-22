//go:build windows

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestAgentHTTP1UploadCanReceiveEarlyFeedback(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NewResponseController(w).EnableFullDuplex()
		first := make([]byte, 4)
		if _, e := io.ReadFull(r.Body, first); e != nil {
			return
		}
		io.WriteString(w, "accepted\n")
		w.(http.Flusher).Flush()
		rest, e := io.ReadAll(r.Body)
		if e == nil && string(first) == "tool" && string(rest) == "result" {
			io.WriteString(w, "confirmed\n")
		}
	}))
	defer origin.Close()
	_, p, _ := fixture(t, Settings{})
	u, _ := url.Parse(p.URL)
	raw, e := net.Dial("tcp", u.Host)
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	raw.SetDeadline(time.Now().Add(time.Second))
	fmt.Fprintf(raw, "POST %s HTTP/1.1\r\nHost: %s\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntool\r\n", origin.URL, strings.TrimPrefix(origin.URL, "http://"))
	response, e := http.ReadResponse(bufio.NewReader(raw), &http.Request{Method: "POST"})
	if e != nil {
		t.Fatalf("early feedback blocked waiting for upload to finish: %v", e)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	first, e := reader.ReadString('\n')
	if e != nil || first != "accepted\n" {
		t.Fatalf("early feedback missing: %q %v", first, e)
	}
	// Continue the same upload only after receiving the server's intermediate ack.
	io.WriteString(raw, "6\r\nresult\r\n0\r\n\r\n")
	rest, e := io.ReadAll(reader)
	if e != nil || string(rest) != "confirmed\n" {
		t.Fatalf("final feedback missing: %q %v", rest, e)
	}
}

func TestProxyPreservesCompressionNegotiation(t *testing.T) {
	observed := make(chan string, 2)
	var zipped bytes.Buffer
	gz := gzip.NewWriter(&zipped)
	gz.Write([]byte("stream-event"))
	gz.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding := r.Header.Get("Accept-Encoding")
		observed <- encoding
		if encoding == "gzip" {
			w.Header().Set("Content-Encoding", "gzip")
			w.Write(zipped.Bytes())
			return
		}
		io.WriteString(w, "stream-event")
	}))
	defer origin.Close()
	_, p, _ := fixture(t, Settings{})
	u, _ := url.Parse(p.URL)
	for _, encoding := range []string{"", "gzip"} {
		raw, e := net.Dial("tcp", u.Host)
		if e != nil {
			t.Fatal(e)
		}
		raw.SetDeadline(time.Now().Add(2 * time.Second))
		header := ""
		if encoding != "" {
			header = "Accept-Encoding: " + encoding + "\r\n"
		}
		fmt.Fprintf(raw, "GET %s HTTP/1.1\r\nHost: %s\r\n%sConnection: close\r\n\r\n", origin.URL, strings.TrimPrefix(origin.URL, "http://"), header)
		response, e := http.ReadResponse(bufio.NewReader(raw), &http.Request{Method: "GET"})
		if e != nil {
			raw.Close()
			t.Fatal(e)
		}
		body, e := io.ReadAll(response.Body)
		response.Body.Close()
		raw.Close()
		if e != nil {
			t.Fatal(e)
		}
		if got := <-observed; got != encoding {
			t.Errorf("proxy changed Accept-Encoding: sent %q upstream got %q", encoding, got)
		}
		if encoding == "gzip" {
			if response.Header.Get("Content-Encoding") != "gzip" || !bytes.Equal(body, zipped.Bytes()) {
				t.Fatal("explicit compression was transformed")
			}
		} else if string(body) != "stream-event" {
			t.Fatal("plain response changed")
		}
	}
}

func TestAgentSessionHeadersAndStatusTrailers(t *testing.T) {
	want := map[string]string{"Mcp-Session-Id": "test-session", "Mcp-Protocol-Version": "2025-11-25", "Last-Event-Id": "event-42", "Idempotency-Key": "test-operation", "Traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for key, value := range want {
			if r.Header.Get(key) != value {
				http.Error(w, "missing protocol header", 400)
				return
			}
		}
		w.Header().Set("Mcp-Session-Id", "test-session")
		w.Header().Set("Trailer", "Grpc-Status, Grpc-Message")
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "id: event-43\ndata: confirmed\n\n")
		w.Header().Set("Grpc-Status", "0")
		w.Header().Set("Grpc-Message", "done")
	}))
	defer origin.Close()
	_, p, _ := fixture(t, Settings{})
	client := streamingClient(t, p, origin)
	req, _ := http.NewRequest("POST", origin.URL, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call"}`))
	for key, value := range want {
		req.Header.Set(key, value)
	}
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("TE", "trailers")
	response, e := client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	body, e := io.ReadAll(response.Body)
	if e != nil || response.StatusCode != 200 || response.Header.Get("Mcp-Session-Id") != "test-session" || string(body) != "id: event-43\ndata: confirmed\n\n" || response.Trailer.Get("Grpc-Status") != "0" || response.Trailer.Get("Grpc-Message") != "done" {
		t.Fatal("session or result status changed", e)
	}
}

func TestAgentCancelStopsUpstreamStream(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("TLS=%v", encrypted), func(t *testing.T) {
			canceled := make(chan struct{})
			origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, "started\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(canceled)
			}))
			if encrypted {
				origin.EnableHTTP2 = true
				origin.StartTLS()
			} else {
				origin.Start()
			}
			defer origin.Close()
			_, p, _ := fixture(t, Settings{})
			client := streamingClient(t, p, origin)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, "GET", origin.URL, nil)
			response, e := client.Do(req)
			if e != nil {
				t.Fatal(e)
			}
			defer response.Body.Close()
			if _, e = bufio.NewReader(response.Body).ReadString('\n'); e != nil {
				t.Fatal(e)
			}
			cancel()
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("canceled generation kept upstream running")
			}
		})
	}
}

func TestAgentGRPCBypassEnvironmentIsScoped(t *testing.T) {
	parent := []string{"no_grpc_proxy=googleapis.com", "HTTPS_PROXY=http://obsolete:1", "grpc_proxy=http://obsolete:2", "No_Proxy=*", "ELECTRON_RUN_AS_NODE=1", "EXAMPLE_KEEP=value"}
	copyOfParent := append([]string(nil), parent...)
	got := map[string]string{}
	for _, pair := range proxyEnvironment(parent, 17891) {
		parts := strings.SplitN(pair, "=", 2)
		key := strings.ToUpper(parts[0])
		if _, exists := got[key]; exists {
			t.Fatalf("duplicate proxy variable: %s", key)
		}
		got[key] = parts[1]
	}
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "GRPC_PROXY"} {
		if got[key] != "http://127.0.0.1:17891" {
			t.Fatalf("stale %s", key)
		}
	}
	for _, key := range []string{"NO_PROXY", "NO_GRPC_PROXY"} {
		if got[key] != "localhost,127.0.0.1,::1" {
			t.Fatalf("gRPC can bypass bridge: %s", key)
		}
	}
	if _, exists := got["ELECTRON_RUN_AS_NODE"]; exists {
		t.Fatal("application would run as a Node shell")
	}
	if got["EXAMPLE_KEEP"] != "value" || strings.Join(parent, "\n") != strings.Join(copyOfParent, "\n") {
		t.Fatal("unrelated or parent environment changed")
	}
}
