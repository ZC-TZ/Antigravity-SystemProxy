//go:build windows

package main

import (
	"bufio"
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

func TestEarlyUploadRejectionCompletesWithoutRemainingBody(t *testing.T) {
	for _, mode := range []string{"chunked", "content-length"} {
		t.Run(mode, func(t *testing.T) {
			origin := echoListener(t, func(c net.Conn) {
				r, e := http.ReadRequest(bufio.NewReader(c))
				if e != nil {
					return
				}
				prefix := make([]byte, 4)
				if _, e = io.ReadFull(r.Body, prefix); e != nil {
					return
				}
				io.WriteString(c, "HTTP/1.1 413 Content Too Large\r\nContent-Length: 8\r\nConnection: close\r\n\r\nrejected")
			})
			c, p, _ := fixture(t, Settings{})
			u, _ := url.Parse(p.URL)
			raw, e := net.Dial("tcp", u.Host)
			if e != nil {
				t.Fatal(e)
			}
			defer raw.Close()
			raw.SetDeadline(time.Now().Add(time.Second))
			upload := "Transfer-Encoding: chunked\r\n\r\n4\r\ntool\r\n"
			if mode == "content-length" {
				upload = "Content-Length: 32\r\n\r\ntool"
			}
			fmt.Fprintf(raw, "POST http://%s/upload HTTP/1.1\r\nHost: %s\r\n%s", origin.Addr(), origin.Addr(), upload)
			response, e := http.ReadResponse(bufio.NewReader(raw), &http.Request{Method: "POST"})
			if e != nil {
				t.Fatal(e)
			}
			defer response.Body.Close()
			body, e := io.ReadAll(response.Body)
			if e != nil || response.StatusCode != 413 || string(body) != "rejected" {
				t.Fatalf("early rejection never completed: status=%d body=%q error=%v", response.StatusCode, body, e)
			}
			await(t, func() bool { return len(c.slots) == 0 })
		})
	}
}

func TestBusyUploadReturns503BeforeUploadFinishes(t *testing.T) {
	c, p := admissionFixture(t, .05)
	if e := c.acquire(t.Context()); e != nil {
		t.Fatal(e)
	}
	defer func() { <-c.slots }()
	u, _ := url.Parse(p.URL)
	raw, e := net.Dial("tcp", u.Host)
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	raw.SetDeadline(time.Now().Add(time.Second))
	io.WriteString(raw, "POST http://example.test/tool HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntool\r\n")
	response, e := http.ReadResponse(bufio.NewReader(raw), &http.Request{Method: "POST"})
	if e != nil {
		t.Fatalf("busy response blocked draining unfinished upload: %v", e)
	}
	defer response.Body.Close()
	_, e = io.ReadAll(response.Body)
	if e != nil || response.StatusCode != 503 || response.Header.Get("Retry-After") != "1" {
		t.Fatal("incomplete busy response", e)
	}
	await(t, func() bool { return len(c.pending) == 0 })
}

func TestCompletedChunkedUploadRetainsKeepAlive(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); io.WriteString(w, "ok") }))
	defer origin.Close()
	c, p, _ := fixture(t, Settings{})
	u, _ := url.Parse(p.URL)
	raw, e := net.Dial("tcp", u.Host)
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	raw.SetDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(raw)
	for i := 0; i < 10; i++ {
		upload := "Transfer-Encoding: chunked\r\n\r\n4\r\ntool\r\n0\r\n\r\n"
		if i%2 == 1 {
			upload = "Content-Length: 4\r\n\r\ntool"
		}
		fmt.Fprintf(raw, "POST %s HTTP/1.1\r\nHost: %s\r\n%s", origin.URL, strings.TrimPrefix(origin.URL, "http://"), upload)
		response, e := http.ReadResponse(reader, &http.Request{Method: "POST"})
		if e != nil {
			t.Fatalf("keepalive failed at request %d: %v", i, e)
		}
		body, e := io.ReadAll(response.Body)
		response.Body.Close()
		if e != nil || string(body) != "ok" {
			t.Fatal("completed upload corrupted", e)
		}
	}
	if c.dials.Load() != 1 {
		t.Fatalf("upstream connection reuse lost: %d dials", c.dials.Load())
	}
}

func TestAbsoluteHealthPathIsForwardedToOrigin(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "origin-health") }))
	defer origin.Close()
	_, p, _ := fixture(t, Settings{})
	client := streamingClient(t, p, origin)
	response, e := client.Get(origin.URL + healthPath)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	body, e := io.ReadAll(response.Body)
	if e != nil || string(body) != "origin-health" {
		t.Fatalf("proxy intercepted origin's health URL: %q %v", body, e)
	}
	response, e = http.Get(p.URL + healthPath)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	body, e = io.ReadAll(response.Body)
	if e != nil || response.StatusCode != http.StatusBadRequest || string(body) == marker {
		t.Fatalf("data listener exposed retired health endpoint: status=%d body=%q err=%v", response.StatusCode, body, e)
	}
}
