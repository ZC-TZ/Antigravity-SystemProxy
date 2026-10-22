//go:build windows

package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
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

func websocketOrigin(t *testing.T, encrypted bool, fn func(net.Conn, *bufio.ReadWriter)) *httptest.Server {
	t.Helper()
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Sec-WebSocket-Key")
		if key == "" || r.Header.Get("Sec-WebSocket-Version") != "13" {
			http.Error(w, "invalid handshake", 400)
			return
		}
		c, rw, e := w.(http.Hijacker).Hijack()
		if e != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(6 * time.Second))
		digest := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\nSec-WebSocket-Protocol: agent-test\r\n\r\n", base64.StdEncoding.EncodeToString(digest[:]))
		if rw.Flush() == nil {
			fn(c, rw)
		}
	}))
	if encrypted {
		s.EnableHTTP2 = true
		s.StartTLS()
	} else {
		s.Start()
	}
	t.Cleanup(s.Close)
	return s
}
func websocketClient(t *testing.T, p *httptest.Server, target string) (*net.TCPConn, *bufio.Reader) {
	t.Helper()
	u, _ := url.Parse(p.URL)
	conn, e := net.DialTCP("tcp", nil, mustTCPAddress(t, u.Host))
	if e != nil {
		t.Fatal(e)
	}
	conn.SetReadBuffer(1024)
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: agent-test\r\n\r\n", target, mustURL(t, target).Host)
	reader := bufio.NewReader(conn)
	response, e := http.ReadResponse(reader, &http.Request{Method: "GET"})
	if e != nil || response.StatusCode != 101 {
		conn.Close()
		t.Fatalf("WebSocket upgrade failed: %v %v", response, e)
	}
	if response.Header.Get("Sec-WebSocket-Accept") != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" || response.Header.Get("Sec-WebSocket-Protocol") != "agent-test" {
		conn.Close()
		t.Fatal("handshake fields changed")
	}
	return conn, reader
}
func mustTCPAddress(t *testing.T, address string) *net.TCPAddr {
	t.Helper()
	a, e := net.ResolveTCPAddr("tcp", address)
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func mustURL(t *testing.T, value string) *url.URL {
	t.Helper()
	u, e := url.Parse(value)
	if e != nil {
		t.Fatal(e)
	}
	return u
}
func maskedFrame(opcode byte, payload string) []byte {
	b := []byte{opcode, 0x80 | byte(len(payload)), 1, 2, 3, 4}
	for i, v := range []byte(payload) {
		b = append(b, v^byte(i%4+1))
	}
	return b
}

func TestWebSocketHalfCloseRetainsFinalCloseFrame(t *testing.T) {
	origin := websocketOrigin(t, false, func(c net.Conn, rw *bufio.ReadWriter) { io.Copy(io.Discard, rw); c.Write([]byte{0x88, 2, 3, 0xe8}) })
	_, p, _ := fixture(t, Settings{})
	c, r := websocketClient(t, p, origin.URL)
	defer c.Close()
	c.Write(maskedFrame(0x88, string([]byte{3, 0xe8})))
	c.CloseWrite()
	body, e := io.ReadAll(r)
	if e != nil || !bytes.Equal(body, []byte{0x88, 2, 3, 0xe8}) {
		t.Fatalf("final Close frame lost after TCP half-close: %x %v", body, e)
	}
}
func TestWebSocketBackpressureReleasesCapacity(t *testing.T) {
	origin := websocketOrigin(t, false, func(c net.Conn, rw *bufio.ReadWriter) {
		go io.Copy(io.Discard, rw)
		block := bytes.Repeat([]byte{0x82, 0}, 16*1024)
		for i := 0; i < 2048; i++ {
			if _, e := c.Write(block); e != nil {
				return
			}
		}
	})
	core, p, _ := fixture(t, Settings{})
	core.a.cfg.Idle = .1
	c, _ := websocketClient(t, p, origin.URL)
	defer c.Close()
	go func() {
		for {
			if _, e := c.Write(maskedFrame(0x89, "")); e != nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	// Do not consume server frames. Upload traffic must not let a blocked client
	// writer retain a request slot forever.
	await(t, func() bool { return len(core.slots) == 0 })
}
func TestWebSocketFragmentPingPongAndCloseBytes(t *testing.T) {
	upload := append(maskedFrame(0x01, "hel"), maskedFrame(0x80, "lo")...)
	upload = append(upload, maskedFrame(0x89, "ping")...)
	upload = append(upload, maskedFrame(0x88, string([]byte{3, 0xe8}))...)
	download := []byte{0x01, 3, 'h', 'e', 'l', 0x80, 2, 'l', 'o', 0x8a, 4, 'p', 'i', 'n', 'g', 0x88, 2, 3, 0xe8}
	observed := make(chan bool, 1)
	origin := websocketOrigin(t, false, func(c net.Conn, rw *bufio.ReadWriter) {
		got := make([]byte, len(upload))
		_, e := io.ReadFull(rw, got)
		observed <- e == nil && bytes.Equal(got, upload)
		c.Write(download)
	})
	_, p, _ := fixture(t, Settings{})
	c, r := websocketClient(t, p, origin.URL)
	defer c.Close()
	c.Write(upload)
	got := make([]byte, len(download))
	_, e := io.ReadFull(r, got)
	if e != nil || !bytes.Equal(got, download) || !<-observed {
		t.Fatal("WebSocket frames changed", e)
	}
}
func TestWebSocketOneWayActivitySurvivesReverseIdle(t *testing.T) {
	origin := websocketOrigin(t, false, func(c net.Conn, rw *bufio.ReadWriter) {
		for i := 0; i < 16; i++ {
			if _, e := c.Write([]byte{0x82, 1, byte(i)}); e != nil {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
	})
	core, p, _ := fixture(t, Settings{})
	core.a.cfg.Idle = .1
	c, r := websocketClient(t, p, origin.URL)
	defer c.Close()
	for i := 0; i < 16; i++ {
		got := make([]byte, 3)
		if _, e := io.ReadFull(r, got); e != nil || !bytes.Equal(got, []byte{0x82, 1, byte(i)}) {
			t.Fatalf("one-way stream stopped at frame %d: %v", i, e)
		}
	}
}
func TestWebSocketRouteSwitchReleasesUpgrade(t *testing.T) {
	origin := websocketOrigin(t, false, func(c net.Conn, rw *bufio.ReadWriter) { io.Copy(c, rw) })
	core, p, _ := fixture(t, Settings{true, origin.URL, ""})
	c, r := websocketClient(t, p, "http://example.test/socket")
	defer c.Close()
	core.updateSettings(Settings{})
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, e := r.ReadByte(); e == nil {
		t.Fatal("changed proxy route survived")
	}
	await(t, func() bool { return len(core.slots) == 0 })
}
func TestWebSocketUnchangedLocalRouteSurvivesToggle(t *testing.T) {
	origin := websocketOrigin(t, false, func(c net.Conn, rw *bufio.ReadWriter) { io.Copy(c, rw) })
	core, p, _ := fixture(t, Settings{})
	c, r := websocketClient(t, p, "ws"+strings.TrimPrefix(origin.URL, "http"))
	defer c.Close()
	core.updateSettings(Settings{true, "127.0.0.1:9", ""})
	frame := maskedFrame(0x89, "alive")
	c.Write(frame)
	got := make([]byte, len(frame))
	_, e := io.ReadFull(r, got)
	if e != nil || !bytes.Equal(got, frame) {
		t.Fatal("unchanged local WebSocket interrupted", e)
	}
}
func TestAbsoluteWSSUpgradeWithVerifiedTLS(t *testing.T) {
	origin := websocketOrigin(t, true, func(c net.Conn, rw *bufio.ReadWriter) { c.Write([]byte{0x82, 2, 'o', 'k'}) })
	core, p, _ := fixture(t, Settings{})
	roots := x509.NewCertPool()
	roots.AddCert(origin.Certificate())
	core.transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	c, r := websocketClient(t, p, "wss"+strings.TrimPrefix(origin.URL, "https"))
	defer c.Close()
	got := make([]byte, 4)
	_, e := io.ReadFull(r, got)
	if e != nil || !bytes.Equal(got, []byte{0x82, 2, 'o', 'k'}) {
		t.Fatal("WSS bytes changed", e)
	}
}

func TestWSSRejectsUntrustedCertificate(t *testing.T) {
	origin := websocketOrigin(t, true, func(c net.Conn, rw *bufio.ReadWriter) {})
	core, p, _ := fixture(t, Settings{})
	u := mustURL(t, p.URL)
	c, e := net.Dial("tcp", u.Host)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprintf(c, "GET wss%s HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", strings.TrimPrefix(origin.URL, "https"), mustURL(t, origin.URL).Host)
	response, e := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: "GET"})
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)
	if response.StatusCode != 502 {
		t.Fatalf("untrusted certificate accepted: %d", response.StatusCode)
	}
	await(t, func() bool { return len(core.slots) == 0 })
}
func TestWSSOverCONNECTPreservesFrames(t *testing.T) {
	want := []byte{0x89, 2, 'h', 'i', 0x88, 2, 3, 0xe8}
	origin := websocketOrigin(t, true, func(c net.Conn, rw *bufio.ReadWriter) { c.Write(want) })
	_, p, _ := fixture(t, Settings{})
	target := mustURL(t, origin.URL)
	raw, _ := connectTest(t, p, target.Host)
	defer raw.Close()
	roots := x509.NewCertPool()
	roots.AddCert(origin.Certificate())
	c := tls.Client(raw, &tls.Config{RootCAs: roots, ServerName: target.Hostname(), MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}})
	if e := c.Handshake(); e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	fmt.Fprintf(c, "GET /socket HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: agent-test\r\n\r\n", target.Host)
	reader := bufio.NewReader(c)
	response, e := http.ReadResponse(reader, &http.Request{Method: "GET"})
	if e != nil || response.StatusCode != 101 {
		t.Fatal("WSS CONNECT upgrade failed", e)
	}
	got := make([]byte, len(want))
	_, e = io.ReadFull(reader, got)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatal("encrypted WebSocket frames changed", e)
	}
}
func TestWebSocketShutdownClosesBothPeers(t *testing.T) {
	origin := websocketOrigin(t, false, func(c net.Conn, rw *bufio.ReadWriter) { io.Copy(c, rw) })
	core, p, _ := fixture(t, Settings{})
	c, r := websocketClient(t, p, origin.URL)
	defer c.Close()
	core.close()
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, e := r.ReadByte(); e == nil {
		t.Fatal("upgraded client was left open during shutdown")
	}
	await(t, func() bool { return len(core.slots) == 0 })
	core.mu.Lock()
	upgrades := len(core.upgrades)
	core.mu.Unlock()
	if upgrades != 0 {
		t.Fatal("upgraded connection registry leaked")
	}
}

func TestWebSocketWriteTimeoutDoesNotShortenSilentReads(t *testing.T) {
	origin := websocketOrigin(t, false, func(c net.Conn, rw *bufio.ReadWriter) {
		time.Sleep(200 * time.Millisecond)
		c.Write([]byte{0x82, 2, 'o', 'k'})
	})
	core, p, _ := fixture(t, Settings{})
	core.a.cfg.Idle = 1
	core.a.cfg.WSWrite = .05
	c, r := websocketClient(t, p, origin.URL)
	defer c.Close()
	got := make([]byte, 4)
	_, e := io.ReadFull(r, got)
	if e != nil || !bytes.Equal(got, []byte{0x82, 2, 'o', 'k'}) {
		t.Fatal("write budget incorrectly shortened silent read tolerance", e)
	}
}
