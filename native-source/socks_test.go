//go:build windows

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Consume exactly one request, leaving any subsequent application bytes untouched.
func readSOCKSRequest(c net.Conn) (byte, string, int, error) {
	var h [4]byte
	if _, e := io.ReadFull(c, h[:]); e != nil {
		return 0, "", 0, e
	}
	if h[0] != 5 || h[1] != 1 || h[2] != 0 {
		return 0, "", 0, fmt.Errorf("bad request header")
	}
	n := 0
	switch h[3] {
	case 1:
		n = 4
	case 4:
		n = 16
	case 3:
		var l [1]byte
		if _, e := io.ReadFull(c, l[:]); e != nil {
			return 0, "", 0, e
		}
		n = int(l[0])
	default:
		return 0, "", 0, fmt.Errorf("bad address type")
	}
	b := make([]byte, n+2)
	if _, e := io.ReadFull(c, b); e != nil {
		return 0, "", 0, e
	}
	host := string(b[:n])
	if h[3] != 3 {
		host = net.IP(b[:n]).String()
	}
	return h[3], host, int(binary.BigEndian.Uint16(b[n:])), nil
}
func readSOCKSGreeting(c net.Conn) error {
	var h [2]byte
	if _, e := io.ReadFull(c, h[:]); e != nil {
		return e
	}
	if h[0] != 5 {
		return fmt.Errorf("bad greeting")
	}
	_, e := io.CopyN(io.Discard, c, int64(h[1]))
	return e
}

func TestSOCKSHTTPRejectsUnadvertisedMethod(t *testing.T) {
	for _, method := range []byte{1, 2, 4, 255} {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			l := echoListener(t, func(c net.Conn) {
				if readSOCKSGreeting(c) != nil {
					return
				}
				c.Write([]byte{5, method})
				if _, _, _, e := readSOCKSRequest(c); e != nil {
					return
				}
				c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
				r, e := http.ReadRequest(bufio.NewReader(c))
				if e != nil {
					return
				}
				r.Body.Close()
				io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nbad")
			})
			_, p, _ := fixture(t, Settings{true, "socks5://" + l.Addr().String(), ""})
			proxyURL, _ := url.Parse(p.URL)
			tr := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
			defer tr.CloseIdleConnections()
			resp, e := (&http.Client{Transport: tr, Timeout: 3 * time.Second}).Get("http://remote.invalid/")
			if e != nil {
				t.Fatal(e)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 502 {
				t.Fatalf("unadvertised method accepted: %d", resp.StatusCode)
			}
		})
	}
}
func TestSOCKSRejectsEmptyBoundDomain(t *testing.T) {
	l := echoListener(t, func(c net.Conn) {
		if readSOCKSGreeting(c) != nil {
			return
		}
		c.Write([]byte{5, 0})
		if _, _, _, e := readSOCKSRequest(c); e != nil {
			return
		}
		c.Write([]byte{5, 0, 0, 3, 0, 0, 0})
	})
	u, _ := url.Parse("socks5://" + l.Addr().String())
	c, e := (&app{}).dialTunnel(context.Background(), "remote.invalid:443", u)
	if c != nil {
		c.Close()
	}
	if e == nil {
		t.Fatal("empty bound domain accepted")
	}
}
func TestSOCKSCredentialsValidatedBeforeRouting(t *testing.T) {
	for _, value := range []string{"socks5://:pw@localhost:1080", "socks5://user@localhost:1080", "socks5://user:@localhost:1080", "socks5://" + strings.Repeat("u", 256) + ":pw@localhost:1080"} {
		if _, e := choose(Settings{true, value, ""}, "remote.invalid", "https", 17891); e == nil {
			t.Errorf("invalid credentials accepted (%d bytes)", len(value))
		}
	}
}
func TestSOCKSAddressReplyAndFirstPayload(t *testing.T) {
	for _, scheme := range []string{"socks5", "socks5h"} {
		for _, tc := range []struct {
			host string
			kind byte
		}{{"not-resolved-locally.invalid", 3}, {"192.0.2.9", 1}, {"2001:db8::9", 4}} {
			t.Run(scheme+"/"+tc.host, func(t *testing.T) {
				observed := make(chan error, 1)
				l := echoListener(t, func(c net.Conn) {
					if e := readSOCKSGreeting(c); e != nil {
						observed <- e
						return
					}
					c.Write([]byte{5, 0})
					kind, host, port, e := readSOCKSRequest(c)
					if e == nil && (kind != tc.kind || host != tc.host || port != 443) {
						e = fmt.Errorf("wrong destination: %d %s %d", kind, host, port)
					}
					observed <- e
					if e != nil {
						return
					}
					// Fragment the reply across writes, then coalesce its final bytes with payload.
					for _, b := range []byte{5, 0, 0, 3, 3, 'p', 'r', 'x', 0} {
						if _, e = c.Write([]byte{b}); e != nil {
							return
						}
					}
					c.Write(append([]byte{0}, []byte("first-payload")...))
				})
				u, _ := url.Parse(scheme + "://" + l.Addr().String())
				c, e := (&app{}).dialTunnel(context.Background(), net.JoinHostPort(tc.host, "443"), u)
				if e != nil {
					t.Fatal(e)
				}
				defer c.Close()
				got, e := io.ReadAll(c)
				if e != nil || !bytes.Equal(got, []byte("first-payload")) {
					t.Fatalf("payload %q: %v", got, e)
				}
				if e := <-observed; e != nil {
					t.Fatal(e)
				}
			})
		}
	}
}

func TestSOCKSHandshakeFailures(t *testing.T) {
	cases := []struct {
		name        string
		method      byte
		auth, reply []byte
		creds       bool
	}{
		{"wrong-method-version", 0, nil, nil, false},
		{"bad-auth-version", 2, []byte{5, 0}, nil, true},
		{"auth-rejected", 2, []byte{1, 1}, nil, true},
		{"bad-reply-version", 0, nil, []byte{4, 0, 0, 1}, false},
		{"reserved-byte", 0, nil, []byte{5, 0, 1, 1}, false},
		{"connect-rejected", 0, nil, []byte{5, 5, 0, 1}, false},
		{"bad-address-type", 0, nil, []byte{5, 0, 0, 9}, false},
		{"truncated-ipv4", 0, nil, []byte{5, 0, 0, 1, 0}, false},
		{"truncated-ipv6", 0, nil, []byte{5, 0, 0, 4, 0}, false},
		{"truncated-domain", 0, nil, []byte{5, 0, 0, 3, 5, 'a'}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := echoListener(t, func(c net.Conn) {
				if readSOCKSGreeting(c) != nil {
					return
				}
				if tc.name == "wrong-method-version" {
					c.Write([]byte{4, 0})
					return
				}
				c.Write([]byte{5, tc.method})
				if tc.creds {
					var h [2]byte
					if _, e := io.ReadFull(c, h[:]); e != nil {
						return
					}
					if _, e := io.CopyN(io.Discard, c, int64(h[1])); e != nil {
						return
					}
					if _, e := io.ReadFull(c, h[:1]); e != nil {
						return
					}
					if _, e := io.CopyN(io.Discard, c, int64(h[0])); e != nil {
						return
					}
					c.Write(tc.auth)
					return
				}
				if _, _, _, e := readSOCKSRequest(c); e != nil {
					return
				}
				c.Write(tc.reply)
			})
			u, _ := url.Parse("socks5://" + l.Addr().String())
			if tc.creds {
				u.User = url.UserPassword("user", "secret")
			}
			c, e := (&app{}).dialTunnel(context.Background(), "remote.invalid:443", u)
			if c != nil {
				c.Close()
			}
			if e == nil {
				t.Fatal("malformed or rejected handshake accepted")
			}
			if strings.Contains(e.Error(), "secret") {
				t.Fatal("credentials leaked")
			}
		})
	}
}
func TestSOCKSTargetValidationBeforeHandshake(t *testing.T) {
	for _, host := range []string{"", strings.Repeat("x", 256), "bad\x00name", "has space", "fe80::1%3"} {
		left, right := net.Pipe()
		left.SetDeadline(time.Now().Add(200 * time.Millisecond))
		u, _ := url.Parse("socks5://user:secret@proxy.invalid:1080")
		if e := socksConnect(left, host, "443", u); e == nil {
			t.Errorf("invalid target accepted")
		}
		left.Close()
		b, e := io.ReadAll(right)
		right.Close()
		if e != nil || len(b) != 0 {
			t.Errorf("handshake sent for invalid target: %d %v", len(b), e)
		}
	}
	for _, port := range []string{"0", "65536", "bad"} {
		if _, e := socksRequest("remote.invalid", port); e == nil {
			t.Errorf("invalid port accepted")
		}
	}
}
func TestSOCKSHandshakeCancellation(t *testing.T) {
	for _, stage := range []string{"method", "auth", "reply"} {
		t.Run(stage, func(t *testing.T) {
			reached := make(chan struct{})
			closed := make(chan struct{})
			l := echoListener(t, func(c net.Conn) {
				defer close(closed)
				if readSOCKSGreeting(c) != nil {
					return
				}
				if stage != "method" {
					if stage == "auth" {
						c.Write([]byte{5, 2})
						var b [11]byte
						io.ReadFull(c, b[:])
					} else {
						c.Write([]byte{5, 0})
						readSOCKSRequest(c)
					}
				}
				close(reached)
				io.Copy(io.Discard, c)
			})
			u, _ := url.Parse("socks5://" + l.Addr().String())
			if stage == "auth" {
				u.User = url.UserPassword("user", "pass")
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				c, e := (&app{}).dialTunnel(ctx, "remote.invalid:443", u)
				if c != nil {
					c.Close()
				}
				done <- e
			}()
			defer cancel()
			select {
			case <-reached:
			case <-time.After(time.Second):
				t.Fatal("handshake not reached")
			}
			cancel()
			select {
			case e := <-done:
				if e == nil {
					t.Fatal("cancelled handshake succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("cancel did not unblock handshake")
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("upstream socket leaked")
			}
		})
	}
}
func TestSOCKSHTTPHandshakeDeadline(t *testing.T) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		c, e := l.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		readSOCKSGreeting(c)
		io.Copy(io.Discard, c)
	}()
	c, p, _ := fixture(t, Settings{true, "socks5://" + l.Addr().String(), ""})
	proxyURL, _ := url.Parse(p.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	defer tr.CloseIdleConnections()
	start := time.Now()
	resp, e := (&http.Client{Transport: tr, Timeout: 24 * time.Second}).Get("http://remote.invalid/")
	if e != nil {
		t.Fatal("handshake did not finish within its own bound:", e)
	}
	resp.Body.Close()
	if resp.StatusCode != 502 || time.Since(start) > 23*time.Second {
		t.Fatalf("handshake bound not honored: %d %v", resp.StatusCode, time.Since(start))
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("timed-out socket leaked")
	}
	if len(c.slots) != 0 {
		t.Fatal("handshake leaked admission slot")
	}
}

// The destination stays unresolved locally; the test proxy maps it to a local origin.
func socksRelay(t *testing.T, target string, auth bool, calls *atomic.Int64) net.Listener {
	return echoListener(t, func(c net.Conn) {
		calls.Add(1)
		if readSOCKSGreeting(c) != nil {
			return
		}
		if auth {
			c.Write([]byte{5, 2})
			var h [2]byte
			if _, e := io.ReadFull(c, h[:]); e != nil {
				return
			}
			user := make([]byte, int(h[1]))
			if _, e := io.ReadFull(c, user); e != nil {
				return
			}
			if _, e := io.ReadFull(c, h[:1]); e != nil {
				return
			}
			pass := make([]byte, int(h[0]))
			if _, e := io.ReadFull(c, pass); e != nil {
				return
			}
			if h[0] == 0 || string(user) != "user@name" || string(pass) != "p:密碼" {
				c.Write([]byte{1, 1})
				return
			}
			c.Write([]byte{1, 0})
		} else {
			c.Write([]byte{5, 0})
		}
		if _, host, _, e := readSOCKSRequest(c); e != nil || host != "remote.invalid" {
			return
		}
		remote, e := net.Dial("tcp", target)
		if e != nil {
			return
		}
		defer remote.Close()
		c.Write([]byte{5, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
		bridge(c, remote)
	})
}
func TestSOCKSHTTPReuseAndRouteSwitch(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Proxy-Authorization") != "" {
			t.Error("proxy credentials reached origin")
		}
		io.WriteString(w, "ok")
	}))
	defer origin.Close()
	var calls atomic.Int64
	l := socksRelay(t, mustURL(t, origin.URL).Host, false, &calls)
	before := Settings{true, "socks5://" + l.Addr().String(), ""}
	c, p, _ := fixture(t, before)
	proxyURL, _ := url.Parse(p.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 3 * time.Second}
	get := func(want int) {
		t.Helper()
		resp, e := client.Get("http://remote.invalid/")
		if e != nil {
			t.Fatal(e)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("status %d, want %d", resp.StatusCode, want)
		}
	}
	for i := 0; i < 3; i++ {
		get(200)
	}
	c.updateSettings(Settings{true, before.Server, "unrelated.invalid"})
	get(200)
	if calls.Load() != 1 {
		t.Fatalf("unnecessary SOCKS redial: %d", calls.Load())
	}
	// Invalid replacement must fail closed rather than reuse the earlier target pool.
	c.updateSettings(Settings{true, "socks5://127.0.0.1:0", ""})
	get(502)
	c.updateSettings(before)
	get(200)
	if calls.Load() != 2 {
		t.Fatalf("stale SOCKS pool reused: %d", calls.Load())
	}
}
func TestSOCKSAuthenticatedHTTP(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	defer origin.Close()
	var calls atomic.Int64
	l := socksRelay(t, mustURL(t, origin.URL).Host, true, &calls)
	u := &url.URL{Scheme: "socks5h", Host: l.Addr().String(), User: url.UserPassword("user@name", "p:密碼")}
	_, p, _ := fixture(t, Settings{true, u.String(), ""})
	proxyURL, _ := url.Parse(p.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	defer tr.CloseIdleConnections()
	resp, e := (&http.Client{Transport: tr, Timeout: 3 * time.Second}).Get("http://remote.invalid/")
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("encoded UTF-8 credentials rejected: %d", resp.StatusCode)
	}
}
func TestSOCKSWebSocketLifecycle(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			origin := websocketOrigin(t, encrypted, func(c net.Conn, rw *bufio.ReadWriter) { io.Copy(c, rw) })
			var calls atomic.Int64
			l := socksRelay(t, mustURL(t, origin.URL).Host, false, &calls)
			before := Settings{true, "socks5h://" + l.Addr().String(), ""}
			c, p, _ := fixture(t, before)
			scheme := "ws"
			if encrypted {
				roots := x509.NewCertPool()
				roots.AddCert(origin.Certificate())
				c.transport.TLSClientConfig = &tls.Config{RootCAs: roots, ServerName: "example.com", MinVersion: tls.VersionTLS12}
				scheme = "wss"
			}
			client, reader := websocketClient(t, p, scheme+"://remote.invalid/agent")
			defer client.Close()
			c.updateSettings(Settings{true, before.Server, "unrelated.invalid"})
			want := maskedFrame(0x89, "still-alive")
			client.Write(want)
			got := make([]byte, len(want))
			if _, e := io.ReadFull(reader, got); e != nil || !bytes.Equal(got, want) {
				t.Fatalf("SOCKS WebSocket interrupted: %v", e)
			}
			c.updateSettings(Settings{})
			client.SetReadDeadline(time.Now().Add(time.Second))
			if _, e := reader.ReadByte(); e == nil {
				t.Fatal("changed SOCKS route kept old WebSocket")
			} else if ne, ok := e.(net.Error); ok && ne.Timeout() {
				t.Fatal("route change did not close WebSocket")
			}
		})
	}
}
