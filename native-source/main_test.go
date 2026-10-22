//go:build windows

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type dataTimeoutConn struct {
	net.Conn
	calls int
}
type timeoutError struct{}

func (timeoutError) Error() string                         { return "timeout" }
func (timeoutError) Timeout() bool                         { return true }
func (timeoutError) Temporary() bool                       { return true }
func (c *dataTimeoutConn) SetReadDeadline(time.Time) error { return nil }
func (c *dataTimeoutConn) Read(p []byte) (int, error) {
	c.calls++
	if c.calls == 1 {
		return copy(p, "last bytes"), timeoutError{}
	}
	return 0, io.EOF
}
func TestReadPreservesBytesAlongsideTimeout(t *testing.T) {
	raw := &dataTimeoutConn{}
	activity := &atomic.Int64{}
	activity.Store(time.Now().UnixNano())
	c := &idleConn{Conn: raw, activity: activity, timeout: time.Second}
	b := make([]byte, 100)
	n, _ := c.Read(b)
	if string(b[:n]) != "last bytes" {
		t.Fatalf("lost final bytes: %q", b[:n])
	}
}
func TestStatusRedactsCredentialsWithoutScheme(t *testing.T) {
	for _, value := range []string{"user:password@localhost:7890", "http://user:password@localhost:7890", "http=user:password@localhost:7890;https=https://user:password@localhost:7891"} {
		got := redactProxyServer(value)
		if strings.Contains(got, "user") || strings.Contains(got, "password") {
			t.Fatalf("credentials exposed: %s", got)
		}
		if !strings.Contains(got, "localhost:") {
			t.Fatal("proxy endpoint lost")
		}
	}
}
func TestRelativeCAUsesConfigDirectory(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "config.json"), []byte(`{"proxy_ca_file":"certs/root.pem"}`), 0600)
	t.Setenv("AG_SYSTEM_PROXY_STATE_DIR", root)
	t.Setenv("AG_SYSTEM_PROXY_PORT", "")
	a, e := loadApp()
	if e != nil {
		t.Fatal(e)
	}
	if a.cfg.ProxyCA != filepath.Join(root, "certs/root.pem") {
		t.Fatalf("CA depends on working directory: %s", a.cfg.ProxyCA)
	}
}
func TestProxyHandshakeCancellation(t *testing.T) {
	accepted := make(chan struct{})
	l := echoListener(t, func(c net.Conn) { close(accepted); io.Copy(io.Discard, c) })
	u, _ := url.Parse("http://" + l.Addr().String())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		c, e := (&app{}).dialTunnel(ctx, "example.test:443", u)
		if c != nil {
			c.Close()
		}
		done <- e
	}()
	<-accepted
	cancel()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("canceled handshake succeeded")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("canceled handshake kept waiting for upstream")
	}
}
func TestSelfForwardingIsRejected(t *testing.T) {
	c, p, _ := fixture(t, Settings{})
	u, _ := url.Parse(p.URL)
	c.a.cfg.Port, _ = strconv.Atoi(u.Port())
	for _, method := range []string{"GET", "CONNECT"} {
		req, _ := http.NewRequest(method, p.URL, nil)
		if method == "GET" {
			req.URL.Path = "/recursive"
		} else {
			req.URL = &url.URL{Host: u.Host}
			req.Host = u.Host
		}
		// Send absolute proxy-form manually, so the request really targets the bridge.
		raw, e := net.Dial("tcp", u.Host)
		if e != nil {
			t.Fatal(e)
		}
		raw.SetDeadline(time.Now().Add(time.Second))
		if method == "GET" {
			fmt.Fprintf(raw, "GET %s/recursive HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", p.URL, u.Host)
		} else {
			fmt.Fprintf(raw, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", u.Host, u.Host)
		}
		res, e := http.ReadResponse(bufio.NewReader(raw), req)
		raw.Close()
		if e != nil || res.StatusCode != 400 {
			t.Fatalf("%s self forwarding not rejected: %v %v", method, res, e)
		}
	}
}

func fixture(t *testing.T, s Settings) (*core, *httptest.Server, string) {
	t.Helper()
	root := t.TempDir()
	file := filepath.Join(root, "settings.json")
	b, _ := json.Marshal(s)
	os.WriteFile(file, b, 0600)
	a := &app{root: root, settingsFile: file, cfg: Config{Port: 17891, Idle: 5}}
	c, e := newCore(a)
	if e != nil {
		t.Fatal(e)
	}
	p := httptest.NewServer(c)
	t.Cleanup(func() { c.close(); p.Close() })
	return c, p, file
}
func connectTest(t *testing.T, p *httptest.Server, target string) (*net.TCPConn, *bufio.Reader) {
	t.Helper()
	u, _ := url.Parse(p.URL)
	raw, e := net.Dial("tcp", u.Host)
	if e != nil {
		t.Fatal(e)
	}
	c := raw.(*net.TCPConn)
	c.SetDeadline(time.Now().Add(8 * time.Second))
	fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	r := bufio.NewReader(c)
	response, e := http.ReadResponse(r, &http.Request{Method: "CONNECT"})
	if e != nil {
		c.Close()
		t.Fatal(e)
	}
	if response.StatusCode != 200 {
		body, _ := io.ReadAll(response.Body)
		c.Close()
		t.Fatalf("CONNECT %d: %s", response.StatusCode, body)
	}
	return c, r
}
func echoListener(t *testing.T, fn func(net.Conn)) net.Listener {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); c.SetDeadline(time.Now().Add(8 * time.Second)); fn(c) }()
		}
	}()
	t.Cleanup(func() { l.Close() })
	return l
}
func TestRouting(t *testing.T) {
	for _, tc := range []struct {
		s                  Settings
		host, scheme, want string
	}{{Settings{}, "example.test", "https", "DIRECT"}, {Settings{true, "127.0.0.1:7890", ""}, "example.test", "https", "http://127.0.0.1:7890"}, {Settings{true, "http=localhost:80;https=localhost:81", ""}, "example.test", "https", "http://localhost:81"}, {Settings{true, "socks5h://localhost:1080", ""}, "example.test", "https", "socks5h://localhost:1080"}, {Settings{true, "127.0.0.1:7890", "*.test"}, "example.test", "https", "DIRECT"}, {Settings{true, "127.0.0.1:7890", ""}, "::1", "https", "DIRECT"}} {
		u, e := choose(tc.s, tc.host, tc.scheme, 17891)
		if e != nil || routeKey(u) != tc.want {
			t.Fatalf("route got %v %v", u, e)
		}
	}
	if _, e := choose(Settings{true, "socks=localhost:1080", ""}, "example.test", "https", 17891); e == nil {
		t.Fatal("ambiguous SOCKS accepted")
	}
	if _, e := choose(Settings{true, "http://localhost:7890/ignored-path", ""}, "example.test", "https", 17891); e == nil {
		t.Fatal("invalid proxy path silently ignored")
	}
}
func TestClosedCoreRejectsHTTPDial(t *testing.T) {
	c, _, _ := fixture(t, Settings{})
	l := echoListener(t, func(conn net.Conn) { io.Copy(io.Discard, conn) })
	c.close()
	conn, e := c.transport.DialContext(context.Background(), "tcp", l.Addr().String())
	if conn != nil {
		conn.Close()
	}
	if e == nil {
		t.Fatal("dial registered after core shutdown")
	}
}
func TestHTTPPersistentConnectionReuse(t *testing.T) {
	var accepted atomic.Int64
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); io.WriteString(w, "ok") }))
	origin.Config.ConnState = func(c net.Conn, s http.ConnState) {
		if s == http.StateNew {
			accepted.Add(1)
		}
	}
	origin.Start()
	defer origin.Close()
	core, proxy, _ := fixture(t, Settings{})
	u, _ := url.Parse(proxy.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(u)}
	defer tr.CloseIdleConnections()
	client := http.Client{Transport: tr}
	for i := 0; i < 30; i++ {
		res, e := client.Get(origin.URL)
		if e != nil {
			t.Fatal(e)
		}
		body, e := io.ReadAll(res.Body)
		res.Body.Close()
		if e != nil || string(body) != "ok" {
			t.Fatal("bad response")
		}
	}
	if accepted.Load() != 1 || core.dials.Load() != 1 {
		t.Fatalf("reuse failed origin=%d outbound=%d", accepted.Load(), core.dials.Load())
	}
}
func TestHTTPChunkedBodiesTrailersAndHEAD(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", "123")
			return
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "request-body" || r.Trailer.Get("X-Input") != "done" {
			http.Error(w, "bad request", 400)
			return
		}
		w.Header().Set("Trailer", "X-Output")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		io.WriteString(w, "response-body")
		w.Header().Set("X-Output", "done")
	}))
	defer origin.Close()
	_, proxy, _ := fixture(t, Settings{})
	u, _ := url.Parse(proxy.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(u)}
	defer tr.CloseIdleConnections()
	client := http.Client{Transport: tr}
	req, _ := http.NewRequest("POST", origin.URL, io.NopCloser(strings.NewReader("request-body")))
	req.Trailer = http.Header{"X-Input": []string{"done"}}
	res, e := client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	body, e := io.ReadAll(res.Body)
	res.Body.Close()
	if e != nil || res.StatusCode != 200 || string(body) != "response-body" || res.Trailer.Get("X-Output") != "done" {
		t.Fatalf("chunked/trailer failed: %d %q %v", res.StatusCode, body, e)
	}
	res, e = client.Head(origin.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer res.Body.Close()
	if res.ContentLength != 123 {
		t.Fatal("HEAD framing lost")
	}
}
func TestCONNECTHalfCloseAndDuplex(t *testing.T) {
	payload := bytes.Repeat([]byte{'x'}, 2*1024*1024)
	l := echoListener(t, func(c net.Conn) {
		c.Write(payload)
		got, _ := io.ReadAll(c)
		if len(got) == len(payload) {
			c.Write([]byte("last-response"))
		}
	})
	_, p, _ := fixture(t, Settings{})
	c, r := connectTest(t, p, l.Addr().String())
	defer c.Close()
	done := make(chan error, 1)
	go func() { _, e := c.Write(payload); c.CloseWrite(); done <- e }()
	got, e := io.ReadAll(r)
	if e != nil || !bytes.Equal(got, append(bytes.Clone(payload), []byte("last-response")...)) {
		t.Fatalf("duplex/half-close failed len=%d err=%v", len(got), e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
}
func TestTLSProxyHalfClose(t *testing.T) {
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			response := bytes.Repeat([]byte{'z'}, 70000)
			up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, rw, e := w.(http.Hijacker).Hijack()
				if e != nil {
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(5 * time.Second))
				rw.WriteString("HTTP/1.1 200 OK\r\n\r\n")
				rw.Flush()
				if version == tls.VersionTLS13 {
					io.Copy(io.Discard, rw)
				} else {
					time.Sleep(100 * time.Millisecond)
				}
				c.Write(response[:35000])
				time.Sleep(30 * time.Millisecond)
				c.Write(response[35000:])
			}))
			up.TLS = &tls.Config{MinVersion: version, MaxVersion: version}
			up.StartTLS()
			defer up.Close()
			core, p, _ := fixture(t, Settings{true, up.URL, ""})
			ca := filepath.Join(t.TempDir(), "ca.pem")
			os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: up.Certificate().Raw}), 0600)
			core.a.cfg.ProxyCA = ca
			c, r := connectTest(t, p, "example.test:443")
			defer c.Close()
			c.CloseWrite()
			got, e := io.ReadAll(r)
			if e != nil || !bytes.Equal(got, response) {
				t.Fatalf("TLS response corrupted: %d %v", len(got), e)
			}
		})
	}
}
func TestSOCKS5AuthenticationAndRemoteDNS(t *testing.T) {
	observed := make(chan string, 1)
	l := echoListener(t, func(c net.Conn) {
		h := make([]byte, 2)
		io.ReadFull(c, h)
		methods := make([]byte, int(h[1]))
		io.ReadFull(c, methods)
		c.Write([]byte{5, 2})
		io.ReadFull(c, h)
		user := make([]byte, int(h[1]))
		io.ReadFull(c, user)
		io.ReadFull(c, h[:1])
		pw := make([]byte, int(h[0]))
		io.ReadFull(c, pw)
		if string(user) != "user" || string(pw) != "pass" {
			return
		}
		c.Write([]byte{1, 0})
		head := make([]byte, 4)
		io.ReadFull(c, head)
		if head[3] != 3 {
			return
		}
		io.ReadFull(c, h[:1])
		host := make([]byte, int(h[0]))
		io.ReadFull(c, host)
		io.ReadFull(c, h)
		observed <- string(host)
		c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
		body, _ := io.ReadAll(c)
		c.Write(body)
	})
	_, p, _ := fixture(t, Settings{true, "socks5://user:pass@" + l.Addr().String(), ""})
	c, r := connectTest(t, p, "no-local-dns.invalid:443")
	defer c.Close()
	c.Write([]byte("hello"))
	c.CloseWrite()
	body, e := io.ReadAll(r)
	if e != nil || string(body) != "hello" || <-observed != "no-local-dns.invalid" {
		t.Fatal("SOCKS forwarding failed")
	}
}
func TestHTTPProxyBasicAuth(t *testing.T) {
	observed := make(chan string, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- r.Header.Get("Proxy-Authorization")
		c, rw, e := w.(http.Hijacker).Hijack()
		if e != nil {
			return
		}
		defer c.Close()
		rw.WriteString("HTTP/1.1 200 OK\r\n\r\n")
		rw.Flush()
		body, _ := io.ReadAll(rw)
		c.Write(body)
	}))
	defer up.Close()
	uri := strings.Replace(up.URL, "://", "://user:pass@", 1)
	_, p, _ := fixture(t, Settings{true, uri, ""})
	c, r := connectTest(t, p, "example.test:443")
	defer c.Close()
	c.Write([]byte("hello"))
	c.CloseWrite()
	body, _ := io.ReadAll(r)
	if string(body) != "hello" || <-observed != "Basic dXNlcjpwYXNz" {
		t.Fatal("HTTP proxy auth failed")
	}
}
func TestWebSocketUpgrade(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, rw, e := w.(http.Hijacker).Hijack()
		if e != nil {
			return
		}
		defer c.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		rw.Flush()
		frame := make([]byte, 8)
		io.ReadFull(rw, frame)
		c.Write([]byte{130, 2, 'h', 'i'})
	}))
	defer origin.Close()
	_, p, _ := fixture(t, Settings{})
	pu, _ := url.Parse(p.URL)
	raw, e := net.Dial("tcp", pu.Host)
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	raw.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(raw, "GET %s/chat HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n", origin.URL, strings.TrimPrefix(origin.URL, "http://"))
	reader := bufio.NewReader(raw)
	res, e := http.ReadResponse(reader, &http.Request{Method: "GET"})
	if e != nil || res.StatusCode != 101 {
		t.Fatal("upgrade failed", e)
	}
	raw.Write([]byte{130, 130, 0, 0, 0, 0, 'h', 'i'})
	reply := make([]byte, 4)
	_, e = io.ReadFull(reader, reply)
	if e != nil || !bytes.Equal(reply, []byte{130, 2, 'h', 'i'}) {
		t.Fatal("WebSocket bytes changed")
	}
}
func TestProxySwitchAndLoopbackPreservation(t *testing.T) {
	echo := echoListener(t, func(c net.Conn) { io.Copy(c, c) })
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, rw, e := w.(http.Hijacker).Hijack()
		if e != nil {
			return
		}
		defer c.Close()
		rw.WriteString("HTTP/1.1 200 OK\r\n\r\n")
		rw.Flush()
		io.Copy(c, rw)
	}))
	defer proxy.Close()
	_, p, file := fixture(t, Settings{true, proxy.URL, ""})
	remote, _ := connectTest(t, p, "example.test:443")
	defer remote.Close()
	local, lr := connectTest(t, p, echo.Addr().String())
	defer local.Close()
	data, _ := json.Marshal(Settings{})
	temp := file + ".tmp"
	os.WriteFile(temp, data, 0600)
	os.Rename(temp, file)
	time.Sleep(700 * time.Millisecond)
	remote.SetReadDeadline(time.Now().Add(time.Second))
	if _, e := remote.Read(make([]byte, 1)); e == nil {
		t.Fatal("old proxy tunnel survived route switch")
	}
	local.Write([]byte("alive"))
	buf := make([]byte, 5)
	if _, e := io.ReadFull(lr, buf); e != nil || string(buf) != "alive" {
		t.Fatal("loopback tunnel interrupted")
	}
}
func TestOneWayStreamDoesNotHitReverseIdleTimeout(t *testing.T) {
	l := echoListener(t, func(c net.Conn) {
		for i := 0; i < 12; i++ {
			c.Write([]byte{'x'})
			time.Sleep(150 * time.Millisecond)
		}
	})
	core, p, _ := fixture(t, Settings{})
	core.a.cfg.Idle = 1
	c, r := connectTest(t, p, l.Addr().String())
	defer c.Close()
	body, e := io.ReadAll(r)
	if e != nil || string(body) != strings.Repeat("x", 12) {
		t.Fatal("active one-way stream timed out", e)
	}
}
func TestPIDReuseIsRejected(t *testing.T) {
	exe, _ := os.Executable()
	a := &app{root: t.TempDir(), exe: canonical(exe), cfg: Config{Port: 17891}}
	r, e := a.publish(false, 1000)
	if e != nil {
		t.Fatal(e)
	}
	r.Created++
	b, _ := json.Marshal(r)
	os.WriteFile(a.statePath(false), b, 0600)
	p, e := a.adopt(false)
	if p != nil {
		p.close()
	}
	if e == nil {
		t.Fatal("creation time mismatch accepted")
	}
}
