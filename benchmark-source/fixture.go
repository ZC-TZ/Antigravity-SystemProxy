package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"time"
)

type repeating struct{}

func (repeating) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}
func digest(n int64) string {
	h := sha256.New()
	io.CopyN(h, repeating{}, n)
	return hex.EncodeToString(h.Sum(nil))
}

type fixture struct {
	origin, proxy *httptest.Server
	roots         *x509.CertPool
	mu            sync.Mutex
	conns         map[net.Conn]bool
	closing       bool
}

func (f *fixture) track(c net.Conn) func() {
	f.mu.Lock()
	if f.closing {
		c.Close()
	} else {
		f.conns[c] = true
	}
	f.mu.Unlock()
	return func() { c.Close(); f.mu.Lock(); delete(f.conns, c); f.mu.Unlock() }
}
func newFixture() (*fixture, error) {
	f := &fixture{conns: map[net.Conn]bool{}}
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return nil, e
	}
	serial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if e != nil {
		return nil, e
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "proxy benchmark fixture"}, DNSNames: []string{"bench.invalid"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(7 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	der, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if e != nil {
		return nil, e
	}
	private, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		return nil, e
	}
	cert, e := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}))
	if e != nil {
		return nil, e
	}
	f.roots = x509.NewCertPool()
	parsed, e := x509.ParseCertificate(der)
	if e != nil {
		return nil, e
	}
	f.roots.AddCert(parsed)
	f.origin = httptest.NewUnstartedServer(http.HandlerFunc(f.serve))
	f.origin.EnableHTTP2 = true
	f.origin.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	f.origin.StartTLS()
	f.proxy = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "CONNECT" {
			if r.URL.Hostname() != "bench.invalid" {
				http.Error(w, "fixture target required", 403)
				return
			}
			f.serve(w, r)
			return
		}
		if r.Host != "bench.invalid:443" {
			http.Error(w, "fixture CONNECT target required", 403)
			return
		}
		remote, e := net.DialTimeout("tcp", f.origin.Listener.Addr().String(), time.Second)
		if e != nil {
			http.Error(w, "fixture unavailable", 502)
			return
		}
		defer f.track(remote)()
		local, rw, e := http.NewResponseController(w).Hijack()
		if e != nil {
			return
		}
		defer f.track(local)()
		io.WriteString(rw, "HTTP/1.1 200 Connection Established\r\n\r\n")
		if rw.Flush() != nil {
			return
		}
		done := make(chan struct{}, 2)
		go func() { io.Copy(remote, rw); remote.Close(); local.Close(); done <- struct{}{} }()
		go func() { io.Copy(local, remote); local.Close(); remote.Close(); done <- struct{}{} }()
		<-done
		<-done
	}))
	return f, nil
}
func (f *fixture) close() {
	f.mu.Lock()
	f.closing = true
	for c := range f.conns {
		c.Close()
	}
	f.mu.Unlock()
	f.proxy.Close()
	f.origin.Close()
}
func (f *fixture) serve(w http.ResponseWriter, r *http.Request) {
	if streamFixture(w, r) {
		return
	}
	if r.URL.Path == "/ws" {
		f.websocket(w, r)
		return
	}
	n, e := strconv.ParseInt(r.URL.Query().Get("n"), 10, 64)
	if e != nil || n < 0 || n > 64<<20 {
		http.Error(w, "bad fixture size", 400)
		return
	}
	switch r.URL.Path {
	case "/bytes":
		w.Header().Set("Content-Length", fmt.Sprint(n))
		w.Header().Set("Content-Type", "application/octet-stream")
		io.CopyN(w, repeating{}, n)
	case "/upload":
		h := sha256.New()
		got, e := io.Copy(h, r.Body)
		if e != nil || got != n {
			http.Error(w, "upload incomplete", 400)
			return
		}
		w.Header().Set("Content-Length", "64")
		io.WriteString(w, hex.EncodeToString(h.Sum(nil)))
	case "/events":
		if n > 100 {
			http.Error(w, "bad event count", 400)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for i := int64(0); i < n; i++ {
			fmt.Fprintf(w, "data: %d\n\n", i)
			if http.NewResponseController(w).Flush() != nil {
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	default:
		http.NotFound(w, r)
	}
}
func (f *fixture) websocket(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Upgrade") != "websocket" {
		http.Error(w, "upgrade required", 400)
		return
	}
	c, rw, e := http.NewResponseController(w).Hijack()
	if e != nil {
		return
	}
	defer f.track(c)()
	h := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(h[:]))
	if rw.Flush() != nil {
		return
	}
	for {
		c.SetDeadline(time.Now().Add(15 * time.Second))
		var header [2]byte
		if _, e = io.ReadFull(rw, header[:]); e != nil {
			return
		}
		n := int(header[1] & 127)
		if header[0] == 0x88 {
			return
		}
		if header[0] != 0x89 || header[1]&128 == 0 || n > 125 {
			return
		}
		mask := make([]byte, 4)
		if _, e = io.ReadFull(rw, mask); e != nil {
			return
		}
		payload := make([]byte, n)
		if _, e = io.ReadFull(rw, payload); e != nil {
			return
		}
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
		if _, e = rw.Write(append([]byte{0x8a, byte(n)}, payload...)); e != nil {
			return
		}
		if rw.Flush() != nil {
			return
		}
	}
}
func wsPing(ctx context.Context, proxy string, roots *x509.CertPool, route string, add func(sample)) error {
	client, tr, e := makeClient(proxy, roots, false, false)
	if e != nil {
		return e
	}
	defer tr.close()
	client.Timeout = 0
	tr.ForceAttemptHTTP2 = false
	tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
	request, _ := http.NewRequestWithContext(ctx, "GET", "https://bench.invalid/ws", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	response, e := client.Do(request)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	if response.StatusCode != 101 || response.Header.Get("Sec-WebSocket-Accept") != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		return fmt.Errorf("fixture WebSocket handshake rejected")
	}
	rw, ok := response.Body.(io.ReadWriteCloser)
	if !ok {
		return fmt.Errorf("upgraded connection is not duplex")
	}
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			response.Body.Close()
		case <-stopped:
		}
	}()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			frame := []byte{0x89, 0x88, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4}
			start := tickNS()
			deadline := time.AfterFunc(10*time.Second, func() { rw.Close() })
			_, e = rw.Write(frame)
			var got [10]byte
			if e == nil {
				_, e = io.ReadFull(rw, got[:])
			}
			deadline.Stop()
			if ctx.Err() != nil {
				return nil
			}
			s := sample{Route: route, Scenario: "soak_websocket_ping", TTFBMS: float64(tickNS()-start) / float64(time.Millisecond), TotalMS: float64(tickNS()-start) / float64(time.Millisecond), Bytes: 8}
			if e != nil {
				s.Error = "websocket_io"
			} else if got != [10]byte{0x8a, 8, 0, 0, 0, 0, 0, 0, 0, 0} {
				s.Error = "websocket_payload"
			}
			add(s)
			if s.Error != "" {
				return fmt.Errorf("WebSocket ping failed")
			}
		}
	}
}
