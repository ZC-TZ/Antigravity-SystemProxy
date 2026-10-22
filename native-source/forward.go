//go:build windows

package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type proxyTLSState struct {
	caPath   string
	roots    *x509.CertPool
	sessions tls.ClientSessionCache
}

// Configuration is immutable during a worker lifetime. A replacement CA at the
// same path takes effect on a maintenance restart, just like other TLS policy.
func (a *app) tlsConfig(host string) (*tls.Config, error) {
	a.tlsMu.Lock()
	defer a.tlsMu.Unlock()
	if a.tlsState == nil || a.tlsState.caPath != a.cfg.ProxyCA {
		state := &proxyTLSState{caPath: a.cfg.ProxyCA, sessions: tls.NewLRUClientSessionCache(64)}
		if state.caPath != "" {
			pool, e := x509.SystemCertPool()
			if e != nil {
				pool = x509.NewCertPool()
			}
			data, e := os.ReadFile(state.caPath)
			if e != nil {
				return nil, e
			}
			if !pool.AppendCertsFromPEM(data) {
				return nil, errors.New("invalid proxy CA file")
			}
			state.roots = pool
		}
		a.tlsState = state
	}
	return &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12, RootCAs: a.tlsState.roots, ClientSessionCache: a.tlsState.sessions}, nil
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	if c.reader.Buffered() > 0 {
		return c.reader.Read(p)
	}
	return c.Conn.Read(p)
}

func (c *bufferedConn) CloseWrite() error { return closeWrite(c.Conn) }

func rawBase(c net.Conn) net.Conn {
	switch x := c.(type) {
	case *httpUploadConn:
		return rawBase(x.idleConn)
	case *bufferedConn:
		return rawBase(x.Conn)
	case *idleConn:
		return rawBase(x.Conn)
	default:
		return c
	}
}

func halfClose(c net.Conn) {
	_ = closeWrite(c)
}

func closeWrite(c net.Conn) error {
	base := rawBase(c)
	if t, ok := base.(*tls.Conn); ok {
		if t.ConnectionState().Version == tls.VersionTLS13 {
			return t.CloseWrite()
		}
		return nil
	}
	if t, ok := base.(interface{ CloseWrite() error }); ok {
		return t.CloseWrite()
	}
	return http.ErrNotSupported
}

type idleConn struct {
	observation *connectionObservation
	upstream    bool
	net.Conn
	activity                    *atomic.Int64
	timeout                     time.Duration
	onClose                     func()
	once                        sync.Once
	writeLimit                  atomic.Int64
	deadlineMu                  sync.Mutex
	writeMu                     sync.Mutex
	readDeadline, writeDeadline time.Time
}

// Keep caller deadlines authoritative when refreshing an internal idle limit.
func (c *idleConn) SetDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.readDeadline, c.writeDeadline = t, t
	return errors.Join(c.Conn.SetReadDeadline(effectiveDeadline(t, c.timeout)), c.Conn.SetWriteDeadline(effectiveDeadline(t, c.writeTimeout())))
}
func (c *idleConn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.readDeadline = t
	return c.Conn.SetReadDeadline(effectiveDeadline(t, c.timeout))
}
func (c *idleConn) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.writeDeadline = t
	return c.Conn.SetWriteDeadline(effectiveDeadline(t, c.writeTimeout()))
}
func effectiveDeadline(hard time.Time, limit time.Duration) time.Time {
	deadline := time.Now().Add(limit)
	if !hard.IsZero() && hard.Before(deadline) {
		return hard
	}
	return deadline
}
func (c *idleConn) writeTimeout() time.Duration {
	if configured := c.writeLimit.Load(); configured > 0 {
		return time.Duration(configured)
	}
	return c.timeout
}
func (c *idleConn) applyLimit(write bool, limit time.Duration) {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	hard := c.readDeadline
	if write {
		hard = c.writeDeadline
	}
	deadline := effectiveDeadline(hard, limit)
	if write {
		_ = c.Conn.SetWriteDeadline(deadline)
	} else {
		_ = c.Conn.SetReadDeadline(deadline)
	}
}
func (c *idleConn) deadlineExpired(write bool) bool {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	hard := c.readDeadline
	if write {
		hard = c.writeDeadline
	}
	return !hard.IsZero() && !time.Now().Before(hard)
}

// The standard server body may drain unread upload bytes on Close. A completed
// response must not retain an admission slot waiting for a client that stopped
// uploading after an early rejection. Preserve EOF for normal keep-alive reuse.
type forwardingBody struct {
	io.ReadCloser
	controller *http.ResponseController
	remaining  atomic.Int64
	complete   atomic.Bool
	once       sync.Once
	closeErr   error
}

func (b *forwardingBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if n > 0 && b.remaining.Load() >= 0 && b.remaining.Add(-int64(n)) <= 0 {
		b.complete.Store(true)
	}
	if e == io.EOF {
		b.complete.Store(true)
	}
	return n, e
}

func (b *forwardingBody) Close() error {
	b.once.Do(func() {
		if !b.complete.Load() {
			_ = b.controller.SetReadDeadline(time.Now())
		}
		b.closeErr = b.ReadCloser.Close()
	})
	return b.closeErr
}

func (c *idleConn) Read(p []byte) (int, error) {
	for {
		c.applyLimit(false, c.timeout)
		n, e := c.Conn.Read(p)
		if n > 0 {
			c.activity.Store(time.Now().UnixNano())
			c.observeIO(n, e, false)
			return n, e // A Reader may return data and an error together; never discard data.
		}
		if ne, ok := e.(net.Error); ok && ne.Timeout() && !c.deadlineExpired(false) {
			idleFor := time.Since(time.Unix(0, c.activity.Load()))
			// A wall-clock rollback can put the last activity in the future.
			// It must not restart an already expired read indefinitely.
			if idleFor >= 0 && idleFor < c.timeout {
				continue
			}
		}
		c.observeIO(n, e, false)
		return n, e
	}
}

func (c *idleConn) Write(p []byte) (int, error) {
	// Keep concurrent callers from interleaving the remainder of a partial write.
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	limit := c.writeTimeout()
	total := 0
	for {
		c.applyLimit(true, limit)
		var stats *socketWriteStats
		if c.observation != nil && c.observation.d.writeEnabled {
			stats = &c.observation.d.clientWrites
			if c.upstream {
				stats = &c.observation.d.upstreamWrites
			}
		}
		var started time.Time
		if stats != nil {
			started = stats.begin()
		}
		n, e := c.Conn.Write(p[total:])
		if stats != nil {
			stats.end(started)
		}
		total += n
		if n > 0 {
			c.activity.Store(time.Now().UnixNano())
		}
		ne, timedOut := e.(net.Error)
		_, tlsConnection := rawBase(c.Conn).(*tls.Conn)
		// Resume only bytes the raw connection has not accepted. TLS writes
		// cannot recover from a timeout; their internal write error is sticky.
		if timedOut && ne.Timeout() && n > 0 && !tlsConnection && !c.deadlineExpired(true) {
			c.observeIO(n, nil, true)
			if total == len(p) {
				return total, nil
			}
			continue
		}
		if e == nil && total < len(p) {
			e = io.ErrShortWrite
		}
		c.observeIO(n, e, true)
		return total, e
	}
}

// Transport's fixed-length body copy bypasses its buffered writer via
// persistConnWriter.ReadFrom. Keep that path on our bounded reusable buffer
// while still using Write for deadline, partial-write and byte observations.
type httpUploadConn struct{ *idleConn }

func (c *httpUploadConn) ReadFrom(r io.Reader) (int64, error) {
	b := buffers.Get().([]byte)
	defer buffers.Put(b)
	return io.CopyBuffer(struct{ io.Writer }{c}, struct{ io.Reader }{r}, b)
}

func (c *idleConn) Close() error {
	e := c.Conn.Close()
	c.once.Do(func() {
		c.observation.finish()
		if c.onClose != nil {
			c.onClose()
		}
	})
	return e
}

func (c *idleConn) CloseWrite() error { return closeWrite(c.Conn) }

var buffers = sync.Pool{New: func() any { return make([]byte, 64*1024) }}

type bufferPool struct{}

func (bufferPool) Get() []byte { return buffers.Get().([]byte) }

func (bufferPool) Put(b []byte) {
	if cap(b) == 64*1024 {
		buffers.Put(b[:64*1024])
	}
}

func bridge(left, right net.Conn) {
	done := make(chan error, 2)
	pump := func(dst, src net.Conn) {
		b := buffers.Get().([]byte)
		_, e := io.CopyBuffer(dst, src, b)
		buffers.Put(b)
		if e == nil {
			halfClose(dst)
		} else {
			_ = left.Close()
			_ = right.Close()
		}
		done <- e
	}
	go pump(right, left)
	go pump(left, right)
	<-done
	<-done
	_ = left.Close()
	_ = right.Close()
}

// Credentials are URL-decoded UTF-8 bytes; RFC 1929 bounds each field in octets.
func socksCredentials(u *url.URL) ([]byte, []byte, error) {
	if u.User == nil {
		return nil, nil, nil
	}
	user := []byte(u.User.Username())
	pw, _ := u.User.Password()
	pass := []byte(pw)
	if len(user) < 1 || len(user) > 255 || len(pass) < 1 || len(pass) > 255 {
		return nil, nil, errors.New("invalid SOCKS credentials length")
	}
	return user, pass, nil
}

func socksRequest(host, port string) ([]byte, error) {
	p, e := strconv.Atoi(port)
	if e != nil || p < 1 || p > 65535 {
		return nil, errors.New("invalid destination port")
	}
	msg := []byte{5, 1, 0}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			msg = append(msg, 1)
			msg = append(msg, v4...)
		} else {
			msg = append(msg, 4)
			msg = append(msg, ip.To16()...)
		}
	} else {
		if len(host) < 1 || len(host) > 255 {
			return nil, errors.New("invalid SOCKS hostname")
		}
		// Scoped IPv6 cannot be represented by SOCKS5; do not send it as a DNS name.
		for _, b := range []byte(host) {
			if b <= 32 || b == 127 || b == ':' || b == '%' {
				return nil, errors.New("invalid SOCKS hostname")
			}
		}
		msg = append(msg, 3, byte(len(host)))
		msg = append(msg, host...)
	}
	return binary.BigEndian.AppendUint16(msg, uint16(p)), nil
}

func socksConnect(c net.Conn, host, port string, u *url.URL) error {
	// Validate before sending credentials or starting a handshake for a bad target.
	msg, e := socksRequest(host, port)
	if e != nil {
		return e
	}
	user, pass, e := socksCredentials(u)
	if e != nil {
		return e
	}
	methods := []byte{5, 1, 0}
	if u.User != nil {
		methods = []byte{5, 2, 0, 2}
	}
	if _, e = c.Write(methods); e != nil {
		return e
	}
	var reply [2]byte
	if _, e = io.ReadFull(c, reply[:]); e != nil {
		return e
	}
	if reply[0] != 5 {
		return errors.New("invalid SOCKS version")
	}
	switch reply[1] {
	case 0:
	case 2:
		if u.User == nil {
			return errors.New("SOCKS selected an unadvertised authentication method")
		}
		auth := append([]byte{1, byte(len(user))}, user...)
		auth = append(auth, byte(len(pass)))
		auth = append(auth, pass...)
		if _, e = c.Write(auth); e != nil {
			return e
		}
		if _, e = io.ReadFull(c, reply[:]); e != nil {
			return e
		}
		if reply[0] != 1 || reply[1] != 0 {
			return errors.New("SOCKS authentication failed")
		}
	default:
		return errors.New("SOCKS authentication method rejected")
	}
	if _, e = c.Write(msg); e != nil {
		return e
	}
	var header [4]byte
	if _, e = io.ReadFull(c, header[:]); e != nil {
		return e
	}
	if header[0] != 5 || header[2] != 0 {
		return errors.New("invalid SOCKS reply header")
	}
	if header[1] != 0 {
		return fmt.Errorf("SOCKS connection rejected (reply 0x%02x)", header[1])
	}
	size := 0
	switch header[3] {
	case 1:
		size = 4
	case 4:
		size = 16
	case 3:
		if _, e = io.ReadFull(c, reply[:1]); e != nil {
			return e
		}
		size = int(reply[0])
		if size == 0 {
			return errors.New("empty SOCKS bound hostname")
		}
	default:
		return errors.New("invalid SOCKS reply address type")
	}
	// Read the exact reply length, preserving coalesced application bytes.
	_, e = io.CopyN(io.Discard, c, int64(size+2))
	return e
}

func (a *app) dialTunnel(ctx context.Context, target string, u *url.URL) (net.Conn, error) {
	if u != nil && (u.Scheme == "http" || u.Scheme == "https") {
		if e := validateBasicCredentials(u); e != nil {
			return nil, e
		}
	}
	address := target
	if u != nil {
		address = u.Host
	}
	c, e := timedTCP(ctx, "tcp", address)
	if e != nil {
		return nil, e
	}
	// DialContext only cancels dialing. Also cancel blocked proxy handshakes.
	raw := c
	stopCancel := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stopCancel()
	ok := false
	defer func() {
		if !ok {
			_ = c.Close()
		}
	}()
	deadline := time.Now().Add(20 * time.Second)
	if d, hasDeadline := ctx.Deadline(); hasDeadline && d.Before(deadline) {
		deadline = d
	}
	_ = c.SetDeadline(deadline)
	if u != nil {
		if d, ok := ctx.Value(metricsContext{}).(*diagnostics); ok {
			start := time.Now()
			defer func() { d.handshake.observe(time.Since(start)) }()
		}
		host, port, e := net.SplitHostPort(target)
		if e != nil {
			return nil, e
		}
		switch u.Scheme {
		case "https":
			cfg, e := a.tlsConfig(u.Hostname())
			if e != nil {
				return nil, e
			}
			tc := tls.Client(c, cfg)
			if e = tc.HandshakeContext(ctx); e != nil {
				return nil, e
			}
			c = tc
			fallthrough
		case "http":
			auth := ""
			if u.User != nil {
				pw, _ := u.User.Password()
				auth = "Proxy-Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte(u.User.Username()+":"+pw)) + "\r\n"
			}
			if _, e = fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n%s\r\n", target, target, auth); e != nil {
				return nil, e
			}
			reader := bufio.NewReader(c)
			response, e := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
			if e != nil {
				return nil, e
			}
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				// This is an owned raw connection, not a Transport response.
				// Do not drain an unbounded/absent rejection body on Close.
				// The failure defer closes the socket immediately.
				if response.StatusCode == http.StatusProxyAuthRequired {
					if d, ok := ctx.Value(metricsContext{}).(*diagnostics); ok {
						d.authFailures.Add(1)
					}
					return nil, errProxyAuthentication
				}
				return nil, errors.New("upstream CONNECT rejected")
			}
			c = &bufferedConn{Conn: c, reader: reader}
		case "socks5", "socks5h":
			if e = socksConnect(c, host, port, u); e != nil {
				return nil, e
			}
		}
	}
	_ = c.SetDeadline(time.Time{})
	if !stopCancel() {
		return nil, ctx.Err()
	}
	ok = true
	return c, nil
}
