//go:build windows

package main

import (
	"bufio"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
)

func findIdleConn(conn net.Conn) *idleConn {
	for {
		switch c := conn.(type) {
		case *httpUploadConn:
			return c.idleConn
		case *idleConn:
			return c
		case *tls.Conn:
			conn = c.NetConn()
		case *bufferedConn:
			conn = c.Conn
		default:
			return nil
		}
	}
}

type upgradeWriter struct {
	http.ResponseWriter
	core    *core
	backend atomic.Pointer[idleConn]
	route   *connectionRoute
}

func (w *upgradeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *upgradeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	back := w.backend.Load()
	if back == nil {
		return nil, nil, errors.New("upgraded backend is not tracked")
	}
	conn, rw, e := http.NewResponseController(w.ResponseWriter).Hijack()
	if e != nil {
		return nil, nil, e
	}
	if e = rw.Writer.Flush(); e != nil {
		_ = conn.Close()
		return nil, nil, e
	}
	front := &idleConn{observation: back.observation, Conn: conn, activity: back.activity, timeout: w.core.a.idle()}
	limit := w.core.a.websocketWriteTimeout()
	front.writeLimit.Store(int64(limit))
	back.writeLimit.Store(int64(limit))
	front.onClose = func() { w.core.mu.Lock(); delete(w.core.upgrades, front); w.core.mu.Unlock() }
	w.core.mu.Lock()
	_, alive := w.core.httpConns[back]
	unchanged := false
	if w.route != nil {
		u, e := choose(*w.core.state.Load(), w.route.host, w.route.scheme, w.core.a.cfg.Port, w.core.a.dataPort, w.core.a.controlPort)
		unchanged = e == nil && routeKey(u) == w.route.key
	}
	if w.core.closed.Load() || !alive || !unchanged {
		w.core.mu.Unlock()
		_ = conn.Close()
		return nil, nil, errors.New("upgrade closed during handoff")
	}
	// After 101 this connection is dedicated to one origin, even if it used
	// an HTTP proxy pool earlier. Bind its route rather than treating it as shared.
	w.core.httpConns[back] = w.route
	w.core.upgrades[front] = back
	w.core.mu.Unlock()
	// Keep any bytes already read with the HTTP headers. All subsequent socket
	// reads and writes go through the shared activity and timeout wrapper.
	buffered := &bufferedConn{Conn: front, reader: rw.Reader}
	return buffered, bufio.NewReadWriter(bufio.NewReader(buffered), bufio.NewWriter(front)), nil
}
