//go:build windows

package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type tunnel struct {
	left, right       net.Conn
	host, scheme, key string
}

type routeContext struct{}

type destinationContext struct{}

type destination struct{ host, scheme string }

type connectionRoute struct {
	destination
	key string
}

type upgradeContext struct{}

type core struct {
	destinationAdmission destinationAdmission
	history              *historyJournal
	metrics              diagnostics
	admission            sync.RWMutex
	drainUntil           atomic.Pointer[time.Time]
	a                    *app
	state                atomic.Pointer[Settings]
	mu                   sync.Mutex
	tunnels              map[*tunnel]struct{}
	httpConns            map[*idleConn]*connectionRoute
	upgrades             map[*idleConn]*idleConn
	transport            *http.Transport
	proxy                *httputil.ReverseProxy
	slots                chan struct{}
	pending              chan struct{}
	queueWait            time.Duration
	queueTimeouts        atomic.Int64
	queueRejected        atomic.Int64
	dials                atomic.Int64
	stop                 chan struct{}
	closed               atomic.Bool
}

func newCore(a *app) (*core, error) {
	s, e := a.settings()
	if e != nil {
		return nil, e
	}
	max, pending, wait := a.cfg.MaxConnections, a.cfg.MaxPending, a.cfg.QueueWait
	if max == 0 {
		max = 128
	}
	if pending == 0 {
		pending = 64
	}
	if wait == 0 {
		wait = 2
	}
	c := &core{a: a, tunnels: map[*tunnel]struct{}{}, httpConns: map[*idleConn]*connectionRoute{}, upgrades: map[*idleConn]*idleConn{}, slots: make(chan struct{}, max), pending: make(chan struct{}, pending), queueWait: time.Duration(wait * float64(time.Second)), stop: make(chan struct{})}
	c.metrics.writeEnabled = a.cfg.SocketWriteMetrics
	c.state.Store(&s)
	// Share the admission ceiling instead of hiding an earlier per-key queue.
	// A bounded 64 KiB writer coalesces chunk framing and upload payload writes;
	// keep immediate response flushing so small agent events never wait for EOF.
	c.transport = &http.Transport{Proxy: func(r *http.Request) (*url.URL, error) {
		s, ok := r.Context().Value(routeContext{}).(Settings)
		if !ok {
			s = *c.state.Load()
		}
		u, e := choose(s, r.URL.Hostname(), r.URL.Scheme, a.cfg.Port, a.dataPort, a.controlPort)
		// SOCKS tunnels are established in DialContext so every protocol uses
		// the same validation, cancellation and bounded handshake. HTTP framing
		// and connection reuse remain owned by Transport.
		if e == nil && u != nil && (u.Scheme == "socks5" || u.Scheme == "socks5h") {
			return nil, nil
		}
		return u, e
	}, MaxIdleConns: min(max, 128), MaxIdleConnsPerHost: min(max, 128), MaxConnsPerHost: max, WriteBufferSize: 64 * 1024, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 12 * time.Second, ExpectContinueTimeout: time.Second, ForceAttemptHTTP2: true, DisableCompression: true, ResponseHeaderTimeout: a.idle()}
	c.transport.OnProxyConnectResponse = func(_ context.Context, _ *url.URL, _ *http.Request, response *http.Response) error {
		if response.StatusCode == http.StatusProxyAuthRequired {
			c.metrics.authFailures.Add(1)
			return errProxyAuthentication
		}
		return nil
	}
	c.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		ctx = context.WithValue(ctx, metricsContext{}, &c.metrics)
		var chosen *url.URL
		if dest, ok := ctx.Value(destinationContext{}).(destination); ok {
			if snapshot, ok := ctx.Value(routeContext{}).(Settings); ok {
				var e error
				chosen, e = choose(snapshot, dest.host, dest.scheme, a.cfg.Port, a.dataPort, a.controlPort)
				if e != nil {
					return nil, e
				}
			}
		}
		var raw net.Conn
		var e error
		socks := chosen != nil && (chosen.Scheme == "socks5" || chosen.Scheme == "socks5h")
		if socks {
			raw, e = a.dialTunnel(ctx, address, chosen)
		} else {
			raw, e = timedTCP(ctx, network, address)
		}
		if e != nil {
			c.metrics.dialFailures.Add(1)
			return nil, e
		}
		activity := &atomic.Int64{}
		activity.Store(time.Now().UnixNano())
		tracked := &idleConn{observation: &connectionObservation{d: &c.metrics, start: time.Now()}, upstream: true, Conn: raw, activity: activity, timeout: a.idle()}
		tracked.onClose = func() { c.mu.Lock(); delete(c.httpConns, tracked); c.mu.Unlock() }
		c.mu.Lock()
		if c.closed.Load() {
			c.mu.Unlock()
			_ = raw.Close()
			return nil, errors.New("proxy is stopping")
		}
		if snapshot, ok := ctx.Value(routeContext{}).(Settings); ok && snapshot != *c.state.Load() {
			unchanged := false
			if dest, ok := ctx.Value(destinationContext{}).(destination); ok {
				unchanged = sameRoute(snapshot, *c.state.Load(), dest, a.cfg.Port, a.dataPort, a.controlPort)
			}
			if !unchanged {
				c.mu.Unlock()
				_ = raw.Close()
				return nil, errors.New("system proxy changed during HTTP connection setup")
			}
		}
		var direct *connectionRoute
		if dest, ok := ctx.Value(destinationContext{}).(destination); ok {
			if snapshot, ok := ctx.Value(routeContext{}).(Settings); ok {
				if chosen, e := choose(snapshot, dest.host, dest.scheme, a.cfg.Port, a.dataPort, a.controlPort); e == nil && (chosen == nil || socks) {
					direct = &connectionRoute{dest, routeKey(chosen)}
				}
			}
		}
		c.httpConns[tracked] = direct
		c.mu.Unlock()
		c.dials.Add(1)
		return &httpUploadConn{tracked}, nil
	}
	tc, e := a.tlsConfig("")
	if e != nil {
		return nil, e
	}
	c.transport.TLSClientConfig = tc
	c.proxy = &httputil.ReverseProxy{Transport: observedHTTPTransport{base: c.transport, metrics: &c.metrics.httpAcquire}, FlushInterval: -1, BufferPool: bufferPool{}, ModifyResponse: func(response *http.Response) error {
		if response.StatusCode != http.StatusProxyAuthRequired {
			return nil
		}
		req := response.Request
		snapshot, ok := req.Context().Value(routeContext{}).(Settings)
		if !ok {
			return nil
		}
		u, e := choose(snapshot, req.URL.Hostname(), req.URL.Scheme, a.cfg.Port, a.dataPort, a.controlPort)
		if e == nil && u != nil && (u.Scheme == "http" || u.Scheme == "https") {
			c.metrics.authFailures.Add(1)
			return errProxyAuthentication
		}
		return nil
	}, Rewrite: func(p *httputil.ProxyRequest) {
		p.Out.Header.Del("Proxy-Authorization")
		p.Out.Header.Del("Proxy-Connection")
		// Incoming trailers are populated only when the streaming body reaches EOF.
		p.Out.Trailer = p.In.Trailer
		snapshot := *c.state.Load()
		dest := destination{p.Out.URL.Hostname(), p.Out.URL.Scheme}
		ctx := context.WithValue(p.Out.Context(), routeContext{}, snapshot)
		ctx = context.WithValue(ctx, destinationContext{}, dest)
		if upgrade, ok := ctx.Value(upgradeContext{}).(*upgradeWriter); ok {
			if u, e := choose(snapshot, dest.host, dest.scheme, a.cfg.Port, a.dataPort, a.controlPort); e == nil {
				upgrade.route = &connectionRoute{dest, routeKey(u)}
			}
		}

		started := time.Now()
		var written atomic.Int64
		trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				written.Store(time.Now().UnixNano())
			}
		}, GotFirstResponseByte: func() {
			now := time.Now()
			c.metrics.firstByte.observe(now.Sub(started))
			if n := written.Load(); n > 0 {
				c.metrics.responseWait.observe(now.Sub(time.Unix(0, n)))
			}
		}}
		p.Out = p.Out.WithContext(httptrace.WithClientTrace(ctx, trace))
	}, ErrorHandler: func(w http.ResponseWriter, r *http.Request, e error) {
		c.metrics.httpFailures.Add(1)
		if errors.Is(e, errProxyAuthentication) {
			http.Error(w, "Upstream proxy authentication failed", http.StatusBadGateway)
			return
		}
		http.Error(w, "Upstream request failed", http.StatusBadGateway)
	}}
	go c.watch()
	return c, nil
}

func (c *core) watch() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			s, e := c.a.settings()
			if e != nil {
				continue
			}
			c.updateSettings(s)
		}
	}
}

func (c *core) updateSettings(s Settings) {
	c.mu.Lock()
	old := c.state.Load()
	if c.closed.Load() || s == *old {
		c.mu.Unlock()
		return
	}
	c.state.Store(&s)
	var closing []net.Conn
	for t := range c.tunnels {
		u, e := choose(s, t.host, t.scheme, c.a.cfg.Port, c.a.dataPort, c.a.controlPort)
		if e != nil || routeKey(u) != t.key {
			closing = append(closing, t.left, t.right)
		}
	}
	if old.Enabled || s.Enabled {
		for conn, binding := range c.httpConns {
			// Direct pool connections and upgraded sockets each have one origin.
			// Preserve them whenever that origin's effective route is unchanged.
			if binding != nil {
				if u, e := choose(s, binding.host, binding.scheme, c.a.cfg.Port, c.a.dataPort, c.a.controlPort); e == nil && routeKey(u) == binding.key {
					continue
				}
			}
			// Proxied HTTP connections may serve multiple origins; retire them
			// conservatively rather than retaining an obsolete bypass decision.
			closing = append(closing, conn)
		}
	}
	for front, back := range c.upgrades {
		for _, conn := range closing {
			if conn == back {
				closing = append(closing, front)
				break
			}
		}
	}
	c.mu.Unlock()
	for _, conn := range closing {
		noteClose(conn, closeRoute)
		_ = conn.Close()
	}
	// Affected sockets (including idle ones) were closed individually above.
	// Flushing the whole pool here would also retire unchanged direct/SOCKS routes.
	log.Printf("System proxy changed; closed %d affected sockets", len(closing))
}

var errCapacity = errors.New("proxy admission capacity exhausted")

func (c *core) acquire(ctx context.Context) error {
	c.admission.RLock()
	if c.draining() {
		c.admission.RUnlock()
		return errDraining
	}

	if e := ctx.Err(); e != nil {
		c.admission.RUnlock()
		return e
	}
	if c.closed.Load() {
		c.admission.RUnlock()
		return errors.New("proxy is stopping")
	}
	select {
	case c.slots <- struct{}{}:
		c.admission.RUnlock()
		if e := ctx.Err(); e != nil {
			<-c.slots
			return e
		}
		if c.closed.Load() {
			<-c.slots
			return errors.New("proxy is stopping")
		}
		return nil
	default:
	}
	c.admission.RUnlock()
	started := time.Now()
	select {
	case c.pending <- struct{}{}:
		defer func() { c.metrics.queue.observe(time.Since(started)) }()
		defer func() { <-c.pending }()
	default:
		c.queueRejected.Add(1)
		return errCapacity
	}
	timer := time.NewTimer(c.queueWait)
	defer timer.Stop()
	select {
	case c.slots <- struct{}{}:
		c.admission.RLock()
		draining := c.draining()
		c.admission.RUnlock()
		if draining {
			<-c.slots
			return errDraining
		}

		if e := ctx.Err(); e != nil {
			<-c.slots
			return e
		}
		if c.closed.Load() {
			<-c.slots
			return errors.New("proxy is stopping")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.stop:
		return errors.New("proxy is stopping")
	case <-timer.C:
		c.queueTimeouts.Add(1)
		return errCapacity
	}
}

func (c *core) close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	close(c.stop)
	var closing []net.Conn
	c.mu.Lock()
	for t := range c.tunnels {
		closing = append(closing, t.left, t.right)
	}
	for conn := range c.httpConns {
		closing = append(closing, conn)
	}
	for front := range c.upgrades {
		closing = append(closing, front)
	}
	c.mu.Unlock()
	for _, conn := range closing {
		noteClose(conn, closeShutdown)
		_ = conn.Close()
	}
	c.transport.CloseIdleConnections()
}

func (c *core) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Scheme {
	case "ws":
		r.URL.Scheme = "http"
	case "wss":
		r.URL.Scheme = "https"
	}
	if r.ProtoMajor == 1 && r.Method != http.MethodConnect {
		controller := http.NewResponseController(w)
		if e := controller.EnableFullDuplex(); e != nil {
			http.Error(w, "Full-duplex forwarding unavailable", http.StatusInternalServerError)
			return
		}
		if r.Body != nil && r.Body != http.NoBody {
			body := &forwardingBody{ReadCloser: r.Body, controller: controller}
			body.remaining.Store(r.ContentLength)
			if r.ContentLength == 0 {
				body.complete.Store(true)
			}
			r.Body = body
			defer body.Close()
		}
	}
	if c.selfTarget(r) {
		http.Error(w, "Proxy cannot forward to itself", http.StatusBadRequest)
		return
	}
	admissionKey := ""
	if c.a.cfg.MaxPerDestination > 0 {
		admissionKey = destinationAdmissionKey(r)
	}
	release, admissionErr := c.requestAdmission(r.Context(), admissionKey)
	if e := admissionErr; e != nil {
		if errors.Is(e, errCapacity) || errors.Is(e, errDraining) {
			w.Header().Set("Retry-After", "1")
		}
		http.Error(w, "Proxy temporarily busy or stopping", http.StatusServiceUnavailable)
		return
	}
	defer release()
	if r.Method == http.MethodConnect {
		c.connect(w, r)
		return
	}
	if r.URL.Host == "" || (r.URL.Scheme != "http" && r.URL.Scheme != "https") {
		http.Error(w, "Absolute HTTP URL required", 400)
		return
	}
	if _, e := choose(*c.state.Load(), r.URL.Hostname(), r.URL.Scheme, c.a.cfg.Port, c.a.dataPort, c.a.controlPort); e != nil {
		http.Error(w, "Invalid system proxy configuration", 502)
		return
	}
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		tracked := &upgradeWriter{ResponseWriter: w, core: c}
		trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { tracked.backend.Store(findIdleConn(info.Conn)) }}
		r = r.WithContext(httptrace.WithClientTrace(context.WithValue(r.Context(), upgradeContext{}, tracked), trace))
		w = tracked
	}
	c.proxy.ServeHTTP(w, r)
}

func (c *core) selfTarget(r *http.Request) bool {
	host, port := r.URL.Hostname(), r.URL.Port()
	if r.Method == http.MethodConnect {
		var e error
		host, port, e = net.SplitHostPort(r.Host)
		if e != nil {
			return false
		}
	} else if port == "" {
		port = "80"
		if r.URL.Scheme == "https" {
			port = "443"
		}
	}
	n, _ := strconv.Atoi(port)
	ip := net.ParseIP(host)
	return (n == c.a.cfg.Port || n == c.a.dataPort || n == c.a.controlPort) && (strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") || (ip != nil && ip.IsLoopback()))
}

func (c *core) connect(w http.ResponseWriter, r *http.Request) {
	host, port, e := net.SplitHostPort(r.Host)
	if e != nil || host == "" {
		http.Error(w, "Invalid CONNECT authority", 400)
		return
	}
	n, e := strconv.Atoi(port)
	if e != nil || n < 1 || n > 65535 {
		http.Error(w, "Invalid port", 400)
		return
	}
	snapshot := *c.state.Load()
	chosen, e := choose(snapshot, host, "https", c.a.cfg.Port, c.a.dataPort, c.a.controlPort)
	if e != nil {
		http.Error(w, "Invalid system proxy configuration", 502)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), metricsContext{}, &c.metrics), 20*time.Second)
	defer cancel()
	remote, e := c.a.dialTunnel(ctx, r.Host, chosen)
	if e != nil {
		c.metrics.dialFailures.Add(1)
		if errors.Is(e, errProxyAuthentication) {
			http.Error(w, "Upstream proxy authentication failed", 502)
			return
		}
		http.Error(w, "Upstream connection failed", 502)
		return
	}
	defer remote.Close()
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "CONNECT unavailable", 500)
		return
	}
	client, rw, e := hijacker.Hijack()
	if e != nil {
		return
	}
	defer client.Close()
	activity := &atomic.Int64{}
	activity.Store(time.Now().UnixNano())
	observation := &connectionObservation{d: &c.metrics, start: time.Now()}
	left := &idleConn{observation: observation, Conn: client, activity: activity, timeout: c.a.idle()}
	right := &idleConn{observation: observation, upstream: true, Conn: remote, activity: activity, timeout: c.a.idle()}
	t := &tunnel{left: left, right: right, host: host, scheme: "https", key: routeKey(chosen)}
	c.mu.Lock()
	current, e := choose(*c.state.Load(), host, "https", c.a.cfg.Port, c.a.dataPort, c.a.controlPort)
	if c.closed.Load() || e != nil || routeKey(current) != t.key {
		c.mu.Unlock()
		return
	}
	c.tunnels[t] = struct{}{}
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.tunnels, t); c.mu.Unlock() }()
	if _, e = rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); e != nil {
		return
	}
	if e = rw.Flush(); e != nil {
		return
	}
	bridge(&bufferedConn{Conn: left, reader: rw.Reader}, right)
}
