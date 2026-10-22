//go:build windows

package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type destinationWaiter struct {
	key      string
	done     chan struct{}
	admitted bool
}
type destinationAdmission struct {
	mu      sync.Mutex
	active  map[string]int
	waiters []*destinationWaiter
	waits   int64
}

func destinationAdmissionKey(r *http.Request) string {
	host, port := r.URL.Hostname(), r.URL.Port()
	if r.Method == http.MethodConnect {
		var e error
		host, port, e = net.SplitHostPort(r.Host)
		if e != nil {
			return "invalid-authority"
		}
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}
	if port == "" {
		port = "80"
		if r.URL.Scheme == "https" {
			port = "443"
		}
	}
	if n, e := strconv.Atoi(port); e == nil {
		port = strconv.Itoa(n)
	}
	return net.JoinHostPort(host, port)
}
func (c *core) allocateDestinationLocked(key string) bool {
	q := &c.destinationAdmission
	c.admission.RLock()
	defer c.admission.RUnlock()
	if c.closed.Load() || c.draining() {
		return false
	}
	if q.active[key] >= c.a.cfg.MaxPerDestination {
		return false
	}
	select {
	case c.slots <- struct{}{}:
		if q.active == nil {
			q.active = make(map[string]int)
		}
		q.active[key]++
		return true
	default:
		return false
	}
}
func (c *core) dispatchDestinationsLocked() {
	q := &c.destinationAdmission
	if c.closed.Load() || c.draining() {
		return
	}
	// Scan in arrival order, skipping a saturated destination rather than blocking
	// unrelated destinations behind it. The pending list has the existing bound.
	for i := 0; i < len(q.waiters); {
		waiter := q.waiters[i]
		if !c.allocateDestinationLocked(waiter.key) {
			i++
			continue
		}
		copy(q.waiters[i:], q.waiters[i+1:])
		q.waiters[len(q.waiters)-1] = nil
		q.waiters = q.waiters[:len(q.waiters)-1]
		<-c.pending
		waiter.admitted = true
		close(waiter.done)
	}
}
func (c *core) releaseDestinationLocked(key string) {
	q := &c.destinationAdmission
	<-c.slots
	q.active[key]--
	if q.active[key] == 0 {
		delete(q.active, key)
	}
	c.dispatchDestinationsLocked()
}
func (c *core) destinationRelease(key string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			q := &c.destinationAdmission
			q.mu.Lock()
			defer q.mu.Unlock()
			c.releaseDestinationLocked(key)
		})
	}
}
func (c *core) requestAdmission(ctx context.Context, key string) (func(), error) {
	if c.a.cfg.MaxPerDestination == 0 {
		if e := c.acquire(ctx); e != nil {
			return nil, e
		}
		return func() { <-c.slots }, nil
	}
	q := &c.destinationAdmission
	q.mu.Lock()
	if e := ctx.Err(); e != nil {
		q.mu.Unlock()
		return nil, e
	}
	if c.closed.Load() {
		q.mu.Unlock()
		return nil, errors.New("proxy is stopping")
	}
	if c.draining() {
		q.mu.Unlock()
		return nil, errDraining
	}
	c.dispatchDestinationsLocked()
	if c.allocateDestinationLocked(key) {
		if e := ctx.Err(); e != nil {
			c.releaseDestinationLocked(key)
			q.mu.Unlock()
			return nil, e
		}
		q.mu.Unlock()
		return c.destinationRelease(key), nil
	}
	select {
	case c.pending <- struct{}{}:
	default:
		c.queueRejected.Add(1)
		q.mu.Unlock()
		return nil, errCapacity
	}
	if q.active[key] >= c.a.cfg.MaxPerDestination {
		q.waits++
	}
	waiter := &destinationWaiter{key: key, done: make(chan struct{})}
	q.waiters = append(q.waiters, waiter)
	started := time.Now()
	q.mu.Unlock()
	defer func() { c.metrics.queue.observe(time.Since(started)) }()
	timer := time.NewTimer(c.queueWait)
	defer timer.Stop()
	var result error
	select {
	case <-waiter.done:
	case <-ctx.Done():
		result = ctx.Err()
	case <-c.stop:
		result = errors.New("proxy is stopping")
	case <-timer.C:
		if c.draining() {
			result = errDraining
		} else {
			result = errCapacity
			c.queueTimeouts.Add(1)
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if result == nil {
		if e := ctx.Err(); e != nil {
			result = e
		} else if c.closed.Load() {
			result = errors.New("proxy is stopping")
		} else if c.draining() {
			result = errDraining
		}
	}
	if waiter.admitted {
		if result != nil {
			c.releaseDestinationLocked(key)
			return nil, result
		}
		return c.destinationRelease(key), nil
	}
	// Canceled/timed-out waiters never acquire a global slot. Cancellation racing
	// an admission is handled above and returns both slot and destination permit.
	for i, w := range q.waiters {
		if w == waiter {
			copy(q.waiters[i:], q.waiters[i+1:])
			q.waiters[len(q.waiters)-1] = nil
			q.waiters = q.waiters[:len(q.waiters)-1]
			<-c.pending
			break
		}
	}
	c.dispatchDestinationsLocked()
	return nil, result
}
func (c *core) destinationAdmissionSnapshot() map[string]any {
	q := &c.destinationAdmission
	q.mu.Lock()
	defer q.mu.Unlock()
	return map[string]any{"max_per_destination": c.a.cfg.MaxPerDestination, "active_destination_count": len(q.active), "queued_requests": len(q.waiters), "destination_limited_waits": q.waits, "scope": "optional host and port admission limit; no target names; CONNECT internals are opaque"}
}
