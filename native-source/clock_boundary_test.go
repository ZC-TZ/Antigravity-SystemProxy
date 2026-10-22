//go:build windows

package main

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestFutureActivityDoesNotExtendExpiredRead(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	activity := &atomic.Int64{}
	// Model a wall-clock rollback: the previously stored activity is now future.
	activity.Store(time.Now().Add(time.Hour).UnixNano())
	c := &idleConn{Conn: left, activity: activity, timeout: 20 * time.Millisecond}
	done := make(chan error, 1)
	go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
	select {
	case err := <-done:
		if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
			t.Fatal("expected expired read", err)
		}
	case <-time.After(300 * time.Millisecond):
		left.Close()
		<-done
		t.Fatal("future wall timestamp kept restarting an expired idle read")
	}
}

func TestDrainLeaseRetainsMonotonicDeadline(t *testing.T) {
	c := &core{}
	c.beginDrain(time.Minute)
	until := c.drainUntil.Load()
	if until == nil || *until == until.Round(0) {
		t.Fatal("drain deadline lost its monotonic clock")
	}
	if !c.draining() {
		t.Fatal("new drain lease not active")
	}
	c.resume()
	if c.draining() || c.drainUntil.Load() != nil {
		t.Fatal("resume retained the lease")
	}
	c.beginDrain(-time.Second)
	if c.draining() {
		t.Fatal("expired lease still blocks admission")
	}
}
