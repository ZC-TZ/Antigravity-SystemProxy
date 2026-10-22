//go:build windows

package main

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type dataAndFailure struct{ sent bool }

func (r *dataAndFailure) Read(p []byte) (int, error) {
	if r.sent {
		return 0, io.EOF
	}
	r.sent = true
	return copy(p, []byte("last-bytes")), io.ErrUnexpectedEOF
}

func TestReadFromPreservesDataReturnedWithReadError(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	activity := &atomic.Int64{}
	activity.Store(time.Now().UnixNano())
	metrics := &diagnostics{}
	base := &idleConn{Conn: left, activity: activity, timeout: time.Second, upstream: true, observation: &connectionObservation{d: metrics, start: time.Now()}}
	c := &httpUploadConn{base}
	got := make(chan []byte, 1)
	go func() { data := make([]byte, 10); io.ReadFull(right, data); got <- data }()
	n, err := c.ReadFrom(&dataAndFailure{})
	if n != 10 || !errors.Is(err, io.ErrUnexpectedEOF) || !bytes.Equal(<-got, []byte("last-bytes")) || metrics.upstreamWritten.Load() != 10 {
		t.Fatalf("data/error accounting: %d %v %+v", n, err, metrics.snapshot())
	}
}

func TestReadFromHonorsDeadlineWhileReceiverStalls(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	activity := &atomic.Int64{}
	activity.Store(time.Now().UnixNano())
	base := &idleConn{Conn: left, activity: activity, timeout: time.Second}
	c := &httpUploadConn{base}
	c.SetWriteDeadline(time.Now().Add(40 * time.Millisecond))
	start := time.Now()
	n, err := c.ReadFrom(bytes.NewReader([]byte("pending-command")))
	var timeout net.Error
	if n != 0 || !errors.As(err, &timeout) || !timeout.Timeout() || time.Since(start) > time.Second {
		t.Fatalf("deadline lost: %d %v", n, err)
	}
}

func TestReadFromDoesNotBufferUntilProducerEOF(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	source, producer := io.Pipe()
	defer source.Close()
	defer producer.Close()
	activity := &atomic.Int64{}
	activity.Store(time.Now().UnixNano())
	base := &idleConn{Conn: left, activity: activity, timeout: time.Second}
	c := &httpUploadConn{base}
	done := make(chan error, 1)
	go func() { _, err := c.ReadFrom(source); done <- err }()
	go producer.Write([]byte("prefix"))
	right.SetReadDeadline(time.Now().Add(time.Second))
	prefix := make([]byte, 6)
	if _, err := io.ReadFull(right, prefix); err != nil || string(prefix) != "prefix" {
		t.Fatalf("prefix delayed: %q %v", prefix, err)
	}
	select {
	case err := <-done:
		t.Fatalf("copy ended before producer close: %v", err)
	default:
	}
	producer.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
