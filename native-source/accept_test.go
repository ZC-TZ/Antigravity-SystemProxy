//go:build windows

package main

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestAcceptErrorClassification(t *testing.T) {
	for _, code := range []syscall.Errno{10004, 10024, 10050, 10051, 10052, 10053, 10054, 10055, 10060, 10064, 10065} {
		err := &net.OpError{Op: "accept", Err: os.NewSyscallError("AcceptEx", code)}
		if !retryableAcceptError(err) || socketErrorCode(err) != uint32(code) {
			t.Fatalf("lost retry or wrapped error code %d", code)
		}
	}
	for _, err := range []error{net.ErrClosed, syscall.Errno(10022), syscall.Errno(10038), errors.New("permanent failure")} {
		if retryableAcceptError(err) {
			t.Fatalf("retried terminal error: %v", err)
		}
	}
}

func TestAcceptOutageBackoffAndRecovery(t *testing.T) {
	listener := testTCP(t)
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(3 * time.Second))
	var events []string
	var waits []time.Duration
	calls := 0
	accept := func() (*net.TCPConn, error) {
		calls++
		if calls <= 20 {
			return nil, &net.OpError{Op: "accept", Err: syscall.Errno(10050)}
		}
		return listener.AcceptTCP()
	}
	conn, err := acceptWithRetry(accept, func(d time.Duration) bool {
		waits = append(waits, d)
		return true
	}, func(kind string, _ error) { events = append(events, kind) })
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if calls != 21 || len(waits) != 20 || len(events) != 2 || events[0] != "entry_accept_retrying" || events[1] != "entry_accept_recovered" {
		t.Fatalf("calls=%d waits=%v events=%v", calls, waits, events)
	}
	for i, delay := range waits {
		want := 5 * time.Millisecond * time.Duration(1<<min(i, 8))
		if want > time.Second {
			want = time.Second
		}
		if delay != want {
			t.Fatalf("wait %d=%v want %v", i, delay, want)
		}
	}
	// A pending client remains on the original listener and receives one reply.
	go conn.Write([]byte("confirmed-once"))
	buf := make([]byte, len("confirmed-once"))
	if _, err := io.ReadFull(client, buf); err != nil || string(buf) != "confirmed-once" {
		t.Fatalf("reply=%q err=%v", buf, err)
	}
	_, err = acceptWithRetry(func() (*net.TCPConn, error) { return nil, net.ErrClosed }, func(time.Duration) bool { t.Fatal("closed listener retried"); return true }, func(string, error) { t.Fatal("unexpected retry notification") })
	if !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}

func TestAcceptRetryCancellation(t *testing.T) {
	stop := make(chan struct{})
	close(stop)
	started := time.Now()
	if waitAcceptRetry(stop, time.Hour) || time.Since(started) > time.Second {
		t.Fatal("retry did not cancel promptly")
	}
	calls := 0
	_, err := acceptWithRetry(func() (*net.TCPConn, error) { calls++; return nil, syscall.Errno(10050) }, func(time.Duration) bool { return false }, func(string, error) {})
	if calls != 1 || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestAcceptEventsPersistNumericErrorWithoutMessage(t *testing.T) {
	a := historyTestApp(t)
	h := newHistory(a, "supervisor")
	err := &net.OpError{Op: "accept", Err: os.NewSyscallError("AcceptEx", syscall.Errno(10050))}
	h.emit(historyEvent{Kind: "entry_accept_retrying", Reason: "listener_accept", ErrorCode: socketErrorCode(err)})
	h.emit(historyEvent{Kind: "supervisor_exit", Reason: "entry_accept_failed", ErrorCode: socketErrorCode(syscall.Errno(10022))})
	h.close()
	report, readErr := a.readHistory()
	if readErr != nil || len(report.Events) != 2 || report.Events[0].ErrorCode != 10050 || report.Events[1].ErrorCode != 10022 {
		t.Fatalf("error observations lost: %+v %v", report, readErr)
	}
}

func TestAcceptRetryDoesNotInterruptEstablishedConnection(t *testing.T) {
	listener := testTCP(t)
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(3 * time.Second))
	server, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.SetDeadline(time.Now().Add(3 * time.Second))
	_, err = acceptWithRetry(func() (*net.TCPConn, error) { return nil, syscall.Errno(10050) }, func(time.Duration) bool {
		go server.Write([]byte("live"))
		buf := make([]byte, 4)
		if _, e := io.ReadFull(client, buf); e != nil || string(buf) != "live" {
			t.Fatalf("existing stream failed: %q %v", buf, e)
		}
		return false
	}, func(string, error) {})
	if !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}
