//go:build windows

package main

import (
	"errors"
	"net"
	"syscall"
	"time"
)

func socketErrorCode(err error) uint32 {
	var code syscall.Errno
	if errors.As(err, &code) {
		return uint32(code)
	}
	return 0
}

func retryableAcceptError(err error) bool {
	if errors.Is(err, net.ErrClosed) {
		return false
	}
	// Winsock may report network transitions and aborted pending accepts even
	// though the listening socket remains usable. Never recreate the listener
	// or terminate existing tunnels merely because one accept failed.
	switch socketErrorCode(err) {
	case 10004, 10024, 10050, 10051, 10052, 10053, 10054, 10055, 10060, 10064, 10065:
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && (ne.Timeout() || ne.Temporary())
}

func waitAcceptRetry(stop <-chan struct{}, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-stop:
		return false
	case <-timer.C:
		return true
	}
}

// Keep one admission slot across retries. Backoff is bounded and resets after
// success; notification occurs only at the beginning/end of an outage.
func acceptWithRetry(accept func() (*net.TCPConn, error), wait func(time.Duration) bool, notify func(string, error)) (*net.TCPConn, error) {
	delay := time.Duration(0)
	for {
		conn, err := accept()
		if err == nil {
			if delay != 0 {
				notify("entry_accept_recovered", nil)
			}
			return conn, nil
		}
		if !retryableAcceptError(err) {
			return nil, err
		}
		if delay == 0 {
			notify("entry_accept_retrying", err)
			delay = 5 * time.Millisecond
		} else {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
		if !wait(delay) {
			return nil, net.ErrClosed
		}
	}
}
