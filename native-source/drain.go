//go:build windows

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

var errDraining = errors.New("proxy is draining")

func (c *core) draining() bool {
	until := c.drainUntil.Load()
	return until != nil && time.Now().Before(*until)
}
func (c *core) beginDrain(lease time.Duration) {
	c.admission.Lock()
	defer c.admission.Unlock()
	// Keep time.Now's monotonic component: clock synchronization must not
	// prematurely resume admission or extend a maintenance lease indefinitely.
	until := time.Now().Add(lease)
	c.drainUntil.Store(&until)
}
func (c *core) resume() { c.admission.Lock(); defer c.admission.Unlock(); c.drainUntil.Store(nil) }

func (a *app) controlCall(r record, method, path string, body []byte) (map[string]any, error) {
	return a.callRecord(r, method, path, body, 2*time.Second)
}

func (a *app) callRecord(r record, method, path string, body []byte, timeout time.Duration) (map[string]any, error) {
	p, e := a.ownedRecord(r)
	if e != nil {
		return nil, e
	}
	p.close()
	req, e := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", r.HealthPort, path), bytes.NewReader(body))
	if e != nil {
		return nil, e
	}
	req.Header.Set("X-Worker-Generation", r.Generation)
	req.Header.Set("Content-Type", "application/json")
	tr := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	client := &http.Client{Timeout: timeout, Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, e := client.Do(req)
	if e != nil {
		return nil, e
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("X-Worker-Generation") != r.Generation || response.Header.Get("X-Worker-Pid") != strconv.Itoa(r.PID) {
		return nil, errors.New("control response identity or status mismatch")
	}
	var result map[string]any
	e = json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&result)
	return result, e
}
func (a *app) diagnosticStatus() error {
	r, e := a.readRecord(false)
	if e != nil {
		return e
	}
	result, e := a.controlCall(r, "GET", "/status", nil)
	if e != nil {
		return e
	}
	if r.DataPort > 0 {
		result["upgrade_entry"] = map[string]any{"available": false}
	}
	if super, e := a.readRecord(true); e == nil && super.HealthPort > 0 {
		if broker, e := a.callRecord(super, "GET", "/status", nil, 2*time.Second); e == nil {
			result["upgrade_entry"] = broker
		}
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
func (a *app) drain(seconds int) error {
	r, e := a.readRecord(false)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	payload, _ := json.Marshal(map[string]int{"lease_seconds": seconds + 10})
	if _, e = a.controlCall(r, "POST", "/drain", payload); e != nil {
		return e
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = a.controlCall(r, "POST", "/resume", nil)
		}
	}()
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for {
		status, e := a.controlCall(r, "GET", "/status", nil)
		if e != nil {
			return e
		}
		draining, _ := status["draining"].(bool)
		active, ok := status["active_requests"].(float64)
		queued, qok := status["queued_requests"].(float64)
		if !draining || !ok || !qok {
			return errors.New("invalid drain status")
		}
		retiredPending := false
		if r.DataPort > 0 {
			super, err := a.readRecord(true)
			if err != nil {
				return err
			}
			entry, err := a.callRecord(super, "GET", "/status", nil, 2*time.Second)
			if err != nil {
				return err
			}
			retired, ok := entry["retired_workers"].([]any)
			if !ok {
				return errors.New("invalid entry drain status")
			}
			retiredPending = len(retired) > 0
		}
		if active == 0 && queued == 0 && !retiredPending {
			committed = true
			return json.NewEncoder(os.Stdout).Encode(map[string]any{"drained": true, "pid": r.PID, "lease_seconds": seconds + 10})
		}
		if !time.Now().Before(deadline) {
			_, _ = a.controlCall(r, "POST", "/drain-timeout", nil)
			return errors.New("drain timed out; existing connections retained and service resumed")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
