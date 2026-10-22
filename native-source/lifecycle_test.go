//go:build windows

package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestApplicationLifetimeRestartGraceAndUnknownScan(t *testing.T) {
	now := time.Now()
	state := applicationLifetime{}
	if state.absentLongEnough(now, false, nil) || state.absentLongEnough(now.Add(29*time.Second), false, nil) {
		t.Fatal("premature stop")
	}
	if state.absentLongEnough(now.Add(30*time.Second), true, nil) {
		t.Fatal("new instance not retained")
	}
	if state.absentLongEnough(now.Add(31*time.Second), false, nil) {
		t.Fatal("restart did not reset grace")
	}
	if state.absentLongEnough(now.Add(62*time.Second), false, errors.New("process list unavailable")) {
		t.Fatal("scan error treated as app exit")
	}
	if state.absentLongEnough(now.Add(63*time.Second), false, nil) {
		t.Fatal("uncertain scan did not reset grace")
	}
	if !state.absentLongEnough(now.Add(93*time.Second), false, nil) {
		t.Fatal("clean exit not detected")
	}
}

func TestApplicationRecoveryBackoffBoundAndReset(t *testing.T) {
	now := time.Now()
	state := applicationLifetime{}
	for i := 0; i < 20; i++ {
		state.attempted(now, errors.New("unavailable"))
		want := min(time.Second*time.Duration(1<<min(i, 6)), time.Minute)
		if state.retryDelay != want || !state.nextAttempt.Equal(now.Add(want)) {
			t.Fatalf("attempt %d: %+v", i, state)
		}
	}
	state.attempted(now, nil)
	if state.retryDelay != 0 || !state.nextAttempt.IsZero() {
		t.Fatal("successful recovery retained failure backoff")
	}
}

func TestApplicationScanRequiresExecutablePath(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if present, err := applicationPresent(canonical(self)); err != nil || !present {
		t.Fatalf("own process absent: %v %v", present, err)
	}
	other := filepath.Join(t.TempDir(), filepath.Base(self))
	if present, err := applicationPresent(other); err != nil || present {
		t.Fatalf("same name at different path accepted: %v %v", present, err)
	}
}

func TestLifecycleMaintenanceMutexSerializesStartup(t *testing.T) {
	a := historyTestApp(t)
	release, err := a.lifecycleLock(0)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		unlock, err := a.lifecycleLock(0)
		if err == nil {
			unlock()
		}
		done <- err
	}()
	if err := <-done; err == nil {
		release()
		t.Fatal("maintenance lock did not exclude startup/recovery")
	}
	release()
	release, err = a.lifecycleLock(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestUnverifiedFollowerRecordCannotTerminateProcess(t *testing.T) {
	a := historyTestApp(t)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err = writeRecord(a.followerPath(), record{PID: os.Getpid(), Created: 1, Executable: self, Script: self, ProxyPort: a.cfg.Port, Generation: nonce(), Version: marker}); err != nil {
		t.Fatal(err)
	}
	if a.followerRunning() {
		t.Fatal("forged identity accepted")
	}
	if err = a.stopFollower(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationLifetimeRespectsInterruptedMaintenance(t *testing.T) {
	a := historyTestApp(t)
	if a.lifecycleMaintenancePending() {
		t.Fatal("fresh install incorrectly blocked")
	}
	if err := os.Mkdir(filepath.Join(a.root, "upgrade-pending"), 0700); err != nil {
		t.Fatal(err)
	}
	if !a.lifecycleMaintenancePending() {
		t.Fatal("interrupted transaction not protected")
	}
}

func TestApplicationLifetimeDoesNotTreatUnknownEntryAsExited(t *testing.T) {
	a := historyTestApp(t)
	if a.entryConfirmedExited() {
		t.Fatal("missing record is not confirmed exit")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	a.exe = canonical(self)
	p, err := openOwned(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	r := record{PID: p.pid, Created: p.created, Executable: p.image, Script: a.exe, ProxyPort: a.cfg.Port, Generation: nonce(), Version: marker}
	if err = writeRecord(a.statePath(true), r); err != nil {
		t.Fatal(err)
	}
	if a.entryConfirmedExited() {
		t.Fatal("live entry treated as exited")
	}
	r.Created--
	if err = writeRecord(a.statePath(true), r); err != nil {
		t.Fatal(err)
	}
	if !a.entryConfirmedExited() {
		t.Fatal("PID reuse not recognized")
	}
}
