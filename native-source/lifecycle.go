//go:build windows

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const applicationGrace = 30 * time.Second

// All comparisons use monotonic times. Unknown process-enumeration results
// cancel the absence countdown instead of risking shutdown of a live app.
type applicationLifetime struct {
	absentSince time.Time
	nextAttempt time.Time
	retryDelay  time.Duration
}

func (s *applicationLifetime) absentLongEnough(now time.Time, present bool, err error) bool {
	if err != nil || present {
		s.absentSince = time.Time{}
		return false
	}
	if s.absentSince.IsZero() {
		s.absentSince = now
	}
	return now.Sub(s.absentSince) >= applicationGrace
}

func (s *applicationLifetime) attempted(now time.Time, err error) {
	if err == nil {
		s.retryDelay = 0
		s.nextAttempt = time.Time{}
		return
	}
	if s.retryDelay == 0 {
		s.retryDelay = time.Second
	} else {
		s.retryDelay = min(s.retryDelay*2, time.Minute)
	}
	s.nextAttempt = now.Add(s.retryDelay)
}

func (a *app) followerPath() string {
	return filepath.Join(a.root, fmt.Sprintf("lifecycle-%d.json", a.cfg.Port))
}

func (a *app) lifecycleMaintenancePending() bool {
	_, err := os.Stat(filepath.Join(a.root, "upgrade-pending"))
	return !os.IsNotExist(err)
}

func (a *app) follower() (*process, error) {
	data, err := os.ReadFile(a.followerPath())
	if err != nil {
		return nil, err
	}
	var r record
	if err = json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return a.ownedRecord(r)
}

func (a *app) followerRunning() bool {
	p, err := a.follower()
	if err != nil {
		return false
	}
	p.close()
	return true
}

func (a *app) stopFollower() error {
	p, err := a.follower()
	if err != nil {
		return nil
	} // No unverified PID is ever terminated.
	defer p.close()
	if err = p.kill(); err != nil {
		return err
	}
	if err = p.wait(); err != nil {
		return err
	}
	err = os.Remove(a.followerPath())
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (a *app) startFollower() error {
	if a.followerRunning() {
		return nil
	}
	p, err := a.start("--watch-app")
	if err != nil {
		return err
	}
	defer p.close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a.followerRunning() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("application lifetime monitor failed to publish its identity")
}

// Match the installer's current-user mutex exactly. Windows mutex ownership is
// thread-based, so pin only while holding it, including abandoned acquisitions.
func (a *app) lifecycleLock(timeout time.Duration) (func(), error) {
	hash := sha256.Sum256([]byte(strings.ToLower(a.root)))
	name := "Local\\AntigravitySystemProxy-install-" + strings.ToUpper(hex.EncodeToString(hash[:]))
	runtime.LockOSThread()
	h, _, err := createMutex.Call(0, 0, uintptr(unsafe.Pointer(wide(name))))
	if h == 0 {
		runtime.UnlockOSThread()
		return nil, err
	}
	result, err := syscall.WaitForSingleObject(syscall.Handle(h), uint32(timeout/time.Millisecond))
	if err != nil || (result != 0 && result != 0x80) {
		syscall.CloseHandle(syscall.Handle(h))
		runtime.UnlockOSThread()
		if err == nil {
			err = errors.New("installation or application startup is busy")
		}
		return nil, err
	}
	return func() {
		kernel.NewProc("ReleaseMutex").Call(h)
		syscall.CloseHandle(syscall.Handle(h))
		runtime.UnlockOSThread()
	}, nil
}

// Enumerate every process at the configured executable path, not a single PID
// or a name supplied by a state file. App handles never request terminate rights.
func applicationPresent(image string) (bool, error) {
	snapshot, err := syscall.CreateToolhelp32Snapshot(syscall.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return false, err
	}
	defer syscall.CloseHandle(snapshot)
	var item syscall.ProcessEntry32
	item.Size = uint32(unsafe.Sizeof(item))
	err = syscall.Process32First(snapshot, &item)
	var uncertain error
	for err == nil {
		if strings.EqualFold(syscall.UTF16ToString(item.ExeFile[:]), filepath.Base(image)) {
			p, openErr := openProcessAccess(int(item.ProcessID), 0x00101000)
			if openErr == nil {
				matches := p.alive() && strings.EqualFold(canonical(p.image), image)
				p.close()
				if matches {
					return true, nil
				}
			} else if !errors.Is(openErr, syscall.Errno(87)) {
				uncertain = openErr
			}
		}
		err = syscall.Process32Next(snapshot, &item)
	}
	if !errors.Is(err, syscall.ERROR_NO_MORE_FILES) {
		return false, err
	}
	return false, uncertain
}

func (a *app) runtimeQuiet() bool {
	r, err := a.readRecord(false)
	if err != nil {
		return false
	}
	state, err := a.controlCall(r, "GET", "/status", nil)
	if err != nil || state["active_requests"] != float64(0) || state["queued_requests"] != float64(0) {
		return false
	}
	super, err := a.readRecord(true)
	if err != nil {
		return false
	}
	entry, err := a.controlCall(super, "GET", "/status", nil)
	if err != nil || entry["active_connections"] != float64(0) {
		return false
	}
	retired, ok := entry["retired_workers"].([]any)
	return ok && len(retired) == 0
}

// An unavailable control record is not proof of exit. Only a missing/reused PID
// or a signalled verified process permits orphan cleanup without drain status.
func (a *app) entryConfirmedExited() bool {
	r, err := a.readRecord(true)
	if err != nil {
		return false
	}
	p, err := openProcessAccess(r.PID, 0x00101000)
	if err != nil {
		return errors.Is(err, syscall.Errno(87))
	}
	defer p.close()
	return p.created != r.Created || !p.alive()
}

func (a *app) followApplication() error {
	// Reuse the same identity scheme as the entry guard with a distinct key.
	guardApp := &app{root: a.root + "-application-lifetime", cfg: a.cfg}
	handle, exists, err := guardApp.entryGuard()
	if err != nil {
		return err
	}
	defer syscall.CloseHandle(handle)
	if exists {
		return nil
	}
	p, err := openOwned(os.Getpid())
	if err != nil {
		return err
	}
	r := record{PID: p.pid, Created: p.created, Executable: p.image, Script: a.exe, ProxyPort: a.cfg.Port, Generation: nonce(), Version: marker}
	p.close()
	if err = writeRecord(a.followerPath(), r); err != nil {
		return err
	}
	defer os.Remove(a.followerPath())
	journal := newHistory(a, "lifecycle")
	defer journal.close()
	journal.emit(historyEvent{Kind: "application_follow_started", Reason: "all_matching_instances"})
	image := canonical(a.cfg.Application)
	lifetime := applicationLifetime{}
	for {
		time.Sleep(time.Second)
		release, lockErr := a.lifecycleLock(0)
		if lockErr != nil {
			continue
		} // Installation/restore/startup owns recovery.
		finished := func() bool {
			defer release()
			if a.lifecycleMaintenancePending() {
				return false
			}
			now := time.Now()
			present, scanErr := applicationPresent(image)
			if lifetime.absentLongEnough(now, present, scanErr) {
				if a.runtimeQuiet() {
					// A second scan closes the enumeration-to-stop window. Startup
					// through our launcher is serialized by the same mutex.
					if alive, e := applicationPresent(image); e == nil && !alive {
						if err := a.stopRuntime(); err == nil {
							journal.emit(historyEvent{Kind: "application_follow_stopped", Reason: "application_exited_and_connections_finished"})
							return true
						}
					}
				} else if a.entryConfirmedExited() {
					if a.stopRuntime() == nil {
						return true
					}
				}
				return false
			}
			if !present || scanErr != nil || now.Before(lifetime.nextAttempt) {
				return false
			}
			entry, entryErr := a.adopt(true)
			if entryErr == nil {
				entry.close()
				lifetime.attempted(now, nil)
				return false
			}
			journal.emit(historyEvent{Kind: "application_proxy_recovery", Reason: "entry_identity_unavailable"})
			err := a.ensure()
			lifetime.attempted(time.Now(), err)
			if err != nil {
				journal.emit(historyEvent{Kind: "application_proxy_recovery_failed", Reason: "ensure_failed"})
			}
			return false
		}()
		if finished {
			return nil
		}
	}
}
