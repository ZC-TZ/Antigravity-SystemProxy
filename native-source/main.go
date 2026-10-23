//go:build windows

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

func proxyEnvironment(parent []string, port int) []string {
	env := []string{}
	for _, pair := range parent {
		key := strings.ToUpper(strings.SplitN(pair, "=", 2)[0])
		switch key {
		case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "NO_GRPC_PROXY", "GRPC_PROXY", "ELECTRON_RUN_AS_NODE", "NODE_USE_ENV_PROXY":
			continue
		}
		env = append(env, pair)
	}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "GRPC_PROXY"} {
		env = append(env, key+"="+endpoint)
	}
	env = append(env, "NO_PROXY=localhost,127.0.0.1,::1", "NO_GRPC_PROXY=localhost,127.0.0.1,::1", "NODE_USE_ENV_PROXY=1")
	return env
}

func (a *app) launch(args []string) error {
	release, e := a.lifecycleLock(20 * time.Second)
	if e != nil {
		return e
	}
	defer release()
	if a.lifecycleMaintenancePending() {
		return errors.New("interrupted installation detected; rerun install.cmd before starting Antigravity")
	}
	if err := a.waitForApplication(10*time.Second, registeredApplicationPaths, validApplicationReplacement); err != nil {
		return err
	}
	if e := a.ensure(); e != nil {
		return e
	}
	if e := a.startFollower(); e != nil {
		return e
	}
	cmd := exec.Command(a.cfg.Application, append([]string{fmt.Sprintf("--proxy-server=http://127.0.0.1:%d", a.cfg.Port)}, args...)...)
	cmd.Env = proxyEnvironment(os.Environ(), a.cfg.Port)
	if e := cmd.Start(); e != nil {
		return e
	}
	return cmd.Process.Release()
}

func logFile(root, name string) {
	path := filepath.Join(root, name)
	if info, e := os.Stat(path); e == nil && info.Size() > 512000 {
		_ = os.Remove(path + ".1")
		_ = os.Rename(path, path+".1")
	}
	file, e := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e == nil {
		log.SetOutput(file)
	}
}

func run() error {
	if len(os.Args) == 3 && os.Args[1] == "--resolve-dir" {
		fmt.Println(canonical(os.Args[2]))
		return nil
	}
	a, e := loadApp()
	if e != nil {
		return e
	}
	if len(os.Args) == 2 && os.Args[1] == "--check-config" {
		if _, e = a.tlsConfig(""); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"config_valid": true, "revision": revision, "runtime_fingerprint": a.runtimeFingerprint()})
	}
	if e = os.MkdirAll(a.root, 0700); e != nil {
		return e
	}
	mode := ""
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	switch mode {
	case "--application-check":
		return a.applicationCheck()
	case "--follow-app":
		return a.startFollower()
	case "--watch-app":
		logFile(a.root, "lifecycle.log")
		return a.followApplication()
	case "--serve":
		logFile(a.root, "proxy.log")
		return a.serve()
	case "--supervise":
		if strings.EqualFold(canonical(filepath.Dir(a.exe)), a.root) {
			if e = a.initializeRelease(); e != nil {
				return e
			}
			p, e := a.start("--supervise")
			if e != nil {
				return e
			}
			p.close()
			return nil
		}
		logFile(a.root, "supervisor.log")
		return a.supervise()
	case "--ensure":
		return a.ensure()
	case "--upgrade":
		if len(os.Args) != 3 {
			return errors.New("--upgrade requires a candidate executable path")
		}
		return a.upgrade(os.Args[2])
	case "--activate":
		return a.activate(a.exe)
	case "--reconcile-active":
		r, e := a.readRecord(false)
		if e != nil {
			return e
		}
		p, e := a.ownedRecord(r)
		if e != nil {
			return e
		}
		p.close()
		return a.selectRelease(r.Script)
	case "--diagnostics-history":
		return a.diagnosticHistory()
	case "--diagnostics":
		return a.diagnosticStatus()
	case "--drain":
		seconds := 60
		if len(os.Args) > 2 {
			var e error
			seconds, e = strconv.Atoi(os.Args[2])
			if e != nil || seconds < 1 || seconds > 3600 {
				return errors.New("drain seconds must be 1..3600")
			}
		}
		return a.drain(seconds)
	case "--resume":
		r, e := a.readRecord(false)
		if e != nil {
			return e
		}
		_, e = a.controlCall(r, "POST", "/resume", nil)
		return e
	case "--stop":
		return a.stopAll()
	case "--status":
		s, routingErr := a.settings()
		settingsReadOK := routingErr == nil
		if routingErr == nil {
			routingErr = validateSettings(s, a.cfg.Port)
		}
		var systemProxy any
		if settingsReadOK {
			s.Server = redactProxyServer(s.Server)
			systemProxy = s
		}
		_, ae := os.Stat(a.cfg.Application)
		running := a.healthy() && a.entryReady()
		draining, workerStateReadOK := false, false
		if running {
			if r, e := a.readRecord(false); e == nil {
				if state, e := a.controlCall(r, "GET", "/status", nil); e == nil {
					draining, workerStateReadOK = state["draining"].(bool)
				}
			}
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"application_following": a.followerRunning(), "application_exit_grace_seconds": int(applicationGrace / time.Second), "bridge_running": running, "routing_config_valid": routingErr == nil, "ready": running && routingErr == nil && workerStateReadOK && !draining, "draining": draining, "worker_state_read_ok": workerStateReadOK, "application": a.cfg.Application, "application_exists": ae == nil, "port": a.cfg.Port, "system_proxy": systemProxy, "settings_read_ok": settingsReadOK, "version": marker, "revision": revision, "root": a.root, "idle_timeout_seconds": a.cfg.Idle, "max_connections": a.cfg.MaxConnections, "max_connections_per_destination": a.cfg.MaxPerDestination, "max_pending_connections": a.cfg.MaxPending, "admission_wait_seconds": a.cfg.QueueWait, "health_failures_before_restart": a.cfg.HealthFailures, "websocket_write_timeout_seconds": a.cfg.WSWrite})
	default:
		return a.launch(os.Args[1:])
	}
}

func main() {
	if e := run(); e != nil {
		log.Print(e)
		if len(os.Args) == 1 {
			syscall.NewLazyDLL("user32.dll").NewProc("MessageBoxW").Call(0, uintptr(unsafe.Pointer(wide(e.Error()))), uintptr(unsafe.Pointer(wide("Antigravity system proxy"))), 0x10)
		}
		os.Exit(1)
	}
}
