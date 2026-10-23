//go:build windows

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const marker = "Antigravity-SystemProxy-v5"

const revision = "5.28"

const healthPath = "/__antigravity_system_proxy_health__"

type Config struct {
	SocketWriteMetrics bool    `json:"socket_write_metrics,omitempty"`
	MaxPerDestination  int     `json:"max_connections_per_destination,omitempty"`
	Application        string  `json:"application"`
	Port               int     `json:"port"`
	Idle               float64 `json:"idle_timeout_seconds"`
	ProxyCA            string  `json:"proxy_ca_file,omitempty"`
	MaxConnections     int     `json:"max_connections"`
	MaxPending         int     `json:"max_pending_connections"`
	QueueWait          float64 `json:"admission_wait_seconds"`
	HealthFailures     int     `json:"health_failures_before_restart"`
	WSWrite            float64 `json:"websocket_write_timeout_seconds"`
}

type Settings struct {
	Enabled bool   `json:"enabled"`
	Server  string `json:"server"`
	Bypass  string `json:"bypass"`
}

type app struct {
	tlsMu                   sync.Mutex
	tlsState                *proxyTLSState
	root, exe, settingsFile string
	workerID                string
	dataPort, controlPort   int
	cfg                     Config
}

func loadApp() (*app, error) {
	exe, e := os.Executable()
	if e != nil {
		return nil, e
	}
	exe = canonical(exe)
	root := os.Getenv("AG_SYSTEM_PROXY_STATE_DIR")
	if root == "" {
		root = filepath.Dir(exe)
	}
	root = canonical(root)
	a := &app{root: root, exe: exe, settingsFile: os.Getenv("AG_SYSTEM_PROXY_SETTINGS_FILE"), cfg: Config{Port: 17891, Idle: 3600, MaxConnections: 128, MaxPending: 64, QueueWait: 2, HealthFailures: 5, WSWrite: 60}}
	a.workerID = os.Getenv("AG_SYSTEM_PROXY_WORKER_ID")
	if a.workerID != "" && (len(a.workerID) != 32 || strings.Trim(a.workerID, "0123456789abcdef") != "") {
		return nil, errors.New("invalid private worker identity")
	}
	data, e := os.ReadFile(filepath.Join(root, "config.json"))
	if e == nil {
		if e = json.Unmarshal(bytes.TrimPrefix(data, []byte{239, 187, 191}), &a.cfg); e != nil {
			return nil, fmt.Errorf("invalid config.json: %w", e)
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	if p := os.Getenv("AG_SYSTEM_PROXY_PORT"); p != "" {
		a.cfg.Port, e = strconv.Atoi(p)
		if e != nil {
			return nil, e
		}
	}
	if a.cfg.Port < 1 || a.cfg.Port > 65535 || a.cfg.Idle < 1 || a.cfg.Idle > 86400 {
		return nil, errors.New("invalid port or idle timeout")
	}
	if a.cfg.MaxConnections < 1 || a.cfg.MaxConnections > 2048 || a.cfg.MaxPending < 1 || a.cfg.MaxPending > 512 || a.cfg.QueueWait < 0.05 || a.cfg.QueueWait > 10 || a.cfg.HealthFailures < 3 || a.cfg.HealthFailures > 20 {
		return nil, errors.New("invalid capacity, admission timeout or health threshold")
	}
	if a.cfg.MaxPerDestination < 0 || a.cfg.MaxPerDestination > a.cfg.MaxConnections {
		return nil, errors.New("invalid per-destination connection limit")
	}
	if a.cfg.WSWrite < 1 || a.cfg.WSWrite > 3600 {
		return nil, errors.New("invalid WebSocket write timeout")
	}
	if a.cfg.Application == "" {
		a.cfg.Application = filepath.Join(os.Getenv("LOCALAPPDATA"), "Programs", "antigravity", "Antigravity.exe")
	}
	if a.cfg.ProxyCA != "" && !filepath.IsAbs(a.cfg.ProxyCA) {
		a.cfg.ProxyCA = filepath.Join(root, a.cfg.ProxyCA)
	}
	return a, nil
}

func (a *app) settings() (Settings, error) {
	if a.settingsFile != "" {
		var s Settings
		data, e := os.ReadFile(a.settingsFile)
		if e != nil {
			return s, e
		}
		e = json.Unmarshal(data, &s)
		return s, e
	}
	return windowsSettings(regEnabled, regString)
}

func (a *app) idle() time.Duration { return time.Duration(a.cfg.Idle * float64(time.Second)) }

func (a *app) websocketWriteTimeout() time.Duration {
	seconds := a.cfg.WSWrite
	if seconds == 0 {
		seconds = 60
	}
	return min(a.idle(), time.Duration(seconds*float64(time.Second)))
}

func (a *app) statePath(super bool) string {
	if !super && a.workerID != "" {
		return filepath.Join(a.root, "worker-private-"+a.workerID+".json")
	}
	prefix := "worker"
	if super {
		prefix = "supervisor"
	}
	return filepath.Join(a.root, fmt.Sprintf("%s-%d.json", prefix, a.cfg.Port))
}
