package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type worker struct {
	cmd                      *exec.Cmd
	root                     string
	port, health, pid        int
	generation, revision     string
	handle                   syscall.Handle
	wait                     chan error
	log                      *os.File
	client                   *http.Client
	once                     sync.Once
	entryMode                bool
	entryPID, entryHealth    int
	entryGeneration          string
	entryHandle              syscall.Handle
	entryJob, launcherHandle syscall.Handle
	image                    string
	env                      []string
}

func startWorker(ctx context.Context, exe, upstream string) (w *worker, err error) {
	return startBridge(ctx, exe, upstream, "worker")
}

func startBridge(ctx context.Context, exe, upstream, mode string) (w *worker, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if mode != "worker" && mode != "entry" {
		return nil, errors.New("invalid bridge mode")
	}
	root, e := os.MkdirTemp("", "antigravity-benchmark-")
	if e != nil {
		return nil, e
	}
	w = &worker{root: root, wait: make(chan error, 1), client: &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}}
	w.entryMode = mode == "entry"
	created := w
	defer func() {
		if err != nil {
			created.close()
		}
	}()
	probe, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return nil, e
	}
	w.port = probe.Addr().(*net.TCPAddr).Port
	probe.Close()
	config, _ := json.Marshal(map[string]any{"port": w.port, "idle_timeout_seconds": 3600, "socket_write_metrics": ctx.Value(observeWritesKey{}) == true})
	if e = os.WriteFile(filepath.Join(root, "config.json"), config, 0600); e != nil {
		return nil, e
	}
	settings, _ := json.Marshal(map[string]any{"enabled": true, "server": upstream, "bypass": ""})
	if e = os.WriteFile(filepath.Join(root, "settings.json"), settings, 0600); e != nil {
		return nil, e
	}
	w.log, e = os.Create(filepath.Join(root, "worker.log"))
	if e != nil {
		return nil, e
	}
	w.image = exe
	args := "--serve"
	if w.entryMode {
		binary, err := os.ReadFile(exe)
		if err != nil {
			return nil, err
		}
		w.image = filepath.Join(root, "AntigravitySystemProxy.exe")
		if err = os.WriteFile(w.image, binary, 0700); err != nil {
			return nil, err
		}
		args = "--ensure"
	}
	w.cmd = exec.Command(w.image, args)
	w.cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	w.cmd.Stdout = w.log
	w.cmd.Stderr = w.log
	for _, pair := range os.Environ() {
		key := strings.ToUpper(strings.SplitN(pair, "=", 2)[0])
		if !strings.HasPrefix(key, "AG_SYSTEM_PROXY_") {
			w.cmd.Env = append(w.cmd.Env, pair)
		}
	}
	w.cmd.Env = append(w.cmd.Env, "AG_SYSTEM_PROXY_STATE_DIR="+root, fmt.Sprint("AG_SYSTEM_PROXY_PORT=", w.port), "AG_SYSTEM_PROXY_SETTINGS_FILE="+filepath.Join(root, "settings.json"))
	w.env = w.cmd.Env
	if w.entryMode {
		w.entryJob, w.launcherHandle, w.pid, e = launchInJob(w.cmd)
		if e != nil {
			return nil, e
		}
		go func() {
			defer close(created.wait)
			_, err := syscall.WaitForSingleObject(created.launcherHandle, syscall.INFINITE)
			var code uint32
			if err == nil {
				err = syscall.GetExitCodeProcess(created.launcherHandle, &code)
			}
			if err == nil && code != 0 {
				err = fmt.Errorf("isolated launcher exit %d", code)
			}
			created.wait <- err
		}()
	} else {
		if e = w.cmd.Start(); e != nil {
			return nil, e
		}
		w.pid = w.cmd.Process.Pid
		go func() { defer close(created.wait); created.wait <- created.cmd.Wait() }()
	}
	if w.entryMode {
		select {
		case err := <-w.wait:
			if err != nil {
				return nil, fmt.Errorf("isolated entry startup: %w", err)
			}
		case <-time.After(20 * time.Second):
			return nil, errors.New("isolated entry startup timed out")
		}
		// Let the bounded --ensure command finish before cleanup on cancellation,
		// so it cannot spawn a child after --stop has scanned our private records.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := w.attachEntry(); err != nil {
			return nil, err
		}
		state, err := w.status()
		if err != nil {
			return nil, err
		}
		w.revision, _ = state["revision"].(string)
		return w, nil
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var record struct {
			PID, HealthPort int
			Generation      string
		}
		data, e := os.ReadFile(filepath.Join(root, fmt.Sprintf("worker-%d.json", w.port)))
		if e == nil {
			var raw map[string]json.RawMessage
			if json.Unmarshal(data, &raw) == nil {
				json.Unmarshal(raw["pid"], &record.PID)
				json.Unmarshal(raw["health_port"], &record.HealthPort)
				json.Unmarshal(raw["generation"], &record.Generation)
			}
			if record.PID == w.pid && record.HealthPort > 0 {
				w.health = record.HealthPort
				w.generation = record.Generation
				if state, e := w.status(); e == nil {
					w.revision, _ = state["revision"].(string)
					w.handle, e = syscall.OpenProcess(0x0410, false, uint32(w.pid))
					if e != nil {
						return nil, e
					}
					return w, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w.wait:
			return nil, errors.New("isolated worker exited during startup")
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil, errors.New("isolated worker did not become healthy")
}
func (w *worker) endpoint() string { return fmt.Sprintf("http://127.0.0.1:%d", w.port) }
func (w *worker) status() (map[string]any, error) {
	response, e := w.client.Get(fmt.Sprintf("http://127.0.0.1:%d/status", w.health))
	if e != nil {
		return nil, e
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("X-Worker-Pid") != fmt.Sprint(w.pid) || response.Header.Get("X-Worker-Generation") != w.generation {
		return nil, errors.New("isolated worker identity changed")
	}
	var result map[string]any
	e = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result)
	if e == nil && w.entryMode {
		r, err := w.client.Get(fmt.Sprintf("http://127.0.0.1:%d/status", w.entryHealth))
		if err != nil {
			return nil, err
		}
		defer r.Body.Close()
		var entry map[string]any
		if r.StatusCode != 200 || r.Header.Get("X-Worker-Pid") != fmt.Sprint(w.entryPID) || r.Header.Get("X-Worker-Generation") != w.entryGeneration {
			return nil, errors.New("isolated entry identity changed")
		}
		if err = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&entry); err != nil {
			return nil, err
		}
		if entry["active_pid"] != float64(w.pid) {
			return nil, errors.New("isolated backend identity changed")
		}
		result["benchmark_entry"] = entry
	}
	return result, e
}
func (w *worker) close() {
	if w == nil {
		return
	}
	w.once.Do(func() {
		if w.entryMode && w.entryJob != 0 {
			// Use an independent cleanup context even after Ctrl+C. The native CLI
			// verifies creation times and image ownership in our private state root.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			stop := exec.CommandContext(ctx, w.image, "--stop")
			stop.Env = w.env
			stop.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
			_ = stop.Run()
			cancel()
			syscall.CloseHandle(w.entryJob)
			select {
			case <-w.wait:
			case <-time.After(5 * time.Second):
			}
			if w.launcherHandle != 0 {
				syscall.CloseHandle(w.launcherHandle)
			}
			for _, handle := range []syscall.Handle{w.entryHandle, w.handle} {
				if handle != 0 {
					_ = syscall.TerminateProcess(handle, 0)
					syscall.WaitForSingleObject(handle, 5000)
				}
			}
		}
		if w.entryHandle != 0 {
			syscall.CloseHandle(w.entryHandle)
		}
		if w.handle != 0 {
			syscall.CloseHandle(w.handle)
		}
		if w.cmd != nil && w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
			select {
			case <-w.wait:
			case <-time.After(5 * time.Second):
			}
		}
		if w.log != nil {
			w.log.Close()
		}
		if w.client != nil {
			w.client.CloseIdleConnections()
		}
		// Only known files inside the freshly-created private scratch directory.
		historyPath := filepath.Join(w.root, "diagnostics.jsonl")
		for _, suffix := range []string{"", ".1", ".2", ".3", ".lock"} {
			_ = os.Remove(historyPath + suffix)
		}
		for _, name := range []string{"config.json", "settings.json", "worker.log", "proxy.log", "proxy.log.1", fmt.Sprintf("worker-%d.json", w.port)} {
			_ = os.Remove(filepath.Join(w.root, name))
		}
		_ = os.Remove(w.root)
		if w.entryMode {
			// root came from MkdirTemp, never from config/user input. RemoveAll
			// removes directory links themselves, without following their targets.
			abs, err := filepath.Abs(w.root)
			if err == nil && filepath.Dir(abs) == filepath.Clean(os.TempDir()) && strings.HasPrefix(filepath.Base(abs), "antigravity-benchmark-") {
				_ = os.RemoveAll(abs)
			}
		}
	})
}

type benchmarkRecord struct {
	PID        int    `json:"pid"`
	Created    uint64 `json:"created"`
	Executable string `json:"executable"`
	HealthPort int    `json:"health_port"`
	Generation string `json:"generation"`
}

func (w *worker) attachEntry() error {
	for _, super := range []bool{true, false} {
		prefix := "worker"
		if super {
			prefix = "supervisor"
		}
		data, err := os.ReadFile(filepath.Join(w.root, fmt.Sprintf("%s-%d.json", prefix, w.port)))
		if err != nil {
			return err
		}
		var r benchmarkRecord
		if err = json.Unmarshal(data, &r); err != nil {
			return err
		}
		image, err := filepath.Abs(r.Executable)
		rel, relErr := filepath.Rel(filepath.Join(w.root, "releases"), image)
		if err != nil || relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
			return errors.New("benchmark process image outside isolated releases")
		}
		h, err := syscall.OpenProcess(0x100411, false, uint32(r.PID))
		if err != nil {
			return err
		}
		var created, exited, k, u syscall.Filetime
		ok, _, _ := processTimes.Call(uintptr(h), uintptr(unsafe.Pointer(&created)), uintptr(unsafe.Pointer(&exited)), uintptr(unsafe.Pointer(&k)), uintptr(unsafe.Pointer(&u)))
		var name [32768]uint16
		n := uint32(len(name))
		imageOK, _, _ := kernel.NewProc("QueryFullProcessImageNameW").Call(uintptr(h), 0, uintptr(unsafe.Pointer(&name[0])), uintptr(unsafe.Pointer(&n)))
		if ok == 0 || imageOK == 0 || uint64(created.HighDateTime)<<32|uint64(created.LowDateTime) != r.Created || !strings.EqualFold(syscall.UTF16ToString(name[:n]), image) {
			syscall.CloseHandle(h)
			return errors.New("benchmark process ownership mismatch")
		}
		if super {
			w.entryPID, w.entryHealth, w.entryGeneration, w.entryHandle = r.PID, r.HealthPort, r.Generation, h
		} else {
			w.pid, w.health, w.generation, w.handle = r.PID, r.HealthPort, r.Generation, h
		}
	}
	return nil
}

func (w *worker) resources() (processSample, error) {
	s, err := resourceSample(w.handle)
	if err != nil || !w.entryMode {
		return s, err
	}
	e, err := resourceSample(w.entryHandle)
	if err != nil {
		return s, err
	}
	s.CPUSeconds += e.CPUSeconds
	s.WorkingSet += e.WorkingSet
	s.Private += e.Private
	s.PeakWorkingSet += e.PeakWorkingSet
	s.Handles += e.Handles
	return s, nil
}

type memoryCounters struct {
	CB, PageFaults                                                                        uint32
	PeakWS, WS, PeakPaged, Paged, PeakNonPaged, NonPaged, Pagefile, PeakPagefile, Private uintptr
}

var psapi = syscall.NewLazyDLL("psapi.dll")
var processMemory = psapi.NewProc("GetProcessMemoryInfo")
var kernel = syscall.NewLazyDLL("kernel32.dll")
var processTimes = kernel.NewProc("GetProcessTimes")
var processHandles = kernel.NewProc("GetProcessHandleCount")

type processSample struct {
	CPUSeconds                          float64 `json:"cpu_seconds"`
	WorkingSet, Private, PeakWorkingSet uint64
	Handles                             uint32
}

func resourceSample(handle syscall.Handle) (s processSample, e error) {
	m := memoryCounters{}
	m.CB = uint32(unsafe.Sizeof(m))
	ok, _, err := processMemory.Call(uintptr(handle), uintptr(unsafe.Pointer(&m)), uintptr(m.CB))
	if ok == 0 {
		return s, err
	}
	var created, exited, k, u syscall.Filetime
	ok, _, err = processTimes.Call(uintptr(handle), uintptr(unsafe.Pointer(&created)), uintptr(unsafe.Pointer(&exited)), uintptr(unsafe.Pointer(&k)), uintptr(unsafe.Pointer(&u)))
	if ok == 0 {
		return s, err
	}
	ok, _, err = processHandles.Call(uintptr(handle), uintptr(unsafe.Pointer(&s.Handles)))
	if ok == 0 {
		return s, err
	}
	s.CPUSeconds = float64((uint64(k.HighDateTime)<<32|uint64(k.LowDateTime))+(uint64(u.HighDateTime)<<32|uint64(u.LowDateTime))) / 1e7
	s.WorkingSet = uint64(m.WS)
	s.Private = uint64(m.Private)
	s.PeakWorkingSet = uint64(m.PeakWS)
	return s, nil
}

func currentProcessHandle() syscall.Handle { return syscall.Handle(^uintptr(0)) }

var performanceCounter = kernel.NewProc("QueryPerformanceCounter")
var performanceFrequency = func() int64 {
	var value int64
	ok, _, _ := kernel.NewProc("QueryPerformanceFrequency").Call(uintptr(unsafe.Pointer(&value)))
	if ok == 0 || value <= 0 {
		panic("high resolution clock unavailable")
	}
	return value
}()

func tickNS() int64 {
	var value int64
	ok, _, _ := performanceCounter.Call(uintptr(unsafe.Pointer(&value)))
	if ok == 0 {
		panic("high resolution clock failed")
	}
	return value/performanceFrequency*1e9 + (value%performanceFrequency)*1e9/performanceFrequency
}
