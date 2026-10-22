//go:build windows

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const historySegmentBytes int64 = 4 << 20
const historySegments = 4
const historyRecordBytes = 64 << 10
const historyInterval = 30 * time.Second
const historyReadLimit = 200

// History contains only typed resource/counter snapshots and fixed reason codes.
// It deliberately never accepts an error string, URL, settings, or auth token.
type historyEvent struct {
	ProxyPort int                   `json:"proxy_port"`
	Schema    int                   `json:"schema"`
	TimeUTC   time.Time             `json:"time_utc"`
	Revision  string                `json:"revision"`
	Role      string                `json:"role"`
	Session   string                `json:"session"`
	Sequence  uint64                `json:"sequence"`
	UptimeMS  int64                 `json:"uptime_ms"`
	Kind      string                `json:"kind"`
	Reason    string                `json:"reason,omitempty"`
	Subject   string                `json:"subject_session,omitempty"`
	Failures  int                   `json:"consecutive_failures,omitempty"`
	ExitCode  *uint32               `json:"exit_code,omitempty"`
	ErrorCode uint32                `json:"socket_error_code,omitempty"`
	Snapshot  *workerStatusSnapshot `json:"snapshot,omitempty"`
	Faults    *historyFaults        `json:"fault_summary,omitempty"`
}
type historyFaults struct {
	Dial          int64 `json:"dial_failures"`
	HTTP          int64 `json:"http_forward_failures"`
	Auth          int64 `json:"proxy_auth_failures"`
	AcquireEnded  int64 `json:"acquire_ended_without_connection"`
	QueueTimeout  int64 `json:"queue_timeouts"`
	QueueRejected int64 `json:"queue_rejections"`
	UpstreamRead  int64 `json:"upstream_read_errors"`
	UpstreamWrite int64 `json:"upstream_write_errors"`
	Idle          int64 `json:"idle_timeouts"`
	WriteTimeout  int64 `json:"write_timeouts"`
}

func (f historyFaults) changed(old historyFaults) bool { return f != old }

type historyItem struct {
	event   historyEvent
	barrier chan struct{}
}
type historyJournal struct {
	a                            *app
	role, session                string
	started                      time.Time
	mu                           sync.Mutex
	closing                      bool
	sequence                     uint64
	queue                        chan historyItem
	done                         chan struct{}
	failures, dropped, lastWrite atomic.Int64
	segmentBytes                 int64
	segments                     int
}

func processSession(pid int, created uint64) string { return fmt.Sprintf("%d:%x", pid, created) }
func newHistory(a *app, role string) *historyJournal {
	session := fmt.Sprintf("%d:unverified-%x", os.Getpid(), time.Now().UnixNano())
	if p, e := openOwned(os.Getpid()); e == nil {
		session = processSession(p.pid, p.created)
		p.close()
	}
	h := &historyJournal{a: a, role: role, session: session, started: time.Now(), queue: make(chan historyItem, 16), done: make(chan struct{}), segmentBytes: historySegmentBytes, segments: historySegments}
	go h.run()
	return h
}
func (h *historyJournal) emit(e historyEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closing {
		return
	}
	h.sequence++
	e.ProxyPort = h.a.cfg.Port
	e.Schema = 1
	e.TimeUTC = time.Now().UTC()
	e.Revision = revision
	e.Role = h.role
	e.Session = h.session
	e.Sequence = h.sequence
	e.UptimeMS = time.Since(h.started).Milliseconds()
	select {
	case h.queue <- historyItem{event: e}:
	default:
		h.dropped.Add(1)
	}
}
func (h *historyJournal) flush(timeout time.Duration) bool {
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		return false
	}
	done := make(chan struct{})
	select {
	case h.queue <- historyItem{barrier: done}:
	default:
		h.mu.Unlock()
		return false
	}
	h.mu.Unlock()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
func (h *historyJournal) close() {
	h.mu.Lock()
	if !h.closing {
		h.closing = true
		close(h.queue)
	}
	h.mu.Unlock()
	select {
	case <-h.done:
	case <-time.After(500 * time.Millisecond):
	}
}
func (h *historyJournal) run() {
	defer close(h.done)
	if h.role == "worker" {
		h.notePrevious()
	}
	for item := range h.queue {
		if item.barrier != nil {
			close(item.barrier)
			continue
		}
		if e := h.write(item.event); e != nil {
			if h.failures.Add(1) == 1 {
				log.Print("Diagnostic history write failed; forwarding continues")
			}
		} else {
			h.lastWrite.Store(time.Now().UnixNano())
		}
	}
}
func (h *historyJournal) health() map[string]any {
	var last any
	if n := h.lastWrite.Load(); n > 0 {
		last = time.Unix(0, n).UTC()
	}
	return map[string]any{"write_failures": h.failures.Load(), "queue_dropped": h.dropped.Load(), "pending": len(h.queue), "last_write_utc": last, "snapshot_interval_seconds": historyInterval.Seconds(), "segment_bytes": h.segmentBytes, "segments": h.segments, "scope": "current state directory across proxy ports; typed aggregate observations only"}
}
func (a *app) historyPath() string {
	return filepath.Join(a.root, "diagnostics.jsonl")
}

var lockFileEx = kernel.NewProc("LockFileEx")
var unlockFileEx = kernel.NewProc("UnlockFileEx")

func historyUnlock(handle syscall.Handle) {
	var overlapped syscall.Overlapped
	unlockFileEx.Call(uintptr(handle), 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	syscall.CloseHandle(handle)
}

func historyLock(path string) (syscall.Handle, error) {
	handle, e := syscall.CreateFile(wide(path+".lock"), syscall.GENERIC_READ|syscall.GENERIC_WRITE, 7, nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if e != nil {
		return 0, e
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for {
		var overlapped syscall.Overlapped
		ok, _, err := lockFileEx.Call(uintptr(handle), 3, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
		if ok != 0 {
			return handle, nil
		}
		if !errors.Is(err, syscall.Errno(33)) || time.Now().After(deadline) {
			syscall.CloseHandle(handle)
			return 0, errors.New("history lock unavailable")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Closing a handle releases its byte-range lock even when a process was killed.
// Unlike a named mutex, the lock is not owned by an OS thread/goroutine.
func (h *historyJournal) write(e historyEvent) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > historyRecordBytes || int64(len(data)) > h.segmentBytes {
		return errors.New("history event exceeds bound")
	}
	path := h.a.historyPath()
	lock, err := historyLock(path)
	if err != nil {
		return err
	}
	defer historyUnlock(lock)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}
	size := info.Size()
	separator := false
	if size > 0 {
		var last [1]byte
		if _, err = file.ReadAt(last[:], size-1); err != nil {
			file.Close()
			return err
		}
		separator = last[0] != '\n'
	}
	extra := int64(0)
	if separator {
		extra = 1
	}
	if size+extra+int64(len(data)) > h.segmentBytes {
		file.Close()
		if err = os.Remove(path + "." + strconv.Itoa(h.segments-1)); err != nil && !os.IsNotExist(err) {
			return err
		}
		for i := h.segments - 2; i >= 0; i-- {
			from := path
			if i > 0 {
				from += "." + strconv.Itoa(i)
			}
			if err = os.Rename(from, path+"."+strconv.Itoa(i+1)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		file, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		separator = false
	}
	defer file.Close()
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	// Separate a killed writer's partial last line from the next complete event.
	// Readers skip the damaged record; earlier complete records are preserved.
	if separator {
		if _, err = file.Write([]byte{'\n'}); err != nil {
			return err
		}
	}
	if _, err = file.Write(data); err != nil {
		return err
	}
	return file.Sync()
}

type historyExport struct {
	Schema       int            `json:"schema"`
	Events       []historyEvent `json:"events"`
	Warnings     map[string]int `json:"read_warnings"`
	SegmentBytes int64          `json:"segment_bytes"`
	Segments     int            `json:"segments"`
	Limit        int            `json:"event_limit"`
}

func (a *app) readHistory() (historyExport, error) {
	result := historyExport{Schema: 1, Events: []historyEvent{}, Warnings: map[string]int{}, SegmentBytes: historySegmentBytes, Segments: historySegments, Limit: historyReadLimit}
	lock, e := historyLock(a.historyPath())
	if e != nil {
		return result, e
	}

	locked := true
	defer func() {
		if locked {
			historyUnlock(lock)
		}
	}()
	var segments [][]byte
	for i := historySegments - 1; i >= 0; i-- {
		path := a.historyPath()
		if i > 0 {
			path += "." + strconv.Itoa(i)
		}
		file, e := os.Open(path)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			result.Warnings["unreadable_segment"]++
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(file, historySegmentBytes+1))
		file.Close()
		if readErr != nil {
			result.Warnings["segment_read_error"]++
		}
		if int64(len(data)) > historySegmentBytes {
			result.Warnings["oversized_segment"]++
			data = data[:historySegmentBytes]
		}
		segments = append(segments, data)
	}
	historyUnlock(lock)
	locked = false
	// JSON decoding and output never hold the cross-process writer lock.
	for _, data := range segments {
		reader := bufio.NewReaderSize(bytes.NewReader(data), historyRecordBytes)
		oversized := false
		for {
			line, err := reader.ReadSlice('\n')
			if errors.Is(err, bufio.ErrBufferFull) {
				oversized = true
				continue
			}
			if oversized {
				result.Warnings["oversized_record"]++
				oversized = false
			} else if len(line) > 0 {
				if err == io.EOF && line[len(line)-1] != '\n' {
					result.Warnings["partial_tail"]++
				} else {
					var event historyEvent
					if json.Unmarshal(bytes.TrimSpace(line), &event) != nil {
						result.Warnings["invalid_record"]++
					} else if event.Schema != 1 {
						result.Warnings["unknown_schema"]++
					} else {
						if len(result.Events) == historyReadLimit {
							copy(result.Events, result.Events[1:])
							result.Events = result.Events[:historyReadLimit-1]
						}
						result.Events = append(result.Events, event)
					}
				}
			}
			if err != nil {
				if err != io.EOF {
					result.Warnings["segment_read_error"]++
				}
				break
			}
		}
	}

	return result, nil
}
func (a *app) diagnosticHistory() error {
	result, e := a.readHistory()
	if e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
func (h *historyJournal) notePrevious() {
	// This is evidence of a missing terminal record, not proof of a crash.
	exported, e := h.a.readHistory()
	if e != nil {
		return
	}
	terminal := map[string]bool{}
	for i := len(exported.Events) - 1; i >= 0; i-- {
		event := exported.Events[i]
		if event.ProxyPort != h.a.cfg.Port {
			continue
		}
		if event.Kind == "worker_exit" || event.Kind == "worker_exit_observed" || event.Kind == "process_stopped" || event.Kind == "retired_worker_stopped" {
			subject := event.Subject
			if subject == "" {
				subject = event.Session
			}
			terminal[subject] = true
		}
		if event.Role == "worker" && (event.Kind == "worker_starting" || event.Kind == "worker_started" || event.Kind == "snapshot") {
			if event.Session != h.session && !terminal[event.Session] && !h.a.liveSession(event.Session) {
				h.emit(historyEvent{Kind: "previous_worker_end_unobserved", Reason: "no_terminal_record", Subject: event.Session})
			}
			return
		}
	}
}

func (a *app) liveSession(session string) bool {
	pidText, createdText, ok := strings.Cut(session, ":")
	if !ok {
		return false
	}
	pid, e := strconv.Atoi(pidText)
	if e != nil {
		return false
	}
	created, e := strconv.ParseUint(createdText, 16, 64)
	if e != nil {
		return false
	}
	p, e := openOwned(pid)
	if e != nil {
		return false
	}
	defer p.close()
	return p.created == created && p.alive() && a.trustedImage(p.image)
}
func (h *historyJournal) periodic(core *core, pid int, interval time.Duration) func() {
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		previous := historyFaults{}
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				snapshot := core.statusSnapshot(pid)
				h.emit(historyEvent{Kind: "snapshot", Snapshot: &snapshot})
				current := core.historyFaults()
				if current.changed(previous) {
					value := current
					h.emit(historyEvent{Kind: "fault_summary", Reason: "counter_change_not_necessarily_fault", Faults: &value})
				}
				previous = current
			}
		}
	}()
	return func() { close(stop); <-done }
}
func (p *process) exitCode() *uint32 {
	var value uint32
	if e := syscall.GetExitCodeProcess(p.handle, &value); e != nil {
		return nil
	}
	return &value
}
