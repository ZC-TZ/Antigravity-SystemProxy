//go:build windows

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func historyTestApp(t *testing.T) *app {
	t.Helper()
	return &app{root: t.TempDir(), cfg: Config{Port: 17891}}
}
func historyTestEvent(n int) historyEvent {
	return historyEvent{Schema: 1, ProxyPort: 17891, Kind: "test", Role: "test", Session: "test-session", Sequence: uint64(n), Revision: revision, TimeUTC: time.Now().UTC()}
}
func TestHistoryPersistsAcrossWritersAndCLIReader(t *testing.T) {
	a := historyTestApp(t)
	for _, kind := range []string{"before_restart", "after_restart"} {
		h := newHistory(a, "test")
		h.emit(historyEvent{Kind: kind})
		h.close()
	}
	exported, e := a.readHistory()
	if e != nil || len(exported.Events) != 2 || exported.Events[0].Kind != "before_restart" || exported.Events[1].Kind != "after_restart" {
		t.Fatalf("history lost %+v %v", exported, e)
	}
}
func TestHistoryRotationBoundsAndNewestOrder(t *testing.T) {
	a := historyTestApp(t)
	h := &historyJournal{a: a, segmentBytes: 1024, segments: 4}
	for i := 1; i <= 100; i++ {
		if e := h.write(historyTestEvent(i)); e != nil {
			t.Fatal(e)
		}
	}
	for _, suffix := range []string{"", ".1", ".2", ".3"} {
		info, e := os.Stat(a.historyPath() + suffix)
		if e != nil || info.Size() > 1024 {
			t.Fatal("unbounded rotation", info, e)
		}
	}
	if _, e := os.Stat(a.historyPath() + ".4"); !os.IsNotExist(e) {
		t.Fatal("extra segment")
	}
	exported, e := a.readHistory()
	if e != nil || len(exported.Events) < 4 || exported.Events[len(exported.Events)-1].Sequence != 100 {
		t.Fatal(exported, e)
	}
	for i := 1; i < len(exported.Events); i++ {
		if exported.Events[i].Sequence != exported.Events[i-1].Sequence+1 {
			t.Fatal("rotation lost/reordered retained events")
		}
	}
}
func TestHistoryPartialTailRecoveryAndOversizedLines(t *testing.T) {
	a := historyTestApp(t)
	h := &historyJournal{a: a, segmentBytes: historySegmentBytes, segments: historySegments}
	first, _ := json.Marshal(historyTestEvent(1))
	if e := os.WriteFile(a.historyPath(), append(append(first, '\n'), []byte(`{"schema":1,"kind":"interrupted`)...), 0600); e != nil {
		t.Fatal(e)
	}
	before, e := a.readHistory()
	if e != nil || len(before.Events) != 1 || before.Warnings["partial_tail"] != 1 {
		t.Fatal(before, e)
	}
	if e = h.write(historyTestEvent(2)); e != nil {
		t.Fatal(e)
	}
	after, e := a.readHistory()
	if e != nil || len(after.Events) != 2 || after.Warnings["invalid_record"] != 1 {
		t.Fatal(after, e)
	}
	second, _ := json.Marshal(historyTestEvent(3))
	data := append(append(append(first, '\n'), bytes.Repeat([]byte{'x'}, historyRecordBytes+100)...), '\n')
	data = append(append(data, second...), '\n')
	os.WriteFile(a.historyPath(), data, 0600)
	after, e = a.readHistory()
	if e != nil || len(after.Events) != 2 || after.Warnings["oversized_record"] != 1 {
		t.Fatal(after, e)
	}
}
func TestHistoryConcurrentWritersSerializeWholeRecords(t *testing.T) {
	a := historyTestApp(t)
	var wg sync.WaitGroup
	for writer := 0; writer < 3; writer++ {
		writer := writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			h := &historyJournal{a: a, segmentBytes: historySegmentBytes, segments: historySegments}
			for i := 0; i < 30; i++ {
				event := historyTestEvent(i + 1)
				event.Session = fmt.Sprint("writer-", writer)
				var e error
				for retry := 0; retry < 10; retry++ {
					if e = h.write(event); e == nil {
						break
					}
				}
				if e != nil {
					t.Error(e)
					return
				}
			}
		}()
	}
	wg.Wait()
	exported, e := a.readHistory()
	if e != nil || len(exported.Events) != 90 || len(exported.Warnings) != 0 {
		t.Fatal("interleaved/truncated writes", exported, e)
	}
}
func TestHistoryQueueBoundedAndLockFailureRecovers(t *testing.T) {
	a := historyTestApp(t)
	lock, e := historyLock(a.historyPath())
	if e != nil {
		t.Fatal(e)
	}
	h := newHistory(a, "test")
	start := time.Now()
	for i := 0; i < 1000; i++ {
		h.emit(historyEvent{Kind: "burst"})
	}
	if time.Since(start) > time.Second || len(h.queue) > 16 || h.dropped.Load() == 0 {
		t.Fatal("history emission blocked or grew unbounded")
	}
	deadline := time.Now().Add(time.Second)
	for h.failures.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	syscall.CloseHandle(lock)
	if h.failures.Load() == 0 {
		t.Fatal("storage lock failure not reported")
	}
	h.emit(historyEvent{Kind: "recovered"})
	h.close()
	if h.lastWrite.Load() == 0 {
		t.Fatal("writer did not recover")
	}
}
func TestHistoryStorageFailureDoesNotBreakForwarding(t *testing.T) {
	c, p, _ := fixture(t, Settings{})
	os.Mkdir(c.a.historyPath(), 0700)
	h := newHistory(c.a, "test")
	defer h.close()
	c.history = h
	h.emit(historyEvent{Kind: "snapshot"})
	deadline := time.Now().Add(time.Second)
	for h.failures.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.failures.Load() == 0 {
		t.Fatal("write failure hidden")
	}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	defer origin.Close()
	u, _ := url.Parse(p.URL)
	tr := &http.Transport{Proxy: http.ProxyURL(u)}
	defer tr.CloseIdleConnections()
	r, e := (&http.Client{Transport: tr, Timeout: time.Second}).Get(origin.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	if string(b) != "ok" {
		t.Fatal("diagnostic storage failure broke forwarding")
	}
}
func TestHistoryPeriodicSnapshotFaultSummaryAndPrivacy(t *testing.T) {
	c, _, _ := fixture(t, Settings{true, "http://secret-user:secret-password@proxy.invalid:7890", ""})
	h := newHistory(c.a, "test")
	c.history = h
	c.metrics.dialFailures.Add(1)
	stop := h.periodic(c, 123, 5*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	stop()
	h.close()
	exported, e := c.a.readHistory()
	if e != nil {
		t.Fatal(e)
	}
	snapshots, faults := 0, 0
	for _, event := range exported.Events {
		if event.Kind == "snapshot" {
			snapshots++
			if event.Snapshot == nil || event.Snapshot.Diagnostics["http_connection_acquire"] == nil {
				t.Fatal("snapshot omitted diagnostics")
			}
		}
		if event.Kind == "fault_summary" {
			faults++
			if event.Faults.Dial != 1 {
				t.Fatal(event)
			}
		}
	}
	data, _ := json.Marshal(exported)
	if snapshots == 0 || faults != 1 || bytes.Contains(data, []byte("secret")) || bytes.Contains(data, []byte("proxy.invalid")) || bytes.Contains(data, []byte(c.a.root)) {
		t.Fatal("snapshot/fault/privacy error", string(data))
	}
}
func TestHistoryReadLimitAndSchemaWarnings(t *testing.T) {
	a := historyTestApp(t)
	h := &historyJournal{a: a, segmentBytes: historySegmentBytes, segments: historySegments}
	for i := 1; i <= historyReadLimit+10; i++ {
		if e := h.write(historyTestEvent(i)); e != nil {
			t.Fatal(e)
		}
	}
	future := historyTestEvent(999)
	future.Schema = 999
	if e := h.write(future); e != nil {
		t.Fatal(e)
	}
	exported, e := a.readHistory()
	if e != nil || len(exported.Events) != historyReadLimit || exported.Events[0].Sequence != 11 || exported.Events[historyReadLimit-1].Sequence != 210 || exported.Warnings["unknown_schema"] != 1 {
		t.Fatal(exported, e)
	}
}
func TestHistoryUnobservedPreviousWorkerIsNotCalledCrash(t *testing.T) {
	a := historyTestApp(t)
	storage := &historyJournal{a: a, segmentBytes: historySegmentBytes, segments: historySegments}
	previous := historyTestEvent(1)
	previous.Role = "worker"
	previous.Kind = "snapshot"
	previous.Session = "old-session"
	if e := storage.write(previous); e != nil {
		t.Fatal(e)
	}
	h := newHistory(a, "worker")
	h.emit(historyEvent{Kind: "worker_started"})
	h.flush(2 * time.Second)
	h.close()
	exported, e := a.readHistory()
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, event := range exported.Events {
		if event.Kind == "previous_worker_end_unobserved" {
			found = event.Subject == "old-session" && event.Reason == "no_terminal_record"
		}
	}
	if !found {
		t.Fatal("unobserved termination missing", exported)
	}
}
func TestHistoryKnownStopSuppressesUnobservedMarker(t *testing.T) {
	a := historyTestApp(t)
	storage := &historyJournal{a: a, segmentBytes: historySegmentBytes, segments: historySegments}
	previous := historyTestEvent(1)
	previous.Role = "worker"
	previous.Kind = "snapshot"
	previous.Session = "old-session"
	storage.write(previous)
	terminal := historyTestEvent(2)
	terminal.Kind = "process_stopped"
	terminal.Subject = "old-session"
	storage.write(terminal)
	h := newHistory(a, "worker")
	h.emit(historyEvent{Kind: "worker_started"})
	h.flush(2 * time.Second)
	h.close()
	exported, e := a.readHistory()
	if e != nil {
		t.Fatal(e)
	}
	for _, event := range exported.Events {
		if event.Kind == "previous_worker_end_unobserved" {
			t.Fatal("known stop incorrectly marked unobserved")
		}
	}
}

func TestHistoryLiveOldGenerationDoesNotLookLikeMissingExit(t *testing.T) {
	a := historyTestApp(t)
	a.exe, _ = os.Executable()
	p, e := openOwned(os.Getpid())
	if e != nil {
		t.Fatal(e)
	}
	session := processSession(p.pid, p.created)
	p.close()
	storage := &historyJournal{a: a, segmentBytes: historySegmentBytes, segments: historySegments}
	event := historyTestEvent(1)
	event.Role = "worker"
	event.Kind = "snapshot"
	event.Session = session
	if e = storage.write(event); e != nil {
		t.Fatal(e)
	}
	h := newHistory(a, "test")
	h.session = "next-generation"
	h.notePrevious()
	h.flush(time.Second)
	h.close()
	exported, e := a.readHistory()
	if e != nil {
		t.Fatal(e)
	}
	for _, event := range exported.Events {
		if event.Kind == "previous_worker_end_unobserved" {
			t.Fatal("live old generation marked ended", event)
		}
	}
}
func TestHistoryEmptyArchiveIsReadable(t *testing.T) {
	a := historyTestApp(t)
	result, e := a.readHistory()
	if e != nil || len(result.Events) != 0 || len(result.Warnings) != 0 {
		t.Fatal(result, e)
	}
	if _, e = os.Stat(filepath.Join(a.root, "diagnostics.jsonl")); !os.IsNotExist(e) {
		t.Fatal("reader created fake history")
	}
}

func TestHistoryPortChangesShareBoundedArchive(t *testing.T) {
	a := historyTestApp(t)
	other := &app{root: a.root, cfg: Config{Port: 17892}}
	for _, instance := range []*app{a, other} {
		h := newHistory(instance, "test")
		h.emit(historyEvent{Kind: "port_change"})
		h.close()
	}
	events, e := other.readHistory()
	if e != nil || len(events.Events) != 2 || events.Events[0].ProxyPort != 17891 || events.Events[1].ProxyPort != 17892 || a.historyPath() != other.historyPath() {
		t.Fatal("port changes escaped shared archive", events, e)
	}
}
