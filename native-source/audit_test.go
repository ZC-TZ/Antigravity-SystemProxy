//go:build windows

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestOfflineActivationRejectsLiveEntryAndRetiredWorker(t *testing.T) {
	for _, role := range []string{"entry", "retired"} {
		t.Run(role, func(t *testing.T) {
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			a := &app{root: t.TempDir(), exe: canonical(exe), cfg: Config{Port: 17891}}
			if _, err = a.publish(true, 0); err != nil {
				t.Fatal(err)
			}
			if role == "retired" {
				if err = os.Rename(a.statePath(true), filepath.Join(a.root, "worker-private-"+nonce()+".json")); err != nil {
					t.Fatal(err)
				}
			}
			source := filepath.Join(a.root, "candidate.exe")
			os.WriteFile(source, []byte("not executed"), 0600)
			ptr := []byte(`{"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)
			os.WriteFile(filepath.Join(a.root, "active-release.json"), ptr, 0600)
			if err = a.activate(source); err == nil {
				t.Fatal("offline activation allowed while owned background still alive")
			}
			after, _ := os.ReadFile(filepath.Join(a.root, "active-release.json"))
			if !bytes.Equal(after, ptr) {
				t.Fatal("active selection changed")
			}
		})
	}
}

func TestOfflineActivationRejectsEntryBeforeRecordPublished(t *testing.T) {
	a := &app{root: t.TempDir(), cfg: Config{Port: 17891}}
	h, exists, err := a.entryGuard()
	if err != nil || exists {
		t.Fatal(exists, err)
	}
	defer syscall.CloseHandle(h)
	if err = a.activate("unused.exe"); err == nil || !strings.Contains(err.Error(), "stopped entry") {
		t.Fatalf("startup guard ignored: %v", err)
	}
}

func TestControlJSONRejectsTrailingAndOversizedMessages(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"valid", `{"lease_seconds":3}`, true},
		{"whitespace", `{"lease_seconds":3}` + "\r\n ", true},
		{"second", `{"lease_seconds":3}{"lease_seconds":1}`, false},
		{"garbage", `{"lease_seconds":3}junk`, false},
		{"oversized_after_prefix", `{"lease_seconds":3}` + strings.Repeat(" ", 128), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/drain", strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			var input struct {
				Seconds int `json:"lease_seconds"`
			}
			err := decodeControlJSON(w, r, 64, &input)
			if (err == nil) != tc.valid {
				t.Fatal(err)
			}
			if !tc.valid {
				// This is the exact former parsing expression: it accepts the same
				// invalid input's first object, ignoring trailing data and the bound.
				if e := json.NewDecoder(io.LimitReader(strings.NewReader(tc.body), 64)).Decode(&input); e != nil {
					t.Fatal("legacy reproduction failed", e)
				}
			}
		})
	}
}

func TestControlCompletedBodyCanWaitForLongHandler(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input map[string]any
		if err := decodeControlJSON(w, r, 64, &input); err != nil {
			http.Error(w, "invalid", 400)
			return
		}
		time.Sleep(80 * time.Millisecond)
		io.WriteString(w, "confirmed")
	})
	srv := newControlServer(handler)
	srv.ReadTimeout = 30 * time.Millisecond
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	defer srv.Close()
	go srv.Serve(l)
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	resp, err := client.Post("http://"+l.Addr().String(), "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 || string(body) != "confirmed" {
		t.Fatal(resp.StatusCode, string(body), err)
	}
}

func TestControlSlowBodyHasBoundedRead(t *testing.T) {
	for _, authenticated := range []bool{true, false} {
		t.Run(fmt.Sprint(authenticated), func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !authenticated {
					http.Error(w, "forbidden", 403)
					return
				}
				var v map[string]any
				if json.NewDecoder(r.Body).Decode(&v) != nil {
					http.Error(w, "invalid body", 400)
					return
				}
				w.WriteHeader(204)
			})
			srv := newControlServer(handler)
			// Scale a real configured bound down for a fast network regression. Zero
			// remains zero, reproducing the old indefinitely blocked body read/drain.
			if srv.ReadTimeout > 0 {
				srv.ReadTimeout = 50 * time.Millisecond
			}
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			defer srv.Close()
			go srv.Serve(l)
			c, err := net.Dial("tcp", l.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(300 * time.Millisecond))
			io.WriteString(c, "POST /control HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n{")
			resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: "POST"})
			if err != nil {
				t.Fatalf("slow body retained control connection: %v", err)
			}
			resp.Body.Close()
			want := 400
			if !authenticated {
				want = 403
			}
			if resp.StatusCode != want {
				t.Fatal(resp.StatusCode)
			}
		})
	}
}

func TestPublishedRecordContainsPrivatePortImmediately(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	a := &app{root: t.TempDir(), exe: canonical(exe), workerID: nonce(), dataPort: 32123, cfg: Config{Port: 17891}}
	r, err := a.publish(false, 32124)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := a.readRecord(false)
	if err != nil || stored != r || stored.DataPort != 32123 {
		t.Fatalf("incomplete published identity: %+v %v", stored, err)
	}
	files, _ := filepath.Glob(filepath.Join(a.root, "*.tmp"))
	if len(files) != 0 {
		t.Fatal("temporary state leaked", files)
	}
}

func TestAdoptRejectsTrustedScriptWithUnrelatedExecutable(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	a := &app{root: t.TempDir(), exe: canonical(exe), cfg: Config{Port: 17891}}
	r, err := a.publish(false, 32124)
	if err != nil {
		t.Fatal(err)
	}
	r.Script = filepath.Join(a.root, nativeImage)
	if err = writeRecord(a.statePath(false), r); err != nil {
		t.Fatal(err)
	}
	if p, err := a.adopt(false); err == nil {
		p.close()
		t.Fatal("adopt accepted script/image identity mismatch")
	}
}

func TestStandaloneWorkerDoesNotSatisfyEntryReadiness(t *testing.T) {
	a := &app{root: t.TempDir(), cfg: Config{Port: 17891}}
	for _, port := range []int{0, 17891} {
		a.exe = filepath.Join(a.root, nativeImage)
		r := record{PID: os.Getpid(), Script: a.exe, Version: marker, ProxyPort: 17891, DataPort: port, Generation: nonce()}
		if err := writeRecord(a.statePath(false), r); err != nil {
			t.Fatal(err)
		}
		if a.entryReady() {
			t.Fatal("standalone worker treated as stable entry")
		}
	}
}
