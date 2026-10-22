//go:build windows

package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testTCP(t *testing.T) *net.TCPListener {
	t.Helper()
	l, e := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func TestEntryPreservesUploadHalfCloseAndDelayedConfirmation(t *testing.T) {
	origin := testTCP(t)
	front := testTCP(t)
	w := &entryWorker{record: record{DataPort: origin.Addr().(*net.TCPAddr).Port}}
	b := &entry{active: w}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, e := front.AcceptTCP()
		if e == nil {
			b.forward(c)
		}
	}()
	body := bytes.Repeat([]byte("upload"), 128*1024)
	originDone := make(chan error, 1)
	go func() {
		c, e := origin.AcceptTCP()
		if e != nil {
			originDone <- e
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(5 * time.Second))
		data, e := io.ReadAll(c)
		if e == nil && !bytes.Equal(data, body) {
			e = io.ErrUnexpectedEOF
		}
		if e == nil {
			time.Sleep(20 * time.Millisecond)
			_, e = c.Write([]byte("command-confirmed"))
		}
		originDone <- e
	}()
	c, e := net.DialTCP("tcp", nil, front.Addr().(*net.TCPAddr))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, e = c.Write(body); e != nil {
		t.Fatal(e)
	}
	if e = c.CloseWrite(); e != nil {
		t.Fatal(e)
	}
	reply, e := io.ReadAll(c)
	if e != nil || string(reply) != "command-confirmed" {
		t.Fatalf("reply=%q error=%v", reply, e)
	}
	if e = <-originDone; e != nil {
		t.Fatal(e)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("entry did not release half-closed connection")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if w.connections != 0 {
		t.Fatal("connection reference leaked")
	}
}

func TestEntryPinsExistingConnectionsAcrossSwitch(t *testing.T) {
	front := testTCP(t)
	one := testTCP(t)
	two := testTCP(t)
	w1 := &entryWorker{record: record{DataPort: one.Addr().(*net.TCPAddr).Port}}
	w2 := &entryWorker{record: record{DataPort: two.Addr().(*net.TCPAddr).Port}}
	b := &entry{active: w1}
	for _, l := range []*net.TCPListener{one, two} {
		go func(l *net.TCPListener) {
			c, e := l.AcceptTCP()
			if e == nil {
				defer c.Close()
				io.Copy(c, c)
			}
		}(l)
	}
	for i := 0; i < 2; i++ {
		go func() {
			c, e := front.AcceptTCP()
			if e == nil {
				b.forward(c)
			}
		}()
	}
	open := func() *net.TCPConn {
		c, e := net.DialTCP("tcp", nil, front.Addr().(*net.TCPAddr))
		if e != nil {
			t.Fatal(e)
		}
		c.SetDeadline(time.Now().Add(3 * time.Second))
		t.Cleanup(func() { c.Close() })
		return c
	}
	check := func(c *net.TCPConn) {
		t.Helper()
		c.Write([]byte("ok"))
		buf := make([]byte, 2)
		if _, e := io.ReadFull(c, buf); e != nil || string(buf) != "ok" {
			t.Fatalf("confirmation %q %v", buf, e)
		}
	}
	c1 := open()
	check(c1)
	b.mu.Lock()
	b.active = w2
	b.mu.Unlock()
	c2 := open()
	check(c2)
	check(c1)
	b.mu.Lock()
	if w1.connections != 1 || w2.connections != 1 {
		t.Error("incorrect generation ownership")
	}
	b.mu.Unlock()
}

func TestReleaseStagingIntegrityAndRelativeSelection(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "candidate.exe")
	os.WriteFile(source, []byte("immutable-test-image"), 0600)
	a := &app{root: root, exe: source}
	image, e := a.stageRelease(source)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.selectRelease(image); e != nil {
		t.Fatal(e)
	}
	active, e := a.activeImage()
	if e != nil || active != image {
		t.Fatal(active, e)
	}
	ptr, _ := os.ReadFile(filepath.Join(root, "active-release.json"))
	if bytes.Contains(ptr, []byte(root)) {
		t.Fatal("absolute installation path persisted in release selection")
	}
	os.WriteFile(image, []byte("tampered"), 0600)
	if _, e = a.activeImage(); e == nil {
		t.Fatal("accepted tampered active image")
	}
	if _, e = a.stageRelease(source); e == nil {
		t.Fatal("overwrote immutable release")
	}
}

func TestReleaseRejectsPathsOutsideInstallation(t *testing.T) {
	a := &app{root: t.TempDir(), exe: filepath.Join(t.TempDir(), "launcher.exe")}
	if a.trustedImage(filepath.Join(t.TempDir(), nativeImage)) {
		t.Fatal("outside image accepted")
	}
	for _, hash := range []string{"..", "../outside", "", string(bytes.Repeat([]byte("g"), 64))} {
		if validHash(hash) {
			t.Fatal("invalid release hash", hash)
		}
	}
	os.WriteFile(filepath.Join(a.root, "active-release.json"), []byte(`{"sha256":"../outside"}`), 0600)
	if _, e := a.activeImage(); e == nil {
		t.Fatal("invalid pointer accepted")
	}
}

func TestPrivateWorkerEnvironmentDoesNotLeakIdentity(t *testing.T) {
	t.Setenv("AG_SYSTEM_PROXY_WORKER_ID", "old-worker")
	t.Setenv("AG_SYSTEM_PROXY_STATE_DIR", "old-root")
	a := &app{root: t.TempDir()}
	for _, v := range a.childEnvironment("") {
		if v == "AG_SYSTEM_PROXY_WORKER_ID=old-worker" || v == "AG_SYSTEM_PROXY_STATE_DIR=old-root" {
			t.Fatal("stale identity inherited")
		}
	}
}

func TestPrivateListenersCannotBeForwardingOrUpstreamTargets(t *testing.T) {
	a := &app{cfg: Config{Port: 17891}, dataPort: 24501, controlPort: 24502}
	c := &core{a: a}
	for _, authority := range []string{"localhost:17891", "127.0.0.1:24501", "[::1]:24502"} {
		for _, method := range []string{"GET", "CONNECT"} {
			r, e := http.NewRequest(method, "http://"+authority+"/", nil)
			if e != nil {
				t.Fatal(e)
			}
			if method == "CONNECT" {
				r.Host = authority
			}
			if !c.selfTarget(r) {
				t.Fatalf("allowed self target %s %s", method, authority)
			}
		}
		if _, e := choose(Settings{Enabled: true, Server: "http://" + authority}, "remote.invalid", "https", a.cfg.Port, a.dataPort, a.controlPort); e == nil {
			t.Fatalf("allowed internal upstream %s", authority)
		}
	}
}

func TestInactiveReleaseRetentionIsBounded(t *testing.T) {
	root := t.TempDir()
	a := &app{root: root, exe: filepath.Join(root, "launcher.exe")}
	source := filepath.Join(root, "candidate.exe")
	for i := 0; i < 6; i++ {
		os.WriteFile(source, bytes.Repeat([]byte{byte(i)}, 20), 0600)
		image, e := a.stageRelease(source)
		if e != nil {
			t.Fatal(e)
		}
		if e = a.selectRelease(image); e != nil {
			t.Fatal(e)
		}
	}
	a.pruneReleases()
	paths, _ := filepath.Glob(filepath.Join(root, "releases", "*", nativeImage))
	if len(paths) != 3 {
		t.Fatalf("retained %d, want active plus two spares", len(paths))
	}
	if _, e := a.activeImage(); e != nil {
		t.Fatal("removed active image", e)
	}
}
