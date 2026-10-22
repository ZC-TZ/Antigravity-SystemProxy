//go:build windows

package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

const nativeImage = "AntigravitySystemProxy.exe"

type releasePointer struct {
	SHA256 string `json:"sha256"`
}

func nonce() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}

func writeRecord(path string, value any) error {
	data, e := json.Marshal(value)
	if e != nil {
		return e
	}
	tmp := path + "." + nonce() + ".tmp"
	if e = os.WriteFile(tmp, data, 0600); e != nil {
		return e
	}
	defer os.Remove(tmp)
	return os.Rename(tmp, path)
}

func imageHash(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func validHash(h string) bool              { return len(h) == 64 && strings.Trim(h, "0123456789abcdef") == "" }
func (a *app) releasePath(h string) string { return filepath.Join(a.root, "releases", h, nativeImage) }
func (a *app) trustedImage(path string) bool {
	path = canonical(path)
	if strings.EqualFold(path, a.exe) || strings.EqualFold(path, canonical(filepath.Join(a.root, nativeImage))) {
		return true
	}
	dir := filepath.Dir(path)
	hash := filepath.Base(dir)
	return validHash(hash) && strings.EqualFold(path, a.releasePath(hash)) && strings.EqualFold(canonical(filepath.Dir(dir)), canonical(filepath.Join(a.root, "releases")))
}
func (a *app) ownedRecord(r record) (*process, error) {
	if r.Version != marker || r.ProxyPort != a.cfg.Port || !a.trustedImage(r.Script) || !strings.EqualFold(r.Script, r.Executable) || len(r.Generation) != 32 {
		return nil, errors.New("record identity mismatch")
	}
	p, e := openOwned(r.PID)
	if e != nil {
		return nil, e
	}
	if p.created != r.Created || !strings.EqualFold(p.image, r.Executable) || !p.alive() {
		p.close()
		return nil, errors.New("process identity mismatch")
	}
	return p, nil
}
func (a *app) activeImage() (string, error) {
	data, e := os.ReadFile(filepath.Join(a.root, "active-release.json"))
	if os.IsNotExist(e) {
		return a.exe, nil
	}
	if e != nil {
		return "", e
	}
	var ptr releasePointer
	if json.Unmarshal(data, &ptr) != nil || !validHash(ptr.SHA256) {
		return "", errors.New("invalid active release pointer")
	}
	path := a.releasePath(ptr.SHA256)
	if !a.trustedImage(path) {
		return "", errors.New("release path escaped installation")
	}
	h, e := imageHash(path)
	if e != nil {
		return "", e
	}
	if h != ptr.SHA256 {
		return "", errors.New("active release checksum mismatch")
	}
	return path, nil
}
func (a *app) stageRelease(source string) (string, error) {
	h, e := imageHash(source)
	if e != nil {
		return "", e
	}
	path := a.releasePath(h)
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return "", e
	}
	if !a.trustedImage(path) {
		return "", errors.New("release path escaped installation")
	}
	if existing, e := imageHash(path); e == nil {
		if existing == h {
			return path, nil
		}
		return "", errors.New("staged release checksum mismatch")
	}
	tmp := path + "." + nonce() + ".tmp"
	defer os.Remove(tmp)
	in, e := os.Open(source)
	if e != nil {
		return "", e
	}
	defer in.Close()
	out, e := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return "", e
	}
	_, e = io.Copy(out, in)
	if e == nil {
		e = out.Sync()
	}
	ce := out.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return "", e
	}
	actual, e := imageHash(tmp)
	if e != nil || actual != h {
		return "", errors.New("candidate changed while copying")
	}
	if e = os.Rename(tmp, path); e != nil {
		return "", e
	}
	return path, nil
}
func (a *app) selectRelease(path string) error {
	h := filepath.Base(filepath.Dir(path))
	if !validHash(h) || !a.trustedImage(path) {
		return errors.New("invalid release selection")
	}
	return writeRecord(filepath.Join(a.root, "active-release.json"), releasePointer{h})
}
func (a *app) childEnvironment(id string) []string {
	env := []string{}
	for _, v := range os.Environ() {
		k := strings.ToUpper(strings.SplitN(v, "=", 2)[0])
		if k != "AG_SYSTEM_PROXY_STATE_DIR" && k != "AG_SYSTEM_PROXY_WORKER_ID" {
			env = append(env, v)
		}
	}
	env = append(env, "AG_SYSTEM_PROXY_STATE_DIR="+a.root)
	if id != "" {
		env = append(env, "AG_SYSTEM_PROXY_WORKER_ID="+id)
	}
	return env
}

// Bootstrap a versioned image before starting a broker. The root EXE remains a
// replaceable launcher; running processes never lock it during future upgrades.
func (a *app) initializeRelease() error {
	if _, e := os.Stat(filepath.Join(a.root, "active-release.json")); e == nil {
		return nil
	}
	path, e := a.stageRelease(a.exe)
	if e != nil {
		return e
	}
	return a.selectRelease(path)
}
func (a *app) upgrade(source string) error {
	path, e := a.stageRelease(source)
	if e != nil {
		return e
	}
	r, e := a.readRecord(true)
	if e != nil {
		return e
	}
	body, _ := json.Marshal(map[string]string{"image": path})
	result, e := a.callRecord(r, "POST", "/upgrade", body, 25e9)
	if e != nil {
		return fmt.Errorf("upgrade not confirmed; inspect --diagnostics before retrying: %w", e)
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func (a *app) runtimeFingerprint() string {
	cfg := a.cfg
	cfg.Application = ""
	data, _ := json.Marshal(cfg)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func (a *app) activate(source string) error {
	guard, exists, err := a.entryGuard()
	if err != nil {
		return err
	}
	defer syscall.CloseHandle(guard)
	if exists {
		return errors.New("offline activation requires a stopped entry; use --upgrade for a live entry")
	}
	paths := []string{a.statePath(false), a.statePath(true)}
	private, _ := filepath.Glob(filepath.Join(a.root, "worker-private-*.json"))
	paths = append(paths, private...)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		var r record
		if err != nil || json.Unmarshal(data, &r) != nil {
			continue
		}
		if p, e := a.ownedRecord(r); e == nil {
			p.close()
			return errors.New("offline activation requires a stopped entry and all workers; use --upgrade for a live entry")
		}
	}
	path, e := a.stageRelease(source)
	if e != nil {
		return e
	}
	return a.selectRelease(path)
}

// Keep running images, the active selection, and two spare releases. Never
// recurse: unexpected files or directories remain for explicit inspection.
func (a *app) pruneReleases() {
	selected, e := a.activeImage()
	if e != nil {
		return
	}
	keep := map[string]bool{strings.ToLower(canonical(selected)): true, strings.ToLower(canonical(a.exe)): true}
	paths, _ := filepath.Glob(filepath.Join(a.root, "*.json"))
	for _, path := range paths {
		name := filepath.Base(path)
		if !strings.HasPrefix(name, "worker-") && !strings.HasPrefix(name, "supervisor-") && !strings.HasPrefix(name, "lifecycle-") {
			continue
		}
		var r record
		d, _ := os.ReadFile(path)
		if json.Unmarshal(d, &r) != nil || !a.trustedImage(r.Executable) {
			continue
		}
		p, err := openOwned(r.PID)
		if err != nil {
			continue
		}
		if p.created == r.Created && strings.EqualFold(p.image, r.Executable) && p.alive() {
			keep[strings.ToLower(canonical(p.image))] = true
		}
		p.close()
	}
	type spare struct {
		path     string
		modified int64
	}
	var spares []spare
	dirs, _ := os.ReadDir(filepath.Join(a.root, "releases"))
	for _, dir := range dirs {
		if !dir.IsDir() || !validHash(dir.Name()) {
			continue
		}
		path := a.releasePath(dir.Name())
		if !a.trustedImage(path) || keep[strings.ToLower(canonical(path))] {
			continue
		}
		if info, err := os.Stat(path); err == nil {
			spares = append(spares, spare{path, info.ModTime().UnixNano()})
		}
	}
	sort.Slice(spares, func(i, j int) bool { return spares[i].modified > spares[j].modified })
	for i := 2; i < len(spares); i++ {
		path := spares[i].path
		if os.Remove(path) == nil {
			_ = os.Remove(filepath.Dir(path))
		}
	}
}
