//go:build windows

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestApplicationRegistrationAndIconParsing(t *testing.T) {
	for _, name := range []string{"Antigravity", "Google Antigravity", "Antigravity 2.25.0", "Google Antigravity 3.0.1-beta"} {
		if !applicationRegistrationName.MatchString(name) {
			t.Fatal(name)
		}
	}
	for _, name := range []string{"NotAntigravity", "Antigravity Helper", "Antigravity Malware 2", "Antigravity\n2.25.0x extra"} {
		if applicationRegistrationName.MatchString(name) {
			t.Fatal(name)
		}
	}
	for _, value := range []string{`C:\Apps\Antigravity.exe,0`, `"C:\Apps, with spaces\Antigravity.exe",-1`, `C:\Apps\Antigravity.exe`} {
		if applicationIconPath(value) == "" {
			t.Fatal(value)
		}
	}
	for _, value := range []string{`Antigravity.exe`, `C:\Apps\Other.exe,0`, `"C:\Apps\Antigravity.exe" --run`, `"C:\Apps\Antigravity.exe",junk`, `\\server\share\Antigravity.exe`, `\\?\C:\Apps\Antigravity.exe`} {
		if applicationIconPath(value) != "" {
			t.Fatal(value)
		}
	}
}

func TestApplicationReplacementSelectionRejectsAmbiguity(t *testing.T) {
	first := filepath.Join(t.TempDir(), "Antigravity.exe")
	second := filepath.Join(t.TempDir(), "Antigravity.exe")
	validate := func(path string) bool { return path == first || path == second }
	if result, err := selectApplicationReplacement([]string{first, first, "rejected"}, validate); err != nil || !strings.EqualFold(result, canonical(first)) {
		t.Fatal(result, err)
	}
	if _, err := selectApplicationReplacement([]string{first, second}, validate); err == nil {
		t.Fatal("ambiguous installations selected")
	}
	if result, err := selectApplicationReplacement([]string{"rejected"}, validate); err != nil || result != "" {
		t.Fatal(result, err)
	}
}

func applicationRecoveryTestApp(t *testing.T) *app {
	t.Helper()
	a := historyTestApp(t)
	a.cfg.Application = filepath.Join(a.root, "old", "Antigravity.exe")
	data, _ := json.Marshal(map[string]any{"application": a.cfg.Application, "port": a.cfg.Port, "unknown_future_setting": map[string]string{"keep": "yes"}})
	if err := os.WriteFile(filepath.Join(a.root, "config.json"), append([]byte{239, 187, 191}, data...), 0600); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestApplicationPathRecoveryPreservesConfiguration(t *testing.T) {
	a := applicationRecoveryTestApp(t)
	newPath := filepath.Join(a.root, "new", "Antigravity.exe")
	if err := os.MkdirAll(filepath.Dir(newPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newPath, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.waitForApplication(time.Second, func() []string { return []string{newPath} }, func(p string) bool { return p == newPath }); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(a.root, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if json.Unmarshal(data, &config) != nil || config["application"] != canonical(newPath) || config["port"] != float64(a.cfg.Port) || config["unknown_future_setting"].(map[string]any)["keep"] != "yes" {
		t.Fatalf("settings changed: %s", data)
	}
	history, err := a.readHistory()
	if err != nil || len(history.Events) != 1 || history.Events[0].Kind != "application_path_recovered" {
		t.Fatal(history, err)
	}
}

func TestApplicationPathRecoveryRefusesChangedConfig(t *testing.T) {
	a := applicationRecoveryTestApp(t)
	path := filepath.Join(a.root, "config.json")
	data := []byte(`{"application":"changed-by-other-process","port":17891}`)
	os.WriteFile(path, data, 0600)
	if a.persistApplicationReplacement(filepath.Join(a.root, "new", "Antigravity.exe")) == nil {
		t.Fatal("concurrent configuration overwritten")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(data, after) {
		t.Fatal("config changed")
	}
}

func TestApplicationWaitsForTransientImageReplacement(t *testing.T) {
	a := applicationRecoveryTestApp(t)
	os.MkdirAll(filepath.Dir(a.cfg.Application), 0700)
	done := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		done <- os.WriteFile(a.cfg.Application, []byte("restored"), 0600)
	}()
	if err := a.waitForApplication(time.Second, func() []string { return nil }, func(string) bool { return false }); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestApplicationMissingTimeoutDoesNotChangeConfig(t *testing.T) {
	a := applicationRecoveryTestApp(t)
	path := filepath.Join(a.root, "config.json")
	before, _ := os.ReadFile(path)
	if a.waitForApplication(0, func() []string { return nil }, func(string) bool { return false }) == nil {
		t.Fatal("missing image accepted")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("failed recovery mutated config")
	}
}

func TestApplicationUpdateGraceIsBoundedAndResetsOnRestore(t *testing.T) {
	now := time.Now()
	state := applicationLifetime{}
	if state.absentAfterUpdate(now, false, nil, true) || state.absentAfterUpdate(now.Add(119*time.Second), false, nil, true) {
		t.Fatal("update grace too short")
	}
	if !state.absentAfterUpdate(now.Add(120*time.Second), false, nil, true) {
		t.Fatal("missing image keeps proxy forever")
	}
	state = applicationLifetime{}
	state.absentAfterUpdate(now, false, nil, true)
	if state.absentAfterUpdate(now.Add(time.Minute), false, nil, false) || state.absentAfterUpdate(now.Add(89*time.Second), false, nil, false) {
		t.Fatal("restored image did not receive normal restart grace")
	}
	if state.absentAfterUpdate(now.Add(90*time.Second), true, nil, false) {
		t.Fatal("restarted app not retained")
	}
}

func writeTestApplicationArchive(t *testing.T, path string, identity string, offset string) {
	t.Helper()
	data := []byte(identity)
	header, _ := json.Marshal(map[string]any{"files": map[string]any{"package.json": map[string]any{"size": len(data), "offset": offset}}})
	file := make([]byte, 16)
	binary.LittleEndian.PutUint32(file, 4)
	binary.LittleEndian.PutUint32(file[4:], uint32(8+len(header)))
	binary.LittleEndian.PutUint32(file[12:], uint32(len(header)))
	file = append(append(file, header...), data...)
	if err := os.WriteFile(path, file, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationReplacementRequiresPEAndBoundedPackageIdentity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Antigravity.exe")
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(self)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	os.Mkdir(filepath.Join(dir, "resources"), 0700)
	archive := filepath.Join(dir, "resources", "app.asar")
	writeTestApplicationArchive(t, archive, `{"name":"antigravity","productName":"Antigravity"}`, "0")
	if !validApplicationReplacement(path) {
		t.Fatal("valid registered package rejected")
	}
	writeTestApplicationArchive(t, archive, `{"name":"other","productName":"Antigravity"}`, "0")
	if validApplicationReplacement(path) {
		t.Fatal("same filename accepted without package identity")
	}
	writeTestApplicationArchive(t, archive, `{"name":"antigravity","productName":"Antigravity"}`, "9223372036854775807")
	if validApplicationReplacement(path) {
		t.Fatal("overflow package offset accepted")
	}
	malformed := make([]byte, 16)
	binary.LittleEndian.PutUint32(malformed[12:], (4<<20)+1)
	os.WriteFile(archive, malformed, 0600)
	if validApplicationReplacement(path) {
		t.Fatal("unbounded header accepted")
	}
	writeTestApplicationArchive(t, archive, `{"name":"antigravity","productName":"Antigravity"}`, "0")
	os.WriteFile(path, []byte("not an executable"), 0600)
	if validApplicationReplacement(path) {
		t.Fatal("invalid executable accepted")
	}
}
