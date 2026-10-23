//go:build windows

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"
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

func TestApplicationOriginalRestoreWinsOverOtherInstallations(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprint(ambiguous), func(t *testing.T) {
			a := applicationRecoveryTestApp(t)
			original := a.cfg.Application
			os.MkdirAll(filepath.Dir(original), 0700)
			before, _ := os.ReadFile(filepath.Join(a.root, "config.json"))
			done := make(chan error, 1)
			go func() {
				time.Sleep(800 * time.Millisecond)
				done <- os.WriteFile(original, []byte("restored original"), 0600)
			}()
			candidates := func() []string {
				if ambiguous {
					return []string{filepath.Join(a.root, "other-one", "Antigravity.exe"), filepath.Join(a.root, "other-two", "Antigravity.exe")}
				}
				return []string{filepath.Join(a.root, "other-one", "Antigravity.exe")}
			}
			err := a.waitForApplication(1200*time.Millisecond, candidates, func(string) bool { return true })
			writeErr := <-done
			if err != nil || writeErr != nil || a.cfg.Application != original {
				t.Fatal(err, writeErr, a.cfg.Application)
			}
			after, _ := os.ReadFile(filepath.Join(a.root, "config.json"))
			if !bytes.Equal(before, after) {
				t.Fatal("temporary update migrated configuration")
			}
		})
	}
}

func TestApplicationMonitorDefersPathMigration(t *testing.T) {
	now := time.Now()
	s := applicationLifetime{}
	if s.canRecoverApplicationPath(now) {
		t.Fatal("unobserved update permits migration")
	}
	s.absentAfterUpdate(now, false, nil, true)
	if s.canRecoverApplicationPath(now.Add(29 * time.Second)) {
		t.Fatal("migrated before restart grace")
	}
	if !s.canRecoverApplicationPath(now.Add(30 * time.Second)) {
		t.Fatal("recovery never enabled")
	}
	s.absentAfterUpdate(now.Add(31*time.Second), true, nil, false)
	if s.canRecoverApplicationPath(now.Add(time.Minute)) {
		t.Fatal("old update observation reused")
	}
}

func TestApplicationRegistryExpandablePaths(t *testing.T) {
	advapi := syscall.NewLazyDLL("advapi32.dll")
	name := fmt.Sprintf(`Software\AntigravitySystemProxy-Test-%d-%d`, os.Getpid(), time.Now().UnixNano())
	var key syscall.Handle
	status, _, _ := advapi.NewProc("RegCreateKeyExW").Call(uintptr(syscall.HKEY_CURRENT_USER), uintptr(unsafe.Pointer(wide(name))), 0, 0, 0, 0xf003f, 0, uintptr(unsafe.Pointer(&key)), 0)
	if status != 0 {
		t.Fatal(status)
	}
	defer func() {
		syscall.RegCloseKey(key)
		advapi.NewProc("RegDeleteKeyW").Call(uintptr(syscall.HKEY_CURRENT_USER), uintptr(unsafe.Pointer(wide(name))))
	}()
	t.Setenv("AG_APPLICATION_REGISTRY_FIXTURE", t.TempDir())
	value, _ := syscall.UTF16FromString(`%AG_APPLICATION_REGISTRY_FIXTURE%\Antigravity.exe`)
	status, _, _ = advapi.NewProc("RegSetValueExW").Call(uintptr(key), uintptr(unsafe.Pointer(wide("DisplayIcon"))), 0, 2, uintptr(unsafe.Pointer(&value[0])), uintptr(len(value)*2))
	if status != 0 {
		t.Fatal(status)
	}
	expected := filepath.Join(os.Getenv("AG_APPLICATION_REGISTRY_FIXTURE"), "Antigravity.exe")
	if got := registryApplicationValue(key, "DisplayIcon"); got != expected {
		t.Fatalf("expandable path: got %q want %q", got, expected)
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
