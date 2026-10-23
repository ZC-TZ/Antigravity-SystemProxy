//go:build windows

package main

import (
	"bytes"
	"context"
	"debug/pe"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const applicationUpdateGrace = 2 * time.Minute

var applicationRegistrationName = regexp.MustCompile(`(?i)^(Google )?Antigravity(?:\s+\d[\w.\-+]*)?$`)

func localApplicationPath(path string) bool {
	volume := filepath.VolumeName(path)
	return filepath.IsAbs(path) && len(volume) == 2 && volume[1] == ':' && strings.EqualFold(filepath.Base(path), "Antigravity.exe")
}

// DisplayIcon is data, never a command to execute. An index is optional and a
// quoted path may itself contain commas.
func applicationIconPath(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, `"`) {
		end := strings.Index(value[1:], `"`)
		if end < 0 {
			return ""
		}
		end++
		tail := strings.TrimSpace(value[end+1:])
		if tail != "" {
			if !strings.HasPrefix(tail, ",") {
				return ""
			}
			if _, err := strconv.Atoi(strings.TrimSpace(tail[1:])); err != nil {
				return ""
			}
		}
		value = value[1:end]
	} else if index := strings.LastIndex(value, ","); index >= 0 {
		if _, err := strconv.Atoi(strings.TrimSpace(value[index+1:])); err == nil {
			value = value[:index]
		}
	}
	if !localApplicationPath(value) {
		return ""
	}
	return filepath.Clean(value)
}

func registryApplicationValue(key syscall.Handle, name string) string {
	return readRegistryString(func(buf []uint16, size *uint32) uintptr {
		status, _, _ := regGet.Call(uintptr(key), 0, uintptr(unsafe.Pointer(wide(name))), 2, 0, uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(size)))
		return status
	})
}

// Only registered installation locations are considered. No drive scan, PATH
// lookup, process-name guess or UninstallString execution is permitted.
func registeredApplicationPaths() []string {
	var paths []string
	for _, hive := range []syscall.Handle{syscall.HKEY_CURRENT_USER, syscall.HKEY_LOCAL_MACHINE} {
		for _, view := range []uint32{0x100, 0x200} {
			var key syscall.Handle
			if syscall.RegOpenKeyEx(hive, wide(`Software\Microsoft\Windows\CurrentVersion\Uninstall`), 0, 0x20019|view, &key) != nil {
				continue
			}
			for index := uint32(0); index < 8192; index++ {
				name := make([]uint16, 256)
				n := uint32(len(name))
				err := syscall.RegEnumKeyEx(key, index, &name[0], &n, nil, nil, nil, nil)
				if errors.Is(err, syscall.Errno(259)) {
					break
				}
				if err != nil {
					continue
				}
				var child syscall.Handle
				if syscall.RegOpenKeyEx(key, &name[0], 0, 0x20019|view, &child) != nil {
					continue
				}
				if applicationRegistrationName.MatchString(registryApplicationValue(child, "DisplayName")) {
					if location := registryApplicationValue(child, "InstallLocation"); filepath.IsAbs(location) {
						paths = append(paths, filepath.Join(location, "Antigravity.exe"))
					}
					if icon := applicationIconPath(registryApplicationValue(child, "DisplayIcon")); icon != "" {
						paths = append(paths, icon)
					}
				}
				syscall.RegCloseKey(child)
			}
			syscall.RegCloseKey(key)
		}
	}
	return paths
}

// Package identity is a compatibility check, not a signature or trust boundary.
// Existing explicitly configured applications retain their previous semantics;
// automatic replacement additionally requires a registered path and this layout.
func validApplicationReplacement(path string) bool {
	if !localApplicationPath(path) {
		return false
	}
	exe, err := pe.Open(path)
	if err != nil {
		return false
	}
	machine := exe.FileHeader.Machine
	exe.Close()
	if machine != pe.IMAGE_FILE_MACHINE_AMD64 && machine != pe.IMAGE_FILE_MACHINE_I386 {
		return false
	}
	archive, err := os.Open(filepath.Join(filepath.Dir(path), "resources", "app.asar"))
	if err != nil {
		return false
	}
	defer archive.Close()
	var prefix [16]byte
	if _, err = io.ReadFull(archive, prefix[:]); err != nil {
		return false
	}
	headerBytes := binary.LittleEndian.Uint32(prefix[12:])
	if headerBytes == 0 || headerBytes > 4<<20 {
		return false
	}
	header := make([]byte, headerBytes)
	if _, err = io.ReadFull(archive, header); err != nil {
		return false
	}
	var files struct {
		Files map[string]struct {
			Size     int64  `json:"size"`
			Offset   string `json:"offset"`
			Unpacked bool   `json:"unpacked"`
			Link     string `json:"link"`
		} `json:"files"`
	}
	if json.Unmarshal(header, &files) != nil {
		return false
	}
	entry, ok := files.Files["package.json"]
	offset, err := strconv.ParseInt(entry.Offset, 10, 64)
	base := int64(8) + int64(binary.LittleEndian.Uint32(prefix[4:]))
	info, statErr := archive.Stat()
	if !ok || err != nil || statErr != nil || entry.Unpacked || entry.Link != "" || entry.Size <= 0 || entry.Size > 64<<10 || offset < 0 || base < 16+int64(headerBytes) || base > info.Size() || offset > info.Size()-base || entry.Size > info.Size()-base-offset {
		return false
	}
	data := make([]byte, entry.Size)
	if _, err = archive.ReadAt(data, base+offset); err != nil {
		return false
	}
	var identity struct {
		Name        string `json:"name"`
		ProductName string `json:"productName"`
	}
	return json.Unmarshal(data, &identity) == nil && strings.EqualFold(identity.Name, "antigravity") && strings.EqualFold(identity.ProductName, "Antigravity")
}

func selectApplicationReplacement(paths []string, validate func(string) bool) (string, error) {
	unique := map[string]string{}
	for _, path := range paths {
		if validate(path) {
			path = canonical(path)
			unique[strings.ToLower(path)] = path
		}
	}
	if len(unique) > 1 {
		return "", errors.New("multiple registered Antigravity installations found; rerun install.cmd with -AntigravityPath")
	}
	for _, path := range unique {
		return path, nil
	}
	return "", nil
}

// Caller holds the installation mutex. Preserve all other fields, including
// unknown future settings, and refuse to overwrite a concurrently changed path.
func (a *app) persistApplicationReplacement(path string) error {
	configPath := filepath.Join(a.root, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	var settings map[string]json.RawMessage
	if json.Unmarshal(bytes.TrimPrefix(data, []byte{239, 187, 191}), &settings) != nil || settings == nil {
		return errors.New("invalid application configuration")
	}
	var previous string
	if json.Unmarshal(settings["application"], &previous) != nil || !strings.EqualFold(previous, a.cfg.Application) {
		return errors.New("application configuration changed; retry launcher")
	}
	settings["application"], _ = json.Marshal(path)
	if err = writeRecord(configPath, settings); err != nil {
		return err
	}
	a.cfg.Application = path
	journal := newHistory(a, "launcher")
	journal.emit(historyEvent{Kind: "application_path_recovered", Reason: "unique_registered_package"})
	journal.close()
	return nil
}

func (a *app) waitForApplication(timeout time.Duration, candidates func() []string, validate func(string) bool) error {
	deadline := time.Now().Add(timeout)
	var selected string
	stableSince := time.Now()
	var selectionErr error
	for {
		info, err := os.Stat(a.cfg.Application)
		if err == nil {
			if !info.Mode().IsRegular() {
				return errors.New("configured application is not a regular file")
			}
			return nil
		}
		if !os.IsNotExist(err) {
			return err
		}
		// Give the configured installation the full replacement grace before
		// migrating to another registered copy. Ambiguity must not prevent an
		// in-place updater from restoring the original image during that grace.
		path, candidateErr := selectApplicationReplacement(candidates(), validate)
		selectionErr = candidateErr
		if path != selected {
			selected = path
			stableSince = time.Now()
		}
		if !time.Now().Before(deadline) {
			if selectionErr != nil {
				return selectionErr
			}
			if selected != "" && time.Since(stableSince) >= 500*time.Millisecond {
				return a.persistApplicationReplacement(selected)
			}
			return fmt.Errorf("Antigravity executable is unavailable after update grace; finish its update or rerun install.cmd with -AntigravityPath")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (a *app) applicationCheck() error {
	// This is an explicit diagnostic, not a polling dependency. Do not print
	// command lines or inspect the user's tokens/environment; missing Chromium
	// arguments alone cannot prove that inherited proxy variables were lost.
	const script = `[Console]::OutputEncoding=New-Object Text.UTF8Encoding
$ErrorActionPreference='Stop'
$instances=@(Get-CimInstance Win32_Process -Filter "Name='Antigravity.exe'" | Where-Object { $_.ExecutablePath -eq $env:AG_APPLICATION_CHECK_PATH -and $_.CommandLine -notmatch '(?:^|\s)--type=' } | ForEach-Object {
    $available=-not [string]::IsNullOrEmpty($_.CommandLine)
    $pattern='(?:^|\s)"?'+[regex]::Escape($env:AG_APPLICATION_CHECK_ARGUMENT)+'"?(?=\s|$)'
    [pscustomobject]@{pid=$_.ProcessId;command_line_available=$available;expected_proxy_argument_present=($available -and $_.CommandLine -match $pattern);proxy_environment_verified=$false}
})
[pscustomobject]@{instances=$instances} | ConvertTo-Json -Depth 4`
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	powershell := filepath.Join(os.Getenv("SystemRoot"), "System32", "WindowsPowerShell", "v1.0", "powershell.exe")
	cmd := exec.CommandContext(ctx, powershell, "-NoProfile", "-NonInteractive", "-Command", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	cmd.Env = append(os.Environ(), "AG_APPLICATION_CHECK_PATH="+a.cfg.Application, fmt.Sprintf("AG_APPLICATION_CHECK_ARGUMENT=--proxy-server=http://127.0.0.1:%d", a.cfg.Port))
	data, err := cmd.Output()
	var processes any
	queryOK := err == nil && len(data) <= 64<<10 && json.Unmarshal(bytes.TrimPrefix(data, []byte{239, 187, 191}), &processes) == nil
	_, imageErr := os.Stat(a.cfg.Application)
	var validPaths []string
	seen := map[string]bool{}
	for _, path := range registeredApplicationPaths() {
		path = canonical(path)
		key := strings.ToLower(path)
		if !seen[key] && validApplicationReplacement(path) {
			seen[key] = true
			validPaths = append(validPaths, path)
		}
	}
	result := map[string]any{"revision": revision, "application": a.cfg.Application, "application_exists": imageErr == nil, "application_update_grace_seconds": int(applicationUpdateGrace / time.Second), "registered_candidates": validPaths, "process_query_ok": queryOK, "process_check": processes, "proxy_environment_verified": false, "interpretation": "A matching proxy argument confirms launcher-style Chromium configuration; absence does not prove inherited proxy environment is missing. Save work and relaunch through Antigravity (System Proxy) when in doubt; no application is terminated automatically."}
	return json.NewEncoder(os.Stdout).Encode(result)
}
