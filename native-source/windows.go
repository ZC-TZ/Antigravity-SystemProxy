//go:build windows

package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

var kernel = syscall.NewLazyDLL("kernel32.dll")

var advapi = syscall.NewLazyDLL("advapi32.dll")

var openProcess = kernel.NewProc("OpenProcess")

var getTimes = kernel.NewProc("GetProcessTimes")

var queryImage = kernel.NewProc("QueryFullProcessImageNameW")

var queryFinalPath = kernel.NewProc("GetFinalPathNameByHandleW")

var createMutex = kernel.NewProc("CreateMutexW")

var regGet = advapi.NewProc("RegGetValueW")

func wide(s string) *uint16 { p, _ := syscall.UTF16PtrFromString(s); return p }

// The common case is a short path. Preserve the full Windows UTF-16 bound
// without allocating 64 KiB on every background process-identity check.
func readWindowsPath(query func([]uint16) (uint32, error)) (string, error) {
	const maxUnits = 32768
	for size := 512; ; size *= 2 {
		buf := make([]uint16, size)
		n, err := query(buf)
		if err == nil && n > 0 && n < uint32(len(buf)) {
			return syscall.UTF16ToString(buf[:n]), nil
		}
		if err != nil && !errors.Is(err, syscall.ERROR_INSUFFICIENT_BUFFER) {
			return "", err
		}
		if err == nil && n == 0 {
			return "", errors.New("Windows path query returned an empty path")
		}
		if size == maxUnits {
			return "", syscall.ERROR_INSUFFICIENT_BUFFER
		}
	}
}

func canonical(path string) string {
	path, _ = filepath.Abs(path)
	p, e := syscall.CreateFile(wide(path), 0, 7, nil, 3, 0x02000000, 0)
	if e != nil {
		return filepath.Clean(path)
	}
	defer syscall.CloseHandle(p)
	result, err := readWindowsPath(func(buf []uint16) (uint32, error) {
		n, _, err := queryFinalPath.Call(uintptr(p), uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), 0)
		if n == 0 {
			return 0, err
		}
		return uint32(n), nil
	})
	if err != nil {
		return filepath.Clean(path)
	}
	if strings.HasPrefix(result, `\\?\UNC\`) {
		return `\\` + result[8:]
	}
	return strings.TrimPrefix(result, `\\?\`)
}

func regString(name string) (string, error) {
	var status uintptr
	value := readRegistryString(func(buf []uint16, size *uint32) uintptr {
		r, _, _ := regGet.Call(0x80000001, uintptr(unsafe.Pointer(wide(`Software\Microsoft\Windows\CurrentVersion\Internet Settings`))), uintptr(unsafe.Pointer(wide(name))), 2, 0, uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(size)))
		status = r
		return r
	})
	if status != 0 && status != 2 {
		return "", fmt.Errorf("reading system proxy setting %s: %w", name, syscall.Errno(status))
	}
	return value, nil
}

func readRegistryString(read func([]uint16, *uint32) uintptr) string {
	buf := make([]uint16, 512)
	// ERROR_MORE_DATA reports the required byte size. Retry within a fixed bound
	// because another process can change the value between these calls.
	for attempt := 0; attempt < 3; attempt++ {
		size := uint32(len(buf) * 2)
		r := read(buf, &size)
		if r == 0 {
			return syscall.UTF16ToString(buf)
		}
		if r != 234 || size < 2 || size > 65536 || size%2 != 0 {
			return ""
		}
		buf = make([]uint16, int(size/2)+1)
	}
	return ""
}

func regEnabled() (bool, error) {
	var value, size uint32
	size = 4
	r, _, _ := regGet.Call(0x80000001, uintptr(unsafe.Pointer(wide(`Software\Microsoft\Windows\CurrentVersion\Internet Settings`))), uintptr(unsafe.Pointer(wide("ProxyEnable"))), 0x10, 0, uintptr(unsafe.Pointer(&value)), uintptr(unsafe.Pointer(&size)))
	if r != 0 && r != 2 {
		return false, fmt.Errorf("reading system proxy enable: %w", syscall.Errno(r))
	}
	return r == 0 && value != 0, nil
}
func windowsSettings(enabled func() (bool, error), readString func(string) (string, error)) (Settings, error) {
	on, e := enabled()
	if e != nil {
		return Settings{}, e
	}
	if !on {
		return Settings{}, nil
	}
	server, e := readString("ProxyServer")
	if e != nil {
		return Settings{}, e
	}
	bypass, e := readString("ProxyOverride")
	if e != nil {
		return Settings{}, e
	}
	return Settings{true, server, bypass}, nil
}

type process struct {
	handle  syscall.Handle
	pid     int
	created uint64
	image   string
}

func openOwned(pid int) (*process, error) {
	return openProcessAccess(pid, 0x00101001)
}

func openProcessAccess(pid int, access uintptr) (*process, error) {
	h, _, e := openProcess.Call(access, 0, uintptr(pid))
	if h == 0 {
		return nil, e
	}
	p := &process{handle: syscall.Handle(h), pid: pid}
	var exit, kernelTime, user uint64
	r, _, e := getTimes.Call(h, uintptr(unsafe.Pointer(&p.created)), uintptr(unsafe.Pointer(&exit)), uintptr(unsafe.Pointer(&kernelTime)), uintptr(unsafe.Pointer(&user)))
	if r == 0 {
		p.close()
		return nil, e
	}
	image, err := readWindowsPath(func(buf []uint16) (uint32, error) {
		size := uint32(len(buf))
		r, _, err := queryImage.Call(h, 0, uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(&size)))
		if r == 0 {
			return 0, err
		}
		return size, nil
	})
	if err != nil {
		p.close()
		return nil, err
	}
	p.image = strings.ToLower(image)
	return p, nil
}

func (p *process) alive() bool {
	r, e := syscall.WaitForSingleObject(p.handle, 0)
	return e == nil && r == 258
}

func (p *process) kill() error {
	if !p.alive() {
		return nil
	}
	return syscall.TerminateProcess(p.handle, 1)
}

func (p *process) wait() error {
	r, e := syscall.WaitForSingleObject(p.handle, 10000)
	if e != nil {
		return e
	}
	if r == 258 {
		return errors.New("worker did not exit")
	}
	return nil
}

func (p *process) close() {
	if p.handle != 0 {
		syscall.CloseHandle(p.handle)
		p.handle = 0
	}
}
