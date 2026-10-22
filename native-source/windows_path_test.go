//go:build windows

package main

import (
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"unsafe"
)

func TestWindowsPathBufferSupportsUnicodeAndGrowth(t *testing.T) {
	for _, text := range []string{`C:\\tools\\测试🚀.exe`, strings.Repeat("长", 3000), strings.Repeat("x", 32767)} {
		encoded, err := syscall.UTF16FromString(text)
		if err != nil {
			t.Fatal(err)
		}
		for _, style := range []string{"required_length", "insufficient_buffer"} {
			calls := 0
			got, err := readWindowsPath(func(buf []uint16) (uint32, error) {
				calls++
				if len(buf) < len(encoded) {
					if style == "insufficient_buffer" {
						return 0, syscall.ERROR_INSUFFICIENT_BUFFER
					}
					return uint32(len(encoded)), nil
				}
				copy(buf, encoded)
				return uint32(len(encoded) - 1), nil
			})
			if err != nil || got != text || calls > 7 {
				t.Fatal("path growth changed UTF-16 result", len(text), style, calls, err)
			}
			if len(encoded) <= 512 && calls != 1 {
				t.Fatal("short path unnecessarily grew")
			}
		}
	}
}

func TestWindowsPathBufferFailureIsBounded(t *testing.T) {
	for _, mode := range []string{"denied", "too_long", "empty"} {
		calls := 0
		_, err := readWindowsPath(func(buf []uint16) (uint32, error) {
			calls++
			if len(buf) > 32768 {
				t.Fatal("path buffer exceeded Windows bound")
			}
			switch mode {
			case "denied":
				return 0, syscall.ERROR_ACCESS_DENIED
			case "empty":
				return 0, nil
			default:
				return uint32(len(buf) + 1), nil
			}
		})
		if err == nil || calls > 7 {
			t.Fatal("failed path query accepted", mode, calls, err)
		}
		if mode == "denied" && (!errors.Is(err, syscall.ERROR_ACCESS_DENIED) || calls != 1) {
			t.Fatal("fatal query retried", calls, err)
		}
		if mode == "too_long" && (!errors.Is(err, syscall.ERROR_INSUFFICIENT_BUFFER) || calls != 7) {
			t.Fatal("growth not bounded", calls, err)
		}
	}
}

func TestWindowsPathQueriesValidateCurrentProcess(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := openOwned(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if !strings.EqualFold(p.image, canonical(exe)) || p.created == 0 || !p.alive() {
		t.Fatal("process identity changed", p.image, exe)
	}
	// Verify both real API contracts used by the growth helper, rather than
	// relying only on callbacks that simulate an undersized buffer.
	var short [1]uint16
	size := uint32(len(short))
	result, _, queryErr := queryImage.Call(uintptr(p.handle), 0, uintptr(unsafe.Pointer(&short[0])), uintptr(unsafe.Pointer(&size)))
	if result != 0 || !errors.Is(queryErr, syscall.ERROR_INSUFFICIENT_BUFFER) {
		t.Fatal("unexpected process-image buffer contract", result, queryErr)
	}
	file, err := os.Open(exe)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	required, _, queryErr := queryFinalPath.Call(file.Fd(), uintptr(unsafe.Pointer(&short[0])), uintptr(len(short)), 0)
	if required <= uintptr(len(short)) {
		t.Fatal("unexpected final-path required-length contract", required, queryErr)
	}
}

func BenchmarkWindowsPathQueries(b *testing.B) {
	exe, err := os.Executable()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("process_image", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			p, err := openOwned(os.Getpid())
			if err != nil {
				b.Fatal(err)
			}
			p.close()
		}
	})
	b.Run("canonical_path", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if canonical(exe) == "" {
				b.Fatal("empty canonical path")
			}
		}
	})
}
