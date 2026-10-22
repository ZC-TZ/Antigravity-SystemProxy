//go:build windows

package main

import (
	"regexp"
	"strings"
	"syscall"
	"testing"
)

func BenchmarkRouting(b *testing.B) {
	s := Settings{true, "127.0.0.1:7890", "localhost;127.*;192.168.*;10.*;172.16.*;172.17.*;172.18.*;172.19.*;172.20.*;172.21.*;172.22.*;172.23.*;172.24.*;172.25.*;172.26.*;172.27.*;172.28.*;172.29.*;172.30.*;172.31.*;<local>"}
	b.ReportAllocs()
	for b.Loop() {
		if _, e := choose(s, "oauth2.googleapis.com", "https", 17891); e != nil {
			b.Fatal(e)
		}
	}
}

func BenchmarkRegistrySettings(b *testing.B) {
	a := &app{}
	b.ReportAllocs()
	for b.Loop() {
		if _, e := a.settings(); e != nil {
			b.Fatal(e)
		}
	}
}

func TestWildcardMatchesPreviousSemantics(t *testing.T) {
	patterns := []string{"", "*", "?", "*.test", "172.2?.*", "a**b?c", "[ab]", "a.b", "中文?*", "*文?", "?*?", "a*b*c", "*."}
	hosts := []string{"", "a", "ab", "abc", "172.20.1.1", "EXAMPLE.TEST", "a.b", "[ab]", "中文域名", "a文b", "abbbc", "example."}
	// Exhaust short combinations as well as Unicode and literal regex characters.
	var generate func(string, int)
	generate = func(s string, n int) {
		patterns = append(patterns, s)
		if n > 0 {
			for _, v := range []string{"a", "b", "*", "?"} {
				generate(s+v, n-1)
			}
		}
	}
	generate("", 4)
	for _, p := range patterns {
		expr := regexp.QuoteMeta(strings.ToLower(p))
		expr = strings.ReplaceAll(strings.ReplaceAll(expr, `\*`, ".*"), `\?`, ".")
		re := regexp.MustCompile("^" + expr + "$")
		for _, host := range hosts {
			if got, want := wildcard(p, host), re.MatchString(strings.ToLower(host)); got != want {
				t.Fatalf("pattern=%q host=%q got=%v want=%v", p, host, got, want)
			}
		}
	}
}

func TestRegistryBufferGrowthAndBounds(t *testing.T) {
	value := strings.Repeat("排除.example;", 200)
	encoded, _ := syscall.UTF16FromString(value)
	calls := 0
	got := readRegistryString(func(buf []uint16, size *uint32) uintptr {
		calls++
		*size = uint32(len(encoded) * 2)
		if len(buf) < len(encoded) {
			return 234
		}
		copy(buf, encoded)
		return 0
	})
	if got != value || calls != 2 {
		t.Fatalf("long value truncated: calls=%d len=%d", calls, len(got))
	}
	for _, code := range []uintptr{2, 234} {
		calls = 0
		got = readRegistryString(func(buf []uint16, size *uint32) uintptr { calls++; *size = uint32(len(buf)*2 + 2); return code })
		if got != "" || calls > 3 {
			t.Fatal("unbounded retry or stale data")
		}
	}
	got = readRegistryString(func(buf []uint16, size *uint32) uintptr { *size = 1000000; return 234 })
	if got != "" {
		t.Fatal("oversized registry value accepted")
	}
}
