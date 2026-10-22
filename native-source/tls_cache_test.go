//go:build windows

package main

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProxyTLSConfigCacheScopeAndTrust(t *testing.T) {
	up := httptest.NewTLSServer(http.NotFoundHandler())
	defer up.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: up.Certificate().Raw}), 0600)
	a := &app{cfg: Config{ProxyCA: ca}}
	first, e := a.tlsConfig("localhost")
	if e != nil {
		t.Fatal(e)
	}
	if first.InsecureSkipVerify || first.RootCAs == nil || first.ClientSessionCache == nil {
		t.Fatal("unsafe TLS config")
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg, e := a.tlsConfig("other.example")
			if e != nil {
				t.Error(e)
				return
			}
			if cfg.RootCAs != first.RootCAs || cfg.ClientSessionCache != first.ClientSessionCache || cfg.ServerName != "other.example" {
				t.Error("cache not shared or host mutated")
			}
		}()
	}
	wg.Wait()
	if first.ServerName != "localhost" {
		t.Fatal("config shared mutably")
	}
	// Trust files are a per-worker snapshot. A fresh process must still reject a
	// missing/invalid configured file instead of borrowing another worker's roots.
	os.Remove(ca)
	if _, e = a.tlsConfig("localhost"); e != nil {
		t.Fatal("cached snapshot reread file", e)
	}
	fresh := &app{cfg: Config{ProxyCA: ca}}
	if _, e = fresh.tlsConfig("localhost"); e == nil {
		t.Fatal("missing CA accepted by fresh worker")
	}
	a.cfg.ProxyCA = ""
	next, e := a.tlsConfig("localhost")
	if e != nil || next.RootCAs != nil || next.ClientSessionCache == first.ClientSessionCache {
		t.Fatal("trust scope did not reset", e)
	}
}

func TestHTTPSProxySessionResumptionPreservesTunnel(t *testing.T) {
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			var resumed atomic.Int64
			up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.TLS.DidResume {
					resumed.Add(1)
				}
				c, rw, e := http.NewResponseController(w).Hijack()
				if e != nil {
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(3 * time.Second))
				io.WriteString(rw, "HTTP/1.1 200 OK\r\n\r\n")
				rw.Flush()
				io.Copy(c, rw)
			}))
			up.TLS = &tls.Config{MinVersion: version, MaxVersion: version}
			up.StartTLS()
			defer up.Close()
			ca := filepath.Join(t.TempDir(), "ca.pem")
			os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: up.Certificate().Raw}), 0600)
			a := &app{cfg: Config{Port: 17891, ProxyCA: ca}}
			const count = 12
			start := time.Now()
			for i := 0; i < count; i++ {
				c, e := a.dialTunnel(context.Background(), "remote.invalid:443", mustURL(t, up.URL))
				if e != nil {
					t.Fatal(e)
				}
				c.SetDeadline(time.Now().Add(3 * time.Second))
				c.Write([]byte("confirm"))
				got := make([]byte, 7)
				_, e = io.ReadFull(c, got)
				c.Close()
				if e != nil || string(got) != "confirm" {
					t.Fatal("tunnel confirmation lost", e)
				}
			}
			t.Logf("tls=%d handshakes=%d resumed=%d total_ms=%.3f", version, count, resumed.Load(), float64(time.Since(start))/float64(time.Millisecond))
			if os.Getenv("AG_TEST_ALLOW_NO_TLS_RESUME") == "" && resumed.Load() < count-1 {
				t.Fatalf("session cache unused: %d/%d resumed", resumed.Load(), count-1)
			}
		})
	}
}

func BenchmarkProxyTLSConfigCA(b *testing.B) {
	up := httptest.NewTLSServer(http.NotFoundHandler())
	defer up.Close()
	ca := filepath.Join(b.TempDir(), "ca.pem")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: up.Certificate().Raw}), 0600)
	a := &app{cfg: Config{ProxyCA: ca}}
	if _, e := a.tlsConfig("localhost"); e != nil {
		b.Fatal(e)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, e := a.tlsConfig("localhost"); e != nil {
			b.Fatal(e)
		}
	}
}
