//go:build windows

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Compare protocol costs with one upstream and checked body sizes, separately
// from the external EXE benchmark. Parameters affect this fixture only.
func BenchmarkHTTPBulk(b *testing.B) {
	payload := bytes.Repeat([]byte("payload-integrity"), 1<<20)
	for _, method := range []string{"upload", "download"} {
		for _, variant := range []struct {
			name        string
			flush       time.Duration
			writeBuffer int
		}{{"immediate_4KiB", -1, 4096}, {"automatic_4KiB", 0, 4096}, {"immediate_64KiB", -1, 65536}, {"automatic_64KiB", 0, 65536}} {
			b.Run(method+"/"+variant.name, func(b *testing.B) {
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if method == "upload" {
						h := sha256.New()
						n, e := io.Copy(h, r.Body)
						if e != nil || n != int64(len(payload)) {
							http.Error(w, "incomplete", 400)
							return
						}
						w.Header().Set("Content-Length", "2")
						io.WriteString(w, "ok")
						return
					}
					w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
					_, _ = w.Write(payload)
				}))
				defer upstream.Close()
				root := b.TempDir()
				settings := filepath.Join(root, "settings.json")
				data, _ := json.Marshal(Settings{Enabled: true, Server: upstream.URL})
				if e := os.WriteFile(settings, data, 0600); e != nil {
					b.Fatal(e)
				}
				app := &app{root: root, settingsFile: settings, cfg: Config{Port: 17891, Idle: 3600, MaxConnections: 128, MaxPending: 64, QueueWait: 2}}
				core, e := newCore(app)
				if e != nil {
					b.Fatal(e)
				}
				defer core.close()
				core.proxy.FlushInterval = variant.flush
				core.transport.WriteBufferSize = variant.writeBuffer
				proxy := httptest.NewServer(core)
				defer proxy.Close()
				u, _ := url.Parse(proxy.URL)
				tr := &http.Transport{Proxy: http.ProxyURL(u), DisableCompression: true}
				defer tr.CloseIdleConnections()
				client := &http.Client{Transport: tr, Timeout: 10 * time.Second}
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					var body io.Reader
					verb := "GET"
					if method == "upload" {
						verb = "POST"
						body = io.LimitReader(bytes.NewReader(payload), int64(len(payload)))
					}
					req, _ := http.NewRequest(verb, "http://bench.invalid/data", body)
					resp, e := client.Do(req)
					if e != nil {
						b.Fatal(e)
					}
					n, e := io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					want := int64(len(payload))
					if method == "upload" {
						want = 2
					}
					if e != nil || resp.StatusCode != 200 || n != want {
						b.Fatal(n, e, resp.StatusCode)
					}
				}
			})
		}
	}
}
