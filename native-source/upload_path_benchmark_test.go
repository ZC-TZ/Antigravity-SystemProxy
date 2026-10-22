//go:build windows

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func BenchmarkUploadPath(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 16<<20)
	for _, path := range []string{"worker", "entry"} {
		for _, known := range []bool{true, false} {
			for _, writeBuffer := range []int{4096, 32768, 65536, 262144} {
				b.Run(path+"/known="+strconv.FormatBool(known)+"/buffer="+strconv.Itoa(writeBuffer), func(b *testing.B) {
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						h := sha256.New()
						n, e := io.Copy(h, r.Body)
						if e != nil || n != int64(len(payload)) {
							http.Error(w, "incomplete", 400)
							return
						}
						w.Header().Set("Content-Length", "2")
						io.WriteString(w, "ok")
					}))
					defer upstream.Close()
					root := b.TempDir()
					settings := filepath.Join(root, "settings.json")
					data, _ := json.Marshal(Settings{Enabled: true, Server: upstream.URL})
					if e := os.WriteFile(settings, data, 0600); e != nil {
						b.Fatal(e)
					}
					a := &app{root: root, settingsFile: settings, cfg: Config{Port: 17891, Idle: 3600, MaxConnections: 128, MaxPending: 64, QueueWait: 2}}
					core, e := newCore(a)
					if e != nil {
						b.Fatal(e)
					}
					defer core.close()
					core.transport.WriteBufferSize = writeBuffer
					proxy := httptest.NewServer(core)
					defer proxy.Close()
					proxyURL := proxy.URL
					if path == "entry" {
						backend, _ := net.ResolveTCPAddr("tcp", proxy.Listener.Addr().String())
						front, e := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
						if e != nil {
							b.Fatal(e)
						}
						defer front.Close()
						entry := &entry{active: &entryWorker{record: record{DataPort: backend.Port}}}
						go func() {
							for {
								c, e := front.AcceptTCP()
								if e != nil {
									return
								}
								go entry.forward(c)
							}
						}()
						proxyURL = "http://" + front.Addr().String()
					}
					u, _ := url.Parse(proxyURL)
					tr := &http.Transport{Proxy: http.ProxyURL(u), DisableCompression: true}
					defer tr.CloseIdleConnections()
					client := &http.Client{Transport: tr, Timeout: 10 * time.Second}
					b.SetBytes(int64(len(payload)))
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						req, _ := http.NewRequest("POST", "http://bench.invalid/upload", io.LimitReader(bytes.NewReader(payload), int64(len(payload))))
						if known {
							req.ContentLength = int64(len(payload))
						}
						resp, e := client.Do(req)
						if e != nil {
							b.Fatal(e)
						}
						n, e := io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						if e != nil || n != 2 || resp.StatusCode != 200 {
							b.Fatal(n, e, resp.StatusCode)
						}
					}
				})
			}
		}
	}
}
