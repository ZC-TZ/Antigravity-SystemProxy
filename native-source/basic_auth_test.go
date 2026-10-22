//go:build windows

package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBasicBareCredentialEqualsDoesNotSelectDirect(t *testing.T) {
	for _, value := range []string{"user:p=word@localhost:7890", "user:p==@localhost:7890"} {
		u, e := choose(Settings{true, value, ""}, "remote.invalid", "https", 17891)
		if e != nil || u == nil {
			t.Fatalf("credential parsed as direct route: %v", e)
		}
		pw, _ := u.User.Password()
		if !strings.Contains(pw, "=") {
			t.Fatal("password changed")
		}
	}
	for _, value := range []string{"http=http://user:p;tail@localhost:7890;https=http://user:p=word@localhost:7891;", "http=localhost:7890;;https=localhost:7891;;"} {
		u, e := choose(Settings{true, value, ""}, "remote.invalid", "https", 17891)
		if e != nil || u == nil || u.Port() != "7891" {
			t.Fatal("protocol map/trailing separator compatibility lost", e)
		}
	}
}
func TestBasicInvalidCredentialSyntax(t *testing.T) {
	for _, tc := range []struct{ user, pw string }{{"user:part", "pw"}, {"user", "bad\r\n"}, {"user\x00", "pw"}, {"user", "bad\x7f"}} {
		u := &url.URL{Scheme: "http", Host: "localhost:7890", User: url.UserPassword(tc.user, tc.pw)}
		if _, e := choose(Settings{true, u.String(), ""}, "remote.invalid", "https", 17891); e == nil {
			t.Error("invalid Basic credentials accepted")
		}
	}
	for _, tc := range []struct{ user, pw string }{{"user", ""}, {"", "pw"}, {"", ""}, {"user", "p:;=@ /瀵嗙⒓"}} {
		u := &url.URL{Scheme: "https", Host: "localhost:7890", User: url.UserPassword(tc.user, tc.pw)}
		if _, e := choose(Settings{true, u.String(), ""}, "remote.invalid", "https", 17891); e != nil {
			t.Fatal("valid Basic credentials rejected", e)
		}
	}
}
func TestBasicStatusRedactionCoversSemicolonAndInvalidInput(t *testing.T) {
	for _, value := range []string{"http://user:secret;tail@localhost:7890", "user:secret;tail@localhost:7890", "http=http://user:secret;tail@localhost:7890;https=http://user:secret@localhost:7891", "http://user:secret/part@localhost:7890", "http://user:secret tail@localhost:7890"} {
		got := redactProxyServer(value)
		if strings.Contains(got, "user") || strings.Contains(got, "secret") {
			t.Errorf("status leaks credential prefix for input length %d", len(value))
		}
	}
}
func TestBasicCONNECT407DoesNotDrainErrorBody(t *testing.T) {
	closed := make(chan struct{})
	l := echoListener(t, func(c net.Conn) {
		defer close(closed)
		if _, e := http.ReadRequest(bufio.NewReader(c)); e != nil {
			return
		}
		io.WriteString(c, "HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 1000000\r\nProxy-Authenticate: Basic realm=upstream\r\n\r\n")
		io.Copy(io.Discard, c)
	})
	u, _ := url.Parse("http://user:wrong@" + l.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	started := time.Now()
	conn, e := (&app{}).dialTunnel(ctx, "remote.invalid:443", u)
	if conn != nil {
		conn.Close()
	}
	if e == nil || time.Since(started) > 350*time.Millisecond {
		t.Fatalf("407 blocked draining body: %v %v", time.Since(started), e)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("rejected proxy socket leaked")
	}
}
func TestBasicHTTP407DoesNotChallengeLocalClientOrReplay(t *testing.T) {
	var calls atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Proxy-Authenticate", "Basic realm=upstream")
		w.WriteHeader(407)
		io.WriteString(w, "sensitive-error-body")
	}))
	defer up.Close()
	proxyURL := mustURL(t, up.URL)
	proxyURL.User = url.UserPassword("user", "wrong")
	_, p, _ := fixture(t, Settings{true, proxyURL.String(), ""})
	client := streamingClient(t, p, up)
	response, e := client.Post("http://remote.invalid/upload", "text/plain", strings.NewReader("once"))
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 502 || response.Header.Get("Proxy-Authenticate") != "" || strings.Contains(string(body), "sensitive") {
		t.Fatalf("upstream auth response leaked: %d", response.StatusCode)
	}
	if calls.Load() != 1 {
		t.Fatal("failed authenticated POST replayed")
	}
	// An absolute HTTPS URL uses Transport's own CONNECT callback rather than
	// the inbound CONNECT handler. It must classify 407 in the same way.
	u, _ := url.Parse(p.URL)
	raw, e := net.Dial("tcp", u.Host)
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	raw.SetDeadline(time.Now().Add(time.Second))
	io.WriteString(raw, "GET https://remote.invalid/ HTTP/1.1\r\nHost: remote.invalid\r\nConnection: close\r\n\r\n")
	next, e := http.ReadResponse(bufio.NewReader(raw), &http.Request{Method: "GET"})
	if e != nil {
		t.Fatal(e)
	}
	io.Copy(io.Discard, next.Body)
	next.Body.Close()
	if next.StatusCode != 502 || calls.Load() != 2 {
		t.Fatal("Transport CONNECT auth failure was not classified")
	}
}
func TestBasicSpecialCharactersAndAuthorizationIsolation(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			user, pw := "user@name", "p:;=+ /瀵嗙⒓"
			expected := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pw))
			up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Proxy-Authorization") != expected || r.Header.Get("Authorization") != "Bearer origin-secret" {
					http.Error(w, "bad credentials", 400)
					return
				}
				io.WriteString(w, "ok")
			}))
			if encrypted {
				up.StartTLS()
			} else {
				up.Start()
			}
			defer up.Close()
			u := mustURL(t, up.URL)
			u.User = url.UserPassword(user, pw)
			c, p, _ := fixture(t, Settings{true, u.String(), ""})
			if encrypted {
				roots := x509.NewCertPool()
				roots.AddCert(up.Certificate())
				c.transport.TLSClientConfig = &tls.Config{RootCAs: roots}
			}
			client := streamingClient(t, p, up)
			req, _ := http.NewRequest("POST", "http://remote.invalid/", strings.NewReader("file"))
			req.Header.Set("Authorization", "Bearer origin-secret")
			req.Header.Set("Proxy-Authorization", "Basic client-credential")
			response, e := client.Do(req)
			if e != nil {
				t.Fatal(e)
			}
			data, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != 200 || string(data) != "ok" {
				t.Fatal("encoded Basic credential changed", response.StatusCode)
			}
			// Switching to direct must not forward either upstream or local proxy auth.
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Proxy-Authorization") != "" {
					t.Error("proxy credential leaked to origin")
				}
				io.WriteString(w, "direct")
			}))
			defer origin.Close()
			c.updateSettings(Settings{})
			req, _ = http.NewRequest("GET", origin.URL, nil)
			req.Header.Set("Proxy-Authorization", "Basic local-secret")
			response, e = client.Do(req)
			if e != nil {
				t.Fatal(e)
			}
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
		})
	}
}

func TestBasicCredentialRotationRetiresOldConnections(t *testing.T) {
	for _, tunnel := range []bool{false, true} {
		t.Run(fmt.Sprint(tunnel), func(t *testing.T) {
			observed := make(chan string, 4)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				observed <- r.Header.Get("Proxy-Authorization")
				if r.Method == "CONNECT" {
					client, rw, e := w.(http.Hijacker).Hijack()
					if e != nil {
						return
					}
					defer client.Close()
					io.WriteString(rw, "HTTP/1.1 200 OK\r\n\r\n")
					rw.Flush()
					io.Copy(client, rw)
					return
				}
				io.WriteString(w, "ok")
			}))
			defer up.Close()
			u := mustURL(t, up.URL)
			u.User = url.UserPassword("user", "old")
			before := Settings{true, u.String(), ""}
			c, p, _ := fixture(t, before)
			var first net.Conn
			var reader *bufio.Reader
			if tunnel {
				first, reader = connectTest(t, p, "remote.invalid:443")
				defer first.Close()
			} else {
				client := streamingClient(t, p, up)
				resp, e := client.Get("http://remote.invalid/")
				if e != nil {
					t.Fatal(e)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			if got := <-observed; got != "Basic "+base64.StdEncoding.EncodeToString([]byte("user:old")) {
				t.Fatal("initial auth changed")
			}
			u.User = url.UserPassword("user", "new")
			c.updateSettings(Settings{true, u.String(), ""})
			if tunnel {
				first.SetReadDeadline(time.Now().Add(time.Second))
				if _, e := reader.ReadByte(); e == nil {
					t.Fatal("old authenticated tunnel survived credential change")
				} else if ne, ok := e.(net.Error); ok && ne.Timeout() {
					t.Fatal("old authenticated tunnel only timed out instead of closing")
				}
				next, _ := connectTest(t, p, "remote.invalid:443")
				defer next.Close()
			} else {
				client := streamingClient(t, p, up)
				resp, e := client.Get("http://remote.invalid/")
				if e != nil {
					t.Fatal(e)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if c.dials.Load() != 2 {
					t.Fatal("old authenticated HTTP pool survived credential change")
				}
			}
			if got := <-observed; got != "Basic "+base64.StdEncoding.EncodeToString([]byte("user:new")) {
				t.Fatal("new request used stale credentials")
			}
		})
	}
}
func TestBasicCONNECTSpecialCharactersOverHTTPAndHTTPS(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			user, pw := "user@name", "p:;=+ /密碼"
			expected := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pw))
			up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "CONNECT" || r.Header.Get("Proxy-Authorization") != expected {
					http.Error(w, "bad auth", 407)
					return
				}
				conn, rw, e := w.(http.Hijacker).Hijack()
				if e != nil {
					return
				}
				defer conn.Close()
				io.WriteString(rw, "HTTP/1.1 200 OK\r\n\r\n")
				rw.Flush()
				io.Copy(conn, rw)
			}))
			if encrypted {
				up.StartTLS()
			} else {
				up.Start()
			}
			defer up.Close()
			u := mustURL(t, up.URL)
			u.User = url.UserPassword(user, pw)
			c, p, _ := fixture(t, Settings{true, u.String(), ""})
			if encrypted {
				path := filepath.Join(t.TempDir(), "ca.pem")
				if e := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: up.Certificate().Raw}), 0600); e != nil {
					t.Fatal(e)
				}
				c.a.cfg.ProxyCA = path
			}
			conn, reader := connectTest(t, p, "remote.invalid:443")
			defer conn.Close()
			conn.Write([]byte("hello"))
			got := make([]byte, 5)
			if _, e := io.ReadFull(reader, got); e != nil || string(got) != "hello" {
				t.Fatal("Basic CONNECT changed bytes", e)
			}
		})
	}
}
func TestBasicOrigin401RemainsOriginAuthentication(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", "Basic realm=origin")
		w.WriteHeader(401)
		io.WriteString(w, "origin-login")
	}))
	defer origin.Close()
	c, p, _ := fixture(t, Settings{})
	response, e := streamingClient(t, p, origin).Get(origin.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if response.StatusCode != 401 || response.Header.Get("WWW-Authenticate") == "" || string(data) != "origin-login" || c.metrics.authFailures.Load() != 0 {
		t.Fatal("origin authentication misclassified")
	}
}
func TestBasicStatusValidationUsesOriginalCredentials(t *testing.T) {
	root := t.TempDir()
	settings := filepath.Join(root, "settings.json")
	if e := os.WriteFile(settings, []byte(`{"enabled":true,"server":"socks5://user:secret@localhost:7890"}`), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("AG_SYSTEM_PROXY_STATE_DIR", root)
	t.Setenv("AG_SYSTEM_PROXY_SETTINGS_FILE", settings)
	t.Setenv("AG_SYSTEM_PROXY_PORT", "17891")
	oldArgs, oldOut := os.Args, os.Stdout
	defer func() { os.Args, os.Stdout = oldArgs, oldOut }()
	read, write, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer read.Close()
	os.Args = []string{oldArgs[0], "--status"}
	os.Stdout = write
	e = run()
	write.Close()
	os.Stdout = oldOut
	if e != nil {
		t.Fatal(e)
	}
	data, e := io.ReadAll(read)
	if e != nil {
		t.Fatal(e)
	}
	var status map[string]any
	if e = json.Unmarshal(data, &status); e != nil {
		t.Fatal(e)
	}
	if status["routing_config_valid"] != true || strings.Contains(string(data), "secret") {
		t.Fatal("status validated masked SOCKS credentials or leaked secret")
	}
}
