//go:build windows

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUploadWriteProgressSurvivesIdleInterval(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	activity := &atomic.Int64{}
	activity.Store(time.Now().UnixNano())
	c := &idleConn{Conn: left, activity: activity, timeout: 30 * time.Millisecond}
	payload := bytes.Repeat([]byte("0123456789abcdef"), 512)
	received := make(chan []byte, 1)
	go func() {
		var got bytes.Buffer
		b := make([]byte, 256)
		for got.Len() < len(payload) {
			n, e := right.Read(b)
			got.Write(b[:n])
			if e != nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		received <- got.Bytes()
	}()
	n, e := c.Write(payload)
	if e != nil || n != len(payload) {
		t.Fatalf("progressing upload aborted: %d/%d %v", n, len(payload), e)
	}
	select {
	case got := <-received:
		if !bytes.Equal(got, payload) {
			t.Fatal("resuming write duplicated or lost bytes")
		}
	case <-time.After(time.Second):
		t.Fatal("upload did not complete")
	}
}
func TestUploadExplicitDeadlineIsNotOverridden(t *testing.T) {
	for _, write := range []bool{false, true} {
		t.Run(fmt.Sprint(write), func(t *testing.T) {
			left, right := net.Pipe()
			defer left.Close()
			defer right.Close()
			activity := &atomic.Int64{}
			activity.Store(time.Now().UnixNano())
			c := &idleConn{Conn: left, activity: activity, timeout: 300 * time.Millisecond}
			c.SetDeadline(time.Now().Add(30 * time.Millisecond))
			start := time.Now()
			var e error
			if write {
				_, e = c.Write([]byte("blocked upload"))
			} else {
				_, e = c.Read(make([]byte, 1))
			}
			if e == nil || time.Since(start) > 180*time.Millisecond {
				t.Fatalf("caller deadline overridden: %v %v", time.Since(start), e)
			}
		})
	}
}
func TestUploadDeadlineCanChangeAndClear(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	activity := &atomic.Int64{}
	activity.Store(time.Now().UnixNano())
	c := &idleConn{Conn: left, activity: activity, timeout: 300 * time.Millisecond}
	done := make(chan error, 1)
	go func() { _, e := c.Read(make([]byte, 1)); done <- e }()
	time.Sleep(10 * time.Millisecond)
	c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("deadline ignored")
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("changed read deadline ignored")
	}
	c.SetDeadline(time.Time{})
	go right.Write([]byte("a"))
	b := make([]byte, 1)
	if _, e := c.Read(b); e != nil || b[0] != 'a' {
		t.Fatal("cleared deadline stayed expired", e)
	}
	// Removing a caller deadline must retain the wrapper's own idle protection,
	// including a Read already blocked in the underlying socket.
	c.timeout = 40 * time.Millisecond
	go func() { _, e := c.Read(make([]byte, 1)); done <- e }()
	time.Sleep(10 * time.Millisecond)
	c.SetReadDeadline(time.Time{})
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("idle read succeeded")
		}
	case <-time.After(180 * time.Millisecond):
		t.Fatal("clearing deadline disabled idle protection")
	}
}

type repeatedUploadReader struct{ value byte }

func (r repeatedUploadReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.value
	}
	return len(p), nil
}
func uploadDigest(n int64) string {
	h := sha256.New()
	io.Copy(h, io.LimitReader(repeatedUploadReader{'x'}, n))
	return hex.EncodeToString(h.Sum(nil))
}

func TestLargeUploadStreamingIntegrity(t *testing.T) {
	const size = int64(32 << 20)
	want := uploadDigest(size)
	for _, encrypted := range []bool{false, true} {
		for _, chunked := range []bool{false, true} {
			t.Run(fmt.Sprintf("tls=%v/chunked=%v", encrypted, chunked), func(t *testing.T) {
				prefix := make(chan struct{})
				gate := make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(gate) }) }
				defer release()
				origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					h := sha256.New()
					n, e := io.CopyN(h, r.Body, 64<<10)
					close(prefix)
					if e == nil {
						var rest int64
						rest, e = io.Copy(h, r.Body)
						n += rest
					}
					if e != nil || n != size || hex.EncodeToString(h.Sum(nil)) != want {
						http.Error(w, "bad upload", 400)
						return
					}
					io.WriteString(w, "verified")
				}))
				if encrypted {
					origin.EnableHTTP2 = true
					origin.StartTLS()
				} else {
					origin.Start()
				}
				defer origin.Close()
				c, p, _ := fixture(t, Settings{})
				client := streamingClient(t, p, origin)
				client.Timeout = 20 * time.Second
				read, write := io.Pipe()
				defer read.Close()
				defer write.Close()
				produced := make(chan error, 1)
				go func() {
					_, e := io.CopyN(write, repeatedUploadReader{'x'}, 64<<10)
					if e == nil {
						<-gate
						_, e = io.CopyN(write, repeatedUploadReader{'x'}, size-(64<<10))
					}
					write.CloseWithError(e)
					produced <- e
				}()
				req, _ := http.NewRequest("POST", origin.URL+"/upload", read)
				if !chunked {
					req.ContentLength = size
				}
				req.Header.Set("Content-Type", "application/octet-stream")
				result := make(chan error, 1)
				go func() {
					response, e := client.Do(req)
					if e == nil {
						b, err := io.ReadAll(response.Body)
						response.Body.Close()
						e = err
						if response.StatusCode != 200 || string(b) != "verified" {
							e = fmt.Errorf("upload rejected: %d %q", response.StatusCode, b)
						}
					}
					result <- e
				}()
				select {
				case <-prefix:
					release()
				case <-time.After(2 * time.Second):
					release()
					t.Fatal("proxy buffered upload instead of streaming prefix")
				}
				if e := <-result; e != nil {
					t.Fatal(e)
				}
				if e := <-produced; e != nil {
					t.Fatal(e)
				}
				if !encrypted {
					await(t, func() bool { return len(c.slots) == 0 })
				}
			})
		}
	}
}
func TestMultiFileMultipartIntegrity(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			const count = 1200
			origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reader, e := r.MultipartReader()
				if e != nil {
					http.Error(w, "missing boundary", 400)
					return
				}
				for i := 0; i < count; i++ {
					part, e := reader.NextPart()
					if e != nil {
						http.Error(w, "missing file", 400)
						return
					}
					data, e := io.ReadAll(part)
					part.Close()
					want := strings.Repeat(fmt.Sprint(i), i%33)
					if e != nil || part.FormName() != "files" || part.FileName() != fmt.Sprintf("文件-%d.bin", i) || string(data) != want {
						http.Error(w, "changed part", 400)
						return
					}
				}
				if _, e = reader.NextPart(); e != io.EOF {
					http.Error(w, "extra part", 400)
					return
				}
				io.WriteString(w, "verified")
			}))
			if encrypted {
				origin.EnableHTTP2 = true
				origin.StartTLS()
			} else {
				origin.Start()
			}
			defer origin.Close()
			_, p, _ := fixture(t, Settings{})
			client := streamingClient(t, p, origin)
			read, write := io.Pipe()
			defer read.Close()
			defer write.Close()
			multipartWriter := multipart.NewWriter(write)
			done := make(chan error, 1)
			go func() {
				var e error
				for i := 0; i < count && e == nil; i++ {
					var part io.Writer
					part, e = multipartWriter.CreateFormFile("files", fmt.Sprintf("文件-%d.bin", i))
					if e == nil {
						_, e = io.WriteString(part, strings.Repeat(fmt.Sprint(i), i%33))
					}
				}
				if e == nil {
					e = multipartWriter.Close()
				}
				write.CloseWithError(e)
				done <- e
			}()
			request, _ := http.NewRequest("POST", origin.URL, read)
			request.Header.Set("Content-Type", multipartWriter.FormDataContentType())
			response, e := client.Do(request)
			if e != nil {
				t.Fatal(e)
			}
			data, e := io.ReadAll(response.Body)
			response.Body.Close()
			if e != nil || response.StatusCode != 200 || string(data) != "verified" {
				t.Fatalf("multipart corrupted: %d %s %v", response.StatusCode, data, e)
			}
			if e := <-done; e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestUploadExpectContinueAndEarlyRejection(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint(reject), func(t *testing.T) {
			origin := echoListener(t, func(c net.Conn) {
				reader := bufio.NewReader(c)
				r, e := http.ReadRequest(reader)
				if e != nil {
					return
				}
				if reject {
					io.WriteString(c, "HTTP/1.1 413 Content Too Large\r\nContent-Length: 2\r\nConnection: close\r\n\r\nno")
					return
				}
				io.WriteString(c, "HTTP/1.1 100 Continue\r\n\r\n")
				data, e := io.ReadAll(r.Body)
				if e != nil || string(data) != "file" {
					return
				}
				io.WriteString(c, "HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nok")
			})
			c, p, _ := fixture(t, Settings{})
			u, _ := url.Parse(p.URL)
			raw, e := net.Dial("tcp", u.Host)
			if e != nil {
				t.Fatal(e)
			}
			defer raw.Close()
			raw.SetDeadline(time.Now().Add(2 * time.Second))
			fmt.Fprintf(raw, "POST http://%s/upload HTTP/1.1\r\nHost: %s\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n", origin.Addr(), origin.Addr())
			reader := bufio.NewReader(raw)
			response, e := http.ReadResponse(reader, &http.Request{Method: "POST"})
			if e != nil {
				t.Fatal(e)
			}
			if !reject {
				if response.StatusCode != 100 {
					t.Fatalf("missing continue: %d", response.StatusCode)
				}
				io.WriteString(raw, "file")
				response, e = http.ReadResponse(reader, &http.Request{Method: "POST"})
				if e != nil {
					t.Fatal(e)
				}
			}
			data, e := io.ReadAll(response.Body)
			response.Body.Close()
			want := 201
			if reject {
				want = 413
			}
			if e != nil || response.StatusCode != want || len(data) != 2 {
				t.Fatal("upload final reply lost", response.StatusCode, e)
			}
			await(t, func() bool { return len(c.slots) == 0 })
		})
	}
}
func TestUploadCancelDoesNotReplayOrLeak(t *testing.T) {
	seen := make(chan struct{}, 1)
	finished := make(chan struct{}, 1)
	var requests atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		b := make([]byte, 4)
		if _, e := io.ReadFull(r.Body, b); e == nil {
			seen <- struct{}{}
		}
		io.Copy(io.Discard, r.Body)
		finished <- struct{}{}
	}))
	defer origin.Close()
	c, p, _ := fixture(t, Settings{})
	client := streamingClient(t, p, origin)
	read, write := io.Pipe()
	defer read.Close()
	defer write.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", origin.URL, read)
	result := make(chan error, 1)
	go func() {
		r, e := client.Do(req)
		if r != nil {
			r.Body.Close()
		}
		result <- e
	}()
	write.Write([]byte("file"))
	select {
	case <-seen:
	case <-time.After(time.Second):
		t.Fatal("upload not started")
	}
	cancel()
	write.CloseWithError(context.Canceled)
	select {
	case e := <-result:
		if e == nil {
			t.Fatal("cancelled upload succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel blocked")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("upstream upload leaked")
	}
	await(t, func() bool { return len(c.slots) == 0 })
	if requests.Load() != 1 {
		t.Fatal("upload replayed")
	}
}

func TestManySmallUploadsConcurrent(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			var mu sync.Mutex
			seen := map[string]bool{}
			origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, e := io.ReadAll(r.Body)
				id := r.Header.Get("X-Upload-Id")
				expected := strings.Repeat(id, len(id)%5*100)
				mu.Lock()
				duplicate := seen[id]
				seen[id] = true
				mu.Unlock()
				if duplicate || e != nil || string(data) != expected {
					http.Error(w, "changed or duplicate upload", 400)
					return
				}
				w.WriteHeader(201)
			}))
			if encrypted {
				origin.EnableHTTP2 = true
				origin.StartTLS()
			} else {
				origin.Start()
			}
			defer origin.Close()
			c, p, _ := fixture(t, Settings{})
			client := streamingClient(t, p, origin)
			var workers sync.WaitGroup
			failures := make(chan error, 24)
			for worker := 0; worker < 24; worker++ {
				workers.Add(1)
				go func(worker int) {
					defer workers.Done()
					for file := 0; file < 4; file++ {
						id := fmt.Sprintf("%d/%d", worker, file)
						request, _ := http.NewRequest("POST", origin.URL, strings.NewReader(strings.Repeat(id, len(id)%5*100)))
						request.Header.Set("X-Upload-Id", id)
						response, e := client.Do(request)
						if e != nil {
							failures <- e
							return
						}
						io.Copy(io.Discard, response.Body)
						response.Body.Close()
						if response.StatusCode != 201 {
							failures <- fmt.Errorf("small upload status %d", response.StatusCode)
							return
						}
					}
				}(worker)
			}
			workers.Wait()
			close(failures)
			for e := range failures {
				t.Error(e)
			}
			mu.Lock()
			count := len(seen)
			mu.Unlock()
			if count != 96 {
				t.Fatalf("received %d/96 uploads", count)
			}
			if !encrypted {
				await(t, func() bool { return len(c.slots) == 0 && len(c.pending) == 0 })
			}
		})
	}
}

func TestUploadTruncatedBodyDoesNotSucceedOrReplay(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprint(chunked), func(t *testing.T) {
			var requests atomic.Int64
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, e := io.Copy(io.Discard, r.Body)
				if e != nil {
					http.Error(w, "incomplete upload", 400)
					return
				}
				w.WriteHeader(201)
			}))
			defer origin.Close()
			c, p, _ := fixture(t, Settings{})
			u, _ := url.Parse(p.URL)
			raw, e := net.Dial("tcp", u.Host)
			if e != nil {
				t.Fatal(e)
			}
			defer raw.Close()
			raw.SetDeadline(time.Now().Add(2 * time.Second))
			body := "Content-Length: 128\r\n\r\nfile"
			if chunked {
				body = "Transfer-Encoding: chunked\r\n\r\n80\r\nfile"
			}
			fmt.Fprintf(raw, "POST %s HTTP/1.1\r\nHost: %s\r\n%s", origin.URL, mustURL(t, origin.URL).Host, body)
			raw.(*net.TCPConn).CloseWrite()
			response, e := http.ReadResponse(bufio.NewReader(raw), &http.Request{Method: "POST"})
			if e == nil {
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode >= 200 && response.StatusCode < 300 {
					t.Fatal("truncated upload acknowledged as success")
				}
			}
			await(t, func() bool { return len(c.slots) == 0 })
			if requests.Load() > 1 {
				t.Fatal("truncated upload replayed")
			}
		})
	}
}

func TestUploadPartialWriteHonorsHardDeadline(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	activity := &atomic.Int64{}
	activity.Store(time.Now().UnixNano())
	c := &idleConn{Conn: left, activity: activity, timeout: 300 * time.Millisecond}
	payload := bytes.Repeat([]byte("0123456789abcdef"), 512)
	done := make(chan []byte, 1)
	go func() {
		var got bytes.Buffer
		buffer := make([]byte, 256)
		for {
			n, e := right.Read(buffer)
			got.Write(buffer[:n])
			if e != nil {
				done <- got.Bytes()
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	c.SetWriteDeadline(time.Now().Add(30 * time.Millisecond))
	start := time.Now()
	n, e := c.Write(payload)
	left.Close()
	if e == nil || n == 0 || n == len(payload) || time.Since(start) > 180*time.Millisecond {
		t.Fatalf("hard deadline/progress result: n=%d e=%v", n, e)
	}
	select {
	case got := <-done:
		if !bytes.Equal(got, payload[:n]) {
			t.Fatal("partial write count did not match accepted bytes")
		}
	case <-time.After(time.Second):
		t.Fatal("reader leaked")
	}
}

func TestConcurrentPartialUploadWritesRemainSerialized(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	activity := &atomic.Int64{}
	activity.Store(time.Now().UnixNano())
	c := &idleConn{Conn: left, activity: activity, timeout: 20 * time.Millisecond}
	a, b := bytes.Repeat([]byte("a"), 4096), bytes.Repeat([]byte("b"), 4096)
	first := make(chan struct{})
	received := make(chan []byte, 1)
	go func() {
		var got bytes.Buffer
		buffer := make([]byte, 256)
		for got.Len() < len(a)+len(b) {
			n, e := right.Read(buffer)
			got.Write(buffer[:n])
			if got.Len() == n {
				close(first)
			}
			if e != nil {
				break
			}
			time.Sleep(3 * time.Millisecond)
		}
		received <- got.Bytes()
	}()
	results := make(chan error, 2)
	go func() { _, e := c.Write(a); results <- e }()
	<-first
	go func() { _, e := c.Write(b); results <- e }()
	for i := 0; i < 2; i++ {
		if e := <-results; e != nil {
			t.Fatal(e)
		}
	}
	got := <-received
	if !bytes.Equal(got, append(a, b...)) {
		t.Fatal("concurrent resumed writes interleaved")
	}
}
