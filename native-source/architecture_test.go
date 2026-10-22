//go:build windows

package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
)

func TestProxiedWebSocketSurvivesUnrelatedBypassEdit(t *testing.T) {
	origin := websocketOrigin(t, false, func(c net.Conn, rw *bufio.ReadWriter) { io.Copy(c, rw) })
	before := Settings{true, origin.URL, ""}
	c, p, _ := fixture(t, before)
	client, reader := websocketClient(t, p, "http://example.test/agent")
	defer client.Close()
	c.updateSettings(Settings{true, origin.URL, "unrelated.invalid"})
	want := maskedFrame(0x89, "still-alive")
	client.Write(want)
	got := make([]byte, len(want))
	_, e := io.ReadFull(reader, got)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatal("unchanged proxied WebSocket route was interrupted", e)
	}
}

func TestWindowsSettingsReadErrorsAreNotProxyDisableEvents(t *testing.T) {
	failure := errors.New("registry temporarily inaccessible")
	if _, e := windowsSettings(func() (bool, error) { return false, failure }, func(string) (string, error) { t.Fatal("read after enable failure"); return "", nil }); !errors.Is(e, failure) {
		t.Fatal("enable read failure became a disable event")
	}
	if _, e := windowsSettings(func() (bool, error) { return true, nil }, func(string) (string, error) { return "", failure }); !errors.Is(e, failure) {
		t.Fatal("address read failure was swallowed")
	}
	state, e := windowsSettings(func() (bool, error) { return false, nil }, func(string) (string, error) { t.Fatal("disabled proxy read unused address"); return "", nil })
	if e != nil || state.Enabled {
		t.Fatal("explicit system proxy off was not honored")
	}
}
func TestEffectiveRouteEquality(t *testing.T) {
	for _, tc := range []struct {
		before, after Settings
		want          bool
	}{
		{Settings{}, Settings{false, "unused:9", "anything"}, true},
		{Settings{true, "127.0.0.1:7890", ""}, Settings{true, "http://127.0.0.1:7890/", "other.invalid"}, true},
		{Settings{true, "127.0.0.1:7890", ""}, Settings{true, "http://127.0.0.1:7890", "other.invalid"}, true},
		{Settings{true, "127.0.0.1:7890", "example.test"}, Settings{true, "127.0.0.1:7891", "example.test"}, true},
		{Settings{true, "127.0.0.1:7890", ""}, Settings{}, false},
		{Settings{true, "http://u:old@127.0.0.1:7890", ""}, Settings{true, "http://u:new@127.0.0.1:7890", ""}, false},
	} {
		if got := sameRoute(tc.before, tc.after, destination{"example.test", "https"}, 17891); got != tc.want {
			t.Fatalf("effective route equality got %v want %v", got, tc.want)
		}
	}
}
func TestRoutingReadinessDoesNotConfuseDisabledProxyWithInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		s     Settings
		valid bool
	}{
		{Settings{}, true}, {Settings{false, "unsupported://ignored", ""}, true},
		{Settings{true, "", "*"}, false}, {Settings{true, "127.0.0.1:17891", ""}, false},
		{Settings{true, "localhost.:17891", ""}, false},
		{Settings{true, "socks=localhost:1080", ""}, false},
		{Settings{true, "http=127.0.0.1:7890;https=127.0.0.1:7891", ""}, true},
	} {
		if got := validateSettings(tc.s, 17891) == nil; got != tc.valid {
			t.Fatalf("readiness got %v want %v", got, tc.valid)
		}
	}
}
