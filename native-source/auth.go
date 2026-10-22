//go:build windows

package main

import (
	"errors"
	"net/url"
	"sort"
	"strings"
)

var errProxyAuthentication = errors.New("upstream proxy authentication failed")

func validateBasicCredentials(u *url.URL) error {
	if u.User == nil {
		return nil
	}
	user := u.User.Username()
	pass, _ := u.User.Password()
	if strings.Contains(user, ":") {
		return errors.New("Basic username cannot contain a colon")
	}
	for _, v := range []string{user, pass} {
		for i := 0; i < len(v); i++ {
			if v[i] < 32 || v[i] == 127 {
				return errors.New("Basic credentials cannot contain control characters")
			}
		}
	}
	return nil
}

// Structural redaction also fails closed for invalid URLs; regex suffix masking
// can reveal the prefix of a password containing a delimiter or whitespace.
func redactProxyServer(server string) string {
	server = strings.TrimSpace(server)
	if server == "" {
		return ""
	}
	entries, mapped, e := proxyEntries(server)
	if e != nil {
		return "[redacted invalid proxy address]"
	}
	if !mapped {
		return redactProxyEntry(server)
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+redactProxyEntry(entries[key]))
	}
	return strings.Join(parts, ";")
}
func redactProxyEntry(value string) string {
	if value == "" {
		return ""
	}
	u, e := choose(Settings{true, value, ""}, "redaction.invalid", "https", 0)
	if e != nil || u == nil {
		return "[redacted invalid proxy address]"
	}
	if u.User == nil {
		return value
	}
	u.User = nil
	return strings.Replace(u.String(), "://", "://***@", 1)
}
