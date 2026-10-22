//go:build windows

package main

import (
	"errors"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var proxyMapStart = regexp.MustCompile(`(?i)^(http|https|ftp|socks)\s*=`)
var proxyMapSeparator = regexp.MustCompile(`(?i);\s*([a-z][a-z0-9+.-]*)\s*=`)

func proxyEntries(server string) (map[string]string, bool, error) {
	first := proxyMapStart.FindStringSubmatchIndex(server)
	if first == nil {
		return nil, false, nil
	}
	entries := map[string]string{}
	key := strings.ToLower(server[first[2]:first[3]])
	start := first[1]
	for _, sep := range proxyMapSeparator.FindAllStringSubmatchIndex(server, -1) {
		if _, exists := entries[key]; exists {
			return nil, true, errors.New("duplicate proxy protocol entry")
		}
		entries[key] = strings.Trim(server[start:sep[0]], "; \t\r\n")
		key = strings.ToLower(server[sep[2]:sep[3]])
		start = sep[1]
	}
	if _, exists := entries[key]; exists {
		return nil, true, errors.New("duplicate proxy protocol entry")
	}
	entries[key] = strings.Trim(server[start:], "; \t\r\n")
	return entries, true, nil
}

func wildcard(pattern, host string) bool {
	pattern, host = strings.ToLower(pattern), strings.ToLower(host)
	i, j, star, retry := 0, 0, -1, 0
	for j < len(host) {
		switch {
		case i < len(pattern) && pattern[i] == '*':
			star, retry = i, j
			i++
		case i < len(pattern) && pattern[i] == '?' && host[j] != '\n':
			_, n := utf8.DecodeRuneInString(host[j:])
			j += n
			i++
		case i < len(pattern) && pattern[i] == host[j]:
			i++
			j++
		case star >= 0 && retry < len(host) && host[retry] != '\n':
			_, n := utf8.DecodeRuneInString(host[retry:])
			retry += n
			j = retry
			i = star + 1
		default:
			return false
		}
	}
	for i < len(pattern) && pattern[i] == '*' {
		i++
	}
	return i == len(pattern)
}

func choose(s Settings, host, scheme string, localPort int, additionalPorts ...int) (*url.URL, error) {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if !s.Enabled || host == "localhost" {
		return nil, nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil, nil
	}
	for _, pattern := range strings.Split(s.Bypass, ";") {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if strings.EqualFold(pattern, "<local>") && !strings.ContainsAny(host, ".:") {
			return nil, nil
		}
		if wildcard(pattern, host) {
			return nil, nil
		}
	}
	server := strings.TrimSpace(s.Server)
	if server == "" {
		return nil, errors.New("system proxy enabled without an address")
	}
	entries, mapped, parseErr := proxyEntries(server)
	if parseErr != nil {
		return nil, parseErr
	}
	if mapped {
		server = entries[scheme]
		if server == "" {
			server = entries["socks"]
			if server != "" && !strings.Contains(server, "://") {
				return nil, errors.New("SOCKS version is ambiguous; use socks5://")
			}
		}
		if server == "" {
			return nil, nil
		}
	}

	if !strings.Contains(server, "://") {
		server = "http://" + server
	}
	u, e := url.Parse(server)
	if e != nil {
		return nil, errors.New("invalid proxy URL")
	}
	if u.Hostname() == "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("invalid proxy address")
	}
	var defaultPort string
	switch strings.ToLower(u.Scheme) {
	case "http":
		defaultPort = "80"
	case "https":
		defaultPort = "443"
	case "socks5", "socks5h":
		defaultPort = "1080"
	default:
		return nil, errors.New("unsupported proxy scheme")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme == "http" || u.Scheme == "https" {
		if e := validateBasicCredentials(u); e != nil {
			return nil, e
		}
	}
	if strings.ContainsAny(u.Hostname(), ";= \t\r\n") {
		return nil, errors.New("invalid proxy hostname")
	}
	if u.Scheme == "socks5" || u.Scheme == "socks5h" {
		if _, _, e := socksCredentials(u); e != nil {
			return nil, e
		}
	}
	p := u.Port()
	if p == "" {
		p = defaultPort
	}
	port, e := strconv.Atoi(p)
	if e != nil || port < 1 || port > 65535 {
		return nil, errors.New("invalid proxy port")
	}
	h := strings.ToLower(u.Hostname())
	if h == "localhost." {
		h = "localhost"
	}
	isLocalPort := port == localPort
	for _, p := range additionalPorts {
		isLocalPort = isLocalPort || port == p
	}
	if isLocalPort && (h == "localhost" || (net.ParseIP(h) != nil && net.ParseIP(h).IsLoopback())) {
		return nil, errors.New("proxy loop")
	}
	u.Host = net.JoinHostPort(h, p)
	u.Path = ""
	u.RawPath = ""
	return u, nil
}

func routeKey(u *url.URL) string {
	if u == nil {
		return "DIRECT"
	}
	return u.String()
}

func sameRoute(before, after Settings, dest destination, port int, additionalPorts ...int) bool {
	u, e := choose(before, dest.host, dest.scheme, port, additionalPorts...)
	if e != nil {
		return false
	}
	v, e := choose(after, dest.host, dest.scheme, port, additionalPorts...)
	return e == nil && routeKey(u) == routeKey(v)
}

func validateSettings(s Settings, port int, additionalPorts ...int) error {
	// Validate configuration independently of a particular target's bypass rule.
	s.Bypass = ""
	for _, scheme := range []string{"http", "https"} {
		if _, e := choose(s, "proxy-check.invalid", scheme, port, additionalPorts...); e != nil {
			return e
		}
	}
	return nil
}
