package messaging

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// BrokerURL is the HTTP base URL a client reaches a local broker on at addr, or
// "" when addr is empty. A scoped IPv6 zone is escaped, so the result parses
// back to the bound host.
func BrokerURL(addr string) string {
	if addr == "" {
		return ""
	}
	return (&url.URL{Scheme: "http", Host: addr}).String()
}

// ParseBrokerURL validates an operator-supplied broker base URL and returns it
// in canonical form, or "" when raw is blank because no URL was supplied. The
// URL must be one a peer can post broker routes to: absolute http or https, a
// host, and nothing after it. A path would be prepended to every route, a
// query would be swallowed by the first one, and userinfo would travel as a
// credential the broker never authenticates against. One cosmetic trailing
// slash is accepted and dropped.
func ParseBrokerURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("%q is not a URL: %v", trimmed, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("%q must be an http:// or https:// URL", trimmed)
	}
	if parsed.Hostname() == "" {
		return "", fmt.Errorf("%q names no host", trimmed)
	}
	if ip := net.ParseIP(parsed.Hostname()); ip != nil && ip.IsUnspecified() {
		return "", fmt.Errorf("%q names an unspecified host", trimmed)
	}
	port := parsed.Port()
	if strings.HasSuffix(parsed.Host, ":") || port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", fmt.Errorf("%q has an invalid port", trimmed)
		}
	}
	if parsed.User != nil {
		return "", fmt.Errorf("%q carries credentials; broker routes authenticate per run", trimmed)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", fmt.Errorf("%q carries a query; broker routes append their own path", trimmed)
	}
	if parsed.Fragment != "" {
		return "", fmt.Errorf("%q carries a fragment; a fragment never reaches the broker", trimmed)
	}
	if path := parsed.EscapedPath(); path != "" && path != "/" {
		return "", fmt.Errorf("%q must be the broker's root URL; a path prefix would be prepended to every route", trimmed)
	}
	return (&url.URL{Scheme: parsed.Scheme, Host: parsed.Host}).String(), nil
}
