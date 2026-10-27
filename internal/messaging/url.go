package messaging

import "net/url"

// BrokerURL is the HTTP base URL a client reaches a local broker on at addr, or
// "" when addr is empty. A scoped IPv6 zone is escaped, so the result parses
// back to the bound host.
func BrokerURL(addr string) string {
	if addr == "" {
		return ""
	}
	return (&url.URL{Scheme: "http", Host: addr}).String()
}
