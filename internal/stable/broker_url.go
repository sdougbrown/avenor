package stable

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// BrokerURLPath is the discovery record a supervisor publishes beside its
// control socket once it owns that socket: one line holding the http:// URL of
// the broker it bound, actual port included.
func BrokerURLPath(socketPath string) string { return socketPath + ".broker.url" }

// validateBrokerAddr refuses a broker bind whose own address cannot be handed
// to anyone. The bound address is what children and parent branches are told to
// reach, so a wildcard bind would publish an unroutable destination; an
// operator must name a concrete interface address instead.
func validateBrokerAddr(addr string) error {
	if addr == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("broker address %q must be a host and port, e.g. 127.0.0.1:8787: %w", addr, err)
	}
	if host == "" {
		return wildcardBrokerAddrError(addr)
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return wildcardBrokerAddrError(addr)
	}
	return nil
}

func wildcardBrokerAddrError(addr string) error {
	return fmt.Errorf("broker address %q is a wildcard bind; bind a concrete interface address (e.g. 127.0.0.1:8787 or 192.0.2.10:8787) because children and parent branches are told to reach the bound address", addr)
}

// publishBrokerURL records the broker's actual address beside the control
// socket. Run calls it only after the control server owns the socket, so a
// competing supervisor that loses the socket never overwrites the live record.
func (s *Supervisor) publishBrokerURL() error {
	url := s.brokerURL()
	if url == "" || s.config.ControlSocket == "" {
		return nil
	}
	path := BrokerURLPath(s.config.ControlSocket)
	if err := durablefile.WriteFileAtomic(path, durablefile.TempPattern(path), []byte(url+"\n"), 0o600); err != nil {
		return fmt.Errorf("publish broker URL %s: %w", path, err)
	}
	s.brokerURLPath = path
	s.brokerURLValue = url
	return nil
}

// removeBrokerURL removes the record this supervisor published. It leaves a
// record whose contents do not match this supervisor's own URL alone: that
// record belongs to a replacement that owns the socket now.
func (s *Supervisor) removeBrokerURL() {
	path, url := s.brokerURLPath, s.brokerURLValue
	s.brokerURLPath, s.brokerURLValue = "", ""
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil || strings.TrimSpace(string(data)) != url {
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(s.stderrWriter(), "avenor stable: remove broker URL record %s: %v\n", path, err)
	}
}
