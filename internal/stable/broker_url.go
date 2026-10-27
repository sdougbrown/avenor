package stable

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// BrokerURLPath locates the one-line HTTP URL record beside the control socket.
func BrokerURLPath(socketPath string) string { return socketPath + ".broker.url" }

// Children and parent branches connect to the bound address, so it must not
// advertise a wildcard destination.
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
	if ip, err := netip.ParseAddr(host); err == nil && ip.WithZone("").Unmap().IsUnspecified() {
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

// removeBrokerURL runs before control.Stop releases the live socket, excluding
// replacement publishers. A changed record is left untouched.
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
