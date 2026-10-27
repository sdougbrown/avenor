package stable

import (
	"fmt"
	"os"
	"strings"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// BrokerURLPath locates the one-line HTTP URL record beside the control socket.
func BrokerURLPath(socketPath string) string { return socketPath + ".broker.url" }

// publishBrokerURL records the broker's advertised address beside the control
// socket. Run calls it only after the control server owns the socket, so a
// competing supervisor that loses the socket never overwrites the live record.
func (s *Supervisor) publishBrokerURL() error {
	url := s.advertisedBrokerURL()
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
