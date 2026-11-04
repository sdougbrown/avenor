// Package askclaim persists the handoff between an inbox feed and reply surface.
package askclaim

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/sdougbrown/avenor/internal/childmessaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// messageIDPattern restricts record names to a single safe path component.
var messageIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// validID reports whether id can name a claim file.
func validID(id string) bool { return id != "." && id != ".." && messageIDPattern.MatchString(id) }

// Record persists an inbound ask without replacing an existing record.
func Record(dir, messageID, askerRunID string) error {
	if !validID(messageID) || dir == "" {
		return fmt.Errorf("invalid ask record path")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	path := filepath.Join(dir, messageID+".json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if os.IsExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(struct {
		MessageID  string `json:"message_id"`
		AskerRunID string `json:"asker_run_id"`
	}{messageID, askerRunID})
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
	}
	return err
}

// Provider adds disk-backed ask claims to the shared messaging provider.
type Provider struct {
	*childmessaging.MessageProvider
	// Dir holds the inbound ask records.
	Dir string
}

var _ router.AskClaimer = (*Provider)(nil)

// ClaimAsk atomically claims an ask and reads its stamped asker.
func (p *Provider) ClaimAsk(id string) (string, bool) {
	if p.Dir == "" || !validID(id) {
		return "", false
	}
	claimed := filepath.Join(p.Dir, id+".claimed")
	if err := os.Rename(filepath.Join(p.Dir, id+".json"), claimed); err != nil {
		return "", false
	}
	raw, err := os.ReadFile(claimed)
	if err != nil {
		return "", false
	}
	var record struct {
		AskerRunID string `json:"asker_run_id"`
	}
	if json.Unmarshal(raw, &record) != nil || record.AskerRunID == "" {
		return "", false
	}
	return record.AskerRunID, true
}

// RestoreAsk returns a claimed ask to the queue without trusting the caller.
func (p *Provider) RestoreAsk(id, _ string) {
	if p.Dir == "" || !validID(id) {
		return
	}
	_ = os.Rename(filepath.Join(p.Dir, id+".claimed"), filepath.Join(p.Dir, id+".json"))
}

// BrokerKey hashes a normalized broker URL and run ID for local identity files.
func BrokerKey(brokerURL, runID string) (string, error) {
	u, err := url.Parse(brokerURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("invalid broker URL")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	if (u.Scheme == "http" && u.Port() == "80") || (u.Scheme == "https" && u.Port() == "443") {
		u.Host = u.Hostname()
		if strings.Contains(u.Host, ":") {
			u.Host = "[" + u.Host + "]"
		}
	} else if u.Port() != "" {
		u.Host = net.JoinHostPort(u.Hostname(), u.Port())
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	sum := sha256.Sum256([]byte(u.String() + "\x00" + runID))
	return hex.EncodeToString(sum[:]), nil
}

// DefaultDir returns the claim directory for a broker and receiving run.
func DefaultDir(stateHome, brokerURL, runID string) string {
	key, err := BrokerKey(brokerURL, runID)
	if err != nil {
		sum := sha256.Sum256([]byte(brokerURL + "\x00" + runID))
		key = hex.EncodeToString(sum[:])
	}
	return filepath.Join(stateHome, "feed-asks", key)
}
