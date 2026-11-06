// Package askclaim persists the handoff between an inbox feed and reply surface.
package askclaim

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/internal/childmessaging"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// messageIDPattern restricts record names to a single safe path component.
var messageIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ErrAskerMismatch reports an existing ask with a different stamped asker.
var ErrAskerMismatch = errors.New("ask claim: asker mismatch")

// validID reports whether id can name a claim file.
func validID(id string) bool { return id != "." && id != ".." && messageIDPattern.MatchString(id) }

// Record persists an inbound ask without replacing an existing record, and
// records nothing for an ask that was already claimed.
func Record(dir, messageID, askerRunID string) error {
	if !validID(messageID) || dir == "" {
		return fmt.Errorf("invalid ask record path")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	path := filepath.Join(dir, messageID+".json")
	claimed := filepath.Join(dir, messageID+".claimed")
	check := func() (bool, error) {
		found := false
		for _, name := range []string{claimed, path} {
			raw, err := os.ReadFile(name)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return false, err
			}
			found = true
			var record struct {
				AskerRunID string `json:"asker_run_id"`
			}
			if err := json.Unmarshal(raw, &record); err != nil {
				return false, err
			}
			if record.AskerRunID != askerRunID {
				return false, ErrAskerMismatch
			}
		}
		return found, nil
	}
	found, err := check()
	if err != nil || found {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if os.IsExist(err) {
		_, err = check()
		return err
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
	source := filepath.Join(p.Dir, id+".json")
	if err := os.Link(source, claimed); err != nil {
		return "", false
	}
	_ = os.Remove(source)
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
	claimed := filepath.Join(p.Dir, id+".claimed")
	_ = os.Rename(claimed, filepath.Join(p.Dir, id+".json"))
}

// BrokerKey hashes a normalized broker URL and run ID for local identity files.
func BrokerKey(brokerURL, runID string) (string, error) {
	// The transport takes only a root URL, so the key accepts exactly
	// what messaging.ParseBrokerURL does: a URL the feed cannot poll gets
	// no lock and no asks directory.
	root, err := messaging.ParseBrokerURL(brokerURL)
	if err != nil || root == "" {
		return "", fmt.Errorf("invalid broker URL")
	}
	u, err := url.Parse(root)
	if err != nil {
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

// DefaultDir returns the claim directory for a valid broker and receiving run.
func DefaultDir(stateHome, brokerURL, runID string) (string, error) {
	key, err := BrokerKey(brokerURL, runID)
	if err != nil {
		return "", err
	}
	return filepath.Join(stateHome, "feed-asks", key), nil
}

// AskClaimMaxAge bounds how long a claim record is kept. A caller can hold an
// ask open no longer than the messaging wait cap, so a day leaves a wide margin
// while keeping the directory from growing without bound.
const AskClaimMaxAge = 24 * time.Hour

// PruneRoot removes claim records older than maxAge from every run directory
// under root, then removes a run directory left empty. Each supervisor-minted
// run gets its own directory, so without this the state home accumulates one
// directory per delegation and nothing ever removes them.
//
// Only files named like a claim record are considered, so an unrelated file in
// a run directory is left alone. Callers treat a failure as housekeeping that
// did not happen rather than a reason to stop.
func PruneRoot(root string, maxAge time.Duration) error {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-maxAge)
	var firstErr error
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		if err := pruneRunDir(dir, cutoff); err != nil && firstErr == nil {
			firstErr = err
		}
		removeIfEmptyDir(dir)
	}
	return firstErr
}

// pruneRunDir removes the expired claim records in one run directory.
func pruneRunDir(dir string, cutoff time.Time) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !isClaimRecordName(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
	return nil
}

// isClaimRecordName reports whether name is a claim record's file name: a
// message id this package accepts, with the answered or unanswered suffix.
func isClaimRecordName(name string) bool {
	for _, suffix := range []string{".json", ".claimed"} {
		if id, ok := strings.CutSuffix(name, suffix); ok {
			return validID(id)
		}
	}
	return false
}

// removeIfEmptyDir drops a run directory that no longer holds anything.
func removeIfEmptyDir(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) > 0 {
		return
	}
	_ = os.Remove(dir)
}
