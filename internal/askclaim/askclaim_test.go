package askclaim

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestRecord checks the persisted ask and its permissions.
func TestRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "asks")
	if err := Record(dir, "av1.ask", "rt_sender"); err != nil {
		t.Fatal(err)
	}
	if err := Record(dir, "av1.ask", "rt_sender"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "av1.ask.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		MessageID  string `json:"message_id"`
		AskerRunID string `json:"asker_run_id"`
	}
	if err = json.Unmarshal(raw, &record); err != nil || record.MessageID != "av1.ask" || record.AskerRunID != "rt_sender" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	info, err := os.Stat(filepath.Join(dir, "av1.ask.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("file mode=%v err=%v", info, err)
	}
	info, err = os.Stat(dir)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("dir mode=%v err=%v", info, err)
	}
}

// TestRecordRejectsDifferentAsker preserves existing pending and claimed records.
func TestRecordRejectsDifferentAsker(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(fmt.Sprint(claimed), func(t *testing.T) {
			dir := t.TempDir()
			if err := Record(dir, "id", "first"); err != nil {
				t.Fatal(err)
			}
			name := "id.json"
			if claimed {
				if err := os.Rename(filepath.Join(dir, name), filepath.Join(dir, "id.claimed")); err != nil {
					t.Fatal(err)
				}
				name = "id.claimed"
			}
			before, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if err := Record(dir, "id", "second"); !errors.Is(err, ErrAskerMismatch) {
				t.Fatalf("error %v", err)
			}
			after, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil || string(before) != string(after) {
				t.Fatalf("record changed: %q error %v", after, err)
			}
			if err := Record(dir, "id", "first"); err != nil {
				t.Fatalf("same asker: %v", err)
			}
		})
	}
}

// TestInvalidMessageIDDoesNotTouchFilesystem rejects paths and unsafe names.
func TestInvalidMessageIDDoesNotTouchFilesystem(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "asks")
	for _, id := range []string{"../x", ".", "..", "", "a/b"} {
		if err := Record(dir, id, "s"); err == nil {
			t.Errorf("Record(%q) succeeded", id)
		}
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("directory touched: %v", err)
	}
}

// TestDefaultDirNormalizesBrokerIdentity checks equivalent and separate brokers.
func TestDefaultDirNormalizesBrokerIdentity(t *testing.T) {
	root := t.TempDir()
	a, err := DefaultDir(root, "http://EXAMPLE.com:80/", "rt_1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := DefaultDir(root, "http://example.com", "rt_1")
	if err != nil {
		t.Fatal(err)
	}
	c, err := DefaultDir(root, "http://example.com:81", "rt_1")
	if err != nil {
		t.Fatal(err)
	}
	d, err := DefaultDir(root, "http://example.com", "rt_2")
	if err != nil {
		t.Fatal(err)
	}
	if a != b || a == c || a == d {
		t.Fatalf("identity collision: %q %q", a, b)
	}
	ipv6Default, err := DefaultDir(root, "http://[::1]:80/", "rt_1")
	if err != nil {
		t.Fatal(err)
	}
	ipv6Bare, err := DefaultDir(root, "http://[::1]", "rt_1")
	if err != nil {
		t.Fatal(err)
	}
	ipv6Other, err := DefaultDir(root, "http://[::1]:81", "rt_1")
	if err != nil {
		t.Fatal(err)
	}
	if ipv6Default != ipv6Bare || ipv6Other == ipv6Bare {
		t.Fatalf("IPv6 identity: default=%q bare=%q other=%q", ipv6Default, ipv6Bare, ipv6Other)
	}
}

// TestDefaultDirRejectsInvalidBroker rejects the same URLs as BrokerKey.
func TestDefaultDirRejectsInvalidBroker(t *testing.T) {
	for _, raw := range []string{"not-a-url", "http://user:secret@example.com", "http://example.com/?query=x", "http://example.com:8080/api", ""} {
		if dir, err := DefaultDir(t.TempDir(), raw, "r"); err == nil || dir != "" {
			t.Fatalf("url %q returned %q, %v", raw, dir, err)
		}
	}
}

// TestAClaimedAskIsNotRecordedAgain checks one reply per ask: a second
// delivery of an ask that was already claimed leaves it unclaimable.
func TestAClaimedAskIsNotRecordedAgain(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "asks")
	if err := Record(dir, "av1.ask", "rt_sender"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "av1.ask.json"), filepath.Join(dir, "av1.ask.claimed")); err != nil {
		t.Fatal(err)
	}
	if err := Record(dir, "av1.ask", "rt_sender"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "av1.ask.json")); !os.IsNotExist(err) {
		t.Fatalf("claimed ask was recorded again: %v", err)
	}
}
