package askclaim

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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

// TestPruneRootRemovesExpiredRecords proves the housekeeping a feed does on
// start: expired records and the run directory they left behind go, a recent
// record and anything that is not a claim record stay.
func TestPruneRootRemovesExpiredRecords(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-2 * AskClaimMaxAge)
	write := func(dir, name string, mod time.Time) {
		t.Helper()
		path := filepath.Join(root, dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(`{"message_id":"m","asker_run_id":"r"}`), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mod, mod); err != nil {
			t.Fatal(err)
		}
	}
	// One run directory whose records all expired, one holding a live ask.
	write("broker-a", "answered.claimed", old)
	write("broker-a", "unanswered.json", old)
	write("broker-b", "live.json", time.Now())
	// Not claim records: kept whatever their age.
	write("broker-b", "notes.txt", old)
	// A directory holding only an unrelated file survives the empty sweep.
	write("broker-c", "keep.txt", old)

	if err := PruneRoot(root, AskClaimMaxAge); err != nil {
		t.Fatalf("PruneRoot: %v", err)
	}
	for _, gone := range []string{"broker-a"} {
		if _, err := os.Stat(filepath.Join(root, gone)); !os.IsNotExist(err) {
			t.Errorf("%s survived with only expired records", gone)
		}
	}
	for _, kept := range []string{"broker-b/live.json", "broker-b/notes.txt", "broker-c/keep.txt"} {
		if _, err := os.Stat(filepath.Join(root, kept)); err != nil {
			t.Errorf("%s was removed: %v", kept, err)
		}
	}
}

// TestPruneRootMissingRoot proves a first run, whose root does not exist yet,
// is not an error.
func TestPruneRootMissingRoot(t *testing.T) {
	if err := PruneRoot(filepath.Join(t.TempDir(), "absent"), AskClaimMaxAge); err != nil {
		t.Fatalf("PruneRoot on a missing root: %v", err)
	}
}
