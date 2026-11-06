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
// start over the state matrix that matters: a run directory whose records all
// expired goes, a directory holding a pending ask keeps the ask and itself
// while its expired sibling goes, a directory a feed is writing to right now is
// left alone, and anything that is not a claim record survives.
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
	// Ageing a directory means writing the files first, because writing updates
	// the directory's own mtime.
	ageDir := func(dir string, mod time.Time) {
		t.Helper()
		if err := os.Chtimes(filepath.Join(root, dir), mod, mod); err != nil {
			t.Fatal(err)
		}
	}
	// broker-a: everything expired, so the directory goes.
	write("broker-a", "answered.claimed", old)
	write("broker-a", "unanswered.json", old)
	ageDir("broker-a", old)
	// broker-b: a live ask and an unrelated file, both kept.
	write("broker-b", "live.json", time.Now())
	write("broker-b", "notes.txt", old)
	ageDir("broker-b", old)
	// broker-c: only an unrelated file, so the directory survives the sweep.
	write("broker-c", "keep.txt", old)
	ageDir("broker-c", old)
	// broker-d: an expired record beside a pending one.
	write("broker-d", "expired.claimed", old)
	write("broker-d", "pending.json", time.Now())
	ageDir("broker-d", old)
	// broker-e: an old record in a directory a feed touched just now. The
	// record goes, because each record answers for its own age, but the
	// directory stays: Record creates it before it opens the file, so removing a
	// directory touched since the cutoff would fail a record for a live ask.
	write("broker-e", "stale.json", old)
	ageDir("broker-e", time.Now())
	// broker-f: dotted message ids, the shape this package really writes.
	write("broker-f", "av1.ask.json", old)
	write("broker-f", "av1.ask.claimed", time.Now())
	ageDir("broker-f", old)

	if err := PruneRoot(root, AskClaimMaxAge); err != nil {
		t.Fatalf("PruneRoot: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "broker-a")); !os.IsNotExist(err) {
		t.Error("broker-a survived with only expired records")
	}
	for _, kept := range []string{
		"broker-b/live.json",
		"broker-b/notes.txt",
		"broker-c/keep.txt",
		"broker-d/pending.json",
		"broker-f/av1.ask.claimed",
	} {
		if _, err := os.Stat(filepath.Join(root, kept)); err != nil {
			t.Errorf("%s was removed: %v", kept, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "broker-d/expired.claimed")); !os.IsNotExist(err) {
		t.Error("the expired record beside a pending one survived")
	}
	// The young directory keeps its place but loses its expired record; the
	// dotted pair loses only the expired half.
	if _, err := os.Stat(filepath.Join(root, "broker-e")); err != nil {
		t.Errorf("a directory a feed is writing to was removed: %v", err)
	}
	for _, gone := range []string{"broker-e/stale.json", "broker-f/av1.ask.json"} {
		if _, err := os.Stat(filepath.Join(root, gone)); !os.IsNotExist(err) {
			t.Errorf("%s survived", gone)
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

// TestPruneRootReportsAFileRoot proves an error surfaces rather than being
// swallowed: a caller that treats the prune as housekeeping still learns it did
// not happen, and a root that is a regular file is the shape that produces one.
func TestPruneRootReportsAFileRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "feed-asks")
	if err := os.WriteFile(root, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := PruneRoot(root, AskClaimMaxAge); err == nil {
		t.Fatal("PruneRoot on a file root returned no error")
	}
}
