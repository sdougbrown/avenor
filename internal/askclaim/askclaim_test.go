package askclaim

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestRecordClaimAndRestore checks the single-use claim handoff.
func TestRecordClaimAndRestore(t *testing.T) {
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
	p := &Provider{Dir: dir}
	if sender, ok := p.ClaimAsk("av1.ask"); !ok || sender != "rt_sender" {
		t.Fatalf("claim %q %v", sender, ok)
	}
	if sender, ok := p.ClaimAsk("av1.ask"); ok || sender != "" {
		t.Fatalf("second claim %q %v", sender, ok)
	}
	p.RestoreAsk("av1.ask", "attacker")
	if sender, ok := p.ClaimAsk("av1.ask"); !ok || sender != "rt_sender" {
		t.Fatalf("restored claim %q %v", sender, ok)
	}
}

// TestClaimWithExistingClaimedFileDoesNotWin preserves the original claim.
func TestClaimWithExistingClaimedFileDoesNotWin(t *testing.T) {
	dir := t.TempDir()
	if err := Record(dir, "id", "first"); err != nil {
		t.Fatal(err)
	}
	p := &Provider{Dir: dir}
	if _, ok := p.ClaimAsk("id"); !ok {
		t.Fatal("first claim failed")
	}
	if err := os.WriteFile(filepath.Join(dir, "id.json"), []byte(`{"asker_run_id":"second"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if asker, ok := p.ClaimAsk("id"); ok || asker != "" {
		t.Fatalf("second claim %q %v", asker, ok)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "id.claimed"))
	if err != nil || !strings.Contains(string(raw), "first") {
		t.Fatalf("claimed %q error %v", raw, err)
	}
}

// TestConcurrentClaimsHaveOneWinner verifies the atomic handoff.
func TestConcurrentClaimsHaveOneWinner(t *testing.T) {
	dir := t.TempDir()
	if err := Record(dir, "id", "sender"); err != nil {
		t.Fatal(err)
	}
	p := &Provider{Dir: dir}
	var wg sync.WaitGroup
	results := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, ok := p.ClaimAsk("id"); results <- ok }()
	}
	wg.Wait()
	close(results)
	wins := 0
	for ok := range results {
		if ok {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("winners=%d", wins)
	}
}

// TestInvalidMessageIDDoesNotTouchFilesystem rejects paths and unsafe names.
func TestInvalidMessageIDDoesNotTouchFilesystem(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "asks")
	p := &Provider{Dir: dir}
	for _, id := range []string{"../x", ".", "..", "", "a/b"} {
		if err := Record(dir, id, "s"); err == nil {
			t.Errorf("Record(%q) succeeded", id)
		}
		if _, ok := p.ClaimAsk(id); ok {
			t.Errorf("ClaimAsk(%q) succeeded", id)
		}
		p.RestoreAsk(id, "s")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("directory touched: %v", err)
	}
}

// TestDefaultDirNormalizesBrokerIdentity checks equivalent and separate brokers.
func TestDefaultDirNormalizesBrokerIdentity(t *testing.T) {
	root := t.TempDir()
	a := DefaultDir(root, "http://EXAMPLE.com:80/", "rt_1")
	b := DefaultDir(root, "http://example.com", "rt_1")
	if a != b || a == DefaultDir(root, "http://example.com:81", "rt_1") || a == DefaultDir(root, "http://example.com", "rt_2") {
		t.Fatalf("identity collision: %q %q", a, b)
	}
}

// TestAClaimedAskIsNotRecordedAgain checks one reply per ask: a second
// delivery of an ask that was already claimed leaves it unclaimable.
func TestAClaimedAskIsNotRecordedAgain(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "asks")
	if err := Record(dir, "av1.ask", "rt_sender"); err != nil {
		t.Fatal(err)
	}
	p := &Provider{Dir: dir}
	if _, ok := p.ClaimAsk("av1.ask"); !ok {
		t.Fatal("the first claim failed")
	}
	if err := Record(dir, "av1.ask", "rt_sender"); err != nil {
		t.Fatal(err)
	}
	if asker, ok := p.ClaimAsk("av1.ask"); ok {
		t.Fatalf("an ask claimed once was claimable again, for %q", asker)
	}
}
