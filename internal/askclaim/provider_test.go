package askclaim

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestClaimDoesNotReplaceExistingClaim preserves an existing claim when both files exist.
func TestClaimDoesNotReplaceExistingClaim(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"ask.json", "ask.claimed"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(`{"asker_run_id":"sender"}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := (&Provider{Dir: dir}).ClaimAsk("ask"); ok {
		t.Fatal("claimed an already claimed ask")
	}
}

// TestConcurrentClaimAndRestore checks that one caller claims the stamped asker.
func TestConcurrentClaimAndRestore(t *testing.T) {
	dir := t.TempDir()
	if err := Record(dir, "ask", "sender"); err != nil {
		t.Fatal(err)
	}
	p := &Provider{Dir: dir}
	var wg sync.WaitGroup
	results := make(chan string, 20)
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if asker, ok := p.ClaimAsk("ask"); ok {
				results <- asker
			}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var wins int
	for asker := range results {
		wins++
		if asker != "sender" {
			t.Fatalf("asker: %s", asker)
		}
	}
	if wins != 1 {
		t.Fatalf("claims: %d", wins)
	}
	if err := Record(dir, "ask", "sender"); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.ClaimAsk("ask"); ok {
		t.Fatal("claimed ask was recorded again")
	}
	if err := os.WriteFile(filepath.Join(dir, "ask.json"), []byte(`{"asker_run_id":"other"}`), 0600); err != nil {
		t.Fatal(err)
	}
	p.RestoreAsk("ask", "attacker")
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "ask.json" {
		t.Fatalf("restored files: %v, %v", entries, err)
	}
	if asker, ok := p.ClaimAsk("ask"); !ok || asker != "sender" {
		t.Fatalf("restored: %q %v", asker, ok)
	}
}
