package askclaim

import (
	"sync"
	"testing"
)

// TestConcurrentClaimAndRestore checks that one caller claims the stamped asker.
func TestConcurrentClaimAndRestore(t *testing.T) {
	dir := t.TempDir()
	if err := Record(dir, "ask", "sender"); err != nil {
		t.Fatal(err)
	}
	p := &Provider{Dir: dir}
	var wg sync.WaitGroup
	results := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if asker, ok := p.ClaimAsk("ask"); ok {
				results <- asker
			}
		}()
	}
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
	p.RestoreAsk("ask", "attacker")
	if asker, ok := p.ClaimAsk("ask"); !ok || asker != "sender" {
		t.Fatalf("restored: %q %v", asker, ok)
	}
}
