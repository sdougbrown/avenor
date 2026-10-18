package diagnote

import (
	"fmt"
	"sync"
	"testing"
)

func TestGateAdmitsDistinctKeysOnce(t *testing.T) {
	var g Gate
	log, suppressed := g.Admit("mystery")
	if !log || suppressed {
		t.Fatalf("first Admit = (%v, %v), want (true, false)", log, suppressed)
	}
	log, suppressed = g.Admit("mystery")
	if log || suppressed {
		t.Fatalf("second Admit of the same key = (%v, %v), want (false, false)", log, suppressed)
	}
}

func TestGateSuppressesOnceAfterCap(t *testing.T) {
	var g Gate
	for i := 0; i < Cap; i++ {
		log, suppressed := g.Admit(fmt.Sprintf("key_%d", i))
		if !log || suppressed {
			t.Fatalf("Admit(key_%d) = (%v, %v), want (true, false)", i, log, suppressed)
		}
	}
	log, suppressed := g.Admit("overflow")
	if log || !suppressed {
		t.Fatalf("first post-cap Admit = (%v, %v), want (false, true)", log, suppressed)
	}
	for _, key := range []string{"overflow", "another"} {
		log, suppressed := g.Admit(key)
		if log || suppressed {
			t.Fatalf("post-suppression Admit(%q) = (%v, %v), want (false, false)", key, log, suppressed)
		}
	}
}

func TestGateNilAdmitsNothing(t *testing.T) {
	var g *Gate
	if log, suppressed := g.Admit("mystery"); log || suppressed {
		t.Fatalf("nil Admit = (%v, %v), want (false, false)", log, suppressed)
	}
}

func TestGateConcurrentAdmitsStayConsistent(t *testing.T) {
	var g Gate
	var mu sync.Mutex
	logs := make(map[string]int)
	suppressions := 0
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				key := fmt.Sprintf("key_%d", (worker*3+i)%20)
				log, suppressed := g.Admit(key)
				mu.Lock()
				if log {
					logs[key]++
				}
				if suppressed {
					suppressions++
				}
				mu.Unlock()
			}
		}(worker)
	}
	wg.Wait()
	if len(logs) != Cap {
		t.Fatalf("distinct keys logged = %d, want %d", len(logs), Cap)
	}
	for key, count := range logs {
		if count != 1 {
			t.Fatalf("key %q logged %d times, want 1", key, count)
		}
	}
	if suppressions != 1 {
		t.Fatalf("suppression admissions = %d, want 1", suppressions)
	}
}
