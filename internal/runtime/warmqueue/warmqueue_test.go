package warmqueue

import (
	"fmt"
	"sync"
	"testing"
)

func TestDiscardStopsAcceptingPrompts(t *testing.T) {
	q := NewQueue()
	q.Enqueue("session", "one")
	q.Discard()
	q.Enqueue("session", "two")
	msgs, done := q.Take("session")
	if len(msgs) != 0 || done != nil {
		t.Fatalf("Take after Discard delivered %q with done non-nil, want nothing", msgs)
	}
	var nilQ *Queue
	nilQ.Discard()
}

func TestResetClearsPendingAndKeepsAccepting(t *testing.T) {
	q := NewQueue()
	q.Enqueue("session", "stale")
	q.Reset()
	if msgs, done := q.Take("session"); len(msgs) != 0 || done != nil {
		t.Fatalf("Take after Reset delivered %q with done non-nil, want the pending prompt dropped", msgs)
	}
	q.Enqueue("session", "fresh")
	msgs, done := q.Take("session")
	if len(msgs) != 1 || msgs[0] != "fresh" || done == nil {
		t.Fatalf("Take after Reset and Enqueue = %q with done nil, want the new prompt", msgs)
	}
	done()
}

func TestAdmissionGuardKeepsInjectionInFlightAfterDiscardAndReset(t *testing.T) {
	q := NewQueue()
	q.Enqueue("session", "one")
	_, done := q.Take("session")
	if done == nil {
		t.Fatal("Take delivered nothing to inject")
	}
	if _, admitted := q.LockWarmMessageAdmission("session"); admitted {
		t.Fatal("admitted while an injection is in flight")
	}
	q.Discard()
	if _, admitted := q.LockWarmMessageAdmission("session"); admitted {
		t.Fatal("Discard cleared an in-flight injection")
	}
	q.Reset()
	if _, admitted := q.LockWarmMessageAdmission("session"); admitted {
		t.Fatal("Reset cleared an in-flight injection")
	}
	done()
	unlock, admitted := q.LockWarmMessageAdmission("session")
	if !admitted {
		t.Fatal("refused after the injection finished")
	}
	unlock()
}

// TestWarmQueueConcurrentAdmissionRefusesDuringInjection runs producers,
// injectors, and admission attempts against one queue concurrently: under
// -race it fails on any data race or mis-scoped mutex, and no admission may
// succeed for a session while one of its taken batches is still in flight. A
// refusal must also leave no callable unlock behind.
func TestWarmQueueConcurrentAdmissionRefusesDuringInjection(t *testing.T) {
	q := NewQueue()
	const sessions = 4
	const messages = 200
	var mu sync.Mutex
	var violations []string
	fail := func(format string, args ...any) {
		mu.Lock()
		violations = append(violations, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	var wg sync.WaitGroup
	// Producers keep every session's queue fed while injectors deliver.
	for s := 0; s < sessions; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for i := 0; i < messages; i++ {
				q.Enqueue(fmt.Sprintf("s%d", s), fmt.Sprintf("m%d", i))
			}
		}(s)
	}
	// Injectors take each delivered batch and hold it in flight, so every
	// admission attempt overlapping the delivery must be refused.
	for s := 0; s < sessions; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			session := fmt.Sprintf("s%d", s)
			for delivered := 0; delivered < messages; {
				msgs, done := q.Take(session)
				if len(msgs) == 0 {
					continue
				}
				delivered += len(msgs)
				if _, ok := q.LockWarmMessageAdmission(session); ok {
					fail("session %s: admitted while a delivery of %d messages is in flight", session, len(msgs))
				}
				done()
			}
		}(s)
	}
	// Admitters race the injections on every session; each admitted guard is
	// released immediately so the interleaving keeps changing.
	for s := 0; s < sessions; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			session := fmt.Sprintf("s%d", s)
			for i := 0; i < messages*2; i++ {
				unlock, ok := q.LockWarmMessageAdmission(session)
				if !ok && unlock != nil {
					fail("session %s: refusal handed back a callable unlock", session)
				}
				if ok {
					unlock()
				}
			}
		}(s)
	}
	wg.Wait()
	if len(violations) != 0 {
		t.Fatalf("admission guard violations: %v", violations)
	}
}
