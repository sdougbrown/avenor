package warmqueue

import "testing"

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
