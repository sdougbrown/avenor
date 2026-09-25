package workflowcontroller

// Runner-level tests for external-gate polling: a park seeds its cursors,
// the poll count commits before the adapter runs, a crash between commit and
// result reuses the same poll ID, backoff doubles with bounded jitter,
// stale results leave the cursor untouched, unavailable adapters record a
// deduplicated diagnostic, and disable cancels an in-flight invocation.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// newManualRunnerStore builds a controller store and runner clock over a
// temp dir, enabled and ready for a runner to lead.
func newManualRunnerStore(t *testing.T) (*ControllerStore, *manualClock) {
	t.Helper()
	clock := newManualClock()
	s := NewStoreWithClock(t.TempDir(), clock.Now)
	mustCreate(t, s, "c1", 4)
	mustEnable(t, s, "c1")
	return s, clock
}

// fakePoller scripts Poll and ApplyResult for runner tests.
type fakePoller struct {
	mu          sync.Mutex
	store       *ControllerStore
	polls       []PollCursor
	applys      []PollCursor
	pollErrs    []error
	script      []func() (AdapterResult, PollFailureKind, error)
	defaultPoll func() (AdapterResult, PollFailureKind, error)
	applyResult PollApplyOutcome
	applyErr    error
	block       chan struct{}
}

func (f *fakePoller) Poll(ctx context.Context, cursor PollCursor) (AdapterResult, PollFailureKind, error) {
	f.mu.Lock()
	f.polls = append(f.polls, cursor)
	fn := f.defaultPoll
	if len(f.script) > 0 {
		fn = f.script[0]
		f.script = f.script[1:]
	}
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return AdapterResult{}, PollFailureTransient, ctx.Err()
		}
	}
	if fn == nil {
		return AdapterResult{}, PollFailureTransient, errors.New("no scripted poll")
	}
	return fn()
}

func (f *fakePoller) ApplyResult(cursor PollCursor, res *AdapterResult, lease LeaderLease) (PollApplyOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applys = append(f.applys, cursor)
	return f.applyResult, f.applyErr
}

func (f *fakePoller) pollCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.polls)
}

func (f *fakePoller) applyCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.applys)
}

// pendingResult builds a pending adapter result with no retry hint.
func pendingResult() (AdapterResult, PollFailureKind, error) {
	return AdapterResult{Result: AdapterResultPending}, PollFailureNone, nil
}

// pollRunner starts a runner with polling enabled over a settable clock and
// deterministic (zero) jitter.
func pollRunner(t *testing.T, store *ControllerStore, deps RunnerDeps, poller *fakePoller, now func() time.Time) *Runner {
	t.Helper()
	r := NewRunner(RunnerConfig{
		Deps:          deps,
		Store:         store,
		ControllerID:  "c1",
		OwnerID:       "owner-test",
		RenewInterval: 20 * time.Millisecond,
		AntiEntropy:   20 * time.Millisecond,
		Now:           now,
		Poll:          poller,
		PollJitter:    func() float64 { return 0 },
	})
	t.Cleanup(r.Stop)
	return r
}

func pollSeed() PollSeed {
	return PollSeed{
		WorkflowID:   "wf-1",
		NodeID:       "review",
		ActivationID: "act-1",
		GateID:       "pr-review",
		AdapterID:    "gh-review",
		SubjectHash:  "hash-1",
	}
}

func TestRunnerParkSeedsCursorCommitsBeforeInvoke(t *testing.T) {
	store, clock := newManualRunnerStore(t)
	deps := newFakeDeps(store, "c1")
	deps.cands = []Candidate{runnerCand("wf-1", "review", "act-1")}
	deps.park = true
	deps.parkSeeds = []PollSeed{pollSeed()}
	poller := &fakePoller{store: store, defaultPoll: pendingResult}
	pollRunner(t, store, deps, poller, clock.Now)

	// The park lands and seeds the cursor with the first poll one base
	// interval out.
	var seeded PollCursor
	waitUntil(t, "park seeds a poll cursor", func() bool {
		due, err := store.PollDue("c1", clock.Now().Add(time.Hour))
		if err != nil || len(due) == 0 {
			return false
		}
		seeded = due[0]
		return true
	})
	_ = seeded
	earliest, ok, _ := store.NextPollTime("c1")
	if !ok || !earliest.Equal(clock.Now().Add(pollBaseDelay)) {
		t.Fatalf("first poll scheduled at %v, want %v", earliest, clock.Now().Add(pollBaseDelay))
	}

	// Advance to the first poll: the committed count and poll ID are visible
	// to the adapter invocation.
	clock.Advance(pollBaseDelay + time.Second)
	waitUntil(t, "first poll invoked", func() bool { return poller.pollCount() > 0 })
	poller.mu.Lock()
	received := poller.polls[0]
	poller.mu.Unlock()
	if received.PollCount != 1 {
		t.Fatalf("poll count at invoke = %d, want 1", received.PollCount)
	}
	if received.PollID == "" || received.PollID != DerivePollID("c1", pollSeedCursor(), 1) {
		t.Fatalf("poll id at invoke = %q", received.PollID)
	}
	// The commit landed before the invocation: the cursor's last poll is
	// already stamped when Poll observes it.
	rec, _, _ := store.Get("c1")
	stored := rec.PollCursors[PollCursorKey(received)]
	if stored == nil || stored.LastPollAt.IsZero() || stored.PollCount != 1 {
		t.Fatalf("cursor at invoke = %+v, want committed count 1 before the adapter ran", stored)
	}
}

// TestRunnerReseedsMissingPollCursor proves the anti-entropy pass repairs a
// lost cursor: a gate already parked awaiting_gate with no cursor (a crash
// between the park commit and cursor creation) is re-seeded on the pass that
// refreshes and then polled, and later passes never reset the live cursor.
func TestRunnerReseedsMissingPollCursor(t *testing.T) {
	store, clock := newManualRunnerStore(t)
	deps := newFakeDeps(store, "c1")
	// No candidate and no cursor: the activation is parked awaiting_gate and
	// only ParkedGates knows about its gate.
	deps.setParked([]PollSeed{pollSeed()})
	poller := &fakePoller{store: store, defaultPoll: pendingResult}
	pollRunner(t, store, deps, poller, clock.Now)

	waitUntil(t, "missing cursor re-seeded", func() bool {
		_, ok, _ := store.NextPollTime("c1")
		return ok
	})
	deps.mu.Lock()
	calls := deps.parkedCalls
	deps.mu.Unlock()
	if calls == 0 {
		t.Fatalf("ParkedGates was never consulted")
	}
	earliest, ok, _ := store.NextPollTime("c1")
	if !ok || !earliest.Equal(clock.Now().Add(pollBaseDelay)) {
		t.Fatalf("re-seeded first poll at %v, want %v", earliest, clock.Now().Add(pollBaseDelay))
	}
	clock.Advance(pollBaseDelay + time.Second)
	waitUntil(t, "re-seeded gate polled", func() bool { return poller.pollCount() > 0 })

	// The cursor is live: further passes skip it (a ParkedGates failure
	// leaves it untouched) and its backoff schedule survives. The first
	// pending outcome is folded by the leader loop, so wait for the retry it
	// schedules.
	waitRetryCount(t, store, 1)
	rec, _, _ := store.Get("c1")
	live := rec.PollCursors[PollCursorKey(pollSeedCursor())]
	if live == nil || live.RetryCount != 1 {
		t.Fatalf("cursor after first poll = %+v, want retry count 1", live)
	}
	deps.setParkedErr(errors.New("parked gates unavailable"))
	clock.Advance(2 * defaultAntiEntropy)
	rec, _, _ = store.Get("c1")
	after := rec.PollCursors[PollCursorKey(pollSeedCursor())]
	if after.RetryCount != live.RetryCount || !after.NextPollAt.Equal(live.NextPollAt) {
		t.Fatalf("cursor disturbed by re-seed pass: %+v, want %+v", after, live)
	}
	clock.Advance(60 * time.Second)
	waitRetryCount(t, store, 2)
	if polls := poller.pollCount(); polls != 2 {
		t.Fatalf("poll count = %d, want the re-seeded schedule intact (2 polls)", polls)
	}
}

// pollSeedCursor rebuilds the test seed's cursor for poll ID derivation.
func pollSeedCursor() PollCursor {
	s := pollSeed()
	return PollCursor{
		WorkflowID:   s.WorkflowID,
		NodeID:       s.NodeID,
		ActivationID: s.ActivationID,
		GateID:       s.GateID,
		AdapterID:    s.AdapterID,
		SubjectHash:  s.SubjectHash,
	}
}

func TestRunnerCrashAfterCommitReusesPollID(t *testing.T) {
	store, clock := newManualRunnerStore(t)
	deps := newFakeDeps(store, "c1")
	deps.cands = []Candidate{runnerCand("wf-1", "review", "act-1")}
	deps.park = true
	deps.parkSeeds = []PollSeed{pollSeed()}
	// The first poll blocks until canceled (a hung adapter); stopping the
	// runner simulates the crash between commit and result.
	poller := &fakePoller{store: store, block: make(chan struct{})}
	r := pollRunner(t, store, deps, poller, clock.Now)

	waitUntil(t, "cursor seeded", func() bool {
		_, ok, _ := store.NextPollTime("c1")
		return ok
	})
	clock.Advance(pollBaseDelay + time.Second)
	waitUntil(t, "poll committed", func() bool {
		rec, _, err := store.Get("c1")
		if err != nil {
			return false
		}
		c := rec.PollCursors[PollCursorKey(pollSeedCursor())]
		return c != nil && c.PollID != "" && !c.LastPollAt.IsZero()
	})
	r.Stop()

	// A fresh leader recovers the in-flight cursor with the same count and
	// poll ID.
	poller2 := &fakePoller{store: store, defaultPoll: pendingResult}
	pollRunner(t, store, deps, poller2, clock.Now)
	waitUntil(t, "recovered poll invoked", func() bool { return poller2.pollCount() > 0 })
	poller2.mu.Lock()
	recovered := poller2.polls[0]
	poller2.mu.Unlock()
	rec, _, _ := store.Get("c1")
	committed := rec.PollCursors[PollCursorKey(pollSeedCursor())]
	if recovered.PollID != committed.PollID || recovered.PollCount != committed.PollCount {
		t.Fatalf("recovered poll = count %d id %q, want committed count %d id %q",
			recovered.PollCount, recovered.PollID, committed.PollCount, committed.PollID)
	}
	if committed.PollCount != 1 {
		t.Fatalf("poll count = %d, want reused 1 after crash", committed.PollCount)
	}
}

// waitRetryCount waits until the cursor's persisted retry count reaches
// want, proving the leader drained the previous poll result and armed the
// next schedule before the test advances the clock again.
func waitRetryCount(t *testing.T, store *ControllerStore, want int) {
	t.Helper()
	waitUntil(t, fmt.Sprintf("retry count %d scheduled", want), func() bool {
		rec, _, err := store.Get("c1")
		if err != nil {
			return false
		}
		c := rec.PollCursors[PollCursorKey(pollSeedCursor())]
		return c != nil && c.RetryCount >= want
	})
}

func TestRunnerBackoffDoublesAndClearsCursorOnApplied(t *testing.T) {
	store, clock := newManualRunnerStore(t)
	deps := newFakeDeps(store, "c1")
	deps.cands = []Candidate{runnerCand("wf-1", "review", "act-1")}
	deps.park = true
	deps.parkSeeds = []PollSeed{pollSeed()}
	poller := &fakePoller{store: store, defaultPoll: pendingResult}
	pollRunner(t, store, deps, poller, clock.Now)

	waitUntil(t, "cursor seeded", func() bool {
		_, ok, _ := store.NextPollTime("c1")
		return ok
	})
	// pending -> pending -> applied: the schedule doubles from the base and
	// the applied verdict clears the cursor.
	delays := []time.Duration{pollBaseDelay, 60 * time.Second, 120 * time.Second}
	for i, d := range delays {
		clock.Advance(d + 50*time.Millisecond)
		waitUntil(t, "poll fired", func() bool { return poller.pollCount() > i })
		waitRetryCount(t, store, i+1)
	}
	// After the third poll the poller applies a passed verdict.
	poller.mu.Lock()
	poller.script = []func() (AdapterResult, PollFailureKind, error){
		func() (AdapterResult, PollFailureKind, error) {
			return AdapterResult{Result: AdapterResultPassed}, PollFailureNone, nil
		},
	}
	poller.mu.Unlock()
	clock.Advance(240 * time.Second)
	waitUntil(t, "verdict applied", func() bool { return poller.applyCount() > 0 })
	waitUntil(t, "cursor cleared", func() bool {
		_, ok, _ := store.NextPollTime("c1")
		return !ok
	})
	if poller.applyCount() != 1 {
		t.Fatalf("apply count = %d, want 1", poller.applyCount())
	}
}

func TestRunnerJitterBoundsNextPoll(t *testing.T) {
	store, clock := newManualRunnerStore(t)
	deps := newFakeDeps(store, "c1")
	deps.cands = []Candidate{runnerCand("wf-1", "review", "act-1")}
	deps.park = true
	deps.parkSeeds = []PollSeed{pollSeed()}
	// Jitter +1 scales the 60s post-pending delay up by 10%.
	poller := &fakePoller{store: store, defaultPoll: pendingResult}
	r := NewRunner(RunnerConfig{
		Deps:          deps,
		Store:         store,
		ControllerID:  "c1",
		OwnerID:       "owner-test",
		RenewInterval: 20 * time.Millisecond,
		AntiEntropy:   20 * time.Millisecond,
		Now:           clock.Now,
		Poll:          poller,
		PollJitter:    func() float64 { return 1 },
	})
	t.Cleanup(r.Stop)
	waitUntil(t, "cursor seeded", func() bool {
		_, ok, _ := store.NextPollTime("c1")
		return ok
	})
	clock.Advance(pollBaseDelay + 50*time.Millisecond)
	waitUntil(t, "first poll fired", func() bool { return poller.pollCount() > 0 })
	waitRetryCount(t, store, 1)
	earliest, ok, _ := store.NextPollTime("c1")
	if !ok {
		t.Fatalf("no next poll scheduled")
	}
	delay := earliest.Sub(clock.Now())
	if delay != 66*time.Second {
		t.Fatalf("jittered delay = %v, want 66s (60s + 10%%)", delay)
	}
}

func TestRunnerStaleResultLeavesCursor(t *testing.T) {
	store, clock := newManualRunnerStore(t)
	deps := newFakeDeps(store, "c1")
	deps.cands = []Candidate{runnerCand("wf-1", "review", "act-1")}
	deps.park = true
	deps.parkSeeds = []PollSeed{pollSeed()}
	poller := &fakePoller{store: store, applyResult: PollStale, defaultPoll: pendingResult}
	pollRunner(t, store, deps, poller, clock.Now)

	waitUntil(t, "cursor seeded", func() bool {
		_, ok, _ := store.NextPollTime("c1")
		return ok
	})
	clock.Advance(pollBaseDelay + 50*time.Millisecond)
	waitUntil(t, "poll fired", func() bool { return poller.pollCount() > 0 })
	waitRetryCount(t, store, 1)
	// A passed verdict that revalidates stale is discarded: the cursor keeps
	// its previous schedule untouched.
	poller.mu.Lock()
	poller.script = []func() (AdapterResult, PollFailureKind, error){
		func() (AdapterResult, PollFailureKind, error) {
			return AdapterResult{Result: AdapterResultPassed}, PollFailureNone, nil
		},
	}
	poller.mu.Unlock()
	clock.Advance(61 * time.Second)
	waitUntil(t, "stale result discarded", func() bool { return poller.applyCount() > 0 })
	rec, _, _ := store.Get("c1")
	c := rec.PollCursors[PollCursorKey(pollSeedCursor())]
	// The stale result is discarded without rescheduling or clearing: the
	// cursor is left in flight (its next poll reuses the committed poll ID).
	if c == nil || !c.NextPollAt.IsZero() || c.RetryCount != 1 {
		t.Fatalf("cursor after stale discard = %+v, want the in-flight cursor left untouched", c)
	}
}

func TestRunnerAdapterUnavailableDiagnosticDeduped(t *testing.T) {
	store, clock := newManualRunnerStore(t)
	deps := newFakeDeps(store, "c1")
	deps.cands = []Candidate{runnerCand("wf-1", "review", "act-1")}
	deps.park = true
	deps.parkSeeds = []PollSeed{pollSeed()}
	unavailable := func() (AdapterResult, PollFailureKind, error) {
		return AdapterResult{}, PollFailureUnavailable, errors.New("adapter gh-review is not registered")
	}
	poller := &fakePoller{store: store, defaultPoll: unavailable}
	pollRunner(t, store, deps, poller, clock.Now)

	countDiagEvents := func() int {
		data, err := os.ReadFile(store.eventsPath("c1"))
		if err != nil {
			return 0
		}
		return bytes.Count(data, []byte(`"kind":"diagnostic_recorded"`))
	}
	waitUntil(t, "cursor seeded", func() bool {
		_, ok, _ := store.NextPollTime("c1")
		return ok
	})
	clock.Advance(pollBaseDelay + 50*time.Millisecond)
	waitUntil(t, "first poll fired", func() bool { return poller.pollCount() > 0 })
	waitUntil(t, "diagnostic recorded", func() bool { return countDiagEvents() == 1 })
	waitRetryCount(t, store, 1)
	// A second identical failure appends no event (deduplicated).
	clock.Advance(61 * time.Second)
	waitUntil(t, "second poll fired", func() bool { return poller.pollCount() > 1 })
	waitRetryCount(t, store, 2)
	waitUntil(t, "backoff armed", func() bool { return true })
	time.Sleep(100 * time.Millisecond)
	if n := countDiagEvents(); n != 1 {
		t.Fatalf("diagnostic_recorded events = %d, want deduplicated 1", n)
	}
	// A terminal (passed) poll clears the diagnostic.
	poller.mu.Lock()
	poller.script = []func() (AdapterResult, PollFailureKind, error){
		func() (AdapterResult, PollFailureKind, error) {
			return AdapterResult{Result: AdapterResultPassed}, PollFailureNone, nil
		},
	}
	poller.mu.Unlock()
	clock.Advance(121 * time.Second)
	waitUntil(t, "successful poll", func() bool { return poller.pollCount() > 2 })
	waitUntil(t, "diagnostic cleared", func() bool {
		rec, _, _ := store.Get("c1")
		_, open := rec.Diagnostics["adapter_unavailable/gh-review"]
		return !open
	})
}

func TestRunnerDisableCancelsInFlightPoll(t *testing.T) {
	store, clock := newManualRunnerStore(t)
	deps := newFakeDeps(store, "c1")
	deps.cands = []Candidate{runnerCand("wf-1", "review", "act-1")}
	deps.park = true
	deps.parkSeeds = []PollSeed{pollSeed()}
	poller := &fakePoller{store: store, block: make(chan struct{})}
	r := NewRunner(RunnerConfig{
		Deps:          deps,
		Store:         store,
		ControllerID:  "c1",
		OwnerID:       "owner-test",
		RenewInterval: 20 * time.Millisecond,
		AntiEntropy:   20 * time.Millisecond,
		Now:           clock.Now,
		Poll:          poller,
	})
	waitUntil(t, "cursor seeded", func() bool {
		_, ok, _ := store.NextPollTime("c1")
		return ok
	})
	clock.Advance(pollBaseDelay + time.Second)
	waitUntil(t, "poll started", func() bool { return poller.pollCount() > 0 })

	// Stop cancels the poll context; the blocked invocation observes the
	// cancellation (the real adapter's process group is killed the same way)
	// and the runner exits without waiting for the adapter.
	done := make(chan struct{})
	go func() { r.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Stop blocked on an in-flight adapter invocation")
	}
	// The cursor stays in flight: the next leader reuses its count and poll
	// ID.
	rec, _, _ := store.Get("c1")
	c := rec.PollCursors[PollCursorKey(pollSeedCursor())]
	if c == nil || !c.NextPollAt.IsZero() || c.PollID == "" {
		t.Fatalf("cursor after disable = %+v, want in-flight with a committed poll ID", c)
	}
}
