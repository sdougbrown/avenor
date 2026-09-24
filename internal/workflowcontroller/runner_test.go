package workflowcontroller

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/workflow"
)

// tickClock is a concurrency-safe manual clock that advances one millisecond
// on every call. Because RenewLease reads the clock exactly once per call and
// stamps RenewedAt with it, each renewal leaves a distinct, strictly
// increasing timestamp — which lets a dispatch observe whether a renewal
// happened before it without intercepting the store.
type tickClock struct {
	nanos atomic.Int64
}

func (c *tickClock) Now() time.Time {
	n := c.nanos.Add(int64(time.Millisecond))
	return time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC).Add(time.Duration(n))
}

// dispatchRecord is one observed deps.Dispatch call.
type dispatchRecord struct {
	identity  workflow.ExecutionIdentity
	lease     LeaderLease
	renewedAt time.Time // store leader RenewedAt observed at dispatch entry
}

// fakeDeps is a scripted RunnerDeps over a real ControllerStore. Dispatch
// calls record the store's current leader RenewedAt so tests can prove
// renew-before-dispatch ordering without intercepting the store.
type fakeDeps struct {
	mu         sync.Mutex
	store      *ControllerStore
	controller string

	cands       []Candidate // candidates visible to Candidates()
	pending     []Candidate // candidates revealed only by Refresh()
	inflight    []InFlightAttempt
	refreshes   int
	refreshErr  error // returned by Refresh() when non-nil
	candsErr    error // returned by Candidates() when non-nil
	inflightErr error // returned by InFlight() when non-nil

	scripted      []DispatchResult // popped per dispatch; falls back to defaultResult
	errScript     []error          // popped per dispatch before the result script
	defaultResult DispatchResult

	park             bool // dispatches return ResultParked with parkSeeds
	parkSeeds        []PollSeed
	unresolved       bool // dispatches return ResultUnresolvedBinding
	unresolvedDetail string

	block                chan struct{} // when non-nil, dispatches block until closed
	blockAll             bool          // block every dispatch
	blockFirst           int           // or only the first N dispatches
	uninterruptibleFirst int           // the first N blocking dispatches ignore ctx (like a started host dispatch)

	dispatches []dispatchRecord
	started    chan struct{} // signaled (non-blocking) on every dispatch start
}

func newFakeDeps(store *ControllerStore, controller string) *fakeDeps {
	return &fakeDeps{store: store, controller: controller, started: make(chan struct{}, 64)}
}

func (d *fakeDeps) Candidates(controllerID string) ([]Candidate, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Candidate, len(d.cands))
	copy(out, d.cands)
	return out, d.candsErr
}

func (d *fakeDeps) InFlight() ([]InFlightAttempt, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]InFlightAttempt, len(d.inflight))
	copy(out, d.inflight)
	return out, d.inflightErr
}

// Refresh swaps the pending candidates into view, simulating a host-side
// cached view that only a rebuild can update. It returns refreshErr when set.
func (d *fakeDeps) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.refreshes++
	d.cands = append(d.cands, d.pending...)
	d.pending = nil
	return d.refreshErr
}

func (d *fakeDeps) Dispatch(ctx context.Context, dec Decision, lease LeaderLease) (DispatchResult, error) {
	rec := dispatchRecord{identity: dec.Candidate.Identity, lease: lease}
	if srec, _, err := d.store.Get(d.controller); err == nil && srec.Leader != nil {
		rec.renewedAt = srec.Leader.RenewedAt
	}
	d.mu.Lock()
	d.dispatches = append(d.dispatches, rec)
	var dispatchErr error
	if len(d.errScript) > 0 {
		dispatchErr = d.errScript[0]
		d.errScript = d.errScript[1:]
	}
	scripted := d.defaultResult
	if len(d.scripted) > 0 {
		scripted = d.scripted[0]
		d.scripted = d.scripted[1:]
	}
	block := d.block != nil && (d.blockAll || len(d.dispatches) <= d.blockFirst)
	uninterruptible := len(d.dispatches) <= d.uninterruptibleFirst
	d.mu.Unlock()
	select {
	case d.started <- struct{}{}:
	default:
	}
	if d.park {
		return DispatchResult{Kind: ResultParked, PollSeeds: d.parkSeeds}, nil
	}
	if d.unresolved {
		return DispatchResult{Kind: ResultUnresolvedBinding, Detail: d.unresolvedDetail}, nil
	}

	if block {
		if uninterruptible {
			<-d.block
		} else {
			select {
			case <-d.block:
			case <-ctx.Done():
				return DispatchResult{Kind: ResultCanceled}, nil
			}
		}
	}
	if ctx.Err() != nil {
		return DispatchResult{Kind: ResultCanceled}, nil
	}
	return scripted, dispatchErr
}

// --- test-only accessors ---

func (d *fakeDeps) dispatchCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.dispatches)
}

func (d *fakeDeps) allDispatches() []dispatchRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]dispatchRecord, len(d.dispatches))
	copy(out, d.dispatches)
	return out
}

func (d *fakeDeps) refreshCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.refreshes
}

func (d *fakeDeps) setCandidates(cands []Candidate) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cands = cands
}

func (d *fakeDeps) setPending(pending []Candidate) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending = pending
}

func (d *fakeDeps) setInFlight(inflight []InFlightAttempt) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.inflight = inflight
}

func (d *fakeDeps) setScript(results ...DispatchResult) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.scripted = results
}

func (d *fakeDeps) setErrors(errs ...error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.errScript = errs
}

func (d *fakeDeps) setDefaultResult(res DispatchResult) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.defaultResult = res
}

func (d *fakeDeps) setRefreshError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.refreshErr = err
}

func (d *fakeDeps) setCandidatesError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.candsErr = err
}

func (d *fakeDeps) setInFlightError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.inflightErr = err
}

// waitUntil polls cond until it holds or the deadline passes.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// manualClock is a mutex-guarded clock the test advances explicitly. The
// runner reads it from its leader goroutine while the test advances it, so
// reads and writes must not race.
type manualClock struct {
	mu  sync.Mutex
	cur time.Time
}

func newManualClock() *manualClock {
	return &manualClock{cur: time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.cur = c.cur.Add(d)
	c.mu.Unlock()
}

// newRunnerStore builds a real ControllerStore over a temp dir with the given
// max_inflight, enabled and ready for a runner to lead.
func newRunnerStore(t *testing.T, maxInflight int) (*ControllerStore, *tickClock) {
	t.Helper()
	clock := &tickClock{}
	s := NewStoreWithClock(t.TempDir(), clock.Now)
	mustCreate(t, s, "c1", maxInflight)
	mustEnable(t, s, "c1")
	return s, clock
}

// runnerCand builds a provider candidate owned by c1 (reuses the package's
// cand helper with controller/kind overrides).
func runnerCand(wf, node, act string) Candidate {
	c := cand(wf, node, act, withController("c1"), withKind(CandidateProvider))
	c.Revision = 7
	return c
}

// runnerCandWithKey builds a provider candidate owned by c1 with the given
// concurrency key.
func runnerCandWithKey(wf, node, act, key string) Candidate {
	c := cand(wf, node, act, withController("c1"), withKind(CandidateProvider), withKey(key))
	c.Revision = 7
	return c
}

// startRunner constructs a runner with short test cadences and registers its
// stop as cleanup.
func startRunner(t *testing.T, store *ControllerStore, deps RunnerDeps, renew, antiEntropy time.Duration, changeCh, capacityCh chan struct{}) *Runner {
	t.Helper()
	r := NewRunner(RunnerConfig{
		Deps:          deps,
		Store:         store,
		ControllerID:  "c1",
		OwnerID:       "owner-test",
		RenewInterval: renew,
		AntiEntropy:   antiEntropy,
		ChangeCh:      changeCh,
		CapacityCh:    capacityCh,
	})
	t.Cleanup(r.Stop)
	return r
}

// TestRunnerRenewsBeforeEveryDispatch proves the pass structure: a lease
// renewal precedes dispatch on every pass and again immediately before each
// hand-off, observed via the strictly increasing RenewedAt each dispatch
// reads from the store.
func TestRunnerRenewsBeforeEveryDispatch(t *testing.T) {
	store, _ := newRunnerStore(t, 4)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1")})

	startRunner(t, store, deps, 20*time.Millisecond, 50*time.Millisecond, nil, nil)

	waitUntil(t, "at least three dispatches", func() bool { return deps.dispatchCount() >= 3 })
	recs := deps.allDispatches()
	for i, rec := range recs {
		if rec.renewedAt.IsZero() {
			t.Fatalf("dispatch %d observed no leader lease renewal", i)
		}
		if i > 0 && !rec.renewedAt.After(recs[i-1].renewedAt) {
			t.Fatalf("dispatch %d renewedAt %s did not advance past dispatch %d's %s: a renewal must precede every dispatch",
				i, rec.renewedAt, i-1, recs[i-1].renewedAt)
		}
		if rec.lease.LeaseID == "" || rec.lease.OwnerEpoch == 0 {
			t.Fatalf("dispatch %d carried an empty lease: %+v", i, rec.lease)
		}
	}
}

// TestRunnerFailedRenewalStopsDispatchAndReacquires proves a failed renewal
// stops dispatching under the lost lease and the runner falls back to
// acquisition: after an out-of-band lease release, no further dispatch runs
// under the old lease epoch and subsequent dispatches carry the re-acquired
// lease's epoch.
func TestRunnerFailedRenewalStopsDispatchAndReacquires(t *testing.T) {
	store, _ := newRunnerStore(t, 2)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1"), runnerCand("wf2", "start", "a2")})
	block := make(chan struct{})
	deps.mu.Lock()
	deps.block = block
	deps.blockFirst = 2
	deps.mu.Unlock()

	startRunner(t, store, deps, 20*time.Millisecond, 50*time.Millisecond, nil, nil)

	// Both decisions are handed off under epoch 1 and block in Dispatch.
	waitUntil(t, "two in-flight dispatches", func() bool { return deps.dispatchCount() >= 2 })

	// Kill the lease out-of-band: the runner's next renewal must fail.
	rec, _, err := store.Get("c1")
	if err != nil || rec.Leader == nil {
		t.Fatalf("get lease before release: rec=%+v err=%v", rec, err)
	}
	if _, err := store.ReleaseLease("c1", rec.Leader.LeaseID, rec.Leader.OwnerEpoch); err != nil {
		t.Fatalf("out-of-band release: %v", err)
	}
	epoch1Dispatches := deps.dispatchCount()

	// Unblock the two in-flight dispatches; their results are consumed.
	close(block)

	// The runner must re-acquire (new owner epoch) and resume dispatching
	// under the new lease — and every post-release dispatch must carry the
	// new epoch, proving dispatch stopped under the failed renewal.
	waitUntil(t, "dispatch resumed under a re-acquired lease", func() bool {
		for _, rec := range deps.allDispatches() {
			if rec.lease.OwnerEpoch >= 2 {
				return true
			}
		}
		return false
	})
	for i, rec := range deps.allDispatches() {
		wantEpoch := int64(1)
		if i >= epoch1Dispatches {
			wantEpoch = 2
		}
		if rec.lease.OwnerEpoch != wantEpoch {
			t.Fatalf("dispatch %d ran under owner epoch %d, want %d (dispatches must stop after a failed renewal and resume only after re-acquisition)",
				i, rec.lease.OwnerEpoch, wantEpoch)
		}
	}
	waitUntil(t, "runner leading again", func() bool { return storeLeaderEpoch(t, store) >= 2 })
}

func storeLeaderEpoch(t *testing.T, store *ControllerStore) int64 {
	t.Helper()
	rec, ok, err := store.Get("c1")
	if err != nil || !ok || rec.Leader == nil {
		return 0
	}
	return rec.Leader.OwnerEpoch
}

// TestRunnerMaxInflightCountsUnreportedWorkers proves the in-flight limit
// counts decisions handed to workers whose results have not arrived: with a
// blocked Dispatch, at most max_inflight dispatches run concurrently and no
// further dispatch starts across subsequent reconcile passes.
func TestRunnerMaxInflightCountsUnreportedWorkers(t *testing.T) {
	store, _ := newRunnerStore(t, 2)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})
	deps.setCandidates([]Candidate{
		runnerCand("wf1", "start", "a1"),
		runnerCand("wf2", "start", "a2"),
		runnerCand("wf3", "start", "a3"),
	})
	block := make(chan struct{})
	deps.mu.Lock()
	deps.block = block
	deps.blockAll = true // every dispatch blocks
	deps.mu.Unlock()

	r := startRunner(t, store, deps, 20*time.Millisecond, 50*time.Millisecond, nil, nil)

	waitUntil(t, "max_inflight dispatches started", func() bool { return deps.dispatchCount() >= 2 })

	// Across several fast reconcile passes (renew interval is 20ms), the
	// third candidate must never dispatch while the two workers are
	// unreported.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := deps.dispatchCount(); n > 2 {
			t.Fatalf("%d dispatches started with max_inflight=2 and two unreported workers", n)
		}
		if s := r.Status(); s.Inflight > 2 {
			t.Fatalf("status inflight = %d, want <= 2", s.Inflight)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := deps.dispatchCount(); got != 2 {
		t.Fatalf("dispatch count after window = %d, want exactly 2", got)
	}

	close(block)
	waitUntil(t, "third dispatch after capacity freed", func() bool { return deps.dispatchCount() >= 3 })
}

// TestRunnerCapacityBlockedSuppressionPersistsAcrossPasses proves the
// capacity_blocked contract: one blocked dispatch suppresses every later
// dispatch attempt until a capacity-change signal or an anti-entropy pass
// clears the block, and each clear produces exactly one new dispatch attempt.
// Workflow change signals and worker results never clear the block.
func TestRunnerCapacityBlockedSuppressionPersistsAcrossPasses(t *testing.T) {
	store, _ := newRunnerStore(t, 4)
	deps := newFakeDeps(store, "c1")
	// First dispatch holds the pass open until released, then reports stale;
	// second dispatch reports capacity_blocked immediately. Both results
	// must leave the persisted block in place.
	deps.setScript(
		DispatchResult{Kind: ResultStale},
		DispatchResult{Kind: ResultCapacityBlocked, Source: "local"},
	)
	deps.setDefaultResult(DispatchResult{Kind: ResultCapacityBlocked, Source: "local"})
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1"), runnerCand("wf2", "start", "a2")})
	block := make(chan struct{})
	deps.mu.Lock()
	deps.block = block
	deps.blockFirst = 1 // only the first dispatch blocks
	deps.mu.Unlock()

	clock := newManualClock()
	changeCh := make(chan struct{}, 1)
	capacityCh := make(chan struct{}, 1)
	r := NewRunner(RunnerConfig{
		Deps:          deps,
		Store:         store,
		ControllerID:  "c1",
		OwnerID:       "owner-test",
		RenewInterval: 20 * time.Millisecond,
		AntiEntropy:   5 * time.Second,
		ChangeCh:      changeCh,
		CapacityCh:    capacityCh,
		Now:           clock.Now,
	})
	t.Cleanup(r.Stop)

	// Both candidates dispatch in the first pass: the first call blocks, the
	// second returns capacity_blocked and the block is recorded.
	waitUntil(t, "two dispatch attempts", func() bool { return deps.dispatchCount() >= 2 })
	waitUntil(t, "block recorded in status", func() bool {
		s := r.Status()
		return s.CapacityBlocked == "local" && s.LastOutcome == "capacity_blocked(local)"
	})

	// Fifty workflow change signals while blocked: no pass may dispatch.
	for i := 0; i < 50; i++ {
		select {
		case changeCh <- struct{}{}:
		default:
		}
		time.Sleep(2 * time.Millisecond)
	}
	if n := deps.dispatchCount(); n != 2 {
		t.Fatalf("%d dispatches after 50 change signals while blocked, want 2", n)
	}

	// The held first worker completes with stale: the worker result is
	// processed (status shows it) but must not clear the block.
	close(block)
	waitUntil(t, "stale worker result processed while blocked", func() bool {
		return r.Status().LastOutcome == "stale"
	})
	for i := 0; i < 50; i++ {
		select {
		case changeCh <- struct{}{}:
		default:
		}
		time.Sleep(2 * time.Millisecond)
	}
	if n := deps.dispatchCount(); n != 2 {
		t.Fatalf("%d dispatches after change signals and worker results while blocked, want 2", n)
	}
	if s := r.Status(); s.CapacityBlocked != "local" {
		t.Fatalf("status capacity blocked = %q, want local (worker results must not clear the block)", s.CapacityBlocked)
	}

	// The host view shrinks to one dispatchable candidate so each cleared
	// pass makes exactly one dispatch attempt.
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1")})

	// One capacity-change signal: exactly one new dispatch attempt follows,
	// which re-blocks (the default result is capacity_blocked).
	capacityCh <- struct{}{}
	waitUntil(t, "one dispatch after capacity signal", func() bool { return deps.dispatchCount() >= 3 })
	time.Sleep(150 * time.Millisecond)
	if n := deps.dispatchCount(); n != 3 {
		t.Fatalf("%d dispatches after capacity signal, want exactly 3", n)
	}
	waitUntil(t, "block re-recorded", func() bool { return r.Status().CapacityBlocked == "local" })

	// Advancing the clock past the anti-entropy interval: exactly one new
	// dispatch attempt follows, then the block is re-recorded.
	clock.Advance(6 * time.Second)
	waitUntil(t, "one dispatch after anti-entropy", func() bool { return deps.dispatchCount() >= 4 })
	time.Sleep(150 * time.Millisecond)
	if n := deps.dispatchCount(); n != 4 {
		t.Fatalf("%d dispatches after anti-entropy tick, want exactly 4", n)
	}
	if s := r.Status(); s.CapacityBlocked != "local" {
		t.Fatalf("status capacity blocked = %q, want local after re-block", s.CapacityBlocked)
	}
}

// TestRunnerMissedNotificationRecoveredByAntiEntropy proves a candidate that
// appears in the host without any change signal is still picked up: the
// anti-entropy refresh rebuilds the host view and the next pass dispatches
// the new candidate.
func TestRunnerMissedNotificationRecoveredByAntiEntropy(t *testing.T) {
	store, _ := newRunnerStore(t, 4)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})

	r := startRunner(t, store, deps, 20*time.Millisecond, 60*time.Millisecond, nil, nil)

	// The first pass refreshes an empty host view: no candidate, no
	// dispatch.
	waitUntil(t, "initial idle refresh", func() bool { return deps.refreshCount() >= 1 })
	if n := deps.dispatchCount(); n != 0 {
		t.Fatalf("%d dispatches before any candidate existed", n)
	}

	// A candidate appears host-side but only becomes visible to the
	// candidate query after a refresh (a missed change notification). No
	// change signal is sent; only the anti-entropy cadence can reveal it.
	deps.setPending([]Candidate{runnerCand("wf-late", "start", "a1")})
	waitUntil(t, "late candidate dispatched via anti-entropy", func() bool {
		return deps.dispatchCount() >= 1
	})
	if got := deps.refreshCount(); got < 2 {
		t.Fatalf("refreshes = %d, want at least 2 (the idle pass plus the anti-entropy rebuild that revealed the candidate)", got)
	}
	if s := r.Status(); s.LastReconcile.IsZero() {
		t.Fatal("runner never reconciled")
	}
	recs := deps.allDispatches()
	if recs[0].identity.WorkflowID != workflow.WorkflowID("wf-late") {
		t.Fatalf("first dispatch identity = %+v, want the late candidate", recs[0].identity)
	}
}

// TestRunnerNotLeaderDropsLeadershipAndReacquires proves a not_leader
// dispatch result stops dispatching under the stale lease and the runner
// attempts re-acquisition; once the store-side lease is gone, leadership is
// regained and dispatching resumes.
func TestRunnerNotLeaderDropsLeadershipAndReacquires(t *testing.T) {
	store, _ := newRunnerStore(t, 4)
	deps := newFakeDeps(store, "c1")
	deps.setScript(DispatchResult{Kind: ResultNotLeader})
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1")})

	startRunner(t, store, deps, 20*time.Millisecond, 50*time.Millisecond, nil, nil)

	waitUntil(t, "first dispatch attempted", func() bool { return deps.dispatchCount() >= 1 })

	// While the store still shows a live lease for this runner, the
	// re-acquisition cannot succeed and no further dispatch may start.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := deps.dispatchCount(); n > 1 {
			t.Fatalf("%d dispatches after not_leader while the lease was still live; dispatching must stop until leadership is regained", n)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Simulate the store-side invalidation that made Dispatch report
	// not_leader: the lease goes away, so re-acquisition succeeds.
	rec, _, err := store.Get("c1")
	if err != nil || rec.Leader == nil {
		t.Fatalf("get lease: rec=%+v err=%v", rec, err)
	}
	if _, err := store.ReleaseLease("c1", rec.Leader.LeaseID, rec.Leader.OwnerEpoch); err != nil {
		t.Fatalf("release: %v", err)
	}

	waitUntil(t, "dispatch resumed after re-acquisition", func() bool { return deps.dispatchCount() >= 2 })
	waitUntil(t, "runner leading again", func() bool { return storeLeaderEpoch(t, store) >= 2 })
}

// TestRunnerDisableCancelsUnstartedWorkersOnly proves Stop cancels workers
// that have not started their dispatch while an already-started dispatch
// completes and its result is consumed: Stop does not return until the
// in-flight dispatch finishes, and no dispatch begins after cancellation.
func TestRunnerDisableCancelsUnstartedWorkersOnly(t *testing.T) {
	store, _ := newRunnerStore(t, 2)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1"), runnerCand("wf2", "start", "a2")})
	block := make(chan struct{})
	deps.mu.Lock()
	deps.block = block
	deps.blockAll = true          // both dispatches block
	deps.uninterruptibleFirst = 1 // the first is an already-started dispatch
	deps.mu.Unlock()

	r := NewRunner(RunnerConfig{
		Deps:          deps,
		Store:         store,
		ControllerID:  "c1",
		OwnerID:       "owner-test",
		RenewInterval: 20 * time.Millisecond,
		AntiEntropy:   50 * time.Millisecond,
	})
	var closeOnce sync.Once
	t.Cleanup(func() { closeOnce.Do(func() { close(block) }); r.Stop() })

	// Both decisions handed off: worker 1 blocks on the gate, worker 2
	// blocks on the runner's cancellation.
	waitUntil(t, "two in-flight dispatches", func() bool { return deps.dispatchCount() >= 2 })

	stopped := make(chan struct{})
	go func() { r.Stop(); close(stopped) }()

	// Stop must not complete while the started dispatch is still running.
	select {
	case <-stopped:
		t.Fatal("Stop returned while a started dispatch was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	// No new dispatch may start after cancellation, and no dispatch may be
	// entered with an already-canceled context.
	closeOnce.Do(func() { close(block) })
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the in-flight dispatch completed")
	}
	if n := deps.dispatchCount(); n != 2 {
		t.Fatalf("dispatch count = %d, want exactly 2 (no new dispatch after Stop)", n)
	}
	for i, rec := range deps.allDispatches() {
		if rec.lease.LeaseID == "" {
			t.Fatalf("dispatch %d carried no lease", i)
		}
	}
	if s := r.Status(); s.Leading {
		t.Fatal("runner still reports leading after Stop")
	}
}

// TestRunnerStopCleanNoGoroutineLeak proves an idle runner's goroutines all
// exit on Stop: the goroutine count returns to its pre-stop level within a
// loose, retry-sampled tolerance.
func TestRunnerStopCleanNoGoroutineLeak(t *testing.T) {
	store, _ := newRunnerStore(t, 2)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})

	r := NewRunner(RunnerConfig{
		Deps:          deps,
		Store:         store,
		ControllerID:  "c1",
		OwnerID:       "owner-test",
		RenewInterval: 20 * time.Millisecond,
		AntiEntropy:   50 * time.Millisecond,
	})
	waitUntil(t, "runner leading", func() bool { return r.Status().Leading })

	baseline := runtime.NumGoroutine() // includes the runner's leader goroutine
	r.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for {
		now := runtime.NumGoroutine()
		if now <= baseline+2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines after Stop = %d, baseline while leading = %d (leak suspected)", now, baseline)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestRunnerDispatchErrorDoesNotStopPasses proves a deps.Dispatch error is
// contained: the runner records a dispatch_error outcome, keeps leading, and
// later passes keep dispatching. Timer cadences are long so no timer-driven
// pass can run during the assertions; both dispatch attempts are held
// blocked until released, so the follow-up pass triggered by the failed
// worker's result cannot overwrite the recorded outcome.
func TestRunnerDispatchErrorDoesNotStopPasses(t *testing.T) {
	store, _ := newRunnerStore(t, 2)
	deps := newFakeDeps(store, "c1")
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1")})
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})
	deps.setErrors(errors.New("injected dispatch failure"))
	block := make(chan struct{}, 2)
	deps.mu.Lock()
	deps.block = block
	deps.blockFirst = 2 // both dispatch attempts block until released
	deps.mu.Unlock()

	changeCh := make(chan struct{}, 1)
	r := startRunner(t, store, deps, 10*time.Second, 10*time.Second, changeCh, nil)

	// The first pass runs on startup; release the held dispatch so it
	// reports the injected error.
	waitUntil(t, "first dispatch attempted", func() bool { return deps.dispatchCount() >= 1 })
	block <- struct{}{}
	waitUntil(t, "dispatch_error outcome recorded", func() bool {
		return r.Status().LastOutcome == "dispatch_error"
	})
	// The failed worker's result wakes a follow-up pass whose dispatch is
	// held blocked, so the outcome above is stable here.
	if s := r.Status(); !s.Leading {
		t.Fatal("runner dropped leadership after a dispatch error")
	}

	// Clear the injected error, wake the runner, and release the held
	// second dispatch so the follow-up pass dispatches successfully.
	deps.setErrors()
	select {
	case changeCh <- struct{}{}:
	default:
	}
	waitUntil(t, "second dispatch attempted", func() bool { return deps.dispatchCount() >= 2 })
	block <- struct{}{}
	waitUntil(t, "second dispatch recorded", func() bool {
		return r.Status().LastOutcome == "dispatched"
	})
	if s := r.Status(); !s.Leading {
		t.Fatal("runner dropped leadership after a dispatch error")
	}
}

// TestRunnerInflightRefreshedDuringCapacityBlock proves a blocked pass
// refreshes the published in-flight count from the live view: while the
// capacity block persists, the count tracks the live in-flight set (a worker
// reporting or a live attempt terminating changes it) instead of holding the
// value from the last unblocked pass.
func TestRunnerInflightRefreshedDuringCapacityBlock(t *testing.T) {
	store, _ := newRunnerStore(t, 4)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultCapacityBlocked, Source: "local"})
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1"), runnerCand("wf2", "start", "a2")})

	changeCh := make(chan struct{}, 1)
	capacityCh := make(chan struct{}, 1)
	r := startRunner(t, store, deps, 20*time.Millisecond, 5*time.Second, changeCh, capacityCh)

	// Block on the first pass, then wait for the two blocked workers to
	// report so the unreported set is empty and the live view is the only
	// source of the count.
	waitUntil(t, "capacity blocked", func() bool { return r.Status().CapacityBlocked == "local" })
	waitUntil(t, "inflight back to zero while blocked", func() bool {
		s := r.Status()
		return s.CapacityBlocked == "local" && s.Inflight == 0
	})

	// The live in-flight set grows to three c1-owned attempts; a blocked
	// pass (workflow change signal, which does not clear the block) must
	// publish the new count.
	deps.setInFlight([]InFlightAttempt{
		inf("wf1", "start", "a1", withInfController("c1")),
		inf("wf2", "start", "a2", withInfController("c1")),
		inf("wf3", "start", "a3", withInfController("c1")),
	})
	select {
	case changeCh <- struct{}{}:
	default:
	}
	waitUntil(t, "inflight refreshed to 3 while blocked", func() bool {
		s := r.Status()
		return s.CapacityBlocked == "local" && s.Inflight == 3
	})

	// The live set shrinks to one (a worker reported / an attempt
	// terminated); the next blocked pass must publish the lower count.
	deps.setInFlight([]InFlightAttempt{
		inf("wf1", "start", "a1", withInfController("c1")),
	})
	select {
	case changeCh <- struct{}{}:
	default:
	}
	waitUntil(t, "inflight refreshed to 1 while blocked", func() bool {
		s := r.Status()
		return s.CapacityBlocked == "local" && s.Inflight == 1
	})
}

// TestRunnerUnreportedWorkerHoldsConcurrencyKey proves a decision handed to a
// worker that has not reported yet keeps its candidate's concurrency key held:
// with two same-key candidates and a blocked first dispatch, the second
// candidate must never be selected (and thus never dispatched) while the first
// worker is unreported, instead of being picked and failing host-side as
// key_held.
func TestRunnerUnreportedWorkerHoldsConcurrencyKey(t *testing.T) {
	store, _ := newRunnerStore(t, 2)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})
	deps.setCandidates([]Candidate{
		runnerCandWithKey("wf1", "start", "a1", "k"),
		runnerCandWithKey("wf2", "start", "a2", "k"),
	})
	block := make(chan struct{})
	deps.mu.Lock()
	deps.block = block
	deps.blockAll = true // every dispatch blocks
	deps.mu.Unlock()

	startRunner(t, store, deps, 20*time.Millisecond, 50*time.Millisecond, nil, nil)

	// The first candidate is handed to a worker that blocks (has not
	// reported).
	waitUntil(t, "first dispatch started", func() bool { return deps.dispatchCount() >= 1 })

	// Across several fast reconcile passes, the second candidate (same
	// concurrency key) must never dispatch while the first worker is
	// unreported: the seeded in-flight entry holds the key.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := deps.dispatchCount(); n > 1 {
			t.Fatalf("%d dispatches started with two same-key candidates and one unreported worker", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := deps.dispatchCount(); got != 1 {
		t.Fatalf("dispatch count after window = %d, want exactly 1", got)
	}
}

// TestRunnerRefreshErrorKeepsRunningAndRecovers proves an anti-entropy refresh
// error is contained: the runner records a refresh_error outcome, keeps
// leading, and dispatches on the following pass once the error is cleared.
func TestRunnerRefreshErrorKeepsRunningAndRecovers(t *testing.T) {
	store, _ := newRunnerStore(t, 4)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1")})
	deps.setRefreshError(errors.New("injected refresh failure"))

	r := startRunner(t, store, deps, 20*time.Millisecond, 50*time.Millisecond, nil, nil)

	waitUntil(t, "refresh_error outcome recorded", func() bool {
		return r.Status().LastOutcome == "refresh_error"
	})
	if s := r.Status(); !s.Leading {
		t.Fatal("runner dropped leadership after a refresh error")
	}

	// Clear the error; the next pass refreshes and dispatches.
	deps.setRefreshError(nil)
	waitUntil(t, "dispatch after refresh error cleared", func() bool {
		return deps.dispatchCount() >= 1
	})
	if s := r.Status(); !s.Leading {
		t.Fatal("runner not leading after recovery")
	}
}

// TestRunnerCandidatesErrorKeepsRunningAndRecovers proves a candidate view
// error is contained: the runner records a candidates_error outcome, keeps
// leading without dispatching, and dispatches on the following pass once the
// error is cleared.
func TestRunnerCandidatesErrorKeepsRunningAndRecovers(t *testing.T) {
	store, _ := newRunnerStore(t, 4)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1")})
	deps.setCandidatesError(errors.New("injected candidates failure"))

	r := startRunner(t, store, deps, 20*time.Millisecond, 50*time.Millisecond, nil, nil)

	waitUntil(t, "candidates_error outcome recorded", func() bool {
		return r.Status().LastOutcome == "candidates_error"
	})
	if s := r.Status(); !s.Leading {
		t.Fatal("runner dropped leadership after a candidates error")
	}
	if n := deps.dispatchCount(); n != 0 {
		t.Fatalf("runner dispatched %d times despite a candidates error", n)
	}

	// Clear the error; the next pass reads candidates and dispatches.
	deps.setCandidatesError(nil)
	waitUntil(t, "dispatch after candidates error cleared", func() bool {
		return deps.dispatchCount() >= 1
	})
	if s := r.Status(); !s.Leading {
		t.Fatal("runner not leading after recovery")
	}
}

// TestRunnerInFlightErrorKeepsRunningAndRecovers proves an in-flight view
// error is contained: the runner records an inflight_error outcome, keeps
// leading without dispatching, and dispatches on the following pass once the
// error is cleared.
func TestRunnerInFlightErrorKeepsRunningAndRecovers(t *testing.T) {
	store, _ := newRunnerStore(t, 4)
	deps := newFakeDeps(store, "c1")
	deps.setDefaultResult(DispatchResult{Kind: ResultDispatched})
	deps.setCandidates([]Candidate{runnerCand("wf1", "start", "a1")})
	deps.setInFlightError(errors.New("injected in-flight failure"))

	r := startRunner(t, store, deps, 20*time.Millisecond, 50*time.Millisecond, nil, nil)

	waitUntil(t, "inflight_error outcome recorded", func() bool {
		return r.Status().LastOutcome == "inflight_error"
	})
	if s := r.Status(); !s.Leading {
		t.Fatal("runner dropped leadership after an in-flight error")
	}
	if n := deps.dispatchCount(); n != 0 {
		t.Fatalf("runner dispatched %d times despite an in-flight error", n)
	}

	// Clear the error; the next pass reads the in-flight view and dispatches.
	deps.setInFlightError(nil)
	waitUntil(t, "dispatch after in-flight error cleared", func() bool {
		return deps.dispatchCount() >= 1
	})
	if s := r.Status(); !s.Leading {
		t.Fatal("runner not leading after recovery")
	}
}
