package workflowcontroller

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/workflow"
)

// DispatchResultKind mirrors the host dispatch outcomes the runner reacts to.
type DispatchResultKind string

const (
	// ResultDispatched means the host claimed the candidate and started its
	// runtime.
	ResultDispatched DispatchResultKind = "dispatched"
	// ResultCapacityBlocked means admission was unavailable; the candidate
	// remains dispatchable.
	ResultCapacityBlocked DispatchResultKind = "capacity_blocked"
	// ResultStale means the candidate was no longer dispatchable (stale
	// revision, competing claim, or policy mismatch).
	ResultStale DispatchResultKind = "stale"
	// ResultKeyHeld means another live attempt holds the candidate's
	// concurrency key.
	ResultKeyHeld DispatchResultKind = "key_held"
	// ResultNotLeader means the runner's leader lease did not validate at
	// dispatch time.
	ResultNotLeader DispatchResultKind = "not_leader"
	// ResultStartFailed means the runtime start failed after the attempt was
	// durably recorded; kernel attempt/retry semantics apply.
	ResultStartFailed DispatchResultKind = "start_failed"
	// ResultCanceled means the dispatch was canceled through its context.
	ResultCanceled DispatchResultKind = "canceled"
	// ResultParked means the candidate was an auto external activation parked
	// kernel-locally; its poll seeds schedule the gate's polling cursors.
	ResultParked DispatchResultKind = "parked"
	// ResultUnresolvedBinding means the candidate's gate bindings were not
	// fully resolved; the activation stays ready and a deduplicated
	// diagnostic is recorded.
	ResultUnresolvedBinding DispatchResultKind = "unresolved_binding"
)

// DispatchResult is the outcome of one host dispatch attempt.
type DispatchResult struct {
	Kind   DispatchResultKind
	Source string // capacity source: "local" or "tree"
	Detail string // inert diagnostic detail
	// PollSeeds carries the parked activation's required external gates when
	// Kind is ResultParked; each gate becomes a poll cursor.
	PollSeeds []workflow.ParkedGateRef
}

// RunnerDeps is the host-side surface the runner drives. The host implements
// it over its own manager and dispatch boundary; the runner never imports
// host code.
type RunnerDeps interface {
	// Candidates returns the ready candidates for the controller.
	Candidates(controllerID string) ([]Candidate, error)
	// InFlight returns the currently live attempts across all controllers.
	InFlight() ([]InFlightAttempt, error)
	// Refresh rebuilds any host-side cached view the candidate and in-flight
	// queries read from.
	Refresh() error
	// ParkedGates returns one gate reference per resolved bound required
	// external gate on the controller's parked awaiting_gate activations. The
	// runner re-seeds missing poll cursors from it on every anti-entropy pass.
	ParkedGates(controllerID string) ([]workflow.ParkedGateRef, error)
	// Dispatch dispatches one selected candidate under the runner's lease.
	Dispatch(ctx context.Context, d Decision, lease LeaderLease) (DispatchResult, error)
}

// RunnerStatus is a point-in-time snapshot of a runner for host status
// reporting.
type RunnerStatus struct {
	Leading         bool
	LastReconcile   time.Time
	LastOutcome     string
	Inflight        int
	CapacityBlocked string // "" | "local" | "tree"
	CapacityDetail  string
}

// DefaultAntiEntropy is the cadence at which a leading runner refreshes the
// host view and reconciles even without change signals.
const DefaultAntiEntropy = 5 * time.Second

// RunnerConfig configures one Runner. Deps, Store, ControllerID, and OwnerID
// are required; zero durations fall back to the package defaults and nil
// channels disable the corresponding wake signals.
type RunnerConfig struct {
	Deps          RunnerDeps
	Store         *ControllerStore
	ControllerID  string
	OwnerID       string
	RenewInterval time.Duration
	AntiEntropy   time.Duration
	ChangeCh      <-chan struct{}
	CapacityCh    <-chan struct{}
	Now           func() time.Time
	// Poll, when non-nil, enables external-gate adapter polling alongside
	// dispatch. PollBaseDelay is the interval before the first poll after a
	// park (production: 30s). PollJitter returns a bounded jitter scalar in
	// [-1, 1]; MaxPollWorkers bounds concurrent adapter invocations.
	Poll           Poller
	PollBaseDelay  time.Duration
	PollJitter     func() float64
	MaxPollWorkers int
}

// dispatchWorkerResult carries one worker's outcome back to the leader loop.
type dispatchWorkerResult struct {
	identity workflow.ExecutionIdentity
	result   DispatchResult
	err      error
}

// unreportedEntry tracks the pending-decision count for one identity and the
// candidate's concurrency key, so the seeded in-flight view keeps the key held
// while the worker's result is still outstanding.
type unreportedEntry struct {
	count int
	key   string
}

// wakeReason reports which wakeup ended the previous pass. Only a
// capacity-change wakeup or an anti-entropy pass clears a persisted capacity
// block; workflow changes and worker results must not.
type wakeReason int

const (
	wakeNone     wakeReason = iota // no wakeup observed yet (first pass)
	wakeTimer                      // cadence timer elapsed
	wakeChange                     // workflow change signal
	wakeCapacity                   // capacity-change signal
	wakeResult                     // worker result arrived
)

// Runner is one controller's in-process reconciler. It holds the controller's
// leadership lease, periodically selects ready candidates via Select, and
// hands each decision to an untracked worker goroutine that calls the host's
// Dispatch. It owns no authoritative state: the candidate and in-flight
// views come from RunnerDeps, and durable controller state lives in the
// ControllerStore.
type Runner struct {
	deps          RunnerDeps
	store         *ControllerStore
	controllerID  string
	ownerID       string
	now           func() time.Time
	renewInterval time.Duration
	antiEntropy   time.Duration
	changeCh      <-chan struct{}
	capacityCh    <-chan struct{}

	statusMu sync.Mutex
	status   RunnerStatus

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	// results carries worker outcomes to the leader loop. It is buffered so
	// a worker never blocks longer than one outstanding result while the
	// leader is inside a pass.
	results chan dispatchWorkerResult
	// pollResults carries poll worker outcomes to the leader loop.
	pollResults chan pollWorkerResult
	// pollInFlight holds the cursor keys with a poll worker still running
	// (leader-goroutine-local), so a due cursor is never re-offered while its
	// invocation runs.
	pollInFlight map[string]bool
	// pendingPolls is the number of poll workers that have not delivered a
	// result yet. Leader-goroutine-local.
	pendingPolls int
	// poll is the optional host poll surface; nil disables polling.
	poll Poller
	// pollBase is the interval before the first poll after a park and the
	// floor for adapter-requested delays.
	pollBase time.Duration
	// pollJitter scales backoff delays within bounded bounds.
	pollJitter func() float64
	// maxPollWorkers bounds concurrent adapter invocations.
	maxPollWorkers int
	// unreported tracks decisions handed to workers whose results have not
	// arrived yet, keyed by execution identity. Leader-goroutine-local.
	unreported map[workflow.ExecutionIdentity]unreportedEntry
	// outstanding is the number of workers that have not delivered a result
	// yet. Leader-goroutine-local.
	outstanding int
}

// NewRunner constructs a Runner and starts its leader goroutine. Stop ends
// it.
func NewRunner(cfg RunnerConfig) *Runner {
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = RenewInterval
	}
	if cfg.AntiEntropy <= 0 {
		cfg.AntiEntropy = DefaultAntiEntropy
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	ctx, cancel := context.WithCancel(context.Background())
	jitter := cfg.PollJitter
	if jitter == nil {
		jitter = defaultPollJitter
	}
	maxPollWorkers := cfg.MaxPollWorkers
	if maxPollWorkers <= 0 {
		maxPollWorkers = defaultMaxPollWorkers
	}
	pollBase := cfg.PollBaseDelay
	if pollBase <= 0 {
		pollBase = pollBaseDelay
	}
	r := &Runner{
		deps:           cfg.Deps,
		store:          cfg.Store,
		controllerID:   cfg.ControllerID,
		ownerID:        cfg.OwnerID,
		now:            now,
		renewInterval:  cfg.RenewInterval,
		antiEntropy:    cfg.AntiEntropy,
		changeCh:       cfg.ChangeCh,
		capacityCh:     cfg.CapacityCh,
		ctx:            ctx,
		cancel:         cancel,
		done:           make(chan struct{}),
		results:        make(chan dispatchWorkerResult, 1),
		pollResults:    make(chan pollWorkerResult, 1),
		pollInFlight:   map[string]bool{},
		poll:           cfg.Poll,
		pollBase:       pollBase,
		pollJitter:     jitter,
		maxPollWorkers: maxPollWorkers,
		unreported:     map[workflow.ExecutionIdentity]unreportedEntry{},
	}
	go r.loop()
	return r
}

// Stop cancels the runner and waits for the leader goroutine and all
// outstanding workers to finish. Idempotent. Already-started dispatch calls
// are not interrupted; workers started after the cancel never dispatch.
func (r *Runner) Stop() {
	r.cancel()
	<-r.done
}

// Done is closed when the runner has fully stopped.
func (r *Runner) Done() <-chan struct{} { return r.done }

// Status returns a snapshot of the runner's current status.
func (r *Runner) Status() RunnerStatus {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	return r.status
}

func (r *Runner) setStatus(mut func(s *RunnerStatus)) {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	mut(&r.status)
}

// clearLeadStatus clears the leadership-scoped status fields; LastReconcile
// and LastOutcome are retained.
func (r *Runner) clearLeadStatus() {
	r.setStatus(func(s *RunnerStatus) {
		s.Leading = false
		s.Inflight = 0
		s.CapacityBlocked = ""
		s.CapacityDetail = ""
	})
}

// buildInflightView seeds the live in-flight view with decisions handed to
// workers whose results have not arrived yet, so a pass never double-dispatches
// them. Each seeded entry carries the candidate's concurrency key so the key
// stays held while the worker's result is outstanding.
func (r *Runner) buildInflightView(inflight []InFlightAttempt) []InFlightAttempt {
	view := make([]InFlightAttempt, 0, len(inflight)+len(r.unreported))
	view = append(view, inflight...)
	for identity, entry := range r.unreported {
		for i := 0; i < entry.count; i++ {
			view = append(view, InFlightAttempt{
				Identity:       identity,
				ControllerID:   r.controllerID,
				ConcurrencyKey: entry.key,
				Terminal:       false,
			})
		}
	}
	return view
}

// seedCursor builds the poll cursor a parked gate reference stands for.
func seedCursor(seed workflow.ParkedGateRef) PollCursor {
	return PollCursor{
		WorkflowID:   string(seed.WorkflowID),
		NodeID:       string(seed.NodeID),
		ActivationID: string(seed.ActivationID),
		GateID:       string(seed.GateID),
		AdapterID:    seed.AdapterID,
		SubjectHash:  seed.SubjectHash,
	}
}

// reseedPollCursors re-creates poll cursors for parked external gates whose
// cursor is missing. A crash or a failed EnsurePollCursor between a park
// commit and cursor creation would otherwise strand the activation in
// awaiting_gate with nothing polling it; the anti-entropy pass repairs that.
// EnsurePollCursor is idempotent, so an existing cursor is never reset; the
// known cursor keys only prune the work.
func (r *Runner) reseedPollCursors(existing map[string]*PollCursor) {
	seeds, err := r.deps.ParkedGates(r.controllerID)
	if err != nil {
		log.Printf("workflow controller %s: parked gates: %v", r.controllerID, err)
		r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "parked_gates_error" })
		return
	}
	for _, seed := range seeds {
		cursor := seedCursor(seed)
		if _, ok := existing[PollCursorKey(cursor)]; ok {
			continue
		}
		if _, _, err := r.store.EnsurePollCursor(r.controllerID, cursor, r.now().Add(r.pollBase)); err != nil {
			log.Printf("workflow controller %s: ensure poll cursor %s: %v", r.controllerID, PollCursorKey(cursor), err)
		}
	}
}

// loop is the leader goroutine: it acquires and holds the controller's lease
// and runs reconcile passes while leading.
func (r *Runner) loop() {
	defer close(r.done)
	defer r.clearLeadStatus()

	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	var leaseID string
	var ownerEpoch int64
	holding := false
	var lastRefresh time.Time
	wake := wakeNone
	var exit bool
	// blockedSource is the persisted capacity block: while set, passes keep
	// renewing the lease, refreshing views, publishing status, and processing
	// worker results, but take no dispatch decisions. It clears on a
	// capacity-change signal, an anti-entropy pass, a successful dispatch, or
	// a leadership loss.
	var blockedSource string

	release := func() {
		if !holding {
			return
		}
		holding = false
		// Best-effort: a concurrent disable already released the lease
		// durably, so a CAS failure here is expected and ignorable.
		_, _ = r.store.ReleaseLease(r.controllerID, leaseID, ownerEpoch)
	}
	dropLeadership := func() {
		holding = false
		leaseID = ""
		blockedSource = ""
		r.clearLeadStatus()
	}

	// clearBlock drops the persisted capacity block and its status fields.
	clearBlock := func() {
		blockedSource = ""
		r.setStatus(func(s *RunnerStatus) {
			s.CapacityBlocked = ""
			s.CapacityDetail = ""
		})
	}
	// handleResult incorporates one worker outcome. It reports whether the
	// outcome was a leadership loss (stop the pass and drop leadership). A
	// capacity block persists across passes until a capacity-change signal or
	// an anti-entropy pass clears it.
	handleResult := func(res dispatchWorkerResult) (lostLead bool) {
		if res.err != nil {
			log.Printf("workflow controller %s: dispatch %v: %v", r.controllerID, res.identity, res.err)
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "dispatch_error" })
			return false
		}
		switch res.result.Kind {
		case ResultDispatched:
			blockedSource = ""
			if _, _, err := r.store.ClearCapacityBlocked(r.controllerID); err != nil {
				log.Printf("workflow controller %s: clear capacity blocked: %v", r.controllerID, err)
			}
			r.setStatus(func(s *RunnerStatus) {
				s.LastOutcome = string(ResultDispatched)
				s.CapacityBlocked = ""
				s.CapacityDetail = ""
			})
		case ResultCapacityBlocked:
			source := res.result.Source
			detail := "local capacity exhausted"
			if source == "tree" {
				detail = "descendant_budget"
			}
			blockedSource = source
			if _, _, err := r.store.RecordCapacityBlocked(r.controllerID, source, detail); err != nil {
				log.Printf("workflow controller %s: record capacity blocked: %v", r.controllerID, err)
			}
			r.setStatus(func(s *RunnerStatus) {
				s.LastOutcome = string(ResultCapacityBlocked) + "(" + source + ")"
				s.CapacityBlocked = source
				s.CapacityDetail = detail
			})
		case ResultNotLeader:
			dropLeadership()
			lostLead = true
		case ResultParked:
			for _, seed := range res.result.PollSeeds {
				cursor := seedCursor(seed)
				if _, _, err := r.store.EnsurePollCursor(r.controllerID, cursor, r.now().Add(r.pollBase)); err != nil {
					log.Printf("workflow controller %s: ensure poll cursor %s: %v", r.controllerID, PollCursorKey(cursor), err)
				}
			}
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = string(ResultParked) })
		case ResultUnresolvedBinding:
			name := string(res.identity.WorkflowID) + "/" + string(res.identity.NodeID) + "/" + string(res.identity.ActivationID)
			if _, err := r.store.RecordDiagnostic(r.controllerID, "unresolved_binding", name, res.result.Detail); err != nil {
				log.Printf("workflow controller %s: record unresolved_binding: %v", r.controllerID, err)
			}
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = string(ResultUnresolvedBinding) })
		default:
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = string(res.result.Kind) })
		}
		return lostLead
	}
	// decrementUnreported drops one pending-decision count for an identity.
	decrementUnreported := func(identity workflow.ExecutionIdentity) {
		e := r.unreported[identity]
		if e.count <= 0 {
			return
		}
		e.count--
		if e.count == 0 {
			delete(r.unreported, identity)
		} else {
			r.unreported[identity] = e
		}
	}
	// drainResults consumes completed worker results without blocking,
	// folding their outcomes into the current pass.
	drainResults := func() (lostLead bool) {
		for {
			select {
			case res := <-r.results:
				r.outstanding--
				decrementUnreported(res.identity)
				if handleResult(res) {
					lostLead = true
				}
			default:
				return lostLead
			}
		}
	}
	// handlePollResult folds one poll worker outcome into the current pass.
	handlePollResult := func(res pollWorkerResult) {
		r.pendingPolls--
		if r.handlePollOutcome(res.outcome, LeaderLease{LeaseID: leaseID, OwnerEpoch: ownerEpoch}) {
			dropLeadership()
		}
	}
	// drainPollResults consumes completed poll worker results without
	// blocking, folding their outcomes into the current pass.
	drainPollResults := func() {
		for {
			select {
			case res := <-r.pollResults:
				handlePollResult(res)
			default:
				return
			}
		}
	}
	// drainWorkers waits for every outstanding worker before exiting so no
	// goroutine outlives the runner.
	drainWorkers := func() {
		for r.outstanding > 0 {
			res := <-r.results
			r.outstanding--
			decrementUnreported(res.identity)
		}
		for r.pendingPolls > 0 {
			handlePollResult(<-r.pollResults)
		}
	}
	// arm schedules the next wakeup at d.
	arm := func(d time.Duration) {
		if d < 0 {
			d = 0
		}
		timer.Reset(d)
	}
	// sleep arms the timer at d and waits for it, cancellation, or a worker
	// result (so blocked workers are never stranded while not leading).
	sleep := func(d time.Duration) {
		arm(d)
		select {
		case <-timer.C:
		case <-r.ctx.Done():
		case res := <-r.results:
			r.outstanding--
			decrementUnreported(res.identity)
			handleResult(res)
		case res := <-r.pollResults:
			handlePollResult(res)
		}
	}
	// waitOrCancel arms the timer at d and waits for it, cancellation, or a
	// wake signal; it reports the wakeup reason and whether the runner should
	// exit.
	waitOrCancel := func(d time.Duration) (wake wakeReason, exit bool) {
		arm(d)
		select {
		case <-r.ctx.Done():
			release()
			drainWorkers()
			return wakeNone, true
		case <-timer.C:
			return wakeTimer, false
		case <-r.changeCh:
			drainSignal(r.changeCh)
			return wakeChange, false
		case <-r.capacityCh:
			drainSignal(r.capacityCh)
			return wakeCapacity, false
		case res := <-r.results:
			r.outstanding--
			decrementUnreported(res.identity)
			handleResult(res)
			return wakeResult, false
		case res := <-r.pollResults:
			handlePollResult(res)
			return wakeResult, false
		}
	}

	for {
		if r.ctx.Err() != nil {
			release()
			drainWorkers()
			return
		}

		if !holding {
			rec, granted, err := r.store.AcquireLease(r.controllerID, r.ownerID)
			if err != nil {
				if errors.Is(err, ErrDisabled) {
					drainWorkers()
					return
				}
				log.Printf("workflow controller %s: acquire lease: %v", r.controllerID, err)
				r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "acquire_error" })
				sleep(r.renewInterval)
				continue
			}
			if !granted {
				sleep(r.renewInterval)
				continue
			}
			leaseID = rec.Leader.LeaseID
			ownerEpoch = rec.Leader.OwnerEpoch
			holding = true
			blockedSource = ""
			lastRefresh = time.Time{}
			r.setStatus(func(s *RunnerStatus) { s.Leading = true })
		}

		// Renew first on every pass. A failed renewal stops dispatching and
		// falls back to acquisition.
		if _, err := r.store.RenewLease(r.controllerID, leaseID, ownerEpoch); err != nil {
			dropLeadership()
			sleep(r.renewInterval)
			continue
		}
		nextRenewDue := r.now().Add(r.renewInterval)

		rec, ok, err := r.store.Get(r.controllerID)
		if err != nil {
			// Transient read error: keep the lease and retry on the next
			// tick rather than releasing leadership.
			log.Printf("workflow controller %s: read state: %v", r.controllerID, err)
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "state_error" })
			wake, exit = waitOrCancel(r.renewInterval)
			if exit {
				return
			}
			continue
		}
		if !ok || rec.DesiredState != DesiredEnabled {
			release()
			drainWorkers()
			return
		}

		// Reconcile pass.
		wait := r.antiEntropy
		if d := nextRenewDue.Sub(r.now()); d < wait {
			wait = d
		}
		// Poll scheduling shares the pass: due cursors are offered to bounded
		// workers and the wait folds in the earliest scheduled next poll.
		if r.poll != nil {
			drainPollResults()
			if !holding {
				// A stale apply dropped leadership mid-pass; re-acquire before
				// offering any further polls.
				sleep(r.renewInterval)
				continue
			}
			r.offerPolls()
			wait = r.nextPollWait(wait)
		}
		refreshed := false
		if lastRefresh.IsZero() || r.now().Sub(lastRefresh) >= r.antiEntropy {
			if err := r.deps.Refresh(); err != nil {
				log.Printf("workflow controller %s: refresh: %v", r.controllerID, err)
				r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "refresh_error" })
				wake, exit = waitOrCancel(wait)
				if exit {
					return
				}
				continue
			}
			lastRefresh = r.now()
			refreshed = true
			// The same pass that refreshes re-seeds poll cursors lost to a
			// crash between a park commit and cursor creation.
			if r.poll != nil {
				r.reseedPollCursors(rec.PollCursors)
			}
		}
		// A capacity-change signal or an anti-entropy pass clears a persisted
		// capacity block; the pass that clears it dispatches again.
		if blockedSource != "" && (wake == wakeCapacity || refreshed) {
			clearBlock()
		}
		wake = wakeNone
		if blockedSource != "" {
			// Blocked: this pass renews, refreshes, and publishes status but
			// takes no dispatch decisions until the block clears. The in-flight
			// count is refreshed from the live view so it never goes stale
			// while the block persists.
			inflight, err := r.deps.InFlight()
			if err != nil {
				log.Printf("workflow controller %s: in-flight: %v", r.controllerID, err)
				r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "inflight_error" })
				wake, exit = waitOrCancel(wait)
				if exit {
					return
				}
				continue
			}
			r.setStatus(func(s *RunnerStatus) {
				s.LastReconcile = r.now()
				s.Inflight = activeCount(r.buildInflightView(inflight), r.controllerID)
			})
			wake, exit = waitOrCancel(wait)
			if exit {
				return
			}
			continue
		}
		candidates, err := r.deps.Candidates(r.controllerID)
		if err != nil {
			log.Printf("workflow controller %s: candidates: %v", r.controllerID, err)
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "candidates_error" })
			wake, exit = waitOrCancel(wait)
			if exit {
				return
			}
			continue
		}
		inflight, err := r.deps.InFlight()
		if err != nil {
			log.Printf("workflow controller %s: in-flight: %v", r.controllerID, err)
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "inflight_error" })
			wake, exit = waitOrCancel(wait)
			if exit {
				return
			}
			continue
		}
		if rec.MaxInflight <= 0 {
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "no_capacity" })
			wake, exit = waitOrCancel(wait)
			if exit {
				return
			}
			continue
		}

		// Seed the in-flight view with decisions handed to workers whose
		// results have not arrived yet, so a pass never double-dispatches
		// them.
		view := r.buildInflightView(inflight)
		decisions := Select(SelectInput{
			ControllerID: r.controllerID,
			Candidates:   candidates,
			InFlight:     view,
			Now:          r.now(),
			MaxInflight:  rec.MaxInflight,
		})
		reconciledAt := r.now()
		r.setStatus(func(s *RunnerStatus) {
			s.LastReconcile = reconciledAt
			s.Inflight = activeCount(view, r.controllerID)
			if len(decisions) == 0 && s.CapacityBlocked == "" {
				s.LastOutcome = "idle"
			}
		})

		// Dispatch loop: renew before each hand-off, and honor worker
		// results that land mid-pass. A capacity block from any result
		// suppresses the remaining hand-offs and persists across passes.
		for _, d := range decisions {
			lostLead := drainResults()
			if lostLead || blockedSource != "" || !holding {
				break
			}
			if r.ctx.Err() != nil {
				break
			}
			if _, err := r.store.RenewLease(r.controllerID, leaseID, ownerEpoch); err != nil {
				dropLeadership()
				break
			}
			e := r.unreported[d.Candidate.Identity]
			e.count++
			e.key = d.Candidate.ConcurrencyKey
			r.unreported[d.Candidate.Identity] = e
			r.outstanding++
			go r.dispatchWorker(d, LeaderLease{LeaseID: leaseID, OwnerEpoch: ownerEpoch})
		}

		if !holding {
			// Leadership was lost mid-pass; fall back to acquisition.
			sleep(r.renewInterval)
			continue
		}
		wake, exit = waitOrCancel(wait)
		if exit {
			return
		}
	}
}

// drainSignal empties a coalescing wake channel non-blockingly.
func drainSignal(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// dispatchWorker runs one host dispatch on its own goroutine. It checks for
// cancellation before starting and always delivers exactly one result to the
// leader loop; the leader drains the channel before exiting, so a blocking
// send cannot outlive the runner.
func (r *Runner) dispatchWorker(d Decision, lease LeaderLease) {
	res := dispatchWorkerResult{identity: d.Candidate.Identity}
	if r.ctx.Err() == nil {
		res.result, res.err = r.deps.Dispatch(r.ctx, d, lease)
	} else {
		res.result = DispatchResult{Kind: ResultCanceled}
	}
	r.results <- res
}
