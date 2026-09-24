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
)

// DispatchResult is the outcome of one host dispatch attempt.
type DispatchResult struct {
	Kind   DispatchResultKind
	Source string // capacity source: "local" or "tree"
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

// defaultAntiEntropy is the cadence at which a leading runner refreshes the
// host view and reconciles even without change signals.
const defaultAntiEntropy = 5 * time.Second

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
}

// dispatchWorkerResult carries one worker's outcome back to the leader loop.
type dispatchWorkerResult struct {
	identity workflow.ExecutionIdentity
	result   DispatchResult
	err      error
}

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
	// unreported counts decisions handed to workers whose results have not
	// arrived yet, keyed by execution identity. Leader-goroutine-local.
	unreported map[workflow.ExecutionIdentity]int
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
		cfg.AntiEntropy = defaultAntiEntropy
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Runner{
		deps:          cfg.Deps,
		store:         cfg.Store,
		controllerID:  cfg.ControllerID,
		ownerID:       cfg.OwnerID,
		now:           now,
		renewInterval: cfg.RenewInterval,
		antiEntropy:   cfg.AntiEntropy,
		changeCh:      cfg.ChangeCh,
		capacityCh:    cfg.CapacityCh,
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
		results:       make(chan dispatchWorkerResult, 1),
		unreported:    map[workflow.ExecutionIdentity]int{},
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
		r.clearLeadStatus()
	}

	// handleResult incorporates one worker outcome. It reports whether the
	// outcome was a capacity block (stop dispatching for the current pass)
	// or a leadership loss (stop the pass and drop leadership).
	handleResult := func(res dispatchWorkerResult) (blocked, lostLead bool) {
		if res.err != nil {
			log.Printf("workflow controller %s: dispatch %v: %v", r.controllerID, res.identity, res.err)
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "dispatch_error" })
			return false, false
		}
		switch res.result.Kind {
		case ResultDispatched:
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
			if _, _, err := r.store.RecordCapacityBlocked(r.controllerID, source, detail); err != nil {
				log.Printf("workflow controller %s: record capacity blocked: %v", r.controllerID, err)
			}
			r.setStatus(func(s *RunnerStatus) {
				s.LastOutcome = string(ResultCapacityBlocked) + "(" + source + ")"
				s.CapacityBlocked = source
				s.CapacityDetail = detail
			})
			blocked = true
		case ResultNotLeader:
			dropLeadership()
			lostLead = true
		default:
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = string(res.result.Kind) })
		}
		return blocked, lostLead
	}
	// decrementUnreported drops one pending-decision count for an identity.
	decrementUnreported := func(identity workflow.ExecutionIdentity) {
		if r.unreported[identity] <= 0 {
			return
		}
		r.unreported[identity]--
		if r.unreported[identity] == 0 {
			delete(r.unreported, identity)
		}
	}
	// drainResults consumes completed worker results without blocking,
	// folding their outcomes into the current pass.
	drainResults := func() (blocked, lostLead bool) {
		for {
			select {
			case res := <-r.results:
				r.outstanding--
				decrementUnreported(res.identity)
				b, l := handleResult(res)
				blocked = blocked || b
				lostLead = lostLead || l
			default:
				return blocked, lostLead
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
		}
	}
	// waitOrCancel arms the timer at d and waits for it, cancellation, or a
	// wake signal; it reports whether the runner should exit.
	waitOrCancel := func(d time.Duration) bool {
		arm(d)
		select {
		case <-r.ctx.Done():
			release()
			drainWorkers()
			return true
		case <-timer.C:
			return false
		case <-r.changeCh:
			drainSignal(r.changeCh)
			return false
		case <-r.capacityCh:
			drainSignal(r.capacityCh)
			return false
		case res := <-r.results:
			r.outstanding--
			decrementUnreported(res.identity)
			handleResult(res)
			return false
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
			if sleepOrExit := waitOrCancel(r.renewInterval); sleepOrExit {
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
		if lastRefresh.IsZero() || r.now().Sub(lastRefresh) >= r.antiEntropy {
			if err := r.deps.Refresh(); err != nil {
				log.Printf("workflow controller %s: refresh: %v", r.controllerID, err)
				r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "refresh_error" })
				if exit := waitOrCancel(wait); exit {
					return
				}
				continue
			}
			lastRefresh = r.now()
		}
		candidates, err := r.deps.Candidates(r.controllerID)
		if err != nil {
			log.Printf("workflow controller %s: candidates: %v", r.controllerID, err)
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "candidates_error" })
			if exit := waitOrCancel(wait); exit {
				return
			}
			continue
		}
		inflight, err := r.deps.InFlight()
		if err != nil {
			log.Printf("workflow controller %s: in-flight: %v", r.controllerID, err)
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "inflight_error" })
			if exit := waitOrCancel(wait); exit {
				return
			}
			continue
		}
		if rec.MaxInflight <= 0 {
			r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "no_capacity" })
			if exit := waitOrCancel(wait); exit {
				return
			}
			continue
		}

		// Seed the in-flight view with decisions handed to workers whose
		// results have not arrived yet, so a pass never double-dispatches
		// them.
		view := make([]InFlightAttempt, 0, len(inflight)+len(r.unreported))
		view = append(view, inflight...)
		for identity, count := range r.unreported {
			for i := 0; i < count; i++ {
				view = append(view, InFlightAttempt{
					Identity:     identity,
					ControllerID: r.controllerID,
					Terminal:     false,
				})
			}
		}
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
		// results that land mid-pass.
		for _, d := range decisions {
			blocked, lostLead := drainResults()
			if blocked || lostLead || !holding {
				break
			}
			if r.ctx.Err() != nil {
				break
			}
			if _, err := r.store.RenewLease(r.controllerID, leaseID, ownerEpoch); err != nil {
				dropLeadership()
				break
			}
			r.unreported[d.Candidate.Identity]++
			r.outstanding++
			go r.dispatchWorker(d, LeaderLease{LeaseID: leaseID, OwnerEpoch: ownerEpoch})
		}

		if !holding {
			// Leadership was lost mid-pass; fall back to acquisition.
			sleep(r.renewInterval)
			continue
		}
		if exit := waitOrCancel(wait); exit {
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
