package workflowcontroller

// poller.go implements external-gate adapter polling inside the serialized
// leader runner. Due cursors are committed (count + poll ID) by the leader
// goroutine and then handed to bounded worker goroutines that perform the
// adapter invocation only — an I/O goroutine never writes gate state. The
// leader receives each result, revalidates leadership and the parked
// activation through the host's ApplyResult, schedules the next backoff, and
// records deduplicated diagnostics.

import (
	"context"
	"log"
	"math/rand/v2"
	"time"
)

// PollFailureKind classifies why a poll produced no adapter result.
type PollFailureKind string

const (
	// PollFailureNone means the adapter produced a parsed result.
	PollFailureNone PollFailureKind = ""
	// PollFailureTransient means the invocation itself failed (timeout,
	// invalid result, context cancellation, I/O error).
	PollFailureTransient PollFailureKind = "transient"
	// PollFailureUnavailable means the adapter ID is unknown or its
	// executable could not be run; the gate stays pending and the failure is
	// recorded as a deduplicated diagnostic.
	PollFailureUnavailable PollFailureKind = "adapter_unavailable"
	// PollFailureObsolete means the cursor no longer matches the parked
	// activation (resolved or superseded by a new head); its cursor is
	// dropped.
	PollFailureObsolete PollFailureKind = "obsolete"
)

// PollOutcome is one poll worker's outcome delivered to the leader loop.
type PollOutcome struct {
	Cursor  PollCursor
	Result  *AdapterResult
	Failure PollFailureKind
	Err     error
}

// PollApplyOutcome classifies how the host landed a completed adapter result.
type PollApplyOutcome string

const (
	// PollApplied means the evidence was staged and the external_result gate
	// command landed (or was an idempotent replay).
	PollApplied PollApplyOutcome = "applied"
	// PollStale means the leader lease or the parked activation revalidation
	// failed; the result was discarded and the cursor left untouched.
	PollStale PollApplyOutcome = "stale"
	// PollObsolete means the activation is no longer parked on this gate
	// (resolved or superseded by a new head); the cursor is dropped.
	PollObsolete PollApplyOutcome = "obsolete"
)

// Poller is the host-side external-poll surface. Poll performs one adapter
// invocation for the cursor — I/O only, never gate state — and classifies a
// missing result as transient, unavailable, or obsolete. ApplyResult runs on
// the leader goroutine: it revalidates the leader lease and the parked
// activation, stages the bounded raw stdout as evidence, and submits the
// structured external_result gate command.
type Poller interface {
	Poll(ctx context.Context, cursor PollCursor) (AdapterResult, PollFailureKind, error)
	ApplyResult(cursor PollCursor, res *AdapterResult, lease LeaderLease) (PollApplyOutcome, error)
}

// defaultMaxPollWorkers bounds the adapter invocations running concurrently.
const defaultMaxPollWorkers = 4

// defaultPollJitter returns a bounded jitter scalar in [-1, 1].
func defaultPollJitter() float64 { return 2*rand.Float64() - 1 }

// pollWorkerResult carries one poll worker outcome to the leader loop.
type pollWorkerResult struct {
	outcome PollOutcome
}

// pollWorker runs one adapter invocation on its own goroutine. It checks for
// cancellation before starting and always delivers exactly one result to the
// leader loop; the leader drains the channel before exiting, so a blocking
// send cannot outlive the runner. The invocation's context is the runner's:
// cancellation (disable or shutdown) kills the adapter's process group.
func (r *Runner) pollWorker(cursor PollCursor) {
	out := PollOutcome{Cursor: cursor, Failure: PollFailureTransient}
	if r.ctx.Err() == nil {
		res, failure, err := r.poll.Poll(r.ctx, cursor)
		out.Result, out.Failure, out.Err = &res, failure, err
	} else {
		out.Err = r.ctx.Err()
	}
	r.pollResults <- pollWorkerResult{outcome: out}
}

// offerPolls commits every due cursor and hands it to a bounded worker. The
// count commits BEFORE the invocation so a crash reuses the same poll ID; a
// cursor whose worker is still running is never re-offered.
func (r *Runner) offerPolls() {
	due, err := r.store.PollDue(r.controllerID, r.now())
	if err != nil {
		log.Printf("workflow controller %s: poll due: %v", r.controllerID, err)
		return
	}
	for _, cursor := range due {
		key := PollCursorKey(cursor)
		if r.pollInFlight[key] {
			continue
		}
		if len(r.pollInFlight) >= r.maxPollWorkers {
			return
		}
		committed, err := r.store.CommitPoll(r.controllerID, cursor, r.now())
		if err != nil {
			log.Printf("workflow controller %s: commit poll %s: %v", r.controllerID, key, err)
			continue
		}
		r.pollInFlight[key] = true
		r.pendingPolls++
		go r.pollWorker(committed)
	}
}

// handlePollOutcome folds one poll result into the schedule: backoff on
// pending, transient, or unavailable outcomes; the host's ApplyResult on a
// completed verdict, with the cursor cleared once the result is applied.
func (r *Runner) handlePollOutcome(out PollOutcome, lease LeaderLease) {
	key := PollCursorKey(out.Cursor)
	delete(r.pollInFlight, key)

	if r.ctx.Err() != nil {
		// The runner is exiting (disable or shutdown): the canceled poll is
		// left in flight so the next leader reuses its count and poll ID,
		// exactly as after a crash.
		return
	}

	if out.Failure == PollFailureObsolete {
		// The cursor no longer matches the parked activation; drop it.
		if err := r.store.ClearPollCursor(r.controllerID, out.Cursor); err != nil {
			log.Printf("workflow controller %s: clear obsolete poll cursor %s: %v", r.controllerID, key, err)
		}
		r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "poll_obsolete" })
		return
	}
	if out.Failure == PollFailureUnavailable {
		detail := "adapter unavailable"
		if out.Err != nil {
			detail = out.Err.Error()
		}
		if _, err := r.store.RecordDiagnostic(r.controllerID, string(PollFailureUnavailable), out.Cursor.AdapterID, detail); err != nil {
			log.Printf("workflow controller %s: record adapter_unavailable: %v", r.controllerID, err)
		}
		r.schedulePollRetry(out.Cursor, nil)
		r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "adapter_unavailable" })
		return
	}
	if out.Err != nil {
		log.Printf("workflow controller %s: poll %s: %v", r.controllerID, key, out.Err)
		r.schedulePollRetry(out.Cursor, nil)
		r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "poll_error" })
		return
	}
	// A successful invocation clears any open unavailable diagnostic for the
	// adapter.
	if err := r.store.ClearDiagnostic(r.controllerID, string(PollFailureUnavailable), out.Cursor.AdapterID); err != nil {
		log.Printf("workflow controller %s: clear adapter_unavailable: %v", r.controllerID, err)
	}
	res := out.Result
	if res == nil || res.Result == AdapterResultPending {
		r.schedulePollRetry(out.Cursor, retryAfterOf(res))
		r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "pending" })
		return
	}
	apply, err := r.poll.ApplyResult(out.Cursor, res, lease)
	if err != nil {
		log.Printf("workflow controller %s: apply poll result %s: %v", r.controllerID, key, err)
		r.schedulePollRetry(out.Cursor, nil)
		r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "apply_error" })
		return
	}
	if apply == PollStale {
		// The leader lease or the parked activation no longer matches; the
		// result is discarded and the cursor left for the active leader.
		r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "poll_stale" })
		return
	}
	if apply == PollObsolete {
		// The gate is no longer parked on this activation; no further polls
		// are meaningful, so drop the cursor.
		if err := r.store.ClearPollCursor(r.controllerID, out.Cursor); err != nil {
			log.Printf("workflow controller %s: clear obsolete poll cursor %s: %v", r.controllerID, key, err)
		}
		r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "poll_obsolete" })
		return
	}
	if err := r.store.ClearPollCursor(r.controllerID, out.Cursor); err != nil {
		log.Printf("workflow controller %s: clear poll cursor %s: %v", r.controllerID, key, err)
	}
	r.setStatus(func(s *RunnerStatus) { s.LastOutcome = "applied:" + res.Result })
}

// retryAfterOf returns the adapter's requested retry delay, if any.
func retryAfterOf(res *AdapterResult) *int64 {
	if res == nil {
		return nil
	}
	return res.RetryAfterMS
}

// schedulePollRetry arms the cursor's next poll with backoff: the adapter's
// clamped retry_after when it requested one, otherwise the doubling interval
// bounded by the cap, with bounded jitter.
func (r *Runner) schedulePollRetry(cursor PollCursor, retryAfterMS *int64) {
	delay, nextRetry := PollBackoffDelay(cursor.RetryCount, retryAfterMS, r.pollJitter)
	if _, err := r.store.SchedulePollRetry(r.controllerID, cursor, r.now().Add(delay), nextRetry); err != nil {
		log.Printf("workflow controller %s: schedule poll retry %s: %v", r.controllerID, PollCursorKey(cursor), err)
	}
}

// nextPollWait shortens wait to the earliest scheduled next poll.
func (r *Runner) nextPollWait(wait time.Duration) time.Duration {
	if r.poll == nil {
		return wait
	}
	earliest, ok, err := r.store.NextPollTime(r.controllerID)
	if err != nil || !ok {
		return wait
	}
	d := earliest.Sub(r.now())
	if d < 0 {
		d = 0
	}
	if d < wait {
		return d
	}
	return wait
}
