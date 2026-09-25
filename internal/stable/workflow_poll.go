package stable

// workflow_poll.go implements the stable host surface for external-gate
// adapter polling: kernel-local parking of auto external candidates under
// the leader lease, direct adapter invocation for poll cursors, and
// leader-side landing of adapter results (activation revalidation, evidence
// staging, and the structured external_result gate command). It also owns
// the adapter-poll staging area under the workflow root and the startup
// sweep for files orphaned by a crash between staging and the command.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// adapterStagingDir returns the workflow root's adapter-poll staging
// directory. It is created lazily by the first staged poll.
func adapterStagingDir(root string) string {
	return filepath.Join(root, "adapter-poll")
}

// sweepOrphanedAdapterFiles removes leftover staged adapter temp files from a
// crash between evidence staging and the gate command. The evidence copies
// themselves are immutable and stay; only the staging files are removed. A
// missing directory is not an error.
func sweepOrphanedAdapterFiles(root string) {
	dir := adapterStagingDir(root)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			log.Printf("workflow: adapter sweep: remove %s: %v", entry.Name(), err)
			continue
		}
		removed++
	}
	if removed > 0 {
		log.Printf("workflow: startup sweep removed %d orphaned adapter staging file(s)", removed)
	}
}

// parkExternalNode parks one selected auto external candidate under the
// runner's leader lease. Stale candidates (manual claim, revision change)
// and unresolved bindings map onto their own dispatch result kinds; a
// successful park returns the gate seeds the runner turns into poll
// cursors. No admission is consumed and no attempt is recorded.
func (s *Supervisor) parkExternalNode(dec workflowcontroller.Decision, lease workflowcontroller.LeaderLease) (workflowcontroller.DispatchResult, error) {
	identity := dec.Candidate.Identity
	out := workflowcontroller.DispatchResult{}
	mgr, store, err := s.workflowBarrierResult()
	if err != nil {
		return out, err
	}
	parkErr := store.WithLeader(dec.Candidate.ControllerID, lease.LeaseID, lease.OwnerEpoch, func() error {
		park, err := mgr.ParkExternal(workflow.ParkExternalRequest{
			WorkflowID:       identity.WorkflowID,
			NodeID:           identity.NodeID,
			ActivationID:     identity.ActivationID,
			ExpectedRevision: dec.Candidate.Revision,
			ControllerID:     dec.Candidate.ControllerID,
			LeaderLeaseID:    lease.LeaseID,
		})
		if err != nil {
			return err
		}
		seeds := make([]workflowcontroller.PollSeed, 0, len(park.Gates))
		for _, g := range park.Gates {
			seeds = append(seeds, workflowcontroller.PollSeed{
				WorkflowID:   string(identity.WorkflowID),
				NodeID:       string(identity.NodeID),
				ActivationID: string(identity.ActivationID),
				GateID:       string(g.GateID),
				AdapterID:    g.AdapterID,
				SubjectHash:  g.SubjectHash,
			})
		}
		out.Kind = workflowcontroller.ResultParked
		out.PollSeeds = seeds
		return nil
	})
	if parkErr != nil {
		if errors.Is(parkErr, workflowcontroller.ErrNotLeader) {
			return workflowcontroller.DispatchResult{Kind: workflowcontroller.ResultNotLeader}, nil
		}
		if errors.Is(parkErr, workflow.ErrStaleCandidate) {
			out.Kind = workflowcontroller.ResultStale
			return out, nil
		}
		if errors.Is(parkErr, workflow.ErrUnresolvedBinding) {
			out.Kind = workflowcontroller.ResultUnresolvedBinding
			out.Detail = parkErr.Error()
			return out, nil
		}
		return out, parkErr
	}
	return out, nil
}

// pollExternalGate runs one adapter invocation for a poll cursor. It reads
// the pinned gate state (inputs and subject), verifies the cursor's subject
// hash still matches, and invokes the registered adapter executable — I/O
// only; it never writes gate state.
func (s *Supervisor) pollExternalGate(ctx context.Context, cursor workflowcontroller.PollCursor) (workflowcontroller.AdapterResult, workflowcontroller.PollFailureKind, error) {
	registry := s.workflowAdapters.Load()
	manifest, ok := registry.Lookup(cursor.AdapterID)
	if !ok || manifest == nil {
		return workflowcontroller.AdapterResult{}, workflowcontroller.PollFailureUnavailable,
			fmt.Errorf("adapter %q is not registered", cursor.AdapterID)
	}
	mgr, _, err := s.workflowBarrierResult()
	if err != nil {
		return workflowcontroller.AdapterResult{}, workflowcontroller.PollFailureTransient, err
	}
	// A cursor whose activation resolved or whose pinned subject moved on is
	// obsolete: no adapter invocation is spent on it.
	subject, parked, err := mgr.ParkedGateSubject(workflow.WorkflowID(cursor.WorkflowID), workflow.NodeID(cursor.NodeID),
		workflow.ActivationID(cursor.ActivationID), workflow.GateID(cursor.GateID))
	if err != nil {
		return workflowcontroller.AdapterResult{}, workflowcontroller.PollFailureTransient, err
	}
	if !parked || workflow.SubjectHash(subject) != cursor.SubjectHash {
		return workflowcontroller.AdapterResult{}, workflowcontroller.PollFailureObsolete, nil
	}
	state, err := mgr.ExternalGateState(workflow.WorkflowID(cursor.WorkflowID), workflow.NodeID(cursor.NodeID),
		workflow.ActivationID(cursor.ActivationID), workflow.GateID(cursor.GateID))
	if err != nil {
		if errors.Is(err, workflow.ErrUnresolvedBinding) {
			return workflowcontroller.AdapterResult{}, workflowcontroller.PollFailureObsolete, nil
		}
		return workflowcontroller.AdapterResult{}, workflowcontroller.PollFailureTransient, err
	}
	if workflow.SubjectHash(state.Subject) != cursor.SubjectHash {
		// The activation was superseded by a new head; the cursor is stale.
		return workflowcontroller.AdapterResult{}, workflowcontroller.PollFailureObsolete, nil
	}
	input, err := json.Marshal(state.Inputs)
	if err != nil {
		return workflowcontroller.AdapterResult{}, workflowcontroller.PollFailureTransient, err
	}
	res, err := workflowcontroller.Invoke(ctx, manifest, workflowcontroller.PollRequest{
		Version:      1,
		PollID:       cursor.PollID,
		WorkflowID:   cursor.WorkflowID,
		NodeID:       cursor.NodeID,
		ActivationID: cursor.ActivationID,
		GateID:       cursor.GateID,
		Input:        input,
	})
	if err != nil {
		if errors.Is(err, workflowcontroller.ErrAdapterChanged) {
			return workflowcontroller.AdapterResult{}, workflowcontroller.PollFailureUnavailable, err
		}
		return workflowcontroller.AdapterResult{}, workflowcontroller.PollFailureTransient, err
	}
	return res, workflowcontroller.PollFailureNone, nil
}

// applyPollResult lands a completed adapter result on the leader goroutine:
// under the controller-store lock it revalidates the leader lease and the
// parked activation's pinned subject, stages the bounded raw stdout as
// evidence, and submits the structured external_result gate command. A
// failed gate command discards the freshly staged evidence. The outcome
// tells the runner whether the result was applied, stale (leadership lost —
// the runner drops leadership and leaves the cursor for the next leader), or
// obsolete (drop the cursor).
func (s *Supervisor) applyPollResult(controllerID string, cursor workflowcontroller.PollCursor, res *workflowcontroller.AdapterResult, lease workflowcontroller.LeaderLease) (workflowcontroller.PollApplyOutcome, error) {
	mgr, store, err := s.workflowBarrierResult()
	if err != nil {
		return workflowcontroller.PollStale, err
	}
	wstore := mgr.Store()
	wf := workflow.WorkflowID(cursor.WorkflowID)
	outcome := workflowcontroller.PollStale
	applyErr := store.WithLeader(controllerID, lease.LeaseID, lease.OwnerEpoch, func() error {
		subject, parked, err := mgr.ParkedGateSubject(wf, workflow.NodeID(cursor.NodeID),
			workflow.ActivationID(cursor.ActivationID), workflow.GateID(cursor.GateID))
		if err != nil {
			return err
		}
		if !parked || workflow.SubjectHash(subject) != cursor.SubjectHash {
			outcome = workflowcontroller.PollObsolete
			return nil
		}
		// Stage the exact bounded stdout as evidence under the workflow root.
		root := wstore.Root()
		if err := os.MkdirAll(adapterStagingDir(root), 0o755); err != nil {
			return err
		}
		tmp, err := os.CreateTemp(adapterStagingDir(root), "poll-*.json")
		if err != nil {
			return err
		}
		tmpName := tmp.Name()
		defer os.Remove(tmpName)
		if _, err := tmp.Write(res.RawStdout); err != nil {
			tmp.Close()
			return err
		}
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			return err
		}
		if err := tmp.Close(); err != nil {
			return err
		}
		staged, err := wstore.StageEvidence(wf, tmpName, "adapter-response.json", true, "")
		if err != nil {
			return err
		}
		// A gate command failure must not leave freshly staged evidence.
		commandErr := s.submitExternalResult(wf, cursor, res, subject, staged.EvidenceID)
		if commandErr != nil {
			if discErr := wstore.DiscardEvidence(wf, staged.EvidenceID); discErr != nil {
				log.Printf("workflow %s: discard evidence %s after failed gate command: %v", wf, staged.EvidenceID, discErr)
			}
			return commandErr
		}
		outcome = workflowcontroller.PollApplied
		return nil
	})
	if applyErr != nil {
		if errors.Is(applyErr, workflowcontroller.ErrNotLeader) {
			// Lost leadership mid-apply: PollStale with no error is reserved
			// for this case, so the runner drops leadership instead of
			// re-offering the in-flight cursor under a dead lease.
			return workflowcontroller.PollStale, nil
		}
		return workflowcontroller.PollStale, applyErr
	}
	return outcome, nil
}

// submitExternalResult submits the structured external_result gate command
// through the manager's command boundary.
func (s *Supervisor) submitExternalResult(wf workflow.WorkflowID, cursor workflowcontroller.PollCursor, res *workflowcontroller.AdapterResult, subject *workflow.Subject, evidenceID workflow.EvidenceID) error {
	payload, err := json.Marshal(map[string]any{
		"op":            "gate",
		"node_id":       cursor.NodeID,
		"activation_id": cursor.ActivationID,
		"gate_id":       cursor.GateID,
		"operation":     "external_result",
		"result":        res.Result,
		"poll_id":       cursor.PollID,
		"source":        "adapter:" + cursor.AdapterID,
		"subject":       subject,
		"response_hash": res.ResponseHash,
		"observed_at":   res.ObservedAt.UTC(),
		"evidence_ids":  []string{string(evidenceID)},
	})
	if err != nil {
		return err
	}
	_, err = s.workflowManager().WorkflowCommand(string(wf), payload)
	return err
}

// stablePoller adapts the supervisor's park/poll/apply surface to the
// runner's Poller interface for one controller.
type stablePoller struct {
	s            *Supervisor
	controllerID string
}

// Poll runs one adapter invocation (I/O only) on a poll worker goroutine.
func (p *stablePoller) Poll(ctx context.Context, cursor workflowcontroller.PollCursor) (workflowcontroller.AdapterResult, workflowcontroller.PollFailureKind, error) {
	return p.s.pollExternalGate(ctx, cursor)
}

// ApplyResult lands a completed adapter result on the leader goroutine.
func (p *stablePoller) ApplyResult(cursor workflowcontroller.PollCursor, res *workflowcontroller.AdapterResult, lease workflowcontroller.LeaderLease) (workflowcontroller.PollApplyOutcome, error) {
	return p.s.applyPollResult(p.controllerID, cursor, res, lease)
}
