package stable

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runstate"
	"github.com/sdougbrown/avenor/internal/runtime/factory"
)

// Resume continues a prior run in a new one. The target is a resolved broker
// runtime id; the public run id and artifact paths are minted by the calling
// surface and never re-derived here.
func (s *Supervisor) Resume(caller messaging.Caller, req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	if caller.Kind != messaging.CallerOwner {
		return messaging.ResumeResult{}, &router.ToolError{
			Code:    "not_permitted",
			Message: "only the control owner may resume a run",
		}
	}
	if req.Target == "" {
		return messaging.ResumeResult{}, unknownTargetError(req.Target)
	}
	runID, err := s.resolveRunReference(req.Target)
	if err != nil {
		return messaging.ResumeResult{}, err
	}
	// The supervisor session is not a child runtime this supervisor resumed; it
	// has no prior session to continue.
	if runID == messaging.SupervisorRunID {
		return messaging.ResumeResult{}, &router.ToolError{
			Code:    "refused",
			Message: "the supervisor session is not a resume target; resume continues a child runtime",
		}
	}
	prior := s.runtimeByID(runID)
	if prior == nil {
		return messaging.ResumeResult{}, &router.ToolError{
			Code:    "refused",
			Message: fmt.Sprintf("runtime %q is not owned by this supervisor", runID),
		}
	}
	if req.IdempotencyKey != "" {
		key := runstate.FollowUpIdempotencyKeyPrefix + req.IdempotencyKey
		hash, err := ResumeIdempotencyHash(runID, req)
		if err != nil {
			return messaging.ResumeResult{}, err
		}
		result, err := s.runIdempotentOperation(key, hash, func() (SpawnResult, error) {
			return s.performResume(prior, runID, req)
		})
		if err != nil {
			var toolErr *router.ToolError
			if errors.As(err, &toolErr) {
				return messaging.ResumeResult{}, err
			}
			return messaging.ResumeResult{}, &router.ToolError{Code: "refused", Message: err.Error()}
		}
		return resumeResult(result), nil
	}
	result, err := s.performResume(prior, runID, req)
	if err != nil {
		return messaging.ResumeResult{}, err
	}
	return resumeResult(result), nil
}

func (s *Supervisor) performResume(prior *childRuntime, runID string, req messaging.ResumeRequest) (SpawnResult, error) {
	if req.Message == "" {
		return SpawnResult{}, &router.ToolError{Code: "refused", Message: "resume message is required"}
	}
	prior.mu.Lock()
	label := prior.label
	agent, model := prior.effectiveAgent, prior.effectiveModel
	if agent == "" {
		agent = prior.agent
	}
	if model == "" {
		model = prior.model
	}
	backend := prior.resumeBackendLocked()
	thinking := prior.thinking
	agentProfile := prior.agentProfile
	dir := prior.dir
	autoApprove := prior.autoApprove
	rosterFile := prior.rosterFile
	rosterEntry := prior.rosterEntry
	prior.mu.Unlock()

	// A roster-selected run resumes through its selector alone: the entry
	// supplies agent, model and backend. A spawn carrying both is refused as
	// "direct identity fields are disabled in roster mode". The session mapping
	// still pins the resolved identity, so a roster that changed under the run
	// conflicts instead of launching a new identity.
	if rosterEntry != "" {
		agent, model, backend = "", "", ""
	}

	// The prior label is the current display name, so a rename is honoured
	// here rather than the stale launch label.
	if s.broker != nil {
		if current := s.broker.DisplayName(runID); current != "" {
			label = current
		}
	}
	newLabel := req.Label
	if newLabel == "" {
		newLabel = label + runstate.FollowUpLabelSuffix
	}

	idempotencyKey := ""
	if req.IdempotencyKey != "" {
		idempotencyKey = runstate.FollowUpIdempotencyKeyPrefix + req.IdempotencyKey
	}

	params := SpawnParams{
		Prompt:         req.Message,
		Dir:            dir,
		Agent:          agent,
		AgentProfile:   agentProfile,
		Label:          newLabel,
		Model:          model,
		Thinking:       thinking,
		Backend:        backend,
		OnEvent:        req.EventLog,
		SentinelFile:   req.SentinelFile,
		AutoApprove:    autoApprove,
		RosterFile:     rosterFile,
		RosterEntry:    rosterEntry,
		IdempotencyKey: idempotencyKey,
		// The surface mints the public run id; carrying it through the spawn
		// result is what lets a keyed replay report the first run's id instead
		// of the retry's.
		resume: resumeSpawn{
			runID:            req.RunID,
			labelReservation: true,
			implicitLabel:    req.Label == "",
		},
	}
	spawned, err := s.resumeOnce(prior, runID, params)
	if err != nil {
		var toolErr *router.ToolError
		if errors.As(err, &toolErr) {
			return SpawnResult{}, err
		}
		return SpawnResult{}, &router.ToolError{Code: "refused", Message: err.Error()}
	}
	if spawned.RunID == "" {
		spawned.RunID = req.RunID
	}
	if spawned.Label == "" {
		spawned.Label = newLabel
	}
	if spawned.SentinelFile == "" {
		spawned.SentinelFile = req.SentinelFile
	}
	if spawned.OnEvent == "" {
		spawned.OnEvent = req.EventLog
	}
	return spawned, nil
}

func resumeResult(result SpawnResult) messaging.ResumeResult {
	return messaging.ResumeResult{RunID: result.RunID, Label: result.Label, RuntimeID: result.RuntimeID, SessionID: result.SessionID, SentinelFile: result.SentinelFile, OnEvent: result.OnEvent}
}

// resumeBackendLocked returns the backend the prior run launched with: the
// resolved value when present, otherwise the requested one. The caller holds
// c.mu.
func (c *childRuntime) resumeBackendLocked() string {
	if c.effectiveBackend != "" {
		return c.effectiveBackend
	}
	return c.backend
}

// resumeOnce admits a parked turn or starts a cold continuation. The session
// ladder runs only on the cold arm, just before the spawn that uses it: a warm
// admission continues child.session, and pi without persisted sessions has no
// cold arm, so no other resume pays for the ladder's sentinel reads.
func (s *Supervisor) resumeOnce(prior *childRuntime, runID string, p SpawnParams) (result SpawnResult, err error) {
	warmParams := p
	if p.resume.implicitLabel {
		if harness := s.brokerHarnessName(runID); harness != "" {
			warmParams.Label = harness
		}
	}
	result, warm, err := s.admitWarmResume(prior, warmParams)
	if err != nil {
		return result, err
	}
	if warm {
		return result, nil
	}
	// The gate asks whether the backend continuing this session resumes across
	// processes. A roster-selected resume leaves Backend empty for the spawn
	// validator, because the entry supplies it. The session mapping pins the
	// entry to the identity the prior run launched with, so the gate reads that
	// identity instead.
	gateBackend := p.Backend
	if gateBackend == "" {
		prior.mu.Lock()
		gateBackend = prior.resumeBackendLocked()
		prior.mu.Unlock()
	}
	if !factory.ResumesAcrossProcesses(gateBackend) {
		return result, notResumableError(runID, s.piResumeDeniedStatus(prior))
	}
	// Completion admission closes before provider teardown. Do not open the
	// same session in another provider until the old run has fully exited.
	select {
	case <-prior.runDone:
	default:
		return result, fmt.Errorf("runtime %q teardown is still in progress; retry resume", runID)
	}
	sessionID, err := s.resumeSessionID(prior, runID)
	if err != nil {
		return result, err
	}
	p.SessionID = sessionID
	result, err = s.spawn(p)
	if err != nil {
		return result, err
	}
	return result, nil
}

// piResumeDeniedStatus names the not_resumable status for a pi runtime the
// warm path cannot admit: expiry when the run's session has ended, shutdown
// when the runtime (or its supervisor) is being torn down instead.
func (s *Supervisor) piResumeDeniedStatus(prior *childRuntime) string {
	s.controlMu.Lock()
	shuttingDown := s.shuttingDown
	s.controlMu.Unlock()
	prior.mu.Lock()
	tornDown := prior.lc.isShuttingDown() || prior.lc.isWriterClosing()
	prior.mu.Unlock()
	if shuttingDown || tornDown {
		return "shutdown"
	}
	return "expired"
}

// resumeSessionID reads the session ladder: the sentinel's SESSION, then the
// runtime's live session id, then a sentinel re-read. A non-resumable sentinel
// status is one denial; a ladder that reaches the end with nothing to continue
// is the other, because an empty session id would make the spawn open a fresh
// session behind a run the caller was told was resumed. Both render the shared
// not_resumable repair template.
func (s *Supervisor) resumeSessionID(prior *childRuntime, runID string) (string, error) {
	prior.mu.Lock()
	sentinelPath := prior.sentinelFile
	prior.mu.Unlock()

	status := ""
	sessionID, err := runstate.ReadSentinelSession(sentinelPath)
	switch {
	case err == nil:
		return sessionID, nil
	case errors.Is(err, os.ErrNotExist):
		// No sentinel yet: the run may still be live.
	default:
		sentinel, readErr := runstate.ReadSentinel(sentinelPath)
		if readErr != nil {
			return "", err
		}
		if !runstate.IsResumableStatus(sentinel.Status) {
			return "", notResumableError(runID, sentinel.Status)
		}
		status = sentinel.Status
		// A resumable sentinel with no SESSION: fall through to the live value.
	}

	if live := prior.sessionID(); live != "" {
		return live, nil
	}
	if sessionID, err := runstate.ReadSentinelSession(sentinelPath); err == nil {
		return sessionID, nil
	}
	if status == "" {
		status = string(runstate.StatusUnknown)
	}
	return "", notResumableError(runID, status)
}

// brokerHarnessName reports the harness-supplied name the broker run carries,
// or "" when there is no broker, the run is unknown, or it has none.
func (s *Supervisor) brokerHarnessName(runID string) string {
	if s.broker == nil {
		return ""
	}
	st := s.broker.GetRun(runID)
	if st == nil {
		return ""
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	return st.HarnessName
}

// notResumableError renders the one denial the session ladder and the sentinel
// status produce.
func notResumableError(runID, status string) *router.ToolError {
	facts := map[string]string{
		"run_id": messaging.Quote(runID),
		"status": messaging.Sanitize(strings.ToLower(status)),
	}
	return &router.ToolError{
		Code:    "not_resumable",
		Message: messaging.Repair("not_resumable", facts),
		Facts:   facts,
	}
}

// runtimeByID returns a registered runtime without holding controlMu across the
// caller's work.
func (s *Supervisor) runtimeByID(rtID string) *childRuntime {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	return s.runtimes[rtID]
}
