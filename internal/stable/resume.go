package stable

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runstate"
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
	if runID == "supervisor" {
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
	sessionID, err := s.resumeSessionID(prior, runID)
	if err != nil {
		return messaging.ResumeResult{}, err
	}

	prior.mu.Lock()
	label := prior.label
	agent, model, backend := prior.effectiveAgent, prior.effectiveModel, prior.effectiveBackend
	if agent == "" {
		agent = prior.agent
	}
	if model == "" {
		model = prior.model
	}
	if backend == "" {
		backend = prior.backend
	}
	thinking := prior.thinking
	agentProfile := prior.agentProfile
	dir := prior.dir
	autoApprove := prior.autoApprove
	rosterFile := prior.rosterFile
	rosterEntry := prior.rosterEntry
	prior.mu.Unlock()

	// The prior label is the current display name, so a rename is honoured
	// here rather than the stale launch label.
	if s.broker != nil {
		if current := s.broker.DisplayName(runID); current != "" {
			label = current
		}
	}
	newLabel := req.Label
	if newLabel == "" {
		// label falls back to the prior runtime id when the broker has no
		// display name, so this is never just the suffix.
		newLabel = label + runstate.FollowUpLabelSuffix
	}

	idempotencyKey := ""
	if req.IdempotencyKey != "" {
		idempotencyKey = runstate.FollowUpIdempotencyKeyPrefix + req.IdempotencyKey
	} else if holder := s.liveLabelHolder(newLabel); holder != "" {
		// Only an unkeyed spawn reaches a taken label: a keyed resume is gated
		// by idempotentSpawn, which rejects the label itself. This pre-check
		// only fast-fails the plain case with the same text.
		return messaging.ResumeResult{}, &router.ToolError{
			Code:    "refused",
			Message: fmt.Sprintf("label already in use: %s (runtime %s)", newLabel, holder),
		}
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
		SessionID:      sessionID,
		IdempotencyKey: idempotencyKey,
		// The surface mints the public run id; carrying it through the spawn
		// result is what lets a keyed replay report the first run's id instead
		// of the retry's.
		resumeRunID: req.RunID,
	}
	// finishSpawn is the control-socket spawn gate: it hashes and gates a keyed
	// spawn and replays a stored result, so a keyed resume cannot diverge from a
	// keyed spawn.
	res, err := s.finishSpawn(params)
	if err != nil {
		return messaging.ResumeResult{}, &router.ToolError{Code: "refused", Message: err.Error()}
	}
	spawned, ok := res.(SpawnResult)
	if !ok {
		return messaging.ResumeResult{}, &router.ToolError{
			Code:    "refused",
			Message: fmt.Sprintf("spawn returned %T, not a spawn result", res),
		}
	}

	// The spawn defaults the artifact paths when the request leaves them empty,
	// so the result reports where the run actually writes on both paths.
	result := messaging.ResumeResult{
		RunID:        req.RunID,
		Label:        newLabel,
		RuntimeID:    spawned.RuntimeID,
		SessionID:    spawned.SessionID,
		SentinelFile: spawned.SentinelFile,
		OnEvent:      spawned.OnEvent,
	}
	if result.SentinelFile == "" {
		result.SentinelFile = req.SentinelFile
	}
	if result.OnEvent == "" {
		result.OnEvent = req.EventLog
	}
	// A keyed call replays the first run, so the first run's id wins over the
	// freshly minted one.
	if spawned.RunID != "" {
		result.RunID = spawned.RunID
	}
	return result, nil
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

// liveLabelHolder reports the runtime a live run holds newLabel under, or "".
func (s *Supervisor) liveLabelHolder(label string) string {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	return s.labelHolderLocked(label)
}
