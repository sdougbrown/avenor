package mcpserver

// Temporary until 356-08: this file is the test-only shim for the retired
// avenor_follow_up handler and the messaging verbs the follow-up idempotency
// fake stubs, whose final version supplies them itself. Slice 08 deletes it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runstate"
)

func (f *fakeIdempotentSupervisor) MessageSendContext(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	return messaging.SendResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageWaitContext(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	return messaging.WaitResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageReplyContext(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	return messaging.SendResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageWithdrawContext(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	return messaging.WithdrawResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageReceiveContext(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return messaging.ReceiveResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessagePeersContext(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	return messaging.PeersResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessagePreflightContext(ctx context.Context) (messaging.PreflightResult, error) {
	return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-test"}, nil
}
func (f *fakeIdempotentSupervisor) ResumeContext(_ context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	return messaging.ResumeResult{}, nil
}
func (f *fakeIdempotentSupervisor) SessionRenameContext(_ context.Context, sessionID, name string) (messaging.RenameResult, error) {
	return messaging.RenameResult{}, nil
}
func (f *fakeIdempotentSupervisor) SessionLastContext(_ context.Context, runtimeID string) (messaging.PeerLast, error) {
	return messaging.PeerLast{}, nil
}

type followUpArgs struct {
	RunID          string `json:"run_id" jsonschema:"required prior run ID or label"`
	Message        string `json:"message" jsonschema:"required follow-up message"`
	Label          string `json:"label,omitempty" jsonschema:"optional label for the new run (defaults to <prior-label>-followup)"`
	SupervisorID   string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
	IdempotencyKey string `json:"idempotency_key,omitempty" jsonschema:"optional idempotency key; a retry with the same key and parameters returns the original follow-up run instead of spawning a second one"`
}

func (s *Server) handleAvenorFollowUp(ctx context.Context, req *mcp.CallToolRequest, args followUpArgs) (*mcp.CallToolResult, any, error) {
	supervisorID := s.resultSupervisorID(args.RunID, args.SupervisorID)

	// Resolve the supervisor's control client once and store it in cl.
	// The status lookup and follow-up Spawn call both use cl.
	// They therefore reuse one control-client connection.
	cl, cleanup, err := s.getClientForSupervisor(supervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	ri, err := s.lookupRun(cl, s.getSupervisorPath(supervisorID), args.RunID)
	if err != nil {
		return nil, nil, err
	}
	if ri == nil {
		return nil, nil, fmt.Errorf("run not found in registry")
	}

	sessionID, err := runstate.ReadSentinelSession(ri.SentinelPath)
	if err != nil {
		// Return errors for failed or non-resumable sentinel files unchanged. A
		// missing file may mean the run is not terminal. Query supervisor status for
		// the adopted session ID. If status has no usable ID, re-read the sentinel
		// before using the spawn-time registry value.
		if !errors.Is(err, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("read sentinel session: %w", err)
		}
		if ri.RuntimeID != "" {
			if status, statusErr := cl.Status(ri.RuntimeID); statusErr == nil {
				if currentID, _ := status["session_id"].(string); currentID != "" {
					sessionID = currentID
				}
			}
		}
		if sessionID == "" {
			// If status yields no session ID, retry the sentinel because the
			// supervisor may have written it since the initial read.
			retryID, retryErr := runstate.ReadSentinelSession(ri.SentinelPath)
			if retryErr == nil {
				sessionID = retryID
			} else if !errors.Is(retryErr, os.ErrNotExist) {
				return nil, nil, fmt.Errorf("read sentinel session: %w", retryErr)
			}
		}
		if sessionID == "" {
			if ri.SessionID == "" {
				return nil, nil, fmt.Errorf("read sentinel session: %w", err)
			}
			sessionID = ri.SessionID
		}
	}

	identity := resolvedSpawnIdentity{
		Agent: ri.Agent, Model: ri.Model, Backend: ri.Backend,
		RosterFile: ri.RosterFile, RosterEntry: ri.RosterEntry,
		EffectiveAgent: ri.EffectiveAgent, EffectiveModel: ri.EffectiveModel,
		EffectiveBackend: ri.EffectiveBackend, AgentProfile: ri.AgentProfile,
	}
	// A completed workflow's authoritative identity belongs to its final phase,
	// not its run-level selector. Refresh from the stable tombstone before
	// follow-up and never reread the roster file.
	var liveIdentityStatus map[string]any
	if ri.RuntimeID != "" {
		if status, statusErr := cl.Status(ri.RuntimeID); statusErr == nil {
			liveIdentityStatus = status
			applySpawnIdentity(status, &identity)
			if currentID, _ := status["session_id"].(string); currentID != "" {
				sessionID = currentID
			}
		}
	}

	runID := uuid.New().String()
	priorLabel := ri.Label
	// The default is the current display name, so a rename reaches the
	// follow-up instead of the launch-time label.
	if liveIdentityStatus != nil {
		if current, ok := liveIdentityStatus["label"].(string); ok && current != "" {
			priorLabel = current
		}
	}
	followupLabel := args.Label
	if followupLabel == "" {
		followupLabel = priorLabel + runstate.FollowUpLabelSuffix
	}

	sentinelPath, eventLogPath := runArtifactPaths(runID)

	effectiveAgent := identity.EffectiveAgent
	if effectiveAgent == "" {
		effectiveAgent = identity.Agent
	}
	effectiveModel := identity.EffectiveModel
	if effectiveModel == "" {
		effectiveModel = identity.Model
	}
	effectiveBackend := identity.EffectiveBackend
	if effectiveBackend == "" {
		effectiveBackend = identity.Backend
	}
	agentProfile := identity.AgentProfile
	// Presence is authoritative even for an empty string. This clears stale
	// run-level fields when the final workflow phase is agent-only/model-only.
	if liveIdentityStatus != nil {
		exact := func(effectiveKey, directKey string) (string, bool) {
			if value, ok := liveIdentityStatus[effectiveKey].(string); ok {
				return value, true
			}
			value, ok := liveIdentityStatus[directKey].(string)
			return value, ok
		}
		if value, ok := exact("effective_agent", "agent"); ok {
			effectiveAgent = value
		}
		if value, ok := exact("effective_model", "model"); ok {
			effectiveModel = value
		}
		if value, ok := exact("effective_backend", "backend"); ok {
			effectiveBackend = value
		}
		if value, ok := liveIdentityStatus["agent_profile"].(string); ok {
			agentProfile = value
		}
	}
	params := map[string]any{
		"dir":           ri.Dir,
		"prompt":        args.Message,
		"label":         followupLabel,
		"session_id":    sessionID,
		"sentinel_file": sentinelPath,
		"on_event":      eventLogPath,
	}
	if args.IdempotencyKey != "" {
		params["idempotency_key"] = runstate.FollowUpIdempotencyKeyPrefix + args.IdempotencyKey
	}
	if effectiveAgent != "" {
		params["agent"] = effectiveAgent
	}
	if effectiveModel != "" {
		params["model"] = effectiveModel
	}
	if effectiveBackend != "" {
		params["backend"] = effectiveBackend
	}
	if ri.Thinking != "" {
		params["thinking"] = ri.Thinking
	}
	if agentProfile != "" {
		params["agent_profile"] = agentProfile
	}
	if ri.AutoApprove {
		params["auto_approve"] = true
	}

	if followupLabel != "" && args.IdempotencyKey == "" {
		// Same keyed-spawn rule as spawn: a retry reaches the idempotency gate.
		if err := s.checkLabelAvailable(cl, s.getSupervisorPath(supervisorID), followupLabel); err != nil {
			return nil, nil, err
		}
	}

	result, err := cl.Spawn(params)
	if err != nil {
		return nil, nil, fmt.Errorf("spawn: %w", err)
	}

	if args.IdempotencyKey != "" {
		if sent, ok := result["sentinel_file"].(string); ok && sent != "" {
			sentinelPath = sent
			if id := runstate.RunIDFromSentinel(sent); id != "" {
				runID = id
			}
		}
		if ev, ok := result["on_event"].(string); ok && ev != "" {
			eventLogPath = ev
		}
	}

	runtimeID, _ := result["runtime_id"].(string)
	newSessionID, _ := result["session_id"].(string)

	supervisorPath := s.getSupervisorPath(supervisorID)

	if err := s.registry.Store(&RunInfo{
		RunID:            runID,
		Label:            followupLabel,
		RuntimeID:        runtimeID,
		SessionID:        newSessionID,
		SupervisorID:     supervisorPath,
		SentinelPath:     sentinelPath,
		EventLogPath:     eventLogPath,
		Agent:            effectiveAgent,
		Model:            effectiveModel,
		Backend:          effectiveBackend,
		RosterFile:       identity.RosterFile,
		RosterEntry:      identity.RosterEntry,
		EffectiveAgent:   effectiveAgent,
		EffectiveModel:   effectiveModel,
		EffectiveBackend: effectiveBackend,
		AgentProfile:     agentProfile,
		Thinking:         ri.Thinking,
		Dir:              ri.Dir,
		AutoApprove:      ri.AutoApprove,
		CreatedAt:        time.Now(),
	}); err != nil {
		return nil, nil, fmt.Errorf("registry store: %w", err)
	}

	return nil, map[string]any{
		"run_id": runID,
		"label":  followupLabel,
	}, nil
}
