package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runstate"
)

// mcpMessageProvider is the router Provider for the Go host surface. It carries
// no broker token and implements no rename on the transport: resume and rename
// are control verbs, and a reply has no inbound delivery to claim here.
type mcpMessageProvider struct {
	server         *Server
	client         messageControlClient
	supervisorPath string
	generationMu   sync.RWMutex
	generation     string
	transport      *controlTransport
}

// messageControlClient is the control surface the messaging provider speaks.
// Temporary until 356-06, when ControlClient carries these verbs and this
// interface is deleted with it.
type messageControlClient interface {
	ControlClient
	MessageSendContext(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error)
	MessageWaitContext(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error)
	MessageReplyContext(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error)
	MessageWithdrawContext(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error)
	MessageReceiveContext(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error)
	MessagePeersContext(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error)
	MessagePreflightContext(ctx context.Context) (messaging.PreflightResult, error)
	ResumeContext(ctx context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error)
	SessionRenameContext(ctx context.Context, sessionID, name string) (messaging.RenameResult, error)
	SessionLastContext(ctx context.Context, runtimeID string) (messaging.PeerLast, error)
}

// newMCPMessageProvider preflights the control store under ctx, so a caller can
// bound or cancel the constructor's one blocking RPC.
func newMCPMessageProvider(ctx context.Context, server *Server, client messageControlClient, supervisorPath string) (*mcpMessageProvider, error) {
	pre, err := client.MessagePreflightContext(ctx)
	if err != nil {
		return nil, err
	}
	p := &mcpMessageProvider{
		server:         server,
		client:         client,
		supervisorPath: supervisorPath,
		transport:      newControlTransport(client),
	}
	p.setGeneration(pre.Generation)
	return p, nil
}

func (p *mcpMessageProvider) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	res, err := versionedRetry(ctx, p, &req.Generation, func(ctx context.Context) (messaging.SendResult, error) {
		return p.transport.Send(ctx, req)
	})
	p.setGeneration(res.Generation)
	return res, err
}

func (p *mcpMessageProvider) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	return versionedRetry(ctx, p, &req.Generation, func(ctx context.Context) (messaging.WaitResult, error) {
		return p.transport.Wait(ctx, req)
	})
}

func (p *mcpMessageProvider) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	return retryGenerationMismatch(ctx, p, func(ctx context.Context) (messaging.SendResult, error) {
		return p.transport.Reply(ctx, req)
	})
}

func (p *mcpMessageProvider) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	res, err := versionedRetry(ctx, p, &req.Generation, func(ctx context.Context) (messaging.WithdrawResult, error) {
		return p.transport.Withdraw(ctx, req)
	})
	p.setGeneration(res.Fence.Generation)
	return res, err
}

func (p *mcpMessageProvider) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	res, err := versionedRetry(ctx, p, &req.Generation, func(ctx context.Context) (messaging.ReceiveResult, error) {
		return p.transport.Receive(ctx, req)
	})
	p.setGeneration(res.Generation)
	return res, err
}

func (p *mcpMessageProvider) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	res, err := versionedRetry(ctx, p, &req.Generation, func(ctx context.Context) (messaging.PeersResult, error) {
		return p.transport.Peers(ctx, req)
	})
	p.setGeneration(res.Generation)
	return res, err
}

func (p *mcpMessageProvider) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
	res, err := p.transport.Preflight(ctx)
	p.setGeneration(res.Generation)
	return res, err
}

func (p *mcpMessageProvider) Close() error { return nil }

// Generation reports the control store generation the provider is currently
// bound to. The store mints a new generation on every restart, so the value is
// refreshed from every transport result that reports one.
func (p *mcpMessageProvider) Generation() string {
	p.generationMu.RLock()
	defer p.generationMu.RUnlock()
	return p.generation
}

func (p *mcpMessageProvider) setGeneration(generation string) {
	if generation == "" {
		return
	}
	p.generationMu.Lock()
	p.generation = generation
	p.generationMu.Unlock()
}

// refreshGeneration re-preflights the control store and caches the generation
// it reports. A failed preflight leaves the cached generation untouched.
func (p *mcpMessageProvider) refreshGeneration(ctx context.Context) (string, error) {
	pre, err := p.transport.Preflight(ctx)
	if err != nil {
		return "", err
	}
	p.setGeneration(pre.Generation)
	return pre.Generation, nil
}

// versionedRetry binds one generation-carrying request to the provider's
// current generation and retries it once when the store reports that pin
// stale. The retry re-preflights first, so the request carries the generation
// the restarted store just minted instead of the dead one.
func versionedRetry[T any](ctx context.Context, p *mcpMessageProvider, generation *string, call func(context.Context) (T, error)) (T, error) {
	if *generation == "" {
		*generation = p.Generation()
	}
	result, err := call(ctx)
	if errors.Is(err, messaging.ErrGenerationMismatch) {
		if fresh, preflightErr := p.refreshGeneration(ctx); preflightErr == nil {
			*generation = fresh
			return call(ctx)
		}
	}
	return result, err
}

// retryGenerationMismatch retries one unversioned control verb once after
// re-preflighting, so a stale cached generation does not fail the verb for the
// rest of the process.
func retryGenerationMismatch[T any](ctx context.Context, p *mcpMessageProvider, call func(context.Context) (T, error)) (T, error) {
	result, err := call(ctx)
	if errors.Is(err, messaging.ErrGenerationMismatch) {
		if _, preflightErr := p.refreshGeneration(ctx); preflightErr == nil {
			return call(ctx)
		}
	}
	return result, err
}

func (p *mcpMessageProvider) Caller() messaging.Caller {
	return messaging.Caller{Kind: messaging.CallerOwner, RunID: messaging.SupervisorRunID}
}

func (p *mcpMessageProvider) Surface() messaging.Surface { return messaging.SurfaceMCP }

func (p *mcpMessageProvider) Role() string { return "host" }

// listedPeers lists the broker's visible peers for the supervisor session and
// decodes each session with the router's own peer record, so Resolve and
// peerRunState read one peer shape.
func (p *mcpMessageProvider) listedPeers(ctx context.Context) ([]messaging.PeerRecord, error) {
	peers, err := p.Peers(ctx, messaging.PeersRequest{Sender: messaging.Identity{RunID: messaging.SupervisorRunID}})
	if err != nil {
		return nil, err
	}
	records := make([]messaging.PeerRecord, 0, len(peers.Peers))
	for _, peer := range peers.Peers {
		var record messaging.PeerRecord
		if len(peer.Session) > 0 {
			if err := json.Unmarshal(peer.Session, &record); err != nil {
				return nil, err
			}
		}
		if record.RunID == "" {
			record.RunID = peer.RunID
		}
		records = append(records, record)
	}
	return records, nil
}

// Resolve is the host ladder: the reserved self alias already became the
// supervisor run id, then a public run id by ID only, then an exact broker
// label with an ambiguity list. It never reads the label cache or a sentinel
// basename.
func (p *mcpMessageProvider) Resolve(ctx context.Context, ref string) (messaging.Resolution, error) {
	if ref == "" {
		return messaging.Resolution{Match: messaging.MatchNone}, nil
	}
	if ref == messaging.SupervisorRunID {
		return messaging.Resolution{RunID: messaging.SupervisorRunID, Label: messaging.SupervisorRunID, Match: messaging.MatchUnique}, nil
	}
	if info := p.server.registry.Lookup(p.supervisorPath, ref); info != nil && info.RuntimeID != "" {
		return messaging.Resolution{
			RunID:       info.RuntimeID,
			PublicRunID: info.RunID,
			Label:       firstNonEmptyString(info.Label, info.RunID),
			Match:       messaging.MatchUnique,
		}, nil
	}
	// A public run id outlives the MCP server process, so the id-only rung
	// rehydrates it from the supervisor's list. Labels and runtime ids stay
	// with the broker listing below, keeping the two resolution planes.
	if info, err := p.server.lookupRunByPublicID(p.client, p.supervisorPath, ref); err != nil {
		return messaging.Resolution{}, err
	} else if info != nil && info.RuntimeID != "" {
		return messaging.Resolution{
			RunID:       info.RuntimeID,
			PublicRunID: info.RunID,
			Label:       firstNonEmptyString(info.Label, info.RunID),
			Match:       messaging.MatchUnique,
		}, nil
	}
	peers, err := p.listedPeers(ctx)
	if err != nil {
		return messaging.Resolution{}, err
	}
	var candidates []string
	label := ""
	for _, peer := range peers {
		if peer.RunID == ref {
			return p.peerResolution(peer.RunID, peer.Label), nil
		}
		if peer.Label == ref {
			candidates = append(candidates, peer.RunID)
			label = peer.Label
		}
	}
	switch len(candidates) {
	case 1:
		return p.peerResolution(candidates[0], label), nil
	case 0:
		return messaging.Resolution{Match: messaging.MatchNone}, nil
	default:
		return messaging.Resolution{Match: messaging.MatchAmbiguous, Candidates: candidates}, nil
	}
}

// peerResolution projects one broker-listing match into a resolution. The
// public run id is filled from the registry when this server already knows the
// run, so peers `last` reports one id space whichever address resolved it.
func (p *mcpMessageProvider) peerResolution(runID, label string) messaging.Resolution {
	res := messaging.Resolution{
		RunID: runID,
		Label: firstNonEmptyString(label, runID),
		Match: messaging.MatchUnique,
	}
	if info := p.server.registry.LookupRuntime(p.supervisorPath, runID); info != nil {
		res.PublicRunID = info.RunID
	}
	return res
}

// RunState reads the supervisor's status for the target and classifies the
// turn and resume facts. Resumable is a resumable sentinel or a live session
// id; a missing sentinel is simply not resumable from the sentinel. The
// supervisor session is broker-only (it has no runtime), so its state comes
// from the peer listing the same surface already reads.
func (p *mcpMessageProvider) RunState(ctx context.Context, runID string) (messaging.RunFacts, error) {
	if runID == messaging.SupervisorRunID {
		return p.peerRunState(ctx, runID)
	}
	status, err := p.client.Status(runID)
	if err != nil {
		return messaging.RunFacts{}, err
	}
	rawStatus, _ := status["status"].(string)
	rawPhase, _ := status["phase"].(string)
	translation := runstate.Translate(runstate.RuntimeStatus(rawStatus), runstate.Phase(rawPhase))
	resumable := false
	if sessionID, _ := status["session_id"].(string); sessionID != "" {
		resumable = true
	}
	if path, _ := status["sentinel_file"].(string); path != "" {
		if sentinel, err := runstate.ReadSentinel(path); err == nil && runstate.IsResumableStatus(sentinel.Status) {
			resumable = true
		}
	}
	return messaging.RunFacts{
		RunID:     runID,
		Status:    translation.Status,
		Phase:     translation.Phase,
		Live:      !translation.TurnComplete,
		Resumable: resumable,
	}, nil
}

// peerRunState reads one broker-only target's state from the peer listing. A
// broker-only run holds no sentinel and no session, so it is never resumable
// from here; the supervisor owns a resume.
func (p *mcpMessageProvider) peerRunState(ctx context.Context, runID string) (messaging.RunFacts, error) {
	peers, err := p.listedPeers(ctx)
	if err != nil {
		return messaging.RunFacts{}, err
	}
	for _, peer := range peers {
		if peer.RunID != runID {
			continue
		}
		translation := runstate.Translate(runstate.RuntimeStatus(peer.Status), runstate.PhaseNone)
		return messaging.RunFacts{
			RunID:     runID,
			Status:    translation.Status,
			Phase:     runstate.PhaseNone,
			Live:      !translation.TurnComplete,
			Resumable: false,
		}, nil
	}
	return messaging.RunFacts{}, messaging.ErrTargetUnavailable
}

// Resume forwards every router-supplied field to the control verb and mints
// only the run id and paths the provider owns. A registration failure is a
// plain error, not a typed refusal: the resume already ran, so reporting a
// refusal would deny an action that was performed. The error names both facts
// and keeps the cause; the run itself is never killed.
func (p *mcpMessageProvider) Resume(ctx context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	runID := uuid.New().String()
	sentinelFile, eventLog := runArtifactPaths(runID)
	res, err := p.client.ResumeContext(ctx, messaging.ResumeRequest{
		RunID:          runID,
		Target:         req.Target,
		Message:        req.Message,
		Label:          req.Label,
		IdempotencyKey: req.IdempotencyKey,
		SentinelFile:   sentinelFile,
		EventLog:       eventLog,
	})
	if err != nil {
		return messaging.ResumeResult{}, authorityToolError(err)
	}
	if err := p.server.registerResumed(p.client, p.supervisorPath, res); err != nil {
		return messaging.ResumeResult{}, fmt.Errorf("resume succeeded but registration failed for run_id %q runtime_id %q: %w", res.RunID, res.RuntimeID, err)
	}
	return res, nil
}

func (p *mcpMessageProvider) Rename(ctx context.Context, req messaging.RenameRequest) (messaging.RenameResult, error) {
	result, err := p.client.SessionRenameContext(ctx, req.SessionID, req.Name)
	if err != nil {
		return messaging.RenameResult{}, authorityToolError(err)
	}
	return result, nil
}

func (p *mcpMessageProvider) Last(ctx context.Context, runID string) (messaging.PeerLast, error) {
	last, err := p.client.SessionLastContext(ctx, runID)
	if err != nil {
		return messaging.PeerLast{}, authorityToolError(err)
	}
	return last, nil
}

// authorityToolError turns a control failure that carries a typed router code
// into the router's tool error, and reports everything else as a refusal with
// the control text. The control plane also fills the code with the ask
// transport's wire class (a hyphenated "invalid-request"), which names no
// router code, so that case is a refusal rather than an unknown code published
// to the model.
func authorityToolError(err error) error {
	var rpcErr *client.RPCError
	if errors.As(err, &rpcErr) && len(rpcErr.Data) > 0 {
		var data struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(rpcErr.Data, &data) == nil && routerToolCodes[data.Code] {
			return &router.ToolError{Code: data.Code, Message: rpcErr.Message, Facts: map[string]string{}}
		}
	}
	return &router.ToolError{Code: "refused", Message: err.Error(), Facts: map[string]string{}}
}

// routerToolCodes is the tool-error vocabulary a result may publish. A code
// outside it came from the wire contract, not from a router decision.
var routerToolCodes = map[string]bool{
	"ambiguous_name":         true,
	"invalid_action":         true,
	"invalid_name":           true,
	"invalid_request":        true,
	"not_permitted":          true,
	"not_resumable":          true,
	"refused":                true,
	"target_not_terminal":    true,
	"unknown_target":         true,
	"unsupported_action":     true,
	"wrong_action_for_state": true,
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
