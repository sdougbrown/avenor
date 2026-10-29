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
	client         ControlClient
	supervisorPath string
	generationMu   sync.RWMutex
	generation     string
	transport      *controlTransport
	// resolvedRun is the turn a Resolve call bound on this provider. The
	// provider is minted per tool call by messageSurfaceFor, consumed by
	// one router Execute on the call's goroutine, and discarded with the
	// call, so the field cannot be observed by a concurrent call.
	resolvedRun *RunInfo
}

// newMCPMessageProvider preflights the control store under ctx, so a caller can
// bound or cancel the constructor's one blocking RPC.
func newMCPMessageProvider(ctx context.Context, server *Server, client ControlClient, supervisorPath string) (*mcpMessageProvider, error) {
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
	sentinel, err := p.turnIdentity(req.To.RunID)
	if err != nil {
		return messaging.SendResult{}, err
	}
	req.ExpectedSentinelFile = sentinel
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
	return versionedRetry(ctx, p, nil, func(ctx context.Context) (messaging.SendResult, error) {
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

// versionedRetry retries one control verb once after a re-preflight, so a
// stale cached generation does not fail the verb for the rest of the process.
// A generation-carrying verb passes its request field, which is pinned to the
// provider's current generation when empty and updated to the freshly
// preflighted one on retry; a verb that carries no generation passes nil and
// the pin/update is skipped. One policy, one place.
func versionedRetry[T any](ctx context.Context, p *mcpMessageProvider, generation *string, call func(context.Context) (T, error)) (T, error) {
	if generation != nil && *generation == "" {
		*generation = p.Generation()
	}
	result, err := call(ctx)
	if errors.Is(err, messaging.ErrGenerationMismatch) {
		if fresh, preflightErr := p.refreshGeneration(ctx); preflightErr == nil {
			if generation != nil {
				*generation = fresh
			}
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
// decodes them with the contract's own peer decoder, so Resolve and
// peerRunState read one peer shape.
func (p *mcpMessageProvider) listedPeers(ctx context.Context) ([]messaging.PeerRecord, error) {
	peers, err := p.Peers(ctx, messaging.PeersRequest{Sender: messaging.Identity{RunID: messaging.SupervisorRunID}})
	if err != nil {
		return nil, err
	}
	return messaging.PeerListing(peers.Peers, err)
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
		p.resolvedRun = info
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
		p.resolvedRun = info
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
	// The run-id rung stays a separate pass: an exact run id wins over any
	// label, so a label collected alongside it must not be able to answer first.
	for _, peer := range peers {
		if peer.RunID == ref {
			return p.peerResolution(peer.RunID, peer.Label), nil
		}
	}
	candidates, label := messaging.LabelCandidates(peers, ref)
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
		p.resolvedRun = info
	} else if status, err := p.client.Status(runID); err == nil {
		// Several registered turns can share a runtime id (a warm resume,
		// or a restart that re-minted one while the pre-restart entry stays
		// registered). The current turn is the one whose sentinel the runtime
		// currently holds; a sentinel no registered turn owns, or a status
		// that cannot be read, keeps the ambiguity rather than answering
		// with an entry the map iteration reached.
		if path := stringField(status, "sentinel_file"); path != "" {
			if info := p.server.registry.LookupRuntimeTurn(p.supervisorPath, runID, path); info != nil {
				res.PublicRunID = info.RunID
				p.resolvedRun = info
			}
		}
	}
	return res
}

// RunState reads the supervisor's status for the target and classifies the
// turn and resume facts through runFactsFromStatus. A status failure falls back
// to the registry's terminal sentinel, so a run that finished before a
// supervisor restart still reports as done, and then to the broker's peer
// listing, which is the only authority for a run a registered child broker
// owns. That second fallback reads only the supervisor's own absence answer for
// this runtime, so a refusal, a stopped control plane or a transport failure
// keeps its error, and an unknown run stays refused because a listing without
// the target changes nothing. The supervisor session is broker-only (it has no
// runtime anywhere), so its state comes from the peer listing the same surface
// already reads.
func (p *mcpMessageProvider) RunState(ctx context.Context, runID string) (messaging.RunFacts, error) {
	if runID == messaging.SupervisorRunID {
		return p.peerRunState(ctx, runID)
	}
	if info := p.resolvedRun; info != nil && info.RuntimeID == runID {
		status, err := p.server.queryRunStatus(p.client, p.supervisorPath, info.RunID)
		if err != nil {
			// A historical turn whose terminal sentinel cannot be read: the
			// turn is over, its final status is unknown, and it is not
			// resumable through the shared runtime. The router answers
			// not_resumable from these facts.
			if errors.Is(err, ErrRunMovedTurn) {
				return messaging.RunFacts{RunID: runID, Status: runstate.StatusUnknown}, nil
			}
			return messaging.RunFacts{}, err
		}
		return runFactsFromStatus(runID, status), nil
	}
	status, err := p.client.Status(runID)
	if err == nil {
		return runFactsFromStatus(runID, status), nil
	}
	// The registry deliberately keeps a dead run discoverable by run id for its
	// persisted terminal result. Read that fallback narrowly: no entry, no
	// terminal sentinel, or a sentinel read error keeps the original failure.
	if info := p.server.registry.Lookup(p.supervisorPath, runID); info != nil {
		if terminal, ok := terminalStatusFromRunInfo(info); ok {
			return runFactsFromStatus(runID, terminal), nil
		}
	}
	if !statusReportsNoRuntime(err, runID) {
		return messaging.RunFacts{}, err
	}
	// A run a registered child broker owns has no runtime here, so the broker
	// that adopted its claim is the only authority for it. The listing is read
	// over the same control connection, so a store that cannot answer at all
	// fails here too and the original failure survives.
	if facts, peerErr := p.peerRunState(ctx, runID); peerErr == nil {
		return facts, nil
	}
	return messaging.RunFacts{}, err
}

// statusReportsNoRuntime reports whether a status failure is the supervisor's
// absence answer for this runtime rather than a refusal or a transport
// failure. The control plane carries that answer as a server failure (-32000)
// whose message names the runtime it could not find
// (Supervisor.RuntimeStatus), with no structured data to key on, so the report
// is matched exactly: same code, same runtime id, same sentence. Anything else
// is a failure the caller keeps.
func statusReportsNoRuntime(err error, runID string) bool {
	var rpcErr *client.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 {
		return false
	}
	return rpcErr.Message == fmt.Sprintf("runtime %q not found", runID)
}

// runFactsFromStatus classifies one supervisor status map into the turn and
// resume facts the router reads. Resumable is a resumable sentinel or a live
// session id; a missing sentinel is simply not resumable from the sentinel.
// A status flagged _historical_turn is the runtime's current turn read for a
// prior one: its session belongs to the current turn, not the target's, and
// the target's turn is over, so neither the session nor the sentinel counts
// and the turn is not live. The live status and the registry's
// terminal-status fallback share this classification, so the two paths cannot
// drift.
func runFactsFromStatus(runID string, status map[string]any) messaging.RunFacts {
	rawStatus, _ := status["status"].(string)
	rawPhase, _ := status["phase"].(string)
	translation := runstate.Translate(runstate.RuntimeStatus(rawStatus), runstate.Phase(rawPhase))
	historical, _ := status["_historical_turn"].(bool)
	resumable := false
	if !historical {
		if sessionID, _ := status["session_id"].(string); sessionID != "" {
			resumable = true
		}
		if path, _ := status["sentinel_file"].(string); path != "" {
			if sentinel, err := runstate.ReadSentinel(path); err == nil && runstate.IsResumableStatus(sentinel.Status) {
				resumable = true
			}
		}
	}
	return messaging.RunFacts{
		RunID:     runID,
		Status:    translation.Status,
		Phase:     translation.Phase,
		Live:      !translation.TurnComplete && !historical,
		Resumable: resumable,
	}
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
		return messaging.PeerRunFacts(peer), nil
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
	sentinel, err := p.turnIdentity(runID)
	if err != nil {
		return messaging.PeerLast{}, err
	}
	var last messaging.PeerLast
	if sentinel == "" {
		last, err = p.client.SessionLastContext(ctx, runID)
	} else {
		last, err = p.client.SessionLastTurnContext(ctx, runID, sentinel)
	}
	if err != nil {
		return messaging.PeerLast{}, authorityToolError(err)
	}
	return last, nil
}

// turnIdentity returns the sentinel the target's turn must still own. A target
// the broker lists but this server holds no registry entry for has no turn
// identity to confuse: with no registered turn there is no prior-vs-current
// turn to mix up, and refusing would break tell, ask and last for runs this
// server never registered — the same open path the TS provider keeps through
// expectedSentinel(undefined). A registry-known target, though, is only safe to
// use through the entry that owns its current turn, so a call reached without
// that resolution — or with one whose turn has no sentinel of its own — fails
// closed instead of running unguarded.
func (p *mcpMessageProvider) turnIdentity(target string) (string, error) {
	if target == messaging.SupervisorRunID {
		return "", nil
	}
	if p.server.registry.LookupRuntime(p.supervisorPath, target) == nil {
		return "", nil
	}
	info := p.resolvedRun
	if info == nil || info.RuntimeID != target {
		return "", fmt.Errorf("turn identity unavailable for %q", target)
	}
	if info.SentinelPath == "" {
		return "", fmt.Errorf("turn identity unavailable for %q", info.RunID)
	}
	return info.SentinelPath, nil
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
