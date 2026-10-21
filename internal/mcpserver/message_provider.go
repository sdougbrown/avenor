package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runstate"
)

// supervisorRunID is the broker run id the Go host's owner connection acts as.
const supervisorRunID = "supervisor"

// mcpMessageProvider is the router Provider for the Go host surface. It carries
// no broker token and implements no rename on the transport: resume and rename
// are control verbs, and a reply has no inbound delivery to claim here.
type mcpMessageProvider struct {
	server         *Server
	client         ControlClient
	supervisorPath string
	generation     string
	transport      *controlTransport
}

func newMCPMessageProvider(server *Server, client ControlClient, supervisorPath string) (*mcpMessageProvider, error) {
	pre, err := client.MessagePreflight()
	if err != nil {
		return nil, err
	}
	return &mcpMessageProvider{
		server:         server,
		client:         client,
		supervisorPath: supervisorPath,
		generation:     pre.Generation,
		transport:      newControlTransport(client),
	}, nil
}

func (p *mcpMessageProvider) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	return p.transport.Send(ctx, req)
}

func (p *mcpMessageProvider) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	return p.transport.Wait(ctx, req)
}

func (p *mcpMessageProvider) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	return p.transport.Reply(ctx, req)
}

func (p *mcpMessageProvider) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	return p.transport.Withdraw(ctx, req)
}

func (p *mcpMessageProvider) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return p.transport.Receive(ctx, req)
}

func (p *mcpMessageProvider) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	return p.transport.Peers(ctx, req)
}

func (p *mcpMessageProvider) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
	return p.transport.Preflight(ctx)
}

func (p *mcpMessageProvider) Close() error { return nil }

func (p *mcpMessageProvider) Generation() string { return p.generation }

func (p *mcpMessageProvider) Caller() messaging.Caller {
	return messaging.Caller{Kind: messaging.CallerOwner, RunID: supervisorRunID}
}

func (p *mcpMessageProvider) Surface() messaging.Surface { return messaging.SurfaceMCP }

func (p *mcpMessageProvider) Role() string { return "host" }

// Resolve is the host ladder: the reserved self alias already became the
// supervisor run id, then a public run id by ID only, then an exact broker
// label with an ambiguity list. It never reads the label cache or a sentinel
// basename.
func (p *mcpMessageProvider) Resolve(ctx context.Context, ref string) (messaging.Resolution, error) {
	if ref == "" {
		return messaging.Resolution{Match: messaging.MatchNone}, nil
	}
	if ref == supervisorRunID {
		return messaging.Resolution{RunID: supervisorRunID, Label: supervisorRunID, Match: messaging.MatchUnique}, nil
	}
	if info := p.server.registry.Lookup(p.supervisorPath, ref); info != nil && info.RuntimeID != "" {
		return messaging.Resolution{
			RunID:       info.RuntimeID,
			PublicRunID: info.RunID,
			Label:       firstNonEmptyString(info.Label, info.RunID),
			Match:       messaging.MatchUnique,
		}, nil
	}
	peers, err := p.transport.Peers(ctx, messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: supervisorRunID},
		Generation: p.generation,
	})
	if err != nil {
		return messaging.Resolution{}, err
	}
	var candidates []string
	label := ""
	for _, peer := range peers.Peers {
		var session struct {
			RunID string `json:"run_id"`
			Label string `json:"label"`
		}
		if len(peer.Session) > 0 {
			_ = json.Unmarshal(peer.Session, &session)
		}
		if session.RunID == "" {
			session.RunID = peer.RunID
		}
		if session.RunID == ref {
			return messaging.Resolution{RunID: session.RunID, Label: firstNonEmptyString(session.Label, session.RunID), Match: messaging.MatchUnique}, nil
		}
		if session.Label == ref {
			candidates = append(candidates, session.RunID)
			label = session.Label
		}
	}
	switch len(candidates) {
	case 1:
		return messaging.Resolution{RunID: candidates[0], Label: label, Match: messaging.MatchUnique}, nil
	case 0:
		return messaging.Resolution{Match: messaging.MatchNone}, nil
	default:
		return messaging.Resolution{Match: messaging.MatchAmbiguous, Candidates: candidates}, nil
	}
}

// RunState reads the supervisor's status for the target and classifies the
// turn and resume facts. Resumable is a resumable sentinel or a live session
// id; a missing sentinel is simply not resumable from the sentinel.
func (p *mcpMessageProvider) RunState(ctx context.Context, runID string) (messaging.RunFacts, error) {
	_ = ctx
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

// Resume forwards every router-supplied field to the control verb and mints
// only the run id and paths the provider owns. A registration failure is a
// typed refusal that still names both ids; the run itself is never killed.
func (p *mcpMessageProvider) Resume(ctx context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	_ = ctx
	runID := uuid.New().String()
	sentinelFile := filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.done", runID))
	eventLog := filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.log", runID))
	res, err := p.client.Resume(messaging.ResumeRequest{
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
	if err := p.server.registerResumed(p.client, p.supervisorPath, req, res); err != nil {
		return messaging.ResumeResult{}, &router.ToolError{
			Code:    "refused",
			Message: fmt.Sprintf("resume succeeded but registration failed for run_id %q runtime_id %q: %v", res.RunID, res.RuntimeID, err),
			Facts:   map[string]string{},
		}
	}
	return res, nil
}

func (p *mcpMessageProvider) Rename(ctx context.Context, req messaging.RenameRequest) (messaging.RenameResult, error) {
	_ = ctx
	result, err := p.client.SessionRename(req.SessionID, req.Name)
	if err != nil {
		return messaging.RenameResult{}, authorityToolError(err)
	}
	return result, nil
}

func (p *mcpMessageProvider) Last(ctx context.Context, runID string) (messaging.PeerLast, error) {
	_ = ctx
	last, err := p.client.SessionLast(runID)
	if err != nil {
		return messaging.PeerLast{}, authorityToolError(err)
	}
	return last, nil
}

// authorityToolError turns a control failure that carries a typed code into the
// router's tool error, and falls back to a refusal with the control text.
func authorityToolError(err error) error {
	var rpcErr *client.RPCError
	if errors.As(err, &rpcErr) && len(rpcErr.Data) > 0 {
		var data struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(rpcErr.Data, &data) == nil && data.Code != "" {
			return &router.ToolError{Code: data.Code, Message: rpcErr.Message, Facts: map[string]string{}}
		}
	}
	return &router.ToolError{Code: "refused", Message: err.Error(), Facts: map[string]string{}}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
