// Package brokertools holds the shared child-side messaging seam used by the
// two Go child surfaces: the channeltools server and the claude-channel
// sidecar. It implements the router Provider over one localhttp transport, so
// a child surface supplies credentials and a surface name and gets the same
// resolution, state, rename and last-message behaviour on both.
package brokertools

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runstate"
)

// Options is what one child surface supplies. RunID and Token are the run's own
// broker credentials; BrokerURL is the local broker base URL. HTTPClient is
// used by the authenticated helpers (rename and last message), not by the
// transport, which owns its own client.
type Options struct {
	RunID      string
	Token      string
	BrokerURL  string
	HTTPClient *http.Client
}

// MessageProvider is the router Provider shared by the Go child surfaces. It
// speaks as the broker-authenticated run it was built for and delegates the
// eight transport methods to one localhttp transport.
type MessageProvider struct {
	opts      Options
	surface   messaging.Surface
	transport messaging.Transport

	mu         sync.Mutex
	generation string
}

// NewMessageProvider builds the shared child provider over one local transport.
// A transport that cannot be built fails here instead of registering a nil
// transport; the generation is read once per provider by Prepare.
func NewMessageProvider(surface messaging.Surface, opts Options) (*MessageProvider, error) {
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{}
	}
	tr, err := localhttp.New("", messaging.LocalCredentials{RunID: opts.RunID, Token: opts.Token, BaseURL: opts.BrokerURL})
	if err != nil {
		return nil, err
	}
	return &MessageProvider{opts: opts, surface: surface, transport: tr}, nil
}

// Prepare reads and caches the broker generation the router binds to. It is
// idempotent, so a surface calls it before NewRouter on every tool call and the
// provider still pins the generation it read the first time.
func (p *MessageProvider) Prepare(ctx context.Context) error {
	p.mu.Lock()
	if p.generation != "" {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()
	pre, err := p.transport.Preflight(ctx)
	if err != nil {
		return err
	}
	p.mu.Lock()
	if p.generation == "" {
		p.generation = pre.Generation
	}
	p.mu.Unlock()
	return nil
}

// Close releases the provider's transport. The surface's shutdown calls it.
func (p *MessageProvider) Close() error { return p.transport.Close() }

func (p *MessageProvider) Generation() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.generation
}

func (p *MessageProvider) Caller() messaging.Caller {
	return messaging.Caller{Kind: messaging.CallerBroker, RunID: p.opts.RunID}
}

func (p *MessageProvider) Surface() messaging.Surface { return p.surface }

func (p *MessageProvider) Role() string { return "agent" }

func (p *MessageProvider) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	return p.transport.Send(ctx, req)
}

func (p *MessageProvider) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	return p.transport.Wait(ctx, req)
}

func (p *MessageProvider) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	return p.transport.Reply(ctx, req)
}

func (p *MessageProvider) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	return p.transport.Withdraw(ctx, req)
}

func (p *MessageProvider) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return p.transport.Receive(ctx, req)
}

func (p *MessageProvider) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	return p.transport.Peers(ctx, req)
}

func (p *MessageProvider) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
	return p.transport.Preflight(ctx)
}

// peerSession is the SessionInfo projection a child can read from /sessions.
type peerSession struct {
	RunID  string `json:"run_id"`
	Label  string `json:"label"`
	Kind   string `json:"kind"`
	Status string `json:"status"`
}

// listedPeers reads the broker's visible peers for this caller.
func (p *MessageProvider) listedPeers(ctx context.Context) ([]peerSession, error) {
	peers, err := p.transport.Peers(ctx, messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: p.opts.RunID},
		Generation: p.Generation(),
	})
	if err != nil {
		return nil, err
	}
	sessions := make([]peerSession, 0, len(peers.Peers))
	for _, peer := range peers.Peers {
		var session peerSession
		if len(peer.Session) > 0 {
			if err := json.Unmarshal(peer.Session, &session); err != nil {
				return nil, err
			}
		}
		if session.RunID == "" {
			session.RunID = peer.RunID
		}
		sessions = append(sessions, session)
	}
	return sessions, nil
}

// Resolve is the child ladder: the run's own id, then a runtime id, then an
// exact label with an ambiguity list. A child never learns a public run id, so
// it resolves nothing else.
func (p *MessageProvider) Resolve(ctx context.Context, ref string) (messaging.Resolution, error) {
	if ref == "" {
		return messaging.Resolution{Match: messaging.MatchNone}, nil
	}
	if ref == p.opts.RunID {
		return messaging.Resolution{RunID: p.opts.RunID, Label: ref, Match: messaging.MatchUnique}, nil
	}
	sessions, err := p.listedPeers(ctx)
	if err != nil {
		return messaging.Resolution{}, err
	}
	for _, session := range sessions {
		if session.RunID == ref {
			return messaging.Resolution{
				RunID: session.RunID,
				Label: firstNonEmpty(session.Label, session.RunID),
				Match: messaging.MatchUnique,
			}, nil
		}
	}
	var candidates []string
	label := ""
	for _, session := range sessions {
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

// RunState normalises the peer's status word the one way the rest of the tree
// does. A child holds no phase stream and cannot read the target's sentinel, so
// it is never resumable from here: the supervisor owns a resume.
func (p *MessageProvider) RunState(ctx context.Context, runID string) (messaging.RunFacts, error) {
	sessions, err := p.listedPeers(ctx)
	if err != nil {
		return messaging.RunFacts{}, err
	}
	for _, session := range sessions {
		if session.RunID != runID {
			continue
		}
		translation := runstate.Translate(runstate.RuntimeStatus(session.Status), runstate.PhaseNone)
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

// Resume is never granted to a child by the route table; this is the defensive
// answer if a future route reaches it.
func (p *MessageProvider) Resume(context.Context, messaging.ResumeRequest) (messaging.ResumeResult, error) {
	return messaging.ResumeResult{}, &router.ToolError{
		Code:    "not_permitted",
		Message: "a broker-authenticated run may resume only a run it spawned, which a child cannot own",
		Facts:   map[string]string{},
	}
}

func (p *MessageProvider) Rename(ctx context.Context, req messaging.RenameRequest) (messaging.RenameResult, error) {
	creds := messaging.LocalCredentials{RunID: p.opts.RunID, Token: p.opts.Token}
	return RenameSelf(ctx, p.opts.HTTPClient, p.opts.BrokerURL, creds, req.SessionID, req.Name)
}

func (p *MessageProvider) Last(ctx context.Context, runID string) (messaging.PeerLast, error) {
	creds := messaging.LocalCredentials{RunID: p.opts.RunID, Token: p.opts.Token}
	return LastMessage(ctx, p.opts.HTTPClient, p.opts.BrokerURL, creds, runID)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// brokerError is the broker's error envelope. code is a wire code; error is
// the human text the surface passes through verbatim.
type brokerError struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// doAuthed performs one authenticated request. GET credentials travel in
// headers, matching the broker's /sessions and /last_message routes; a POST
// body repeats run_id/token because the rename route reads them from the body.
func doAuthed(ctx context.Context, client *http.Client, method, url string, body map[string]any, creds messaging.LocalCredentials, out any) (int, brokerError, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, brokerError{}, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return 0, brokerError{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(messaging.HeaderRunID, creds.RunID)
	req.Header.Set(messaging.HeaderRunToken, creds.Token)
	resp, err := client.Do(req)
	if err != nil {
		return 0, brokerError{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, brokerError{}, err
	}
	var envelope brokerError
	if len(bytes.TrimSpace(raw)) > 0 && bytes.TrimSpace(raw)[0] == '{' {
		_ = json.Unmarshal(raw, &envelope)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if envelope.Error == "" {
			envelope.Error = string(bytes.TrimSpace(raw))
		}
		return resp.StatusCode, envelope, nil
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, envelope, err
		}
	}
	return resp.StatusCode, envelope, nil
}

// wireCode maps a broker error code to the router's typed code. An unrecognised
// code is a refusal, never a silently empty result.
func wireCode(code string) (string, bool) {
	switch code {
	case "not-permitted":
		return "not_permitted", true
	case "invalid-name":
		return "invalid_name", true
	case "unknown-target":
		return "unknown_target", true
	case "ambiguous-name":
		return "ambiguous_name", true
	case "invalid-request":
		return "refused", true
	default:
		return "refused", false
	}
}

func toolErrorFor(status int, envelope brokerError) *router.ToolError {
	code, known := wireCode(envelope.Code)
	if !known {
		switch status {
		case http.StatusForbidden:
			code = "not_permitted"
		case http.StatusNotFound:
			code = "unknown_target"
		case http.StatusConflict:
			code = "ambiguous_name"
		default:
			code = "refused"
		}
	}
	return &router.ToolError{Code: code, Message: envelope.Error, Facts: map[string]string{}}
}

// RenameSelf renames one run through the broker's authenticated /rename route.
// sessionID is forwarded verbatim (empty means the caller's own run), so
// Supervisor.SessionRename decides every rule and this helper only maps the
// wire codes to the router's typed errors.
func RenameSelf(ctx context.Context, client *http.Client, brokerURL string, creds messaging.LocalCredentials, sessionID, name string) (messaging.RenameResult, error) {
	body := map[string]any{"run_id": creds.RunID, "token": creds.Token, "name": name}
	if sessionID != "" {
		body["session_id"] = sessionID
	}
	var result struct {
		RunID         string `json:"run_id"`
		Label         string `json:"label"`
		PreviousLabel string `json:"previous_label"`
	}
	status, envelope, err := doAuthed(ctx, client, http.MethodPost, brokerURL+"/rename", body, creds, &result)
	if err != nil {
		return messaging.RenameResult{}, err
	}
	if status < 200 || status >= 300 {
		return messaging.RenameResult{}, toolErrorFor(status, envelope)
	}
	return messaging.RenameResult{RunID: result.RunID, Label: result.Label, PreviousLabel: result.PreviousLabel}, nil
}

// LastMessage reads one target's recorded last turn through the broker's
// universally visible GET /last_message. has_record:false is a successful empty
// result; a 404 target-unavailable is the unknown_target tool error.
func LastMessage(ctx context.Context, client *http.Client, brokerURL string, creds messaging.LocalCredentials, target string) (messaging.PeerLast, error) {
	endpoint := brokerURL + "/last_message?target=" + url.QueryEscape(target)
	var result struct {
		RunID       string `json:"run_id"`
		Label       string `json:"label"`
		HasRecord   bool   `json:"has_record"`
		FinalOutput string `json:"final_output"`
		StopReason  string `json:"stop_reason"`
		Truncated   bool   `json:"final_output_truncated"`
	}
	status, envelope, err := doAuthed(ctx, client, http.MethodGet, endpoint, nil, creds, &result)
	if err != nil {
		return messaging.PeerLast{}, err
	}
	if status < 200 || status >= 300 {
		return messaging.PeerLast{}, toolErrorFor(status, envelope)
	}
	return messaging.PeerLast{
		RunID:      result.RunID,
		Label:      result.Label,
		HasRecord:  result.HasRecord,
		Text:       result.FinalOutput,
		StopReason: result.StopReason,
		Truncated:  result.Truncated,
	}, nil
}
