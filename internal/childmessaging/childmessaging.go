// Package childmessaging holds the shared child-side messaging seam. It
// implements the router Provider over one localhttp transport, so a Go child
// surface supplies credentials and a surface name and gets the router's
// resolution, state, rename and last-message behaviour over that one transport.
package childmessaging

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// Options is what one child surface supplies. RunID and Token are the run's own
// broker credentials; BrokerURL is the local broker base URL. HTTPClient's
// transport and timeout carry every request the provider makes, so the surface's
// own client bounds rename and last message as well as the transport calls; a
// nil client keeps the transport's default.
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
	transport *localhttp.Transport

	mu         sync.Mutex
	generation string
}

// NewMessageProvider builds the shared child provider over one local transport.
// A transport that cannot be built fails here instead of registering a nil
// transport; the generation is read once per provider by Prepare.
func NewMessageProvider(surface messaging.Surface, opts Options) (*MessageProvider, error) {
	tr, err := localhttp.New("", messaging.LocalCredentials{RunID: opts.RunID, Token: opts.Token, BaseURL: opts.BrokerURL})
	if err != nil {
		return nil, err
	}
	tr.UseHTTPClient(opts.HTTPClient)
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

// listedPeers reads the broker's visible peers for this caller. It decodes them
// with the contract's own peer decoder, so a field added to the peer contract
// reaches Resolve and RunState instead of stopping at a local copy. A listing
// that could not be completed keeps the peers it did read and reports the
// incompleteness alongside them, so a caller can answer for a peer it can see
// and refuses to answer for one it cannot.
func (p *MessageProvider) listedPeers(ctx context.Context) ([]messaging.PeerRecord, error) {
	peers, err := p.transport.Peers(ctx, messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: p.opts.RunID},
		Generation: p.Generation(),
	})
	return messaging.PeerListing(peers.Peers, err)
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
	for _, session := range sessions {
		if session.RunID == ref {
			return messaging.Resolution{
				RunID: session.RunID,
				Label: firstNonEmpty(session.Label, session.RunID),
				Match: messaging.MatchUnique,
			}, nil
		}
	}
	candidates, label := messaging.LabelCandidates(sessions, ref)
	switch len(candidates) {
	case 1:
		return messaging.Resolution{RunID: candidates[0], Label: label, Match: messaging.MatchUnique}, nil
	case 0:
		if err != nil {
			// The listing was incomplete, so this peer may be in the part that
			// was not read. Refuse rather than claim the peer does not exist.
			return messaging.Resolution{}, err
		}
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
	for _, session := range sessions {
		if session.RunID != runID {
			continue
		}
		return messaging.PeerRunFacts(session), nil
	}
	if err != nil {
		return messaging.RunFacts{}, err
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

// Rename renames one run through the transport's authenticated /rename path,
// so it takes the same protocol gate as the ask verbs.
func (p *MessageProvider) Rename(ctx context.Context, req messaging.RenameRequest) (messaging.RenameResult, error) {
	result, err := p.transport.RenameSelf(ctx, req.SessionID, req.Name)
	if err != nil {
		return messaging.RenameResult{}, toolErrorFromBroker(err)
	}
	return result, nil
}

// Last reads one target's recorded last turn through the transport's
// authenticated /last_message path.
func (p *MessageProvider) Last(ctx context.Context, runID string) (messaging.PeerLast, error) {
	last, err := p.transport.LastMessage(ctx, runID)
	if err != nil {
		return messaging.PeerLast{}, toolErrorFromBroker(err)
	}
	return last, nil
}

// toolErrorFromBroker maps a broker refusal onto the router's typed code. A
// rejected run token is the not-permitted refusal the 403 sender-mismatch path
// also reports; a failure that is not a broker answer keeps its own text and
// becomes a refusal at the router boundary.
func toolErrorFromBroker(err error) error {
	if errors.Is(err, messaging.ErrAuthRejected) {
		return &router.ToolError{Code: "not_permitted", Message: err.Error(), Facts: map[string]string{}}
	}
	var brokerErr *localhttp.BrokerError
	if errors.As(err, &brokerErr) {
		return toolErrorFor(brokerErr.Status, brokerError{Error: brokerErr.Message, Code: brokerErr.Code})
	}
	return err
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
