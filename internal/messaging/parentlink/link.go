package parentlink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/internal/httpbase"
)

// Timeouts the link applies to its own bookkeeping. A registration is a small
// request a caller may block on and a deregistration is best effort, so neither
// inherits the messaging wait budget.
const (
	registerTimeout   = 5 * time.Second
	deregisterTimeout = 2 * time.Second
)

// Options configure one branch's child link to its parent broker.
type Options struct {
	// ParentURL is the parent broker's http:// address. Empty means this branch
	// has no parent: nothing is forwarded upward.
	ParentURL string
	// Token is the bearer credential the parent broker's parent routes require.
	Token string
	// Endpoint is this broker's own http:// address, which the parent posts a
	// relay to. Required when ParentURL is set.
	Endpoint string
	// ChildID names this branch at its parent. Empty mints one per link.
	ChildID string
	// Runs reports the runs this branch owns, with the display names it knows
	// them by. It is read at every registration. Required when ParentURL is
	// set: the link publishes this branch's run set to its parent.
	Runs func() []messaging.RunClaim
	// Generation reports this broker's current ask generation. Required: the
	// link pins it and the relay binds every hop to it.
	Generation func() string
	// Timeout bounds one request to a peer broker. Zero uses the messaging
	// default.
	Timeout time.Duration
}

// askKey identifies one ask this branch forwarded upward.
type askKey struct {
	Sender    string
	MessageID string
}

// askRoute is the hop one forwarded ask was admitted on, so a wait or a
// withdrawal reaches the store that owns the edge instead of this one.
type askRoute struct {
	generation string
	recordedAt time.Time
}

// relayRoute is one ask an ancestor relayed into this branch: the run that
// asked and the generation the ancestor's ask store admitted the edge under.
type relayRoute struct {
	askerRunID     string
	edgeGeneration string
	recordedAt     time.Time
}

// Link is one branch's child link to its parent broker. It registers the runs
// this branch owns, forwards the operations no local run can answer, and
// records the per-hop route of every ask so a wait, a withdrawal or a reply
// follows the hop the ask was admitted on.
type Link struct {
	opts   Options
	client *httpbase.Client

	// regMu serializes registrations so one run-set change never races another
	// for the parent's view of this branch.
	regMu sync.Mutex

	mu         sync.Mutex
	generation string
	tokens     map[string]string
	asks       map[askKey]askRoute
	relays     map[string]relayRoute
	closed     bool
	closedCh   chan struct{}

	dirty chan struct{}
	wg    sync.WaitGroup
}

// NewLink builds one branch's link to its parent broker.
func NewLink(opts Options) (*Link, error) {
	if opts.ParentURL == "" {
		return nil, fmt.Errorf("%w: parent broker URL is required", messaging.ErrInvalidRequest)
	}
	if opts.Endpoint == "" {
		return nil, fmt.Errorf("%w: this broker's endpoint is required to register with a parent", messaging.ErrInvalidRequest)
	}
	if opts.Runs == nil || opts.Generation == nil {
		return nil, fmt.Errorf("%w: the link needs this broker's runs and its generation", messaging.ErrInvalidRequest)
	}
	if opts.ChildID == "" {
		opts.ChildID = messaging.NewGeneration()
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = httpbase.DefaultRequestTimeout
	}
	opts.ParentURL = strings.TrimRight(opts.ParentURL, "/")
	return &Link{
		opts:     opts,
		client:   httpbase.New(ErrUnsupportedParentProtocol, timeout),
		tokens:   map[string]string{},
		asks:     map[askKey]askRoute{},
		relays:   map[string]relayRoute{},
		closedCh: make(chan struct{}),
		dirty:    make(chan struct{}, 1),
	}, nil
}

// Start publishes this branch's claim to the parent broker and then re-publishes
// it whenever the run set changes.
func (l *Link) Start(ctx context.Context) error {
	if err := l.Sync(ctx); err != nil {
		return err
	}
	l.wg.Add(1)
	go l.publishLoop(ctx)
	return nil
}

// RunChanged asks the link to re-publish this branch's run set. Changes are
// coalesced: a burst of run creation costs one registration.
func (l *Link) RunChanged() {
	select {
	case <-l.closedCh:
		return
	default:
	}
	select {
	case l.dirty <- struct{}{}:
	default:
	}
}

// publishLoop re-publishes the run set after a change. A parent that is
// unreachable keeps the claim it last accepted; the next change or the next
// upward call re-publishes.
func (l *Link) publishLoop(ctx context.Context) {
	defer l.wg.Done()
	for {
		select {
		case <-l.closedCh:
			return
		case <-ctx.Done():
			return
		case <-l.dirty:
			_ = l.Sync(ctx)
		}
	}
}

// Sync publishes this branch's run set to the parent broker and pins the
// generation it answered with. A registration replaces the previous claim, so a
// run this branch stopped owning is retired at the parent with it.
func (l *Link) Sync(ctx context.Context) error {
	l.regMu.Lock()
	defer l.regMu.Unlock()
	// The branch hands its parent the credentials its own runs already use, so
	// the parent forwards an ask as the run that asked and relays a message back
	// into the run's own queue.
	claims := l.opts.Runs()
	tokens := make(map[string]string, len(claims))
	for _, claim := range claims {
		if claim.Token != "" {
			tokens[claim.RunID] = claim.Token
		}
	}
	status, raw, err := l.post(ctx, pathRegister, map[string]any{
		"child_id":   l.opts.ChildID,
		"endpoint":   l.opts.Endpoint,
		"generation": l.opts.Generation(),
		"runs":       claims,
	}, "", "", registerTimeout)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrParentUnavailable, err)
	}
	registered, err := decode(status, raw)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.tokens = tokens
	l.noteGenerationLocked(registered.Generation)
	l.mu.Unlock()
	return nil
}

// noteGenerationLocked pins the parent's generation. The routes recorded against
// a previous generation keep their own generation, so the hop that admitted them
// refuses them and each operation reports the replacement instead of replaying
// into it.
func (l *Link) noteGenerationLocked(generation string) {
	if generation == "" {
		return
	}
	l.generation = generation
}

// Close retracts this branch's claim and releases the link. The retraction is
// best effort: a parent that is already gone needs no cleanup, and a forwarded
// ask is never recalled.
func (l *Link) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.closedCh)
	l.mu.Unlock()
	l.wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), deregisterTimeout)
	defer cancel()
	// A parent that cannot answer needs no cleanup: the claim dies with it.
	_, _, _ = l.post(ctx, pathDeregister, map[string]any{"child_id": l.opts.ChildID}, "", "", deregisterTimeout)
	return l.client.Close()
}

// Send forwards an ask to the parent broker, which admits the one edge the
// cross-branch ask owns.
func (l *Link) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	fail := func(outcome messaging.Outcome, err error) (messaging.SendResult, error) {
		return messaging.SendResult{Outcome: outcome, MessageID: req.MessageID, Generation: req.Generation}, err
	}
	token, generation, err := l.credentials(ctx, req.From.RunID)
	if err != nil {
		return fail(messaging.OutcomeNotSent, err)
	}
	payload, err := payloadFor(req.Body)
	if err != nil {
		return fail(messaging.OutcomeNotSent, err)
	}
	status, raw, err := l.post(ctx, pathSend, map[string]any{
		"from_run_id":   req.From.RunID,
		"to_run_id":     req.To.RunID,
		"type":          "agent_message",
		"payload":       payload,
		"message_id":    req.MessageID,
		"generation":    generation,
		"expects_reply": !req.NoReply,
	}, req.From.RunID, token, 0)
	if err != nil {
		if httpbase.NeverTransmitted(err) {
			return fail(messaging.OutcomeNotSent, fmt.Errorf("%w: %v", ErrParentUnavailable, err))
		}
		return fail(messaging.OutcomePossiblyPending, fmt.Errorf("%w: %v", ErrParentUnavailable, err))
	}
	body, err := decode(status, raw)
	if err != nil {
		if errors.Is(err, messaging.ErrGenerationMismatch) {
			return fail(l.replacementOutcome(ctx, generation), err)
		}
		return fail(body.sendOutcomeOf(), err)
	}
	outcome := body.Outcome
	if !messaging.IsSendOutcome(outcome) {
		return fail(messaging.OutcomePossiblyPending,
			fmt.Errorf("%w: parent broker answered without a send outcome", ErrUnsupportedParentProtocol))
	}
	if !req.NoReply && (outcome == messaging.OutcomeSent || outcome == messaging.OutcomeWithdrawn) {
		// The edge exists in the parent's store, so every later operation on this
		// ask follows the hop it was admitted on.
		l.recordAsk(req.From.RunID, req.MessageID, askRoute{generation: generation})
	}
	if outcome != messaging.OutcomeSent && outcome != messaging.OutcomeWithdrawn {
		return fail(outcome, nil)
	}
	return messaging.SendResult{Outcome: outcome, MessageID: req.MessageID, Generation: generation}, nil
}

// Wait blocks on the edge a forwarded ask was admitted on. The parent broker
// owns the edge, so this hop only translates the caller's generation into the
// one the parent pinned.
func (l *Link) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	key := askKey{Sender: req.Sender.RunID, MessageID: req.MessageID}
	route, ok := l.askRoute(req.Sender.RunID, req.MessageID)
	if !ok {
		return messaging.WaitResult{Outcome: messaging.OutcomeEdgeAbsent}, nil
	}
	token, _, err := l.credentials(ctx, req.Sender.RunID)
	if err != nil {
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, err
	}
	timeout := httpbase.WaitTimeout(req.Timeout)
	status, raw, err := l.post(ctx, pathWait, map[string]any{
		"waiting_for": req.MessageID,
		"generation":  route.generation,
		"timeout_ms":  timeout.Milliseconds(),
	}, req.Sender.RunID, token, timeout+messaging.CleanupDeadline)
	if err != nil {
		if httpbase.IsTransportCancelled(err) {
			return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, nil
		}
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, fmt.Errorf("%w: %v", ErrParentUnavailable, err)
	}
	body, err := decode(status, raw)
	if err != nil {
		if errors.Is(err, messaging.ErrGenerationMismatch) {
			if l.replacementOutcome(ctx, route.generation) == messaging.OutcomeLost {
				l.dropAsk(key)
				return messaging.WaitResult{Outcome: messaging.OutcomeLost}, nil
			}
			// The parent refused the pinned generation and the ask's fate is
			// unknown, so the wait reports the refusal the withdrawal reports
			// for the same state.
			return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, err
		}
		return messaging.WaitResult{Outcome: body.outcomeOf()}, err
	}
	switch body.Outcome {
	case messaging.OutcomeReplied:
		// A completed edge is consumed, so a later wait finds nothing here, the
		// same way the local store reports a consumed reply.
		l.dropAsk(key)
		return messaging.WaitResult{Outcome: messaging.OutcomeReplied, Reply: &messaging.InboundMessage{
			MessageID: body.MessageID,
			FromRunID: body.FromRunID,
			Sender:    body.Sender,
			Kind:      body.Kind,
			Payload:   body.Payload,
		}}, nil
	case messaging.OutcomeTimedOut, messaging.OutcomeWithdrawn, messaging.OutcomeLost, messaging.OutcomeEdgeAbsent:
		// The parent keeps the terminal fence, so the route stays and a repeat
		// wait reads the same verdict instead of a locally invented one.
		return messaging.WaitResult{Outcome: body.Outcome}, nil
	}
	return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending},
		fmt.Errorf("%w: parent broker answered without a wait outcome", ErrUnsupportedParentProtocol)
}

// Withdraw retracts the edge a forwarded ask was admitted on. It is best
// effort: an ask the parent already relayed into the next queue is not
// recalled.
func (l *Link) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	key := askKey{Sender: req.Sender.RunID, MessageID: req.MessageID}
	route, ok := l.askRoute(req.Sender.RunID, req.MessageID)
	if !ok {
		return messaging.WithdrawResult{Outcome: messaging.OutcomeEdgeAbsent}, nil
	}
	token, _, err := l.credentials(ctx, req.Sender.RunID)
	if err != nil {
		return messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending}, err
	}
	status, raw, err := l.post(ctx, pathWithdraw, map[string]any{
		"cancel_message_id": req.MessageID,
		"generation":        route.generation,
	}, req.Sender.RunID, token, 0)
	if err != nil {
		return messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending},
			fmt.Errorf("%w: %v", ErrParentUnavailable, err)
	}
	body, err := decode(status, raw)
	if err != nil {
		if errors.Is(err, messaging.ErrGenerationMismatch) {
			if l.replacementOutcome(ctx, route.generation) == messaging.OutcomeLost {
				l.dropAsk(key)
				return messaging.WithdrawResult{Outcome: messaging.OutcomeLost}, nil
			}
			return messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending}, err
		}
		return messaging.WithdrawResult{Outcome: body.outcomeOf(), Fence: body.fenceAck()}, err
	}
	// A response that carries no withdrawal verdict describes no result, so
	// the withdrawal reports the protocol mismatch its sibling operations
	// report instead of an outcome outside the outcome set.
	if !messaging.IsWithdrawOutcome(body.Outcome) {
		return messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending},
			fmt.Errorf("%w: parent broker answered with the outcome %q, which is not a withdrawal verdict",
				ErrUnsupportedParentProtocol, body.Outcome)
	}
	// The route stays: the parent keeps the terminal fence, so a repeat wait
	// reads the same verdict, exactly as a local withdrawal's fence does.
	return messaging.WithdrawResult{Outcome: body.Outcome, Fence: body.fenceAck()}, nil
}

// Reply sends a reply up the hop a relayed ask arrived on, so an asker in
// another branch sees its own edge complete instead of a second delivery.
func (l *Link) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.Outcome, error) {
	route, ok := l.relayRoute(req.MessageID)
	if !ok || route.askerRunID != req.ToRunID {
		return messaging.OutcomeEdgeAbsent, nil
	}
	// The reply names the generation its edge was admitted under. A parent that
	// was replaced refuses it, and nothing is replayed onto the replacement.
	if current, pinned := l.pinnedGeneration(); pinned && current != route.edgeGeneration {
		l.dropRelay(req.MessageID)
		return messaging.OutcomeLost, nil
	}
	token, _, err := l.credentials(ctx, req.From.RunID)
	if err != nil {
		return messaging.OutcomeNotSent, err
	}
	payload, err := payloadFor(req.Body)
	if err != nil {
		return messaging.OutcomeNotSent, err
	}
	status, raw, err := l.post(ctx, pathSend, map[string]any{
		"from_run_id": req.From.RunID,
		"to_run_id":   req.ToRunID,
		"type":        "agent_message",
		"payload":     payload,
		"reply_to":    req.MessageID,
		"generation":  route.edgeGeneration,
	}, req.From.RunID, token, 0)
	if err != nil {
		return messaging.OutcomePossiblyPending, fmt.Errorf("%w: %v", ErrParentUnavailable, err)
	}
	body, err := decode(status, raw)
	if err != nil {
		if errors.Is(err, messaging.ErrGenerationMismatch) {
			l.dropRelay(req.MessageID)
			return messaging.OutcomeLost, err
		}
		return body.outcomeOf(), err
	}
	l.dropRelay(req.MessageID)
	if body.Outcome == messaging.OutcomeReplied || body.Outcome == messaging.OutcomeEdgeAbsent {
		return body.Outcome, nil
	}
	return messaging.OutcomePossiblyPending,
		fmt.Errorf("%w: parent broker answered without a reply outcome", ErrUnsupportedParentProtocol)
}

// Deliver forwards a tell to the parent broker, which hands it to the branch
// that owns the target.
func (l *Link) Deliver(ctx context.Context, req messaging.DeliverRequest) error {
	token, generation, err := l.credentials(ctx, req.FromRunID)
	if err != nil {
		return err
	}
	payload, err := payloadFor(string(req.Payload))
	if err != nil {
		return err
	}
	status, raw, err := l.post(ctx, pathSend, map[string]any{
		"from_run_id": req.FromRunID,
		"to_run_id":   req.ToRunID,
		"type":        req.Kind,
		"payload":     payload,
		"generation":  generation,
	}, req.FromRunID, token, 0)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrParentUnavailable, err)
	}
	_, err = decode(status, raw)
	return err
}

// Peers lists the runs the parent broker exports to this branch. A parent that
// cannot finish the listing reports the error beside the peers it did return, so
// a partial list never reads as a complete one. Before the first successful
// registration the link syncs once, so a listing is never answered with a
// silent empty list just because no claim was published yet.
func (l *Link) Peers(ctx context.Context) ([]messaging.Peer, error) {
	caller, _ := messaging.CallerRun(ctx)
	runID, token, ok := l.listingCredentials(caller)
	if !ok {
		// The parent has not accepted this branch's claim yet, so it knows no run
		// of this branch to list with. Sync once and re-pick before giving up:
		// an empty answer would read as "the parent has no peers" when the claim
		// was simply never published.
		if err := l.Sync(ctx); err != nil {
			return nil, err
		}
		runID, token, ok = l.listingCredentials(caller)
		if !ok {
			return nil, fmt.Errorf("%w: this branch has no run registered at the parent broker to list peers with", messaging.ErrTargetUnavailable)
		}
	}
	req, err := http.NewRequest(http.MethodGet, l.opts.ParentURL+pathSessions, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set(messaging.HeaderRunID, runID)
	req.Header.Set(messaging.HeaderRunToken, token)
	if l.opts.Token != "" {
		req.Header.Set("Authorization", "Bearer "+l.opts.Token)
	}
	status, raw, err := l.client.Do(ctx, req, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParentUnavailable, err)
	}
	body, err := decode(status, raw)
	if err != nil {
		return nil, err
	}
	peers := make([]messaging.Peer, 0, len(body.Sessions))
	for _, record := range body.Sessions {
		var session struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(record, &session); err != nil {
			continue
		}
		peers = append(peers, messaging.Peer{RunID: session.RunID, Session: record})
	}
	if body.PeersError != "" {
		return peers, fmt.Errorf("%w: parent peer listing is incomplete: %s", messaging.ErrTargetUnavailable, body.PeersError)
	}
	return peers, nil
}

// RecordRelay binds the reply route of a relayed ask. The broker records it
// before it exposes the ask to the run that owns it, so the reply can never
// outrun its route.
func (l *Link) RecordRelay(route messaging.RelayRoute) error {
	if route.MessageID == "" || route.AskerRunID == "" {
		return fmt.Errorf("%w: a relayed ask needs a message id and its asker", messaging.ErrInvalidRequest)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return messaging.ErrTransportClosed
	}
	now := time.Now()
	l.pruneLocked(now)
	l.relays[route.MessageID] = relayRoute{
		askerRunID:     route.AskerRunID,
		edgeGeneration: route.EdgeGeneration,
		recordedAt:     now,
	}
	return nil
}

// askRoute reports the hop an ask this branch forwarded was admitted on.
func (l *Link) askRoute(senderRunID, messageID string) (askRoute, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	route, ok := l.asks[askKey{Sender: senderRunID, MessageID: messageID}]
	return route, ok
}

// relayRoute reports the reply route recorded for a relayed ask.
func (l *Link) relayRoute(messageID string) (relayRoute, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	route, ok := l.relays[messageID]
	return route, ok
}

func (l *Link) recordAsk(senderRunID, messageID string, route askRoute) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.pruneLocked(now)
	route.recordedAt = now
	l.asks[askKey{Sender: senderRunID, MessageID: messageID}] = route
}

func (l *Link) dropAsk(key askKey) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.asks, key)
}

func (l *Link) dropRelay(messageID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.relays, messageID)
}

// pruneLocked drops route records older than the messaging wait horizon: past it
// no edge or fence can still be live, so no operation can reach them again.
func (l *Link) pruneLocked(now time.Time) {
	horizon := now.Add(-messaging.MaxWait)
	for key, route := range l.asks {
		if route.recordedAt.Before(horizon) {
			delete(l.asks, key)
		}
	}
	for messageID, route := range l.relays {
		if route.recordedAt.Before(horizon) {
			delete(l.relays, messageID)
		}
	}
}

// pinnedGeneration reports the generation the parent broker last answered with.
func (l *Link) pinnedGeneration() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.generation, l.generation != ""
}

// replacementOutcome reports what a rejected generation proves: a parent whose
// generation changed is a replacement, so no edge from the pinned generation can
// exist and nothing is replayed. An unchanged generation leaves the ask
// uncertain.
func (l *Link) replacementOutcome(ctx context.Context, pinned string) messaging.Outcome {
	if pinned == "" {
		return messaging.OutcomePossiblyPending
	}
	if err := l.Sync(ctx); err != nil {
		return messaging.OutcomePossiblyPending
	}
	if current, _ := l.pinnedGeneration(); current != pinned {
		return messaging.OutcomeLost
	}
	return messaging.OutcomePossiblyPending
}

// credentials reports the credential the parent issued for runID and the
// generation the parent pinned, re-registering this branch once when the run is
// not known there yet.
func (l *Link) credentials(ctx context.Context, runID string) (string, string, error) {
	generation, token, ok := l.lookup(runID)
	if ok && generation != "" {
		return token, generation, nil
	}
	if err := l.Sync(ctx); err != nil {
		return "", "", err
	}
	generation, token, ok = l.lookup(runID)
	if !ok || generation == "" {
		return "", "", fmt.Errorf("%w: run %q is not registered at the parent broker", messaging.ErrTargetUnavailable, runID)
	}
	return token, generation, nil
}

func (l *Link) lookup(runID string) (string, string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	token, ok := l.tokens[runID]
	return l.generation, token, ok
}

// listingCredentials picks the run whose credential lists peers: the caller when
// the parent knows it, else the first run in a stable order so two listings of
// the same branch read the same.
func (l *Link) listingCredentials(caller string) (string, string, bool) {
	l.mu.Lock()
	tokens := make(map[string]string, len(l.tokens))
	for runID, token := range l.tokens {
		tokens[runID] = token
	}
	l.mu.Unlock()
	if token, ok := tokens[caller]; ok {
		return caller, token, true
	}
	runs := make([]string, 0, len(tokens))
	for runID := range tokens {
		runs = append(runs, runID)
	}
	if len(runs) == 0 {
		return "", "", false
	}
	sort.Strings(runs)
	return runs[0], tokens[runs[0]], true
}

// post sends one authenticated parent request. A run-scoped request carries the
// credential the parent issued for that run; a parent-link request carries the
// link's bearer token instead.
func (l *Link) post(ctx context.Context, path string, payload map[string]any, runID, token string, timeout time.Duration) (int, []byte, error) {
	request := make(map[string]any, len(payload)+2)
	for key, value := range payload {
		request[key] = value
	}
	if runID != "" {
		request["run_id"] = runID
		request["token"] = token
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return 0, nil, fmt.Errorf("%w: %v", messaging.ErrInvalidRequest, err)
	}
	req, err := http.NewRequest(http.MethodPost, l.opts.ParentURL+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if l.opts.Token != "" {
		req.Header.Set("Authorization", "Bearer "+l.opts.Token)
	}
	return l.client.Do(ctx, req, timeout)
}

// payloadFor renders a message body on the wire. An empty body is no payload
// at all, which keeps the stored payload valid JSON, and a body that is not
// one JSON value is refused here: the request would not render, and the fault
// belongs to the caller rather than to the parent's reachability.
func payloadFor(body string) (json.RawMessage, error) {
	payload := json.RawMessage(body)
	if len(bytes.TrimSpace(payload)) == 0 {
		return json.RawMessage("{}"), nil
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("%w: the message body is not one JSON value", messaging.ErrInvalidRequest)
	}
	return payload, nil
}
