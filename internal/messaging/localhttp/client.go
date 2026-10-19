// Package localhttp implements the messaging Transport over the existing local
// broker router. It authenticates with the caller's own run credentials,
// negotiates the local messaging protocol through the authenticated /sessions
// endpoint and reports facts to the shared ask client. It never spawns, prompts,
// reads a host endpoint or looks up ambient credentials.
package localhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

const (
	// defaultRequestTimeout bounds one request when the caller sets none.
	defaultRequestTimeout = 30 * time.Second

	maxResponseBytes = 1 << 20
)

// Local router paths the adapter drives.
const (
	pathSessions = "/sessions"
	pathSend     = "/send"
	pathWait     = "/wait_reply"
	pathWithdraw = "/cancel_message"
	pathPoll     = "/poll-control"
)

// ErrUnsupportedLocalProtocol reports a local broker that does not answer the
// shared messaging protocol: it predates the protocol marker, or it answered a
// messaging request without the envelope that describes the result.
var ErrUnsupportedLocalProtocol = fmt.Errorf(
	"%w: local broker does not speak the shared messaging protocol; restart the supervisor to pick up the new local protocol",
	messaging.ErrUnsupportedProtocol,
)

// ErrRedirect reports a response that would redirect the request. The adapter
// never follows one, so a run token cannot be offered to another host.
var ErrRedirect = errors.New("localhttp: redirect refused")

// Transport is the local HTTP messaging transport.
type Transport struct {
	creds   messaging.LocalCredentials
	baseURL string
	client  *http.Client

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu         sync.Mutex
	closed     bool
	protocol   int
	generation string
}

// New builds a transport for one caller's existing local run credentials. The
// address names the local broker; creds.BaseURL, when set, is used instead.
func New(addr string, creds messaging.LocalCredentials) (messaging.Transport, error) {
	base := strings.TrimSuffix(creds.BaseURL, "/")
	if base == "" {
		if addr == "" {
			return nil, fmt.Errorf("%w: local broker address is required", messaging.ErrInvalidRequest)
		}
		base = "http://" + addr
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("%w: local broker address %q is not an http(s) URL", messaging.ErrInvalidRequest, base)
	}
	if creds.RunID == "" || creds.Token == "" {
		return nil, fmt.Errorf("%w: local run credentials are required", messaging.ErrAuthRejected)
	}
	if creds.Timeout <= 0 {
		creds.Timeout = defaultRequestTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{
		creds:   creds,
		baseURL: base,
		client: &http.Client{
			Transport:     &http.Transport{},
			CheckRedirect: refuseRedirect,
		},
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func refuseRedirect(req *http.Request, _ []*http.Request) error {
	return fmt.Errorf("%w: %s", ErrRedirect, req.URL.Redacted())
}

func (t *Transport) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	fail := func(outcome messaging.Outcome, err error) (messaging.SendResult, error) {
		return messaging.SendResult{Outcome: outcome, MessageID: req.MessageID, Generation: req.Generation}, err
	}
	if err := t.open(req.Generation); err != nil {
		return fail(messaging.OutcomeNotSent, err)
	}
	// A broker that predates the protocol marker never received this ask, so
	// the failure classifies as not-sent and nothing is retried.
	if _, err := t.ensureProtocol(ctx); err != nil {
		outcome := messaging.OutcomeNotSent
		if errors.Is(err, messaging.ErrAuthRejected) {
			outcome = messaging.OutcomeAuthRejected
		}
		return fail(outcome, err)
	}
	payload := json.RawMessage(req.Body)
	if len(bytes.TrimSpace(payload)) == 0 {
		payload = json.RawMessage("{}")
	}
	if !json.Valid(payload) {
		return fail(messaging.OutcomeNotSent, fmt.Errorf("%w: ask body is not JSON", messaging.ErrInvalidRequest))
	}
	status, raw, err := t.roundTrip(ctx, http.MethodPost, pathSend, 0, map[string]any{
		"from_run_id":   req.From.RunID,
		"to_run_id":     req.To.RunID,
		"type":          "agent_message",
		"generation":    req.Generation,
		"message_id":    req.MessageID,
		"expects_reply": true,
		"sender":        req.From.Sender,
		"payload":       payload,
	})
	if err != nil {
		if neverTransmitted(err) {
			return fail(messaging.OutcomeNotSent, err)
		}
		return fail(messaging.OutcomePossiblyPending, err)
	}
	body, err := envelopeOf(status, raw)
	if err != nil {
		return fail(messaging.OutcomePossiblyPending, err)
	}
	if body.Code != "" {
		typed := messaging.WireErrorFor(body.Code)
		if typed == nil {
			return fail(messaging.OutcomePossiblyPending, body.unknownCode())
		}
		if errors.Is(typed, messaging.ErrGenerationMismatch) {
			return fail(t.generationOutcome(ctx, req.Generation), typed)
		}
		return fail(messaging.SendOutcomeFor(body.Code), typed)
	}
	if body.Outcome == "" {
		return fail(body.unenvelopedOutcome(), body.unsolicited())
	}
	if !messaging.IsSendOutcome(body.Outcome) {
		return fail(messaging.OutcomePossiblyPending, body.unsolicited())
	}
	if body.Outcome != messaging.OutcomeSent && body.Outcome != messaging.OutcomeWithdrawn {
		return fail(body.Outcome, nil)
	}
	messageID := body.MessageID
	if messageID == "" {
		messageID = req.MessageID
	}
	return messaging.SendResult{Outcome: body.Outcome, MessageID: messageID, Generation: req.Generation}, nil
}

func (t *Transport) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.WaitResult{Outcome: messaging.OutcomeNotSent}, err
	}
	timeout := req.Timeout
	if timeout <= 0 || timeout > messaging.MaxWait {
		timeout = messaging.MaxWait
	}
	status, raw, err := t.roundTrip(ctx, http.MethodPost, pathWait, timeout+messaging.CleanupDeadline, map[string]any{
		"waiting_for": req.MessageID,
		"generation":  req.Generation,
		"timeout_ms":  timeout.Milliseconds(),
	})
	if err != nil {
		if t.isClosed() && transportCancelled(err) {
			return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, nil
		}
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, err
	}
	body, err := envelopeOf(status, raw)
	if err != nil {
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, err
	}
	if body.Code != "" {
		typed := messaging.WireErrorFor(body.Code)
		if typed == nil {
			return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, body.unknownCode()
		}
		outcome := body.Outcome
		if errors.Is(typed, messaging.ErrGenerationMismatch) {
			outcome = t.generationOutcome(ctx, req.Generation)
		}
		if outcome == "" {
			outcome = messaging.WireOutcomeFor(body.Code)
		}
		return messaging.WaitResult{Outcome: outcome}, typed
	}
	if body.Outcome == "" {
		return messaging.WaitResult{Outcome: body.unenvelopedOutcome()}, body.unsolicited()
	}
	result := messaging.WaitResult{Outcome: body.Outcome}
	if body.Outcome == messaging.OutcomeReplied {
		result.Reply = &messaging.InboundMessage{
			MessageID: body.MessageID,
			FromRunID: body.FromRunID,
			Sender:    body.Sender,
			Kind:      body.Kind,
			Payload:   body.Payload,
		}
	}
	return result, nil
}

func (t *Transport) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	fail := func(outcome messaging.Outcome, err error) (messaging.SendResult, error) {
		return messaging.SendResult{Outcome: outcome, MessageID: req.MessageID}, err
	}
	if t.isClosed() {
		return fail(messaging.OutcomeNotSent, messaging.ErrTransportClosed)
	}
	payload := json.RawMessage(req.Body)
	if len(bytes.TrimSpace(payload)) == 0 {
		payload = json.RawMessage("{}")
	}
	if !json.Valid(payload) {
		return fail(messaging.OutcomeNotSent, fmt.Errorf("%w: reply body is not JSON", messaging.ErrInvalidRequest))
	}
	status, raw, err := t.roundTrip(ctx, http.MethodPost, pathSend, 0, map[string]any{
		"from_run_id": req.From.RunID,
		"to_run_id":   req.ToRunID,
		"type":        "agent_message",
		"reply_to":    req.MessageID,
		"sender":      req.From.Sender,
		"payload":     payload,
	})
	if err != nil {
		if neverTransmitted(err) {
			return fail(messaging.OutcomeNotSent, err)
		}
		return fail(messaging.OutcomePossiblyPending, err)
	}
	body, err := envelopeOf(status, raw)
	if err != nil {
		return fail(messaging.OutcomePossiblyPending, err)
	}
	if body.Code != "" {
		typed := messaging.WireErrorFor(body.Code)
		if typed == nil {
			return fail(messaging.OutcomePossiblyPending, body.unknownCode())
		}
		outcome := body.Outcome
		if outcome == "" {
			outcome = messaging.WireOutcomeFor(body.Code)
		}
		return fail(outcome, typed)
	}
	switch body.Outcome {
	case messaging.OutcomeReplied, messaging.OutcomeEdgeAbsent:
		return messaging.SendResult{Outcome: body.Outcome, MessageID: req.MessageID}, nil
	}
	if body.Outcome == "" {
		return fail(body.unenvelopedOutcome(), body.unsolicited())
	}
	if !messaging.IsSendOutcome(body.Outcome) {
		return fail(messaging.OutcomePossiblyPending, body.unsolicited())
	}
	return fail(body.Outcome, nil)
}

func (t *Transport) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.WithdrawResult{Outcome: messaging.OutcomeNotSent}, err
	}
	status, raw, err := t.roundTrip(ctx, http.MethodPost, pathWithdraw, 0, map[string]any{
		"cancel_message_id": req.MessageID,
		"generation":        req.Generation,
	})
	if err != nil {
		return messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending}, err
	}
	body, err := envelopeOf(status, raw)
	if err != nil {
		return messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending}, err
	}
	if body.Code != "" {
		typed := messaging.WireErrorFor(body.Code)
		if typed == nil {
			return messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending}, body.unknownCode()
		}
		outcome := body.Outcome
		if errors.Is(typed, messaging.ErrGenerationMismatch) {
			outcome = t.generationOutcome(ctx, req.Generation)
		}
		if outcome == "" {
			outcome = messaging.WireOutcomeFor(body.Code)
		}
		return messaging.WithdrawResult{Outcome: outcome, Fence: body.fenceAck()}, typed
	}
	if body.Outcome == "" {
		return messaging.WithdrawResult{Outcome: body.unenvelopedOutcome()}, body.unsolicited()
	}
	return messaging.WithdrawResult{Outcome: body.Outcome, Fence: body.fenceAck()}, nil
}

func (t *Transport) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.ReceiveResult{}, err
	}
	// agent_only leaves non-agent control messages queued for the consumers
	// that act on them.
	status, raw, err := t.roundTrip(ctx, http.MethodPost, pathPoll, 0, map[string]any{"agent_only": true})
	if err != nil {
		return messaging.ReceiveResult{}, err
	}
	if status != http.StatusOK {
		return messaging.ReceiveResult{}, statusError(envelope{Status: status})
	}
	var entries []controlEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return messaging.ReceiveResult{}, fmt.Errorf("%w: poll response: %v", ErrUnsupportedLocalProtocol, err)
	}
	result := messaging.ReceiveResult{Generation: req.Generation}
	for _, entry := range entries {
		sender := entry.Sender
		if sender == "" {
			sender = entry.FromRunID
		}
		result.Messages = append(result.Messages, messaging.InboundMessage{
			MessageID: entry.ID,
			FromRunID: entry.FromRunID,
			Sender:    sender,
			Kind:      entry.Type,
			Payload:   entry.Payload,
		})
	}
	return result, nil
}

func (t *Transport) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.PeersResult{}, err
	}
	body, err := t.sessions(ctx)
	if err != nil {
		return messaging.PeersResult{}, err
	}
	result := messaging.PeersResult{Generation: body.Generation}
	for _, raw := range body.Sessions {
		var session struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(raw, &session); err != nil {
			continue
		}
		result.Peers = append(result.Peers, messaging.Peer{RunID: session.RunID, Session: raw})
	}
	return result, nil
}

// Preflight reports the broker's messaging protocol and current generation.
// Protocol 0 means the local broker is too old to speak the shared protocol.
func (t *Transport) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
	if t.isClosed() {
		return messaging.PreflightResult{}, messaging.ErrTransportClosed
	}
	body, err := t.sessions(ctx)
	if err != nil {
		return messaging.PreflightResult{}, err
	}
	result := messaging.PreflightResult{Protocol: body.MessagingProtocol, Generation: body.Generation}
	if result.Protocol != messaging.ProtocolVersion {
		return result, ErrUnsupportedLocalProtocol
	}
	t.rememberProtocol(result)
	return result, nil
}

// Close releases client-side resources, unblocks in-flight waits with a
// possibly-pending outcome and joins its goroutines. It never stops the broker.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()
	t.mu.Unlock()
	t.wg.Wait()
	t.client.CloseIdleConnections()
	return nil
}

func (t *Transport) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *Transport) open(generation string) error {
	if t.isClosed() {
		return messaging.ErrTransportClosed
	}
	if generation == "" {
		return messaging.ErrGenerationMismatch
	}
	return nil
}

// ensureProtocol negotiates the protocol marker once per connection, before the
// first ask is transmitted.
func (t *Transport) ensureProtocol(ctx context.Context) (string, error) {
	t.mu.Lock()
	if t.protocol == messaging.ProtocolVersion {
		generation := t.generation
		t.mu.Unlock()
		return generation, nil
	}
	t.mu.Unlock()
	result, err := t.Preflight(ctx)
	if err != nil {
		return "", err
	}
	return result.Generation, nil
}

func (t *Transport) rememberProtocol(result messaging.PreflightResult) {
	t.mu.Lock()
	t.protocol = result.Protocol
	t.generation = result.Generation
	t.mu.Unlock()
}

// generationOutcome applies the local rotation rule to a rejected generation.
// A refresh that reports a different generation proves the broker instance was
// replaced, so no edge from the pinned generation can exist; an unchanged
// generation leaves the ask uncertain. The refresh only updates what the next
// ask pins, and this ask is never re-sent.
func (t *Transport) generationOutcome(ctx context.Context, pinned string) messaging.Outcome {
	result, err := t.Preflight(ctx)
	if err != nil {
		return messaging.OutcomePossiblyPending
	}
	if result.Generation != pinned {
		return messaging.OutcomeLost
	}
	return messaging.OutcomePossiblyPending
}

func (t *Transport) sessions(ctx context.Context) (envelope, error) {
	status, raw, err := t.roundTrip(ctx, http.MethodGet, pathSessions, 0, nil)
	if err != nil {
		return envelope{}, err
	}
	body, err := envelopeOf(status, raw)
	if err != nil {
		return body, err
	}
	if err := statusError(body); err != nil {
		return body, err
	}
	return body, nil
}

// roundTrip performs one authenticated request and returns the raw response.
func (t *Transport) roundTrip(ctx context.Context, method, path string, timeout time.Duration, payload map[string]any) (int, []byte, error) {
	if timeout <= 0 {
		timeout = t.creds.Timeout
	}
	child, release, ok := t.child(ctx, timeout)
	if !ok {
		return 0, nil, messaging.ErrTransportClosed
	}
	defer release()

	target := t.baseURL + path
	var body io.Reader
	if method == http.MethodGet {
		target += "?run_id=" + url.QueryEscape(t.creds.RunID) + "&token=" + url.QueryEscape(t.creds.Token)
	} else {
		request := make(map[string]any, len(payload)+2)
		for key, value := range payload {
			request[key] = value
		}
		request["run_id"] = t.creds.RunID
		request["token"] = t.creds.Token
		raw, err := json.Marshal(request)
		if err != nil {
			return 0, nil, fmt.Errorf("%w: %v", messaging.ErrInvalidRequest, err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(child, method, target, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if len(raw) > maxResponseBytes {
		return resp.StatusCode, nil, fmt.Errorf("%w: response body exceeds %d bytes", ErrUnsupportedLocalProtocol, maxResponseBytes)
	}
	return resp.StatusCode, raw, nil
}

// child returns a request context cancelled by ctx, the request timeout or
// transport shutdown, and registers the watcher so Close can join it.
func (t *Transport) child(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, bool) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, nil, false
	}
	t.wg.Add(1)
	t.mu.Unlock()

	base, stop := context.WithTimeout(ctx, timeout)
	child, cancel := context.WithCancel(base)
	go func() {
		defer t.wg.Done()
		select {
		case <-t.ctx.Done():
			cancel()
		case <-child.Done():
		}
	}()
	return child, func() { cancel(); stop() }, true
}

// neverTransmitted reports whether a request provably never reached the broker
// because the connection could not be established.
func neverTransmitted(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

// transportCancelled reports whether err is the cancellation Close applies to
// in-flight requests rather than a fault the caller or the peer produced.
func transportCancelled(err error) bool {
	return errors.Is(err, messaging.ErrTransportClosed) || errors.Is(err, context.Canceled)
}

// envelope is one local router response. Every field is optional so a response
// carries only the operation's result plus, on failure, its code class.
type envelope struct {
	Status            int               `json:"-"`
	Outcome           messaging.Outcome `json:"outcome"`
	Code              string            `json:"code"`
	Error             string            `json:"error"`
	MessageID         string            `json:"message_id"`
	FromRunID         string            `json:"from_run_id"`
	Sender            string            `json:"sender"`
	Kind              string            `json:"kind"`
	Payload           json.RawMessage   `json:"payload"`
	Generation        string            `json:"generation"`
	MessagingProtocol int               `json:"messaging_protocol"`
	Sessions          []json.RawMessage `json:"sessions"`
	Fence             *wireFence        `json:"fence"`
}

type controlEntry struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	RunID     string          `json:"run_id"`
	FromRunID string          `json:"from_run_id"`
	Sender    string          `json:"sender"`
	Payload   json.RawMessage `json:"payload"`
}

type wireFence struct {
	Sender     string    `json:"sender"`
	MessageID  string    `json:"message_id"`
	Generation string    `json:"generation"`
	Deadline   time.Time `json:"deadline"`
	Terminal   bool      `json:"terminal"`
}

func envelopeOf(status int, raw []byte) (envelope, error) {
	body := envelope{Status: status}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return body, nil
	}
	if err := json.Unmarshal(trimmed, &body); err != nil {
		return envelope{Status: status}, fmt.Errorf("%w: %v", ErrUnsupportedLocalProtocol, err)
	}
	body.Status = status
	return body, nil
}

func (e envelope) fenceAck() messaging.FenceAck {
	if e.Fence == nil {
		return messaging.FenceAck{}
	}
	return messaging.FenceAck{
		Sender:     e.Fence.Sender,
		MessageID:  e.Fence.MessageID,
		Generation: e.Fence.Generation,
		Deadline:   e.Fence.Deadline,
		Terminal:   e.Fence.Terminal,
	}
}

// statusError classifies a response the messaging envelope does not describe.
// The local router's auth layer answers before a messaging handler, so a
// 401/403 there is a credential rejection; any other unenveloped failure is a
// protocol mismatch.
func statusError(e envelope) error {
	if e.Status == http.StatusOK {
		return nil
	}
	switch e.Status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("%w: local broker rejected the run token", messaging.ErrAuthRejected)
	}
	if e.Error != "" {
		return fmt.Errorf("%w: local broker answered %d: %s", ErrUnsupportedLocalProtocol, e.Status, e.Error)
	}
	return fmt.Errorf("%w: local broker answered %d without a messaging result", ErrUnsupportedLocalProtocol, e.Status)
}

// unenvelopedOutcome reports the outcome for a response the messaging envelope
// does not describe. The router's auth layer answers before a handler, so a
// rejected credential is reported as such; every other unenveloped failure
// leaves transmission uncertain.
func (e envelope) unenvelopedOutcome() messaging.Outcome {
	if errors.Is(statusError(e), messaging.ErrAuthRejected) {
		return messaging.OutcomeAuthRejected
	}
	return messaging.OutcomePossiblyPending
}

func (e envelope) unknownCode() error {
	return fmt.Errorf("%w: local broker reported unknown code %q", ErrUnsupportedLocalProtocol, e.Code)
}

func (e envelope) unsolicited() error {
	if e.Error != "" {
		return fmt.Errorf("%w: local broker answered %d: %s", ErrUnsupportedLocalProtocol, e.Status, e.Error)
	}
	return fmt.Errorf("%w: local broker answered %d without an outcome", ErrUnsupportedLocalProtocol, e.Status)
}

var _ messaging.Transport = (*Transport)(nil)
