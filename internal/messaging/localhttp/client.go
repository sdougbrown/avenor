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
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/internal/httpbase"
)

// Local router paths the adapter drives.
const (
	pathSessions = "/sessions"
	pathSend     = "/send"
	pathWait     = "/wait_reply"
	pathWithdraw = "/cancel_message"
	pathPoll     = "/poll-control"
	pathRename   = "/rename"
	pathLast     = "/last_message"
)

// ErrUnsupportedLocalProtocol reports a local broker that does not answer the
// shared messaging protocol: it predates the protocol marker, or it answered a
// messaging request without the envelope that describes the result.
var ErrUnsupportedLocalProtocol = fmt.Errorf(
	"%w: local broker does not speak the shared messaging protocol; restart the supervisor to pick up the new local protocol",
	messaging.ErrUnsupportedProtocol,
)

// Transport is the local HTTP messaging transport.
type Transport struct {
	*httpbase.Client

	creds   messaging.LocalCredentials
	baseURL string

	mu         sync.Mutex
	protocol   int
	generation string
}

// New builds a transport for one caller's existing local run credentials. The
// address names the local broker; creds.BaseURL, when set, is used instead.
func New(addr string, creds messaging.LocalCredentials) (*Transport, error) {
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
		creds.Timeout = httpbase.DefaultRequestTimeout
	}
	return &Transport{
		Client:  httpbase.New(ErrUnsupportedLocalProtocol, creds.Timeout),
		creds:   creds,
		baseURL: base,
	}, nil
}

func (t *Transport) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	fail := func(outcome messaging.Outcome, err error) (messaging.SendResult, error) {
		return messaging.SendResult{Outcome: outcome, MessageID: req.MessageID, Generation: req.Generation}, err
	}
	if err := t.Open(req.Generation); err != nil {
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
		"expects_reply": !req.NoReply,
		"sender":        req.From.Sender,
		"payload":       payload,
	})
	if err != nil {
		if httpbase.NeverTransmitted(err) {
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
		outcome := body.unenvelopedOutcome()
		if !messaging.IsSendOutcome(outcome) {
			outcome = messaging.OutcomeNotSent
		}
		return fail(outcome, body.unsolicited())
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
	if err := t.Open(req.Generation); err != nil {
		return messaging.WaitResult{Outcome: messaging.OutcomeNotSent}, err
	}
	timeout := httpbase.WaitTimeout(req.Timeout)
	status, raw, err := t.roundTrip(ctx, http.MethodPost, pathWait, timeout+messaging.CleanupDeadline, map[string]any{
		"waiting_for": req.MessageID,
		"generation":  req.Generation,
		"timeout_ms":  timeout.Milliseconds(),
	})
	if err != nil {
		if t.IsClosed() && httpbase.IsTransportCancelled(err) {
			return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, nil
		}
		// A connection refused before any byte was written never carried the wait.
		if httpbase.NeverTransmitted(err) {
			return messaging.WaitResult{Outcome: messaging.OutcomeNotSent}, err
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
	if t.IsClosed() {
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
		if httpbase.NeverTransmitted(err) {
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
	if err := t.Open(req.Generation); err != nil {
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
	if err := t.Open(req.Generation); err != nil {
		return messaging.ReceiveResult{}, err
	}
	// agent_only leaves non-agent control messages queued for the consumers
	// that act on them, and limit leaves the excess queued for the next poll.
	status, raw, err := t.roundTrip(ctx, http.MethodPost, pathPoll, req.Timeout, map[string]any{
		"agent_only": true,
		"limit":      messaging.ReceiveLimit(req.Limit),
	})
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
	if err := t.Open(req.Generation); err != nil {
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
	if t.IsClosed() {
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
	target := t.baseURL + path
	var body io.Reader
	if method != http.MethodGet {
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
	req, err := http.NewRequest(method, target, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if method == http.MethodGet {
		req.Header.Set(messaging.HeaderRunID, t.creds.RunID)
		req.Header.Set(messaging.HeaderRunToken, t.creds.Token)
	}
	return t.Do(ctx, req, timeout)
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
	Fence             *httpbase.Fence   `json:"fence"`
}

type controlEntry struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	FromRunID string          `json:"from_run_id"`
	Sender    string          `json:"sender"`
	Payload   json.RawMessage `json:"payload"`
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
	return e.Fence.Ack()
}

// statusError classifies a response the messaging envelope does not describe.
// The local router's auth layer answers before a messaging handler, so only a
// 401 there is a credential rejection; a 403 is the router's not-ask-target
// refusal, and any other unenveloped failure is a protocol mismatch.
func statusError(e envelope) error {
	if e.Status == http.StatusOK {
		return nil
	}
	if e.Status == http.StatusUnauthorized {
		return fmt.Errorf("%w: local broker rejected the run token", messaging.ErrAuthRejected)
	}
	if e.Error != "" {
		return fmt.Errorf("%w: local broker answered %d: %s", ErrUnsupportedLocalProtocol, e.Status, e.Error)
	}
	return fmt.Errorf("%w: local broker answered %d without a messaging result", ErrUnsupportedLocalProtocol, e.Status)
}

// unenvelopedOutcome reports the outcome for a response the messaging envelope
// does not describe. Only a 401 is a credential rejection; a code-less 403 is
// the old router's not-ask-target refusal, whose documented outcome the shared
// table carries. Every other unenveloped failure leaves transmission uncertain.
func (e envelope) unenvelopedOutcome() messaging.Outcome {
	switch e.Status {
	case http.StatusUnauthorized:
		return messaging.OutcomeAuthRejected
	case http.StatusForbidden:
		return messaging.WireOutcomeFor("not-ask-target")
	}
	return messaging.OutcomePossiblyPending
}

func (e envelope) unknownCode() error {
	return fmt.Errorf("%w: local broker reported unknown code %q", ErrUnsupportedLocalProtocol, e.Code)
}

func (e envelope) unsolicited() error {
	// An unenveloped 403 is the old router's not-ask-target refusal, so it
	// carries the typed error its coded form would.
	if e.Status == http.StatusForbidden {
		return fmt.Errorf("%w: %s", messaging.ErrNotAskTarget, e.Error)
	}
	if e.Error != "" {
		return fmt.Errorf("%w: local broker answered %d: %s", ErrUnsupportedLocalProtocol, e.Status, e.Error)
	}
	return fmt.Errorf("%w: local broker answered %d without an outcome", ErrUnsupportedLocalProtocol, e.Status)
}

var _ messaging.Transport = (*Transport)(nil)

// BrokerError is a non-2xx answer from one of the broker's authenticated
// routes: the HTTP status, the wire code when the body carried one, and the
// human text. It lets a child surface map the broker's decision to the router's
// typed tool errors without re-implementing the request path.
type BrokerError struct {
	Status  int
	Code    string
	Message string
}

func (e *BrokerError) Error() string { return e.Message }

// RenameSelf renames one run through the broker's authenticated /rename route.
// sessionID is forwarded verbatim (empty means the caller's own run), so the
// supervisor's rename authority decides every rule. The request passes the same
// protocol gate as the ask verbs, so a broker that does not speak the shared
// protocol refuses the rename instead of accepting it and refusing the next
// send.
func (t *Transport) RenameSelf(ctx context.Context, sessionID, name string) (messaging.RenameResult, error) {
	if t.IsClosed() {
		return messaging.RenameResult{}, messaging.ErrTransportClosed
	}
	if _, err := t.ensureProtocol(ctx); err != nil {
		return messaging.RenameResult{}, err
	}
	payload := map[string]any{"name": name}
	if sessionID != "" {
		payload["session_id"] = sessionID
	}
	var result struct {
		RunID         string `json:"run_id"`
		Label         string `json:"label"`
		PreviousLabel string `json:"previous_label"`
	}
	if err := t.getBrokerJSON(ctx, http.MethodPost, pathRename, "", payload, &result); err != nil {
		return messaging.RenameResult{}, err
	}
	return messaging.RenameResult{
		RunID:         result.RunID,
		Label:         result.Label,
		PreviousLabel: result.PreviousLabel,
	}, nil
}

// LastMessage reads one target's recorded last turn through the broker's
// universally visible /last_message route. It takes the same protocol gate as
// the ask verbs.
func (t *Transport) LastMessage(ctx context.Context, target string) (messaging.PeerLast, error) {
	if t.IsClosed() {
		return messaging.PeerLast{}, messaging.ErrTransportClosed
	}
	if _, err := t.ensureProtocol(ctx); err != nil {
		return messaging.PeerLast{}, err
	}
	var result struct {
		RunID       string `json:"run_id"`
		Label       string `json:"label"`
		HasRecord   bool   `json:"has_record"`
		FinalOutput string `json:"final_output"`
		StopReason  string `json:"stop_reason"`
		Truncated   bool   `json:"final_output_truncated"`
	}
	query := "?" + url.Values{"target": {target}}.Encode()
	if err := t.getBrokerJSON(ctx, http.MethodGet, pathLast, query, nil, &result); err != nil {
		return messaging.PeerLast{}, err
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

// getBrokerJSON performs one authenticated request on the shared request path
// and decodes a successful body. A non-2xx answer is a *BrokerError carrying
// the wire code; anything unusable is a protocol mismatch.
func (t *Transport) getBrokerJSON(ctx context.Context, method, path, query string, payload map[string]any, out any) error {
	status, raw, err := t.roundTrip(ctx, method, path+query, 0, payload)
	if err != nil {
		return err
	}
	trimmed := bytes.TrimSpace(raw)
	if status < 200 || status >= 300 {
		brokerErr := &BrokerError{Status: status}
		if len(trimmed) > 0 && trimmed[0] == '{' {
			var body struct {
				Error string `json:"error"`
				Code  string `json:"code"`
			}
			_ = json.Unmarshal(trimmed, &body)
			brokerErr.Code = body.Code
			brokerErr.Message = body.Error
		}
		if brokerErr.Message == "" {
			brokerErr.Message = string(trimmed)
		}
		if status == http.StatusUnauthorized {
			brokerErr.Message = "local broker rejected the run token"
			brokerErr.Code = "auth-rejected"
		}
		return brokerErr
	}
	if out == nil {
		return nil
	}
	if len(trimmed) == 0 {
		return nil
	}
	if err := json.Unmarshal(trimmed, out); err != nil {
		return fmt.Errorf("%w: %v", ErrUnsupportedLocalProtocol, err)
	}
	return nil
}
