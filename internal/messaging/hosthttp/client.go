// Package hosthttp implements the v1 host messaging client. It owns its own
// HTTP client, talks only to a caller-supplied endpoint descriptor and reports
// facts to the shared ask client; it never registers, spawns, prompts or reads
// ambient credentials.
package hosthttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/internal/httpbase"
)

// v1 endpoint paths. They are the fixture's design and part of the wire
// contract recorded in testdata/host_wire.json.
const (
	pathPreflight = "/v1/preflight"
	pathSend      = "/v1/send"
	pathWait      = "/v1/wait"
	pathReply     = "/v1/reply"
	pathWithdraw  = "/v1/withdraw"
	pathReceive   = "/v1/receive"
	pathPeers     = "/v1/peers"
)

// ErrMalformedResponse reports a v1 response whose envelope cannot be trusted.
// An ask that may already have been transmitted is classified
// possibly-pending, never not-sent.
var ErrMalformedResponse = errors.New("hosthttp: malformed response")

// Transport is the v1 host messaging transport.
type Transport struct {
	*httpbase.Client

	desc    messaging.EndpointDescriptor
	baseURL string
}

// New validates desc and builds a transport bound to its capability. A
// cleartext endpoint must be loopback so real capability material is never
// offered to an unbound address.
func New(desc messaging.EndpointDescriptor) (messaging.Transport, error) {
	base, err := url.Parse(desc.BaseURL)
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") {
		return nil, fmt.Errorf("%w: endpoint %q is not an http(s) URL", messaging.ErrInvalidRequest, desc.BaseURL)
	}
	if base.Scheme == "http" && !loopback(base.Hostname()) {
		return nil, fmt.Errorf("%w: cleartext endpoint %q is not loopback", messaging.ErrInvalidRequest, desc.BaseURL)
	}
	if desc.CapabilityID == "" || desc.CapabilitySecret == "" {
		return nil, fmt.Errorf("%w: capability credentials are required", messaging.ErrAuthRejected)
	}
	if desc.Timeout <= 0 {
		desc.Timeout = httpbase.DefaultRequestTimeout
	}
	return &Transport{
		Client:  httpbase.New(ErrMalformedResponse, desc.Timeout),
		desc:    desc,
		baseURL: strings.TrimSuffix(desc.BaseURL, "/"),
	}, nil
}

func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (t *Transport) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	fail := func(outcome messaging.Outcome, err error) (messaging.SendResult, error) {
		return messaging.SendResult{Outcome: outcome, MessageID: req.MessageID, Generation: req.Generation}, err
	}
	if err := t.Open(req.Generation); err != nil {
		return fail(messaging.OutcomeNotSent, err)
	}
	body, err := t.call(ctx, pathSend, 0, map[string]any{
		"epoch":       req.Generation,
		"message_id":  req.MessageID,
		"from_run_id": req.From.RunID,
		"from_sender": req.From.Sender,
		"to_run_id":   req.To.RunID,
		"body":        req.Body,
	})
	if err != nil {
		// A connection refused before any byte was written never carried the ask.
		if httpbase.NeverTransmitted(err) {
			return fail(messaging.OutcomeNotSent, err)
		}
		// Otherwise the ask may have reached the peer before the response was lost.
		return fail(messaging.OutcomePossiblyPending, err)
	}
	if body.Outcome == "" {
		return fail(messaging.OutcomePossiblyPending, fmt.Errorf("%w: send response carries no outcome", ErrMalformedResponse))
	}
	if body.Error != "" {
		if messaging.WireErrorFor(body.Error) == nil {
			return fail(messaging.OutcomePossiblyPending, fmt.Errorf("%w: send response carries unknown error %q", ErrMalformedResponse, body.Error))
		}
		// The wire contract assigns exactly one outcome to a code, so a
		// peer-supplied outcome never overrides it.
		return fail(messaging.SendOutcomeFor(body.Error), body.err())
	}
	// A send that loses the admission race to a withdrawal reports the
	// fence's outcome instead of admitting the ask.
	if body.Outcome != messaging.OutcomeSent && body.Outcome != messaging.OutcomeWithdrawn {
		return fail(messaging.OutcomePossiblyPending, fmt.Errorf("%w: send response carries outcome %q", ErrMalformedResponse, body.Outcome))
	}
	messageID := body.MessageID
	if messageID == "" {
		messageID = req.MessageID
	}
	return messaging.SendResult{Outcome: body.Outcome, MessageID: messageID, Generation: req.Generation}, body.err()
}

func (t *Transport) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	if err := t.Open(req.Generation); err != nil {
		return messaging.WaitResult{Outcome: messaging.OutcomeNotSent}, err
	}
	timeout := httpbase.WaitTimeout(req.Timeout)
	body, err := t.call(ctx, pathWait, timeout+messaging.CleanupDeadline, map[string]any{
		"epoch":         req.Generation,
		"sender_run_id": req.Sender.RunID,
		"message_id":    req.MessageID,
		"timeout_ms":    timeout.Milliseconds(),
	})
	if err != nil {
		if t.IsClosed() && httpbase.IsTransportCancelled(err) {
			return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, nil
		}
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, err
	}
	if body.Outcome == "" {
		if err := body.err(); err != nil {
			return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, err
		}
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, fmt.Errorf("%w: wait response carries no outcome", ErrMalformedResponse)
	}
	result := messaging.WaitResult{Outcome: body.Outcome}
	if body.Reply != nil {
		reply := body.Reply.inbound()
		result.Reply = &reply
	}
	return result, body.err()
}

func (t *Transport) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	fail := func(outcome messaging.Outcome, err error) (messaging.SendResult, error) {
		return messaging.SendResult{Outcome: outcome, MessageID: req.MessageID}, err
	}
	if t.IsClosed() {
		return fail(messaging.OutcomeNotSent, messaging.ErrTransportClosed)
	}
	body, err := t.call(ctx, pathReply, 0, map[string]any{
		"from_run_id":    req.From.RunID,
		"to_run_id":      req.ToRunID,
		"message_id":     req.MessageID,
		"correlation_id": req.CorrelationID,
		"body":           req.Body,
	})
	if err != nil {
		if httpbase.NeverTransmitted(err) {
			return fail(messaging.OutcomeNotSent, err)
		}
		return fail(messaging.OutcomePossiblyPending, err)
	}
	if body.Outcome == "" {
		return fail(messaging.OutcomePossiblyPending, fmt.Errorf("%w: reply response carries no outcome", ErrMalformedResponse))
	}
	return messaging.SendResult{Outcome: body.Outcome, MessageID: req.MessageID}, body.err()
}

func (t *Transport) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	if err := t.Open(req.Generation); err != nil {
		return messaging.WithdrawResult{Outcome: messaging.OutcomeNotSent}, err
	}
	body, err := t.call(ctx, pathWithdraw, 0, map[string]any{
		"epoch":         req.Generation,
		"sender_run_id": req.Sender.RunID,
		"message_id":    req.MessageID,
	})
	if err != nil {
		return messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending}, err
	}
	if body.Outcome == "" {
		if err := body.err(); err != nil {
			return messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending}, err
		}
		return messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending}, fmt.Errorf("%w: withdrawal response carries no outcome", ErrMalformedResponse)
	}
	result := messaging.WithdrawResult{Outcome: body.Outcome}
	if body.Fence != nil {
		result.Fence = body.Fence.Ack()
	}
	return result, body.err()
}

func (t *Transport) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	if err := t.Open(req.Generation); err != nil {
		return messaging.ReceiveResult{}, err
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 32
	}
	body, err := t.call(ctx, pathReceive, 0, map[string]any{
		"epoch":         req.Generation,
		"sender_run_id": req.Sender.RunID,
		"limit":         limit,
	})
	if err != nil {
		return messaging.ReceiveResult{}, err
	}
	if body.Error != "" {
		return messaging.ReceiveResult{}, body.err()
	}
	result := messaging.ReceiveResult{Generation: body.Epoch}
	for _, msg := range body.Messages {
		result.Messages = append(result.Messages, msg.inbound())
	}
	return result, nil
}

func (t *Transport) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	if err := t.Open(req.Generation); err != nil {
		return messaging.PeersResult{}, err
	}
	body, err := t.call(ctx, pathPeers, 0, map[string]any{
		"epoch":         req.Generation,
		"sender_run_id": req.Sender.RunID,
	})
	if err != nil {
		return messaging.PeersResult{}, err
	}
	if body.Error != "" {
		return messaging.PeersResult{}, body.err()
	}
	result := messaging.PeersResult{Generation: body.Epoch}
	for _, p := range body.Peers {
		result.Peers = append(result.Peers, messaging.Peer{RunID: p.RunID, Session: p.Session})
	}
	return result, nil
}

func (t *Transport) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
	if t.IsClosed() {
		return messaging.PreflightResult{}, messaging.ErrTransportClosed
	}
	body, err := t.call(ctx, pathPreflight, 0, struct{}{})
	if err != nil {
		return messaging.PreflightResult{}, err
	}
	result := messaging.PreflightResult{Protocol: body.Protocol, Generation: body.Epoch}
	if body.Error != "" {
		return result, body.err()
	}
	if body.Protocol != messaging.ProtocolVersion {
		return result, messaging.ErrUnsupportedProtocol
	}
	if t.desc.ExpectedEpoch != "" && body.Epoch != t.desc.ExpectedEpoch {
		return result, messaging.ErrGenerationMismatch
	}
	return result, nil
}

// call performs one v1 request with the pinned capability and returns the
// decoded response envelope. A response body that cannot be decoded is an
// error; a non-nil error means no trustworthy response arrived.
func (t *Transport) call(ctx context.Context, path string, timeout time.Duration, payload any) (envelope, error) {
	var body envelope
	raw, err := json.Marshal(payload)
	if err != nil {
		return body, fmt.Errorf("%w: %v", messaging.ErrInvalidRequest, err)
	}
	req, err := http.NewRequest(http.MethodPost, t.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return body, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set(messaging.HeaderCapability, t.desc.CapabilityID)
	req.Header.Set(messaging.HeaderSecret, t.desc.CapabilitySecret)

	_, raw, err = t.Do(ctx, req, timeout)
	if err != nil {
		return body, err
	}
	if len(raw) == 0 {
		return body, fmt.Errorf("%w: empty response body", ErrMalformedResponse)
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return body, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	return body, nil
}

// envelope is one v1 response. Every field is optional so a response carries
// only the operation's result plus, on failure, its typed error code.
type envelope struct {
	Outcome   messaging.Outcome `json:"outcome"`
	Error     string            `json:"error"`
	Message   string            `json:"message"`
	Protocol  int               `json:"protocol"`
	Epoch     string            `json:"epoch"`
	MessageID string            `json:"message_id"`
	Fence     *httpbase.Fence   `json:"fence"`
	Reply     *wireMessage      `json:"reply"`
	Messages  []wireMessage     `json:"messages"`
	Peers     []wirePeer        `json:"peers"`
}

type wireMessage struct {
	MessageID string          `json:"message_id"`
	FromRunID string          `json:"from_run_id"`
	Sender    string          `json:"sender"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
}

type wirePeer struct {
	RunID   string          `json:"run_id"`
	Session json.RawMessage `json:"session"`
}

func (m wireMessage) inbound() messaging.InboundMessage {
	return messaging.InboundMessage{
		MessageID: m.MessageID,
		FromRunID: m.FromRunID,
		Sender:    m.Sender,
		Kind:      m.Kind,
		Payload:   m.Payload,
	}
}

// err maps the peer's error code to the shared typed error class. An unknown
// code stays untyped, so callers fall back to the response's outcome.
func (e envelope) err() error {
	if e.Error == "" {
		return nil
	}
	if typed := messaging.WireErrorFor(e.Error); typed != nil {
		return typed
	}
	return fmt.Errorf("hosthttp: peer reported %q: %s", e.Error, e.Message)
}

var _ messaging.Transport = (*Transport)(nil)
