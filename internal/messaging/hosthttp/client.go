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
	// defaultRequestTimeout bounds one request when the descriptor sets none.
	defaultRequestTimeout = 30 * time.Second

	maxResponseBytes = 1 << 20
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

// ErrRedirect reports a response that would redirect the request. The client
// never follows one, so the capability headers cannot reach another host.
var ErrRedirect = errors.New("hosthttp: redirect refused")

// Transport is the v1 host messaging transport.
type Transport struct {
	desc    messaging.EndpointDescriptor
	baseURL string
	client  *http.Client

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
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
		desc.Timeout = defaultRequestTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{
		desc:    desc,
		baseURL: strings.TrimSuffix(desc.BaseURL, "/"),
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
	if err := t.open(req.Generation); err != nil {
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
		// The ask may have reached the peer before the response was lost.
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
	if err := t.open(req.Generation); err != nil {
		return messaging.WaitResult{Outcome: messaging.OutcomeNotSent}, err
	}
	timeout := req.Timeout
	if timeout <= 0 || timeout > messaging.MaxWait {
		timeout = messaging.MaxWait
	}
	body, err := t.call(ctx, pathWait, timeout+messaging.CleanupDeadline, map[string]any{
		"epoch":         req.Generation,
		"sender_run_id": req.Sender.RunID,
		"message_id":    req.MessageID,
		"timeout_ms":    timeout.Milliseconds(),
	})
	if err != nil {
		if t.isClosed() && transportCancelled(err) {
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
	if t.isClosed() {
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
		return fail(messaging.OutcomePossiblyPending, err)
	}
	if body.Outcome == "" {
		return fail(messaging.OutcomePossiblyPending, fmt.Errorf("%w: reply response carries no outcome", ErrMalformedResponse))
	}
	return messaging.SendResult{Outcome: body.Outcome, MessageID: req.MessageID}, body.err()
}

func (t *Transport) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	if err := t.open(req.Generation); err != nil {
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
		result.Fence = body.Fence.ack()
	}
	return result, body.err()
}

func (t *Transport) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	if err := t.open(req.Generation); err != nil {
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
	if err := t.open(req.Generation); err != nil {
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
	if t.isClosed() {
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

// Close releases client-side resources, unblocks in-flight waits with a
// possibly-pending outcome and joins its goroutines. It never stops the peer.
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

// call performs one v1 request with the pinned capability and returns the
// decoded response envelope. A response body that cannot be decoded is an
// error; a non-nil error means no trustworthy response arrived.
func (t *Transport) call(ctx context.Context, path string, timeout time.Duration, payload any) (envelope, error) {
	var body envelope
	raw, err := json.Marshal(payload)
	if err != nil {
		return body, fmt.Errorf("%w: %v", messaging.ErrInvalidRequest, err)
	}
	if timeout <= 0 {
		timeout = t.desc.Timeout
	}
	child, release, ok := t.child(ctx, timeout)
	if !ok {
		return body, messaging.ErrTransportClosed
	}
	defer release()

	req, err := http.NewRequestWithContext(child, http.MethodPost, t.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return body, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set(messaging.HeaderCapability, t.desc.CapabilityID)
	req.Header.Set(messaging.HeaderSecret, t.desc.CapabilitySecret)

	resp, err := t.client.Do(req)
	if err != nil {
		return body, err
	}
	defer resp.Body.Close()
	raw, err = io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return body, err
	}
	if len(raw) > maxResponseBytes {
		return body, fmt.Errorf("%w: response body exceeds %d bytes", ErrMalformedResponse, maxResponseBytes)
	}
	if len(raw) == 0 {
		return body, fmt.Errorf("%w: empty response body", ErrMalformedResponse)
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return body, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	return body, nil
}

// transportCancelled reports whether err is the cancellation Close applies to
// in-flight requests rather than a fault the caller or the peer produced.
func transportCancelled(err error) bool {
	return errors.Is(err, messaging.ErrTransportClosed) || errors.Is(err, context.Canceled)
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

// envelope is one v1 response. Every field is optional so a response carries
// only the operation's result plus, on failure, its typed error code.
type envelope struct {
	Outcome   messaging.Outcome `json:"outcome"`
	Error     string            `json:"error"`
	Message   string            `json:"message"`
	Protocol  int               `json:"protocol"`
	Epoch     string            `json:"epoch"`
	MessageID string            `json:"message_id"`
	Fence     *wireFence        `json:"fence"`
	Reply     *wireMessage      `json:"reply"`
	Messages  []wireMessage     `json:"messages"`
	Peers     []wirePeer        `json:"peers"`
}

type wireFence struct {
	Sender     string    `json:"sender"`
	MessageID  string    `json:"message_id"`
	Generation string    `json:"generation"`
	Deadline   time.Time `json:"deadline"`
	Terminal   bool      `json:"terminal"`
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

func (f wireFence) ack() messaging.FenceAck {
	return messaging.FenceAck{
		Sender:     f.Sender,
		MessageID:  f.MessageID,
		Generation: f.Generation,
		Deadline:   f.Deadline,
		Terminal:   f.Terminal,
	}
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
