package control

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// This file is the control wire contract for the message_*/resume/session_*
// methods. #317's result types carry no json tags, so these snake_case DTOs are
// what crosses the control socket; the #317 structs never do. Each direction
// has exactly one conversion here, so no handler and no adapter re-implements
// a mapping.

// MessageSendParams is the message_send request. The control owner always
// sends as the supervisor session, so the request carries no from_run_id.
type MessageSendParams struct {
	ToRunID    string `json:"to_run_id"`
	MessageID  string `json:"message_id"`
	Generation string `json:"generation"`
	Body       string `json:"body"`
	NoReply    bool   `json:"no_reply,omitempty"`
}

// MessageSendResult is the message_send / message_reply response.
type MessageSendResult struct {
	Outcome    string `json:"outcome"`
	MessageID  string `json:"message_id,omitempty"`
	Generation string `json:"generation,omitempty"`
}

// MessageWaitParams is the message_wait request.
type MessageWaitParams struct {
	MessageID   string `json:"message_id"`
	Generation  string `json:"generation"`
	TimeoutMS   int64  `json:"timeout_ms,omitempty"`
}

// MessageInbound is one inbound message on the control wire.
type MessageInbound struct {
	MessageID string          `json:"message_id"`
	FromRunID string          `json:"from_run_id"`
	Sender    string          `json:"sender,omitempty"`
	Kind      string          `json:"kind,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// MessageWaitResult is the message_wait response.
type MessageWaitResult struct {
	Outcome   string          `json:"outcome"`
	MessageID string          `json:"message_id,omitempty"`
	Reply     *MessageInbound `json:"reply,omitempty"`
}

// MessageReplyParams is the message_reply request.
type MessageReplyParams struct {
	ToRunID       string `json:"to_run_id"`
	MessageID     string `json:"message_id"`
	CorrelationID string `json:"correlation_id,omitempty"`
	Body          string `json:"body"`
}

// MessageWithdrawParams is the message_withdraw request.
type MessageWithdrawParams struct {
	MessageID   string `json:"message_id"`
	Generation  string `json:"generation"`
}

// MessageFence is the terminal fence a withdrawal installed.
type MessageFence struct {
	Sender     string `json:"sender"`
	MessageID  string `json:"message_id"`
	Generation string `json:"generation"`
	Deadline   int64  `json:"deadline"`
	Terminal   bool   `json:"terminal"`
}

// MessageWithdrawResult is the message_withdraw response.
type MessageWithdrawResult struct {
	Outcome string        `json:"outcome"`
	Fence   *MessageFence `json:"fence,omitempty"`
}

// MessageReceiveParams is the message_receive request.
type MessageReceiveParams struct {
	Generation  string `json:"generation"`
	TimeoutMS   int64  `json:"timeout_ms,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

// MessageReceiveResult is the message_receive response.
type MessageReceiveResult struct {
	Messages   []MessageInbound `json:"messages"`
	Generation string           `json:"generation,omitempty"`
}

// MessagePeersParams is the message_peers request.
type MessagePeersParams struct {
	Generation string `json:"generation"`
}

// MessagePeer is one visible peer on the control wire.
type MessagePeer struct {
	RunID   string          `json:"run_id"`
	Session json.RawMessage `json:"session,omitempty"`
}

// MessagePeersResult is the message_peers response.
type MessagePeersResult struct {
	Peers      []MessagePeer `json:"peers"`
	Generation string        `json:"generation,omitempty"`
}

// MessagePreflightResult is the message_preflight response.
type MessagePreflightResult struct {
	Protocol   int    `json:"protocol"`
	Generation string `json:"generation"`
}

// ResumeParams is the resume request.
type ResumeParams struct {
	RunID          string `json:"run_id"`
	Target         string `json:"target"`
	Message        string `json:"message"`
	Label          string `json:"label,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	SentinelFile   string `json:"sentinel_file,omitempty"`
	OnEvent        string `json:"on_event,omitempty"`
}

// ResumeResult is the resume response.
type ResumeResult struct {
	RunID        string `json:"run_id"`
	Label        string `json:"label"`
	RuntimeID    string `json:"runtime_id"`
	SessionID    string `json:"session_id"`
	SentinelFile string `json:"sentinel_file"`
	OnEvent      string `json:"on_event"`
}

// SessionRenameParams is the session_rename request.
type SessionRenameParams struct {
	SessionID string `json:"session_id"`
	Name      string `json:"name"`
}

// SessionRenameResult is the session_rename response.
type SessionRenameResult struct {
	RunID         string `json:"run_id"`
	Label         string `json:"label"`
	PreviousLabel string `json:"previous_label"`
}

// SessionLastParams is the session_last request.
type SessionLastParams struct {
	RuntimeID string `json:"runtime_id"`
}

// SessionLastResult is the session_last response. HasRecord distinguishes "no
// completed turn" from "a turn with no final text"; the broker record itself
// deliberately carries no such field.
type SessionLastResult struct {
	RunID       string `json:"run_id"`
	Label       string `json:"label,omitempty"`
	HasRecord   bool   `json:"has_record"`
	FinalOutput string `json:"final_output"`
	StopReason  string `json:"stop_reason,omitempty"`
	Truncated   bool   `json:"final_output_truncated,omitempty"`
	At          int64  `json:"at,omitempty"`
}

// SessionLastSetParams is the session_last_set request.
type SessionLastSetParams struct {
	RuntimeID   string `json:"runtime_id"`
	FinalOutput string `json:"final_output"`
	StopReason  string `json:"stop_reason,omitempty"`
	Truncated   bool   `json:"final_output_truncated,omitempty"`
}

// MessageErrorData is the structured error.data a transport failure carries, so
// a client classifies the failure without parsing text.
type MessageErrorData struct {
	Code      string `json:"code"`
	Outcome   string `json:"outcome,omitempty"`
	MessageID string `json:"message_id,omitempty"`
	WireCode  string `json:"wire_code,omitempty"`
}

// --- request construction (wire → transport) ---

// NewMessageSendParams projects a send request onto the wire shape.
func NewMessageSendParams(req messaging.SendRequest) MessageSendParams {
	return MessageSendParams{
		ToRunID:    req.To.RunID,
		MessageID:  req.MessageID,
		Generation: req.Generation,
		Body:       req.Body,
		NoReply:    req.NoReply,
	}
}

// request builds the transport request. from is the authenticated identity the
// control socket acts as; the wire has no second source for it.
func (p MessageSendParams) request(from messaging.Identity) messaging.SendRequest {
	return messaging.SendRequest{
		From:       from,
		To:         messaging.Identity{RunID: p.ToRunID},
		MessageID:  p.MessageID,
		Generation: p.Generation,
		Body:       p.Body,
		NoReply:    p.NoReply,
	}
}

// NewMessageWaitParams projects a wait request onto the wire shape.
func NewMessageWaitParams(req messaging.WaitRequest) MessageWaitParams {
	return MessageWaitParams{
		MessageID:   req.MessageID,
		Generation:  req.Generation,
		TimeoutMS:   req.Timeout.Milliseconds(),
	}
}

func (p MessageWaitParams) request(from messaging.Identity) messaging.WaitRequest {
	return messaging.WaitRequest{
		Sender:     from,
		MessageID:  p.MessageID,
		Generation: p.Generation,
		Timeout:    boundedTimeout(p.TimeoutMS),
	}
}

// NewMessageReplyParams projects a reply request onto the wire shape.
func NewMessageReplyParams(req messaging.ReplyRequest) MessageReplyParams {
	return MessageReplyParams{
		ToRunID:       req.ToRunID,
		MessageID:     req.MessageID,
		CorrelationID: req.CorrelationID,
		Body:          req.Body,
	}
}

func (p MessageReplyParams) request(from messaging.Identity) messaging.ReplyRequest {
	return messaging.ReplyRequest{
		From:          from,
		ToRunID:       p.ToRunID,
		MessageID:     p.MessageID,
		CorrelationID: p.CorrelationID,
		Body:          p.Body,
	}
}

// NewMessageWithdrawParams projects a withdraw request onto the wire shape.
func NewMessageWithdrawParams(req messaging.WithdrawRequest) MessageWithdrawParams {
	return MessageWithdrawParams{
		MessageID:   req.MessageID,
		Generation:  req.Generation,
	}
}

func (p MessageWithdrawParams) request(from messaging.Identity) messaging.WithdrawRequest {
	return messaging.WithdrawRequest{Sender: from, MessageID: p.MessageID, Generation: p.Generation}
}

// NewMessageReceiveParams projects a receive request onto the wire shape.
func NewMessageReceiveParams(req messaging.ReceiveRequest) MessageReceiveParams {
	return MessageReceiveParams{
		Generation:  req.Generation,
		TimeoutMS:   req.Timeout.Milliseconds(),
		Limit:       req.Limit,
	}
}

func (p MessageReceiveParams) request(from messaging.Identity) messaging.ReceiveRequest {
	return messaging.ReceiveRequest{
		Sender:     from,
		Generation: p.Generation,
		Timeout:    boundedTimeout(p.TimeoutMS),
		Limit:      p.Limit,
	}
}

// NewMessagePeersParams projects a peers request onto the wire shape.
func NewMessagePeersParams(req messaging.PeersRequest) MessagePeersParams {
	return MessagePeersParams{Generation: req.Generation}
}

func (p MessagePeersParams) request(from messaging.Identity) messaging.PeersRequest {
	return messaging.PeersRequest{Sender: from, Generation: p.Generation}
}

// NewResumeParams projects a resume request onto the wire shape.
func NewResumeParams(req messaging.ResumeRequest) ResumeParams {
	return ResumeParams{
		RunID:          req.RunID,
		Target:         req.Target,
		Message:        req.Message,
		Label:          req.Label,
		IdempotencyKey: req.IdempotencyKey,
		SentinelFile:   req.SentinelFile,
		OnEvent:        req.EventLog,
	}
}

func (p ResumeParams) request() messaging.ResumeRequest {
	return messaging.ResumeRequest{
		RunID:          p.RunID,
		Target:         p.Target,
		Message:        p.Message,
		Label:          p.Label,
		IdempotencyKey: p.IdempotencyKey,
		SentinelFile:   p.SentinelFile,
		EventLog:       p.OnEvent,
	}
}

// boundedTimeout converts a wire duration, clamped to the shared wait cap so a
// large integer cannot overflow time.Duration.
func boundedTimeout(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	if ms >= int64(messaging.MaxWait/time.Millisecond) {
		return messaging.MaxWait
	}
	return time.Duration(ms) * time.Millisecond
}

// --- result conversion (transport → wire) ---

func newMessageSendResult(res messaging.SendResult) MessageSendResult {
	return MessageSendResult{Outcome: string(res.Outcome), MessageID: res.MessageID, Generation: res.Generation}
}

func newMessageWaitResult(res messaging.WaitResult) MessageWaitResult {
	out := MessageWaitResult{Outcome: string(res.Outcome)}
	if res.Reply != nil {
		inbound := newMessageInbound(*res.Reply)
		out.MessageID = inbound.MessageID
		out.Reply = &inbound
	}
	return out
}

func newMessageInbound(msg messaging.InboundMessage) MessageInbound {
	return MessageInbound{
		MessageID: msg.MessageID,
		FromRunID: msg.FromRunID,
		Sender:    msg.Sender,
		Kind:      msg.Kind,
		Payload:   msg.Payload,
	}
}

func newMessageWithdrawResult(res messaging.WithdrawResult) MessageWithdrawResult {
	return MessageWithdrawResult{
		Outcome: string(res.Outcome),
		Fence: &MessageFence{
			Sender:     res.Fence.Sender,
			MessageID:  res.Fence.MessageID,
			Generation: res.Fence.Generation,
			Deadline:   res.Fence.Deadline.UnixMilli(),
			Terminal:   res.Fence.Terminal,
		},
	}
}

func newMessageReceiveResult(res messaging.ReceiveResult) MessageReceiveResult {
	out := MessageReceiveResult{Messages: make([]MessageInbound, 0, len(res.Messages)), Generation: res.Generation}
	for _, msg := range res.Messages {
		out.Messages = append(out.Messages, newMessageInbound(msg))
	}
	return out
}

func newMessagePeersResult(res messaging.PeersResult) MessagePeersResult {
	out := MessagePeersResult{Peers: make([]MessagePeer, 0, len(res.Peers)), Generation: res.Generation}
	for _, peer := range res.Peers {
		out.Peers = append(out.Peers, MessagePeer{RunID: peer.RunID, Session: peer.Session})
	}
	return out
}

func newMessagePreflightResult(res messaging.PreflightResult) MessagePreflightResult {
	return MessagePreflightResult{Protocol: res.Protocol, Generation: res.Generation}
}

func newResumeResult(res messaging.ResumeResult) ResumeResult {
	return ResumeResult{
		RunID:        res.RunID,
		Label:        res.Label,
		RuntimeID:    res.RuntimeID,
		SessionID:    res.SessionID,
		SentinelFile: res.SentinelFile,
		OnEvent:      res.OnEvent,
	}
}

func newSessionRenameResult(res messaging.RenameResult) SessionRenameResult {
	return SessionRenameResult{RunID: res.RunID, Label: res.Label, PreviousLabel: res.PreviousLabel}
}

func newSessionLastResult(record broker.LastAgentMessage, ok bool) SessionLastResult {
	return SessionLastResult{
		RunID:       record.RunID,
		Label:       record.Label,
		HasRecord:   ok,
		FinalOutput: record.Text,
		StopReason:  record.StopReason,
		Truncated:   record.Truncated,
		At:          record.At,
	}
}

// messageErrorData classifies a messaging transport failure for error.data.
// Those methods report the ask-transport facts, so the payload carries the
// outcome and the wire code as well as the typed tool code.
func messageErrorData(err error, outcome messaging.Outcome, messageID string) MessageErrorData {
	wireCode := messaging.WireCodeFor(err)
	if outcome == "" {
		outcome = messaging.WireOutcomeFor(wireCode)
	}
	return MessageErrorData{
		Code:      toolCode(err),
		Outcome:   string(outcome),
		MessageID: messageID,
		WireCode:  wireCode,
	}
}

// authorityErrorData classifies a resume/session_* failure for error.data.
// Those verbs report what the authority decided, not what a send did: outcome
// and wire_code belong to the ask transport, so the payload carries the typed
// code alone rather than a manufactured send classification.
func authorityErrorData(err error) MessageErrorData {
	return MessageErrorData{Code: toolCode(err)}
}

// toolCode prefers the typed tool code the authority attached; an error that
// names none keeps the class the wire contract gives it.
func toolCode(err error) string {
	var coded interface{ ToolCode() string }
	if errors.As(err, &coded) && coded.ToolCode() != "" {
		return coded.ToolCode()
	}
	return messaging.WireCodeFor(err)
}

// --- result decoding (wire → transport) for the client ---

// SendResult decodes a send response.
func (r MessageSendResult) SendResult() messaging.SendResult {
	return messaging.SendResult{Outcome: messaging.Outcome(r.Outcome), MessageID: r.MessageID, Generation: r.Generation}
}

// WaitResult decodes a wait response.
func (r MessageWaitResult) WaitResult() messaging.WaitResult {
	out := messaging.WaitResult{Outcome: messaging.Outcome(r.Outcome)}
	if r.Reply != nil {
		reply := r.Reply.InboundMessage()
		out.Reply = &reply
	}
	return out
}

// InboundMessage decodes one inbound message.
func (m MessageInbound) InboundMessage() messaging.InboundMessage {
	return messaging.InboundMessage{
		MessageID: m.MessageID,
		FromRunID: m.FromRunID,
		Sender:    m.Sender,
		Kind:      m.Kind,
		Payload:   m.Payload,
	}
}

// WithdrawResult decodes a withdraw response.
func (r MessageWithdrawResult) WithdrawResult() messaging.WithdrawResult {
	out := messaging.WithdrawResult{Outcome: messaging.Outcome(r.Outcome)}
	if r.Fence != nil {
		out.Fence = messaging.FenceAck{
			Sender:     r.Fence.Sender,
			MessageID:  r.Fence.MessageID,
			Generation: r.Fence.Generation,
			Deadline:   time.UnixMilli(r.Fence.Deadline),
			Terminal:   r.Fence.Terminal,
		}
	}
	return out
}

// ReceiveResult decodes a receive response.
func (r MessageReceiveResult) ReceiveResult() messaging.ReceiveResult {
	out := messaging.ReceiveResult{Generation: r.Generation, Messages: make([]messaging.InboundMessage, 0, len(r.Messages))}
	for _, msg := range r.Messages {
		out.Messages = append(out.Messages, msg.InboundMessage())
	}
	return out
}

// PeersResult decodes a peers response.
func (r MessagePeersResult) PeersResult() messaging.PeersResult {
	out := messaging.PeersResult{Generation: r.Generation, Peers: make([]messaging.Peer, 0, len(r.Peers))}
	for _, peer := range r.Peers {
		out.Peers = append(out.Peers, messaging.Peer{RunID: peer.RunID, Session: peer.Session})
	}
	return out
}

// PreflightResult decodes a preflight response.
func (r MessagePreflightResult) PreflightResult() messaging.PreflightResult {
	return messaging.PreflightResult{Protocol: r.Protocol, Generation: r.Generation}
}

// ResumeResult decodes a resume response.
func (r ResumeResult) ResumeResult() messaging.ResumeResult {
	return messaging.ResumeResult{
		RunID:        r.RunID,
		Label:        r.Label,
		RuntimeID:    r.RuntimeID,
		SessionID:    r.SessionID,
		SentinelFile: r.SentinelFile,
		OnEvent:      r.OnEvent,
	}
}

// RenameResult decodes a session_rename response.
func (r SessionRenameResult) RenameResult() messaging.RenameResult {
	return messaging.RenameResult{RunID: r.RunID, Label: r.Label, PreviousLabel: r.PreviousLabel}
}

// PeerLast decodes a session_last response.
func (r SessionLastResult) PeerLast() messaging.PeerLast {
	return messaging.PeerLast{
		RunID:      r.RunID,
		Label:      r.Label,
		HasRecord:  r.HasRecord,
		Text:       r.FinalOutput,
		StopReason: r.StopReason,
		Truncated:  r.Truncated,
	}
}
