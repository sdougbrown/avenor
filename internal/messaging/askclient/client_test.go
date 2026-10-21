package askclient

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/inprocess"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// scriptedTransport returns pre-set results and records calls.
type scriptedTransport struct {
	sendResults    []messaging.SendResult
	sendErrs       []error
	waitResults    []messaging.WaitResult
	waitErrs       []error
	withdrawResult messaging.WithdrawResult
	withdrawErr    error
	preflightErr   error

	sendCalls           int
	waitCalls           int
	withdrawCalls       int
	generation          string
	lastSend            messaging.SendRequest
	lastWait            messaging.WaitRequest
	lastWithdraw        messaging.WithdrawRequest
	withdrawCtxErr      error
	withdrawHasDeadline bool
}

func (s *scriptedTransport) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	s.lastSend = req
	i := s.sendCalls
	s.sendCalls++
	var res messaging.SendResult
	if i < len(s.sendResults) {
		res = s.sendResults[i]
	}
	var err error
	if i < len(s.sendErrs) {
		err = s.sendErrs[i]
	}
	return res, err
}

func (s *scriptedTransport) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	s.lastWait = req
	i := s.waitCalls
	s.waitCalls++
	var res messaging.WaitResult
	if i < len(s.waitResults) {
		res = s.waitResults[i]
	}
	var err error
	if i < len(s.waitErrs) {
		err = s.waitErrs[i]
	}
	return res, err
}

func (s *scriptedTransport) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	return messaging.SendResult{}, nil
}

func (s *scriptedTransport) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	s.withdrawCalls++
	s.lastWithdraw = req
	s.withdrawCtxErr = ctx.Err()
	_, s.withdrawHasDeadline = ctx.Deadline()
	return s.withdrawResult, s.withdrawErr
}

func (s *scriptedTransport) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return messaging.ReceiveResult{}, nil
}

func (s *scriptedTransport) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	return messaging.PeersResult{}, nil
}

func (s *scriptedTransport) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
	if s.preflightErr != nil {
		return messaging.PreflightResult{}, s.preflightErr
	}
	return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: s.generation}, nil
}

func (s *scriptedTransport) Close() error { return nil }

var alice = messaging.Identity{RunID: "alice", Sender: "alice"}
var bob = messaging.Identity{RunID: "bob", Sender: "bob"}

func sendRequest(msgID string) messaging.SendRequest {
	return messaging.SendRequest{From: alice, To: bob, MessageID: msgID, Body: `{"message":"ping"}`}
}

func waitRequest(msgID string) messaging.WaitRequest {
	return messaging.WaitRequest{Sender: alice, MessageID: msgID, Timeout: time.Second}
}

func TestAskReplied(t *testing.T) {
	tr := &scriptedTransport{
		generation:  "gen-1",
		sendResults: []messaging.SendResult{{Outcome: messaging.OutcomeSent, MessageID: "id-1", Generation: "gen-1"}},
		waitResults: []messaging.WaitResult{{
			Outcome: messaging.OutcomeReplied,
			Reply:   &messaging.InboundMessage{MessageID: "id-1", Payload: []byte(`{"message":"pong"}`)},
		}},
	}
	client := New(tr, Options{})
	res, err := client.Ask(context.Background(), sendRequest("id-1"), waitRequest("id-1"))
	if err != nil {
		t.Fatalf("Ask: %v", err)
	}
	if res.Outcome != messaging.OutcomeReplied {
		t.Fatalf("outcome = %q", res.Outcome)
	}
	text, err := res.Text()
	if err != nil || text != "pong" {
		t.Fatalf("Text = (%q, %v)", text, err)
	}
	if tr.lastSend.Generation != "gen-1" {
		t.Fatalf("send generation = %q", tr.lastSend.Generation)
	}
}

func TestAskMintsVersionedID(t *testing.T) {
	tr := &scriptedTransport{
		generation:  "gen-1",
		sendResults: []messaging.SendResult{{Outcome: messaging.OutcomeSent, Generation: "gen-1"}},
		waitResults: []messaging.WaitResult{{Outcome: messaging.OutcomeTimedOut}},
	}
	client := New(tr, Options{})
	res, err := client.Ask(context.Background(), sendRequest(""), waitRequest(""))
	var askErr *Error
	if !errors.As(err, &askErr) || askErr.Outcome != messaging.OutcomeTimedOut {
		t.Fatalf("Ask error = %v", err)
	}
	if res.MessageID == "" {
		t.Fatal("Ask did not retain the minted message id")
	}
	if _, err := messaging.ParseMsgID(res.MessageID, time.Now()); err != nil {
		t.Fatalf("minted id is not versioned: %v", err)
	}
	if tr.lastWait.MessageID != res.MessageID {
		t.Fatalf("wait id = %q, want %q", tr.lastWait.MessageID, res.MessageID)
	}
	if len(tr.sendResults) != 1 || tr.sendCalls != 1 {
		t.Fatalf("send calls = %d", tr.sendCalls)
	}
}

func TestAskAdoptsThePayloadID(t *testing.T) {
	tr := &scriptedTransport{
		generation:  "gen-1",
		sendResults: []messaging.SendResult{{Outcome: messaging.OutcomeSent, Generation: "gen-1"}},
		waitResults: []messaging.WaitResult{{Outcome: messaging.OutcomeTimedOut}},
	}
	req := sendRequest("")
	req.Body = `{"id":"payload-id","message":"ping"}`
	client := New(tr, Options{})
	res, err := client.Ask(context.Background(), req, waitRequest(""))
	var askErr *Error
	if !errors.As(err, &askErr) || askErr.Outcome != messaging.OutcomeTimedOut {
		t.Fatalf("Ask error = %v", err)
	}
	if res.MessageID != "payload-id" || tr.lastSend.MessageID != "payload-id" {
		t.Fatalf("ask id = (%q, %q), want the payload id", res.MessageID, tr.lastSend.MessageID)
	}
	if tr.lastWait.MessageID != "payload-id" {
		t.Fatalf("wait id = %q, want the payload id", tr.lastWait.MessageID)
	}
}

func TestAskClassifications(t *testing.T) {
	cases := []struct {
		name     string
		send     messaging.SendResult
		sendErr  error
		wait     messaging.WaitResult
		waitErr  error
		withdraw messaging.WithdrawResult
		outcome  messaging.Outcome
		pending  bool
	}{
		{name: "auth rejected", send: messaging.SendResult{Outcome: messaging.OutcomeAuthRejected}, sendErr: messaging.ErrAuthRejected, outcome: messaging.OutcomeAuthRejected},
		{name: "generation mismatch", send: messaging.SendResult{Outcome: messaging.OutcomeNotSent}, sendErr: messaging.ErrGenerationMismatch, outcome: messaging.OutcomeNotSent},
		{name: "invalid request", send: messaging.SendResult{Outcome: messaging.OutcomeNotSent}, sendErr: messaging.ErrInvalidRequest, outcome: messaging.OutcomeNotSent},
		{name: "untyped send failure is uncertain", send: messaging.SendResult{}, sendErr: errors.New("connection reset"), outcome: messaging.OutcomePossiblyPending, pending: true},
		{name: "uncertain wait", send: messaging.SendResult{Outcome: messaging.OutcomeSent}, wait: messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, withdraw: messaging.WithdrawResult{Outcome: messaging.OutcomePossiblyPending}, outcome: messaging.OutcomePossiblyPending, pending: true},
		{name: "withdrawn wait", send: messaging.SendResult{Outcome: messaging.OutcomeSent}, wait: messaging.WaitResult{Outcome: messaging.OutcomeWithdrawn}, outcome: messaging.OutcomeWithdrawn},
		{name: "lost wait", send: messaging.SendResult{Outcome: messaging.OutcomeSent}, wait: messaging.WaitResult{Outcome: messaging.OutcomeLost}, outcome: messaging.OutcomeLost},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := &scriptedTransport{
				generation:     "gen-1",
				sendResults:    []messaging.SendResult{tc.send},
				sendErrs:       []error{tc.sendErr},
				waitResults:    []messaging.WaitResult{tc.wait},
				waitErrs:       []error{tc.waitErr},
				withdrawResult: tc.withdraw,
			}
			client := New(tr, Options{})
			res, err := client.Ask(context.Background(), sendRequest("id-1"), waitRequest("id-1"))
			var askErr *Error
			if !errors.As(err, &askErr) {
				t.Fatalf("Ask error = %v, want *Error", err)
			}
			if res.Outcome != tc.outcome || askErr.Outcome != tc.outcome {
				t.Fatalf("outcome = %q / %q, want %q", res.Outcome, askErr.Outcome, tc.outcome)
			}
			if askErr.Pending() != tc.pending {
				t.Fatalf("Pending = %v, want %v", askErr.Pending(), tc.pending)
			}
			if tr.sendCalls != 1 {
				t.Fatalf("send calls = %d, want 1", tr.sendCalls)
			}
		})
	}
}

func TestAskFailedWaitWithdraws(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("failed wait is retracted under the cleanup context", func(t *testing.T) {
		tr := &scriptedTransport{
			generation:     "gen-1",
			sendResults:    []messaging.SendResult{{Outcome: messaging.OutcomeSent}},
			waitResults:    []messaging.WaitResult{{Outcome: messaging.OutcomePossiblyPending}},
			withdrawResult: messaging.WithdrawResult{Outcome: messaging.OutcomeWithdrawn},
		}
		_, err := New(tr, Options{}).Ask(ctx, sendRequest("id-1"), waitRequest("id-1"))
		var askErr *Error
		if !errors.As(err, &askErr) {
			t.Fatalf("Ask error = %v, want *Error", err)
		}
		if tr.withdrawCalls != 1 {
			t.Fatalf("withdraw calls = %d, want 1", tr.withdrawCalls)
		}
		if tr.lastWithdraw.MessageID != "id-1" || tr.lastWithdraw.Sender != alice || tr.lastWithdraw.Generation != "gen-1" {
			t.Fatalf("withdraw request = %+v, want the ask's identity and id", tr.lastWithdraw)
		}
		if tr.withdrawCtxErr != nil || !tr.withdrawHasDeadline {
			t.Fatalf("cleanup context = (err %v, deadline %v), want a bounded live context", tr.withdrawCtxErr, tr.withdrawHasDeadline)
		}
		if askErr.Pending() {
			t.Fatal("retracted ask is still reported pending")
		}
	})

	t.Run("unconfirmed retraction stays pending", func(t *testing.T) {
		tr := &scriptedTransport{
			generation:  "gen-1",
			sendResults: []messaging.SendResult{{Outcome: messaging.OutcomeSent}},
			waitResults: []messaging.WaitResult{{Outcome: messaging.OutcomePossiblyPending}},
			withdrawErr: errors.New("cleanup unavailable"),
		}
		_, err := New(tr, Options{}).Ask(ctx, sendRequest("id-1"), waitRequest("id-1"))
		var askErr *Error
		if !errors.As(err, &askErr) {
			t.Fatalf("Ask error = %v, want *Error", err)
		}
		if tr.withdrawCalls != 1 || !askErr.Pending() {
			t.Fatalf("withdraw calls = %d (pending %v), want 1 and pending", tr.withdrawCalls, askErr.Pending())
		}
	})

	t.Run("send failure is not withdrawn", func(t *testing.T) {
		tr := &scriptedTransport{
			generation:  "gen-1",
			sendResults: []messaging.SendResult{{Outcome: messaging.OutcomeNotSent}},
			sendErrs:    []error{messaging.ErrTargetUnavailable},
		}
		if _, err := New(tr, Options{}).Ask(ctx, sendRequest("id-1"), waitRequest("id-1")); err == nil {
			t.Fatal("Ask returned no error for a refused send")
		}
		if tr.withdrawCalls != 0 {
			t.Fatalf("withdraw calls = %d, want none for a refused send", tr.withdrawCalls)
		}
	})
}

// TestAskTimeoutRetractsTheUndeliveredAsk pins that a timed-out fence keeps
// its retraction handle, so the failed-wait withdrawal empties the target's
// queue instead of leaving an ask nobody can answer.
func TestAskTimeoutRetractsTheUndeliveredAsk(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	defer b.Stop()
	if _, err := b.CreateRun("bob", ""); err != nil {
		t.Fatalf("create bob: %v", err)
	}

	client := New(inprocess.New(b.Store()), Options{})
	msgID, err := messaging.NewMsgID(time.Now(), time.Time{})
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	sender := messaging.Identity{RunID: "alice", Sender: "alice"}
	_, err = client.Ask(context.Background(), messaging.SendRequest{
		From:      sender,
		To:        messaging.Identity{RunID: "bob", Sender: "bob"},
		MessageID: msgID,
		Body:      `{"message":"ping"}`,
	}, messaging.WaitRequest{Sender: sender, Timeout: 20 * time.Millisecond})
	var askErr *Error
	if !errors.As(err, &askErr) || askErr.Outcome != messaging.OutcomeTimedOut {
		t.Fatalf("Ask = %v, want a timed-out *Error", err)
	}

	queued, err := b.DrainAgentMessages("bob", 0)
	if err != nil {
		t.Fatalf("DrainAgentMessages: %v", err)
	}
	if len(queued) != 0 {
		t.Fatalf("timed-out ask left %d queued message(s)", len(queued))
	}
}

func TestWithdraw(t *testing.T) {
	tr := &scriptedTransport{withdrawResult: messaging.WithdrawResult{
		Outcome: messaging.OutcomeWithdrawn,
		Fence:   messaging.FenceAck{Terminal: true},
	}}
	client := New(tr, Options{})
	res, err := client.Withdraw(context.Background(), messaging.WithdrawRequest{Sender: alice, MessageID: "id-1", Generation: "gen-1"})
	if err != nil || res.Outcome != messaging.OutcomeWithdrawn || !res.Fence.Terminal {
		t.Fatalf("Withdraw = (%+v, %v)", res, err)
	}
	if tr.withdrawCalls != 1 {
		t.Fatalf("withdraw calls = %d", tr.withdrawCalls)
	}

	tr = &scriptedTransport{generation: "gen-live", withdrawResult: messaging.WithdrawResult{Outcome: messaging.OutcomeWithdrawn}}
	if _, err := New(tr, Options{}).Withdraw(context.Background(), messaging.WithdrawRequest{Sender: alice, MessageID: "id-1"}); err != nil {
		t.Fatalf("Withdraw without a pinned generation: %v", err)
	}
	if tr.lastWithdraw.Generation != "gen-live" {
		t.Fatalf("withdraw generation = %q, want the preflighted generation", tr.lastWithdraw.Generation)
	}

	tr = &scriptedTransport{withdrawResult: messaging.WithdrawResult{Outcome: messaging.OutcomeWithdrawn}}
	tr.preflightErr = messaging.ErrTransportClosed
	res, err = New(tr, Options{}).Withdraw(context.Background(), messaging.WithdrawRequest{Sender: alice, MessageID: "id-1"})
	if err == nil || res.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("Withdraw with a failed preflight = (%+v, %v), want not-sent", res, err)
	}
	if tr.withdrawCalls != 0 {
		t.Fatalf("withdraw calls = %d, want none without a generation", tr.withdrawCalls)
	}
}

func TestRenderReply(t *testing.T) {
	cases := []struct {
		outcome messaging.Outcome
		reply   *messaging.InboundMessage
		want    string
		wantErr bool
	}{
		{messaging.OutcomeReplied, &messaging.InboundMessage{Payload: []byte(`{"message":"hello"}`)}, "hello", false},
		{messaging.OutcomeReplied, &messaging.InboundMessage{MessageID: "msg-1", FromRunID: "bob", Payload: []byte(`{"summary":"ok"}`)}, `{"from_run_id":"bob","message_id":"msg-1","payload":{"summary":"ok"}}`, false},
		{messaging.OutcomeReplied, &messaging.InboundMessage{Payload: []byte(`raw`)}, "raw", false},
		{messaging.OutcomeReplied, &messaging.InboundMessage{}, "(empty reply)", false},
		{messaging.OutcomeReplied, nil, "", true},
		{messaging.OutcomeTimedOut, nil, "(timeout: no reply received)", false},
		{messaging.OutcomeWithdrawn, nil, "(cancelled)", false},
		{messaging.OutcomeEdgeAbsent, nil, "(no pending ask)", false},
		{messaging.OutcomePossiblyPending, nil, "(possibly pending: no terminal reply)", false},
		{messaging.OutcomeLost, nil, "(ask lost)", false},
		{messaging.OutcomeNotSent, nil, "", true},
	}
	for _, tc := range cases {
		got, err := RenderReply(messaging.WaitResult{Outcome: tc.outcome, Reply: tc.reply})
		if (err != nil) != tc.wantErr {
			t.Errorf("RenderReply(%q) error = %v", tc.outcome, err)
			continue
		}
		if got != tc.want {
			t.Errorf("RenderReply(%q) = %q, want %q", tc.outcome, got, tc.want)
		}
	}
}
