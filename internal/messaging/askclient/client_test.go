package askclient

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// scriptedTransport returns pre-set results and records calls.
type scriptedTransport struct {
	sendResults    []messaging.SendResult
	sendErrs       []error
	waitResults    []messaging.WaitResult
	waitErrs       []error
	withdrawResult messaging.WithdrawResult
	withdrawErr    error

	sendCalls     int
	waitCalls     int
	withdrawCalls int
	generation    string
	lastSend      messaging.SendRequest
	lastWait      messaging.WaitRequest
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
	return s.withdrawResult, s.withdrawErr
}

func (s *scriptedTransport) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return messaging.ReceiveResult{}, nil
}

func (s *scriptedTransport) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	return messaging.PeersResult{}, nil
}

func (s *scriptedTransport) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
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
		name    string
		send    messaging.SendResult
		sendErr error
		wait    messaging.WaitResult
		waitErr error
		outcome messaging.Outcome
		pending bool
	}{
		{name: "auth rejected", send: messaging.SendResult{Outcome: messaging.OutcomeAuthRejected}, sendErr: messaging.ErrAuthRejected, outcome: messaging.OutcomeAuthRejected},
		{name: "generation mismatch", send: messaging.SendResult{Outcome: messaging.OutcomeNotSent}, sendErr: messaging.ErrGenerationMismatch, outcome: messaging.OutcomeNotSent},
		{name: "invalid request", send: messaging.SendResult{Outcome: messaging.OutcomeNotSent}, sendErr: messaging.ErrInvalidRequest, outcome: messaging.OutcomeNotSent},
		{name: "untyped send failure is uncertain", send: messaging.SendResult{}, sendErr: errors.New("connection reset"), outcome: messaging.OutcomePossiblyPending, pending: true},
		{name: "uncertain wait", send: messaging.SendResult{Outcome: messaging.OutcomeSent}, wait: messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, outcome: messaging.OutcomePossiblyPending, pending: true},
		{name: "withdrawn wait", send: messaging.SendResult{Outcome: messaging.OutcomeSent}, wait: messaging.WaitResult{Outcome: messaging.OutcomeWithdrawn}, outcome: messaging.OutcomeWithdrawn},
		{name: "lost wait", send: messaging.SendResult{Outcome: messaging.OutcomeSent}, wait: messaging.WaitResult{Outcome: messaging.OutcomeLost}, outcome: messaging.OutcomeLost},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := &scriptedTransport{
				generation:  "gen-1",
				sendResults: []messaging.SendResult{tc.send},
				sendErrs:    []error{tc.sendErr},
				waitResults: []messaging.WaitResult{tc.wait},
				waitErrs:    []error{tc.waitErr},
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
}

func TestRenderReply(t *testing.T) {
	cases := []struct {
		outcome messaging.Outcome
		reply   *messaging.InboundMessage
		want    string
		wantErr bool
	}{
		{messaging.OutcomeReplied, &messaging.InboundMessage{Payload: []byte(`{"message":"hello"}`)}, "hello", false},
		{messaging.OutcomeReplied, &messaging.InboundMessage{Payload: []byte(`raw`)}, "raw", false},
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
		if !strings.EqualFold(got, tc.want) {
			t.Errorf("RenderReply(%q) = %q, want %q", tc.outcome, got, tc.want)
		}
	}
}
