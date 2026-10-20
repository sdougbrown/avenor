package control

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

func TestMessageSendParamsRequest(t *testing.T) {
	params := MessageSendParams{
		FromRunID: "rt_child", ToRunID: "rt_peer", MessageID: "msg_1",
		Generation: "gen_1", Body: `{"message":"hi"}`, NoReply: true,
	}
	req := params.request(messaging.Identity{RunID: "supervisor"})
	if req.From.RunID != "supervisor" || req.From.Sender != "" {
		t.Fatalf("from = %+v, want the authenticated identity with no sender name", req.From)
	}
	if req.To.RunID != "rt_peer" || req.MessageID != "msg_1" || req.Generation != "gen_1" || req.Body != `{"message":"hi"}` || !req.NoReply {
		t.Fatalf("request = %+v", req)
	}
	// An empty authenticated identity falls back to the wire's from_run_id.
	if got := params.request(messaging.Identity{}); got.From.RunID != "rt_child" {
		t.Fatalf("fallback from = %+v", got.From)
	}
	round := NewMessageSendParams(req)
	if round.FromRunID != "supervisor" || round.ToRunID != "rt_peer" || round.MessageID != "msg_1" || !round.NoReply {
		t.Fatalf("round trip = %+v", round)
	}
}

func TestMessageWaitParamsBoundsTimeout(t *testing.T) {
	huge := MessageWaitParams{SenderRunID: "rt_a", MessageID: "m", Generation: "g", TimeoutMS: 288230376151711745}
	if got := huge.request(messaging.Identity{RunID: "rt_a"}).Timeout; got != messaging.MaxWait {
		t.Fatalf("huge timeout = %v, want the shared cap %v", got, messaging.MaxWait)
	}
	over := MessageWaitParams{SenderRunID: "rt_a", TimeoutMS: int64(messaging.MaxWait/time.Millisecond) + 1}
	if got := over.request(messaging.Identity{RunID: "rt_a"}).Timeout; got != messaging.MaxWait {
		t.Fatalf("over-cap timeout = %v, want %v", got, messaging.MaxWait)
	}
	zero := MessageWaitParams{SenderRunID: "rt_a", TimeoutMS: 0}
	if got := zero.request(messaging.Identity{RunID: "rt_a"}).Timeout; got != 0 {
		t.Fatalf("zero timeout = %v, want 0", got)
	}
	wait := NewMessageWaitParams(messaging.WaitRequest{
		Sender: messaging.Identity{RunID: "rt_a"}, MessageID: "m", Generation: "g",
		Timeout: 1500 * time.Millisecond,
	})
	if wait.TimeoutMS != 1500 || wait.SenderRunID != "rt_a" {
		t.Fatalf("wait params = %+v", wait)
	}
}

func TestMessageResultConversions(t *testing.T) {
	send := newMessageSendResult(messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: "m", Generation: "g"})
	if send.Outcome != "sent" || send.MessageID != "m" || send.Generation != "g" {
		t.Fatalf("send result = %+v", send)
	}
	if back := send.SendResult(); back.Outcome != messaging.OutcomeSent || back.MessageID != "m" {
		t.Fatalf("send round trip = %+v", back)
	}

	reply := messaging.InboundMessage{MessageID: "m", FromRunID: "rt_b", Sender: "peer-b", Kind: "agent_message", Payload: json.RawMessage(`{"message":"hi"}`)}
	wait := newMessageWaitResult(messaging.WaitResult{Outcome: messaging.OutcomeReplied, Reply: &reply})
	if wait.Reply == nil || wait.MessageID != "m" || wait.Reply.FromRunID != "rt_b" {
		t.Fatalf("wait result = %+v", wait)
	}
	if back := wait.WaitResult(); back.Outcome != messaging.OutcomeReplied || back.Reply == nil || back.Reply.MessageID != "m" {
		t.Fatalf("wait round trip = %+v", back)
	}

	fence := newMessageWithdrawResult(messaging.WithdrawResult{
		Outcome: messaging.OutcomeWithdrawn,
		Fence:   messaging.FenceAck{Sender: "rt_a", MessageID: "m", Generation: "g", Deadline: time.UnixMilli(1234), Terminal: true},
	})
	if fence.Fence == nil || fence.Fence.Deadline != 1234 || !fence.Fence.Terminal {
		t.Fatalf("withdraw result = %+v", fence)
	}
	if back := fence.WithdrawResult(); back.Outcome != messaging.OutcomeWithdrawn || back.Fence.MessageID != "m" || !back.Fence.Terminal {
		t.Fatalf("withdraw round trip = %+v", back)
	}

	receive := newMessageReceiveResult(messaging.ReceiveResult{
		Generation: "g",
		Messages:   []messaging.InboundMessage{reply},
	})
	if len(receive.Messages) != 1 || receive.Generation != "g" {
		t.Fatalf("receive result = %+v", receive)
	}
	if back := receive.ReceiveResult(); len(back.Messages) != 1 || back.Messages[0].MessageID != "m" {
		t.Fatalf("receive round trip = %+v", back)
	}

	peers := newMessagePeersResult(messaging.PeersResult{
		Generation: "g",
		Peers:      []messaging.Peer{{RunID: "rt_b", Session: json.RawMessage(`{"run_id":"rt_b","label":"peer-b"}`)}},
	})
	if len(peers.Peers) != 1 || string(peers.Peers[0].Session) != `{"run_id":"rt_b","label":"peer-b"}` {
		t.Fatalf("peers result = %+v", peers)
	}
	if back := peers.PeersResult(); len(back.Peers) != 1 || back.Peers[0].RunID != "rt_b" {
		t.Fatalf("peers round trip = %+v", back)
	}

	preflight := newMessagePreflightResult(messaging.PreflightResult{Protocol: 1, Generation: "g"})
	if back := preflight.PreflightResult(); back.Protocol != 1 || back.Generation != "g" {
		t.Fatalf("preflight round trip = %+v", back)
	}
}

func TestResumeAndSessionConversions(t *testing.T) {
	resume := newResumeResult(messaging.ResumeResult{
		RunID: "run_1", Label: "follow", RuntimeID: "rt_2", SessionID: "ses_2",
		SentinelFile: "/tmp/run_1.done", OnEvent: "/tmp/run_1.log",
	})
	if resume.RunID != "run_1" || resume.SentinelFile != "/tmp/run_1.done" || resume.OnEvent != "/tmp/run_1.log" {
		t.Fatalf("resume result = %+v", resume)
	}
	if back := resume.ResumeResult(); back.RuntimeID != "rt_2" || back.SessionID != "ses_2" {
		t.Fatalf("resume round trip = %+v", back)
	}

	rename := newSessionRenameResult(messaging.RenameResult{RunID: "rt_2", Label: "new", PreviousLabel: "old"})
	if back := rename.RenameResult(); back.Label != "new" || back.PreviousLabel != "old" {
		t.Fatalf("rename round trip = %+v", back)
	}

	last := newSessionLastResult(broker.LastAgentMessage{
		RunID: "rt_2", Label: "peer", Text: "answer", StopReason: "done", Truncated: true, At: 42,
	}, true)
	if !last.HasRecord || last.FinalOutput != "answer" || !last.Truncated || last.At != 42 {
		t.Fatalf("last result = %+v", last)
	}
	if back := last.PeerLast(); !back.HasRecord || back.Text != "answer" || back.StopReason != "done" {
		t.Fatalf("last round trip = %+v", back)
	}
	empty := newSessionLastResult(broker.LastAgentMessage{RunID: "rt_2", Label: "peer"}, false)
	if empty.HasRecord || empty.RunID != "rt_2" || empty.Label != "peer" {
		t.Fatalf("empty last result = %+v", empty)
	}
}

func TestMessageDTOWireKeys(t *testing.T) {
	raw, err := json.Marshal(MessageSendParams{FromRunID: "a", ToRunID: "b", MessageID: "m", Generation: "g", Body: "x", NoReply: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"from_run_id", "to_run_id", "message_id", "generation", "body", "no_reply"} {
		if _, ok := object[key]; !ok {
			t.Fatalf("missing wire key %q in %s", key, raw)
		}
	}
	if _, ok := object["FromRunID"]; ok {
		t.Fatalf("PascalCase key leaked: %s", raw)
	}
	rename, _ := json.Marshal(SessionRenameResult{RunID: "r", Label: "l", PreviousLabel: "p"})
	if string(rename) != `{"run_id":"r","label":"l","previous_label":"p"}` {
		t.Fatalf("rename wire = %s", rename)
	}
}

func TestMessageErrorDataClassifies(t *testing.T) {
	data := messageErrorData(&router.ToolError{Code: "unknown_target", Message: "no run"}, messaging.OutcomeNotSent, "msg_1")
	if data.Code != "unknown_target" || data.WireCode != "invalid-request" || data.Outcome != "not-sent" || data.MessageID != "msg_1" {
		t.Fatalf("tool error data = %+v", data)
	}
	data = messageErrorData(messaging.ErrTargetUnavailable, "", "")
	if data.Code != "target-unavailable" || data.WireCode != "target-unavailable" || data.Outcome != "not-sent" {
		t.Fatalf("wire error data = %+v", data)
	}
	data = messageErrorData(errors.New("boom"), "", "")
	if data.Code != "invalid-request" || data.WireCode != "invalid-request" {
		t.Fatalf("unclassified data = %+v", data)
	}
}
