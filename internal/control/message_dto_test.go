package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

func TestMessageRequestUsesAuthenticatedIdentity(t *testing.T) {
	params := MessageSendParams{
		ToRunID: "rt_peer", MessageID: "msg_1",
		Generation: "gen_1", Body: `{"message":"hi"}`, NoReply: true,
	}
	req := params.request(messaging.Identity{RunID: "supervisor"})
	if req.From.RunID != "supervisor" || req.From.Sender != "" {
		t.Fatalf("from = %+v, want the authenticated identity with no sender name", req.From)
	}
	if req.To.RunID != "rt_peer" || req.MessageID != "msg_1" || req.Generation != "gen_1" || req.Body != `{"message":"hi"}` || !req.NoReply {
		t.Fatalf("request = %+v", req)
	}
}

// TestMessageWaitParamsLeavesTimeoutToDispatcher pins the layering: the DTO
// converts units only. The control dispatcher owns the one timeout ceiling, so
// a value above it must not be clamped a second time here.
func TestMessageWaitParamsLeavesTimeoutToDispatcher(t *testing.T) {
	over := MessageWaitParams{MessageID: "m", TimeoutMS: int64(messaging.MaxWait/time.Millisecond) + 250}
	if got, want := over.request(messaging.Identity{RunID: "rt_a"}).Timeout, time.Duration(over.TimeoutMS)*time.Millisecond; got != want {
		t.Fatalf("over-cap timeout = %v, want the DTO to leave it at %v", got, want)
	}
	zero := MessageWaitParams{MessageID: "m", TimeoutMS: 0}
	if got := zero.request(messaging.Identity{RunID: "rt_a"}).Timeout; got != 0 {
		t.Fatalf("zero timeout = %v, want 0", got)
	}
	wait := NewMessageWaitParams(messaging.WaitRequest{
		Sender: messaging.Identity{RunID: "rt_a"}, MessageID: "m", Generation: "g",
		Timeout: 1500 * time.Millisecond,
	})
	if wait.TimeoutMS != 1500 || wait.MessageID != "m" {
		t.Fatalf("wait params = %+v", wait)
	}
}

// TestTerminalDTOConversions pins the two conversions that carry real risk: the
// fence deadline unit and the last-message has_record/truncated facts.
func TestTerminalDTOConversions(t *testing.T) {
	fence := newMessageWithdrawResult(messaging.WithdrawResult{
		Outcome: messaging.OutcomeWithdrawn,
		Fence:   messaging.FenceAck{Sender: "rt_a", MessageID: "m", Generation: "g", Deadline: time.UnixMilli(1234), Terminal: true},
	})
	if fence.Fence == nil || fence.Fence.Deadline != 1234 || !fence.Fence.Terminal {
		t.Fatalf("withdraw result = %+v", fence)
	}
	if back := fence.WithdrawResult(); back.Fence.Deadline.UnixMilli() != 1234 {
		t.Fatalf("withdraw round trip = %+v", back)
	}

	last := newSessionLastResult(broker.LastAgentMessage{
		RunID: "rt_2", Label: "peer", Text: "answer", StopReason: "done", Truncated: true, At: 42,
	}, true)
	if !last.HasRecord || !last.Truncated || last.At != 42 {
		t.Fatalf("last result = %+v", last)
	}
	empty := newSessionLastResult(broker.LastAgentMessage{RunID: "rt_2", Label: "peer"}, false)
	if empty.HasRecord || empty.RunID != "rt_2" {
		t.Fatalf("empty last result = %+v", empty)
	}
}

func TestMessageDTOWireKeys(t *testing.T) {
	raw, err := json.Marshal(MessageSendParams{ToRunID: "b", MessageID: "m", Generation: "g", Body: "x", NoReply: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"to_run_id", "message_id", "generation", "body", "no_reply"} {
		if _, ok := object[key]; !ok {
			t.Fatalf("missing wire key %q in %s", key, raw)
		}
	}
	if _, ok := object["from_run_id"]; ok {
		t.Fatalf("the request carries a from_run_id the server ignores: %s", raw)
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
	// A wrapped tool error still classifies, matching the broker's errors.As.
	data = messageErrorData(fmt.Errorf("rename: %w", &router.ToolError{Code: "not_permitted"}), "", "")
	if data.Code != "not_permitted" {
		t.Fatalf("wrapped tool error data = %+v", data)
	}
}
