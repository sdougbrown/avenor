package poo

import (
	"strings"
	"testing"

	"github.com/sdougbrown/poo/wire/protocol"
)

// TestSettleErrorFollowsTheStopReason pins which settles are failures. The
// terminal event carries the reason either way, so Prompt only reports the ones
// a run loop cannot treat as an outcome.
func TestSettleErrorFollowsTheStopReason(t *testing.T) {
	cases := []struct {
		name   string
		reason string
		detail protocol.SettleDetail
		want   string
	}{
		{name: "end turn", reason: "end_turn"},
		{name: "truncated", reason: "output_truncated"},
		{name: "max turns", reason: "max_turns"},
		{
			name:   "rate limited",
			reason: "provider_limit",
			detail: protocol.SettleDetail{Provider: &protocol.ProviderDetail{Kind: "rate_limited"}},
		},
		{
			name:   "quota exhausted",
			reason: "provider_limit",
			detail: protocol.SettleDetail{Provider: &protocol.ProviderDetail{Kind: "usage_limit"}},
		},
		{name: "cancelled", reason: "cancelled", want: "cancelled"},
		{name: "generic failure", reason: "provider_error", detail: protocol.SettleDetail{Text: "boom"}, want: "boom"},
		{
			name:   "provider limit of another kind",
			reason: "provider_limit",
			detail: protocol.SettleDetail{Provider: &protocol.ProviderDetail{Kind: "auth_failed"}},
			want:   "auth_failed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome := classifySettle(tc.reason, tc.detail)
			err := settleError("ses_1", outcome)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("settleError(%s) = %v, want nil: the terminal event carries the reason", tc.reason, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("settleError(%s) = nil, want an error", tc.reason)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("settleError(%s) = %q, want it to name %q", tc.reason, err, tc.want)
			}
		})
	}
}

// TestEmptyMessageDeltaIsDropped proves a delta with no text is not published:
// an empty chunk carries nothing a consumer can read.
func TestEmptyMessageDeltaIsDropped(t *testing.T) {
	empty := translateEvent(sessionEvent{
		SessionID: "ses_1",
		Type:      "message.delta",
		Fields:    map[string]any{"text": "", "kind": "text"},
	})
	if len(empty) != 0 {
		t.Fatalf("an empty delta produced %v, want nothing", empty)
	}

	chunk := translateEvent(sessionEvent{
		SessionID: "ses_1",
		Type:      "message.delta",
		Fields:    map[string]any{"text": "hello", "kind": "thinking"},
	})
	if len(chunk) != 1 || chunk[0].Event != "agent.thought_chunk" {
		t.Fatalf("a thinking delta produced %v, want one agent.thought_chunk", chunk)
	}
}

// TestBackendRecordsTakeTheSharedPrefix pins the pass-through rule for the
// records avenor does not standardize. poo's permission echo takes it like the
// run's progress notes and its jobs do: no name of its own, and its fields
// intact.
func TestBackendRecordsTakeTheSharedPrefix(t *testing.T) {
	for _, name := range []string{"run.settling", "job.started", "provider.retry", "permission.asked", "permission.answered"} {
		got := translateEvent(sessionEvent{
			SessionID: "ses_1",
			Type:      name,
			Fields:    map[string]any{"ask_id": "ask_1"},
		})
		if len(got) != 1 {
			t.Fatalf("%s produced %d events, want one", name, len(got))
		}
		if want := "avenor." + name; got[0].Event != want {
			t.Fatalf("%s became %q, want %q", name, got[0].Event, want)
		}
		if got[0].Fields["ask_id"] != "ask_1" {
			t.Fatalf("%s lost its own fields: %v", name, got[0].Fields)
		}
	}
}

// TestToolResultEventMapsOutcomeToStatus pins what a consumer reads off a tool
// result: the kind, the title naming the tool, and the two-state status poo's
// own outcome reduces to.
func TestToolResultEventMapsOutcomeToStatus(t *testing.T) {
	cases := []struct {
		outcome string
		status  string
	}{
		{outcome: "ok", status: "completed"},
		{outcome: "started", status: "completed"},
		{outcome: "skipped", status: "completed"},
		{outcome: "error", status: "failed"},
		{outcome: "denied", status: "failed"},
		{outcome: "cancelled", status: "failed"},
		{outcome: "interrupted", status: "failed"},
	}

	for _, tc := range cases {
		t.Run(tc.outcome, func(t *testing.T) {
			got := translateEvent(sessionEvent{
				SessionID: "ses_1",
				Type:      "tool.result",
				Fields:    map[string]any{"tool": "shell", "outcome": tc.outcome},
			})
			if len(got) != 1 {
				t.Fatalf("outcome %q produced %d events, want one", tc.outcome, len(got))
			}
			if got[0].Event != "tool.call_update" {
				t.Fatalf("event = %q, want tool.call_update", got[0].Event)
			}
			if status, _ := got[0].Fields["status"].(string); status != tc.status {
				t.Fatalf("outcome %q became status %q, want %q", tc.outcome, status, tc.status)
			}
			if kind, _ := got[0].Fields["kind"].(string); kind != "tool" {
				t.Fatalf("kind = %q, want tool", kind)
			}
			if title, _ := got[0].Fields["title"].(string); title != "shell" {
				t.Fatalf("title = %q, want the tool name", title)
			}
		})
	}
}
