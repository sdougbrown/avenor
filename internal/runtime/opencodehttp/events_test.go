package opencodehttp

import (
	"context"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

func sseFeed(lines ...string) string {
	var b strings.Builder
	for _, l := range lines {
		b.WriteString("data: ")
		b.WriteString(l)
		b.WriteString("\n\n")
	}
	return b.String()
}

func collectEvents(t *testing.T, sseData string) []events.Event {
	t.Helper()
	r := strings.NewReader(sseData)
	out := make(chan events.Event, 64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go readSSEEvents(ctx, r, out)
	var result []events.Event
	for evt := range out {
		result = append(result, evt)
	}
	return result
}

func TestMessagePartDeltaToAgentMessageChunk(t *testing.T) {
	feed := sseFeed(
		`{"type":"message.part.delta","properties":{"sessionID":"ses_1","messageID":"msg_1","partID":"prt_1","field":"text","delta":"hello"}}`,
	)
	events := collectEvents(t, feed)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	e := events[0]
	if e.Event != "agent.message_chunk" {
		t.Errorf("event = %q, want agent.message_chunk", e.Event)
	}
	if e.SessionID != "ses_1" {
		t.Errorf("sessionID = %q, want ses_1", e.SessionID)
	}
	if e.Fields["delta"] != "hello" {
		t.Errorf("delta = %q, want hello", e.Fields["delta"])
	}
}

func TestReasoningPartDeltaToAgentThoughtChunk(t *testing.T) {
	feed := sseFeed(
		`{"type":"message.part.delta","properties":{"sessionID":"ses_1","messageID":"msg_1","partID":"prt_1","field":"reasoning","delta":"thinking..."}}`,
	)
	evts := collectEvents(t, feed)
	if len(evts) != 1 {
		t.Fatalf("got %d events, want 1", len(evts))
	}
	if evts[0].Event != "agent.thought_chunk" {
		t.Errorf("event = %q, want agent.thought_chunk", evts[0].Event)
	}
}

func TestSessionIdleEmitsSessionEnd(t *testing.T) {
	feed := sseFeed(
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"busy"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"idle"}}}`,
		`{"type":"session.idle","properties":{"sessionID":"ses_1"}}`,
	)
	evts := collectEvents(t, feed)
	// Should get one session.end event (the idle transition).
	var endEvents []events.Event
	for _, e := range evts {
		if e.Event == "session.end" {
			endEvents = append(endEvents, e)
		}
	}
	if len(endEvents) != 1 {
		t.Fatalf("got %d session.end events, want 1; all events: %+v", len(endEvents), evts)
	}
	if endEvents[0].Fields["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason = %q, want end_turn", endEvents[0].Fields["stop_reason"])
	}
}

func TestSessionErrorBeforeIdleEmitsCancelled(t *testing.T) {
	feed := sseFeed(
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"busy"}}}`,
		`{"type":"session.error","properties":{"sessionID":"ses_1","error":{"name":"MessageAbortedError","data":{"message":"Aborted"}}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"idle"}}}`,
	)
	evts := collectEvents(t, feed)
	var endEvents []events.Event
	for _, e := range evts {
		if e.Event == "session.end" {
			endEvents = append(endEvents, e)
		}
	}
	if len(endEvents) != 1 {
		t.Fatalf("got %d session.end events, want 1", len(endEvents))
	}
	if endEvents[0].Fields["stop_reason"] != "cancelled" {
		t.Errorf("stop_reason = %q, want cancelled", endEvents[0].Fields["stop_reason"])
	}
}

func TestUnknownErrorMapsToErrorName(t *testing.T) {
	feed := sseFeed(
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"busy"}}}`,
		`{"type":"session.error","properties":{"sessionID":"ses_1","error":{"name":"SomeOtherError"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"idle"}}}`,
	)
	evts := collectEvents(t, feed)
	var endEvents []events.Event
	for _, e := range evts {
		if e.Event == "session.end" {
			endEvents = append(endEvents, e)
		}
	}
	if len(endEvents) != 1 {
		t.Fatalf("got %d session.end events, want 1", len(endEvents))
	}
	if endEvents[0].Fields["stop_reason"] != "SomeOtherError" {
		t.Errorf("stop_reason = %q, want SomeOtherError", endEvents[0].Fields["stop_reason"])
	}
}

func TestConcurrentSessionsDoNotCrossDeliver(t *testing.T) {
	feed := sseFeed(
		`{"type":"session.status","properties":{"sessionID":"ses_a","status":{"type":"busy"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_b","status":{"type":"busy"}}}`,
		`{"type":"session.error","properties":{"sessionID":"ses_a","error":{"name":"MessageAbortedError"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_a","status":{"type":"idle"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_b","status":{"type":"idle"}}}`,
	)
	evts := collectEvents(t, feed)
	var endEvents []events.Event
	for _, e := range evts {
		if e.Event == "session.end" {
			endEvents = append(endEvents, e)
		}
	}
	if len(endEvents) != 2 {
		t.Fatalf("got %d session.end events, want 2", len(endEvents))
	}
	// ses_a should be cancelled, ses_b should be end_turn.
	findEnd := func(sid string) events.Event {
		for _, e := range endEvents {
			if e.SessionID == sid {
				return e
			}
		}
		return events.Event{}
	}
	a := findEnd("ses_a")
	if a.Fields["stop_reason"] != "cancelled" {
		t.Errorf("ses_a stop_reason = %q, want cancelled", a.Fields["stop_reason"])
	}
	b := findEnd("ses_b")
	if b.Fields["stop_reason"] != "end_turn" {
		t.Errorf("ses_b stop_reason = %q, want end_turn", b.Fields["stop_reason"])
	}
}

func TestPermissionRequestMapping(t *testing.T) {
	feed := sseFeed(
		`{"type":"permission.request","properties":{"sessionID":"ses_1","request_id":"req_abc","tool":"bash","options":[{"optionId":"allow","kind":"allow"}]}}`,
	)
	evts := collectEvents(t, feed)
	if len(evts) != 1 {
		t.Fatalf("got %d events, want 1: %+v", len(evts), evts)
	}
	if evts[0].Event != "permission.request" {
		t.Errorf("event = %q, want permission.request", evts[0].Event)
	}
	if evts[0].Fields["request_id"] != "req_abc" {
		t.Errorf("request_id = %v, want req_abc", evts[0].Fields["request_id"])
	}
}

func TestPermissionRequestMappingCamelCaseID(t *testing.T) {
	// opencode server sends camelCase "requestID" — mapper normalizes to snake_case.
	feed := sseFeed(
		`{"type":"permission.request","properties":{"sessionID":"ses_1","requestID":"req_camel","tool":"bash","options":[{"optionId":"allow","kind":"allow"}]}}`,
	)
	evts := collectEvents(t, feed)
	if len(evts) != 1 {
		t.Fatalf("got %d events, want 1: %+v", len(evts), evts)
	}
	if evts[0].Fields["request_id"] != "req_camel" {
		t.Errorf("request_id = %v, want req_camel", evts[0].Fields["request_id"])
	}
	// Verify the raw camelCase key is not leaked.
	if _, ok := evts[0].Fields["requestID"]; ok {
		t.Error("requestID field should be normalized to request_id, not passed through")
	}
}

func TestPermissionAskedMapping(t *testing.T) {
	feed := sseFeed(
		`{"id":"per_123","type":"permission.asked","properties":{"sessionID":"ses_1","id":"per_123","permission":"external_directory","patterns":["/tmp/*"],"always":["/tmp/*"],"metadata":{"filepath":"/tmp/a"},"tool":{"callID":"call_1","messageID":"msg_1"}}}`,
	)
	evts := collectEvents(t, feed)
	if len(evts) != 1 {
		t.Fatalf("got %d events, want 1: %+v", len(evts), evts)
	}
	if evts[0].Event != "permission.request" {
		t.Errorf("event = %q, want permission.request", evts[0].Event)
	}
	if evts[0].Fields["request_id"] != "per_123" {
		t.Errorf("request_id = %v, want per_123", evts[0].Fields["request_id"])
	}
	options, _ := evts[0].Fields["options"].([]any)
	if len(options) != 4 {
		t.Fatalf("options len = %d, want 4: %+v", len(options), options)
	}
	if options[0].(map[string]any)["optionId"] != "reject" {
		t.Errorf("options[0] = %+v, want reject", options[0])
	}
	if options[1].(map[string]any)["optionId"] != "allow_once" {
		t.Errorf("options[1] = %+v, want allow_once", options[1])
	}
	if options[2].(map[string]any)["optionId"] != "per_123" {
		t.Errorf("permission-id alias option = %+v, want per_123", options[2])
	}
	if options[3].(map[string]any)["optionId"] != "allow_always" {
		t.Errorf("options[3] = %+v, want allow_always", options[3])
	}
}

func TestPermissionRequestWithoutSessionIDIsSkipped(t *testing.T) {
	feed := sseFeed(
		`{"type":"permission.request","properties":{"request_id":"req_abc","tool":"bash","options":[{"optionId":"allow","kind":"allow"}]}}`,
	)
	evts := collectEvents(t, feed)
	if len(evts) != 0 {
		t.Fatalf("got %d events, want none: %+v", len(evts), evts)
	}
}

func TestDoubleIdleDoesNotDoubleEmitSessionEnd(t *testing.T) {
	feed := sseFeed(
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"busy"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"idle"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"idle"}}}`,
		`{"type":"session.idle","properties":{"sessionID":"ses_1"}}`,
	)
	evts := collectEvents(t, feed)
	count := 0
	for _, e := range evts {
		if e.Event == "session.end" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("got %d session.end events, want exactly 1", count)
	}
}

func TestBusyWithoutPriorIdleResetsState(t *testing.T) {
	// If we get busy → idle → busy → idle, we should get two session.end events.
	feed := sseFeed(
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"busy"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"idle"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"busy"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"idle"}}}`,
	)
	evts := collectEvents(t, feed)
	count := 0
	for _, e := range evts {
		if e.Event == "session.end" {
			count++
		}
	}
	if count != 2 {
		t.Errorf("got %d session.end events, want 2", count)
	}
}

func TestServerEventsAreSkipped(t *testing.T) {
	feed := sseFeed(
		`{"type":"server.connected","properties":{}}`,
		`{"type":"server.heartbeat","properties":{}}`,
		`{"type":"session.diff","properties":{"sessionID":"ses_1","diff":[]}}`,
		`{"type":"session.updated","properties":{"sessionID":"ses_1","info":{"id":"ses_1"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"busy"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses_1","status":{"type":"idle"}}}`,
		`{"type":"session.idle","properties":{"sessionID":"ses_1"}}`,
	)
	evts := collectEvents(t, feed)
	if len(evts) != 1 {
		t.Fatalf("got %d events, want exactly 1 (session.end): %+v", len(evts), evts)
	}
	if evts[0].Event != "session.end" {
		t.Errorf("event = %q, want session.end", evts[0].Event)
	}
}
