package broker

import (
	"encoding/json"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

func TestRecorderSessionEnd(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	if _, err := b.CreateRun("run_rec"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rec := NewRecorder(b, "run_rec")
	rec.Feed(events.Event{
		Event:     "session.end",
		SessionID: "ses_1",
		Fields: map[string]any{
			"stop_reason": "done",
			"exit_code":   0,
		},
	})

	st := b.GetRun("run_rec")
	if st == nil {
		t.Fatal("run not found")
	}
	if len(st.Finishes) != 1 {
		t.Fatalf("expected 1 finish, got %d", len(st.Finishes))
	}
	if st.Finishes[0].Status != "done" {
		t.Errorf("Status = %q, want done", st.Finishes[0].Status)
	}
}

func TestRecorderSessionEndDefaultDone(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	if _, err := b.CreateRun("run_rec2"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rec := NewRecorder(b, "run_rec2")
	rec.Feed(events.Event{
		Event:     "session.end",
		SessionID: "ses_2",
		Fields:    map[string]any{},
	})

	st := b.GetRun("run_rec2")
	if st == nil || len(st.Finishes) != 1 {
		t.Fatalf("expected 1 finish, got %d", len(st.Finishes))
	}
	if st.Finishes[0].Status != "done" {
		t.Errorf("Status = %q, want done", st.Finishes[0].Status)
	}
}

func TestRecorderAgentStatus(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	if _, err := b.CreateRun("run_rec3"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rec := NewRecorder(b, "run_rec3")
	rec.Feed(events.Event{
		Event:     "agent.status",
		SessionID: "ses_3",
		Fields: map[string]any{
			"phase": "executing",
			"label": "building things",
		},
	})

	st := b.GetRun("run_rec3")
	if len(st.Reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(st.Reports))
	}
	if st.Reports[0].State != "status" {
		t.Errorf("State = %q, want status", st.Reports[0].State)
	}
	// Verify payload round-trips.
	var payload map[string]any
	if err := json.Unmarshal(st.Reports[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["phase"] != "executing" {
		t.Errorf("payload phase = %v", payload["phase"])
	}
}

func TestRecorderPermissionRequest(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	if _, err := b.CreateRun("run_rec4"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rec := NewRecorder(b, "run_rec4")
	rec.Feed(events.Event{
		Event:     "permission.request",
		SessionID: "ses_4",
		Fields: map[string]any{
			"request_id": "req_1",
		},
	})

	st := b.GetRun("run_rec4")
	if len(st.Reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(st.Reports))
	}
	if st.Reports[0].State != "permission_requested" {
		t.Errorf("State = %q, want permission_requested", st.Reports[0].State)
	}
}

func TestRecorderUnknownEventIgnored(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	if _, err := b.CreateRun("run_rec5"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rec := NewRecorder(b, "run_rec5")
	rec.Feed(events.Event{
		Event:     "tool.call",
		SessionID: "ses_5",
	})

	st := b.GetRun("run_rec5")
	if len(st.Reports) != 0 || len(st.Finishes) != 0 {
		t.Fatalf("expected no reports/finishes for unknown event, got %d/%d", len(st.Reports), len(st.Finishes))
	}
}

func TestRecorderMessageChunk(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	if _, err := b.CreateRun("run_msg"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rec := NewRecorder(b, "run_msg")
	rec.Feed(events.Event{
		Event:     "agent.message_chunk",
		SessionID: "ses_msg",
		Fields: map[string]any{
			"content": "thinking aloud...",
		},
	})

	st := b.GetRun("run_msg")
	if len(st.Reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(st.Reports))
	}
	if st.Reports[0].State != "thinking" {
		t.Errorf("State = %q, want thinking", st.Reports[0].State)
	}
	var payload map[string]any
	if err := json.Unmarshal(st.Reports[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["content"] != "thinking aloud..." {
		t.Errorf("payload content = %v", payload["content"])
	}
}

func TestRecorderChildQuestion(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	if _, err := b.CreateRun("run_cq"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rec := NewRecorder(b, "run_cq")
	rec.Feed(events.Event{
		Event:     "child.question",
		SessionID: "ses_cq",
		Fields: map[string]any{
			"message": "should I proceed?",
		},
	})

	st := b.GetRun("run_cq")
	if len(st.Reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(st.Reports))
	}
	if st.Reports[0].State != "child_question" {
		t.Errorf("State = %q, want child_question", st.Reports[0].State)
	}
}

func TestRecorderNilBroker(t *testing.T) {
	// Feed on a nil Recorder must not panic.
	var rec *Recorder
	rec.Feed(events.Event{
		Event:     "agent.status",
		SessionID: "ses_nil",
		Fields:    map[string]any{"phase": "test"},
	})

	// Feed with a non-nil Recorder but nil broker must not panic.
	rec2 := &Recorder{broker: nil, runID: "run_nil"}
	rec2.Feed(events.Event{
		Event:     "agent.status",
		SessionID: "ses_nil2",
		Fields:    map[string]any{"phase": "test"},
	})
}
