package events

import "testing"

func TestSessionTrackerClaudeChannelLifecycle(t *testing.T) {
	tracker := NewSessionTracker("ses-1")
	events := []Event{
		{Event: "session.start", SessionID: "ses-1", Fields: map[string]any{"backend": "claude-channel"}},
		{Event: "agent.channel_ready", SessionID: "ses-1", Fields: map[string]any{"run_id": "run-1", "server_name": "avenor-channel-abcd1234"}},
		{Event: "agent.prompt_queued", SessionID: "ses-1", Fields: map[string]any{"control_id": "c1", "message_type": "continue"}},
		{Event: "agent.prompt_submitted", SessionID: "ses-1", Fields: map[string]any{"delivery": "tmux"}},
		{Event: "agent.report", SessionID: "ses-1", Fields: map[string]any{"state": "checkpoint"}},
		{Event: "agent.reply", SessionID: "ses-1", Fields: map[string]any{"to": "jockey"}},
		{Event: "agent.finish", SessionID: "ses-1", Fields: map[string]any{"status": "done", "summary": "updated docs", "files_changed": []any{"docs/events.md", "docs/watch.md"}}},
		{Event: "session.end", SessionID: "ses-1", Fields: map[string]any{"stop_reason": "end_turn"}},
	}
	for _, ev := range events {
		if !tracker.Observe(ev) {
			t.Fatalf("expected state change for event %s", ev.Event)
		}
	}
	state := tracker.Snapshot()
	if state.Backend != "claude-channel" {
		t.Fatalf("Backend = %q, want claude-channel", state.Backend)
	}
	if state.RunID != "run-1" {
		t.Fatalf("RunID = %q, want run-1", state.RunID)
	}
	if !state.ChannelReady || !state.PromptQueued || !state.PromptSubmitted {
		t.Fatalf("expected channel-ready prompt lifecycle flags, got %+v", state)
	}
	if state.LastReportState != "checkpoint" {
		t.Fatalf("LastReportState = %q, want checkpoint", state.LastReportState)
	}
	if state.LastReplyTo != "jockey" {
		t.Fatalf("LastReplyTo = %q, want jockey", state.LastReplyTo)
	}
	if state.FinishStatus != "done" || state.FinishSummary != "updated docs" {
		t.Fatalf("unexpected finish state: %+v", state)
	}
	if len(state.FilesChanged) != 2 || state.FilesChanged[0] != "docs/events.md" {
		t.Fatalf("unexpected FilesChanged: %+v", state.FilesChanged)
	}
	if !state.Ended || state.StopReason != "end_turn" || state.Phase != "done" {
		t.Fatalf("unexpected terminal state: %+v", state)
	}
}

func TestSessionTrackerPermissionFlow(t *testing.T) {
	tracker := NewSessionTracker("ses-2")
	tracker.Observe(Event{Event: "agent.status", SessionID: "ses-2", Fields: map[string]any{"phase": "working"}})
	tracker.Observe(Event{Event: "permission.request", SessionID: "ses-2", Fields: map[string]any{"question": "allow edit?"}})
	state := tracker.Snapshot()
	if state.Phase != "waiting" || !state.WaitingPermission {
		t.Fatalf("expected waiting permission state, got %+v", state)
	}
	tracker.Observe(Event{Event: "permission.response", SessionID: "ses-2", Fields: map[string]any{"kind": "allow"}})
	tracker.Observe(Event{Event: "agent.status", SessionID: "ses-2", Fields: map[string]any{"phase": "working"}})
	state = tracker.Snapshot()
	if state.WaitingPermission {
		t.Fatalf("expected permission wait cleared, got %+v", state)
	}
	if state.Phase != "working" {
		t.Fatalf("Phase = %q, want working", state.Phase)
	}
}

func TestSessionTrackerIgnoresDifferentSession(t *testing.T) {
	tracker := NewSessionTracker("ses-3")
	if tracker.Observe(Event{Event: "session.start", SessionID: "other", Fields: map[string]any{"backend": "claude-channel"}}) {
		t.Fatal("expected different session to be ignored")
	}
	state := tracker.Snapshot()
	if state.Backend != "" {
		t.Fatalf("expected empty state, got %+v", state)
	}
}
