package events

import "fmt"

// SessionState is a best-effort synthesized view of a live session derived from
// the raw event stream. It is transport-agnostic, but it preserves
// claude-channel-specific lifecycle signals when they are present.
type SessionState struct {
	SessionID         string   `json:"session_id,omitempty"`
	RunID             string   `json:"run_id,omitempty"`
	Backend           string   `json:"backend,omitempty"`
	Phase             string   `json:"phase,omitempty"`
	ChannelReady      bool     `json:"channel_ready,omitempty"`
	PromptQueued      bool     `json:"prompt_queued,omitempty"`
	PromptSubmitted   bool     `json:"prompt_submitted,omitempty"`
	WaitingPermission bool     `json:"waiting_permission,omitempty"`
	LastReportState   string   `json:"last_report_state,omitempty"`
	LastReplyTo       string   `json:"last_reply_to,omitempty"`
	FinishStatus      string   `json:"finish_status,omitempty"`
	FinishSummary     string   `json:"finish_summary,omitempty"`
	FilesChanged      []string `json:"files_changed,omitempty"`
	StopReason        string   `json:"stop_reason,omitempty"`
	Ended             bool     `json:"ended,omitempty"`
	LastEvent         string   `json:"last_event,omitempty"`
}

// SessionTracker incrementally derives SessionState from Avenor events.
type SessionTracker struct {
	state SessionState
}

func NewSessionTracker(sessionID string) *SessionTracker {
	return &SessionTracker{state: SessionState{SessionID: sessionID}}
}

// Observe applies one event to the tracker and reports whether the derived
// state changed.
func (t *SessionTracker) Observe(ev Event) bool {
	changed := false
	if ev.SessionID != "" && ev.SessionID != t.state.SessionID {
		if t.state.SessionID == "" {
			t.state.SessionID = ev.SessionID
			changed = true
		} else {
			return false
		}
	}
	if t.state.LastEvent != ev.Event {
		t.state.LastEvent = ev.Event
		changed = true
	}
	if runID := stringField(ev.Fields, "run_id"); runID != "" && runID != t.state.RunID {
		t.state.RunID = runID
		changed = true
	}

	switch ev.Event {
	case "session.start":
		if backend := stringField(ev.Fields, "backend"); backend != "" && backend != t.state.Backend {
			t.state.Backend = backend
			changed = true
		}
	case "agent.status":
		if phase := stringField(ev.Fields, "phase"); phase != "" && phase != t.state.Phase {
			t.state.Phase = phase
			changed = true
		}
		if t.state.Phase == "waiting" && !t.state.WaitingPermission {
			t.state.WaitingPermission = true
			changed = true
		}
		if (t.state.Phase == "working" || t.state.Phase == "thinking" || t.state.Phase == "done") && t.state.WaitingPermission {
			t.state.WaitingPermission = false
			changed = true
		}
	case "permission.request":
		if !t.state.WaitingPermission {
			t.state.WaitingPermission = true
			changed = true
		}
		if t.state.Phase != "waiting" {
			t.state.Phase = "waiting"
			changed = true
		}
	case "permission.response":
		if t.state.WaitingPermission {
			t.state.WaitingPermission = false
			changed = true
		}
	case "agent.channel_ready":
		if !t.state.ChannelReady {
			t.state.ChannelReady = true
			changed = true
		}
	case "agent.prompt_queued":
		if !t.state.PromptQueued {
			t.state.PromptQueued = true
			changed = true
		}
	case "agent.prompt_submitted":
		if !t.state.PromptSubmitted {
			t.state.PromptSubmitted = true
			changed = true
		}
	case "agent.report":
		if state := stringField(ev.Fields, "state"); state != "" && state != t.state.LastReportState {
			t.state.LastReportState = state
			changed = true
		}
	case "agent.reply":
		if to := stringField(ev.Fields, "to"); to != "" && to != t.state.LastReplyTo {
			t.state.LastReplyTo = to
			changed = true
		}
	case "agent.finish":
		if status := stringField(ev.Fields, "status"); status != "" && status != t.state.FinishStatus {
			t.state.FinishStatus = status
			changed = true
		}
		if summary := stringField(ev.Fields, "summary"); summary != "" && summary != t.state.FinishSummary {
			t.state.FinishSummary = summary
			changed = true
		}
		if files, ok := stringSliceField(ev.Fields, "files_changed"); ok && !equalStrings(files, t.state.FilesChanged) {
			t.state.FilesChanged = files
			changed = true
		}
	case "session.end":
		if stop := stringField(ev.Fields, "stop_reason"); stop != "" && stop != t.state.StopReason {
			t.state.StopReason = stop
			changed = true
		}
		if !t.state.Ended {
			t.state.Ended = true
			changed = true
		}
		if t.state.Phase != "done" {
			t.state.Phase = "done"
			changed = true
		}
		if t.state.WaitingPermission {
			t.state.WaitingPermission = false
			changed = true
		}
	}
	return changed
}

func (t *SessionTracker) Snapshot() SessionState {
	cp := t.state
	if len(cp.FilesChanged) > 0 {
		cp.FilesChanged = append([]string(nil), cp.FilesChanged...)
	}
	return cp
}

func stringField(fields map[string]any, key string) string {
	if fields == nil {
		return ""
	}
	v, _ := fields[key].(string)
	return v
}

func stringSliceField(fields map[string]any, key string) ([]string, bool) {
	if fields == nil {
		return nil, false
	}
	raw, ok := fields[key]
	if !ok || raw == nil {
		return nil, false
	}
	switch v := raw.(type) {
	case []string:
		return append([]string(nil), v...), true
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, false
			}
			out = append(out, s)
		}
		return out, true
	default:
		_ = fmt.Sprintf("%T", v)
		return nil, false
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
