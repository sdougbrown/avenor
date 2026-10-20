package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
)

// postAuthBroker posts an authenticated JSON body to path and returns the
// status and decoded object.
func postAuthBroker(t *testing.T, addr, path, runID, token string, body map[string]any) (int, map[string]any) {
	t.Helper()
	body["run_id"] = runID
	body["token"] = token
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post("http://"+addr+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

func getLastMessage(t *testing.T, addr, runID, token, target string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/last_message?target="+target, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set(messaging.HeaderRunID, runID)
	req.Header.Set(messaging.HeaderRunToken, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get last_message: %v", err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

func TestUpdateSessionInfoPatchKeepsLabelAndKind(t *testing.T) {
	b := New("")
	if _, err := b.CreateRun("rt_1", "launch-label"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	b.UpdateSessionInfo("rt_1", &SessionInfo{
		Kind: "agent", Backend: "pi", Model: "m", Dir: "/repo", Status: "running",
	})
	// A later status-only write must clear nothing and must not touch the label.
	b.UpdateSessionInfo("rt_1", &SessionInfo{Status: "ended"})

	sessions := b.sessions()
	if len(sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(sessions))
	}
	info := sessions[0]
	if info.Label != "launch-label" || info.Kind != "agent" || info.Backend != "pi" ||
		info.Model != "m" || info.Dir != "/repo" || info.Status != "ended" {
		t.Fatalf("session info after status-only patch = %+v", info)
	}
}

func TestRenameRunThenMatchLabelResolvesNewLabelOnly(t *testing.T) {
	b := New("")
	if _, err := b.CreateRun("rt_1", "old-label"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := b.CreateRun("rt_2", "other-label"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := b.RenameRun("rt_1", "new-label"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if got := b.MatchLabel("new-label"); len(got) != 1 || got[0] != "rt_1" {
		t.Fatalf("MatchLabel(new-label) = %v, want [rt_1]", got)
	}
	if got := b.MatchLabel("old-label"); len(got) != 0 {
		t.Fatalf("old label still resolves: %v", got)
	}
	if b.DisplayName("rt_1") != "new-label" {
		t.Fatalf("display name = %q, want new-label", b.DisplayName("rt_1"))
	}
}

func TestRenameEndpointWithoutHandlerIsSelfOnly(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	token, err := b.CreateRun("rt_1", "before")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	status, body := postAuthBroker(t, b.Addr(), "/rename", "rt_1", token, map[string]any{"name": "after"})
	if status != http.StatusOK {
		t.Fatalf("rename status = %d, body %v", status, body)
	}
	if body["run_id"] != "rt_1" || body["label"] != "after" || body["previous_label"] != "before" {
		t.Fatalf("rename body = %v", body)
	}
}

func TestRenameEndpointRows(t *testing.T) {
	rows := []struct {
		name       string
		handlerErr error
		wantStatus int
		wantCode   string
	}{
		{"not permitted", &codedRenameError{code: "not_permitted", message: "the control owner may rename only the supervisor session"}, http.StatusForbidden, "not-permitted"},
		{"invalid name", &codedRenameError{code: "invalid_name", message: "name must not be empty"}, http.StatusBadRequest, "invalid-name"},
		{"unknown target", &codedRenameError{code: "unknown_target", message: `no run matches "ghost"`}, http.StatusNotFound, "unknown-target"},
		{"ambiguous name", &codedRenameError{code: "ambiguous_name", message: `"mesh" matches 2 runs: "rt_1", "rt_2"`}, http.StatusConflict, "ambiguous-name"},
		{"persist failure", errors.New("disk full"), http.StatusBadRequest, "invalid-request"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			b := New("")
			if err := b.Start(); err != nil {
				t.Fatalf("start: %v", err)
			}
			defer b.Stop()
			token, err := b.CreateRun("rt_1", "before")
			if err != nil {
				t.Fatalf("create run: %v", err)
			}
			b.SetRenameHandler(func(callerRunID, sessionID, name string) (messaging.RenameResult, error) {
				return messaging.RenameResult{}, row.handlerErr
			})
			status, body := postAuthBroker(t, b.Addr(), "/rename", "rt_1", token, map[string]any{"name": "after"})
			if status != row.wantStatus {
				t.Fatalf("status = %d, want %d (body %v)", status, row.wantStatus, body)
			}
			if body["code"] != row.wantCode {
				t.Fatalf("code = %v, want %s", body["code"], row.wantCode)
			}
			if body["error"] != row.handlerErr.Error() {
				t.Fatalf("error = %v, want the handler text verbatim", body["error"])
			}
			if got := b.DisplayName("rt_1"); got != "before" {
				t.Fatalf("label after failed rename = %q, want before", got)
			}
		})
	}
}

func TestRenameEndpointSuccessUsesHandlerResult(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	token, err := b.CreateRun("rt_1", "before")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	b.SetRenameHandler(func(callerRunID, sessionID, name string) (messaging.RenameResult, error) {
		if callerRunID != "rt_1" {
			t.Fatalf("caller = %q, want rt_1", callerRunID)
		}
		if sessionID != "supervisor" {
			t.Fatalf("session_id = %q, want forwarded unchanged", sessionID)
		}
		if err := b.RenameRun(callerRunID, name); err != nil {
			return messaging.RenameResult{}, err
		}
		return messaging.RenameResult{RunID: callerRunID, Label: name, PreviousLabel: "before"}, nil
	})
	status, body := postAuthBroker(t, b.Addr(), "/rename", "rt_1", token, map[string]any{"name": "after", "session_id": "supervisor"})
	if status != http.StatusOK {
		t.Fatalf("status = %d, body %v", status, body)
	}
	if body["run_id"] != "rt_1" || body["label"] != "after" || body["previous_label"] != "before" {
		t.Fatalf("body = %v", body)
	}
	if got := b.DisplayName("rt_1"); got != "after" {
		t.Fatalf("display name = %q, want after", got)
	}
}

func TestRenameEndpointRejectsMissingFields(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	token, err := b.CreateRun("rt_1", "")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	for name, body := range map[string]map[string]any{
		"empty name": {"name": ""},
		"no name":    {},
	} {
		status, decoded := postAuthBroker(t, b.Addr(), "/rename", "rt_1", token, body)
		if status != http.StatusBadRequest || decoded["code"] != "invalid-request" {
			t.Fatalf("%s: status = %d, body %v", name, status, decoded)
		}
	}
}

type codedRenameError struct {
	code    string
	message string
}

func (e *codedRenameError) Error() string    { return e.message }
func (e *codedRenameError) ToolCode() string { return e.code }

func TestLastAgentMessageUnknownRunKeepsIdentity(t *testing.T) {
	b := New("")
	b.UpdateSessionInfo("ghost", &SessionInfo{Kind: "agent"})
	record, ok := b.LastAgentMessage("rt_missing")
	if ok {
		t.Fatal("ok = true for a run that never recorded a turn")
	}
	if record.RunID != "rt_missing" || record.Label != "rt_missing" || record.Text != "" {
		t.Fatalf("record = %+v, want synthesized identity only", record)
	}
}

func TestLastAgentMessageBoundsTextAndCarriesTruncation(t *testing.T) {
	b := New("")
	if _, err := b.CreateRun("rt_1", "peer-one"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	long := strings.Repeat("x", events.MaxFinalOutputRunes+10)
	if err := b.SetLastAgentMessage("rt_1", long, "done", true); err != nil {
		t.Fatalf("set last: %v", err)
	}
	record, ok := b.LastAgentMessage("rt_1")
	if !ok {
		t.Fatal("ok = false after a write")
	}
	if len([]rune(record.Text)) != events.MaxFinalOutputRunes {
		t.Fatalf("bounded runes = %d, want %d", len([]rune(record.Text)), events.MaxFinalOutputRunes)
	}
	if !record.Truncated || record.Label != "peer-one" || record.RunID != "rt_1" || record.At == 0 {
		t.Fatalf("record = %+v", record)
	}
}

func TestLastMessageEndpointRows(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	callerToken, err := b.CreateRun("caller", "")
	if err != nil {
		t.Fatalf("create caller: %v", err)
	}
	if _, err := b.CreateRun("rt_target", "target-label"); err != nil {
		t.Fatalf("create target: %v", err)
	}

	// A known target with no turn: a synthesized identity and has_record false.
	status, body := getLastMessage(t, b.Addr(), "caller", callerToken, "rt_target")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body %v", status, body)
	}
	if body["has_record"] != false || body["run_id"] != "rt_target" || body["label"] != "target-label" {
		t.Fatalf("no-record body = %v", body)
	}
	if body["final_output"] != "" || body["stop_reason"] != "" {
		t.Fatalf("no-record body carried text: %v", body)
	}

	// Any authenticated caller may read any known target.
	if err := b.SetLastAgentMessage("rt_target", "the answer", "done", false); err != nil {
		t.Fatalf("set last: %v", err)
	}
	status, body = getLastMessage(t, b.Addr(), "caller", callerToken, "rt_target")
	if status != http.StatusOK || body["has_record"] != true || body["final_output"] != "the answer" {
		t.Fatalf("record body = %d %v", status, body)
	}

	// A target that names no run is 404, never an empty record.
	status, body = getLastMessage(t, b.Addr(), "caller", callerToken, "rt_missing")
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("unknown target = %d %v", status, body)
	}
	if got, _ := body["error"].(string); !strings.Contains(got, "run not found: rt_missing") {
		t.Fatalf("unknown target error = %q", got)
	}

	// Missing target is a bad request.
	status, body = getLastMessage(t, b.Addr(), "caller", callerToken, "")
	if status != http.StatusBadRequest || body["code"] != "invalid-request" {
		t.Fatalf("missing target = %d %v", status, body)
	}
}

func TestLastAgentMessageRecorderWritesEveryTurn(t *testing.T) {
	cases := []struct {
		name       string
		fields     map[string]any
		wantReason string
		wantText   string
		wantTrunc  bool
	}{
		{"empty text", map[string]any{"stop_reason": "cancelled"}, "cancelled", "", false},
		{"error stop reason", map[string]any{"stop_reason": "error", "final_output": "boom"}, "error", "boom", false},
		{"truncated turn", map[string]any{"stop_reason": "done", "final_output": "part", "final_output_truncated": true}, "done", "part", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := New("")
			if _, err := b.CreateRun("rt_rec", "rec-label"); err != nil {
				t.Fatalf("create run: %v", err)
			}
			NewRecorder(b, "rt_rec").Feed(events.Event{Event: "session.end", SessionID: "ses_1", Fields: tc.fields})
			record, ok := b.LastAgentMessage("rt_rec")
			if !ok {
				t.Fatal("ok = false after a completed turn")
			}
			if record.Text != tc.wantText || record.StopReason != tc.wantReason || record.Truncated != tc.wantTrunc {
				t.Fatalf("record = %+v", record)
			}
		})
	}
}

func TestLastAgentMessageRecorderSkipsRunLevelEnd(t *testing.T) {
	b := New("")
	if _, err := b.CreateRun("rt_rec", ""); err != nil {
		t.Fatalf("create run: %v", err)
	}
	rec := NewRecorder(b, "rt_rec")
	rec.Feed(events.Event{Event: "session.end", Fields: map[string]any{
		"stop_reason": "done", "end_kind": "run", "runtime_id": "rt_rec",
	}})
	if _, ok := b.LastAgentMessage("rt_rec"); ok {
		t.Fatal("a run-level end recorded a turn")
	}
	// The run's finish is still recorded; only the last-message store skips it.
	if b.LastFinishStatus("rt_rec") != "done" {
		t.Fatalf("finish status = %q", b.LastFinishStatus("rt_rec"))
	}
}

func TestMatchLabelIsStableAndAmbiguous(t *testing.T) {
	b := New("")
	for _, id := range []string{"rt_b", "rt_a", "rt_c"} {
		if _, err := b.CreateRun(id, "shared"); err != nil {
			t.Fatalf("create run %s: %v", id, err)
		}
	}
	got := b.MatchLabel("shared")
	want := []string{"rt_a", "rt_b", "rt_c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("MatchLabel = %v, want %v", got, want)
	}
}

func TestLastAgentMessageReportsCurrentLabelAfterRename(t *testing.T) {
	b := New("")
	if _, err := b.CreateRun("rt_1", "old-name"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := b.SetLastAgentMessage("rt_1", "the answer", "done", false); err != nil {
		t.Fatalf("set last: %v", err)
	}
	if _, err := b.CreateRun("rt_2", "old-peer"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := b.RenameRun("rt_1", "new-name"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := b.RenameRun("rt_2", "new-peer"); err != nil {
		t.Fatalf("rename: %v", err)
	}

	record, ok := b.LastAgentMessage("rt_1")
	if !ok || record.Label != "new-name" {
		t.Fatalf("record label = %q ok=%v, want the current display name", record.Label, ok)
	}
	// The no-record branch must read the same current name.
	record, ok = b.LastAgentMessage("rt_2")
	if ok || record.Label != "new-peer" {
		t.Fatalf("no-record label = %q ok=%v, want the current display name", record.Label, ok)
	}
}
