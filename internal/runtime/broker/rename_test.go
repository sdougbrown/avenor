package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestRenameEndpointWithoutHandlerUsesSharedNameRule(t *testing.T) {
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

	// The fallback applies the authority's name rule, so it cannot store a name
	// SessionRename would refuse.
	status, body = postAuthBroker(t, b.Addr(), "/rename", "rt_1", token, map[string]any{"name": "two\nlines\u202Eevil"})
	if status != http.StatusBadRequest || body["code"] != "invalid-name" {
		t.Fatalf("invalid name = %d %v, want 400 invalid-name", status, body)
	}
	if got := b.DisplayName("rt_1"); got != "after" {
		t.Fatalf("label after rejected rename = %q, want after", got)
	}
}

// The decoder /rename and /session_name share rejects a valid-JSON body whose
// name field has the wrong type. Syntactically broken JSON never reaches the
// decoder — withAuth refuses it first — so the wrongly typed name is what
// exercises this path, and the error it writes must not name one endpoint.
func TestRenameDecoderErrorIsEndpointNeutral(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	token, err := b.CreateRun("rt_1", "before")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	for _, path := range []string{"/rename", "/session_name"} {
		status, body := postAuthBroker(t, b.Addr(), path, "rt_1", token, map[string]any{"name": 7})
		if status != http.StatusBadRequest || body["code"] != "invalid-request" {
			t.Fatalf("%s: status = %d, body %v, want 400 invalid-request", path, status, body)
		}
		if msg, _ := body["error"].(string); msg != "request body has invalid fields" {
			t.Fatalf("%s: error = %q, want the endpoint-neutral decode message", path, msg)
		}
		if got := b.DisplayName("rt_1"); got != "before" {
			t.Fatalf("%s: label after rejected body = %q, want before", path, got)
		}
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
			b.SetRenameHandler(func(callerRunID, sessionID, name string, _ bool) (messaging.RenameResult, error) {
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
	b.SetRenameHandler(func(callerRunID, sessionID, name string, _ bool) (messaging.RenameResult, error) {
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

	// A producer that reports complete text still gets a truncated record when
	// the store's own bound is what shortened it.
	if err := b.SetLastAgentMessage("rt_1", long, "done", false); err != nil {
		t.Fatalf("set last: %v", err)
	}
	record, _ = b.LastAgentMessage("rt_1")
	if !record.Truncated || len([]rune(record.Text)) != events.MaxFinalOutputRunes {
		t.Fatalf("bounded record = %d runes truncated=%v, want a preview", len([]rune(record.Text)), record.Truncated)
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

// renameRequestWithCaller builds the request withAuth would hand to handleRename:
// the authenticated caller travels in the context, never in the body.
func renameRequestWithCaller(t *testing.T, caller, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/rename", strings.NewReader(body))
	return req.WithContext(messaging.WithCallerRun(req.Context(), caller))
}

func TestRenameEndpointRefusesAnotherRunsRunID(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	tokenA, err := b.CreateRun("rt_a", "name-a")
	if err != nil {
		t.Fatalf("create run a: %v", err)
	}
	if _, err := b.CreateRun("rt_b", "name-b"); err != nil {
		t.Fatalf("create run b: %v", err)
	}
	b.SetRenameHandler(func(callerRunID, sessionID, name string, _ bool) (messaging.RenameResult, error) {
		if err := b.RenameRun(callerRunID, name); err != nil {
			return messaging.RenameResult{}, err
		}
		return messaging.RenameResult{RunID: callerRunID, Label: name}, nil
	})

	status, body := postAuthBroker(t, b.Addr(), "/rename", "rt_b", tokenA, map[string]any{"name": "hijacked"})
	if status == http.StatusOK {
		t.Fatalf("rename with another run's id succeeded: %v", body)
	}
	if got := b.DisplayName("rt_b"); got != "name-b" {
		t.Fatalf("target label = %q, want name-b", got)
	}
}

func TestRenameHandlerRefusesBodyRunIDThatIsNotTheAuthenticatedCaller(t *testing.T) {
	b := New("")
	if _, err := b.CreateRun("rt_a", "name-a"); err != nil {
		t.Fatalf("create run a: %v", err)
	}
	if _, err := b.CreateRun("rt_b", "name-b"); err != nil {
		t.Fatalf("create run b: %v", err)
	}
	handlerCalled := false
	b.SetRenameHandler(func(callerRunID, sessionID, name string, _ bool) (messaging.RenameResult, error) {
		handlerCalled = true
		return messaging.RenameResult{RunID: callerRunID, Label: name}, nil
	})

	req := renameRequestWithCaller(t, "rt_a", `{"run_id":"rt_b","token":"ignored","name":"hijacked"}`)
	rec := httptest.NewRecorder()
	b.handleRename(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	if handlerCalled {
		t.Fatal("the handler was called with an identity the caller did not authenticate as")
	}
	if got := b.DisplayName("rt_b"); got != "name-b" {
		t.Fatalf("target label = %q, want name-b", got)
	}
}

func TestRenameFallbackRefusesBodyRunIDThatIsNotTheAuthenticatedCaller(t *testing.T) {
	b := New("")
	if _, err := b.CreateRun("rt_a", "name-a"); err != nil {
		t.Fatalf("create run a: %v", err)
	}
	if _, err := b.CreateRun("rt_b", "name-b"); err != nil {
		t.Fatalf("create run b: %v", err)
	}

	req := renameRequestWithCaller(t, "rt_a", `{"run_id":"rt_b","token":"ignored","name":"hijacked"}`)
	rec := httptest.NewRecorder()
	b.handleRename(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	if got := b.DisplayName("rt_b"); got != "name-b" {
		t.Fatalf("target label = %q, want name-b", got)
	}
	if got := b.DisplayName("rt_a"); got != "name-a" {
		t.Fatalf("caller label = %q, want name-a", got)
	}
}

func TestRenameRunRefusesAnotherRunsName(t *testing.T) {
	b := New("")
	for _, id := range []string{"rt_1", "rt_2"} {
		if _, err := b.CreateRun(id, id); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	if err := b.RenameRun("rt_1", "mesh"); err != nil {
		t.Fatalf("first rename: %v", err)
	}
	err := b.RenameRun("rt_2", "mesh")
	var taken *LabelTakenError
	if !errors.As(err, &taken) || taken.HolderRunID != "rt_1" || taken.Label != "mesh" {
		t.Fatalf("second rename error = %v, want a LabelTakenError held by rt_1", err)
	}
	if got := b.DisplayName("rt_2"); got != "rt_2" {
		t.Fatalf("refused rename changed the label to %q", got)
	}
	if err := b.RenameRun("rt_1", "mesh"); err != nil {
		t.Fatalf("renaming a run to the name it already wears: %v", err)
	}
	if err := b.RenameRun("rt_1", ""); err != nil {
		t.Fatalf("resetting a name: %v", err)
	}
	if err := b.RenameRun("rt_2", "mesh"); err != nil {
		t.Fatalf("a name the holder gave up: %v", err)
	}
}

func TestRenameRunDefaultSuffixesOnlyATakenName(t *testing.T) {
	b := New("")
	for _, id := range []string{"claude-a001f5", "claude-b002e6", "squatter"} {
		if _, err := b.CreateRun(id, id); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	if got, err := b.RenameRunDefault("claude-b002e6", "claude-avenor"); err != nil || got != "claude-avenor" {
		t.Fatalf("a free default = %q, %v", got, err)
	}
	if got, err := b.RenameRunDefault("claude-a001f5", "claude-avenor"); err != nil || got != "claude-avenor-a001f5" {
		t.Fatalf("a taken default = %q, %v, want claude-avenor-a001f5", got, err)
	}
	if got := b.DisplayName("claude-a001f5"); got != "claude-avenor-a001f5" {
		t.Fatalf("label after the default = %q", got)
	}
	// The holder renaming to its own name keeps it.
	if got, err := b.RenameRunDefault("claude-b002e6", "claude-avenor"); err != nil || got != "claude-avenor" {
		t.Fatalf("the holder's own default = %q, %v", got, err)
	}
	// When the tail name is worn too, a counter goes before the tail.
	if err := b.RenameRun("squatter", "mesh-b002e6"); err != nil {
		t.Fatal(err)
	}
	if err := b.RenameRun("claude-a001f5", "mesh"); err != nil {
		t.Fatal(err)
	}
	if got, err := b.RenameRunDefault("claude-b002e6", "mesh"); err != nil || got != "mesh-2-b002e6" {
		t.Fatalf("a default whose tail name is taken = %q, %v, want mesh-2-b002e6", got, err)
	}
}

func TestConcurrentRenamesToOneNameHaveOneWinner(t *testing.T) {
	const n = 16
	// The race is a short window, so the same contest runs many times.
	for round := 0; round < 100; round++ {
		b := New("")
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("claude-%06x", i+1)
			if _, err := b.CreateRun(ids[i], ids[i]); err != nil {
				t.Fatal(err)
			}
		}
		contest := func(call func(id string) error) []error {
			start := make(chan struct{})
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i, id := range ids {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					errs[i] = call(id)
				}()
			}
			close(start)
			wg.Wait()
			return errs
		}
		won := 0
		for _, err := range contest(func(id string) error { return b.RenameRun(id, "same") }) {
			var taken *LabelTakenError
			switch {
			case err == nil:
				won++
			case !errors.As(err, &taken):
				t.Fatalf("unexpected error: %v", err)
			}
		}
		if won != 1 {
			t.Fatalf("round %d: %d runs won the name, want exactly one", round, won)
		}

		// Defaults chosen at once all succeed, each with a different name.
		var mu sync.Mutex
		names := map[string]bool{}
		for _, err := range contest(func(id string) error {
			got, err := b.RenameRunDefault(id, "shared")
			mu.Lock()
			names[got] = true
			mu.Unlock()
			return err
		}) {
			if err != nil {
				t.Fatalf("round %d: a default was refused: %v", round, err)
			}
		}
		if len(names) != n {
			t.Fatalf("round %d: %d distinct default names for %d runs: %v", round, len(names), n, names)
		}
	}
}

func TestRenameRunCountsAChildBrokersRunButNotALapsedPeer(t *testing.T) {
	b := New("")
	b.ownRun(messaging.ClaimedRun{Key: "desc", RunClaim: messaging.RunClaim{RunID: "desc", Label: "branch-label"}}, true)
	for _, id := range []string{"rt_1", "rt_2", "rt_3"} {
		if _, err := b.CreateRun(id, id); err != nil {
			t.Fatal(err)
		}
	}
	var taken *LabelTakenError
	if err := b.RenameRun("rt_1", "branch-label"); !errors.As(err, &taken) || taken.HolderRunID != "desc" {
		t.Fatalf("renaming to a child broker's label = %v, want it held by desc", err)
	}

	if err := b.RenameRun("rt_2", "worn-by-a-peer"); err != nil {
		t.Fatal(err)
	}
	st := b.GetRun("rt_2")
	st.Mu.Lock()
	st.peer = true
	st.peerDeadline = time.Now().Add(-time.Second)
	st.Mu.Unlock()
	if err := b.RenameRun("rt_3", "worn-by-a-peer"); err != nil {
		t.Fatalf("a lapsed peer's name was held: %v", err)
	}
}

func TestRenameRunKeepsANameAnUncheckedWriterDuplicated(t *testing.T) {
	b := New("")
	for _, id := range []string{"rt_1", "rt_2"} {
		if _, err := b.CreateRun(id, "worker"); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.RenameRun("rt_1", "worker"); err != nil {
		t.Fatalf("a run renaming to the name it already wears: %v", err)
	}
}

func TestRenameEndpointRefusesATakenNameUnlessItIsADefault(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	holderToken, err := b.CreateRun("claude-a001f5", "claude-a001f5")
	if err != nil {
		t.Fatal(err)
	}
	token, err := b.CreateRun("claude-b002e6", "claude-b002e6")
	if err != nil {
		t.Fatal(err)
	}
	if status, body := postAuthBroker(t, b.Addr(), "/rename", "claude-a001f5", holderToken, map[string]any{"name": "claude-avenor"}); status != http.StatusOK {
		t.Fatalf("holder rename = %d %v", status, body)
	}

	status, body := postAuthBroker(t, b.Addr(), "/rename", "claude-b002e6", token, map[string]any{"name": "claude-avenor"})
	if status != http.StatusBadRequest || body["code"] != "invalid-name" || !strings.Contains(body["error"].(string), "already held by claude-a001f5") {
		t.Fatalf("explicit rename to a taken name = %d %v, want 400 invalid-name naming the holder", status, body)
	}
	if got := b.DisplayName("claude-b002e6"); got != "claude-b002e6" {
		t.Fatalf("label after the refusal = %q", got)
	}

	status, body = postAuthBroker(t, b.Addr(), "/rename", "claude-b002e6", token, map[string]any{"name": "claude-avenor", "default": true})
	if status != http.StatusOK || body["label"] != "claude-avenor-b002e6" {
		t.Fatalf("default rename to a taken name = %d %v, want 200 claude-avenor-b002e6", status, body)
	}

	// A default whose name is free is kept as chosen.
	status, body = postAuthBroker(t, b.Addr(), "/rename", "claude-b002e6", token, map[string]any{"name": "claude-other", "default": true})
	if status != http.StatusOK || body["label"] != "claude-other" {
		t.Fatalf("default rename to a free name = %d %v", status, body)
	}

	// A default that is not a valid name keeps the rule's refusal.
	status, body = postAuthBroker(t, b.Addr(), "/rename", "claude-b002e6", token, map[string]any{"name": "two\nlines", "default": true})
	if status != http.StatusBadRequest || body["code"] != "invalid-name" {
		t.Fatalf("invalid default = %d %v", status, body)
	}
}

func TestSessionNameEndpointStaysUnchangedByLabelHolders(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	holderToken, _ := b.CreateRun("rt_1", "rt_1")
	token, _ := b.CreateRun("rt_2", "rt_2")
	postAuthBroker(t, b.Addr(), "/rename", "rt_1", holderToken, map[string]any{"name": "shared"})
	// The harness-name route is not one of the checked paths: a person's name
	// from the host is published as given.
	if status, body := postAuthBroker(t, b.Addr(), "/session_name", "rt_2", token, map[string]any{"name": "shared"}); status != http.StatusOK {
		t.Fatalf("session name = %d %v", status, body)
	}
}

func TestRenameEndpointHandsTheDefaultSwitchToTheAuthority(t *testing.T) {
	b := New("")
	var gotName string
	var gotDefault bool
	b.SetRenameHandler(func(callerRunID, sessionID, name string, asDefault bool) (messaging.RenameResult, error) {
		gotName, gotDefault = name, asDefault
		return messaging.RenameResult{RunID: callerRunID, Label: name + "-tail", PreviousLabel: callerRunID}, nil
	})
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	token, _ := b.CreateRun("rt_1", "rt_1")
	status, body := postAuthBroker(t, b.Addr(), "/rename", "rt_1", token, map[string]any{"name": "claude-avenor", "default": true})
	if status != http.StatusOK || gotName != "claude-avenor" || !gotDefault || body["label"] != "claude-avenor-tail" {
		t.Fatalf("default rename = %d %v, handler got %q default=%v", status, body, gotName, gotDefault)
	}
	status, _ = postAuthBroker(t, b.Addr(), "/rename", "rt_1", token, map[string]any{"name": "claude-avenor"})
	if status != http.StatusOK || gotDefault {
		t.Fatalf("explicit rename reached the handler with default=%v", gotDefault)
	}
}
