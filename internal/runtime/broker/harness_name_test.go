package broker

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// The harness-supplied session name is the human's choice, so it is the name
// peers see as soon as it exists.
func TestHarnessNameIsWhatPeersSee(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	token, err := b.CreateRun("rt_h", "launch-label")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	status, body := postAuthBroker(t, b.Addr(), "/session_name", "rt_h", token, map[string]any{"name": "my-session"})
	if status != 200 {
		t.Fatalf("session_name status = %d (%v)", status, body)
	}
	if got := body["label"]; got != "my-session" {
		t.Fatalf("response label = %v, want my-session", got)
	}

	if got := b.DisplayName("rt_h"); got != "my-session" {
		t.Fatalf("DisplayName = %q, want my-session", got)
	}
	for _, info := range b.Sessions() {
		if info.RunID != "rt_h" {
			continue
		}
		if info.Label != "my-session" {
			t.Fatalf("session label = %q, want my-session", info.Label)
		}
	}
	if record, _ := b.LastAgentMessage("rt_h"); record.Label != "my-session" {
		t.Fatalf("last message label = %q, want my-session", record.Label)
	}
	if got := b.MatchLabel("my-session"); len(got) != 1 || got[0] != "rt_h" {
		t.Fatalf("MatchLabel(my-session) = %v, want [rt_h]", got)
	}
	if got := b.MatchLabel("launch-label"); len(got) != 0 {
		t.Fatalf("MatchLabel(launch-label) = %v, want no match", got)
	}
}

// A model rename writes its own field: the harness name keeps winning, and the
// model name is still recorded so clearing the harness name reveals it.
func TestModelRenameDoesNotClobberHarnessName(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	token, err := b.CreateRun("rt_h", "launch-label")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := b.SetHarnessName("rt_h", "my-session"); err != nil {
		t.Fatalf("SetHarnessName: %v", err)
	}

	status, body := postAuthBroker(t, b.Addr(), "/rename", "rt_h", token, map[string]any{"name": "model-choice"})
	if status != 200 {
		t.Fatalf("rename status = %d (%v)", status, body)
	}
	if got := b.DisplayName("rt_h"); got != "my-session" {
		t.Fatalf("DisplayName after model rename = %q, want my-session", got)
	}
	// Clearing the harness name falls back to the model's name, not the launch
	// label, so the earlier rename was recorded rather than discarded.
	if err := b.SetHarnessName("rt_h", ""); err != nil {
		t.Fatalf("clear harness name: %v", err)
	}
	if got := b.DisplayName("rt_h"); got != "model-choice" {
		t.Fatalf("DisplayName after clear = %q, want model-choice", got)
	}
}

// A model name set before the harness names its session does not block the
// later harness name.
func TestHarnessNameArrivingAfterModelNameWins(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	token, err := b.CreateRun("rt_h", "launch-label")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := b.RenameRun("rt_h", "model-choice"); err != nil {
		t.Fatalf("RenameRun: %v", err)
	}
	if got := b.DisplayName("rt_h"); got != "model-choice" {
		t.Fatalf("DisplayName before harness name = %q, want model-choice", got)
	}

	status, body := postAuthBroker(t, b.Addr(), "/session_name", "rt_h", token, map[string]any{"name": "my-session"})
	if status != 200 {
		t.Fatalf("session_name status = %d (%v)", status, body)
	}
	if got := b.DisplayName("rt_h"); got != "my-session" {
		t.Fatalf("DisplayName after harness name = %q, want my-session", got)
	}
}

// Neither source present keeps today's behaviour: the launch label, then the
// run ID.
func TestHarnessNameAbsentFallsBackToLaunchLabelAndRunID(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	token, err := b.CreateRun("rt_launch", "launch-label")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if got := b.DisplayName("rt_launch"); got != "launch-label" {
		t.Fatalf("DisplayName = %q, want launch-label", got)
	}
	// Clearing a name the harness never set is not a write that changes anything.
	status, body := postAuthBroker(t, b.Addr(), "/session_name", "rt_launch", token, map[string]any{"name": ""})
	if status != 200 {
		t.Fatalf("session_name status = %d (%v)", status, body)
	}
	if got := b.DisplayName("rt_launch"); got != "launch-label" {
		t.Fatalf("DisplayName after empty harness name = %q, want launch-label", got)
	}

	if _, err := b.CreateRun("rt_bare", ""); err != nil {
		t.Fatalf("create bare run: %v", err)
	}
	if got := b.DisplayName("rt_bare"); got != "rt_bare" {
		t.Fatalf("DisplayName of bare run = %q, want its run ID", got)
	}
}

// A harness name is refused rather than published when peers could not render
// it, and one run's name never lands on another.
func TestHarnessNameOnlyForTheAuthenticatedRun(t *testing.T) {
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

	status, body := postAuthBroker(t, b.Addr(), "/session_name", "rt_a", tokenA, map[string]any{"name": "two\nlines"})
	if status != 400 || body["code"] != "invalid-name" {
		t.Fatalf("invalid-name response = %d %v, want 400 invalid-name", status, body)
	}
	if got := b.DisplayName("rt_a"); got != "name-a" {
		t.Fatalf("rt_a label after refused name = %q, want name-a", got)
	}

	status, body = postAuthBroker(t, b.Addr(), "/session_name", "rt_a", tokenA, map[string]any{"name": "mine"})
	if status != 200 {
		t.Fatalf("session_name status = %d (%v)", status, body)
	}
	if got := b.DisplayName("rt_b"); got != "name-b" {
		t.Fatalf("rt_b label = %q, want name-b", got)
	}

	// A run cannot name another run, and it cannot even try: the credential in
	// the body authenticates rt_a, so a body naming rt_b never passes auth.
	status, body = postAuthBroker(t, b.Addr(), "/session_name", "rt_b", tokenA, map[string]any{"name": "hijack"})
	if status != 401 {
		t.Fatalf("cross-run session_name = %d %v, want 401", status, body)
	}
	if got := b.DisplayName("rt_b"); got != "name-b" {
		t.Fatalf("rt_b label after a hijack attempt = %q, want name-b", got)
	}
}

// TestSessionNameRefusesACrossRunCaller pins the handler's own caller check.
// The HTTP surface cannot reach it — withAuth authenticates the very run_id
// field the body carries — so the request is driven at the handler with a
// context that names a different caller, the mismatch the guard refuses.
func TestSessionNameRefusesACrossRunCaller(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	if _, err := b.CreateRun("rt_a", "name-a"); err != nil {
		t.Fatalf("create run a: %v", err)
	}
	if _, err := b.CreateRun("rt_b", "name-b"); err != nil {
		t.Fatalf("create run b: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/session_name", bytes.NewBufferString(`{"run_id":"rt_b","name":"hijack"}`))
	req = req.WithContext(messaging.WithCallerRun(req.Context(), "rt_a"))
	rec := httptest.NewRecorder()
	b.handleSessionName(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-run handler status = %d, want 403", rec.Code)
	}
	if got := b.DisplayName("rt_b"); got != "name-b" {
		t.Fatalf("rt_b label after a hijack attempt = %q, want name-b", got)
	}
}

// A run with no launch label and no harness name reports its run ID as its
// session label, not an empty string.
func TestSessionsStampsRunIDForUnlabeledRun(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	if _, err := b.CreateRun("rt_bare", ""); err != nil {
		t.Fatalf("create run: %v", err)
	}

	found := false
	for _, info := range b.Sessions() {
		if info.RunID != "rt_bare" {
			continue
		}
		found = true
		if info.Label != "rt_bare" {
			t.Fatalf("session label = %q, want the run ID rt_bare", info.Label)
		}
	}
	if !found {
		t.Fatal("Sessions() did not list rt_bare")
	}
}

// TestSetHarnessNameNotifiesRunObserverOnce pins the notification count of a
// harness session-name write: the run-set observer fires exactly once per set
// and per clear, and each firing sees the resolved label the write produced.
// The mirror fires once per write with the name written, and clearing the
// name falls back to the launch label. The callback order is deliberately not
// pinned: the run-set firing sits before the mirror here and moves after it
// when the duplicate firing upstream is collapsed.
func TestSetHarnessNameNotifiesRunObserverOnce(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	if _, err := b.CreateRun("rt_h", "launch-label"); err != nil {
		t.Fatalf("create run: %v", err)
	}

	var runFires, mirrorFires int
	var runLabel, mirrorName string
	b.SetRunObserver(func() {
		runFires++
		runLabel = b.DisplayName("rt_h")
	})
	b.SetHarnessNameObserver(func(runID, name string) {
		mirrorFires++
		mirrorName = name
	})

	// Set: the run observer fires exactly once and sees the resolved harness
	// name; the mirror fires once with the name written.
	if err := b.SetHarnessName("rt_h", "harness-name"); err != nil {
		t.Fatalf("set harness name: %v", err)
	}
	if runFires != 1 {
		t.Fatalf("run observer fired %d times on set, want exactly 1", runFires)
	}
	if runLabel != "harness-name" {
		t.Fatalf("run observer label on set = %q, want the resolved harness name", runLabel)
	}
	if mirrorFires != 1 || mirrorName != "harness-name" {
		t.Fatalf("mirror fired %d times (name %q) on set, want 1 / harness-name", mirrorFires, mirrorName)
	}

	// Clear: the run observer fires exactly once more and sees the resolved
	// fallback (the launch label); the mirror fires once with the empty name.
	if err := b.SetHarnessName("rt_h", ""); err != nil {
		t.Fatalf("clear harness name: %v", err)
	}
	if runFires != 2 {
		t.Fatalf("run observer fired %d times through set+clear, want exactly 2", runFires)
	}
	if runLabel != "launch-label" {
		t.Fatalf("run observer label on clear = %q, want the resolved fallback launch-label", runLabel)
	}
	if mirrorFires != 2 || mirrorName != "" {
		t.Fatalf("mirror fired %d times (name %q) through set+clear, want 2 / empty", mirrorFires, mirrorName)
	}
}
