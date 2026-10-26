package broker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

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
	for _, info := range b.sessions() {
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
	for _, info := range b.sessions() {
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
	// The response reports the stored label the rename accepted, not the
	// resolved display name the harness name shadows.
	if body["label"] != "model-choice" || body["previous_label"] != "launch-label" {
		t.Fatalf("rename response = %v, want launch-label -> model-choice", body)
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
}

// A run may only name itself. The credential and the body's run_id travel in
// the same JSON body, so withAuth authenticates the named run itself: a body
// naming another run carries that run's name and the caller's token, and is
// refused as unauthorized before the handler runs. This pins that transport
// rejection, so one authenticated run cannot overwrite another run's
// user-defined session name.
func TestSessionNameRejectsAnotherRunsBody(t *testing.T) {
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

	// Forge the body directly: postAuthBroker stamps run_id/token from the
	// authenticated caller, but this test needs a body whose run_id names
	// another run while the credential still authenticates rt_a.
	raw, err := json.Marshal(map[string]any{"run_id": "rt_b", "token": tokenA, "name": "hijack"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post("http://"+b.Addr()+"/session_name", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("post session_name: %v", err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if resp.StatusCode != 401 {
		t.Fatalf("mismatched run_id response = %d %v, want 401 unauthorized", resp.StatusCode, body)
	}
	if got := b.DisplayName("rt_b"); got != "name-b" {
		t.Fatalf("rt_b label after refusal = %q, want name-b", got)
	}
	if got := b.DisplayName("rt_a"); got != "name-a" {
		t.Fatalf("rt_a label after refusal = %q, want name-a", got)
	}
}
