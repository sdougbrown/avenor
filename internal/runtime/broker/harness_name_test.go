package broker

import (
	"testing"
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
}
