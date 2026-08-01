package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newWaitTestServer(t *testing.T, snapshots []map[string]any) (*Server, *fakeClient) {
	t.Helper()
	index := 0
	fake := &fakeClient{}
	fake.statusFunc = func(string) (map[string]any, error) {
		if index >= len(snapshots) {
			return snapshots[len(snapshots)-1], nil
		}
		snapshot := snapshots[index]
		index++
		return snapshot, nil
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.sleep = func(ctx context.Context, _ time.Duration) error {
		return ctx.Err()
	}
	return s, fake
}

func TestWaitConditions(t *testing.T) {
	tests := []struct {
		name       string
		condition  waitCondition
		snapshots  []map[string]any
		wantStatus string
		wantPhase  string
		wantCalls  int
	}{
		{
			name:       "terminal",
			condition:  waitTerminal,
			snapshots:  []map[string]any{{"status": "running", "phase": "working"}, {"status": "done", "phase": "done"}},
			wantStatus: "done",
			wantPhase:  "done",
			wantCalls:  2,
		},
		{
			name:       "already terminal",
			condition:  waitTerminal,
			snapshots:  []map[string]any{{"status": "failed"}},
			wantStatus: "failed",
			wantCalls:  1,
		},
		{
			name:       "phase change",
			condition:  waitPhaseChange,
			snapshots:  []map[string]any{{"status": "running", "phase": ""}, {"status": "running", "phase": ""}, {"status": "running", "phase": "working"}},
			wantStatus: "running",
			wantPhase:  "working",
			wantCalls:  3,
		},
		{
			name:       "phase change terminal no phase change",
			condition:  waitPhaseChange,
			snapshots:  []map[string]any{{"status": "running", "phase": ""}, {"status": "done", "phase": ""}},
			wantStatus: "done",
			wantPhase:  "",
			wantCalls:  2,
		},
		{
			name:       "phase change initially terminal",
			condition:  waitPhaseChange,
			snapshots:  []map[string]any{{"status": "done"}},
			wantStatus: "done",
			wantCalls:  1,
		},
		{
			name:       "turn complete parked idle",
			condition:  waitTurnComplete,
			snapshots:  []map[string]any{{"status": "idle", "phase": "done"}},
			wantStatus: "done",
			wantPhase:  "done",
			wantCalls:  1,
		},
		{
			name:       "permission",
			condition:  waitPermission,
			snapshots:  []map[string]any{{"status": "running", "pending_permission": true}},
			wantStatus: "running",
			wantCalls:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, fake := newWaitTestServer(t, test.snapshots)
			status, timedOut, err := s.waitForRun(context.Background(), fake, "run-1", test.condition, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if timedOut {
				t.Fatal("wait unexpectedly timed out")
			}
			if status["status"] != test.wantStatus || (test.wantPhase != "" && status["phase"] != test.wantPhase) {
				t.Fatalf("status = %#v", status)
			}
			if got := len(fake.statusCapturedRuntimeIDs); got != test.wantCalls {
				t.Fatalf("status calls = %d, want %d", got, test.wantCalls)
			}
		})
	}
}

func TestWaitPermissionInterruptsEveryCondition(t *testing.T) {
	for _, condition := range []waitCondition{waitTerminal, waitPhaseChange, waitTurnComplete, waitPermission} {
		t.Run(string(condition), func(t *testing.T) {
			s, fake := newWaitTestServer(t, []map[string]any{{"status": "running", "phase": "working", "pending_permission": true}})
			status, timedOut, err := s.waitForRun(context.Background(), fake, "run-1", condition, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if timedOut || !hasPendingPermission(status) {
				t.Fatalf("status = %#v, timed_out = %v", status, timedOut)
			}
			if len(fake.statusCapturedRuntimeIDs) != 1 {
				t.Fatalf("status calls = %d, want 1", len(fake.statusCapturedRuntimeIDs))
			}
		})
	}
}

func TestWaitPendingPermissionRepresentations(t *testing.T) {
	tests := []struct {
		name      string
		snapshot  map[string]any
		wantCalls int
	}{
		{name: "boolean", snapshot: map[string]any{"status": "running", "pending_permission": true}, wantCalls: 1},
		{name: "legacy object", snapshot: map[string]any{"status": "running", "pending_permission": map[string]any{"request_id": "req-1"}}, wantCalls: 1},
		{name: "legacy waiting status", snapshot: map[string]any{"status": "waiting"}, wantCalls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, fake := newWaitTestServer(t, []map[string]any{test.snapshot})
			status, _, err := s.waitForRun(context.Background(), fake, "run-1", waitTerminal, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if !hasPendingPermission(status) || len(fake.statusCapturedRuntimeIDs) != test.wantCalls {
				t.Fatalf("status = %#v, calls = %d", status, len(fake.statusCapturedRuntimeIDs))
			}
		})
	}
}

func TestWaitPendingPermissionClearingAndEventAlone(t *testing.T) {
	s, fake := newWaitTestServer(t, []map[string]any{
		{"status": "running", "pending_permission": false, "permission": map[string]any{"request_id": "history-only"}},
		{"status": "running", "pending_permission": true},
	})
	status, _, err := s.waitForRun(context.Background(), fake, "run-1", waitTerminal, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasPendingPermission(status) || len(fake.statusCapturedRuntimeIDs) != 2 {
		t.Fatalf("status = %#v, calls = %d", status, len(fake.statusCapturedRuntimeIDs))
	}
}

func TestWaitTurnCompleteTerminalStates(t *testing.T) {
	for _, state := range []string{"done", "failed", "timeout", "killed"} {
		t.Run(state, func(t *testing.T) {
			s, fake := newWaitTestServer(t, []map[string]any{{"status": state}})
			status, timedOut, err := s.waitForRun(context.Background(), fake, "run-1", waitTurnComplete, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if timedOut || status["status"] != state || len(fake.statusCapturedRuntimeIDs) != 1 {
				t.Fatalf("status = %#v, timed_out = %v, calls = %d", status, timedOut, len(fake.statusCapturedRuntimeIDs))
			}
		})
	}
}

func TestWaitDoesNotPromoteActiveTerminalPhase(t *testing.T) {
	s, fake := newWaitTestServer(t, []map[string]any{
		{"status": "running", "phase": "done"},
		{"status": "idle", "phase": "done"},
	})
	status, timedOut, err := s.waitForRun(context.Background(), fake, "run-1", waitTurnComplete, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if timedOut || status["status"] != "done" || len(fake.statusCapturedRuntimeIDs) != 2 {
		t.Fatalf("status = %#v, timed_out = %v, calls = %d", status, timedOut, len(fake.statusCapturedRuntimeIDs))
	}
}

func TestWaitTimeoutReturnsLatestSnapshot(t *testing.T) {
	now := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	s, fake := newWaitTestServer(t, []map[string]any{{"status": "running", "phase": "working"}})
	s.clock = func() time.Time { return now }
	s.sleep = func(ctx context.Context, delay time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		now = now.Add(delay)
		return nil
	}

	status, timedOut, err := s.waitForRun(context.Background(), fake, "run-1", waitTerminal, now.Add(2500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if !timedOut || status["status"] != "running" || status["phase"] != "working" {
		t.Fatalf("status = %#v, timed_out = %v", status, timedOut)
	}
	if len(fake.statusCapturedRuntimeIDs) != 4 {
		t.Fatalf("status calls = %d, want 4", len(fake.statusCapturedRuntimeIDs))
	}
}

func TestWaitCancellationAndStatusError(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s, fake := newWaitTestServer(t, []map[string]any{{"status": "running"}})
		s.sleep = func(context.Context, time.Duration) error {
			cancel()
			return context.Canceled
		}
		_, _, err := s.waitForRun(ctx, fake, "run-1", waitTerminal, time.Time{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context canceled", err)
		}
	})

	t.Run("status error", func(t *testing.T) {
		cause := errors.New("control plane unavailable")
		fake := &fakeClient{statusErr: cause}
		s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = s.waitForRun(context.Background(), fake, "run-1", waitTerminal, time.Time{})
		if !strings.Contains(err.Error(), "status: control plane unavailable") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestWaitValidationAndTimeoutGrammar(t *testing.T) {
	if _, err := parseWaitCondition("bogus"); err == nil {
		t.Fatal("expected invalid wait condition error")
	}
	if _, _, err := (&Server{}).waitForRun(context.Background(), &fakeClient{}, "run-1", waitCondition("bogus"), time.Time{}); err == nil {
		t.Fatal("expected invalid engine condition error")
	}
	for _, value := range []string{"2", "30s", "5m", "1h"} {
		if seconds, err := parseTimeoutSeconds(value); err != nil || seconds <= 0 {
			t.Fatalf("parseTimeoutSeconds(%q) = %d, %v", value, seconds, err)
		}
	}
	if _, err := parseTimeoutSeconds("0s"); err == nil {
		t.Fatal("expected zero timeout error")
	}
}

func TestAvenorStatusWaitValidationAndLifecycleTimeout(t *testing.T) {
	fake := &fakeClient{statusResult: map[string]any{"status": "running"}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range []statusArgs{
		{WaitFor: "terminal"},
		{RunID: "run-1", WaitFor: "bogus"},
		{RunID: "run-1", Timeout: "1s"},
	} {
		if _, _, err := s.handleAvenorStatus(context.Background(), nil, args); err == nil {
			t.Fatalf("args %#v unexpectedly succeeded", args)
		}
	}
	if len(fake.statusCapturedRuntimeIDs) != 0 {
		t.Fatalf("validation acquired client and queried status: %v", fake.statusCapturedRuntimeIDs)
	}

	now := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	fake.statusFunc = func(runtimeID string) (map[string]any, error) {
		return map[string]any{"runtime_id": runtimeID, "status": "running", "phase": "working"}, nil
	}
	s.clock = func() time.Time { return now }
	s.sleep = func(ctx context.Context, delay time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		now = now.Add(delay)
		return nil
	}
	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{
		RunID: "run-1", View: "lifecycle", WaitFor: "terminal", Timeout: "1s",
	})
	if err != nil {
		t.Fatal(err)
	}
	status := result.(map[string]any)
	if status["timed_out"] != true || status["status"] != "running" || status["phase"] != "working" {
		t.Fatalf("status = %#v", status)
	}
}

func TestWaitAutoApprovedFollowUpLifecycle(t *testing.T) {
	var followUpRuntimeID string
	spawnCount := 0
	fake := &fakeClient{
		spawnFunc: func(params map[string]any) (map[string]any, error) {
			spawnCount++
			followUpRuntimeID = fmt.Sprintf("rt_followup_%d", spawnCount)
			return map[string]any{
				"runtime_id": followUpRuntimeID,
				"session_id": fmt.Sprintf("ses_followup_%d", spawnCount),
			}, nil
		},
	}
	// Return running for any runtime until the second call for the follow-up.
	callCount := 0
	fake.statusFunc = func(runtimeID string) (map[string]any, error) {
		fake.statusCapturedRuntimeIDs = append(fake.statusCapturedRuntimeIDs, runtimeID)
		if runtimeID == followUpRuntimeID {
			callCount++
			if callCount <= 1 {
				return map[string]any{
					"runtime_id":         runtimeID,
					"status":             "running",
					"phase":              "working",
					"pending_permission": false,
				}, nil
			}
			return map[string]any{
				"runtime_id":         runtimeID,
				"status":             "idle",
				"phase":              "done",
				"pending_permission": false,
			}, nil
		}
		return map[string]any{"runtime_id": runtimeID, "status": "running"}, nil
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.sleep = func(ctx context.Context, _ time.Duration) error {
		return ctx.Err()
	}

	// Store a prior run with AutoApprove so the follow-up inherits it.
	s.registry.Store(&RunInfo{
		RunID:        "run-auto-prior",
		Label:        "auto-prior",
		RuntimeID:    "rt_auto_prior",
		SessionID:    "ses_auto_prior",
		SentinelPath: filepath.Join(t.TempDir(), "missing.done"),
		Agent:        "claude",
		Backend:      "pi",
		Dir:          "/tmp/auto-repo",
		AutoApprove:  true,
	})

	// Create a follow-up that inherits auto_approve.
	_, followUpResult, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   "run-auto-prior",
		Message: "continue",
	})
	if err != nil {
		t.Fatal(err)
	}
	followUpRunID := followUpResult.(map[string]any)["run_id"].(string)
	if followUpRunID == "" {
		t.Fatal("expected follow-up run ID")
	}
	if ri := s.registry.Lookup(followUpRunID); ri == nil || !ri.AutoApprove {
		t.Fatalf("follow-up registry entry has no auto-approve: %#v", ri)
	}

	// Wait for the follow-up run to complete.
	_, statusResult, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{
		RunID:   followUpRunID,
		WaitFor: "turn_complete",
	})
	if err != nil {
		t.Fatal(err)
	}
	status := statusResult.(map[string]any)
	if status["status"] != "done" || hasPendingPermission(status) {
		t.Fatalf("status = %#v, calls = %d", status, len(fake.statusCapturedRuntimeIDs))
	}
	// Verify status was queried at least twice (running then terminal).
	if len(fake.statusCapturedRuntimeIDs) < 2 {
		t.Fatalf("status calls = %d, want >= 2", len(fake.statusCapturedRuntimeIDs))
	}
}

func TestAvenorResultPermissionInterruptWhileRunning(t *testing.T) {
	fake := &fakeClient{statusResult: map[string]any{
		"runtime_id":         "rt-permission",
		"status":             "running",
		"pending_permission": true,
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "rt-permission"})
	if err != nil {
		t.Fatal(err)
	}
	result := value.(map[string]any)
	if result["ready"] != false || result["status"] != "running" || result["pending_permission"] != true {
		t.Fatalf("result = %#v", result)
	}
	if len(fake.statusCapturedRuntimeIDs) != 1 {
		t.Fatalf("status calls = %d, want 1", len(fake.statusCapturedRuntimeIDs))
	}
}

func TestSleepWithContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepWithContext(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleep error = %v, want context canceled", err)
	}
}
