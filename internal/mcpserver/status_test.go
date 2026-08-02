package mcpserver

import (
	"os"
	"path/filepath"
	"testing"
)

func writeSentinel(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestTranslateStatusPreservesRosterIdentity(t *testing.T) {
	result := translateStatus(map[string]any{
		"status":            "running",
		"roster_file":       "/repo/roster.json",
		"roster_entry":      "planner",
		"effective_backend": "agy",
		"agent_profile":     "cloud",
		"effective_agent":   "planner-agent",
		"effective_model":   "planner-model",
	}, "")
	for key, want := range map[string]any{
		"roster_file":       "/repo/roster.json",
		"roster_entry":      "planner",
		"effective_backend": "agy",
		"agent_profile":     "cloud",
		"effective_agent":   "planner-agent",
		"effective_model":   "planner-model",
	} {
		if result[key] != want {
			t.Fatalf("status[%q] = %v, want %v", key, result[key], want)
		}
	}
}

func TestApplyRunInfoIdentityFallback(t *testing.T) {
	status := map[string]any{}
	applyRunInfoIdentity(status, &RunInfo{
		RosterFile:       "/repo/roster.json",
		RosterEntry:      "planner",
		EffectiveBackend: "agy",
		AgentProfile:     "cloud",
		EffectiveAgent:   "planner-agent",
		EffectiveModel:   "planner-model",
	})
	if status["roster_entry"] != "planner" || status["effective_backend"] != "agy" || status["agent_profile"] != "cloud" || status["agent"] != "planner-agent" {
		t.Fatalf("identity fallback = %#v", status)
	}
}

func TestApplyRunInfoIdentityDoesNotResurrectAuthoritativelyClearedFields(t *testing.T) {
	status := translateStatus(map[string]any{
		"status": "ended", "agent": "", "model": "", "agent_profile": "",
		"effective_agent": "", "effective_model": "", "effective_backend": "agy",
	}, "")
	applyRunInfoIdentity(status, &RunInfo{
		Agent: "stale-agent", Model: "stale-model", AgentProfile: "cloud",
		EffectiveAgent: "stale-agent", EffectiveModel: "stale-model", EffectiveBackend: "pi",
	})
	for _, key := range []string{"agent", "model", "agent_profile", "effective_agent", "effective_model"} {
		value, present := status[key]
		if !present || value != "" {
			t.Fatalf("status[%q] = %#v (present=%v), want authoritative empty string", key, value, present)
		}
	}
	if status["effective_backend"] != "agy" {
		t.Fatalf("effective_backend = %v, want live value", status["effective_backend"])
	}
}

func TestTranslateStatusIdle(t *testing.T) {
	raw := map[string]any{"status": "idle", "session_id": "ses_1", "phase": "working"}
	result := translateStatus(raw, "")
	if result["status"] != "running" {
		t.Errorf("expected running, got %v", result["status"])
	}
	if result["session_id"] != "ses_1" {
		t.Errorf("expected ses_1, got %v", result["session_id"])
	}
}

func TestTranslateStatusRunning(t *testing.T) {
	raw := map[string]any{"status": "running", "session_id": "ses_2"}
	result := translateStatus(raw, "")
	if result["status"] != "running" {
		t.Errorf("expected running, got %v", result["status"])
	}
}

func TestTranslateStatusEndedWithDoneSentinel(t *testing.T) {
	dir := t.TempDir()
	path := writeSentinel(t, dir, "done.sentinel", "DONE\nSESSION=ses_done\nSTOP_REASON=end_turn\n")

	raw := map[string]any{"status": "ended", "session_id": "ses_live"}
	result := translateStatus(raw, path)
	if result["status"] != "done" {
		t.Errorf("expected done, got %v", result["status"])
	}
	if result["stop_reason"] != "end_turn" {
		t.Errorf("expected end_turn, got %v", result["stop_reason"])
	}
	if result["session_id"] != "ses_done" {
		t.Errorf("expected ses_done from sentinel, got %v", result["session_id"])
	}
}

func TestTranslateStatusEndedWithFailedSentinel(t *testing.T) {
	dir := t.TempDir()
	path := writeSentinel(t, dir, "failed.sentinel", "FAILED\nSESSION=ses_fail\nSTOP_REASON=refusal\n")

	raw := map[string]any{"status": "ended"}
	result := translateStatus(raw, path)
	if result["status"] != "failed" {
		t.Errorf("expected failed, got %v", result["status"])
	}
	if result["session_id"] != "ses_fail" {
		t.Errorf("expected session_id ses_fail, got %v", result["session_id"])
	}
	if result["stop_reason"] != "refusal" {
		t.Errorf("expected stop_reason refusal, got %v", result["stop_reason"])
	}
}

func TestTranslateStatusEndedWithoutSentinel(t *testing.T) {
	raw := map[string]any{"status": "ended", "session_id": "ses_none"}
	result := translateStatus(raw, "")
	if result["status"] != "done" {
		t.Errorf("expected done, got %v", result["status"])
	}
	if result["session_id"] != "ses_none" {
		t.Errorf("expected ses_none, got %v", result["session_id"])
	}
}

func TestTranslateStatusNoLiveStatusWithSentinel(t *testing.T) {
	dir := t.TempDir()
	path := writeSentinel(t, dir, "timeout.sentinel", "TIMEOUT\nSESSION=ses_to\nSTOP_REASON=timeout\n")

	raw := map[string]any{}
	result := translateStatus(raw, path)
	if result["status"] != "timeout" {
		t.Errorf("expected timeout, got %v", result["status"])
	}
	if result["session_id"] != "ses_to" {
		t.Errorf("expected session_id ses_to, got %v", result["session_id"])
	}
	if result["stop_reason"] != "timeout" {
		t.Errorf("expected stop_reason timeout, got %v", result["stop_reason"])
	}
}

func TestTranslateStatusNoLiveStatusNoSentinel(t *testing.T) {
	raw := map[string]any{}
	result := translateStatus(raw, "")
	if result["status"] != "running" {
		t.Errorf("expected running fallback, got %v", result["status"])
	}
}

func TestTranslateStatusBlockedLiveStatus(t *testing.T) {
	result := translateStatus(map[string]any{"status": "blocked", "session_id": "ses_block"}, "")
	if result["status"] != "failed" {
		t.Errorf("expected failed, got %v", result["status"])
	}
	if result["session_id"] != "ses_block" {
		t.Errorf("expected ses_block, got %v", result["session_id"])
	}
}

func TestTranslateStatusKilledSentinel(t *testing.T) {
	dir := t.TempDir()
	path := writeSentinel(t, dir, "killed.sentinel", "KILLED\nSESSION=ses_kill\nSTOP_REASON=cancelled\n")

	result := translateStatus(map[string]any{}, path)
	if result["status"] != "killed" {
		t.Errorf("expected killed, got %v", result["status"])
	}
	if result["session_id"] != "ses_kill" {
		t.Errorf("expected session_id ses_kill, got %v", result["session_id"])
	}
	if result["stop_reason"] != "cancelled" {
		t.Errorf("expected stop_reason cancelled, got %v", result["stop_reason"])
	}
}

func TestTranslateStatusBlockedSentinel(t *testing.T) {
	dir := t.TempDir()
	path := writeSentinel(t, dir, "blocked.sentinel", "BLOCKED\nSESSION=ses_block\nSTOP_REASON=permission\n")

	result := translateStatus(map[string]any{}, path)
	if result["status"] != "failed" {
		t.Errorf("expected failed, got %v", result["status"])
	}
	if result["session_id"] != "ses_block" {
		t.Errorf("expected session_id ses_block, got %v", result["session_id"])
	}
	if result["stop_reason"] != "permission" {
		t.Errorf("expected stop_reason permission, got %v", result["stop_reason"])
	}
}

func TestTranslateStatusIdleTerminalPhases(t *testing.T) {
	// Terminal phase metadata determines an idle runtime's public state.
	for _, phase := range []string{"done", "failed", "timeout", "killed"} {
		raw := map[string]any{
			"status":      "idle",
			"session_id":  "ses_raw",
			"phase":       phase,
			"stop_reason": "raw_reason",
		}
		result := translateStatus(raw, "")
		if result["status"] == "running" {
			t.Errorf("idle phase %s returned running: %#v", phase, result)
		}
		if result["phase"] != phase {
			t.Errorf("idle phase = %v, want %s", result["phase"], phase)
		}
		if result["session_id"] != "ses_raw" {
			t.Errorf("idle phase %s session_id = %v, want ses_raw", phase, result["session_id"])
		}
		if result["stop_reason"] != "raw_reason" {
			t.Errorf("idle phase %s stop_reason = %v, want raw_reason", phase, result["stop_reason"])
		}
	}
}

func TestTranslateStatusRunningTerminalPhaseStaysRunning(t *testing.T) {
	for _, phase := range []string{"done", "failed", "timeout", "killed"} {
		raw := map[string]any{
			"status":      "running",
			"session_id":  "ses_active",
			"phase":       phase,
			"stop_reason": "active_failure",
		}
		result := translateStatus(raw, "")
		if result["status"] != "running" {
			t.Errorf("running phase %s status = %v, want running (transient race must not promote)", phase, result["status"])
		}
		if result["phase"] != "" || result["phase_label"] != "" {
			t.Errorf("running phase %s public phase = %q/%q, want empty", phase, result["phase"], result["phase_label"])
		}
		if result["session_id"] != "ses_active" {
			t.Errorf("running phase %s session_id = %v, want ses_active", phase, result["session_id"])
		}
	}
}

func TestTranslateStatusParkedSuccessUsesCurrentMetadata(t *testing.T) {
	path := writeSentinel(t, t.TempDir(), "parked.sentinel", "DONE\nSESSION=ses_stale\nSTOP_REASON=stale\n")

	result := translateStatus(map[string]any{
		"status":      "idle",
		"session_id":  "ses_live",
		"phase":       "done",
		"stop_reason": "end_turn",
	}, path)
	if result["status"] != "done" || result["session_id"] != "ses_live" || result["stop_reason"] != "end_turn" {
		t.Fatalf("parked success = %#v, want current raw metadata", result)
	}
}

func TestTranslateStatusIdleTerminalPhaseWithoutSentinel(t *testing.T) {
	result := translateStatus(map[string]any{
		"status":      "idle",
		"session_id":  "ses_raw",
		"phase":       "timeout",
		"stop_reason": "raw_timeout",
	}, filepath.Join(t.TempDir(), "missing.sentinel"))
	if result["status"] != "timeout" {
		t.Fatalf("status = %v, want timeout", result["status"])
	}
	if result["session_id"] != "ses_raw" || result["stop_reason"] != "raw_timeout" {
		t.Fatalf("terminal phase metadata = %#v, want raw session and stop reason", result)
	}
}

func TestTranslateStatusRunningTerminalPhaseIgnoresStaleSentinel(t *testing.T) {
	dir := t.TempDir()
	path := writeSentinel(t, dir, "stale.sentinel", "DONE\nSESSION=ses_stale\nSTOP_REASON=stale\n")

	result := translateStatus(map[string]any{
		"status":      "running",
		"session_id":  "ses_active",
		"phase":       "failed",
		"stop_reason": "active_failure",
	}, path)
	// An active runtime remains public "running" when its raw phase is terminal; retries are not complete.
	if result["status"] != "running" {
		t.Fatalf("status = %v, want running (running runtime must not promote terminal phase)", result["status"])
	}
	if result["session_id"] != "ses_active" {
		t.Fatalf("session_id = %v, want ses_active", result["session_id"])
	}
}

func TestTranslateStatusIdleTerminalPhaseIgnoresStaleSentinel(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
	}{
		{name: "done", status: "DONE"},
		{name: "failed", status: "FAILED"},
		{name: "timeout", status: "TIMEOUT"},
		{name: "killed", status: "KILLED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeSentinel(t, t.TempDir(), tc.name+".sentinel", tc.status+"\nSESSION=ses_stale\nSTOP_REASON=stale_reason\n")
			result := translateStatus(map[string]any{
				"status":      "idle",
				"session_id":  "ses_current",
				"phase":       tc.name,
				"stop_reason": "current_reason",
			}, path)
			if result["status"] != tc.name || result["session_id"] != "ses_current" || result["stop_reason"] != "current_reason" {
				t.Fatalf("translated status = %#v, want current phase metadata", result)
			}
		})
	}
}

func TestTranslateStatusLiveNonTerminalPhaseIgnoresSentinel(t *testing.T) {
	dir := t.TempDir()
	stalePath := writeSentinel(t, dir, "stale.sentinel", "DONE\nSESSION=ses_stale\nSTOP_REASON=stale\n")

	for _, rawStatus := range []string{"idle", "running"} {
		for _, phase := range []string{"", "working", "waiting", "unknown"} {
			for _, sentinel := range []struct {
				name string
				path string
			}{
				{name: "absent", path: ""},
				{name: "stale", path: stalePath},
			} {
				t.Run(rawStatus+"/phase="+phase+"/sentinel="+sentinel.name, func(t *testing.T) {
					result := translateStatus(map[string]any{
						"status":      rawStatus,
						"session_id":  "ses_live",
						"phase":       phase,
						"phase_label": "live label",
					}, sentinel.path)
					if result["status"] != "running" {
						t.Fatalf("raw status %s phase %q status = %v, want running", rawStatus, phase, result["status"])
					}
					if result["session_id"] != "ses_live" {
						t.Fatalf("raw status %s phase %q session_id = %v, want ses_live", rawStatus, phase, result["session_id"])
					}
					if result["phase"] != phase {
						t.Fatalf("raw status %s phase = %v, want %q", rawStatus, result["phase"], phase)
					}
					if result["phase_label"] != "live label" {
						t.Fatalf("raw status %s phase_label = %v, want live label", rawStatus, result["phase_label"])
					}
				})
			}
		}
	}
}

func TestReadSentinelError(t *testing.T) {
	_, err := readSentinel("/nonexistent/path")
	if err == nil {
		t.Fatal("expected error for nonexistent sentinel")
	}
}
