package mcpserver

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/diagnote"
	"github.com/sdougbrown/avenor/internal/runstate"
)

func writeSentinel(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

// syncWriter serializes writes to a buffer so diagnostics written outside the
// gate lock can arrive from concurrent goroutines.
type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestTranslateStatusPreservesRosterIdentity(t *testing.T) {
	result := translateStatus(nil, "", map[string]any{
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
	status := translateStatus(nil, "", map[string]any{
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
	result := translateStatus(nil, "", raw, "")
	if result["status"] != "running" {
		t.Errorf("expected running, got %v", result["status"])
	}
	if result["session_id"] != "ses_1" {
		t.Errorf("expected ses_1, got %v", result["session_id"])
	}
}

func TestTranslateStatusRunning(t *testing.T) {
	raw := map[string]any{"status": "running", "session_id": "ses_2"}
	result := translateStatus(nil, "", raw, "")
	if result["status"] != "running" {
		t.Errorf("expected running, got %v", result["status"])
	}
}

func TestTranslateStatusEndedWithDoneSentinel(t *testing.T) {
	dir := t.TempDir()
	path := writeSentinel(t, dir, "done.sentinel", "DONE\nSESSION=ses_done\nSTOP_REASON=end_turn\n")

	raw := map[string]any{"status": "ended", "session_id": "ses_live"}
	result := translateStatus(nil, "", raw, path)
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
	result := translateStatus(nil, "", raw, path)
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
	result := translateStatus(nil, "", raw, "")
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
	result := translateStatus(nil, "", raw, path)
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
	result := translateStatus(nil, "", raw, "")
	if result["status"] != "unknown" {
		t.Errorf("expected unknown fallback, got %v", result["status"])
	}
}

func TestTranslateStatusBlockedLiveStatus(t *testing.T) {
	result := translateStatus(nil, "", map[string]any{"status": "blocked", "session_id": "ses_block"}, "")
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

	result := translateStatus(nil, "", map[string]any{}, path)
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

	result := translateStatus(nil, "", map[string]any{}, path)
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
		result := translateStatus(nil, "", raw, "")
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
		result := translateStatus(nil, "", raw, "")
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

// TestTranslateStatusPermissionDeciderReachesLifecycleView proves the
// permission-decider keys survive the whole avenor_status chain: through
// translateStatus's pass-through allowlist, through the lifecycle view
// shaping, and through the final statusRunFromMap typed mapping — the last
// step drops any key without a statusRun field, so the typed struct must
// carry them or the MCP tool output loses the decider identity and spawn
// exhaustion recorded on the runtime.
func TestTranslateStatusPermissionDeciderReachesLifecycleView(t *testing.T) {
	raw := map[string]any{
		"status":                       "blocked",
		"session_id":                   "ses_dec",
		"permission_decider":           "rm-guard",
		"permission_decider_exhausted": true,
	}
	shaped := shapeStatusForView(translateStatus(nil, "", raw, ""), "lifecycle")
	if got, _ := shaped["permission_decider"].(string); got != "rm-guard" {
		t.Fatalf("shaped permission_decider = %q, want rm-guard", got)
	}
	if exhausted, _ := shaped["permission_decider_exhausted"].(bool); !exhausted {
		t.Fatalf("shaped permission_decider_exhausted = %v, want true", shaped["permission_decider_exhausted"])
	}
	// The final typed output (what the avenor_status tool actually returns)
	// must carry both keys too; a missing statusRun field silently drops them.
	run := statusRunFromMap(shaped)
	if run.PermissionDecider == nil || *run.PermissionDecider != "rm-guard" {
		t.Fatalf("mapped PermissionDecider = %v, want rm-guard", run.PermissionDecider)
	}
	if run.PermissionDeciderExhausted == nil || !*run.PermissionDeciderExhausted {
		t.Fatalf("mapped PermissionDeciderExhausted = %v, want true", run.PermissionDeciderExhausted)
	}
}

// The resolved direct-model decider reaches the same avenor_status chain:
// translateStatus, the lifecycle view, and the typed output all carry the
// provider/model identity so an operator (or a harness decider watching a
// sibling) can see which model answered policy for the run.
func TestTranslateStatusPermissionModelDeciderReachesLifecycleView(t *testing.T) {
	raw := map[string]any{
		"status":                   "blocked",
		"session_id":               "ses_mdec",
		"permission_model_decider": "groq/llama-3.3-70b",
	}
	shaped := shapeStatusForView(translateStatus(nil, "", raw, ""), "lifecycle")
	if got, _ := shaped["permission_model_decider"].(string); got != "groq/llama-3.3-70b" {
		t.Fatalf("shaped permission_model_decider = %q, want groq/llama-3.3-70b", got)
	}
	run := statusRunFromMap(shaped)
	if run.PermissionModelDecider == nil || *run.PermissionModelDecider != "groq/llama-3.3-70b" {
		t.Fatalf("mapped PermissionModelDecider = %v, want groq/llama-3.3-70b", run.PermissionModelDecider)
	}
}

func TestTranslateStatusParkedSuccessUsesCurrentMetadata(t *testing.T) {
	path := writeSentinel(t, t.TempDir(), "parked.sentinel", "DONE\nSESSION=ses_stale\nSTOP_REASON=stale\n")

	result := translateStatus(nil, "", map[string]any{
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
	result := translateStatus(nil, "", map[string]any{
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

	result := translateStatus(nil, "", map[string]any{
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
			result := translateStatus(nil, "", map[string]any{
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
					result := translateStatus(nil, "", map[string]any{
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
	_, err := runstate.ReadSentinel("/nonexistent/path")
	if err == nil {
		t.Fatal("expected error for nonexistent sentinel")
	}
}

func TestStatusRunFromMapFieldTypes(t *testing.T) {
	t.Run("string field", func(t *testing.T) {
		run := statusRunFromMap(map[string]any{"status": "running"})
		if run.Status == nil || *run.Status != "running" {
			t.Fatalf("Status = %v, want pointer to 'running'", run.Status)
		}
	})

	t.Run("empty string phase_label presence", func(t *testing.T) {
		run := statusRunFromMap(map[string]any{"phase_label": ""})
		if run.PhaseLabel == nil {
			t.Fatal("PhaseLabel = nil, want non-nil pointer to empty string")
		}
		if *run.PhaseLabel != "" {
			t.Fatalf("PhaseLabel = %q, want ''", *run.PhaseLabel)
		}
	})

	t.Run("pending_permission boolean", func(t *testing.T) {
		run := statusRunFromMap(map[string]any{"pending_permission": true})
		if run.PendingPermission != true {
			t.Fatalf("PendingPermission = %v, want true", run.PendingPermission)
		}
	})

	t.Run("pending_permission legacy object", func(t *testing.T) {
		run := statusRunFromMap(map[string]any{"pending_permission": map[string]any{"request_id": "req-42"}})
		if run.PendingPermission == nil {
			t.Fatal("PendingPermission = nil, want non-nil map")
		}
		m, ok := run.PendingPermission.(map[string]any)
		if !ok {
			t.Fatalf("PendingPermission = %T, want map[string]any", run.PendingPermission)
		}
		if m["request_id"] != "req-42" {
			t.Fatalf("request_id = %v, want req-42", m["request_id"])
		}
	})

	t.Run("latest_seq float64 to int64", func(t *testing.T) {
		run := statusRunFromMap(map[string]any{"latest_seq": float64(42)})
		if run.LatestSeq == nil || *run.LatestSeq != 42 {
			t.Fatalf("LatestSeq = %v, want pointer to 42", run.LatestSeq)
		}
	})

	t.Run("children as []any of strings", func(t *testing.T) {
		run := statusRunFromMap(map[string]any{"children": []any{"a", "b"}})
		if len(run.Children) != 2 || run.Children[0] != "a" || run.Children[1] != "b" {
			t.Fatalf("Children = %v, want [a b]", run.Children)
		}
	})

	t.Run("usage passthrough", func(t *testing.T) {
		usage := map[string]any{"total_tokens": float64(100)}
		run := statusRunFromMap(map[string]any{"usage": usage})
		if run.Usage == nil || run.Usage["total_tokens"] != float64(100) {
			t.Fatalf("Usage = %v, want passthrough", run.Usage)
		}
	})

	t.Run("permission record passthrough", func(t *testing.T) {
		perm := map[string]any{"request_id": "req-9", "options": []any{map[string]any{"option_id": "allow_once"}}}
		run := statusRunFromMap(map[string]any{"permission": perm})
		if run.Permission == nil || run.Permission["request_id"] != "req-9" {
			t.Fatalf("Permission = %v, want passthrough", run.Permission)
		}
	})

	t.Run("mismatched type yields nil", func(t *testing.T) {
		run := statusRunFromMap(map[string]any{"status": 123})
		if run.Status != nil {
			t.Fatalf("Status = %v, want nil for mismatched type", run.Status)
		}
	})
}

// TestTranslateStatusTypedBoundaryKeepsWireFields proves that translated and
// sentinel-derived statuses survive the map[string]any boundary as plain
// strings: the typed output mapping and the wait predicates both read fields
// with .(string) assertions, so a named string type written into the map
// would silently disappear. It also pins the presence semantics for an
// explicitly empty phase and that terminal waits still fire.
func TestTranslateStatusTypedBoundaryKeepsWireFields(t *testing.T) {
	t.Run("translated values keep fields through the typed mapping", func(t *testing.T) {
		translated := translateStatus(nil, "", map[string]any{
			"runtime_id": "rt_1",
			"status":     "ended",
			"phase":      "working",
			"session_id": "ses_1",
		}, "")
		if got, ok := translated["status"].(string); !ok || got != "done" {
			t.Fatalf("status = %#v, want string done", translated["status"])
		}
		run := statusRunFromMap(translated)
		if run.Status == nil || *run.Status != "done" {
			t.Fatalf("Status = %v, want done", run.Status)
		}
		if run.Phase == nil || *run.Phase != "working" {
			t.Fatalf("Phase = %v, want working", run.Phase)
		}
		if run.RuntimeID == nil || *run.RuntimeID != "rt_1" {
			t.Fatalf("RuntimeID = %v, want rt_1", run.RuntimeID)
		}
		if run.SessionID == nil || *run.SessionID != "ses_1" {
			t.Fatalf("SessionID = %v, want ses_1", run.SessionID)
		}
	})

	t.Run("explicit empty phase stays present", func(t *testing.T) {
		translated := translateStatus(nil, "", map[string]any{"status": "running", "phase": ""}, "")
		if got, ok := translated["status"].(string); !ok || got != "running" {
			t.Fatalf("status = %#v, want string running", translated["status"])
		}
		phase, ok := translated["phase"].(string)
		if !ok {
			t.Fatalf("phase = %#v, want present string", translated["phase"])
		}
		if phase != "" {
			t.Fatalf("phase = %q, want empty", phase)
		}
		run := statusRunFromMap(translated)
		if run.Phase == nil || *run.Phase != "" {
			t.Fatalf("Phase = %v, want pointer to empty string", run.Phase)
		}
	})

	t.Run("sentinel-derived status satisfies terminal wait", func(t *testing.T) {
		path := writeSentinel(t, t.TempDir(), "typed.sentinel", "DONE\nSESSION=ses_sentinel\n")
		translated := translateStatus(nil, "", map[string]any{"status": "mystery"}, path)
		if got, ok := translated["status"].(string); !ok || got != "done" {
			t.Fatalf("status = %#v, want string done from sentinel", translated["status"])
		}
		run := statusRunFromMap(translated)
		if run.Status == nil || *run.Status != "done" {
			t.Fatalf("Status = %v, want done", run.Status)
		}
		if run.SessionID == nil || *run.SessionID != "ses_sentinel" {
			t.Fatalf("SessionID = %v, want ses_sentinel", run.SessionID)
		}
		if !isTerminalStatus(translated) {
			t.Fatal("isTerminalStatus = false, want true for sentinel-derived done")
		}
		if !waitConditionSatisfied(waitTerminal, translated, "") {
			t.Fatal("waitTerminal did not fire for sentinel-derived done")
		}
	})

	t.Run("unrecognized status reads unknown and keeps waiting", func(t *testing.T) {
		translated := translateStatus(nil, "", map[string]any{"status": "mystery", "phase": "working"}, "")
		if got, ok := translated["status"].(string); !ok || got != "unknown" {
			t.Fatalf("status = %#v, want string unknown", translated["status"])
		}
		run := statusRunFromMap(translated)
		if run.Status == nil || *run.Status != "unknown" {
			t.Fatalf("Status = %v, want unknown", run.Status)
		}
		if run.Phase == nil || *run.Phase != "working" {
			t.Fatalf("Phase = %v, want working", run.Phase)
		}
		if isTerminalStatus(translated) {
			t.Fatal("isTerminalStatus = true, want false for unknown")
		}
		if waitConditionSatisfied(waitTerminal, translated, "") {
			t.Fatal("waitTerminal fired for unknown")
		}
	})

	t.Run("phase change wait fires on rewritten phase", func(t *testing.T) {
		translated := translateStatus(nil, "", map[string]any{"status": "running", "phase": "done"}, "")
		if !waitConditionSatisfied(waitPhaseChange, translated, "working") {
			t.Fatal("waitPhaseChange did not fire after the running terminal-phase rewrite")
		}
	})
}

// unrecognizedStatusLines parses a diagnostics buffer into one entry per log
// line, with suppression lines marked by an empty status value.
func unrecognizedStatusLines(t *testing.T, output string) []string {
	t.Helper()
	if output == "" {
		return nil
	}
	lines := strings.Split(strings.TrimSuffix(output, "\n"), "\n")
	values := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.HasSuffix(line, diagnote.SuppressedNote) {
			values = append(values, "")
			continue
		}
		const prefix = `avenor mcp: runtime "`
		if !strings.HasPrefix(line, prefix) {
			t.Fatalf("log line = %q, want %q prefix", line, prefix)
		}
		const middle = `" reported unrecognized status "`
		_, rest, ok := strings.Cut(line[len(prefix):], middle)
		if !ok || !strings.HasSuffix(rest, `"`) {
			t.Fatalf("log line = %q, want an unrecognized-status report", line)
		}
		values = append(values, strings.TrimSuffix(rest, `"`))
	}
	return values
}

func TestTranslateStatusLogsUnrecognizedStatus(t *testing.T) {
	t.Run("unknown status is not terminal and logs once across repeated calls", func(t *testing.T) {
		var stderr bytes.Buffer
		diag := &statusDiagnostics{stderr: &stderr}
		for i := 0; i < 3; i++ {
			translated := translateStatus(diag, "rt_1", map[string]any{"status": "mystery", "phase": "working"}, "")
			if got, ok := translated["status"].(string); !ok || got != "unknown" {
				t.Fatalf("status = %#v, want string unknown", translated["status"])
			}
			if isTerminalStatus(translated) {
				t.Fatal("isTerminalStatus = true, want false for unknown")
			}
			if waitConditionSatisfied(waitTerminal, translated, "") {
				t.Fatal("waitTerminal fired for unknown")
			}
		}
		values := unrecognizedStatusLines(t, stderr.String())
		if len(values) != 1 || values[0] != "mystery" {
			t.Fatalf("log lines = %#v, want one line for mystery", values)
		}
	})

	t.Run("missing or empty raw status translates to unknown without logging", func(t *testing.T) {
		var stderr bytes.Buffer
		diag := &statusDiagnostics{stderr: &stderr}
		for _, raw := range []map[string]any{
			{"phase": "working"},
			{"status": "", "phase": "working"},
		} {
			translated := translateStatus(diag, "rt_1", raw, "")
			if got, ok := translated["status"].(string); !ok || got != "unknown" {
				t.Fatalf("status = %#v, want string unknown", translated["status"])
			}
		}
		if stderr.String() != "" {
			t.Fatalf("stderr = %q, want no log lines for a missing or empty status", stderr.String())
		}
	})

	t.Run("recognized statuses never log", func(t *testing.T) {
		var stderr bytes.Buffer
		diag := &statusDiagnostics{stderr: &stderr}
		for _, rawStatus := range []string{"idle", "running", "ended", "done", "failed", "timeout", "killed", "waiting", "blocked"} {
			translateStatus(diag, "rt_1", map[string]any{"status": rawStatus, "phase": "working"}, "")
		}
		if stderr.String() != "" {
			t.Fatalf("stderr = %q, want no log lines for recognized statuses", stderr.String())
		}
	})

	t.Run("same status from two runtimes logs twice", func(t *testing.T) {
		var stderr bytes.Buffer
		diag := &statusDiagnostics{stderr: &stderr}
		translateStatus(diag, "rt_1", map[string]any{"status": "mystery"}, "")
		translateStatus(diag, "rt_2", map[string]any{"status": "mystery"}, "")
		translateStatus(diag, "rt_1", map[string]any{"status": "mystery"}, "")
		values := unrecognizedStatusLines(t, stderr.String())
		if len(values) != 2 || values[0] != "mystery" || values[1] != "mystery" {
			t.Fatalf("log lines = %#v, want mystery logged once per runtime", values)
		}
	})

	t.Run("unknown status with a completion sentinel resolves through the sentinel", func(t *testing.T) {
		path := writeSentinel(t, t.TempDir(), "unknown.sentinel", "DONE\nSESSION=ses_sentinel\n")
		var stderr bytes.Buffer
		diag := &statusDiagnostics{stderr: &stderr}
		translated := translateStatus(diag, "rt_1", map[string]any{"status": "mystery"}, path)
		if got, ok := translated["status"].(string); !ok || got != "done" {
			t.Fatalf("status = %#v, want string done from sentinel", translated["status"])
		}
		if got := translated["session_id"]; got != "ses_sentinel" {
			t.Fatalf("session_id = %#v, want ses_sentinel", got)
		}
		// The live status was still unrecognized, so the diagnostic is due even
		// though the sentinel resolved the run.
		values := unrecognizedStatusLines(t, stderr.String())
		if len(values) != 1 || values[0] != "mystery" {
			t.Fatalf("log lines = %#v, want one line for mystery", values)
		}
	})

	t.Run("cap saturation logs the suppression line once", func(t *testing.T) {
		var stderr bytes.Buffer
		diag := &statusDiagnostics{stderr: &stderr}
		for i := 0; i < 18; i++ {
			translateStatus(diag, "rt_1", map[string]any{"status": fmt.Sprintf("mystery_%d", i)}, "")
		}
		translateStatus(diag, "rt_1", map[string]any{"status": "mystery_0"}, "")
		values := unrecognizedStatusLines(t, stderr.String())
		if len(values) != diagnote.Cap+1 {
			t.Fatalf("log lines = %d (%v), want %d value lines plus one suppression", len(values), values, diagnote.Cap)
		}
		suppressions := 0
		seen := make(map[string]bool)
		for _, value := range values {
			if value == "" {
				suppressions++
				continue
			}
			if seen[value] {
				t.Fatalf("value %q logged twice", value)
			}
			seen[value] = true
		}
		if suppressions != 1 {
			t.Fatalf("suppression lines = %d, want 1", suppressions)
		}
		suppressionLine := "avenor mcp: " + diagnote.SuppressedNote + "\n"
		if got := strings.Count(stderr.String(), suppressionLine); got != 1 {
			t.Fatalf("stderr = %q, want exactly one %q line", stderr.String(), suppressionLine)
		}
	})

	t.Run("concurrent calls stay race-clean", func(t *testing.T) {
		var stderr syncWriter
		diag := &statusDiagnostics{stderr: &stderr}
		var wg sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for i := 0; i < 5; i++ {
					status := fmt.Sprintf("mystery_%d", (worker*3+i)%20)
					translateStatus(diag, "rt_1", map[string]any{"status": status}, "")
				}
			}(worker)
		}
		wg.Wait()
		values := unrecognizedStatusLines(t, stderr.String())
		if len(values) != diagnote.Cap+1 {
			t.Fatalf("log lines = %d, want %d value lines plus one suppression", len(values), diagnote.Cap+1)
		}
		suppressions := 0
		seen := make(map[string]bool)
		for _, value := range values {
			if value == "" {
				suppressions++
				continue
			}
			if seen[value] {
				t.Fatalf("value %q logged twice", value)
			}
			seen[value] = true
		}
		if suppressions != 1 {
			t.Fatalf("suppression lines = %d, want 1", suppressions)
		}
	})
}

func TestServerDiagnosticsStderrRoutesUnrecognizedStatus(t *testing.T) {
	var stderr bytes.Buffer
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, Stderr: &stderr, ControlClient: &fakeClient{}})
	if err != nil {
		t.Fatal(err)
	}
	cl := &fakeClient{statusResult: map[string]any{"runtime_id": "rt_9", "status": "mystery", "phase": "working"}}
	result, err := s.queryRunStatus(cl, "/sup.sock", "run_1")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := result["status"].(string); !ok || got != "unknown" {
		t.Fatalf("status = %#v, want string unknown", result["status"])
	}
	values := unrecognizedStatusLines(t, stderr.String())
	if len(values) != 1 || values[0] != "mystery" {
		t.Fatalf("log lines = %#v, want one line for mystery", values)
	}
}
