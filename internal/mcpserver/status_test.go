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
	if len(result) != 0 {
		t.Errorf("expected empty result, got %v", result)
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

func TestReadSentinelError(t *testing.T) {
	_, err := readSentinel("/nonexistent/path")
	if err == nil {
		t.Fatal("expected error for nonexistent sentinel")
	}
}
