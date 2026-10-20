package runstate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSentinel(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sentinel.done")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	return path
}

func TestReadSentinel(t *testing.T) {
	path := writeSentinel(t, "DONE\nSESSION=ses_7\nSTOP_REASON=end_turn\n")
	sentinel, err := ReadSentinel(path)
	if err != nil {
		t.Fatalf("ReadSentinel: %v", err)
	}
	if sentinel.Status != "DONE" || sentinel.SessionID != "ses_7" || sentinel.StopReason != "end_turn" {
		t.Fatalf("sentinel = %+v", sentinel)
	}
}

func TestReadSentinelErrors(t *testing.T) {
	if _, err := ReadSentinel(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("ReadSentinel of a missing file returned no error")
	}
	if _, err := ReadSentinel(writeSentinel(t, "\n")); err == nil || !strings.Contains(err.Error(), "empty sentinel file") {
		t.Fatalf("empty sentinel error = %v", err)
	}
}

func TestReadSentinelSession(t *testing.T) {
	for _, status := range []string{"DONE", "FAILED", "BLOCKED", "done"} {
		path := writeSentinel(t, status+"\nSESSION=ses_7\n")
		sessionID, err := ReadSentinelSession(path)
		if err != nil {
			t.Fatalf("%s: ReadSentinelSession: %v", status, err)
		}
		if sessionID != "ses_7" {
			t.Fatalf("%s: session = %q", status, sessionID)
		}
	}
	for _, status := range []string{"TIMEOUT", "KILLED"} {
		path := writeSentinel(t, status+"\nSESSION=ses_7\n")
		_, err := ReadSentinelSession(path)
		if err == nil || err.Error() != "run is not resumable (status: "+strings.ToLower(status)+")" {
			t.Fatalf("%s: error = %v", status, err)
		}
	}
	if _, err := ReadSentinelSession(writeSentinel(t, "DONE\nSTOP_REASON=end_turn\n")); err == nil || err.Error() != "no session in sentinel" {
		t.Fatalf("missing session error = %v", err)
	}
}
