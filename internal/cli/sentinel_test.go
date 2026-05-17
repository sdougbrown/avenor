package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// DerivePermBase
// ---------------------------------------------------------------------------

func TestDerivePermBase(t *testing.T) {
	tests := []struct {
		name     string
		sentinel string
		want     string
	}{
		{
			name:     "done suffix stripped and replaced with perm",
			sentinel: "/tmp/jockey-run.done",
			want:     "/tmp/jockey-run.perm",
		},
		{
			name:     "no done suffix appends perm",
			sentinel: "/tmp/jockey-run",
			want:     "/tmp/jockey-run.perm",
		},
		{
			name:     "path with directory and done suffix",
			sentinel: "/var/run/avenor/session.done",
			want:     "/var/run/avenor/session.perm",
		},
		{
			name:     "path with directory and no done suffix",
			sentinel: "/var/run/avenor/session",
			want:     "/var/run/avenor/session.perm",
		},
		{
			name:     "done in middle of path is not stripped",
			sentinel: "/tmp/done-run/session",
			want:     "/tmp/done-run/session.perm",
		},
		{
			name:     "only the trailing .done is stripped",
			sentinel: "/tmp/foo.done.done",
			want:     "/tmp/foo.done.perm",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DerivePermBase(tt.sentinel)
			if got != tt.want {
				t.Fatalf("DerivePermBase(%q) = %q, want %q", tt.sentinel, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// sentinelContent
// ---------------------------------------------------------------------------

func TestSentinelContent(t *testing.T) {
	tests := []struct {
		name       string
		exitCode   int
		sessionID  string
		stopReason string
		runID      string
		reason     string
		want       string
	}{
		{
			name:       "exit 0 clean",
			exitCode:   0,
			sessionID:  "ses_abc123",
			stopReason: "end_turn",
			want:       "DONE\nSESSION=ses_abc123\nSTOP_REASON=end_turn\n",
		},
		{
			name:       "exit 0 with run id",
			exitCode:   0,
			sessionID:  "ses_abc123",
			stopReason: "end_turn",
			runID:      "a1b2c3d4e5f60718",
			want:       "DONE\nSESSION=ses_abc123\nSTOP_REASON=end_turn\nRUN=a1b2c3d4e5f60718\n",
		},
		{
			name:       "run id newlines cannot inject sentinel lines",
			exitCode:   0,
			sessionID:  "ses_abc123",
			stopReason: "end_turn",
			runID:      "run-1\nFAILED\rEXIT_CODE=1",
			want:       "DONE\nSESSION=ses_abc123\nSTOP_REASON=end_turn\nRUN=run-1 FAILED EXIT_CODE=1\n",
		},
		{
			name:       "exit 0 default stop reason",
			exitCode:   0,
			sessionID:  "ses_xyz",
			stopReason: "",
			want:       "DONE\nSESSION=ses_xyz\nSTOP_REASON=end_turn\n",
		},
		{
			name:       "exit 124 timeout",
			exitCode:   124,
			sessionID:  "ses_t",
			stopReason: "timeout",
			want:       "TIMEOUT\nSESSION=ses_t\nSTOP_REASON=timeout\n",
		},
		{
			name:       "exit 124 timeout with run id",
			exitCode:   124,
			sessionID:  "ses_t",
			stopReason: "timeout",
			runID:      "deadbeefcafe0001",
			want:       "TIMEOUT\nSESSION=ses_t\nSTOP_REASON=timeout\nRUN=deadbeefcafe0001\n",
		},
		{
			name:       "exit 124 default stop reason",
			exitCode:   124,
			sessionID:  "ses_t2",
			stopReason: "",
			want:       "TIMEOUT\nSESSION=ses_t2\nSTOP_REASON=timeout\n",
		},
		{
			name:       "exit 130 sigint",
			exitCode:   130,
			sessionID:  "ses_k",
			stopReason: "cancelled",
			want:       "KILLED\nSESSION=ses_k\nSTOP_REASON=cancelled\nEXIT_CODE=130\n",
		},
		{
			name:       "exit 130 sigint with run id",
			exitCode:   130,
			sessionID:  "ses_k",
			stopReason: "cancelled",
			runID:      "deadbeefcafe0002",
			want:       "KILLED\nSESSION=ses_k\nSTOP_REASON=cancelled\nEXIT_CODE=130\nRUN=deadbeefcafe0002\n",
		},
		{
			name:       "exit 130 default stop reason",
			exitCode:   130,
			sessionID:  "ses_k2",
			stopReason: "",
			want:       "KILLED\nSESSION=ses_k2\nSTOP_REASON=cancelled\nEXIT_CODE=130\n",
		},
		{
			name:       "exit 1 generic failure",
			exitCode:   1,
			sessionID:  "ses_f",
			stopReason: "some_error",
			want:       "FAILED\nSESSION=ses_f\nSTOP_REASON=some_error\nEXIT_CODE=1\n",
		},
		{
			name:       "exit 1 failure with run id",
			exitCode:   1,
			sessionID:  "ses_f",
			stopReason: "some_error",
			runID:      "deadbeefcafe0003",
			want:       "FAILED\nSESSION=ses_f\nSTOP_REASON=some_error\nEXIT_CODE=1\nRUN=deadbeefcafe0003\n",
		},
		{
			name:       "exit 1 default stop reason",
			exitCode:   1,
			sessionID:  "ses_f2",
			stopReason: "",
			want:       "FAILED\nSESSION=ses_f2\nSTOP_REASON=exit_1\nEXIT_CODE=1\n",
		},
		{
			name:       "exit 2 refusal",
			exitCode:   2,
			sessionID:  "ses_r",
			stopReason: "refusal",
			want:       "FAILED\nSESSION=ses_r\nSTOP_REASON=refusal\nEXIT_CODE=2\n",
		},
		{
			name:       "empty session id",
			exitCode:   0,
			sessionID:  "",
			stopReason: "end_turn",
			want:       "DONE\nSESSION=\nSTOP_REASON=end_turn\n",
		},
		{
			name:       "exit 5 blocked without reason",
			exitCode:   5,
			sessionID:  "ses_b",
			stopReason: "blocked",
			want:       "BLOCKED\nSESSION=ses_b\nSTOP_REASON=blocked\n",
		},
		{
			name:       "exit 5 blocked with reason",
			exitCode:   5,
			sessionID:  "ses_b",
			stopReason: "blocked",
			reason:     "architectural issue: layering violation in pkg/db",
			want:       "BLOCKED\nSESSION=ses_b\nSTOP_REASON=blocked\nREASON=architectural issue: layering violation in pkg/db\n",
		},
		{
			name:       "exit 5 blocked with reason and run id",
			exitCode:   5,
			sessionID:  "ses_b",
			stopReason: "blocked",
			runID:      "deadbeefcafe0004",
			reason:     "loop limit exceeded",
			want:       "BLOCKED\nSESSION=ses_b\nSTOP_REASON=blocked\nREASON=loop limit exceeded\nRUN=deadbeefcafe0004\n",
		},
		{
			name:       "exit 5 blocked with run id but no reason",
			exitCode:   5,
			sessionID:  "ses_b",
			stopReason: "blocked",
			runID:      "deadbeefcafe0005",
			want:       "BLOCKED\nSESSION=ses_b\nSTOP_REASON=blocked\nRUN=deadbeefcafe0005\n",
		},
		{
			name:       "exit 5 reason newlines are sanitized",
			exitCode:   5,
			sessionID:  "ses_b",
			stopReason: "blocked",
			reason:     "bad\nREASON=injected\rstuff",
			want:       "BLOCKED\nSESSION=ses_b\nSTOP_REASON=blocked\nREASON=bad REASON=injected stuff\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sentinelContent(tt.exitCode, tt.sessionID, tt.stopReason, tt.runID, tt.reason)
			if got != tt.want {
				t.Errorf("sentinelContent(%d, %q, %q, %q, %q):\n got: %q\nwant: %q",
					tt.exitCode, tt.sessionID, tt.stopReason, tt.runID, tt.reason, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// sessionEndFields
// ---------------------------------------------------------------------------

func TestSessionEndFields(t *testing.T) {
	tests := []struct {
		name           string
		ndjson         string
		missingFile    bool
		wantSessionID  string
		wantStopReason string
	}{
		{
			name:           "single session.end event",
			ndjson:         `{"event":"session.end","session_id":"ses_abc","stop_reason":"end_turn"}` + "\n",
			wantSessionID:  "ses_abc",
			wantStopReason: "end_turn",
		},
		{
			name: "multiple events, last session.end wins",
			ndjson: `{"event":"agent.message_chunk","session_id":"ses_x"}` + "\n" +
				`{"event":"session.end","session_id":"ses_first","stop_reason":"timeout"}` + "\n" +
				`{"event":"session.end","session_id":"ses_last","stop_reason":"end_turn"}` + "\n",
			wantSessionID:  "ses_last",
			wantStopReason: "end_turn",
		},
		{
			name:           "no session.end returns empty strings",
			ndjson:         `{"event":"agent.message_chunk","session_id":"ses_x"}` + "\n",
			wantSessionID:  "",
			wantStopReason: "",
		},
		{
			name:           "empty log returns empty strings",
			ndjson:         "",
			wantSessionID:  "",
			wantStopReason: "",
		},
		{
			name: "malformed JSON lines are skipped",
			ndjson: `not-json` + "\n" +
				`{"event":"session.end","session_id":"ses_good","stop_reason":"end_turn"}` + "\n",
			wantSessionID:  "ses_good",
			wantStopReason: "end_turn",
		},
		{
			name:           "sessionID camelCase fallback",
			ndjson:         `{"event":"session.end","sessionID":"ses_camel","stop_reason":"cancelled"}` + "\n",
			wantSessionID:  "ses_camel",
			wantStopReason: "cancelled",
		},
		{
			name:           "missing file returns empty strings",
			missingFile:    true,
			wantSessionID:  "",
			wantStopReason: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var path string
			if tt.missingFile {
				// t.TempDir() guarantees the directory exists but the file does not.
				path = filepath.Join(t.TempDir(), "no-such-file.ndjson")
			} else {
				f, err := os.CreateTemp(t.TempDir(), "events-*.ndjson")
				if err != nil {
					t.Fatalf("create temp: %v", err)
				}
				if _, err := f.WriteString(tt.ndjson); err != nil {
					t.Fatalf("write: %v", err)
				}
				f.Close()
				path = f.Name()
			}

			gotSID, gotSR := sessionEndFields(path)
			if gotSID != tt.wantSessionID {
				t.Errorf("sessionEndFields session_id = %q, want %q", gotSID, tt.wantSessionID)
			}
			if gotSR != tt.wantStopReason {
				t.Errorf("sessionEndFields stop_reason = %q, want %q", gotSR, tt.wantStopReason)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// WriteSentinel
// ---------------------------------------------------------------------------

func TestWriteSentinel(t *testing.T) {
	t.Run("writes correct content for exit 0", func(t *testing.T) {
		dir := t.TempDir()
		logPath := filepath.Join(dir, "events.ndjson")
		sentPath := filepath.Join(dir, "run.done")

		if err := os.WriteFile(logPath, []byte(`{"event":"session.end","session_id":"ses_write","stop_reason":"end_turn"}`+"\n"), 0o600); err != nil {
			t.Fatalf("write log: %v", err)
		}

		var stderr strings.Builder
		WriteSentinel(sentPath, 0, "ses_write", "end_turn", "", &stderr)

		if stderr.Len() > 0 {
			t.Errorf("unexpected stderr: %s", stderr.String())
		}

		content, err := os.ReadFile(sentPath)
		if err != nil {
			t.Fatalf("read sentinel: %v", err)
		}
		const want = "DONE\nSESSION=ses_write\nSTOP_REASON=end_turn\n"
		if got := string(content); got != want {
			t.Errorf("sentinel content:\n got: %q\nwant: %q", got, want)
		}
	})

	t.Run("atomic rename leaves no tmp files on success", func(t *testing.T) {
		dir := t.TempDir()
		logPath := filepath.Join(dir, "events.ndjson")
		sentPath := filepath.Join(dir, "run.done")
		os.WriteFile(logPath, []byte(""), 0o600)

		var stderr strings.Builder
		WriteSentinel(sentPath, 0, "ses_write", "end_turn", "", &stderr)

		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".tmp") {
				t.Errorf("leftover tmp file: %s", e.Name())
			}
		}
	})

	t.Run("missing parent directory logs error and writes no file", func(t *testing.T) {
		dir := t.TempDir()
		logPath := filepath.Join(dir, "events.ndjson")
		// Sentinel path inside a nonexistent subdirectory.
		sentPath := filepath.Join(dir, "nonexistent-subdir", "run.done")
		os.WriteFile(logPath, []byte(""), 0o600)

		var stderr bytes.Buffer
		WriteSentinel(sentPath, 0, "ses_write", "end_turn", "", &stderr)

		if stderr.Len() == 0 {
			t.Error("expected error logged to stderr but got none")
		}
		if _, err := os.Stat(sentPath); !os.IsNotExist(err) {
			t.Error("expected no sentinel file to exist after failed write")
		}
	})
}

// ---------------------------------------------------------------------------
// WriteSentinelWithReason
// ---------------------------------------------------------------------------

func TestWriteSentinelWithReason(t *testing.T) {
	t.Run("exit 5 blocked without reason writes BLOCKED sentinel", func(t *testing.T) {
		dir := t.TempDir()
		sentPath := filepath.Join(dir, "run.done")

		var stderr strings.Builder
		WriteSentinelWithReason(sentPath, 5, "ses_b", "blocked", "", "", &stderr)

		if stderr.Len() > 0 {
			t.Errorf("unexpected stderr: %s", stderr.String())
		}

		content, err := os.ReadFile(sentPath)
		if err != nil {
			t.Fatalf("read sentinel: %v", err)
		}
		const want = "BLOCKED\nSESSION=ses_b\nSTOP_REASON=blocked\n"
		if got := string(content); got != want {
			t.Errorf("sentinel content:\n got: %q\nwant: %q", got, want)
		}
	})

	t.Run("exit 5 blocked with reason includes REASON line", func(t *testing.T) {
		dir := t.TempDir()
		sentPath := filepath.Join(dir, "run.done")

		var stderr strings.Builder
		reason := "architectural issue: layering violation in pkg/db"
		WriteSentinelWithReason(sentPath, 5, "ses_b", "blocked", "", reason, &stderr)

		if stderr.Len() > 0 {
			t.Errorf("unexpected stderr: %s", stderr.String())
		}

		content, err := os.ReadFile(sentPath)
		if err != nil {
			t.Fatalf("read sentinel: %v", err)
		}
		want := "BLOCKED\nSESSION=ses_b\nSTOP_REASON=blocked\nREASON=architectural issue: layering violation in pkg/db\n"
		if got := string(content); got != want {
			t.Errorf("sentinel content:\n got: %q\nwant: %q", got, want)
		}
	})

	t.Run("exit 5 blocked with reason and run id", func(t *testing.T) {
		dir := t.TempDir()
		sentPath := filepath.Join(dir, "run.done")

		var stderr strings.Builder
		WriteSentinelWithReason(sentPath, 5, "ses_b", "blocked", "deadbeefcafe0004", "loop limit exceeded", &stderr)

		if stderr.Len() > 0 {
			t.Errorf("unexpected stderr: %s", stderr.String())
		}

		content, err := os.ReadFile(sentPath)
		if err != nil {
			t.Fatalf("read sentinel: %v", err)
		}
		want := "BLOCKED\nSESSION=ses_b\nSTOP_REASON=blocked\nREASON=loop limit exceeded\nRUN=deadbeefcafe0004\n"
		if got := string(content); got != want {
			t.Errorf("sentinel content:\n got: %q\nwant: %q", got, want)
		}
	})

	t.Run("WriteSentinel still writes BLOCKED for exit 5 backward compat", func(t *testing.T) {
		dir := t.TempDir()
		sentPath := filepath.Join(dir, "run.done")

		var stderr strings.Builder
		WriteSentinel(sentPath, 5, "ses_b", "blocked", "", &stderr)

		if stderr.Len() > 0 {
			t.Errorf("unexpected stderr: %s", stderr.String())
		}

		content, err := os.ReadFile(sentPath)
		if err != nil {
			t.Fatalf("read sentinel: %v", err)
		}
		const want = "BLOCKED\nSESSION=ses_b\nSTOP_REASON=blocked\n"
		if got := string(content); got != want {
			t.Errorf("sentinel content:\n got: %q\nwant: %q", got, want)
		}
	})
}

// ---------------------------------------------------------------------------
// cleanupSentinelFiles
// ---------------------------------------------------------------------------

func TestCleanupSentinelFiles(t *testing.T) {
	t.Run("removes sentinel and perm req files", func(t *testing.T) {
		dir := t.TempDir()
		sentPath := filepath.Join(dir, "run.done")
		permBase := filepath.Join(dir, "run.perm")

		for _, p := range []string{
			sentPath,
			permBase + ".req",
			permBase + ".req.response",
		} {
			if err := os.WriteFile(p, []byte("old"), 0o600); err != nil {
				t.Fatalf("write %s: %v", p, err)
			}
		}

		var stderr strings.Builder
		cleanupSentinelFiles(sentPath, permBase, &stderr)

		if stderr.Len() > 0 {
			t.Errorf("unexpected stderr: %s", stderr.String())
		}
		for _, p := range []string{
			sentPath,
			permBase + ".req",
			permBase + ".req.response",
		} {
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Errorf("expected %s to be removed", p)
			}
		}
	})

	t.Run("missing files do not error", func(t *testing.T) {
		dir := t.TempDir()
		// These paths don't exist; cleanupSentinelFiles should not panic or log.
		var stderr strings.Builder
		cleanupSentinelFiles(filepath.Join(dir, "no-sent.done"), filepath.Join(dir, "no-perm"), &stderr)
		if stderr.Len() > 0 {
			t.Errorf("unexpected stderr for missing files: %s", stderr.String())
		}
	})

	t.Run("empty permBase skips perm cleanup", func(t *testing.T) {
		dir := t.TempDir()
		sentPath := filepath.Join(dir, "run.done")
		os.WriteFile(sentPath, []byte("old"), 0o600)

		var stderr strings.Builder
		cleanupSentinelFiles(sentPath, "", &stderr)

		if _, err := os.Stat(sentPath); !os.IsNotExist(err) {
			t.Error("expected sentinel to be removed")
		}
	})

	t.Run("non-ENOENT remove error is logged to stderr", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("cannot test permission errors as root")
		}
		dir := t.TempDir()
		// Make the directory read-only so Remove fails with permission denied.
		if err := os.Chmod(dir, 0o555); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		t.Cleanup(func() { os.Chmod(dir, 0o755) })

		sentPath := filepath.Join(dir, "run.done")
		// The file doesn't need to exist for Remove to return a non-ENOENT error
		// when the directory is not writable; on macOS/Linux this returns EACCES.
		// If os.Remove returns ErrNotExist instead (dir listing fails), the test
		// falls through harmlessly — we check for at least one log entry.
		var stderr strings.Builder
		cleanupSentinelFiles(sentPath, "", &stderr)

		// On read-only dir, Remove of a nonexistent file returns ENOENT (no log
		// needed). But if the file were to exist it'd be EACCES. Create the file
		// first by temporarily restoring write permission.
		os.Chmod(dir, 0o755)
		os.WriteFile(sentPath, []byte("x"), 0o600)
		os.Chmod(dir, 0o555)

		stderr.Reset()
		cleanupSentinelFiles(sentPath, "", &stderr)
		if stderr.Len() == 0 {
			t.Error("expected non-ENOENT error to be logged to stderr but got none")
		}
	})
}

// ---------------------------------------------------------------------------
// run — flag integration tests
// ---------------------------------------------------------------------------

func TestRunSentinelFileNotSet(t *testing.T) {
	// Regression: callers without --sentinel-file should see zero sentinel behavior.
	dir := t.TempDir()
	promptPath := filepath.Join(dir, "prompt.txt")
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentPath := filepath.Join(dir, "run.done")

	os.WriteFile(promptPath, []byte("hello"), 0o600)

	var stderr strings.Builder
	// The run will fail (no ACP server) but we only care that no sentinel file
	// is created when --sentinel-file is not passed.
	run([]string{
		"--prompt-file", promptPath,
		"--on-event", eventsPath,
	}, func(string) string { return "" }, &stderr)

	if _, err := os.Stat(sentPath); !os.IsNotExist(err) {
		t.Error("sentinel file written when --sentinel-file was not set")
	}
}

func TestRunExplicitPermissionHandlerWins(t *testing.T) {
	// When both --sentinel-file and --permission-handler are supplied, the
	// explicit --permission-handler must be used as-is (no derivation from
	// sentinel path).
	//
	// We verify the explicit perm base .req/.req.response are cleaned (not the
	// derived base), and that a sentinel is written after run completes.
	dir := t.TempDir()
	promptPath := filepath.Join(dir, "prompt.txt")
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentPath := filepath.Join(dir, "run.done")
	explicitPerm := filepath.Join(dir, "custom.perm")
	derivedPerm := filepath.Join(dir, "run.perm") // what would be derived from sentPath

	os.WriteFile(promptPath, []byte("hello"), 0o600)

	// Create stale files: both the explicit and the derived perm bases, plus sentinel.
	os.WriteFile(sentPath, []byte("stale"), 0o600)
	os.WriteFile(explicitPerm+".req", []byte("stale-explicit"), 0o600)
	os.WriteFile(explicitPerm+".req.response", []byte("stale-explicit"), 0o600)
	os.WriteFile(derivedPerm+".req", []byte("stale-derived"), 0o600)

	var stderr strings.Builder
	run([]string{
		"--prompt-file", promptPath,
		"--on-event", eventsPath,
		"--sentinel-file", sentPath,
		"--permission-handler", "file:" + explicitPerm,
	}, func(key string) string {
		if key == "AVENOR_OPENCODE_URL" {
			return "http://localhost:8080"
		}
		return ""
	}, &stderr)

	// The explicit perm's req files should have been cleaned.
	for _, p := range []string{explicitPerm + ".req", explicitPerm + ".req.response"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("expected explicit perm file to be cleaned: %s", p)
		}
	}

	// The derived perm base .req file should NOT have been cleaned (wrong base).
	if _, err := os.Stat(derivedPerm + ".req"); os.IsNotExist(err) {
		t.Errorf("derived perm .req should not have been cleaned when explicit handler was provided")
	}

	// A sentinel file must be written after the run (success or failure).
	if _, err := os.Stat(sentPath); os.IsNotExist(err) {
		t.Error("expected sentinel file to be written after run")
	}
}

func TestRunSentinelWrittenAfterFailedStart(t *testing.T) {
	// --sentinel-file is set; run fails because ACP server is unavailable.
	// Sentinel should still be written (FAILED).
	dir := t.TempDir()
	promptPath := filepath.Join(dir, "prompt.txt")
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentPath := filepath.Join(dir, "run.done")

	os.WriteFile(promptPath, []byte("hello"), 0o600)

	var stderr strings.Builder
	exitCode := run([]string{
		"--prompt-file", promptPath,
		"--on-event", eventsPath,
		"--sentinel-file", sentPath,
	}, func(string) string { return "" }, &stderr)

	if exitCode == 0 {
		t.Skip("unexpected success (opencode may be available); skipping failure-path test")
	}

	content, err := os.ReadFile(sentPath)
	if err != nil {
		t.Fatalf("sentinel not written: %v", err)
	}
	got := string(content)
	if !strings.HasPrefix(got, "FAILED\n") && !strings.HasPrefix(got, "TIMEOUT\n") && !strings.HasPrefix(got, "KILLED\n") {
		t.Errorf("unexpected sentinel content: %q", got)
	}
}

func TestRunPreRunCleanup(t *testing.T) {
	// When --sentinel-file is set, stale sentinel and perm req files must be
	// removed before the run starts (even if it fails immediately after).
	dir := t.TempDir()
	promptPath := filepath.Join(dir, "prompt.txt")
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentPath := filepath.Join(dir, "run.done")
	permBase := filepath.Join(dir, "run.perm")

	os.WriteFile(promptPath, []byte("hello"), 0o600)

	// Create stale files that the shim would clean.
	staleReq := permBase + ".req"
	staleResp := permBase + ".req.response"
	for _, p := range []string{sentPath, staleReq, staleResp} {
		os.WriteFile(p, []byte("stale"), 0o600)
	}

	// We need to observe cleanup BEFORE the run writes a new sentinel. Use a
	// trick: the run itself will write a new sentinel, so we can't check sentPath
	// after. But we CAN check the perm req files – they're cleaned and NOT
	// recreated by the run (the run only creates them when a permission.request
	// event arrives, which won't happen in a failing test).
	var stderr strings.Builder
	run([]string{
		"--prompt-file", promptPath,
		"--on-event", eventsPath,
		"--sentinel-file", sentPath,
	}, func(key string) string {
		if key == "AVENOR_OPENCODE_URL" {
			return "http://localhost:8080"
		}
		return ""
	}, &stderr)

	for _, p := range []string{staleReq, staleResp} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("stale file not cleaned: %s", p)
		}
	}
}
