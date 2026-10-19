package cli

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRunPIDFile records the run's own pid while it is alive and removes the
// file when it exits. The pid is observed from inside the attempt because the
// file is gone by the time run() returns.
func TestRunPIDFile(t *testing.T) {
	oldRunAttempt := runAttempt
	t.Cleanup(func() { runAttempt = oldRunAttempt })

	for _, tt := range []struct {
		name     string
		exitCode int
	}{
		{name: "successful run", exitCode: 0},
		{name: "failed run", exitCode: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			pidPath := filepath.Join(dir, "run.pid")
			wantPID := strconv.Itoa(os.Getpid())

			runAttempt = func(ctx context.Context, cfg attemptConfig, deps attemptDeps) attemptResult {
				raw, err := os.ReadFile(pidPath)
				if err != nil {
					t.Fatalf("pid file not written before the run started: %v", err)
				}
				if got := strings.TrimSpace(string(raw)); got != wantPID {
					t.Fatalf("pid file = %q, want %q", got, wantPID)
				}
				return attemptResult{exitCode: tt.exitCode, sessionID: "ses_pid"}
			}

			var stderr strings.Builder
			if code := run([]string{"--prompt", "hi", "--pid-file", pidPath}, func(string) string { return "" }, &stderr); code != tt.exitCode {
				t.Fatalf("run() = %d, want %d; stderr=%s", code, tt.exitCode, stderr.String())
			}
			if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
				t.Fatalf("pid file still exists after the run exited: %v", err)
			}
		})
	}
}

func TestRemovePIDFileKeepsAnotherRunsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.pid")
	if err := os.WriteFile(path, []byte("424242\n"), 0o600); err != nil {
		t.Fatalf("write pid file: %v", err)
	}
	removePIDFile(path, os.Getpid())
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("removePIDFile deleted a pid file it did not write: %v", err)
	}
}

func TestRunResultFile(t *testing.T) {
	oldRunAttempt := runAttempt
	t.Cleanup(func() { runAttempt = oldRunAttempt })

	for _, tt := range []struct {
		name string
		// subdir, when set, puts the result file under a directory the test
		// never creates, so the write fails.
		subdir      string
		exitCode    int
		finalOutput string
		wantFile    bool
		wantContent string
		wantResult  bool
	}{
		{
			name:        "successful run writes final text",
			finalOutput: "final answer",
			wantFile:    true,
			wantContent: "final answer",
			wantResult:  true,
		},
		{
			name:        "failed run writes the final text it settled with",
			exitCode:    1,
			finalOutput: "refused: needs a decision",
			wantFile:    true,
			wantContent: "refused: needs a decision",
			wantResult:  true,
		},
		{
			name: "no final text writes no file",
		},
		{
			name:        "unwritable path leaves the sentinel without RESULT",
			subdir:      "missing",
			finalOutput: "final answer",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			resultPath := filepath.Join(dir, tt.subdir, "result.txt")
			sentinelPath := filepath.Join(dir, "run.done")
			// A stale answer from an earlier run must never survive.
			if tt.subdir == "" {
				if err := os.WriteFile(resultPath, []byte("stale answer"), 0o600); err != nil {
					t.Fatalf("write stale result: %v", err)
				}
			}

			runAttempt = func(ctx context.Context, cfg attemptConfig, deps attemptDeps) attemptResult {
				return attemptResult{exitCode: tt.exitCode, sessionID: "ses_result", finalOutput: tt.finalOutput}
			}

			var stderr strings.Builder
			code := run([]string{
				"--prompt", "hi",
				"--result-file", resultPath,
				"--sentinel-file", sentinelPath,
			}, func(string) string { return "" }, &stderr)
			if code != tt.exitCode {
				t.Fatalf("run() = %d, want %d; stderr=%s", code, tt.exitCode, stderr.String())
			}

			raw, err := os.ReadFile(resultPath)
			switch {
			case tt.wantFile && err != nil:
				t.Fatalf("result file not written: %v", err)
			case !tt.wantFile && !os.IsNotExist(err):
				t.Fatalf("result file present with %q, want none", string(raw))
			case tt.wantFile && string(raw) != tt.wantContent:
				t.Fatalf("result file = %q, want %q", string(raw), tt.wantContent)
			}

			sentinel, err := os.ReadFile(sentinelPath)
			if err != nil {
				t.Fatalf("read sentinel: %v", err)
			}
			hasResult := strings.Contains(string(sentinel), "RESULT="+resultPath+"\n")
			if hasResult != tt.wantResult {
				t.Fatalf("sentinel RESULT presence = %t, want %t; sentinel=%q", hasResult, tt.wantResult, string(sentinel))
			}
		})
	}
}
