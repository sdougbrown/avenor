package cli

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// TestRunPIDFile records the run's own pid while it is alive, removes the file
// on exit, and leaves the caller's earlier answer alone when the pid write
// fails. The pid is read from inside the attempt because it is gone by the time
// run() returns.
func TestRunPIDFile(t *testing.T) {
	oldRunAttempt := runAttempt
	t.Cleanup(func() { runAttempt = oldRunAttempt })

	for _, tt := range []struct {
		name string
		// pidDir names a directory the test never creates, so the pid write
		// fails before the first attempt.
		pidDir   string
		exitCode int
	}{
		{name: "successful run"},
		{name: "failed run", exitCode: 1},
		{name: "pid file write failure", pidDir: "missing", exitCode: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			pidPath := filepath.Join(dir, tt.pidDir, "run.pid")
			wantPID := strconv.Itoa(os.Getpid())

			attempts := 0
			runAttempt = func(ctx context.Context, cfg attemptConfig, deps attemptDeps) attemptResult {
				attempts++
				raw, err := os.ReadFile(pidPath)
				if err != nil {
					t.Fatalf("pid file not written before the run started: %v", err)
				}
				if got := strings.TrimSpace(string(raw)); got != wantPID {
					t.Fatalf("pid file = %q, want %q", got, wantPID)
				}
				return attemptResult{exitCode: tt.exitCode, sessionID: "ses_pid"}
			}

			args := []string{"--prompt", "hi", "--pid-file", pidPath}
			resultPath := filepath.Join(dir, "result.txt")
			if tt.pidDir != "" {
				// The pid write must fail before the earlier answer is removed.
				if err := os.WriteFile(resultPath, []byte("earlier answer"), 0o600); err != nil {
					t.Fatalf("write earlier answer: %v", err)
				}
				args = append(args, "--result-file", resultPath)
			}

			var stderr strings.Builder
			if code := run(args, func(string) string { return "" }, &stderr); code != tt.exitCode {
				t.Fatalf("run() = %d, want %d; stderr=%s", code, tt.exitCode, stderr.String())
			}
			wantAttempts := 1
			if tt.pidDir != "" {
				wantAttempts = 0
				if !strings.Contains(stderr.String(), "avenor: pid-file:") {
					t.Fatalf("stderr = %q, want a pid-file write error", stderr.String())
				}
				raw, err := os.ReadFile(resultPath)
				if err != nil || string(raw) != "earlier answer" {
					t.Fatalf("pid-file failure touched the earlier answer: %q, %v", raw, err)
				}
			}
			if attempts != wantAttempts {
				t.Fatalf("attempts = %d, want %d", attempts, wantAttempts)
			}
			if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
				t.Fatalf("pid file still exists after run() returned: %v", err)
			}
		})
	}
}

// TestRemovePIDFile checks the compare-and-remove guard: a file whose content
// is another run's pid, and a file a concurrent run renamed into the same path
// after the read, both survive.
func TestRemovePIDFile(t *testing.T) {
	for _, tt := range []struct {
		name     string
		content  string
		replace  bool
		wantGone bool
	}{
		{name: "own pid file", content: strconv.Itoa(os.Getpid()) + "\n", wantGone: true},
		{name: "another run's pid", content: "424242\n"},
		{name: "path replaced after the read", content: strconv.Itoa(os.Getpid()) + "\n", replace: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "run.pid")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatalf("write pid file: %v", err)
			}
			if tt.replace {
				// Same content, different inode: only the re-check after the
				// read can tell this file from the one this run wrote.
				foreign := filepath.Join(dir, "foreign.pid")
				if err := os.WriteFile(foreign, []byte(tt.content), 0o600); err != nil {
					t.Fatalf("write replacement pid file: %v", err)
				}
				pidFileRemoveCheck = func() { _ = os.Rename(foreign, path) }
				t.Cleanup(func() { pidFileRemoveCheck = nil })
			}

			removePIDFile(path, os.Getpid())

			_, err := os.Stat(path)
			if got := os.IsNotExist(err); got != tt.wantGone {
				t.Fatalf("pid file removed = %t, want %t (stat error %v)", got, tt.wantGone, err)
			}
		})
	}
}

// TestRunResultFile covers the text that reaches the result file, the RESULT=
// line that points at it, and the documented ordering: when the sentinel lands,
// the answer is already whole. The attempt reports a distinct final_reply so a
// regression that writes the wrong field cannot pass.
func TestRunResultFile(t *testing.T) {
	oldRunAttempt, oldAtomicWriteHook := runAttempt, atomicWriteHook
	t.Cleanup(func() { runAttempt, atomicWriteHook = oldRunAttempt, oldAtomicWriteHook })

	for _, tt := range []struct {
		name string
		// subdir puts the result file under a directory the test never creates,
		// so the write fails.
		subdir string
		// resultDir makes the result path a non-empty directory, so the rename
		// fails after the tmp file was written.
		resultDir   bool
		exitCode    int
		finalOutput string
		noSentinel  bool
		wantFile    bool
	}{
		{name: "successful run writes final text", finalOutput: "final answer", wantFile: true},
		{name: "failed run writes the final text it settled with", exitCode: 1, finalOutput: "refused: needs a decision", wantFile: true},
		{name: "no final text writes no file"},
		{name: "unwritable path leaves the sentinel without RESULT", subdir: "missing", finalOutput: "final answer"},
		{name: "rename onto a non-empty directory leaves no tmp file", resultDir: true, finalOutput: "final answer"},
		{name: "result file written without a sentinel", finalOutput: "final answer", noSentinel: true, wantFile: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			resultPath := filepath.Join(dir, tt.subdir, "result.txt")
			sentinelPath := filepath.Join(dir, "run.done")
			if tt.resultDir {
				if err := os.MkdirAll(filepath.Join(resultPath, "occupied"), 0o700); err != nil {
					t.Fatalf("make result dir: %v", err)
				}
			} else if tt.subdir == "" {
				// A stale answer from an earlier run must never survive.
				if err := os.WriteFile(resultPath, []byte("stale answer"), 0o600); err != nil {
					t.Fatalf("write stale result: %v", err)
				}
			}

			wantResult := tt.wantFile && !tt.noSentinel
			atomicWriteHook = nil
			if wantResult {
				atomicWriteHook = func(path string) {
					if path != sentinelPath {
						return
					}
					raw, err := os.ReadFile(resultPath)
					if err != nil || string(raw) != tt.finalOutput {
						t.Errorf("result file at sentinel time = %q, %v; want %q", raw, err, tt.finalOutput)
					}
				}
			}

			runAttempt = func(ctx context.Context, cfg attemptConfig, deps attemptDeps) attemptResult {
				return attemptResult{
					exitCode:    tt.exitCode,
					sessionID:   "ses_result",
					finalOutput: tt.finalOutput,
					finalReply:  "reply " + tt.finalOutput,
				}
			}

			args := []string{"--prompt", "hi", "--result-file", resultPath}
			if !tt.noSentinel {
				args = append(args, "--sentinel-file", sentinelPath)
			}
			var stderr strings.Builder
			code := run(args, func(string) string { return "" }, &stderr)
			if code != tt.exitCode {
				t.Fatalf("run() = %d, want %d; stderr=%s", code, tt.exitCode, stderr.String())
			}

			if tt.resultDir {
				if !strings.Contains(stderr.String(), "avenor: result-file: rename:") {
					t.Fatalf("stderr = %q, want a result rename error", stderr.String())
				}
				if leftovers, _ := filepath.Glob(filepath.Join(dir, ".result-*.tmp")); len(leftovers) != 0 {
					t.Fatalf("leftover tmp files: %v", leftovers)
				}
			} else {
				raw, err := os.ReadFile(resultPath)
				switch {
				case tt.wantFile && err != nil:
					t.Fatalf("result file not written: %v", err)
				case !tt.wantFile && !os.IsNotExist(err):
					t.Fatalf("result file present with %q, want none", string(raw))
				case tt.wantFile && string(raw) != tt.finalOutput:
					t.Fatalf("result file = %q, want %q", string(raw), tt.finalOutput)
				}
			}

			if tt.noSentinel {
				if _, err := os.Stat(sentinelPath); !os.IsNotExist(err) {
					t.Fatalf("sentinel written without --sentinel-file: %v", err)
				}
				return
			}
			sentinel, err := os.ReadFile(sentinelPath)
			if err != nil {
				t.Fatalf("read sentinel: %v", err)
			}
			hasResult := strings.Contains(string(sentinel), "RESULT="+resultPath+"\n")
			if hasResult != wantResult {
				t.Fatalf("sentinel RESULT presence = %t, want %t; sentinel=%q", hasResult, wantResult, string(sentinel))
			}
		})
	}
}

// TestRunRejectedInvocationWritesNoArtifacts proves validation sits above the
// artifact writes: a rejected run writes no pid file and leaves the caller's
// earlier answer alone. Every rejection still records a sentinel.
func TestRunRejectedInvocationWritesNoArtifacts(t *testing.T) {
	dir := t.TempDir()
	for _, tt := range []struct {
		name    string
		args    func(dir, result, pid string) []string
		wantErr string
	}{
		{
			name: "result and pid name one path",
			args: func(_, result, _ string) []string {
				return []string{"--prompt", "hi", "--result-file", result, "--pid-file", result}
			},
			wantErr: "--result-file and --pid-file name the same path",
		},
		{
			name: "result path with a newline",
			args: func(dir, _, pid string) []string {
				return []string{"--prompt", "hi", "--result-file", filepath.Join(dir, "line\nbreak.txt"), "--pid-file", pid}
			},
			wantErr: "--result-file must be a single-line path",
		},
		{
			name: "unreadable prompt file",
			args: func(dir, result, pid string) []string {
				return []string{"--prompt-file", filepath.Join(dir, "missing.txt"), "--result-file", result, "--pid-file", pid}
			},
			wantErr: "read prompt file",
		},
		{
			name: "result file rejected in workflow mode",
			args: func(dir, result, pid string) []string {
				return []string{"--loop-file", filepath.Join(dir, "loop.json"), "--result-file", result, "--pid-file", pid}
			},
			wantErr: "--result-file is not supported with --loop-file or --team-file",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resultPath := filepath.Join(dir, tt.name+"-result.txt")
			pidPath := filepath.Join(dir, tt.name+"-run.pid")
			sentinelPath := filepath.Join(dir, tt.name+"-run.done")
			if err := os.WriteFile(resultPath, []byte("earlier answer"), 0o600); err != nil {
				t.Fatalf("write earlier answer: %v", err)
			}

			args := append(tt.args(dir, resultPath, pidPath), "--sentinel-file", sentinelPath)
			var stderr strings.Builder
			if code := run(args, func(string) string { return "" }, &stderr); code != 1 {
				t.Fatalf("run() = %d, want 1; stderr=%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tt.wantErr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tt.wantErr)
			}
			raw, err := os.ReadFile(resultPath)
			if err != nil || string(raw) != "earlier answer" {
				t.Fatalf("rejected invocation touched the earlier answer: %q, %v", raw, err)
			}
			if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
				t.Fatalf("rejected invocation published a pid file: %v", err)
			}
			sentinel, err := os.ReadFile(sentinelPath)
			if err != nil || !strings.HasPrefix(string(sentinel), "FAILED\n") {
				t.Fatalf("sentinel = %q, %v; want a FAILED outcome", sentinel, err)
			}
		})
	}
}

// TestValidateArtifactPaths covers spellings the CLI cannot compare as strings:
// a relative path against an absolute one, a link to an existing file, and
// --on-event sharing a path with another artifact.
func TestValidateArtifactPaths(t *testing.T) {
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	abs := filepath.Join(dir, "same.txt")
	if err := os.WriteFile(abs, nil, 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	rel, err := filepath.Rel(cwd, abs)
	if err != nil {
		t.Fatalf("make relative path: %v", err)
	}
	link := filepath.Join(dir, "link.txt")
	if err := os.Symlink(abs, link); err != nil {
		t.Fatalf("symlink artifact: %v", err)
	}

	for _, tt := range []struct {
		name                           string
		result, pid, sentinel, onEvent string
		wantErr                        string
	}{
		{name: "result and pid abs against rel", result: abs, pid: rel, wantErr: "--result-file and --pid-file name the same path"},
		{name: "result and sentinel spelling", result: abs, sentinel: rel, wantErr: "--result-file and --sentinel-file name the same path"},
		{name: "pid and sentinel", pid: "same.txt", sentinel: "same.txt", wantErr: "--pid-file and --sentinel-file name the same path"},
		{name: "result and on-event", result: abs, onEvent: rel, wantErr: "--result-file and --on-event name the same path"},
		{name: "pid links to result", result: abs, pid: link, wantErr: "--result-file and --pid-file name the same path"},
		{name: "result path with a newline", result: "line\nbreak.txt", wantErr: "--result-file must be a single-line path"},
		{name: "pid path with a newline", pid: "line\nbreak.txt", wantErr: "--pid-file must be a single-line path"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateArtifactPaths(tt.result, tt.pid, tt.sentinel, tt.onEvent)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateArtifactPaths() = %v, want %q", err, tt.wantErr)
			}
		})
	}

	if err := validateArtifactPaths(abs, filepath.Join(dir, "other.txt"), "", ""); err != nil {
		t.Fatalf("distinct paths rejected: %v", err)
	}
}

// TestRunResultFileFollowsRetryAnswer drives the real attempt path so the retry
// loop, not a stub, decides the text: attempt A fails retryably, attempt B
// succeeds, and the result file must hold B.
func TestRunResultFileFollowsRetryAnswer(t *testing.T) {
	oldNewProvider, oldRetryAfter := newProvider, retryAfter
	t.Cleanup(func() {
		newProvider, retryAfter = oldNewProvider, oldRetryAfter
	})

	provider := newScriptedProvider()
	provider.turns = []scriptedTurn{
		{chunk: "first attempt", stopReason: "error"},
		{chunk: "second attempt", stopReason: "end_turn"},
	}
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }
	retryAfter = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}

	dir := t.TempDir()
	resultPath := filepath.Join(dir, "result.txt")
	sentinelPath := filepath.Join(dir, "run.done")
	var stderr strings.Builder
	if code := run([]string{
		"--prompt", "hi",
		"--backend", "pi",
		"--max-retries", "1",
		"--result-file", resultPath,
		"--sentinel-file", sentinelPath,
	}, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	if got := provider.turnsRun(); got != 2 {
		t.Fatalf("turns = %d, want 2", got)
	}
	raw, err := os.ReadFile(resultPath)
	if err != nil || string(raw) != "second attempt" {
		t.Fatalf("result file = %q, %v; want the retry's answer", raw, err)
	}
	sentinel, err := os.ReadFile(sentinelPath)
	if err != nil || !strings.Contains(string(sentinel), "RESULT="+resultPath+"\n") {
		t.Fatalf("sentinel = %q, %v; want RESULT=%s", sentinel, err, resultPath)
	}
}

// TestRunCancelledBySignal sends SIGTERM to the test process once the run is
// live and pins the trap's contract: exit 130, a killed sentinel naming the
// cancel, the pid file removed, and no result file advertising a truncated
// answer. The backoff row covers a cancel that lands between attempts, where
// the previous attempt's stop reason is still loaded.
func TestRunCancelledBySignal(t *testing.T) {
	oldNewProvider, oldRetryAfter := newProvider, retryAfter
	t.Cleanup(func() {
		newProvider, retryAfter = oldNewProvider, oldRetryAfter
	})

	for _, tt := range []struct {
		name  string
		turns []scriptedTurn
		// backoff cancels during the retry backoff instead of a live turn.
		backoff bool
	}{
		{
			name:  "sigterm during a turn",
			turns: []scriptedTurn{{chunk: "half an answer"}},
		},
		{
			name:    "sigterm during retry backoff",
			turns:   []scriptedTurn{{chunk: "first attempt", stopReason: "error"}},
			backoff: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })

			provider := newScriptedProvider()
			provider.turns = tt.turns
			if tt.backoff {
				retryAfter = func(time.Duration) <-chan time.Time {
					started <- struct{}{}
					return make(chan time.Time)
				}
			} else {
				provider.promptStarted = started
				provider.release = release
			}
			newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

			dir := t.TempDir()
			pidPath := filepath.Join(dir, "run.pid")
			resultPath := filepath.Join(dir, "result.txt")
			sentinelPath := filepath.Join(dir, "run.done")

			type outcome struct {
				code   int
				stderr string
			}
			done := make(chan outcome, 1)
			go func() {
				var stderr strings.Builder
				code := run([]string{
					"--prompt", "hi",
					"--backend", "pi",
					"--max-retries", "1",
					"--pid-file", pidPath,
					"--result-file", resultPath,
					"--sentinel-file", sentinelPath,
				}, func(string) string { return "" }, &stderr)
				done <- outcome{code: code, stderr: stderr.String()}
			}()

			select {
			case <-started:
			case <-time.After(10 * time.Second):
				t.Fatal("run never reached the signal window")
			}
			if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
				t.Fatalf("send SIGTERM: %v", err)
			}

			var got outcome
			select {
			case got = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("run did not exit after SIGTERM")
			}
			if got.code != 130 {
				t.Fatalf("run() = %d, want 130; stderr=%s", got.code, got.stderr)
			}

			raw, err := os.ReadFile(sentinelPath)
			if err != nil {
				t.Fatalf("read sentinel: %v", err)
			}
			sentinel := string(raw)
			for _, want := range []string{"KILLED\n", "STOP_REASON=cancelled\n", "EXIT_CODE=130\n"} {
				if !strings.Contains(sentinel, want) {
					t.Fatalf("sentinel = %q, want %q", sentinel, want)
				}
			}
			if strings.Contains(sentinel, "RESULT=") {
				t.Fatalf("cancelled sentinel advertises a result: %q", sentinel)
			}
			if _, err := os.Stat(resultPath); !os.IsNotExist(err) {
				t.Fatalf("cancelled run wrote a result file: %v", err)
			}
			if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
				t.Fatalf("pid file outlived the cancelled run: %v", err)
			}
		})
	}
}
