package cli

import (
	"context"
	"errors"
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
		{name: "pid file published for the run and removed on exit"},
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

// TestRunSetupFailureLeavesEarlierPIDFile proves the pid file is published only
// after every setup step that can fail: a run rejected for a missing prompt
// file must leave another run's pid file untouched.
func TestRunSetupFailureLeavesEarlierPIDFile(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "run.pid")
	const foreign = "424242\n"
	if err := os.WriteFile(pidPath, []byte(foreign), 0o600); err != nil {
		t.Fatalf("write earlier pid file: %v", err)
	}

	var stderr strings.Builder
	if code := run([]string{
		"--prompt-file", filepath.Join(dir, "missing.txt"),
		"--pid-file", pidPath,
	}, func(string) string { return "" }, &stderr); code != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "read prompt file") {
		t.Fatalf("stderr = %q, want a prompt-file error", stderr.String())
	}
	raw, err := os.ReadFile(pidPath)
	if err != nil || string(raw) != foreign {
		t.Fatalf("failed setup touched another run's pid file: %q, %v", raw, err)
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
// the answer is already whole. The reply chunk differs from final_output so a
// regression that writes the wrong field cannot pass, and a stale answer is
// seeded wherever the path is writable so the replacement is visible.
func TestRunResultFile(t *testing.T) {
	oldNewProvider, oldAtomicWriteHook := newProvider, atomicWriteHook
	t.Cleanup(func() {
		newProvider, atomicWriteHook = oldNewProvider, oldAtomicWriteHook
	})

	for _, tt := range []struct {
		name string
		// subdir puts the result file under a directory the test never creates,
		// so the write fails.
		subdir string
		// resultDir makes the result path a non-empty directory, so the rename
		// fails after the tmp file was written.
		resultDir   bool
		stopReason  string
		reply       string
		finalOutput string
		noSentinel  bool
		wantFile    bool
		wantCode    int
	}{
		{name: "successful run writes final text", stopReason: "end_turn", reply: "reply final answer", finalOutput: "final answer", wantFile: true},
		{name: "failed run writes the final text it settled with", stopReason: "error", reply: "reply refused: needs a decision", finalOutput: "refused: needs a decision", wantCode: 1, wantFile: true},
		{name: "no final text writes no file", stopReason: "end_turn"},
		{name: "unwritable path leaves the sentinel without RESULT", subdir: "missing", stopReason: "end_turn", reply: "reply final answer", finalOutput: "final answer"},
		{name: "rename onto a non-empty directory leaves no tmp file", resultDir: true, stopReason: "end_turn", reply: "reply final answer", finalOutput: "final answer"},
		{name: "result file written without a sentinel", stopReason: "end_turn", reply: "reply final answer", finalOutput: "final answer", noSentinel: true, wantFile: true},
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

			provider := newScriptedProvider()
			provider.turns = []scriptedTurn{{chunk: tt.reply, stopReason: tt.stopReason, finalOutput: tt.finalOutput}}
			newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

			args := []string{"--prompt", "hi", "--backend", "pi", "--result-file", resultPath}
			if !tt.noSentinel {
				args = append(args, "--sentinel-file", sentinelPath)
			}
			var stderr strings.Builder
			code := run(args, func(string) string { return "" }, &stderr)
			if code != tt.wantCode {
				t.Fatalf("run() = %d, want %d; stderr=%s", code, tt.wantCode, stderr.String())
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

// TestRunTimeoutWritesNoResult holds a turn open past --timeout: the deadline
// stops the run mid-answer, so the recycled reply is not a final message and
// neither the result file nor a RESULT= line is published.
func TestRunTimeoutWritesNoResult(t *testing.T) {
	oldNewProvider := newProvider
	t.Cleanup(func() { newProvider = oldNewProvider })
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	provider := newScriptedProvider()
	provider.turns = []scriptedTurn{{chunk: "half an answer"}}
	provider.promptStarted = make(chan struct{}, 1)
	provider.release = release
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

	dir := t.TempDir()
	resultPath := filepath.Join(dir, "result.txt")
	if err := os.WriteFile(resultPath, []byte("earlier answer"), 0o600); err != nil {
		t.Fatalf("write earlier answer: %v", err)
	}
	sentinelPath := filepath.Join(dir, "run.done")
	var stderr strings.Builder
	if code := run([]string{
		"--prompt", "hi",
		"--backend", "pi",
		"--timeout", "100ms",
		"--result-file", resultPath,
		"--sentinel-file", sentinelPath,
	}, func(string) string { return "" }, &stderr); code != 124 {
		t.Fatalf("run() = %d, want 124; stderr=%s", code, stderr.String())
	}
	sentinel, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	for _, want := range []string{"TIMEOUT\n", "STOP_REASON=timeout\n"} {
		if !strings.Contains(string(sentinel), want) {
			t.Fatalf("sentinel = %q, want %q", sentinel, want)
		}
	}
	if strings.Contains(string(sentinel), "RESULT=") {
		t.Fatalf("timed-out sentinel advertises a result: %q", sentinel)
	}
	if _, err := os.Stat(resultPath); !os.IsNotExist(err) {
		t.Fatalf("timed-out run wrote or kept a result file: %v", err)
	}
}

// TestRunProviderFailureLeavesEarlierResult proves the stale answer is removed
// only once an attempt reaches its first prompt: a provider that cannot be
// created never prompts, so the caller's earlier answer survives.
func TestRunProviderFailureLeavesEarlierResult(t *testing.T) {
	oldNewProvider := newProvider
	t.Cleanup(func() { newProvider = oldNewProvider })
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) {
		return nil, errors.New("no provider")
	}

	dir := t.TempDir()
	resultPath := filepath.Join(dir, "result.txt")
	if err := os.WriteFile(resultPath, []byte("earlier answer"), 0o600); err != nil {
		t.Fatalf("write earlier answer: %v", err)
	}

	var stderr strings.Builder
	if code := run([]string{
		"--prompt", "hi",
		"--backend", "pi",
		"--result-file", resultPath,
	}, func(string) string { return "" }, &stderr); code != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "create provider") {
		t.Fatalf("stderr = %q, want a provider error", stderr.String())
	}
	raw, err := os.ReadFile(resultPath)
	if err != nil || string(raw) != "earlier answer" {
		t.Fatalf("failed provider touched the earlier answer: %q, %v", raw, err)
	}
}

// TestRunRejectedInvocationWritesNoArtifacts proves validation sits above the
// artifact writes: a rejected run writes no pid file and leaves the caller's
// earlier answer alone. Every rejection still records a sentinel.
func TestRunRejectedInvocationWritesNoArtifacts(t *testing.T) {
	dir := t.TempDir()
	// blockedSocket names a control socket under a regular file, so a row whose
	// expected rejection regressed still fails before any backend starts.
	blockedSocket := func(dir string) string {
		t.Helper()
		blocker := filepath.Join(dir, "not-a-dir")
		if err := os.WriteFile(blocker, nil, 0o600); err != nil {
			t.Fatalf("write socket blocker: %v", err)
		}
		return filepath.Join(blocker, "control.sock")
	}
	for _, tt := range []struct {
		name    string
		args    func(dir, result, pid string) []string
		wantErr string
	}{
		{
			name: "on-event names the result path",
			args: func(dir, result, pid string) []string {
				return []string{"--prompt", "hi", "--result-file", result, "--pid-file", pid, "--on-event", result, "--control-socket", blockedSocket(dir)}
			},
			wantErr: "--result-file and --on-event name the same path",
		},
		{
			// The event log is opened without following a final symlink, so a
			// dangling link aimed at the pid path cannot be created through it.
			name: "on-event link aimed at the pid path",
			args: func(dir, _, pid string) []string {
				link := filepath.Join(dir, "event-link.ndjson")
				if err := os.Symlink(pid, link); err != nil {
					t.Fatalf("symlink event log: %v", err)
				}
				return []string{"--prompt", "hi", "--pid-file", pid, "--on-event", link, "--control-socket", blockedSocket(dir)}
			},
			wantErr: "open event stream",
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
	linkDir := filepath.Join(dir, "alias")
	if err := os.Symlink(dir, linkDir); err != nil {
		t.Fatalf("symlink artifact dir: %v", err)
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
		{name: "pid through a symlinked directory", result: filepath.Join(dir, "absent.txt"), pid: filepath.Join(linkDir, "absent.txt"), wantErr: "--result-file and --pid-file name the same path"},
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
// answer. The rows cover a live turn, the retry backoff, and a start that is
// still in flight — the last one signals only once the pid file is already on
// disk, so a trap installed below that write would kill the test process
// outright.
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
		// startBlock cancels while the provider is starting the first session,
		// before any turn exists to report the cancel.
		startBlock bool
		// retries is the --max-retries value; the start row keeps the default 0
		// so the attempt is the run's only one.
		retries int
	}{
		{
			name:    "sigterm during a turn",
			turns:   []scriptedTurn{{chunk: "half an answer"}},
			retries: 1,
		},
		{
			name:    "sigterm during retry backoff",
			turns:   []scriptedTurn{{chunk: "first attempt", stopReason: "error"}},
			backoff: true,
			retries: 1,
		},
		{
			name:       "sigterm during provider start",
			startBlock: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })

			provider := newScriptedProvider()
			provider.turns = tt.turns
			switch {
			case tt.backoff:
				retryAfter = func(time.Duration) <-chan time.Time {
					started <- struct{}{}
					return make(chan time.Time)
				}
			case tt.startBlock:
				provider.startStarted = started
				provider.blockStart = true
			default:
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
					"--max-retries", strconv.Itoa(tt.retries),
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
			if tt.startBlock {
				if _, err := os.Stat(pidPath); err != nil {
					t.Fatalf("pid file not published before the first session: %v", err)
				}
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
