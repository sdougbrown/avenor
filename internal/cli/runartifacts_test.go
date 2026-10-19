package cli

import (
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

func writeTestFile(t *testing.T, path, text string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func turn(chunk, stopReason, finalOutput string) scriptedTurn {
	return scriptedTurn{chunk: chunk, stopReason: stopReason, finalOutput: finalOutput}
}

// requireFile asserts path holds exactly want, or does not exist when want is
// empty.
func requireFile(t *testing.T, path, want string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if want == "" {
		if !os.IsNotExist(err) {
			t.Fatalf("%s = %q, want none", filepath.Base(path), raw)
		}
		return
	}
	if err != nil || string(raw) != want {
		t.Fatalf("%s = %q, %v; want %q", filepath.Base(path), raw, err, want)
	}
}

func runForTest(t *testing.T, args ...string) (int, string) {
	t.Helper()
	var stderr strings.Builder
	code := run(args, func(string) string { return "" }, &stderr)
	return code, stderr.String()
}

func stubProvider(t *testing.T, p *scriptedProvider) {
	t.Helper()
	old := newProvider
	t.Cleanup(func() { newProvider = old })
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) { return p, nil }
}

func waitForTest(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestRunArtifacts covers the files a direct run publishes for its caller:
// --result-file holds only a settled turn's final text, --pid-file holds this
// run's pid and is removed on exit, and neither may disturb a file another run
// owns.
func TestRunArtifacts(t *testing.T) {
	for _, tt := range []struct {
		name        string
		pidSubdir   string // never created, so the pid write fails
		promptFile  bool   // --prompt-file names a missing file
		providerErr bool
		resultDir   bool // result path is a non-empty directory
		timeout     bool
		progress    bool // --progress-timeout ends the run while a turn is open
		turns       []scriptedTurn
		eventLink   bool // --on-event is left as a dangling symlink
		foreignPID  string
		replacePID  bool // a concurrent run swaps the pid file in before removal
		wantCode    int
		wantErr     string
		wantResult  string // "" means no result file may exist
		wantRESULT  bool
	}{
		{
			name:       "settled turn publishes its final output",
			turns:      []scriptedTurn{turn("reply final answer", "end_turn", "final answer")},
			wantResult: "final answer",
			wantRESULT: true,
		},
		{name: "unsettled timeout publishes no file", timeout: true, turns: []scriptedTurn{turn("half an answer", "", "")}, wantCode: 124},
		{name: "progress timeout publishes no file", progress: true, turns: []scriptedTurn{turn("half an answer", "", "")}, wantCode: 124},
		{
			name:       "settled error stop reason publishes its final output",
			turns:      []scriptedTurn{turn("reply", "error", "answer before the backend failed")},
			wantCode:   1,
			wantResult: "answer before the backend failed",
			wantRESULT: true,
		},
		{
			name:       "a pid file another run owns outlives the run",
			turns:      []scriptedTurn{turn("reply", "end_turn", "final answer")},
			replacePID: true,
			wantResult: "final answer",
			wantRESULT: true,
		},
		{
			name:      "rename failure leaves no tmp file",
			resultDir: true,
			turns:     []scriptedTurn{turn("reply", "end_turn", "final answer")},
			wantErr:   "avenor: result-file: rename:",
		},
		{
			name:       "pid write failure keeps the earlier answer",
			pidSubdir:  "missing",
			wantCode:   1,
			wantErr:    "avenor: pid-file:",
			wantResult: "earlier answer",
		},
		{
			name:        "provider failure keeps the earlier answer",
			providerErr: true,
			wantCode:    1,
			wantErr:     "create provider",
			wantResult:  "earlier answer",
		},
		{
			name:       "setup failure leaves another run's pid file",
			promptFile: true,
			foreignPID: "424242\n",
			wantCode:   1,
			wantErr:    "read prompt file",
			wantResult: "earlier answer",
		},
		{
			name:       "rejected on-event symlink writes neither file",
			eventLink:  true,
			foreignPID: "424242\n",
			wantCode:   1,
			wantErr:    "open event stream",
			wantResult: "earlier answer",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			resultPath := filepath.Join(dir, "result.txt")
			pidPath := filepath.Join(dir, tt.pidSubdir, "run.pid")
			sentinelPath := filepath.Join(dir, "run.done")
			// A seeded answer must survive a run that never reaches its first
			// prompt, and must never survive one that settles.
			if !tt.resultDir {
				writeTestFile(t, resultPath, "earlier answer")
			} else if err := os.MkdirAll(filepath.Join(resultPath, "occupied"), 0o700); err != nil {
				t.Fatalf("make result dir: %v", err)
			}
			if tt.foreignPID != "" {
				writeTestFile(t, pidPath, tt.foreignPID)
			}
			if tt.replacePID {
				// The replacement is another run's file: only the guard in
				// removePIDFile keeps it.
				writeTestFile(t, filepath.Join(dir, "foreign.pid"), "424242\n")
				pidFileRemoveCheck = func() { _ = os.Rename(filepath.Join(dir, "foreign.pid"), pidPath) }
				t.Cleanup(func() { pidFileRemoveCheck = nil })
			}

			provider := newScriptedProvider()
			provider.turns = tt.turns
			stubProvider(t, provider)
			if tt.providerErr {
				newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) {
					return nil, errors.New("no provider")
				}
			}
			if tt.timeout || tt.progress {
				// Hold the first turn open so the deadline, not the reply, ends it.
				release := make(chan struct{})
				t.Cleanup(func() { close(release) })
				provider.promptStarted = make(chan struct{}, 1)
				provider.release = release
			}
			// The sentinel lands only after the answer is whole.
			atomicWriteHook = func(path string) {
				if path == sentinelPath && tt.wantResult != "" {
					requireFile(t, resultPath, tt.wantResult)
				}
			}
			t.Cleanup(func() { atomicWriteHook = nil })

			args := []string{"--prompt", "hi", "--backend", "pi", "--result-file", resultPath, "--pid-file", pidPath, "--sentinel-file", sentinelPath}
			if tt.promptFile {
				args[0], args[1] = "--prompt-file", filepath.Join(dir, "missing.txt")
			}
			if tt.timeout {
				args = append(args, "--timeout", "100ms")
			}
			if tt.progress {
				args = append(args, "--progress-timeout", "100ms")
			}
			if tt.eventLink {
				link := filepath.Join(dir, "event-link.ndjson")
				if err := os.Symlink(filepath.Join(dir, "missing-target.ndjson"), link); err != nil {
					t.Fatalf("symlink event log: %v", err)
				}
				args = append(args, "--on-event", link)
			}

			code, stderr := runForTest(t, args...)
			if code != tt.wantCode {
				t.Fatalf("run() = %d, want %d; stderr=%s", code, tt.wantCode, stderr)
			}
			if tt.wantErr != "" && !strings.Contains(stderr, tt.wantErr) {
				t.Fatalf("stderr = %q, want %q", stderr, tt.wantErr)
			}
			if tt.resultDir {
				if leftovers, _ := filepath.Glob(filepath.Join(dir, ".result-*.tmp")); len(leftovers) != 0 {
					t.Fatalf("leftover tmp files: %v", leftovers)
				}
			} else {
				requireFile(t, resultPath, tt.wantResult)
			}
			if tt.foreignPID != "" || tt.replacePID {
				requireFile(t, pidPath, "424242\n")
			} else {
				requireFile(t, pidPath, "")
			}
			sentinel, err := os.ReadFile(sentinelPath)
			if err != nil {
				t.Fatalf("read sentinel: %v", err)
			}
			if got := strings.Contains(string(sentinel), "RESULT="+resultPath+"\n"); got != tt.wantRESULT {
				t.Fatalf("sentinel RESULT = %t, want %t: %q", got, tt.wantRESULT, sentinel)
			}
		})
	}
}

// TestRunCancelledBySignal sends SIGTERM while the run is live and pins the
// trap's contract: exit 130, a killed sentinel naming the cancel, the pid file
// removed, and no result file advertising a truncated answer. The rows cover a
// live turn, the retry backoff, and a start still in flight.
func TestRunCancelledBySignal(t *testing.T) {
	for _, tt := range []struct {
		name       string
		turns      []scriptedTurn
		backoff    bool
		blockStart bool
	}{
		{name: "sigterm during a turn", turns: []scriptedTurn{{chunk: "half an answer"}}},
		{name: "sigterm during retry backoff", turns: []scriptedTurn{{chunk: "first attempt", stopReason: "error"}}, backoff: true},
		{name: "sigterm before the first session", blockStart: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			provider := newScriptedProvider()
			provider.turns = tt.turns
			stubProvider(t, provider)
			oldRetry := retryAfter
			t.Cleanup(func() { retryAfter = oldRetry })
			switch {
			case tt.backoff:
				retryAfter = func(time.Duration) <-chan time.Time {
					started <- struct{}{}
					return make(chan time.Time)
				}
			case tt.blockStart:
				provider.startStarted, provider.blockStart = started, true
			default:
				provider.promptStarted, provider.release = started, release
			}

			dir := t.TempDir()
			pidPath := filepath.Join(dir, "run.pid")
			resultPath := filepath.Join(dir, "result.txt")
			sentinelPath := filepath.Join(dir, "run.done")
			done := runAsync("--prompt", "hi", "--backend", "pi", "--max-retries", "1",
				"--pid-file", pidPath, "--result-file", resultPath, "--sentinel-file", sentinelPath)

			waitForTest(t, "the signal window", started)
			// Whatever the window, the pid file is already the live run's.
			requireFile(t, pidPath, strconv.Itoa(os.Getpid())+"\n")
			if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
				t.Fatalf("send SIGTERM: %v", err)
			}
			if got := awaitRun(t, done, "SIGTERM"); got.code != 130 {
				t.Fatalf("run() = %d, want 130; stderr=%s", got.code, got.stderr)
			}

			sentinel, err := os.ReadFile(sentinelPath)
			if err != nil {
				t.Fatalf("read sentinel: %v", err)
			}
			for _, want := range []string{"KILLED\n", "STOP_REASON=cancelled\n", "EXIT_CODE=130\n"} {
				if !strings.Contains(string(sentinel), want) {
					t.Fatalf("sentinel = %q, want %q", sentinel, want)
				}
			}
			requireFile(t, resultPath, "")
			requireFile(t, pidPath, "")
		})
	}
}

// TestValidateArtifactPaths covers spellings the CLI cannot compare as strings:
// a relative path against an absolute one, links to one file, and --on-event
// sharing a path with another artifact.
func TestValidateArtifactPaths(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeTestFile(t, "same.txt", "")
	link := filepath.Join(dir, "link.txt")
	if err := os.Symlink("same.txt", link); err != nil {
		t.Fatalf("symlink artifact: %v", err)
	}
	linkDir := filepath.Join(dir, "alias")
	if err := os.Symlink(dir, linkDir); err != nil {
		t.Fatalf("symlink artifact dir: %v", err)
	}
	same := filepath.Join(dir, "same.txt")
	samePathErr := "--result-file and --pid-file name the same path"

	for _, tt := range []struct {
		name               string
		result, pid, event string
		want               string
	}{
		{"abs against rel", same, "same.txt", "", samePathErr},
		{"pid links to result", same, link, "", samePathErr},
		{"pid through a symlinked directory", filepath.Join(dir, "absent.txt"), filepath.Join(linkDir, "absent.txt"), "", samePathErr},
		{"on-event", same, "", "same.txt", "--result-file and --on-event name the same path"},
		{"newline", "line\nbreak.txt", "", "", "--result-file must be a single-line path"},
	} {
		err := validateArtifactPaths(tt.result, tt.pid, "", tt.event)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Fatalf("%s: validateArtifactPaths() = %v, want %q", tt.name, err, tt.want)
		}
	}
	if err := validateArtifactPaths(same, filepath.Join(dir, "other.txt"), "", ""); err != nil {
		t.Fatalf("distinct paths rejected: %v", err)
	}
}
