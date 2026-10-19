package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// TestRunPIDFile records the run's own pid while it is alive and removes the
// file when it exits. The pid is observed from inside the attempt because the
// file is gone by the time run() returns. A pid path the run cannot write
// fails the invocation before any attempt starts.
func TestRunPIDFile(t *testing.T) {
	oldRunAttempt := runAttempt
	t.Cleanup(func() { runAttempt = oldRunAttempt })

	for _, tt := range []struct {
		name string
		// subdir names a directory the test never creates, so the pid write
		// fails.
		subdir       string
		exitCode     int
		wantAttempts int
	}{
		{name: "successful run", exitCode: 0, wantAttempts: 1},
		{name: "failed run", exitCode: 1, wantAttempts: 1},
		{name: "pid file write failure", subdir: "missing", exitCode: 1, wantAttempts: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			pidPath := filepath.Join(dir, tt.subdir, "run.pid")
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

			var stderr strings.Builder
			if code := run([]string{"--prompt", "hi", "--pid-file", pidPath}, func(string) string { return "" }, &stderr); code != tt.exitCode {
				t.Fatalf("run() = %d, want %d; stderr=%s", code, tt.exitCode, stderr.String())
			}
			if attempts != tt.wantAttempts {
				t.Fatalf("attempts = %d, want %d", attempts, tt.wantAttempts)
			}
			if tt.subdir != "" {
				if !strings.Contains(stderr.String(), "avenor: pid-file:") {
					t.Fatalf("stderr = %q, want a pid-file write error", stderr.String())
				}
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

// TestRunResultFile covers the text that reaches the result file and the
// RESULT= line that points at it. The attempt reports a distinct final_reply
// so a regression that writes the wrong field cannot pass.
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
		noSentinel  bool
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
		{
			name:        "result file written without a sentinel",
			finalOutput: "final answer",
			noSentinel:  true,
			wantFile:    true,
			wantContent: "final answer",
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

			raw, err := os.ReadFile(resultPath)
			switch {
			case tt.wantFile && err != nil:
				t.Fatalf("result file not written: %v", err)
			case !tt.wantFile && !os.IsNotExist(err):
				t.Fatalf("result file present with %q, want none", string(raw))
			case tt.wantFile && string(raw) != tt.wantContent:
				t.Fatalf("result file = %q, want %q", string(raw), tt.wantContent)
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
			if hasResult != tt.wantResult {
				t.Fatalf("sentinel RESULT presence = %t, want %t; sentinel=%q", hasResult, tt.wantResult, string(sentinel))
			}
		})
	}
}

// TestRunResultWrittenBeforeSentinel pins the documented ordering: a reader
// that waits for the sentinel never sees a missing or partial answer.
func TestRunResultWrittenBeforeSentinel(t *testing.T) {
	oldRunAttempt, oldAtomicWriteHook := runAttempt, atomicWriteHook
	t.Cleanup(func() {
		runAttempt, atomicWriteHook = oldRunAttempt, oldAtomicWriteHook
	})

	dir := t.TempDir()
	resultPath := filepath.Join(dir, "result.txt")
	sentinelPath := filepath.Join(dir, "run.done")
	var written []string
	atomicWriteHook = func(path string) { written = append(written, path) }

	runAttempt = func(ctx context.Context, cfg attemptConfig, deps attemptDeps) attemptResult {
		return attemptResult{exitCode: 0, sessionID: "ses_order", finalOutput: "final answer"}
	}

	var stderr strings.Builder
	if code := run([]string{
		"--prompt", "hi",
		"--result-file", resultPath,
		"--sentinel-file", sentinelPath,
	}, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	if want := []string{resultPath, sentinelPath}; !slices.Equal(written, want) {
		t.Fatalf("atomic write order = %v, want %v", written, want)
	}
}

// TestRunArtifactPathValidation rejects paths a sentinel line cannot advertise
// and paths two flags would let the last writer clobber.
func TestRunArtifactPathValidation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		result   string
		pid      string
		sentinel string
		wantErr  string
	}{
		{
			name:    "result and pid name one path",
			result:  "same.txt",
			pid:     "same.txt",
			wantErr: "--result-file and --pid-file name the same path",
		},
		{
			name:     "result and sentinel name one path",
			result:   "same.txt",
			sentinel: "./same.txt",
			wantErr:  "--result-file and --sentinel-file name the same path",
		},
		{
			name:     "pid and sentinel name one path",
			pid:      "same.txt",
			sentinel: "same.txt",
			wantErr:  "--pid-file and --sentinel-file name the same path",
		},
		{
			name:    "result path with a newline",
			result:  "line\nbreak.txt",
			wantErr: "--result-file must be a single-line path",
		},
		{
			name:    "pid path with a newline",
			pid:     "line\nbreak.txt",
			wantErr: "--pid-file must be a single-line path",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			args := []string{"--prompt", "hi"}
			for _, flag := range []struct{ name, value string }{
				{"--result-file", tt.result},
				{"--pid-file", tt.pid},
				{"--sentinel-file", tt.sentinel},
			} {
				if flag.value != "" {
					args = append(args, flag.name, filepath.Join(dir, flag.value))
				}
			}

			var stderr strings.Builder
			if code := run(args, func(string) string { return "" }, &stderr); code != 1 {
				t.Fatalf("run() = %d, want 1; stderr=%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tt.wantErr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tt.wantErr)
			}
			for _, name := range []string{tt.result, tt.pid} {
				// A sentinel path is written even on rejection, so only the
				// answer and pid paths prove the writes sit below validation.
				if name == "" || filepath.Clean(name) == filepath.Clean(tt.sentinel) {
					continue
				}
				if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
					t.Fatalf("rejected invocation created %q: %v", name, err)
				}
			}
		})
	}
}

// TestRunRejectedInvocationWritesNoArtifacts proves the artifact writes sit
// below flag validation: a rejected run neither deletes the caller's earlier
// answer nor publishes a pid, including in workflow mode where --result-file
// is unsupported.
func TestRunRejectedInvocationWritesNoArtifacts(t *testing.T) {
	dir := t.TempDir()
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"pre":[]}`), 0o600); err != nil {
		t.Fatalf("write loop config: %v", err)
	}
	promptPath := filepath.Join(dir, "prompt.txt")
	if err := os.WriteFile(promptPath, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write prompt: %v", err)
	}

	for _, tt := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "result file rejected in loop mode",
			args:    []string{"--loop-file", loopPath},
			wantErr: "--result-file is not supported with --loop-file or --team-file",
		},
		{
			name:    "result file rejected in team mode",
			args:    []string{"--team-file", filepath.Join(dir, "team.json")},
			wantErr: "--result-file is not supported with --loop-file or --team-file",
		},
		{
			name:    "mutually exclusive prompt sources",
			args:    []string{"--prompt", "hi", "--prompt-file", promptPath},
			wantErr: "--prompt and --prompt-file are mutually exclusive",
		},
		{
			name:    "unknown backend",
			args:    []string{"--prompt", "hi", "--backend", "nope"},
			wantErr: "unknown backend",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resultPath := filepath.Join(dir, tt.name+"-result.txt")
			pidPath := filepath.Join(dir, tt.name+"-run.pid")
			if err := os.WriteFile(resultPath, []byte("earlier answer"), 0o600); err != nil {
				t.Fatalf("write earlier answer: %v", err)
			}

			args := append(append([]string{}, tt.args...), "--result-file", resultPath, "--pid-file", pidPath)
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
		})
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

	provider := &scriptedArtifactProvider{turns: []scriptedTurn{
		{chunk: "first attempt", stopReason: "error"},
		{chunk: "second attempt", stopReason: "end_turn"},
	}}
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
}

// TestRunResultFileControlPromptTurn drives a control prompt that starts a
// second turn: the result file must hold the last turn's final output, not the
// first turn's text and not the accumulated reply.
func TestRunResultFileControlPromptTurn(t *testing.T) {
	oldNewProvider, oldNewControlServer := newProvider, newControlServer
	t.Cleanup(func() {
		newProvider, newControlServer = oldNewProvider, oldNewControlServer
	})

	provider := &scriptedArtifactProvider{turns: []scriptedTurn{
		{chunk: "first draft", stopReason: "end_turn"},
		{chunk: "second draft", stopReason: "end_turn", finalOutput: "authoritative answer"},
	}}
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }
	newControlServer = func(state *control.ControlState) *control.ControlServer {
		server := control.NewServer(state)
		server.QueuePrompt("second question")
		return server
	}

	dir := t.TempDir()
	resultPath := filepath.Join(dir, "result.txt")
	sentinelPath := filepath.Join(dir, "run.done")
	var stderr strings.Builder
	if code := run([]string{
		"--prompt", "hi",
		"--backend", "pi",
		"--control-socket", filepath.Join(dir, "control.sock"),
		"--result-file", resultPath,
		"--sentinel-file", sentinelPath,
	}, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	if got := provider.turnsRun(); got != 2 {
		t.Fatalf("turns = %d, want 2", got)
	}
	raw, err := os.ReadFile(resultPath)
	if err != nil || string(raw) != "authoritative answer" {
		t.Fatalf("result file = %q, %v; want the control turn's final output", raw, err)
	}
}

// scriptedTurn is one Prompt turn and the events it ends with.
type scriptedTurn struct {
	chunk      string
	stopReason string
	// finalOutput, when set, rides session.end as final_output, which is the
	// run's final text even when the reply chunks say otherwise.
	finalOutput string
}

// scriptedArtifactProvider serves one scriptedTurn per Prompt over a fresh
// event channel per start or resume, so a retry can resume the same session.
type scriptedArtifactProvider struct {
	mu     sync.Mutex
	turns  []scriptedTurn
	next   int
	events map[string]chan events.Event
}

func (p *scriptedArtifactProvider) open(sessionID string) {
	p.events[sessionID] = make(chan events.Event, 8)
}

func (p *scriptedArtifactProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.events == nil {
		p.events = map[string]chan events.Event{}
	}
	const sessionID = "ses_artifact"
	p.open(sessionID)
	return runtime.Session{SessionID: sessionID}, nil
}

func (p *scriptedArtifactProvider) Resume(_ context.Context, sessionID string) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.open(sessionID)
	return runtime.Session{SessionID: sessionID}, nil
}

func (p *scriptedArtifactProvider) Prompt(_ context.Context, sessionID, _ string) error {
	p.mu.Lock()
	if p.next >= len(p.turns) {
		p.mu.Unlock()
		return fmt.Errorf("unscripted turn %d", p.next)
	}
	turn := p.turns[p.next]
	p.next++
	ch := p.events[sessionID]
	p.mu.Unlock()

	if turn.chunk != "" {
		ch <- events.Event{
			Event:     "agent.message_chunk",
			SessionID: sessionID,
			Fields:    map[string]any{"content": map[string]any{"text": turn.chunk}},
		}
	}
	fields := map[string]any{"stop_reason": turn.stopReason}
	if turn.finalOutput != "" {
		fields["final_output"] = turn.finalOutput
	}
	ch <- events.Event{Event: "session.end", SessionID: sessionID, Fields: fields}
	close(ch)
	return nil
}

func (*scriptedArtifactProvider) Cancel(context.Context, string) error { return nil }

func (p *scriptedArtifactProvider) Events(_ context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch, ok := p.events[sessionID]
	if !ok {
		return nil, fmt.Errorf("unknown session %q", sessionID)
	}
	return ch, nil
}

func (*scriptedArtifactProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}

func (*scriptedArtifactProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

func (p *scriptedArtifactProvider) turnsRun() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.next
}
