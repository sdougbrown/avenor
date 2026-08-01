package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type thinkingProvider struct {
	startCalls       int
	resumeCalls      int
	resumeOptsCalls  int
	lastResumeOption runtime.StartOptions
}

func (p *thinkingProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	p.startCalls++
	return runtime.Session{SessionID: "started"}, nil
}
func (p *thinkingProvider) Resume(context.Context, string) (runtime.Session, error) {
	p.resumeCalls++
	return runtime.Session{SessionID: "resumed"}, nil
}
func (p *thinkingProvider) ResumeWithOptions(_ context.Context, _ string, opts runtime.StartOptions) (runtime.Session, error) {
	p.resumeOptsCalls++
	p.lastResumeOption = opts
	return runtime.Session{SessionID: "resumed-options"}, nil
}
func (*thinkingProvider) Prompt(context.Context, string, string) error { return nil }
func (*thinkingProvider) Cancel(context.Context, string) error         { return nil }
func (*thinkingProvider) Events(context.Context, string) (<-chan events.Event, error) {
	return nil, nil
}
func (*thinkingProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}
func (*thinkingProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

type plainThinkingProvider struct{ resumeCalls int }

func (*plainThinkingProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	return runtime.Session{SessionID: "started"}, nil
}
func (p *plainThinkingProvider) Resume(context.Context, string) (runtime.Session, error) {
	p.resumeCalls++
	return runtime.Session{SessionID: "resumed"}, nil
}
func (*plainThinkingProvider) Prompt(context.Context, string, string) error { return nil }
func (*plainThinkingProvider) Cancel(context.Context, string) error         { return nil }
func (*plainThinkingProvider) Events(context.Context, string) (<-chan events.Event, error) {
	return nil, nil
}
func (*plainThinkingProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}
func (*plainThinkingProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

func TestStartSessionThinkingResumeDispatch(t *testing.T) {
	p := &thinkingProvider{}
	if _, err := StartSession(context.Background(), p, "codex-app-server", runtime.StartOptions{Thinking: "high"}, "session"); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if p.resumeOptsCalls != 1 || p.resumeCalls != 0 || p.lastResumeOption.Thinking != "high" {
		t.Fatalf("resume calls = plain:%d options:%d opts:%+v", p.resumeCalls, p.resumeOptsCalls, p.lastResumeOption)
	}

	p = &thinkingProvider{}
	if _, err := StartSession(context.Background(), p, "codex-app-server", runtime.StartOptions{}, "session"); err != nil {
		t.Fatalf("empty StartSession: %v", err)
	}
	if p.resumeCalls != 1 || p.resumeOptsCalls != 0 {
		t.Fatalf("empty resume calls = plain:%d options:%d", p.resumeCalls, p.resumeOptsCalls)
	}
}

func TestStartSessionRejectsExplicitResumeWithoutNativePath(t *testing.T) {
	for _, backend := range []string{"claude", "claude-channel"} {
		for _, level := range []string{"low", "medium", "high", "xhigh", "max"} {
			p := &plainThinkingProvider{}
			_, err := StartSession(context.Background(), p, backend, runtime.StartOptions{Thinking: level}, "session")
			if err == nil || !strings.Contains(err.Error(), backend) || !strings.Contains(err.Error(), "only when starting a session") || !strings.Contains(err.Error(), `value "`+level+`"`) {
				t.Fatalf("%s/%s error = %v", backend, level, err)
			}
			if p.resumeCalls != 0 {
				t.Fatalf("%s/%s called Resume", backend, level)
			}
		}
	}
}

func TestResumeSessionUsesCentralThinkingDispatch(t *testing.T) {
	p := &thinkingProvider{}
	if _, err := resumeSession(context.Background(), p, "pi", runtime.StartOptions{Thinking: "max"}, "session"); err != nil {
		t.Fatalf("resumeSession: %v", err)
	}
	if p.resumeOptsCalls != 1 || p.lastResumeOption.Thinking != "max" {
		t.Fatalf("resume options = %+v calls=%d", p.lastResumeOption, p.resumeOptsCalls)
	}
}

func TestDirectCLIThinkingPropagation(t *testing.T) {
	oldRunAttempt := runAttempt
	oldRetryAfter := retryAfter
	t.Cleanup(func() { runAttempt, retryAfter = oldRunAttempt, oldRetryAfter })
	retryAfter = func(time.Duration) <-chan time.Time { return make(chan time.Time) }

	for _, mode := range []string{"normal", "loop", "team"} {
		t.Run(mode, func(t *testing.T) {
			var captured []runtime.StartOptions
			runAttempt = func(_ context.Context, cfg attemptConfig, _ attemptDeps) attemptResult {
				captured = append(captured, cfg.startOptions)
				return attemptResult{exitCode: 0, sessionID: "session", stopReason: "end_turn"}
			}
			args := []string{"--backend", "pi", "--thinking", "xhigh", "--dir", t.TempDir()}
			switch mode {
			case "normal":
				args = append(args, "--prompt", "work")
			case "loop":
				path := filepath.Join(t.TempDir(), "loop.json")
				if err := os.WriteFile(path, []byte(`{"max_iterations":1,"pre":[{"name":"work","prompt":"work"}]}`), 0o600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--loop-file", path)
			case "team":
				path := filepath.Join(t.TempDir(), "team.json")
				if err := os.WriteFile(path, []byte(`{"team":[{"name":"work","prompt":"work"}]}`), 0o600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--team-file", path)
			}
			var stderr strings.Builder
			if code := run(args, func(string) string { return "" }, &stderr); code != 0 {
				t.Fatalf("run = %d, stderr=%s", code, stderr.String())
			}
			if len(captured) == 0 {
				t.Fatal("no attempt captured")
			}
			for _, opts := range captured {
				if opts.Thinking != "xhigh" {
					t.Fatalf("thinking = %q", opts.Thinking)
				}
			}
		})
	}
}

func TestDirectCLIRejectsUnsupportedThinkingBeforeAttempt(t *testing.T) {
	oldRunAttempt := runAttempt
	t.Cleanup(func() { runAttempt = oldRunAttempt })
	called := false
	runAttempt = func(context.Context, attemptConfig, attemptDeps) attemptResult {
		called = true
		return attemptResult{}
	}
	var stderr strings.Builder
	if code := run([]string{"--backend", "agy", "--thinking", "low", "--prompt", "work"}, func(string) string { return "" }, &stderr); code != 1 {
		t.Fatalf("run = %d", code)
	}
	if called || !strings.Contains(stderr.String(), "agy") || !strings.Contains(stderr.String(), "thinking") {
		t.Fatalf("called=%v stderr=%q", called, stderr.String())
	}
}
