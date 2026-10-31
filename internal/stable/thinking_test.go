package stable

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type thinkingCaptureProvider struct {
	mu   sync.Mutex
	opts []runtime.StartOptions
}

func (p *thinkingCaptureProvider) Start(_ context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	p.opts = append(p.opts, opts)
	p.mu.Unlock()
	return runtime.Session{SessionID: "thinking-session"}, nil
}
func (p *thinkingCaptureProvider) Resume(context.Context, string) (runtime.Session, error) {
	return runtime.Session{SessionID: "thinking-session"}, nil
}
func (p *thinkingCaptureProvider) ResumeWithOptions(ctx context.Context, _ string, opts runtime.StartOptions) (runtime.Session, error) {
	return p.Start(ctx, opts)
}
func (*thinkingCaptureProvider) Prompt(context.Context, string, string) error { return nil }
func (*thinkingCaptureProvider) Cancel(context.Context, string) error         { return nil }
func (*thinkingCaptureProvider) Events(context.Context, string) (<-chan events.Event, error) {
	ch := make(chan events.Event, 1)
	ch <- events.Event{Event: "session.end", SessionID: "thinking-session", Fields: map[string]any{"stop_reason": "end_turn"}}
	close(ch)
	return ch, nil
}
func (*thinkingCaptureProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}
func (*thinkingCaptureProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

func TestSpawnThinkingValidationPrecedesReservation(t *testing.T) {
	t.Parallel()
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/thinking-validation.sock", MaxRuntimes: 1})
	providerCalled := false
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		providerCalled = true
		return &thinkingCaptureProvider{}, nil
	}
	_, err := sup.spawn(SpawnParams{Prompt: "work", Backend: "agy", Thinking: "low"})
	if err == nil || !strings.Contains(err.Error(), "agy") || !strings.Contains(err.Error(), "thinking") {
		t.Fatalf("error = %v", err)
	}
	if providerCalled || len(sup.runtimes) != 0 || sup.nextID != 0 {
		t.Fatalf("provider=%v runtimes=%d nextID=%d", providerCalled, len(sup.runtimes), sup.nextID)
	}
}

func TestSpawnThinkingRawJSONAndExecutionModes(t *testing.T) {
	t.Parallel()
	var decoded SpawnParams
	if err := json.Unmarshal([]byte(`{"prompt":"work","dir":".","backend":"pi","thinking":"high"}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Thinking != "high" {
		t.Fatalf("decoded thinking = %q", decoded.Thinking)
	}

	for _, mode := range []string{"normal", "loop", "team"} {
		t.Run(mode, func(t *testing.T) {
			sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/thinking-" + mode + ".sock", MaxRuntimes: 1})
			provider := &thinkingCaptureProvider{}
			constructorOpts := make(chan runtime.StartOptions, 4)
			sup.newProviderFunc = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
				if backend != "pi" {
					t.Errorf("backend = %q", backend)
				}
				constructorOpts <- opts
				return provider, nil
			}
			params := SpawnParams{Prompt: "work", Dir: t.TempDir(), Backend: "pi", Thinking: "high"}
			switch mode {
			case "loop":
				path := filepath.Join(t.TempDir(), "loop.json")
				if err := os.WriteFile(path, []byte(`{"max_iterations":1,"pre":[{"name":"work","prompt":"work"}]}`), 0o600); err != nil {
					t.Fatal(err)
				}
				params.Prompt, params.LoopFile = "", path
			case "team":
				path := filepath.Join(t.TempDir(), "team.json")
				if err := os.WriteFile(path, []byte(`{"team":[{"name":"work","prompt":"work"}]}`), 0o600); err != nil {
					t.Fatal(err)
				}
				params.Prompt, params.TeamFile = "", path
			}
			result, err := sup.spawn(params)
			if err != nil {
				t.Fatalf("spawn: %v", err)
			}
			select {
			case opts := <-constructorOpts:
				if opts.Thinking != "high" {
					t.Fatalf("constructor thinking = %q", opts.Thinking)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("provider construction was not observed")
			}
			sup.controlMu.Lock()
			child := sup.runtimes[result.RuntimeID]
			sup.controlMu.Unlock()
			if child == nil || child.thinking != "high" || child.backend != "pi" {
				t.Fatalf("child = %+v", child)
			}
			if mode == "normal" {
				child.cancelFn()
			}
		})
	}
}

func TestAttemptSessionRetainsThinkingOnResume(t *testing.T) {
	t.Parallel()
	provider := &thinkingCaptureProvider{}
	child := &childRuntime{provider: provider, backend: "pi", thinking: "max", agent: "agent", model: "model", dir: "/work"}
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/thinking-resume.sock", MaxRuntimes: 1})
	if _, err := sup.attemptSession(context.Background(), child, "session", 1); err != nil {
		t.Fatalf("attemptSession: %v", err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.opts) != 1 || provider.opts[0].Thinking != "max" {
		t.Fatalf("resume options = %+v", provider.opts)
	}
}

type plainResumeProvider struct {
	startOpts   []runtime.StartOptions
	resumeCalls int
}

func (p *plainResumeProvider) Start(_ context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.startOpts = append(p.startOpts, opts)
	return runtime.Session{SessionID: "plain-started"}, nil
}
func (p *plainResumeProvider) Resume(context.Context, string) (runtime.Session, error) {
	p.resumeCalls++
	return runtime.Session{SessionID: "plain-resumed"}, nil
}
func (*plainResumeProvider) Prompt(context.Context, string, string) error { return nil }
func (*plainResumeProvider) Cancel(context.Context, string) error         { return nil }
func (*plainResumeProvider) Events(context.Context, string) (<-chan events.Event, error) {
	return nil, nil
}
func (*plainResumeProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}
func (*plainResumeProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

func TestAttemptSessionDropsStartOnlyOptionsOnClaudeResume(t *testing.T) {
	t.Parallel()
	provider := &plainResumeProvider{}
	child := &childRuntime{provider: provider, backend: "claude", thinking: "max", agent: "agent", model: "model", dir: "/work"}
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/thinking-resume-claude.sock", MaxRuntimes: 1})
	if _, err := sup.attemptSession(context.Background(), child, "session", 1); err != nil {
		t.Fatalf("attemptSession: %v", err)
	}
	if provider.resumeCalls != 1 {
		t.Fatalf("resume calls = %d, want 1", provider.resumeCalls)
	}
}

func TestAttemptSessionRetainsStartOnlyOptionsOnFreshStart(t *testing.T) {
	t.Parallel()
	provider := &plainResumeProvider{}
	child := &childRuntime{provider: provider, backend: "claude", thinking: "max", agent: "agent", model: "model", dir: "/work"}
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/thinking-fresh-claude.sock", MaxRuntimes: 1})
	if _, err := sup.attemptSession(context.Background(), child, "", 1); err != nil {
		t.Fatalf("attemptSession: %v", err)
	}
	if len(provider.startOpts) != 1 || provider.startOpts[0].Thinking != "max" {
		t.Fatalf("fresh start options = %+v, want thinking retained", provider.startOpts)
	}
}

// An omitted backend falls back to the runtime default. When that default
// cannot apply the caller's thinking level, the error must say the backend was
// never named, because the default is not something the caller chose.
func TestSpawnNamesTheOmittedBackendWhenThinkingIsRejected(t *testing.T) {
	t.Parallel()
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/thinking-omitted-backend.sock", MaxRuntimes: 1})
	providerCalled := false
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		providerCalled = true
		return &thinkingCaptureProvider{}, nil
	}
	_, err := sup.spawn(SpawnParams{Prompt: "work", Thinking: "high"})
	if err == nil {
		t.Fatal("spawn with thinking and no backend: want an error naming the defaulted backend")
	}
	for _, want := range []string{"requires an explicit backend", "backend was omitted", `"opencode-acp"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to mention %q", err, want)
		}
	}
	if providerCalled || len(sup.runtimes) != 0 {
		t.Fatalf("provider=%v runtimes=%d", providerCalled, len(sup.runtimes))
	}
}

// The reported combination: a model and a thinking level on an explicitly
// supplied pi backend. Pi applies every canonical level, so this must start a
// runtime rather than reject the pair.
func TestSpawnAcceptsThinkingWithExplicitPiBackendAndModel(t *testing.T) {
	t.Parallel()
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/thinking-pi-model.sock", MaxRuntimes: 1})
	provider := &thinkingCaptureProvider{}
	constructorOpts := make(chan runtime.StartOptions, 1)
	sup.newProviderFunc = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		if backend != "pi" {
			t.Errorf("backend = %q, want pi", backend)
		}
		constructorOpts <- opts
		return provider, nil
	}
	result, err := sup.spawn(SpawnParams{Prompt: "work", Dir: t.TempDir(), Backend: "pi", Model: "m", Thinking: "high"})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	select {
	case opts := <-constructorOpts:
		if opts.Thinking != "high" || opts.Model != "m" {
			t.Fatalf("start options = %+v, want thinking high and model m", opts)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("provider construction was not observed")
	}
	sup.controlMu.Lock()
	child := sup.runtimes[result.RuntimeID]
	sup.controlMu.Unlock()
	if child == nil {
		t.Fatalf("runtime %q not registered", result.RuntimeID)
	}
	if child.thinking != "high" || child.model != "m" || child.backend != "pi" {
		t.Fatalf("child = thinking %q model %q backend %q", child.thinking, child.model, child.backend)
	}
	child.cancelFn()
}

// A resumed session supplies the backend, so its verdict is about the run
// the caller is continuing, not about a runtime default they never chose.
func TestSpawnKeepsTheResumedBackendInTheThinkingError(t *testing.T) {
	t.Parallel()
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/thinking-resumed-backend.sock", MaxRuntimes: 1})
	sup.sessionIdentityMu.Lock()
	if sup.sessionIdentities == nil {
		sup.sessionIdentities = map[string]sessionIdentityEntry{}
	}
	sup.sessionIdentities["ses_resumed"] = sessionIdentityEntry{identity: effectiveIdentity{Backend: "agy"}}
	sup.sessionIdentityMu.Unlock()
	providerCalled := false
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		providerCalled = true
		return &thinkingCaptureProvider{}, nil
	}
	_, err := sup.spawn(SpawnParams{Prompt: "work", SessionID: "ses_resumed", Thinking: "low"})
	if err == nil {
		t.Fatal("resume with thinking on a backend that rejects it: want an error")
	}
	if !strings.Contains(err.Error(), `backend "agy" does not support parameter "thinking"`) {
		t.Fatalf("error = %q, want the resumed backend's own verdict", err)
	}
	if strings.Contains(err.Error(), "backend was omitted") {
		t.Fatalf("error = %q, want no omission claim for a backend the session supplied", err)
	}
	if providerCalled || len(sup.runtimes) != 0 {
		t.Fatalf("provider=%v runtimes=%d", providerCalled, len(sup.runtimes))
	}
}
