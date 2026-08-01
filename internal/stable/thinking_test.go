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
	sup := NewSupervisor(Config{ControlSocket: "/tmp/thinking-validation.sock", MaxRuntimes: 1})
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
	var decoded SpawnParams
	if err := json.Unmarshal([]byte(`{"prompt":"work","dir":".","backend":"pi","thinking":"high"}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Thinking != "high" {
		t.Fatalf("decoded thinking = %q", decoded.Thinking)
	}

	for _, mode := range []string{"normal", "loop", "team"} {
		t.Run(mode, func(t *testing.T) {
			sup := NewSupervisor(Config{ControlSocket: "/tmp/thinking-" + mode + ".sock", MaxRuntimes: 1})
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
	provider := &thinkingCaptureProvider{}
	child := &childRuntime{provider: provider, backend: "pi", thinking: "max", agent: "agent", model: "model", dir: "/work"}
	sup := NewSupervisor(Config{ControlSocket: "/tmp/thinking-resume.sock", MaxRuntimes: 1})
	if _, err := sup.attemptSession(context.Background(), child, "session"); err != nil {
		t.Fatalf("attemptSession: %v", err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.opts) != 1 || provider.opts[0].Thinking != "max" {
		t.Fatalf("resume options = %+v", provider.opts)
	}
}
