package stable

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/teamrunner"
)

// hangingProvider is a continuously active backend: Prompt blocks until the
// caller's context is done and the event channel never delivers or closes, so
// only an enforced deadline can stop a waiter.
type hangingProvider struct{}

func (hangingProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	return runtime.Session{SessionID: "ses_hanging"}, nil
}

func (hangingProvider) Resume(_ context.Context, sessionID string) (runtime.Session, error) {
	return runtime.Session{SessionID: sessionID}, nil
}

func (hangingProvider) Prompt(ctx context.Context, _ string, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

func (hangingProvider) Cancel(context.Context, string) error { return nil }

func (hangingProvider) Events(context.Context, string) (<-chan events.Event, error) {
	return make(chan events.Event), nil
}

func (hangingProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}

func (hangingProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

// TestRunLoopChildTimeoutProducesTimeoutSentinel proves SpawnParams.Timeout is
// an enforceable deadline for stable loop children: a phase that never settles
// is stopped by the shared wall-clock deadline and the sentinel records the
// timeout classification.
func TestRunLoopChildTimeoutProducesTimeoutSentinel(t *testing.T) {
	t.Parallel()
	provider := hangingProvider{}
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/test-loop-timeout.sock", MaxRuntimes: 1})
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

	sentinel := filepath.Join(t.TempDir(), "sentinel.env")
	child := &childRuntime{id: "rt_loop_timeout", done: make(chan struct{}), promptCh: make(chan struct{}, 1), eventWriter: stableTestSink{}, sentinelFile: sentinel, cancelFn: func() {}}
	cfg := &looprunner.LoopConfig{MaxIterations: 1, Loop: []phaseconfig.Phase{{Name: "work", Prompt: "work"}}}
	registerSyntheticChild(t, sup, child, testLifecycleOptions{exec: execStarting, class: workflowAttemptClass})
	go sup.runLoopChild(context.Background(), child, cfg, 1, 0, "", "", "", "", "", "")

	select {
	case <-child.done:
	case <-time.After(10 * time.Second):
		t.Fatal("loop child did not stop at the run deadline")
	}

	raw, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	if !strings.Contains(string(raw), "TIMEOUT") || !strings.Contains(string(raw), "STOP_REASON=timeout") {
		t.Fatalf("sentinel = %q, want TIMEOUT with STOP_REASON=timeout", string(raw))
	}
}

// TestRunChildTimeoutProducesTimeoutSentinel proves SpawnParams.Timeout is an
// enforceable deadline for stable plain run children: a hanging provider is
// stopped by the shared wall-clock deadline and the sentinel records the
// timeout classification, including across retry backoffs.
func TestRunChildTimeoutProducesTimeoutSentinel(t *testing.T) {
	t.Parallel()
	provider := hangingProvider{}
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/test-run-timeout.sock", MaxRuntimes: 1})
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

	sentinel := filepath.Join(t.TempDir(), "sentinel.env")
	child := &childRuntime{id: "rt_run_timeout", done: make(chan struct{}), promptCh: make(chan struct{}, 1), eventWriter: stableTestSink{}, sentinelFile: sentinel, cancelFn: func() {}, provider: provider}
	registerSyntheticChild(t, sup, child, testLifecycleOptions{exec: execStarting, class: directAttemptClass})
	go sup.runChild(context.Background(), child, "work", 1, 0)

	select {
	case <-child.done:
	case <-time.After(10 * time.Second):
		t.Fatal("run child did not stop at the run deadline")
	}

	raw, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	if !strings.Contains(string(raw), "TIMEOUT") || !strings.Contains(string(raw), "STOP_REASON=timeout") {
		t.Fatalf("sentinel = %q, want TIMEOUT with STOP_REASON=timeout", string(raw))
	}
}

// TestRunTeamChildTimeoutProducesTimeoutSentinel proves SpawnParams.Timeout is
// an enforceable deadline for stable team children.
func TestRunTeamChildTimeoutProducesTimeoutSentinel(t *testing.T) {
	t.Parallel()
	provider := hangingProvider{}
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/test-team-timeout.sock", MaxRuntimes: 1})
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

	sentinel := filepath.Join(t.TempDir(), "sentinel.env")
	child := &childRuntime{id: "rt_team_timeout", done: make(chan struct{}), promptCh: make(chan struct{}, 1), eventWriter: stableTestSink{}, sentinelFile: sentinel, cancelFn: func() {}}
	cfg := &teamrunner.TeamConfig{Team: []phaseconfig.Phase{{Name: "work", Prompt: "work"}}}
	registerSyntheticChild(t, sup, child, testLifecycleOptions{exec: execStarting, class: workflowAttemptClass})
	go sup.runTeamChild(context.Background(), child, cfg, 1, 0, "", "", "", "", "", "")

	select {
	case <-child.done:
	case <-time.After(10 * time.Second):
		t.Fatal("team child did not stop at the run deadline")
	}

	raw, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	if !strings.Contains(string(raw), "TIMEOUT") || !strings.Contains(string(raw), "STOP_REASON=timeout") {
		t.Fatalf("sentinel = %q, want TIMEOUT with STOP_REASON=timeout", string(raw))
	}
}
