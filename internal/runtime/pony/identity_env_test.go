package pony

import (
	"context"
	"os"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/pony/model"
)

// TestIdentityOptionsLeaveParentEnvAlone pins the in-process backend's
// documented limitation: it has no harness child to hand identity to, so a
// launch with provenance relays a prompt normally without touching this
// process's environment.
func TestIdentityOptionsLeaveParentEnvAlone(t *testing.T) {
	cleanGlobalSessions(t)
	ctx := context.Background()

	t.Setenv(runtime.EnvRuntimeID, "parent-runtime")
	t.Setenv(runtime.EnvPhase, "parent-phase")
	p := New(Config{
		Model:     "test-model",
		Adapter:   &fakeAdapter{chunks: []model.Chunk{textChunk("ack"), finishChunk("stop")}},
		MaxTokens: 100,
	})

	session, err := p.Start(ctx, runtime.StartOptions{
		Provenance: &runtime.LaunchProvenance{
			RuntimeID:   "rt_child",
			ExecutionID: "rt_child",
			Kind:        "team",
			Phase:       "reviewer",
			Attempt:     1,
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Prompt(ctx, session.SessionID, "hello"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	if got := os.Getenv(runtime.EnvRuntimeID); got != "parent-runtime" {
		t.Fatalf("%s = %q, want the parent value untouched", runtime.EnvRuntimeID, got)
	}
	if got := os.Getenv(runtime.EnvPhase); got != "parent-phase" {
		t.Fatalf("%s = %q, want the parent value untouched", runtime.EnvPhase, got)
	}
}
