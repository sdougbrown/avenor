package poo

import (
	"context"
	"strings"
	"testing"
	"time"

	avruntime "github.com/sdougbrown/avenor/internal/runtime"
)

func reasoningFixture(t *testing.T, endpoint *fakeEndpoint) pooFixture {
	t.Helper()
	return newPooFixtureWith(t, endpoint, map[string]any{
		"models": map[string]any{
			"test-model": map[string]any{
				"endpoint": "fake", "model_id": "test-model", "context_limit": 200000,
				"reasoning":        map[string]any{"mode": "effort", "param": "reasoning_effort"},
				"reasoning_levels": []string{"off", "low", "medium", "high", "max"},
			},
		},
	})
}

func TestThinkingReachesModelOnStartAndResume(t *testing.T) {
	endpoint := newFakeEndpoint(t, scriptedTurn{Text: "OK"})
	opts := reasoningFixture(t, endpoint).startOptions(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	promptAt := func(p *Provider, id, want string) {
		t.Helper()
		before := len(endpoint.requestBodies())
		if err := p.Prompt(ctx, id, "Reply OK."); err != nil {
			t.Fatal(err)
		}
		requests := endpoint.requestBodies()[before:]
		if len(requests) != 1 {
			t.Fatalf("got %d model requests, want one with reasoning_effort %q", len(requests), want)
		}
		if got := requests[0]["reasoning_effort"]; got != want {
			t.Fatalf("reasoning_effort = %v, want %q", got, want)
		}
	}

	initial := opts
	initial.Thinking = "high"
	p, session, _, _ := startProvider(t, initial)
	id := session.SessionID
	promptAt(p, id, "high")

	if _, err := p.ResumeWithOptions(ctx, id, avruntime.StartOptions{Thinking: "medium"}); err != nil {
		t.Fatal(err)
	}
	promptAt(p, id, "medium")
	if _, err := p.ResumeWithOptions(ctx, id, avruntime.StartOptions{Thinking: "xhigh"}); err == nil || !strings.Contains(err.Error(), `"xhigh" is not a reasoning level for test-model`) {
		t.Fatalf("warm resume error = %v, want Poo's model-specific refusal", err)
	}
	if _, err := p.ResumeWithOptions(ctx, id, avruntime.StartOptions{}); err != nil {
		t.Fatal(err)
	}
	promptAt(p, id, "medium")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p = NewWithOptions(opts)
	defer p.Close()
	if _, err := p.ResumeWithOptions(ctx, id, avruntime.StartOptions{Thinking: "xhigh"}); err == nil || !strings.Contains(err.Error(), `"xhigh" is not a reasoning level for test-model`) {
		t.Fatalf("cold resume error = %v, want Poo's model-specific refusal", err)
	}
	if _, err := p.ResumeWithOptions(ctx, id, avruntime.StartOptions{Thinking: "max"}); err != nil {
		t.Fatal(err)
	}
	promptAt(p, id, "max")
}

func TestThinkingRefusalDoesNotLeaveOpenSession(t *testing.T) {
	endpoint := newFakeEndpoint(t, scriptedTurn{Text: "OK"})
	opts := reasoningFixture(t, endpoint).startOptions(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := NewWithOptions(opts)
	defer p.Close()
	_, err := p.Start(ctx, avruntime.StartOptions{Thinking: "xhigh"})
	if err == nil || !strings.Contains(err.Error(), `"xhigh" is not a reasoning level for test-model`) {
		t.Fatalf("start error = %v, want Poo's model-specific refusal", err)
	}
	session, err := p.Start(ctx, avruntime.StartOptions{})
	if err != nil {
		t.Fatalf("start without override after refusal: %v", err)
	}
	if err := p.Prompt(ctx, session.SessionID, "Reply OK."); err != nil {
		t.Fatal(err)
	}
	requests := endpoint.requestBodies()
	if len(requests) != 1 {
		t.Fatalf("got %d model requests, want one", len(requests))
	}
	if got, present := requests[0]["reasoning_effort"]; present {
		t.Fatalf("reasoning_effort = %v, want no override", got)
	}
}

func TestThinkingUnchangedResumeWhileTurnIsActive(t *testing.T) {
	endpoint := newFakeEndpoint(t)
	requested := make(chan struct{})
	release := make(chan struct{})
	endpoint.respond = func(map[string]any) (scriptedTurn, bool) {
		close(requested)
		<-release
		return scriptedTurn{Text: "OK"}, true
	}
	opts := reasoningFixture(t, endpoint).startOptions(t)
	opts.Thinking = "high"
	p, session, _, _ := startProvider(t, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer close(release)
	done := make(chan error, 1)
	go func() { done <- p.Prompt(ctx, session.SessionID, "Reply OK.") }()
	select {
	case <-requested:
	case err := <-done:
		t.Fatalf("prompt ended before sending a request: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := p.ResumeWithOptions(ctx, session.SessionID, avruntime.StartOptions{Thinking: "medium"}); err == nil || !strings.Contains(err.Error(), "session/configure: busy") {
		t.Fatalf("changing the level during an active turn = %v, want Poo's busy refusal", err)
	}
	if _, err := p.ResumeWithOptions(ctx, session.SessionID, avruntime.StartOptions{Thinking: "high"}); err != nil {
		t.Fatalf("resume at the current level while the model is answering: %v", err)
	}
}

func TestThinkingRefusesASetterThatIgnoresTheLevel(t *testing.T) {
	t.Setenv(helperEnv, "ignore-thinking")
	p := NewWithOptions(helperStartOptions())
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := p.Start(ctx, avruntime.StartOptions{Thinking: "high"})
	if err == nil || !strings.Contains(err.Error(), `returned reasoning "", requested "high"`) {
		t.Fatalf("start error = %v, want refusal when Poo does not apply thinking", err)
	}
}
