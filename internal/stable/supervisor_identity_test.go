package stable

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// identityRecordingProvider records the launch options each start/resume
// dispatch receives and delegates the session mechanics to another provider.
type identityRecordingProvider struct {
	inner runtime.Provider

	mu            sync.Mutex
	starts        []runtime.StartOptions
	resumes       []runtime.StartOptions
	plainResumes  int
	resumeCallIDs []string
}

func (p *identityRecordingProvider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	p.starts = append(p.starts, snapshotOptions(opts))
	p.mu.Unlock()
	return p.inner.Start(ctx, opts)
}

func (p *identityRecordingProvider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	p.mu.Lock()
	p.plainResumes++
	p.mu.Unlock()
	return p.inner.Resume(ctx, sessionID)
}

func (p *identityRecordingProvider) ResumeWithOptions(ctx context.Context, sessionID string, opts runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	p.resumes = append(p.resumes, snapshotOptions(opts))
	p.resumeCallIDs = append(p.resumeCallIDs, sessionID)
	p.mu.Unlock()
	return p.inner.Resume(ctx, sessionID)
}

func (p *identityRecordingProvider) Prompt(ctx context.Context, sessionID, text string) error {
	return p.inner.Prompt(ctx, sessionID, text)
}

func (p *identityRecordingProvider) Cancel(ctx context.Context, sessionID string) error {
	return p.inner.Cancel(ctx, sessionID)
}

func (p *identityRecordingProvider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	return p.inner.Events(ctx, sessionID)
}

func (p *identityRecordingProvider) AnswerPermission(ctx context.Context, sessionID, requestID string, resp runtime.PermissionResponse) error {
	return p.inner.AnswerPermission(ctx, sessionID, requestID, resp)
}

func (p *identityRecordingProvider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return p.inner.Capabilities(ctx)
}

func (p *identityRecordingProvider) snapshot() (starts, resumes []runtime.StartOptions, plainResumes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]runtime.StartOptions(nil), p.starts...), append([]runtime.StartOptions(nil), p.resumes...), p.plainResumes
}

// snapshotOptions copies the launch options so later mutation cannot reach the
// recorded value.
func snapshotOptions(opts runtime.StartOptions) runtime.StartOptions {
	if opts.Provenance == nil {
		return runtime.StartOptions{Agent: opts.Agent, Thinking: opts.Thinking}
	}
	provenance := *opts.Provenance
	if opts.Provenance.Iteration != nil {
		iteration := *opts.Provenance.Iteration
		provenance.Iteration = &iteration
	}
	opts.Provenance = &provenance
	return opts
}

// launchIdentityLog records the provenance the supervisor hands to every
// provider construction.
type launchIdentityLog struct {
	mu       sync.Mutex
	launches []runtime.LaunchProvenance
	pairs    [][2]string
}

func (l *launchIdentityLog) add(agent string, provenance *runtime.LaunchProvenance) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if provenance == nil {
		l.launches = append(l.launches, runtime.LaunchProvenance{})
		return
	}
	copied := *provenance
	if provenance.Iteration != nil {
		iteration := *provenance.Iteration
		copied.Iteration = &iteration
	}
	l.launches = append(l.launches, copied)
	l.pairs = append(l.pairs, [2]string{agent, copied.Phase})
}

func (l *launchIdentityLog) all() []runtime.LaunchProvenance {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]runtime.LaunchProvenance(nil), l.launches...)
}

func (l *launchIdentityLog) agentPhases() [][2]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([][2]string(nil), l.pairs...)
}

// captureLaunchIdentities wraps a provider factory, recording the launch
// provenance of every construction.
func captureLaunchIdentities(log *launchIdentityLog, inner func(runtime.StartOptions, string) (runtime.Provider, error)) func(runtime.StartOptions, string) (runtime.Provider, error) {
	return func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		log.add(opts.Agent, opts.Provenance)
		return inner(opts, backend)
	}
}

// perConstructionProvider returns a provider with one scripted attempt per
// factory call, which suits loop phases that construct a provider for each
// retry, nudge or iteration. Every construction gets a unique session id.
func perConstructionProvider(script func(n int) stableScriptedAttempt) func(runtime.StartOptions, string) (runtime.Provider, error) {
	var count atomic.Int64
	return func(runtime.StartOptions, string) (runtime.Provider, error) {
		n := int(count.Add(1))
		return newItestProvider(script(n)), nil
	}
}

// okConstruction is the successful single-attempt script keyed by construction
// ordinal.
func okConstruction(n int) stableScriptedAttempt {
	sessionID := fmt.Sprintf("ses_constructed_%d", n)
	return itestAttempt(sessionID, itestSessionEnd(sessionID, "end_turn", "ok"))
}

// provenanceOf fails the test instead of dereferencing a missing identity, so
// a regression reports the dispatch rather than panicking.
func provenanceOf(t *testing.T, opts runtime.StartOptions) runtime.LaunchProvenance {
	t.Helper()
	if opts.Provenance == nil {
		t.Fatalf("dispatch carried no launch provenance: %+v", opts)
	}
	return *opts.Provenance
}

func requireIdentity(t *testing.T, got runtime.LaunchProvenance, runtimeID, kind, phase, label string, iteration *int, attempt int) {
	t.Helper()
	if got.RuntimeID != runtimeID || got.ExecutionID != runtimeID {
		t.Fatalf("identity = %q/%q, want the containing runtime %q", got.RuntimeID, got.ExecutionID, runtimeID)
	}
	if got.Kind != kind || got.Phase != phase || got.Label != label || got.Attempt != attempt {
		t.Fatalf("provenance = %+v, want kind=%s phase=%q label=%q attempt=%d", got, kind, phase, label, attempt)
	}
	if iteration == nil {
		if got.Iteration != nil {
			t.Fatalf("iteration = %d, want it omitted", *got.Iteration)
		}
		return
	}
	if got.Iteration == nil || *got.Iteration != *iteration {
		t.Fatalf("iteration = %v, want %d", got.Iteration, *iteration)
	}
}

func TestSpawnCarriesDirectLaunchIdentity(t *testing.T) {
	log := &launchIdentityLog{}
	h := newItestHarness(t, Config{}, captureLaunchIdentities(log, perConstructionProvider(okConstruction)))
	rt := h.spawn(map[string]any{"label": "direct-identity"})
	h.log.waitFor(t, "session.end", itestStopReasonIs(rt, "end_turn"))

	launches := log.all()
	if len(launches) != 1 {
		t.Fatalf("launches = %d, want 1", len(launches))
	}
	requireIdentity(t, launches[0], rt, "run", "", "direct-identity", nil, 1)
}

func TestDirectRetryAdvancesLaunchAttempt(t *testing.T) {
	log := &launchIdentityLog{}
	provider := &identityRecordingProvider{inner: newItestProvider(
		itestAttempt("ses_try1", itestSessionEnd("ses_try1", "error", "")),
		itestAttempt("ses_try2", itestSessionEnd("ses_try2", "end_turn", "recovered")),
	)}
	h := newItestHarness(t, Config{}, captureLaunchIdentities(log, itestSingleProvider(provider)))
	rt := h.spawn(map[string]any{"label": "retry-identity", "max_retries": 1})
	h.log.waitFor(t, "session.end", itestStopReasonIs(rt, "end_turn"))

	starts, resumes, _ := provider.snapshot()
	if len(starts) != 1 || len(resumes) != 1 {
		t.Fatalf("dispatches = start:%d resume:%d, want 1/1", len(starts), len(resumes))
	}
	requireIdentity(t, provenanceOf(t, starts[0]), rt, "run", "", "retry-identity", nil, 1)
	requireIdentity(t, provenanceOf(t, resumes[0]), rt, "run", "", "retry-identity", nil, 2)
}

func TestTeamMembersCarryDistinctLaunchIdentity(t *testing.T) {
	log := &launchIdentityLog{}
	h := newItestHarness(t, Config{}, captureLaunchIdentities(log, perConstructionProvider(okConstruction)))
	file := itestWorkflowConfig(t, "team", map[string]any{
		"team": []any{
			map[string]any{"name": "alpha", "prompt": "a", "agent": "agent-a"},
			map[string]any{"name": "beta", "prompt": "b", "agent": "agent-b"},
			map[string]any{"name": "gamma", "prompt": "c", "agent": "agent-c"},
		},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-identity"})
	h.requireWorkflowEnd(rt, "done", "end_turn", 0)

	launches := log.all()
	if len(launches) != 3 {
		t.Fatalf("launches = %d, want one per member", len(launches))
	}
	seen := map[string]bool{}
	for _, got := range launches {
		requireIdentity(t, got, rt, "team", got.Phase, "team-identity", nil, 1)
		seen[got.Phase] = true
	}
	for _, phase := range []string{"alpha", "beta", "gamma"} {
		if !seen[phase] {
			t.Fatalf("member %q not attributed: %v", phase, seen)
		}
	}
	// Members are concurrent, so the factory must pair each member's own agent
	// with its own phase rather than reading shared metadata.
	agents := map[string]string{}
	for _, pair := range log.agentPhases() {
		agents[pair[0]] = pair[1]
	}
	if agents["agent-a"] != "alpha" || agents["agent-b"] != "beta" || agents["agent-c"] != "gamma" {
		t.Fatalf("agent/phase pairs = %v, want each member's own phase", agents)
	}
}

func TestLoopPhasesCarryLaunchIteration(t *testing.T) {
	log := &launchIdentityLog{}
	h := newItestHarness(t, Config{}, captureLaunchIdentities(log, perConstructionProvider(okConstruction)))
	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 2,
		"pre":            []any{map[string]any{"name": "setup", "prompt": "pre"}},
		"loop":           []any{map[string]any{"name": "body", "prompt": "body"}},
		"post":           []any{map[string]any{"name": "report", "prompt": "post"}},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-identity"})
	h.requireWorkflowEnd(rt, "done", "end_turn", 0)

	want := []struct {
		phase     string
		iteration int
	}{{"setup", 0}, {"body", 1}, {"body", 2}, {"report", 0}}
	launches := log.all()
	if len(launches) != len(want) {
		t.Fatalf("launches = %d, want %d", len(launches), len(want))
	}
	for i, expected := range want {
		iteration := expected.iteration
		requireIdentity(t, launches[i], rt, "loop", expected.phase, "loop-identity", &iteration, 1)
	}
}

func TestLoopRetryThenNudgeUsesOneLaunchOrdinal(t *testing.T) {
	log := &launchIdentityLog{}
	h := newItestHarness(t, Config{}, captureLaunchIdentities(log, perConstructionProvider(func(n int) stableScriptedAttempt {
		// The first construction fails retryably; every later attempt ends the
		// turn without producing the required file, so the phase retries and
		// then nudges.
		sessionID := fmt.Sprintf("ses_ordinal_%d", n)
		if n == 1 {
			return itestAttempt(sessionID, itestSessionEnd(sessionID, "error", ""))
		}
		return itestAttempt(sessionID, itestSessionEnd(sessionID, "end_turn", "ok"))
	})))
	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop": []any{map[string]any{
			"name":          "body",
			"prompt":        "body",
			"requires":      map[string]any{"files": []any{"NEVER_WRITTEN.md"}},
			"on_incomplete": map[string]any{"max_nudges": 1},
		}},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "ordinal-identity", "max_retries": 1})
	end := h.log.waitFor(t, "avenor.loop.end", itestEventIs("avenor.loop.end", rt))
	if reason, _ := end.Fields["exit_reason"].(string); reason != "phase_failure" {
		t.Fatalf("avenor.loop.end exit_reason = %q, want phase_failure", reason)
	}

	launches := log.all()
	if len(launches) != 3 {
		t.Fatalf("launches = %d, want retry plus nudge", len(launches))
	}
	iteration := 1
	for i, got := range launches {
		requireIdentity(t, got, rt, "loop", "body", "ordinal-identity", &iteration, i+1)
	}
}

// TestAttemptSessionDispatchKeepsContainingIdentity pins the resume dispatch on
// the supervisor's own attempt path: a fresh resume carries the current attempt
// context, and returning a still-live session launches nothing.
func TestAttemptSessionDispatchKeepsContainingIdentity(t *testing.T) {
	provider := &identityRecordingProvider{inner: newItestProvider(
		itestAttempt("ses_start", itestSessionEnd("ses_start", "end_turn", "ok")),
		itestAttempt("ses_resume", itestSessionEnd("ses_resume", "end_turn", "ok")),
	)}
	child := &childRuntime{
		id:               "rt_attempt",
		provider:         provider,
		backend:          "pony",
		effectiveBackend: "pony",
		effectiveAgent:   "agent",
		label:            "attempt-identity",
		dir:              t.TempDir(),
	}
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "identity-attempt"), MaxRuntimes: 1})

	if _, err := sup.attemptSession(context.Background(), child, "", 1); err != nil {
		t.Fatalf("attemptSession start: %v", err)
	}
	if _, err := sup.attemptSession(context.Background(), child, "ses_start", 2); err != nil {
		t.Fatalf("attemptSession resume: %v", err)
	}

	starts, resumes, plain := provider.snapshot()
	if len(starts) != 1 || len(resumes) != 1 || plain != 0 {
		t.Fatalf("dispatches = start:%d resume:%d plain:%d, want the option-aware resume", len(starts), len(resumes), plain)
	}
	requireIdentity(t, provenanceOf(t, starts[0]), child.id, "run", "", "attempt-identity", nil, 1)
	requireIdentity(t, provenanceOf(t, resumes[0]), child.id, "run", "", "attempt-identity", nil, 2)
	if resumes[0].Provenance.Phase != "" || resumes[0].Provenance.Iteration != nil {
		t.Fatalf("resume provenance = %+v, want the optional fields cleared", resumes[0].Provenance)
	}

	// A still-live session is returned without consuming a launch ordinal or
	// dispatching anything.
	child.session = runtime.Session{SessionID: "ses_live"}
	if _, err := sup.attemptSession(context.Background(), child, "", 7); err != nil {
		t.Fatalf("attemptSession live: %v", err)
	}
	starts, resumes, plain = provider.snapshot()
	if len(starts) != 1 || len(resumes) != 1 || plain != 0 {
		t.Fatalf("live return dispatched = start:%d resume:%d plain:%d, want none", len(starts), len(resumes), plain)
	}
}
