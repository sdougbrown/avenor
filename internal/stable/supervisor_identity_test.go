package stable

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// provenanceRecorder collects the launch provenance of every start and resume
// dispatch the supervisor makes.
type provenanceRecorder struct {
	mu       sync.Mutex
	launches []runtime.LaunchProvenance
}

func (r *provenanceRecorder) add(opts runtime.StartOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var got runtime.LaunchProvenance
	if opts.Provenance != nil {
		got = *opts.Provenance
	}
	r.launches = append(r.launches, got)
}

func (r *provenanceRecorder) all() []runtime.LaunchProvenance {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtime.LaunchProvenance(nil), r.launches...)
}

// wrap records each dispatch built from inner's factory.
func (r *provenanceRecorder) wrap(inner func(runtime.StartOptions, string) (runtime.Provider, error)) func(runtime.StartOptions, string) (runtime.Provider, error) {
	return func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		p, err := inner(opts, backend)
		if err != nil {
			return nil, err
		}
		return &recordingProvider{Provider: p, rec: r}, nil
	}
}

// recordingProvider records each dispatch and delegates to an embedded
// provider.
type recordingProvider struct {
	runtime.Provider
	rec *provenanceRecorder
}

func (p *recordingProvider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.rec.add(opts)
	return p.Provider.Start(ctx, opts)
}

func (p *recordingProvider) ResumeWithOptions(ctx context.Context, sessionID string, opts runtime.StartOptions) (runtime.Session, error) {
	p.rec.add(opts)
	resumer, ok := p.Provider.(interface {
		ResumeWithOptions(context.Context, string, runtime.StartOptions) (runtime.Session, error)
	})
	if !ok {
		return p.Provider.Resume(ctx, sessionID)
	}
	return resumer.ResumeWithOptions(ctx, sessionID, opts)
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

// TestLaunchProvenancePerPath pins the identity each supervisor path hands to
// its launches: the child runtime ID is both the launch and the containing
// execution, never the supervisor run ID, and each path reports its own kind,
// phase, iteration and attempt.
func TestLaunchProvenancePerPath(t *testing.T) {
	t.Parallel()
	iteration := func(v int) *int { return &v }
	perConstruction := func() func(runtime.StartOptions, string) (runtime.Provider, error) {
		return perConstructionProvider(okConstruction)
	}
	team := func(t *testing.T, h *itestHarness) string {
		file := itestWorkflowConfig(t, "team", map[string]any{"team": []any{
			map[string]any{"name": "alpha", "prompt": "a"},
			map[string]any{"name": "beta", "prompt": "b"},
		}})
		return h.spawnWorkflow("team", file, map[string]any{"label": "provenance"})
	}
	loop := func(t *testing.T, h *itestHarness) string {
		file := itestWorkflowConfig(t, "loop", map[string]any{
			"max_iterations": 1,
			"pre":            []any{map[string]any{"name": "setup", "prompt": "pre"}},
			"loop":           []any{map[string]any{"name": "body", "prompt": "body"}},
			"post":           []any{map[string]any{"name": "report", "prompt": "post"}},
		})
		return h.spawnWorkflow("loop", file, map[string]any{"label": "provenance"})
	}

	tests := []struct {
		name    string
		factory func() func(runtime.StartOptions, string) (runtime.Provider, error)
		spawn   func(t *testing.T, h *itestHarness) string
		want    []runtime.LaunchProvenance
	}{
		{
			name:    "direct spawn",
			factory: perConstruction,
			spawn: func(t *testing.T, h *itestHarness) string {
				rt := h.spawn(map[string]any{"label": "provenance"})
				h.log.waitFor(t, "session.end", itestStopReasonIs(rt, "end_turn"))
				return rt
			},
			want: []runtime.LaunchProvenance{{Kind: "run", Label: "provenance", Attempt: 1}},
		},
		{
			name: "retry advances the attempt",
			factory: func() func(runtime.StartOptions, string) (runtime.Provider, error) {
				return itestSingleProvider(newItestProvider(
					itestAttempt("ses_try1", itestSessionEnd("ses_try1", "error", "")),
					itestAttempt("ses_try2", itestSessionEnd("ses_try2", "end_turn", "recovered")),
				))
			},
			spawn: func(t *testing.T, h *itestHarness) string {
				rt := h.spawn(map[string]any{"label": "provenance", "max_retries": 1})
				h.log.waitFor(t, "session.end", itestStopReasonIs(rt, "end_turn"))
				return rt
			},
			want: []runtime.LaunchProvenance{
				{Kind: "run", Label: "provenance", Attempt: 1},
				{Kind: "run", Label: "provenance", Attempt: 2},
			},
		},
		{
			name:    "team member",
			factory: perConstruction,
			spawn: func(t *testing.T, h *itestHarness) string {
				rt := team(t, h)
				h.requireWorkflowEnd(rt, "done", "end_turn", 0)
				return rt
			},
			want: []runtime.LaunchProvenance{
				{Kind: "team", Phase: "alpha", Label: "provenance", Attempt: 1},
				{Kind: "team", Phase: "beta", Label: "provenance", Attempt: 1},
			},
		},
		{
			name:    "loop phase",
			factory: perConstruction,
			spawn: func(t *testing.T, h *itestHarness) string {
				rt := loop(t, h)
				h.requireWorkflowEnd(rt, "done", "end_turn", 0)
				return rt
			},
			want: []runtime.LaunchProvenance{
				{Kind: "loop", Phase: "setup", Iteration: iteration(0), Label: "provenance", Attempt: 1},
				{Kind: "loop", Phase: "body", Iteration: iteration(1), Label: "provenance", Attempt: 1},
				{Kind: "loop", Phase: "report", Iteration: iteration(0), Label: "provenance", Attempt: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &provenanceRecorder{}
			h := newItestHarness(t, Config{}, rec.wrap(tt.factory()))
			rt := tt.spawn(t, h)

			launches := rec.all()
			if len(launches) != len(tt.want) {
				t.Fatalf("launches = %d, want %d (%+v)", len(launches), len(tt.want), launches)
			}
			// Team members may complete in either order; compare by phase.
			sort.SliceStable(launches, func(i, j int) bool { return launches[i].Phase < launches[j].Phase })
			sort.SliceStable(tt.want, func(i, j int) bool { return tt.want[i].Phase < tt.want[j].Phase })
			for i, want := range tt.want {
				got := launches[i]
				if got.RuntimeID == h.sup.runID {
					t.Fatalf("launch %d carries the supervisor run ID %q, want the child runtime ID", i, h.sup.runID)
				}
				if got.RuntimeID != rt || got.ExecutionID != rt {
					t.Fatalf("launch %d identity = %q/%q, want the child runtime %q", i, got.RuntimeID, got.ExecutionID, rt)
				}
				if (got.Iteration == nil) != (want.Iteration == nil) {
					t.Fatalf("launch %d iteration = %v, want %v", i, got.Iteration, want.Iteration)
				}
				if got.Iteration != nil && *got.Iteration != *want.Iteration {
					t.Fatalf("launch %d iteration = %d, want %d", i, *got.Iteration, *want.Iteration)
				}
				gotCopy, wantCopy := got, want
				gotCopy.Iteration, wantCopy.Iteration = nil, nil
				gotCopy.RuntimeID, gotCopy.ExecutionID = "", ""
				if gotCopy != wantCopy {
					t.Fatalf("launch %d = %+v, want %+v", i, gotCopy, wantCopy)
				}
			}
		})
	}
}
