package cli

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// provenanceRecorder captures the launch provenance of every CLI attempt.
type provenanceRecorder struct {
	mu       sync.Mutex
	launches []runtime.LaunchProvenance
}

func (r *provenanceRecorder) record(cfg attemptConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var got runtime.LaunchProvenance
	if cfg.startOptions.Provenance != nil {
		got = *cfg.startOptions.Provenance
	}
	r.launches = append(r.launches, got)
}

func (r *provenanceRecorder) all() []runtime.LaunchProvenance {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtime.LaunchProvenance(nil), r.launches...)
}

// recordProvenance installs a runAttempt stub that records each launch and
// answers with respond.
func recordProvenance(t *testing.T, respond func(launch int) attemptResult) *provenanceRecorder {
	t.Helper()
	recorder := &provenanceRecorder{}
	oldRunAttempt := runAttempt
	oldRetryAfter := retryAfter
	t.Cleanup(func() { runAttempt, retryAfter = oldRunAttempt, oldRetryAfter })
	retryAfter = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	runAttempt = func(_ context.Context, cfg attemptConfig, _ attemptDeps) attemptResult {
		recorder.record(cfg)
		return respond(len(recorder.all()) - 1)
	}
	return recorder
}

func writeCLIConfig(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func runCLI(t *testing.T, args ...string) {
	t.Helper()
	var stderr strings.Builder
	if code := run(args, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run(%v) = %d, stderr=%s", args, code, stderr.String())
	}
}

// TestLaunchProvenancePerPath pins the identity each CLI path hands to its
// launch: the CLI run ID is both the launch and the containing execution, and
// each path reports its own kind, phase, iteration and attempt.
func TestLaunchProvenancePerPath(t *testing.T) {
	iteration := func(v int) *int { return &v }
	success := func(int) attemptResult {
		return attemptResult{exitCode: 0, sessionID: "ses_ok", stopReason: "end_turn"}
	}
	launch := func(kind, phase string, iter *int, attempt int) runtime.LaunchProvenance {
		return runtime.LaunchProvenance{
			RuntimeID: "run_trim", ExecutionID: "run_trim", Kind: kind, Phase: phase,
			Iteration: iter, Label: "trim-label", Attempt: attempt,
		}
	}

	tests := []struct {
		name     string
		teamFile string
		loopFile string
		maxRetry int
		respond  func(launch int) attemptResult
		want     []runtime.LaunchProvenance
	}{
		{
			name: "direct run",
			want: []runtime.LaunchProvenance{launch("run", "", nil, 1)},
		},
		{
			name:     "direct retry advances the attempt",
			maxRetry: 1,
			respond: func(launch int) attemptResult {
				if launch == 0 {
					return attemptResult{exitCode: 1, sessionID: "ses_1"}
				}
				return attemptResult{exitCode: 0, sessionID: "ses_2", stopReason: "end_turn"}
			},
			want: []runtime.LaunchProvenance{launch("run", "", nil, 1), launch("run", "", nil, 2)},
		},
		{
			name:     "team member",
			teamFile: `{"team":[{"name":"alpha","prompt":"a"},{"name":"beta","prompt":"b"}]}`,
			want:     []runtime.LaunchProvenance{launch("team", "alpha", nil, 1), launch("team", "beta", nil, 1)},
		},
		{
			name:     "loop pre, body and post",
			loopFile: `{"max_iterations":1,"pre":[{"name":"setup","prompt":"pre"}],"loop":[{"name":"body","prompt":"body"}],"post":[{"name":"report","prompt":"post"}]}`,
			want: []runtime.LaunchProvenance{
				launch("loop", "setup", iteration(0), 1),
				launch("loop", "body", iteration(1), 1),
				launch("loop", "report", iteration(0), 1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			respond := tt.respond
			if respond == nil {
				respond = success
			}
			recorder := recordProvenance(t, respond)

			dir := t.TempDir()
			args := []string{"--backend", "pi", "--dir", dir, "--run-id", "run_trim", "--label", "trim-label"}
			switch {
			case tt.teamFile != "":
				args = append(args, "--team-file", writeCLIConfig(t, "team.json", tt.teamFile))
			case tt.loopFile != "":
				args = append(args, "--loop-file", writeCLIConfig(t, "loop.json", tt.loopFile))
			default:
				args = append(args, "--prompt", "work")
			}
			if tt.maxRetry > 0 {
				args = append(args, "--max-retries", strconv.Itoa(tt.maxRetry))
			}
			runCLI(t, args...)

			launches := recorder.all()
			if len(launches) != len(tt.want) {
				t.Fatalf("launches = %d, want %d (%+v)", len(launches), len(tt.want), launches)
			}
			// Team members may complete in either order; compare by phase.
			sortRecords(launches)
			sortRecords(tt.want)
			for i, want := range tt.want {
				got := launches[i]
				if got.ExecutionID != "run_trim" || got.RuntimeID != "run_trim" {
					t.Fatalf("launch %d identity = %q/%q, want the CLI run id", i, got.RuntimeID, got.ExecutionID)
				}
				assertLaunchProvenance(t, got, want)
			}
		})
	}
}

func sortRecords(records []runtime.LaunchProvenance) {
	sort.SliceStable(records, func(i, j int) bool { return records[i].Phase < records[j].Phase })
}

func assertLaunchProvenance(t *testing.T, got, want runtime.LaunchProvenance) {
	t.Helper()
	if (got.Iteration == nil) != (want.Iteration == nil) {
		t.Fatalf("iteration = %v, want %v", got.Iteration, want.Iteration)
	}
	if got.Iteration != nil && *got.Iteration != *want.Iteration {
		t.Fatalf("iteration = %d, want %d", *got.Iteration, *want.Iteration)
	}
	gotCopy, wantCopy := got, want
	gotCopy.Iteration, wantCopy.Iteration = nil, nil
	if gotCopy != wantCopy {
		t.Fatalf("provenance = %+v, want %+v", gotCopy, wantCopy)
	}
}
