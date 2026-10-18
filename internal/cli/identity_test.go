package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// launchRecord is the option record one CLI attempt hands to the provider
// layer: its launch provenance, the session it resumes, and its agent.
type launchRecord struct {
	provenance runtime.LaunchProvenance
	resumeID   string
	agent      string
}

// provenanceRecorder captures the record of every CLI attempt.
type provenanceRecorder struct {
	mu       sync.Mutex
	launches []launchRecord
}

func (r *provenanceRecorder) record(cfg attemptConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record := launchRecord{resumeID: cfg.resumeID, agent: cfg.startOptions.Agent}
	if cfg.startOptions.Provenance != nil {
		record.provenance = *cfg.startOptions.Provenance
	}
	r.launches = append(r.launches, record)
}

func (r *provenanceRecorder) all() []launchRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]launchRecord(nil), r.launches...)
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

func runCLI(t *testing.T, args ...string) string {
	t.Helper()
	var stderr strings.Builder
	if code := run(args, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run(%v) = %d, stderr=%s", args, code, stderr.String())
	}
	return stderr.String()
}

// runCLIWithCode runs the CLI and returns its exit code, for scenarios whose
// expected outcome is a terminal failure.
func runCLIWithCode(t *testing.T, args ...string) int {
	t.Helper()
	var stderr strings.Builder
	return run(args, func(string) string { return "" }, &stderr)
}

// sentinelRunID reports the CLI run ID the sentinel recorded.
func sentinelRunID(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if value, ok := strings.CutPrefix(line, "RUN="); ok {
			return value
		}
	}
	t.Fatalf("sentinel has no RUN line: %s", raw)
	return ""
}

func TestDirectRunLaunchProvenance(t *testing.T) {
	recorder := recordProvenance(t, func(int) attemptResult {
		return attemptResult{exitCode: 0, sessionID: "ses_direct", stopReason: "end_turn"}
	})
	sentinel := filepath.Join(t.TempDir(), "done")
	runCLI(t, "--backend", "pi", "--dir", t.TempDir(), "--prompt", "work", "--label", "direct-label", "--sentinel-file", sentinel)

	launches := recorder.all()
	if len(launches) != 1 {
		t.Fatalf("launches = %d, want 1", len(launches))
	}
	runID := sentinelRunID(t, sentinel)
	got := launches[0].provenance
	if got.RuntimeID != runID || got.ExecutionID != runID {
		t.Fatalf("identity = %q/%q, want the CLI run id %q", got.RuntimeID, got.ExecutionID, runID)
	}
	if got.Kind != "run" || got.Phase != "" || got.Iteration != nil || got.Label != "direct-label" || got.Attempt != 1 {
		t.Fatalf("provenance = %+v, want a first direct-run launch", got)
	}
}

func TestDirectRunRetryAdvancesAttempt(t *testing.T) {
	recorder := recordProvenance(t, func(launch int) attemptResult {
		if launch == 0 {
			return attemptResult{exitCode: 1, sessionID: "ses_1"}
		}
		return attemptResult{exitCode: 0, sessionID: "ses_2", stopReason: "end_turn"}
	})
	// The retry attempt reuses the CLI run ID and label, so the event log can
	// corroborate the launch record.
	eventsPath := filepath.Join(t.TempDir(), "events.ndjson")
	sentinel := filepath.Join(t.TempDir(), "done")
	runCLI(t, "--backend", "pi", "--dir", t.TempDir(), "--prompt", "work", "--max-retries", "1", "--label", "retry-label", "--on-event", eventsPath, "--sentinel-file", sentinel)

	launches := recorder.all()
	if len(launches) != 2 {
		t.Fatalf("launches = %d, want 2", len(launches))
	}
	runID := sentinelRunID(t, sentinel)
	for i, want := range []int{1, 2} {
		got := launches[i].provenance
		if got.Attempt != want || got.Kind != "run" || got.Phase != "" || got.Iteration != nil || got.Label != "retry-label" {
			t.Fatalf("launch %d provenance = %+v, want attempt %d of the same direct run", i, got, want)
		}
		if got.ExecutionID != runID {
			t.Fatalf("launch %d execution id = %q, want the CLI run id %q", i, got.ExecutionID, runID)
		}
	}
	if launches[0].provenance.ExecutionID != launches[1].provenance.ExecutionID {
		t.Fatalf("execution id changed across retries: %q vs %q", launches[0].provenance.ExecutionID, launches[1].provenance.ExecutionID)
	}

	retries := eventsNamed(readEventLogForTest(t, eventsPath), "avenor.retry")
	if len(retries) != 1 {
		t.Fatalf("retry events = %d, want 1", len(retries))
	}
	if got := joinedNumber(t, retries[0], "attempt"); got != 2 {
		t.Fatalf("retry attempt = %d, want the upcoming attempt 2", got)
	}
	if got := joinedString(t, retries[0], "run_id"); got != runID {
		t.Fatalf("retry run_id = %q, want the CLI run id %q", got, runID)
	}
	if got := joinedString(t, retries[0], "run_label"); got != "retry-label" {
		t.Fatalf("retry run_label = %q, want retry-label", got)
	}
}

func TestTeamMemberLaunchProvenance(t *testing.T) {
	recorder := recordProvenance(t, func(launch int) attemptResult {
		return attemptResult{exitCode: 0, sessionID: "ses_member", stopReason: "end_turn"}
	})
	config := writeCLIConfig(t, "team.json", `{"team":[{"name":"alpha","prompt":"a"},{"name":"beta","prompt":"b"}]}`)
	sentinel := filepath.Join(t.TempDir(), "done")
	runCLI(t, "--backend", "pi", "--dir", t.TempDir(), "--team-file", config, "--label", "team-label", "--sentinel-file", sentinel)

	launches := recorder.all()
	if len(launches) != 2 {
		t.Fatalf("launches = %d, want 2", len(launches))
	}
	runID := sentinelRunID(t, sentinel)
	seen := map[string]bool{}
	for _, launch := range launches {
		got := launch.provenance
		if got.Kind != "team" || got.Iteration != nil || got.Label != "team-label" || got.Attempt != 1 {
			t.Fatalf("member provenance = %+v, want a first team launch", got)
		}
		if got.Phase != "alpha" && got.Phase != "beta" {
			t.Fatalf("member phase = %q, want alpha or beta", got.Phase)
		}
		if got.ExecutionID != runID {
			t.Fatalf("member execution id = %q, want the CLI run id %q", got.ExecutionID, runID)
		}
		seen[got.Phase] = true
	}
	if !seen["alpha"] || !seen["beta"] {
		t.Fatalf("member phases = %v, want both members attributed", seen)
	}
}

func TestLoopPhaseLaunchProvenance(t *testing.T) {
	recorder := recordProvenance(t, func(launch int) attemptResult {
		return attemptResult{exitCode: 0, sessionID: "ses_phase", stopReason: "end_turn"}
	})
	config := writeCLIConfig(t, "loop.json", `{"max_iterations":2,"pre":[{"name":"setup","prompt":"pre"}],"loop":[{"name":"body","prompt":"body"}],"post":[{"name":"report","prompt":"post"}]}`)
	sentinel := filepath.Join(t.TempDir(), "done")
	runCLI(t, "--backend", "pi", "--dir", t.TempDir(), "--loop-file", config, "--label", "loop-label", "--sentinel-file", sentinel)

	launches := recorder.all()
	type launchKey struct {
		phase     string
		iteration int
	}
	want := []launchKey{{"setup", 0}, {"body", 1}, {"body", 2}, {"report", 0}}
	if len(launches) != len(want) {
		t.Fatalf("launches = %d, want %d (%+v)", len(launches), len(want), launches)
	}
	runID := sentinelRunID(t, sentinel)
	for i, expected := range want {
		got := launches[i].provenance
		if got.Kind != "loop" || got.Phase != expected.phase || got.Iteration == nil || *got.Iteration != expected.iteration {
			t.Fatalf("launch %d provenance = %+v, want loop/%s/%d", i, got, expected.phase, expected.iteration)
		}
		if got.Label != "loop-label" || got.Attempt != 1 {
			t.Fatalf("launch %d provenance = %+v, want the CLI label and first attempt", i, got)
		}
		if got.ExecutionID != runID {
			t.Fatalf("launch %d execution id = %q, want the CLI run id %q", i, got.ExecutionID, runID)
		}
	}
}

func TestNestedTeamInLoopSharesExecutionIDAndUsesInnerKind(t *testing.T) {
	recorder := recordProvenance(t, func(launch int) attemptResult {
		return attemptResult{exitCode: 0, sessionID: "ses_nested", stopReason: "end_turn"}
	})
	dir := t.TempDir()
	teamPath := filepath.Join(dir, "team.json")
	if err := os.WriteFile(teamPath, []byte(`{"team":[{"name":"inner-member","prompt":"m"}]}`), 0o600); err != nil {
		t.Fatalf("write team config: %v", err)
	}
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"max_iterations":1,"loop":[{"name":"inner","team_file":"team.json"}]}`), 0o600); err != nil {
		t.Fatalf("write loop config: %v", err)
	}
	sentinel := filepath.Join(t.TempDir(), "done")
	runCLI(t, "--backend", "pi", "--dir", dir, "--loop-file", loopPath, "--label", "nested-label", "--sentinel-file", sentinel)

	launches := recorder.all()
	if len(launches) != 1 {
		t.Fatalf("launches = %d, want 1", len(launches))
	}
	got := launches[0].provenance
	if got.Kind != "team" || got.Phase != "inner-member" || got.Iteration != nil {
		t.Fatalf("nested member provenance = %+v, want the inner team member", got)
	}
	if got.ExecutionID != sentinelRunID(t, sentinel) {
		t.Fatalf("nested execution id = %q, want the containing CLI run id", got.ExecutionID)
	}
	if got.Label != "nested-label" || got.Attempt != 1 {
		t.Fatalf("nested member provenance = %+v, want the CLI label and first attempt", got)
	}
}

func TestRetryThenNudgeLaunchProvenance(t *testing.T) {
	recorder := recordProvenance(t, func(launch int) attemptResult {
		if launch == 0 {
			return attemptResult{exitCode: 1, sessionID: "ses_phase"}
		}
		return attemptResult{exitCode: 0, sessionID: "ses_phase", stopReason: "end_turn"}
	})
	config := writeCLIConfig(t, "loop.json", `{"max_iterations":1,"loop":[{"name":"body","prompt":"body","requires":{"files":["NEVER_WRITTEN.md"]},"on_incomplete":{"max_nudges":1}}]}`)
	if code := runCLIWithCode(t, "--backend", "pi", "--dir", t.TempDir(), "--loop-file", config, "--max-retries", "1", "--label", "nudge-label"); code != 1 {
		t.Fatalf("run = %d, want the incomplete-output failure", code)
	}

	launches := recorder.all()
	if len(launches) != 3 {
		t.Fatalf("launches = %d, want retry plus nudge (%+v)", len(launches), launches)
	}
	for i, launch := range launches {
		got := launch.provenance
		if got.Attempt != i+1 {
			t.Fatalf("launch %d attempt = %d, want %d", i, got.Attempt, i+1)
		}
		if got.Kind != "loop" || got.Phase != "body" || got.Iteration == nil || *got.Iteration != 1 || got.Label != "nudge-label" {
			t.Fatalf("launch %d provenance = %+v, want the same phase invocation", i, got)
		}
	}
}

// TestLoopPhaseResumeCarriesLaunchIdentity pins the resume-from-previous path:
// a phase that resumes the previous phase's session keeps the containing
// invocation's identity and receives that session ID.
func TestLoopPhaseResumeCarriesLaunchIdentity(t *testing.T) {
	recorder := recordProvenance(t, func(launch int) attemptResult {
		if launch == 0 {
			return attemptResult{exitCode: 0, sessionID: "ses_review", stopReason: "end_turn"}
		}
		return attemptResult{exitCode: 0, sessionID: "ses_verify", stopReason: "end_turn"}
	})
	config := writeCLIConfig(t, "loop.json", `{"max_iterations":1,"loop":[{"name":"review","prompt":"r"},{"name":"verify","prompt":"v","resume_from_previous":true}]}`)
	runCLI(t, "--backend", "pi", "--dir", t.TempDir(), "--loop-file", config, "--label", "resume-label")

	launches := recorder.all()
	if len(launches) != 2 {
		t.Fatalf("launches = %d, want 2", len(launches))
	}
	if got := launches[0].resumeID; got != "" {
		t.Fatalf("first phase resume id = %q, want none", got)
	}
	resumed := launches[1]
	if got := resumed.resumeID; got != "ses_review" {
		t.Fatalf("resumed phase resume id = %q, want the previous session %q", got, "ses_review")
	}
	if resumed.provenance.Phase != "verify" || resumed.provenance.Kind != "loop" || resumed.provenance.Attempt != 1 {
		t.Fatalf("resumed phase provenance = %+v, want a first verify launch", resumed.provenance)
	}
	if launches[0].provenance.ExecutionID != resumed.provenance.ExecutionID {
		t.Fatalf("execution id changed across phases: %q vs %q", launches[0].provenance.ExecutionID, resumed.provenance.ExecutionID)
	}
}

// TestTeamMemberIdentityIsNotShared pins that members do not share a mutable
// option record: each member's phase pairs with its own agent.
func TestTeamMemberIdentityIsNotShared(t *testing.T) {
	recorder := recordProvenance(t, func(int) attemptResult {
		return attemptResult{exitCode: 0, sessionID: "ses_member", stopReason: "end_turn"}
	})
	config := writeCLIConfig(t, "team.json", `{"team":[`+
		`{"name":"alpha","prompt":"a","agent":"agent-a","model":"model-a"},`+
		`{"name":"beta","prompt":"b","agent":"agent-b","model":"model-b"},`+
		`{"name":"gamma","prompt":"c","agent":"agent-c","model":"model-c"}]}`)
	runCLI(t, "--backend", "pi", "--dir", t.TempDir(), "--team-file", config)

	launches := recorder.all()
	if len(launches) != 3 {
		t.Fatalf("launches = %d, want one per member (%+v)", len(launches), launches)
	}
	pairs := map[string]string{}
	for _, launch := range launches {
		pairs[launch.agent] = launch.provenance.Phase
	}
	if pairs["agent-a"] != "alpha" || pairs["agent-b"] != "beta" || pairs["agent-c"] != "gamma" {
		t.Fatalf("agent/phase pairs = %v, want each member's own phase", pairs)
	}
}

// TestNestedLoopInTeamKeepsExecutionIDAndUsesInnerKind pins the other nesting
// direction: an inner loop's phases report the loop kind and iteration while
// sharing the containing CLI execution ID.
func TestNestedLoopInTeamKeepsExecutionIDAndUsesInnerKind(t *testing.T) {
	recorder := recordProvenance(t, func(int) attemptResult {
		return attemptResult{exitCode: 0, sessionID: "ses_nested", stopReason: "end_turn"}
	})
	dir := t.TempDir()
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"max_iterations":1,"pre":[{"name":"inner-pre","prompt":"p"}]}`), 0o600); err != nil {
		t.Fatalf("write loop config: %v", err)
	}
	teamPath := filepath.Join(dir, "team.json")
	if err := os.WriteFile(teamPath, []byte(`{"team":[{"name":"outer","loop_file":"loop.json"}]}`), 0o600); err != nil {
		t.Fatalf("write team config: %v", err)
	}
	sentinel := filepath.Join(t.TempDir(), "done")
	runCLI(t, "--backend", "pi", "--dir", dir, "--team-file", teamPath, "--label", "nested-loop", "--sentinel-file", sentinel)

	launches := recorder.all()
	if len(launches) != 1 {
		t.Fatalf("launches = %d, want 1", len(launches))
	}
	got := launches[0].provenance
	if got.Kind != "loop" || got.Phase != "inner-pre" || got.Iteration == nil || *got.Iteration != 0 {
		t.Fatalf("nested loop provenance = %+v, want the inner loop phase", got)
	}
	if got.ExecutionID != sentinelRunID(t, sentinel) {
		t.Fatalf("nested execution id = %q, want the containing CLI run id", got.ExecutionID)
	}
}
