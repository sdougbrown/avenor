package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// joinLaunchLog records the launch provenance of every provider construction,
// which is the point where the CLI hands a launch to a backend.
type joinLaunchLog struct {
	mu       sync.Mutex
	launches []runtime.LaunchProvenance
}

func (l *joinLaunchLog) add(provenance *runtime.LaunchProvenance) {
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
}

func (l *joinLaunchLog) all() []runtime.LaunchProvenance {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]runtime.LaunchProvenance(nil), l.launches...)
}

// recordJoinLaunches installs a provider factory that records each launch and
// serves it with one shared scripted provider, so sessions advance across
// phases. beforeLaunch runs for every construction.
func recordJoinLaunches(t *testing.T, beforeLaunch func()) *joinLaunchLog {
	t.Helper()
	log := &joinLaunchLog{}
	provider := newScriptedProvider()
	oldNewProvider := newProvider
	newProvider = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		log.add(opts.Provenance)
		if beforeLaunch != nil {
			beforeLaunch()
		}
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })
	return log
}

func eventsNamed(all []events.Event, name string) []events.Event {
	out := make([]events.Event, 0, len(all))
	for _, ev := range all {
		if ev.Event == name {
			out = append(out, ev)
		}
	}
	return out
}

func phaseEvents(all []events.Event, name, phase string) []events.Event {
	out := make([]events.Event, 0, 1)
	for _, ev := range eventsNamed(all, name) {
		if value, _ := ev.Fields["phase"].(string); value == phase {
			out = append(out, ev)
		}
	}
	return out
}

func joinedString(t *testing.T, ev events.Event, key string) string {
	t.Helper()
	value, ok := ev.Fields[key].(string)
	if !ok {
		t.Fatalf("%s has no string %q: %v", ev.Event, key, ev.Fields)
	}
	return value
}

func joinedNumber(t *testing.T, ev events.Event, key string) int {
	t.Helper()
	value, ok := ev.Fields[key].(float64)
	if !ok {
		t.Fatalf("%s has no numeric %q: %v", ev.Event, key, ev.Fields)
	}
	return int(value)
}

// requirePhaseIdentity fails unless ev carries the containing run identity and
// the expected runner kind.
func requirePhaseIdentity(t *testing.T, ev events.Event, runID, runKind, label, phase string) {
	t.Helper()
	if got := joinedString(t, ev, "runtime_id"); got != runID {
		t.Fatalf("%s runtime_id = %q, want %q", ev.Event, got, runID)
	}
	if got := joinedString(t, ev, "run_id"); got != runID {
		t.Fatalf("%s run_id = %q, want %q", ev.Event, got, runID)
	}
	if got := joinedString(t, ev, "run_kind"); got != runKind {
		t.Fatalf("%s run_kind = %q, want %q", ev.Event, got, runKind)
	}
	if got := joinedString(t, ev, "phase"); got != phase {
		t.Fatalf("%s phase = %q, want %q", ev.Event, got, phase)
	}
	if label != "" {
		if got := joinedString(t, ev, "run_label"); got != label {
			t.Fatalf("%s run_label = %q, want %q", ev.Event, got, label)
		}
	}
	if _, ok := ev.Fields["attempt"]; ok {
		t.Fatalf("%s carries a per-attempt field: %v", ev.Event, ev.Fields)
	}
}

// TestTeamLaunchIdentitiesJoinPhaseAndTeamEvents joins each member's launch
// record to exactly one phase start/end pair for that member and to the team's
// terminal outcome.
func TestTeamLaunchIdentitiesJoinPhaseAndTeamEvents(t *testing.T) {
	log := recordJoinLaunches(t, nil)
	config := writeCLIConfig(t, "team.json", `{"team":[`+
		`{"name":"alpha","prompt":"a","agent":"agent-a"},`+
		`{"name":"beta","prompt":"b","agent":"agent-b"},`+
		`{"name":"gamma","prompt":"c","agent":"agent-c"}]}`)
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentinel := filepath.Join(t.TempDir(), "done")
	runCLI(t, "--backend", "pi", "--dir", dir, "--team-file", config,
		"--on-event", eventsPath, "--label", "join-team", "--sentinel-file", sentinel)

	runID := sentinelRunID(t, sentinel)
	launches := log.all()
	if len(launches) != 3 {
		t.Fatalf("launches = %d, want one per member", len(launches))
	}
	all := readEventLogForTest(t, eventsPath)

	for _, launch := range launches {
		if launch.Kind != "team" {
			t.Fatalf("launch %+v is not a team member launch", launch)
		}
		if launch.Iteration != nil {
			t.Fatalf("team member launch carries an iteration: %+v", launch)
		}
		if launch.ExecutionID != runID || launch.RuntimeID != runID {
			t.Fatalf("launch identity = %q/%q, want the CLI run %q", launch.RuntimeID, launch.ExecutionID, runID)
		}

		starts := phaseEvents(all, "avenor.phase.start", launch.Phase)
		ends := phaseEvents(all, "avenor.phase.end", launch.Phase)
		if len(starts) != 1 || len(ends) != 1 {
			t.Fatalf("phase %q has %d starts and %d ends, want one pair", launch.Phase, len(starts), len(ends))
		}
		requirePhaseIdentity(t, starts[0], launch.ExecutionID, "team", "join-team", launch.Phase)
		requirePhaseIdentity(t, ends[0], launch.ExecutionID, "team", "join-team", launch.Phase)
		if got := joinedString(t, starts[0], "kind"); got != "team" {
			t.Fatalf("phase %q section kind = %q, want team", launch.Phase, got)
		}
		if got := joinedNumber(t, starts[0], "iteration"); got != 0 {
			t.Fatalf("phase %q iteration = %d, want the existing 0", launch.Phase, got)
		}
		if got := joinedString(t, ends[0], "stop_reason"); got != "end_turn" {
			t.Fatalf("phase %q stop_reason = %q, want end_turn", launch.Phase, got)
		}
		if _, ok := ends[0].Fields["kind"]; ok {
			t.Fatalf("phase end gained a section kind: %v", ends[0].Fields)
		}
	}

	teamStarts := eventsNamed(all, "avenor.team.start")
	if len(teamStarts) != 1 {
		t.Fatalf("team starts = %d, want 1", len(teamStarts))
	}
	if got := joinedString(t, teamStarts[0], "runtime_id"); got != runID {
		t.Fatalf("team.start runtime_id = %q, want %q", got, runID)
	}
	if got := joinedString(t, teamStarts[0], "run_id"); got != runID {
		t.Fatalf("team.start run_id = %q, want %q", got, runID)
	}
	teamEnds := eventsNamed(all, "avenor.team.end")
	if len(teamEnds) != 1 {
		t.Fatalf("team ends = %d, want 1", len(teamEnds))
	}
	if got := joinedString(t, teamEnds[0], "runtime_id"); got != runID {
		t.Fatalf("team.end runtime_id = %q, want %q", got, runID)
	}
	if got := joinedString(t, teamEnds[0], "run_id"); got != runID {
		t.Fatalf("team.end run_id = %q, want %q", got, runID)
	}
	if got := joinedString(t, teamEnds[0], "exit_reason"); got != "end_turn" {
		t.Fatalf("team.end exit_reason = %q, want end_turn", got)
	}
}

// TestLoopLaunchIdentitiesJoinIterationEvents joins each loop launch to the
// phase pair for its own iteration, keeping the existing section kind.
func TestLoopLaunchIdentitiesJoinIterationEvents(t *testing.T) {
	log := recordJoinLaunches(t, nil)
	config := writeCLIConfig(t, "loop.json", `{"max_iterations":2,`+
		`"pre":[{"name":"setup","prompt":"pre"}],`+
		`"loop":[{"name":"body","prompt":"body"}],`+
		`"post":[{"name":"report","prompt":"post"}]}`)
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentinel := filepath.Join(t.TempDir(), "done")
	runCLI(t, "--backend", "pi", "--dir", dir, "--loop-file", config,
		"--on-event", eventsPath, "--label", "join-loop", "--sentinel-file", sentinel)

	runID := sentinelRunID(t, sentinel)
	launches := log.all()
	want := []struct {
		phase     string
		iteration int
		kind      string
	}{{"setup", 0, "pre"}, {"body", 1, "loop"}, {"body", 2, "loop"}, {"report", 0, "post"}}
	if len(launches) != len(want) {
		t.Fatalf("launches = %d, want %d", len(launches), len(want))
	}
	all := readEventLogForTest(t, eventsPath)

	for i, expected := range want {
		launch := launches[i]
		if launch.Kind != "loop" || launch.Phase != expected.phase {
			t.Fatalf("launch %d = %+v, want the loop phase %q", i, launch, expected.phase)
		}
		if launch.Iteration == nil || *launch.Iteration != expected.iteration {
			t.Fatalf("launch %d iteration = %v, want %d", i, launch.Iteration, expected.iteration)
		}
		if launch.ExecutionID != runID {
			t.Fatalf("launch %d execution id = %q, want %q", i, launch.ExecutionID, runID)
		}

		starts := phaseEvents(all, "avenor.phase.start", expected.phase)
		ends := phaseEvents(all, "avenor.phase.end", expected.phase)
		matching := func(evs []events.Event) []events.Event {
			var out []events.Event
			for _, ev := range evs {
				if joinedNumber(t, ev, "iteration") == expected.iteration {
					out = append(out, ev)
				}
			}
			return out
		}
		starts = matching(starts)
		ends = matching(ends)
		if len(starts) != 1 || len(ends) != 1 {
			t.Fatalf("phase %q iteration %d has %d starts and %d ends, want one pair",
				expected.phase, expected.iteration, len(starts), len(ends))
		}
		requirePhaseIdentity(t, starts[0], runID, "loop", "join-loop", expected.phase)
		requirePhaseIdentity(t, ends[0], runID, "loop", "join-loop", expected.phase)
		if got := joinedString(t, starts[0], "kind"); got != expected.kind {
			t.Fatalf("phase %q section kind = %q, want %q", expected.phase, got, expected.kind)
		}
		if got := joinedString(t, ends[0], "stop_reason"); got != "end_turn" {
			t.Fatalf("phase %q stop_reason = %q, want end_turn", expected.phase, got)
		}
	}

	loopEnds := eventsNamed(all, "avenor.loop.end")
	if len(loopEnds) != 1 {
		t.Fatalf("loop ends = %d, want 1", len(loopEnds))
	}
	if got := joinedString(t, loopEnds[0], "runtime_id"); got != runID {
		t.Fatalf("loop.end runtime_id = %q, want %q", got, runID)
	}
	if got := joinedString(t, loopEnds[0], "exit_reason"); got != "max_iterations" {
		t.Fatalf("loop.end exit_reason = %q, want max_iterations", got)
	}
}

// TestDirectLaunchJoinsSessionEndWithoutPhaseEvents pins the direct-run join:
// the launch and the terminal event share the CLI execution ID, and a direct
// run has no phase events to attribute.
func TestDirectLaunchJoinsSessionEndWithoutPhaseEvents(t *testing.T) {
	log := recordJoinLaunches(t, nil)
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentinel := filepath.Join(t.TempDir(), "done")
	runCLI(t, "--backend", "pi", "--dir", dir, "--prompt", "work",
		"--on-event", eventsPath, "--label", "join-direct", "--sentinel-file", sentinel)

	runID := sentinelRunID(t, sentinel)
	launches := log.all()
	if len(launches) != 1 {
		t.Fatalf("launches = %d, want 1", len(launches))
	}
	launch := launches[0]
	if launch.Kind != "run" || launch.Phase != "" || launch.Iteration != nil || launch.Attempt != 1 {
		t.Fatalf("direct launch = %+v, want an attempt-1 direct identity", launch)
	}
	if launch.RuntimeID != runID || launch.ExecutionID != runID {
		t.Fatalf("direct launch identity = %q/%q, want the CLI run %q", launch.RuntimeID, launch.ExecutionID, runID)
	}

	all := readEventLogForTest(t, eventsPath)
	if starts := eventsNamed(all, "avenor.phase.start"); len(starts) != 0 {
		t.Fatalf("direct run emitted phase starts: %v", starts)
	}
	ends := eventsNamed(all, "avenor.phase.end")
	if len(ends) != 0 {
		t.Fatalf("direct run emitted phase ends: %v", ends)
	}
	sessionEnds := eventsNamed(all, "session.end")
	if len(sessionEnds) != 1 {
		t.Fatalf("session ends = %d, want 1", len(sessionEnds))
	}
	if got := joinedString(t, sessionEnds[0], "runtime_id"); got != launch.ExecutionID {
		t.Fatalf("session.end runtime_id = %q, want %q", got, launch.ExecutionID)
	}
	if got := joinedString(t, sessionEnds[0], "run_id"); got != launch.ExecutionID {
		t.Fatalf("session.end run_id = %q, want %q", got, launch.ExecutionID)
	}
	if got := joinedString(t, sessionEnds[0], "run_label"); got != "join-direct" {
		t.Fatalf("session.end run_label = %q, want join-direct", got)
	}
	if got := joinedString(t, sessionEnds[0], "stop_reason"); got != "end_turn" {
		t.Fatalf("session.end stop_reason = %q, want end_turn", got)
	}
}

// TestRetryThenNudgeKeepsOnePhasePair pins that retries and requirement nudges
// keep the existing single start/end pair and terminal fields; the attempt
// ordinal stays in the launch record only.
func TestRetryThenNudgeKeepsOnePhasePair(t *testing.T) {
	recorder := recordProvenance(t, func(launch int) attemptResult {
		if launch == 0 {
			return attemptResult{exitCode: 1, sessionID: "ses_phase"}
		}
		return attemptResult{exitCode: 0, sessionID: "ses_phase", stopReason: "end_turn"}
	})
	config := writeCLIConfig(t, "loop.json", `{"max_iterations":1,"loop":[{"name":"body","prompt":"body",`+
		`"requires":{"files":["NEVER_WRITTEN.md"]},"on_incomplete":{"max_nudges":1}}]}`)
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentinel := filepath.Join(t.TempDir(), "done")
	if code := runCLIWithCode(t, "--backend", "pi", "--dir", dir, "--loop-file", config,
		"--on-event", eventsPath, "--sentinel-file", sentinel, "--max-retries", "1", "--label", "join-nudge"); code != 1 {
		t.Fatalf("run = %d, want the incomplete-output failure", code)
	}

	launches := recorder.all()
	if len(launches) != 3 {
		t.Fatalf("launches = %d, want retry plus nudge", len(launches))
	}
	for i, launch := range launches {
		if launch.provenance.Attempt != i+1 {
			t.Fatalf("launch %d attempt = %d, want %d", i, launch.provenance.Attempt, i+1)
		}
	}

	all := readEventLogForTest(t, eventsPath)
	runID := sentinelRunID(t, sentinel)
	starts := phaseEvents(all, "avenor.phase.start", "body")
	ends := phaseEvents(all, "avenor.phase.end", "body")
	if len(starts) != 1 || len(ends) != 1 {
		t.Fatalf("phase body has %d starts and %d ends, want one pair across retries and nudges", len(starts), len(ends))
	}
	requirePhaseIdentity(t, starts[0], runID, "loop", "join-nudge", "body")
	requirePhaseIdentity(t, ends[0], runID, "loop", "join-nudge", "body")
	if got := joinedNumber(t, starts[0], "iteration"); got != 1 {
		t.Fatalf("phase start iteration = %d, want 1", got)
	}
	for _, ev := range []events.Event{starts[0], ends[0]} {
		if _, ok := ev.Fields["attempt"]; ok {
			t.Fatalf("%s carries a per-attempt field: %v", ev.Event, ev.Fields)
		}
	}
	if got := joinedString(t, ends[0], "stop_reason"); got != "incomplete_output" {
		t.Fatalf("phase end stop_reason = %q, want incomplete_output", got)
	}
	loopEnds := eventsNamed(all, "avenor.loop.end")
	if len(loopEnds) != 1 {
		t.Fatalf("loop ends = %d, want 1", len(loopEnds))
	}
	if got := joinedString(t, loopEnds[0], "exit_reason"); got != "phase_failure" {
		t.Fatalf("loop.end exit_reason = %q, want phase_failure", got)
	}
}

// TestNestedRepeatedPhaseNamesShareOneScope pins the approved nested
// limitation: identical inner phase names in repeated nested scopes produce
// identical identity tuples, so a consumer must treat the join as ambiguous
// rather than expect a generated scope ID.
func TestNestedRepeatedPhaseNamesShareOneScope(t *testing.T) {
	log := recordJoinLaunches(t, nil)
	dir := t.TempDir()
	inner := filepath.Join(dir, "inner.json")
	if err := os.WriteFile(inner, []byte(`{"max_iterations":1,"loop":[{"name":"step","prompt":"step"}]}`), 0o600); err != nil {
		t.Fatalf("write inner loop: %v", err)
	}
	outer := filepath.Join(dir, "outer.json")
	if err := os.WriteFile(outer, []byte(`{"max_iterations":2,"loop":[{"name":"nested","loop_file":"inner.json"}]}`), 0o600); err != nil {
		t.Fatalf("write outer loop: %v", err)
	}
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentinel := filepath.Join(t.TempDir(), "done")
	runCLI(t, "--backend", "pi", "--dir", dir, "--loop-file", outer,
		"--on-event", eventsPath, "--label", "join-nested", "--sentinel-file", sentinel)

	runID := sentinelRunID(t, sentinel)
	launches := log.all()
	if len(launches) != 2 {
		t.Fatalf("launches = %d, want one per outer iteration", len(launches))
	}
	for i, launch := range launches {
		if launch.Kind != "loop" || launch.Phase != "step" {
			t.Fatalf("launch %d = %+v, want the inner loop phase", i, launch)
		}
		if launch.Iteration == nil || *launch.Iteration != 1 {
			t.Fatalf("launch %d iteration = %v, want the inner iteration 1", i, launch.Iteration)
		}
		if launch.ExecutionID != runID || launch.RuntimeID != runID {
			t.Fatalf("launch %d identity = %q/%q, want the shared CLI run %q", i, launch.RuntimeID, launch.ExecutionID, runID)
		}
	}

	all := readEventLogForTest(t, eventsPath)
	starts := phaseEvents(all, "avenor.phase.start", "step")
	ends := phaseEvents(all, "avenor.phase.end", "step")
	if len(starts) != 2 || len(ends) != 2 {
		t.Fatalf("nested step has %d starts and %d ends, want two of each", len(starts), len(ends))
	}
	for _, ev := range append(append([]events.Event{}, starts...), ends...) {
		requirePhaseIdentity(t, ev, runID, "loop", "join-nested", "step")
		if got := joinedNumber(t, ev, "iteration"); got != 1 {
			t.Fatalf("%s iteration = %d, want 1", ev.Event, got)
		}
	}
	// Both observations and both event pairs carry one identical tuple: the
	// join is many-to-many by approval, and no scope field was invented.
	for _, ev := range append(append([]events.Event{}, starts...), ends...) {
		if _, ok := ev.Fields["scope_id"]; ok {
			t.Fatalf("%s invented a nested scope id: %v", ev.Event, ev.Fields)
		}
	}
}

// TestControlStreamCarriesSamePhaseTagsAsDurableLog runs the CLI with a
// control socket, subscribes while the second phase is still pending, parks the
// run in a third phase until that phase's control events are observed, and
// requires the control copy to match the durable event log field for field.
func TestControlStreamCarriesSamePhaseTagsAsDurableLog(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "control.sock")
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	firstPhase := make(chan struct{})
	releaseFirst := make(chan struct{})
	holdPhase := make(chan struct{})
	releaseHold := make(chan struct{})
	var firstBarrier, holdBarrier sync.Once
	var construction atomic.Int64
	log := recordJoinLaunches(t, func() {
		switch construction.Add(1) {
		case 1:
			firstBarrier.Do(func() {
				close(firstPhase)
				<-releaseFirst
			})
		case 3:
			holdBarrier.Do(func() {
				close(holdPhase)
				<-releaseHold
			})
		}
	})
	config := writeCLIConfig(t, "loop.json", `{"max_iterations":1,"loop":[`+
		`{"name":"first","prompt":"a"},{"name":"second","prompt":"b"},{"name":"hold","prompt":"c"}]}`)

	type runResult struct {
		code   int
		stderr string
	}
	done := make(chan runResult, 1)
	go func() {
		stderr := &safeStringBuilder{}
		code := run([]string{"--backend", "pi", "--dir", dir, "--loop-file", config,
			"--on-event", eventsPath, "--control-socket", socket,
			"--run-id", "run_control_join", "--label", "control-label"}, func(string) string { return "" }, stderr)
		done <- runResult{code: code, stderr: stderr.String()}
	}()

	<-firstPhase
	control, err := client.Dial(socket)
	if err != nil {
		t.Fatalf("dial control socket: %v", err)
	}
	defer control.Close()
	controlEvents := control.Events()
	close(releaseFirst)

	observed := collectControlEvents(t, controlEvents, func(ev client.Event) bool {
		if ev.Event != "avenor.phase.end" {
			return false
		}
		phase, _ := ev.Raw["phase"].(string)
		return phase == "second"
	})
	<-holdPhase
	close(releaseHold)

	result := <-done
	if result.code != 0 {
		t.Fatalf("run = %d, want 0; stderr=%s", result.code, result.stderr)
	}

	durable := readEventLogForTest(t, eventsPath)
	observedEvents := make([]events.Event, 0, len(observed))
	for _, ev := range observed {
		observedEvents = append(observedEvents, events.Event{Event: ev.Event, SessionID: ev.SessionID, Fields: ev.Raw})
	}

	for _, name := range []string{"avenor.phase.start", "avenor.phase.end"} {
		fromLog := phaseEvents(durable, name, "second")
		fromControl := phaseEvents(observedEvents, name, "second")
		if len(fromLog) != 1 || len(fromControl) != 1 {
			t.Fatalf("%s for the second phase: durable=%d control=%d, want one each",
				name, len(fromLog), len(fromControl))
		}
		keys := []string{"runtime_id", "run_id", "run_label", "run_kind", "phase"}
		if name == "avenor.phase.start" {
			keys = append(keys, "kind")
		}
		for _, key := range keys {
			if got, want := joinedString(t, fromControl[0], key), joinedString(t, fromLog[0], key); got != want {
				t.Fatalf("%s %s: control=%q durable=%q", name, key, got, want)
			}
		}
		if got, want := joinedNumber(t, fromControl[0], "iteration"), joinedNumber(t, fromLog[0], "iteration"); got != want {
			t.Fatalf("%s iteration: control=%d durable=%d", name, got, want)
		}
		if got, want := joinedNumber(t, fromControl[0], "seq"), joinedNumber(t, fromLog[0], "seq"); got != want {
			t.Fatalf("%s seq: control=%d durable=%d", name, got, want)
		}
	}

	// The launch log still pairs with both copies of the second phase.
	launches := log.all()
	if len(launches) != 3 {
		t.Fatalf("launches = %d, want one per phase", len(launches))
	}
	if launches[1].Phase != "second" {
		t.Fatalf("second launch = %+v, want the second phase", launches[1])
	}
}

// gatedPromptProvider serves one scripted turn behind two gates: it holds the
// prompt until emit is released so a subscriber registers before anything is
// published, and holds it again after the terminal event until observed is
// released so the CLI cannot exit and close the socket first. The event channel
// stays open, since closing it lets the CLI settle the turn without waiting for
// the prompt call.
type gatedPromptProvider struct {
	*scriptedProvider
	ready    chan struct{}
	emit     chan struct{}
	observed chan struct{}
}

func (p *gatedPromptProvider) Prompt(ctx context.Context, sessionID, _ string) error {
	p.mu.Lock()
	session, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown session %q", sessionID)
	}
	close(p.ready)
	<-p.emit
	session.events <- events.Event{
		Event:     "session.end",
		SessionID: sessionID,
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	<-p.observed
	return nil
}

// TestControlStreamCarriesSameSessionEndTagsAsDurableLog joins the terminal
// identity of a control-enabled direct run: the session.end a subscriber
// observes carries the same run identity the durable log persists, and both
// match the CLI run ID and label.
func TestControlStreamCarriesSameSessionEndTagsAsDurableLog(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "control.sock")
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	provider := &gatedPromptProvider{
		scriptedProvider: newScriptedProvider(),
		ready:            make(chan struct{}),
		emit:             make(chan struct{}),
		observed:         make(chan struct{}),
	}
	oldNewProvider := newProvider
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }
	t.Cleanup(func() { newProvider = oldNewProvider })
	releaseEmit := sync.OnceFunc(func() { close(provider.emit) })
	releaseObserved := sync.OnceFunc(func() { close(provider.observed) })
	defer releaseObserved()
	defer releaseEmit()

	type runResult struct {
		code   int
		stderr string
	}
	done := make(chan runResult, 1)
	go func() {
		stderr := &safeStringBuilder{}
		code := run([]string{"--backend", "pi", "--dir", dir, "--prompt", "work",
			"--on-event", eventsPath, "--control-socket", socket,
			"--run-id", "run_direct_control", "--label", "direct-control"}, func(string) string { return "" }, stderr)
		done <- runResult{code: code, stderr: stderr.String()}
	}()

	<-provider.ready
	control, err := client.Dial(socket)
	if err != nil {
		t.Fatalf("dial control socket: %v", err)
	}
	defer control.Close()
	controlEvents := control.Events()

	releaseEmit()
	observed := collectControlEvents(t, controlEvents, func(ev client.Event) bool {
		return ev.Event == "session.end"
	})
	releaseObserved()

	result := <-done
	if result.code != 0 {
		t.Fatalf("run = %d, want 0; stderr=%s", result.code, result.stderr)
	}

	terminal := observed[len(observed)-1]
	fromControl := events.Event{Event: terminal.Event, SessionID: terminal.SessionID, Fields: terminal.Raw}
	fromLog := eventsNamed(readEventLogForTest(t, eventsPath), "session.end")
	if len(fromLog) != 1 {
		t.Fatalf("durable session ends = %d, want 1", len(fromLog))
	}
	for _, expected := range []struct{ key, value string }{
		{"runtime_id", "run_direct_control"},
		{"run_id", "run_direct_control"},
		{"run_label", "direct-control"},
	} {
		if got := joinedString(t, fromControl, expected.key); got != expected.value {
			t.Fatalf("session.end %s on the control stream = %q, want %q", expected.key, got, expected.value)
		}
		if got := joinedString(t, fromLog[0], expected.key); got != expected.value {
			t.Fatalf("session.end %s in the durable log = %q, want %q", expected.key, got, expected.value)
		}
	}
}

func collectControlEvents(t *testing.T, ch <-chan client.Event, stop func(client.Event) bool) []client.Event {
	t.Helper()
	deadline := time.After(5 * time.Second)
	var out []client.Event
	names := func() []string {
		seen := make([]string, 0, len(out))
		for _, ev := range out {
			seen = append(seen, ev.Event)
		}
		return seen
	}
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("control stream closed before the expected event; got %v", names())
			}
			out = append(out, ev)
			if stop(ev) {
				return out
			}
		case <-deadline:
			t.Fatalf("timed out waiting for the expected control event; got %v", names())
		}
	}
}

// safeStringBuilder is a mutex-guarded strings.Builder for the CLI run
// goroutine.
type safeStringBuilder struct {
	mu  sync.Mutex
	raw []byte
}

func (b *safeStringBuilder) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.raw = append(b.raw, p...)
	return len(p), nil
}

func (b *safeStringBuilder) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.raw)
}
