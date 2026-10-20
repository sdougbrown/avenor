package stable

// Stage 2 fanout-boundary tests: an agent.status phase outside the runstate
// vocabulary normalizes to PhaseUnknown, is logged once per distinct raw
// value under the per-runtime diagnostic cache, never blocks later valid
// updates, and never blocks completion.

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/diagnote"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runstate"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/claudechannel"
)

// syncBuffer is a mutex-guarded bytes.Buffer: concurrent fanout writes report
// diagnostics to the supervisor's stderr writer from multiple goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newUnknownPhaseTestChild(t *testing.T, sup *Supervisor) (*childRuntime, *runtimeFanoutWriter) {
	t.Helper()
	child := &childRuntime{
		id:          "rt_unknown_phase",
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		eventWriter: stableTestSink{},
	}
	registerSyntheticChild(t, sup, child, testLifecycleOptions{id: child.id, exec: execIdle, class: directAttemptClass})
	writer := &runtimeFanoutWriter{base: stableTestSink{}, runtimeID: child.id, child: child, supervisor: sup, control: sup.control}
	return child, writer
}

func writeAgentStatus(t *testing.T, writer *runtimeFanoutWriter, phase string) {
	t.Helper()
	if err := writer.Write(events.Event{Event: "agent.status", SessionID: "ses_unknown_phase", Fields: map[string]any{"phase": phase}}); err != nil {
		t.Fatalf("Write agent.status phase %q: %v", phase, err)
	}
}

func storedChildPhase(t *testing.T, child *childRuntime) runstate.Phase {
	t.Helper()
	child.mu.Lock()
	defer child.mu.Unlock()
	return child.phase
}

func countUnrecognizedPhaseLogs(stderr, phase string) int {
	return strings.Count(stderr, fmt.Sprintf("reported unrecognized phase %q", phase))
}

func assertNoLifecycleErrorLine(t *testing.T, stderr string) {
	t.Helper()
	if strings.Contains(stderr, "lifecycle error") {
		t.Fatalf("unexpected lifecycle error line in %q", stderr)
	}
}

func TestFanoutUnknownPhaseNormalizesLogsOnceAndCompletes(t *testing.T) {
	var stderr bytes.Buffer
	sup := newTestSupervisor(t, Config{
		ControlSocket:   newStableSocketPath(t, "fanout-unknown-phase"),
		MaxRuntimes:     1,
		ShutdownTimeout: 0,
		Stderr:          &stderr,
	})
	child, writer := newUnknownPhaseTestChild(t, sup)

	writeAgentStatus(t, writer, "warp")
	if got := storedChildPhase(t, child); got != runstate.PhaseUnknown {
		t.Fatalf("stored phase = %q, want unknown", got)
	}
	if got := countUnrecognizedPhaseLogs(stderr.String(), "warp"); got != 1 {
		t.Fatalf("unrecognized-phase log lines = %d, want 1", got)
	}
	status, err := sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	if entry, ok := status.(map[string]any); !ok || entry["phase"] != "unknown" {
		t.Fatalf("status phase = %v, want \"unknown\"", status)
	}

	// The same raw value does not log a second line.
	writeAgentStatus(t, writer, "warp")
	if got := countUnrecognizedPhaseLogs(stderr.String(), "warp"); got != 1 {
		t.Fatalf("unrecognized-phase log lines after repeat = %d, want 1", got)
	}

	// A later valid phase replaces the stored unknown phase without logging.
	writeAgentStatus(t, writer, "working")
	if got := storedChildPhase(t, child); got != runstate.PhaseWorking {
		t.Fatalf("stored phase after valid update = %q, want working", got)
	}
	if got := countUnrecognizedPhaseLogs(stderr.String(), "working"); got != 0 {
		t.Fatalf("valid phase logged as unrecognized = %d, want 0", got)
	}

	// The unrecognized phase never blocks completion.
	finishSyntheticChildLifecycle(sup, child)
	if !child.lc.isCompleted() {
		t.Fatal("runtime did not complete after an unrecognized phase")
	}
	assertNoLifecycleErrorLine(t, stderr.String())
}

func TestUnknownPhaseDiagnosticCacheSaturatesAtCap(t *testing.T) {
	var stderr bytes.Buffer
	sup := newTestSupervisor(t, Config{
		ControlSocket:   newStableSocketPath(t, "unknown-phase-cap"),
		MaxRuntimes:     1,
		ShutdownTimeout: 0,
		Stderr:          &stderr,
	})
	child, writer := newUnknownPhaseTestChild(t, sup)

	for i := 0; i < diagnote.Cap; i++ {
		writeAgentStatus(t, writer, fmt.Sprintf("warp_%02d", i))
	}
	if got := strings.Count(stderr.String(), "reported unrecognized phase"); got != diagnote.Cap {
		t.Fatalf("unrecognized-phase log lines = %d, want %d", got, diagnote.Cap)
	}
	if strings.Contains(stderr.String(), diagnote.SuppressedNote) {
		t.Fatal("suppression line logged before the cap was reached")
	}

	// The cap is reached: the next distinct value logs only the suppression line.
	writeAgentStatus(t, writer, "warp_overflow")
	if got := strings.Count(stderr.String(), diagnote.SuppressedNote); got != 1 {
		t.Fatalf("suppression lines = %d, want 1", got)
	}
	if got := countUnrecognizedPhaseLogs(stderr.String(), "warp_overflow"); got != 0 {
		t.Fatalf("post-cap value logged = %d, want 0", got)
	}

	// Further distinct values stay silent and the suppression line stays single.
	writeAgentStatus(t, writer, "warp_overflow_2")
	if got := strings.Count(stderr.String(), diagnote.SuppressedNote); got != 1 {
		t.Fatalf("suppression lines after further values = %d, want 1", got)
	}
	// A repeat of a cached value stays silent too.
	writeAgentStatus(t, writer, "warp_00")
	if got := strings.Count(stderr.String(), "reported unrecognized phase"); got != diagnote.Cap {
		t.Fatalf("unrecognized-phase log lines after repeats = %d, want %d", got, diagnote.Cap)
	}

	// Saturation never blocks later valid updates or completion.
	writeAgentStatus(t, writer, "working")
	if got := storedChildPhase(t, child); got != runstate.PhaseWorking {
		t.Fatalf("stored phase after saturation = %q, want working", got)
	}
	finishSyntheticChildLifecycle(sup, child)
	if !child.lc.isCompleted() {
		t.Fatal("runtime did not complete after diagnostic-cache saturation")
	}
	assertNoLifecycleErrorLine(t, stderr.String())
}

// The fanout stores a claudechannel broker phase verbatim because the mapping
// emits only known phases: a recognized phase never logs and never normalizes
// to unknown.
func TestClaudechannelReportPhaseReachesStoredPhase(t *testing.T) {
	var stderr bytes.Buffer
	sup := newTestSupervisor(t, Config{
		ControlSocket:   newStableSocketPath(t, "claudechannel-phase"),
		MaxRuntimes:     1,
		ShutdownTimeout: 0,
		Stderr:          &stderr,
	})
	child, writer := newUnknownPhaseTestChild(t, sup)

	ev := claudechannel.BrokerEvent("ses_unknown_phase", "blocked", nil)
	if err := writer.Write(ev); err != nil {
		t.Fatalf("Write blocked report event: %v", err)
	}
	if got := storedChildPhase(t, child); got != runstate.PhaseWaiting {
		t.Fatalf("stored phase after blocked report = %q, want waiting", got)
	}
	if got := strings.Count(stderr.String(), "reported unrecognized phase"); got != 0 {
		t.Fatalf("recognized phase logged as unrecognized: %d unrecognized-phase log lines, want 0", got)
	}

	// An arbitrary report state normalizes to working before the fanout, so it
	// never lands in the unknown-phase diagnostic cache.
	ev = claudechannel.BrokerEvent("ses_unknown_phase", "custom_state", nil)
	if err := writer.Write(ev); err != nil {
		t.Fatalf("Write custom report event: %v", err)
	}
	if got := storedChildPhase(t, child); got != runstate.PhaseWorking {
		t.Fatalf("stored phase after custom report = %q, want working", got)
	}
	if got := strings.Count(stderr.String(), "reported unrecognized phase"); got != 0 {
		t.Fatalf("normalized phase logged as unrecognized: %d unrecognized-phase log lines, want 0", got)
	}

	finishSyntheticChildLifecycle(sup, child)
	if !child.lc.isCompleted() {
		t.Fatal("runtime did not complete after claudechannel phases")
	}
	assertNoLifecycleErrorLine(t, stderr.String())
}

func TestUnknownPhaseDiagnosticCacheConcurrentWrites(t *testing.T) {
	var stderr syncBuffer
	sup := newTestSupervisor(t, Config{
		ControlSocket:   newStableSocketPath(t, "unknown-phase-concurrent"),
		MaxRuntimes:     1,
		ShutdownTimeout: 0,
		Stderr:          &stderr,
	})
	child, writer := newUnknownPhaseTestChild(t, sup)

	const goroutines = 8
	const distinct = 24
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for repeat := 0; repeat < 5; repeat++ {
				for idx := g; idx < distinct; idx += goroutines {
					phase := fmt.Sprintf("warp_%02d", idx)
					if err := writer.Write(events.Event{Event: "agent.status", SessionID: "ses_unknown_phase", Fields: map[string]any{"phase": phase}}); err != nil {
						t.Errorf("Write agent.status phase %q: %v", phase, err)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()

	// Every distinct value up to the cap is logged exactly once, then the
	// single suppression line covers the rest.
	if got := strings.Count(stderr.String(), "reported unrecognized phase"); got != diagnote.Cap {
		t.Fatalf("unrecognized-phase log lines = %d, want %d", got, diagnote.Cap)
	}
	if got := strings.Count(stderr.String(), diagnote.SuppressedNote); got != 1 {
		t.Fatalf("suppression lines = %d, want 1", got)
	}

	// Concurrent saturation never blocks later valid updates or completion.
	writeAgentStatus(t, writer, "working")
	if got := storedChildPhase(t, child); got != runstate.PhaseWorking {
		t.Fatalf("stored phase after concurrent writes = %q, want working", got)
	}
	finishSyntheticChildLifecycle(sup, child)
	if !child.lc.isCompleted() {
		t.Fatal("runtime did not complete after concurrent unrecognized phases")
	}
	assertNoLifecycleErrorLine(t, stderr.String())
}

// TestRunEndPublishesOnceWithDerivedExitCode pins the run-level terminal end
// contract: its exit code always comes from the stop reason (a
// backend-supplied number never wins), a session gets at most one run end, and
// a child with no session publishes none.
func TestRunEndPublishesOnceWithDerivedExitCode(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/test-run-end.sock"})
	sink := &seqCaptureSink{}
	child := &childRuntime{
		id:          "rt_run_end",
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		eventWriter: sink,
	}
	registerSyntheticChild(t, sup, child, testLifecycleOptions{id: child.id, exec: execIdle, class: directAttemptClass})
	writer := &runtimeFanoutWriter{base: sink, runtimeID: child.id, child: child, supervisor: sup, control: sup.control}

	runEnd := func(sessionID, stopReason string, exitCode any) events.Event {
		return events.Event{Event: "session.end", SessionID: sessionID, Fields: map[string]any{
			"stop_reason": stopReason,
			"exit_code":   exitCode,
			"end_kind":    "run",
		}}
	}
	if err := writer.writeRunEnd(runEnd("ses_run_end", "cancelled", 0)); err != nil {
		t.Fatalf("writeRunEnd: %v", err)
	}
	if err := writer.writeRunEnd(runEnd("ses_run_end", "timeout", 124)); err != nil {
		t.Fatalf("writeRunEnd repeat: %v", err)
	}
	if err := writer.writeRunEnd(runEnd("", "cancelled", 130)); err != nil {
		t.Fatalf("writeRunEnd without a session: %v", err)
	}
	finishSyntheticChildLifecycle(sup, child)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.events) != 1 {
		t.Fatalf("published %d session.end events, want 1", len(sink.events))
	}
	end := sink.events[0]
	if got := end.SessionID; got != "ses_run_end" {
		t.Fatalf("session_id = %q, want ses_run_end", got)
	}
	if code, ok := events.Int64(end.Fields["exit_code"]); !ok || code != 130 {
		t.Fatalf("exit_code = %v, want 130 from the cancelled stop reason", end.Fields["exit_code"])
	}
}

// TestEndKindIsStampedOnlyByTheRunEnd proves a backend cannot tag its own turn
// end as the run's terminal record: the ordinary write path strips end_kind,
// and the run-level writer stamps it.
func TestEndKindIsStampedOnlyByTheRunEnd(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/test-end-kind.sock"})
	sink := &seqCaptureSink{}
	child := &childRuntime{
		id:          "rt_end_kind",
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		eventWriter: sink,
	}
	registerSyntheticChild(t, sup, child, testLifecycleOptions{id: child.id, exec: execIdle, class: directAttemptClass})
	writer := sup.fanoutWriterLocked(child)

	if err := writer.Write(events.Event{Event: "session.end", SessionID: "ses_end_kind", Fields: map[string]any{
		"stop_reason": "end_turn", "end_kind": "run",
	}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := writer.writeRunEnd(events.Event{Event: "session.end", SessionID: "ses_end_kind", Fields: map[string]any{
		"stop_reason": "cancelled",
	}}); err != nil {
		t.Fatalf("writeRunEnd: %v", err)
	}
	finishSyntheticChildLifecycle(sup, child)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.events) != 2 {
		t.Fatalf("published %d session.end events, want 2", len(sink.events))
	}
	if kind, ok := sink.events[0].Fields["end_kind"]; ok {
		t.Fatalf("backend-supplied end_kind %v survived the ordinary write path", kind)
	}
	if kind, _ := sink.events[1].Fields["end_kind"].(string); kind != "run" {
		t.Fatalf("run-level end end_kind = %v, want run", sink.events[1].Fields["end_kind"])
	}
}

// TestFallbackFanoutWriterCarriesRecorderAndWorkflowMetadata proves the writer
// used before a runtime installs one still records to the broker and carries
// the workflow identity, so an end published early reaches the same places an
// installed writer's does.
func TestFallbackFanoutWriterCarriesRecorderAndWorkflowMetadata(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/test-fanout-fallback.sock"})
	sink := &seqCaptureSink{}
	child := &childRuntime{
		id:          "rt_fanout_fallback",
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		eventWriter: sink,
		workflowID:  "wf_fallback",
		nodeID:      "node_fallback",
	}
	registerSyntheticChild(t, sup, child, testLifecycleOptions{id: child.id, exec: execIdle, class: workflowAttemptClass})
	sup.ensureChildBrokerRun(child)
	writer := sup.fanoutWriterLocked(child)
	if err := writer.writeRunEnd(events.Event{Event: "session.end", SessionID: "ses_fallback", Fields: map[string]any{
		"stop_reason": "cancelled", "end_kind": "run",
	}}); err != nil {
		t.Fatalf("writeRunEnd: %v", err)
	}
	state := sup.broker.GetRun(child.id)
	if state == nil {
		t.Fatalf("broker has no run for %s", child.id)
	}
	state.Mu.Lock()
	reports := append([]broker.Report(nil), state.Reports...)
	state.Mu.Unlock()
	if len(reports) == 0 || reports[len(reports)-1].State != "cancelled" {
		t.Fatalf("broker reports = %v, want the run end's cancelled record", reports)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.events) != 1 {
		t.Fatalf("published %d events, want 1", len(sink.events))
	}
	stamped := sink.events[0].Fields
	if got, _ := stamped["workflow_id"].(string); got != "wf_fallback" {
		t.Fatalf("stamped workflow_id = %v, want wf_fallback", stamped["workflow_id"])
	}
	if got, _ := stamped["node_id"].(string); got != "node_fallback" {
		t.Fatalf("stamped node_id = %v, want node_fallback", stamped["node_id"])
	}
	finishSyntheticChildLifecycle(sup, child)
}

// failingWriteSink rejects every durable write.
type failingWriteSink struct{}

func (failingWriteSink) Write(events.Event) error {
	return errors.New("intentional durable write failure")
}
func (failingWriteSink) Close() error { return nil }

// TestRecorderFeedsOnlyAfterTheDurableWrite proves a failed durable write
// leaves no terminal record in the broker for a run that did not end.
func TestRecorderFeedsOnlyAfterTheDurableWrite(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/test-recorder-order.sock"})
	child := &childRuntime{
		id:          "rt_recorder_order",
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		eventWriter: failingWriteSink{},
	}
	registerSyntheticChild(t, sup, child, testLifecycleOptions{id: child.id, exec: execIdle, class: directAttemptClass})
	sup.ensureChildBrokerRun(child)
	writer := sup.fanoutWriterLocked(child)

	err := writer.writeRunEnd(events.Event{Event: "session.end", SessionID: "ses_recorder_order", Fields: map[string]any{
		"stop_reason": "cancelled",
	}})
	if err == nil {
		t.Fatal("writeRunEnd error = nil, want the durable write failure")
	}
	state := sup.broker.GetRun(child.id)
	if state == nil {
		t.Fatalf("broker has no run for %s", child.id)
	}
	state.Mu.Lock()
	reports := append([]broker.Report(nil), state.Reports...)
	state.Mu.Unlock()
	if len(reports) != 0 {
		t.Fatalf("broker reports = %v, want none after a failed durable write", reports)
	}
	finishSyntheticChildLifecycle(sup, child)
}
