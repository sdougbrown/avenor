package stable

// Stage 2 fanout-boundary tests: an agent.status phase outside the runstate
// vocabulary normalizes to PhaseUnknown, is logged once per distinct raw
// value under the per-runtime diagnostic cache, never blocks later valid
// updates, and never blocks completion.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/diagnote"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runstate"
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

// TestPhaseEventFanoutMatchesDurableAndControlStreams pins that phase start and
// end events keep run_kind, the section kind and the supervisor run tags on
// both the durable file and the control stream, with identical payloads.
func TestPhaseEventFanoutMatchesDurableAndControlStreams(t *testing.T) {
	sup := newTestSupervisor(t, Config{
		ControlSocket:   newStableSocketPath(t, "phase-fanout"),
		MaxRuntimes:     1,
		ShutdownTimeout: 0,
	})
	path := filepath.Join(t.TempDir(), "events.ndjson")
	fileWriter, err := cli.NewEventWriter(path)
	if err != nil {
		t.Fatalf("NewEventWriter: %v", err)
	}
	child := &childRuntime{
		id:          "rt_phase_fanout",
		runID:       sup.runID,
		label:       "phase-fanout",
		eventWriter: fileWriter,
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	liveCh := sup.control.SubscribeEvents(ctx)
	writer := newRuntimeFanoutWriter(sup, child, cli.NewEventMetadata(sup.runID, child.label, child.id), nil)
	for _, ev := range []events.Event{
		{Event: "avenor.phase.start", Fields: map[string]any{"phase": "body", "iteration": 1, "kind": "loop", "run_kind": "loop"}},
		{Event: "avenor.phase.end", Fields: map[string]any{"phase": "body", "iteration": 1, "run_kind": "loop", "stop_reason": "end_turn"}},
	} {
		if err := writer.Write(ev); err != nil {
			t.Fatalf("Write(%s): %v", ev.Event, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("durable events = %d, want 2", len(lines))
	}
	var logged []events.Event
	for _, line := range lines {
		var ev events.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("Unmarshal %q: %v", line, err)
		}
		logged = append(logged, ev)
	}
	var live []events.Event
	for range logged {
		select {
		case ev := <-liveCh:
			live = append(live, ev)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for the live phase event")
		}
	}

	for i, ev := range logged {
		if got := ev.Fields["run_kind"]; got != "loop" {
			t.Fatalf("%s run_kind = %v, want loop", ev.Event, got)
		}
		if got := ev.Fields["runtime_id"]; got != child.id {
			t.Fatalf("%s runtime_id = %v, want %q", ev.Event, got, child.id)
		}
		if got := ev.Fields["run_id"]; got != sup.runID {
			t.Fatalf("%s run_id = %v, want %q", ev.Event, got, sup.runID)
		}
		if ev.Event == "avenor.phase.start" {
			if got := ev.Fields["kind"]; got != "loop" {
				t.Fatalf("phase start kind = %v, want the unchanged section kind loop", got)
			}
		} else if _, ok := ev.Fields["kind"]; ok {
			t.Fatalf("phase end gained a section kind: %v", ev.Fields)
		}
		loggedJSON, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("Marshal logged: %v", err)
		}
		liveJSON, err := json.Marshal(live[i])
		if err != nil {
			t.Fatalf("Marshal live: %v", err)
		}
		if string(loggedJSON) != string(liveJSON) {
			t.Fatalf("file/live mismatch for %s\nfile=%s\nlive=%s", ev.Event, loggedJSON, liveJSON)
		}
	}
}
