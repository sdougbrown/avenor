package stable

import (
	"context"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// newDeferredChild builds a childRuntime backed by a stableDeferredProvider
// whose spawn-time session id is provisional. The child is registered with the
// supervisor so RuntimeStatus/List reflect its aggregate state.
func newDeferredChild(t *testing.T, sup *Supervisor, provisionalID, realID string, evs []stableScriptedEvent) (*childRuntime, *stableDeferredProvider) {
	t.Helper()
	provider := &stableDeferredProvider{
		provisionalID: provisionalID,
		realID:        realID,
		backend:       "agy",
		pid:           4242,
		events:        evs,
	}
	child := &childRuntime{
		id:          "rt_deferred",
		label:       "deferred",
		provider:    provider,
		session:     runtime.Session{SessionID: provisionalID, Backend: "agy", Dir: "/work", PID: 4242},
		eventWriter: stableTestSink{},
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		runID:       sup.runID,
	}
	return child, provider
}

// deferredStartEndEvents returns the canonical headless event sequence: a
// session.start carrying the real conversation_id, then a terminal session.end.
// An optional release gate holds session.start so a test can assert the
// provisional identity is still live before the authoritative event fires.
func deferredStartEndEvents(realID string, release <-chan struct{}) []stableScriptedEvent {
	return []stableScriptedEvent{
		{event: events.Event{Event: "session.start", SessionID: realID, Fields: map[string]any{"conversation_id": realID}}, release: release},
		{event: events.Event{Event: "session.end", SessionID: realID, Fields: map[string]any{"stop_reason": "end_turn"}}},
	}
}

func TestRunChildAttemptAdoptsExternalConversationID(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-direct-adopt.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-direct", "conv-direct-real"
	child, _ := newDeferredChild(t, sup, provisionalID, realID, deferredStartEndEvents(realID, nil))
	sup.runtimes[child.id] = child

	result := sup.runChildAttempt(context.Background(), child, "", "hello", nil)
	if result.exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", result.exitCode)
	}
	if result.sessionID != realID {
		t.Fatalf("attempt sessionID = %q, want %q", result.sessionID, realID)
	}
	child.mu.Lock()
	sessionID, backend, dir, pid := child.session.SessionID, child.session.Backend, child.session.Dir, child.session.PID
	child.mu.Unlock()
	if sessionID != realID {
		t.Fatalf("child session = %q, want %q", sessionID, realID)
	}
	if backend != "agy" || dir != "/work" || pid != 4242 {
		t.Fatalf("non-ID session fields changed: backend=%q dir=%q pid=%d", backend, dir, pid)
	}
	statusAny, err := sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	status, _ := statusAny.(map[string]any)
	if id, _ := status["session_id"].(string); id != realID {
		t.Fatalf("status session_id = %q, want %q", id, realID)
	}
	if statusPid, _ := status["pid"].(int); statusPid != 4242 {
		t.Fatalf("status pid = %d, want 4242", statusPid)
	}
}

func TestRuntimeStatusAndListUseAdoptedSessionID(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-status-list-adopt.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-status", "conv-status-real"
	child, _ := newDeferredChild(t, sup, provisionalID, realID, deferredStartEndEvents(realID, nil))
	sup.runtimes[child.id] = child

	if res := sup.runChildAttempt(context.Background(), child, "", "hello", nil); res.exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", res.exitCode)
	}

	statusAny, err := sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	status, _ := statusAny.(map[string]any)
	if id, _ := status["session_id"].(string); id != realID {
		t.Fatalf("status session_id = %q, want %q", id, realID)
	}

	list, _ := sup.List().([]map[string]any)
	var found bool
	for _, m := range list {
		if m["runtime_id"] != child.id {
			continue
		}
		if id, _ := m["session_id"].(string); id != realID {
			t.Fatalf("list session_id = %q, want %q", id, realID)
		}
		found = true
	}
	if !found {
		t.Fatalf("runtime %q missing from List", child.id)
	}
}

func TestAdoptChildSessionIDIgnoresStaleAttempt(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-adopt-stale.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-stale", "conv-stale-real"
	child, provider := newDeferredChild(t, sup, provisionalID, realID, deferredStartEndEvents(realID, nil))
	sup.runtimes[child.id] = child

	if res := sup.runChildAttempt(context.Background(), child, "", "hello", nil); res.exitCode != 0 {
		t.Fatalf("first attempt exitCode = %d, want 0", res.exitCode)
	}
	// After the attempt completes, simulate a newer active session replacing
	// the child provider/session, then fire the stale adoption callback.
	newProvider := &stableDeferredProvider{
		provisionalID: "agy-pending-new",
		realID:        "conv-new-real",
		backend:       "agy",
		pid:           5555,
		events:        deferredStartEndEvents("conv-new-real", nil),
	}
	child.mu.Lock()
	child.provider = newProvider
	child.session = runtime.Session{SessionID: "agy-pending-new", Backend: "agy", Dir: "/work", PID: 5555}
	child.mu.Unlock()

	// Stale provider + stale expected old id: must NOT overwrite the new session.
	sup.adoptChildSessionID(child, provider, provisionalID, realID)
	child.mu.Lock()
	if child.session.SessionID != "agy-pending-new" {
		t.Fatalf("stale adoption overwrote child session = %q", child.session.SessionID)
	}
	child.mu.Unlock()

	// Matching provider + matching expected old id: updates proceed.
	sup.adoptChildSessionID(child, newProvider, "agy-pending-new", "conv-new-real")
	child.mu.Lock()
	if child.session.SessionID != "conv-new-real" {
		t.Fatalf("matching adoption failed: child session = %q", child.session.SessionID)
	}
	child.mu.Unlock()

	// Empty or same external id is a no-op even when the guard would match.
	sup.adoptChildSessionID(child, newProvider, "conv-new-real", "")
	sup.adoptChildSessionID(child, newProvider, "conv-new-real", "conv-new-real")
	child.mu.Lock()
	if child.session.SessionID != "conv-new-real" {
		t.Fatalf("no-op adoption mutated child session = %q", child.session.SessionID)
	}
	child.mu.Unlock()
}

func TestSessionStartUpdatesChildBeforeEventForwarding(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-adopt-before-forward.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-forward", "conv-forward-real"
	release := make(chan struct{})
	child, _ := newDeferredChild(t, sup, provisionalID, realID, deferredStartEndEvents(realID, release))

	sink := &checkingSink{}
	child.eventWriter = sink
	sup.runtimes[child.id] = child

	resultCh := make(chan childAttemptResult, 1)
	go func() { resultCh <- sup.runChildAttempt(context.Background(), child, "", "hello", nil) }()

	// Wait until the attempt is active (Prompt goroutine subscribed and blocked
	// on the release gate), then assert the provisional identity is still live.
	waitForStableChild(t, child, func(active, completed bool, phase, phaseLabel string) bool { return active })
	child.mu.Lock()
	if id := child.session.SessionID; id != provisionalID {
		t.Fatalf("child session = %q before adoption, want %q", id, provisionalID)
	}
	child.mu.Unlock()

	// When session.start is forwarded, the child must already carry the real id.
	sink.onSessionStart = func() {
		child.mu.Lock()
		defer child.mu.Unlock()
		if child.session.SessionID != realID {
			t.Errorf("child session = %q when session.start forwarded, want %q", child.session.SessionID, realID)
		}
	}
	close(release)

	select {
	case res := <-resultCh:
		if res.exitCode != 0 {
			t.Fatalf("exitCode = %d, want 0", res.exitCode)
		}
		if res.sessionID != realID {
			t.Fatalf("attempt sessionID = %q, want %q", res.sessionID, realID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runChildAttempt did not return")
	}
	if len(sink.events) == 0 || sink.events[0].Event != "session.start" {
		t.Fatalf("first forwarded event = %#v", sink.events)
	}
}