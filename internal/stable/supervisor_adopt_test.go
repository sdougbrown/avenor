package stable

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/teamrunner"
)

// adoptChildSessionID preserves the pre-attempt-token test API. Production
// ownership is registered explicitly and never depends on the aggregate child.
func (s *Supervisor) adoptChildSessionID(child *childRuntime, provider runtime.Provider, expectedOldID, externalID string) bool {
	s.sessionIdentityMu.RLock()
	attempt := s.sessionOwners[expectedOldID]
	s.sessionIdentityMu.RUnlock()
	if attempt == nil {
		child.mu.Lock()
		if child.provider != provider || child.session.SessionID != expectedOldID {
			child.mu.Unlock()
			return false
		}
		identity := effectiveIdentity{
			Backend: child.effectiveBackend, Agent: child.effectiveAgent, Model: child.effectiveModel,
			AgentProfile: child.agentProfile, RosterFile: child.rosterFile, RosterEntry: child.rosterEntry,
		}
		child.mu.Unlock()
		if mapped, ok := s.sessionIdentity(expectedOldID); ok {
			identity = mapped
		}
		var err error
		attempt, err = s.registerSessionAttempt(expectedOldID, identity, provider, expectedOldID)
		if err != nil {
			return false
		}
	}
	if attempt.provider != provider {
		return false
	}
	return s.adoptSessionAttempt(child, attempt, expectedOldID, externalID)
}

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

// deferredGatedEvents returns a headless event sequence.
// It gates session.start and session.end independently.
// Tests can inspect the aggregate child SessionID after adoption but before
// phase termination.
func deferredGatedEvents(realID string, startRelease, endRelease <-chan struct{}) []stableScriptedEvent {
	return []stableScriptedEvent{
		{event: events.Event{Event: "session.start", SessionID: realID, Fields: map[string]any{"conversation_id": realID}}, release: startRelease},
		{event: events.Event{Event: "session.end", SessionID: realID, Fields: map[string]any{"stop_reason": "end_turn"}}, release: endRelease},
	}
}

func assertChildSessionID(t *testing.T, child *childRuntime, want string) {
	t.Helper()
	child.mu.Lock()
	got := child.session.SessionID
	child.mu.Unlock()
	if got != want {
		t.Fatalf("child session = %q, want %q", got, want)
	}
}

func assertOnlySessionID(t *testing.T, child *childRuntime, want string) {
	t.Helper()
	child.mu.Lock()
	session := child.session
	child.mu.Unlock()
	if session != (runtime.Session{SessionID: want}) {
		t.Fatalf("cleaned session = %#v, want only SessionID %q", session, want)
	}
}

func waitForChildSessionID(t *testing.T, child *childRuntime, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		child.mu.Lock()
		got := child.session.SessionID
		child.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for child session %q", want)
}

// waitForChildProviderCleared waits for PhaseAttempt deferred cleanup to clear
// the active provider. A nil provider signals phase termination while the
// runtime remains registered with the supervisor.
func waitForChildProviderCleared(t *testing.T, child *childRuntime) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		child.mu.Lock()
		provider := child.provider
		child.mu.Unlock()
		if provider == nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for child provider to be cleared")
}

func statusSessionID(t *testing.T, sup *Supervisor, rtID string) string {
	t.Helper()
	statusAny, err := sup.RuntimeStatus(rtID)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	status, _ := statusAny.(map[string]any)
	id, _ := status["session_id"].(string)
	return id
}

// findForwardedSessionStart returns the first session.start event recorded by
// the sink, skipping the loop/team runner's avenor.*.start envelope events
// that precede it.
func findForwardedSessionStart(t *testing.T, sink *checkingSink) events.Event {
	t.Helper()
	for _, ev := range sink.events {
		if ev.Event == "session.start" {
			return ev
		}
	}
	t.Fatalf("no session.start event forwarded; events=%#v", sink.events)
	return events.Event{}
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

	if id := statusSessionID(t, sup, child.id); id != realID {
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
	assertChildSessionID(t, child, "agy-pending-new")

	// Matching provider + matching expected old id: updates proceed.
	sup.adoptChildSessionID(child, newProvider, "agy-pending-new", "conv-new-real")
	assertChildSessionID(t, child, "conv-new-real")

	// Empty or same external id is a no-op even when the guard would match.
	sup.adoptChildSessionID(child, newProvider, "conv-new-real", "")
	sup.adoptChildSessionID(child, newProvider, "conv-new-real", "conv-new-real")
	assertChildSessionID(t, child, "conv-new-real")
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
	assertChildSessionID(t, child, provisionalID)

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

func TestRunChildSessionIDConflictIsFatalWithRetriesEnabled(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-direct-conflict-fatal.sock", MaxRuntimes: 1})
	sup.rememberSessionIdentity("occupied", effectiveIdentity{Backend: "agy", Agent: "owner"}, nil)
	provider := &stableDeferredProvider{
		provisionalID: "pending-direct-conflict",
		realID:        "occupied",
		backend:       "agy",
		events:        deferredStartEndEvents("occupied", nil),
	}
	sentinelPath := filepath.Join(t.TempDir(), "direct-conflict.env")
	child := &childRuntime{
		id: "rt_direct_conflict", label: "direct-conflict", provider: provider,
		session:     runtime.Session{SessionID: provider.provisionalID, Backend: "agy"},
		eventWriter: stableTestSink{}, sentinelFile: sentinelPath,
		done: make(chan struct{}), promptCh: make(chan struct{}, 1), promptQueue: []string{"must not run"}, runID: sup.runID,
	}
	sup.runtimes[child.id] = child

	sup.runChild(context.Background(), child, "conflict", 0, 3)

	provider.mu.Lock()
	prompts, resumes := provider.prompts, provider.resumes
	provider.mu.Unlock()
	if prompts != 1 || resumes != 0 {
		t.Fatalf("prompts=%d resumes=%d, want one fatal attempt and no provisional resume", prompts, resumes)
	}
	child.mu.Lock()
	exitCode := child.exitCode
	child.mu.Unlock()
	if exitCode != 1 {
		t.Fatalf("exitCode = %d, want 1", exitCode)
	}
	if content := readSentinel(t, sentinelPath); !strings.Contains(content, "STOP_REASON="+runtime.SessionIDConflictStopReason+"\n") {
		t.Fatalf("sentinel = %q, want explicit fatal conflict", content)
	}
}

func TestRunLoopChildSessionIDConflictIsFatalWithRetriesEnabled(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-loop-conflict-fatal.sock", MaxRuntimes: 1})
	sup.rememberSessionIdentity("occupied", effectiveIdentity{Backend: "agy", Agent: "owner"}, nil)
	provider := &stableDeferredProvider{
		provisionalID: "pending-loop-conflict", realID: "occupied", backend: "agy",
		events: deferredStartEndEvents("occupied", nil),
	}
	factoryCalls := 0
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		factoryCalls++
		return provider, nil
	}
	child := &childRuntime{
		id: "rt_loop_conflict", label: "loop-conflict", eventWriter: stableTestSink{},
		done: make(chan struct{}), promptCh: make(chan struct{}, 1), cancelFn: func() {}, runID: sup.runID,
	}
	sup.runtimes[child.id] = child
	cfg := &looprunner.LoopConfig{MaxIterations: 1, Pre: []phaseconfig.Phase{{Name: "conflict", Prompt: "conflict"}}}

	sup.runLoopChild(context.Background(), child, cfg, 3, "", "", "", "", "", "agy")

	provider.mu.Lock()
	prompts, resumes := provider.prompts, provider.resumes
	provider.mu.Unlock()
	if factoryCalls != 1 || prompts != 1 || resumes != 0 {
		t.Fatalf("factory=%d prompts=%d resumes=%d, want one fatal attempt", factoryCalls, prompts, resumes)
	}
	child.mu.Lock()
	exitCode := child.exitCode
	child.mu.Unlock()
	if exitCode != 1 {
		t.Fatalf("exitCode = %d, want 1", exitCode)
	}
}

func TestRunTeamChildSessionIDConflictIsFatalWithRetriesEnabled(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-team-conflict-fatal.sock", MaxRuntimes: 1})
	sup.rememberSessionIdentity("occupied", effectiveIdentity{Backend: "agy", Agent: "owner"}, nil)
	provider := &stableDeferredProvider{
		provisionalID: "pending-team-conflict", realID: "occupied", backend: "agy",
		events: deferredStartEndEvents("occupied", nil),
	}
	factoryCalls := 0
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		factoryCalls++
		return provider, nil
	}
	child := &childRuntime{
		id: "rt_team_conflict", label: "team-conflict", eventWriter: stableTestSink{},
		done: make(chan struct{}), promptCh: make(chan struct{}, 1), cancelFn: func() {}, runID: sup.runID,
	}
	sup.runtimes[child.id] = child
	cfg := &teamrunner.TeamConfig{Pre: []phaseconfig.Phase{{Name: "conflict", Prompt: "conflict"}}}

	sup.runTeamChild(context.Background(), child, cfg, 3, "", "", "", "", "", "agy")

	provider.mu.Lock()
	prompts, resumes := provider.prompts, provider.resumes
	provider.mu.Unlock()
	if factoryCalls != 1 || prompts != 1 || resumes != 0 {
		t.Fatalf("factory=%d prompts=%d resumes=%d, want one fatal attempt", factoryCalls, prompts, resumes)
	}
	child.mu.Lock()
	exitCode := child.exitCode
	child.mu.Unlock()
	if exitCode != 1 {
		t.Fatalf("exitCode = %d, want 1", exitCode)
	}
}

func TestRunLoopChildAdoptsExternalConversationID(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-loop-adopt.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-loop", "conv-loop-real"
	startRelease := make(chan struct{})
	endRelease := make(chan struct{})
	provider := &stableDeferredProvider{
		provisionalID: provisionalID,
		realID:        realID,
		backend:       "agy",
		pid:           7788,
		events:        deferredGatedEvents(realID, startRelease, endRelease),
	}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

	sink := &checkingSink{}
	child := &childRuntime{
		id:          "rt_loop_adopt",
		label:       "loop-adopt",
		provider:    provider,
		session:     runtime.Session{SessionID: provisionalID, Backend: "agy", Dir: "/work", PID: 7788},
		eventWriter: sink,
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		cancelFn:    func() {},
		runID:       sup.runID,
	}
	sup.runtimes[child.id] = child

	cfg := &looprunner.LoopConfig{MaxIterations: 1, Pre: []phaseconfig.Phase{{Name: "work", Prompt: "work"}}}
	go sup.runLoopChild(context.Background(), child, cfg, 0, "", "", "", "", "", "agy")

	// Before session.start is delivered the aggregate child must still hold the
	// provisional id and status must mirror it.
	waitForStableChild(t, child, func(active, completed bool, phase, phaseLabel string) bool { return active })
	assertChildSessionID(t, child, provisionalID)
	if id := statusSessionID(t, sup, child.id); id != provisionalID {
		t.Fatalf("loop status before adoption = %q, want %q", id, provisionalID)
	}

	// Releasing session.start triggers adoption; the child record and status
	// must converge on the real id while the phase is still active.
	close(startRelease)
	waitForChildSessionID(t, child, realID)
	if id := statusSessionID(t, sup, child.id); id != realID {
		t.Fatalf("loop status after adoption = %q, want %q", id, realID)
	}
	child.mu.Lock()
	pid := child.session.PID
	child.mu.Unlock()
	if pid != 7788 {
		t.Fatalf("loop child pid = %d, want 7788", pid)
	}

	close(endRelease)
	waitForStableDone(t, child)

	evt := findForwardedSessionStart(t, sink)
	if evt.SessionID != realID {
		t.Fatalf("loop session.start event = %#v", evt)
	}
}

func TestRunTeamChildAdoptsExternalConversationID(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-team-adopt.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-team", "conv-team-real"
	startRelease := make(chan struct{})
	endRelease := make(chan struct{})
	provider := &stableDeferredProvider{
		provisionalID: provisionalID,
		realID:        realID,
		backend:       "agy",
		pid:           9090,
		events:        deferredGatedEvents(realID, startRelease, endRelease),
	}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

	sink := &checkingSink{}
	child := &childRuntime{
		id:          "rt_team_adopt",
		label:       "team-adopt",
		provider:    provider,
		session:     runtime.Session{SessionID: provisionalID, Backend: "agy", Dir: "/work", PID: 9090},
		eventWriter: sink,
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		cancelFn:    func() {},
		runID:       sup.runID,
	}
	sup.runtimes[child.id] = child

	cfg := &teamrunner.TeamConfig{Team: []phaseconfig.Phase{{Name: "work", Prompt: "work"}}}
	go sup.runTeamChild(context.Background(), child, cfg, 0, "", "", "", "", "", "agy")

	waitForStableChild(t, child, func(active, completed bool, phase, phaseLabel string) bool { return active })
	assertChildSessionID(t, child, provisionalID)
	if id := statusSessionID(t, sup, child.id); id != provisionalID {
		t.Fatalf("team status before adoption = %q, want %q", id, provisionalID)
	}

	close(startRelease)
	waitForChildSessionID(t, child, realID)
	if id := statusSessionID(t, sup, child.id); id != realID {
		t.Fatalf("team status after adoption = %q, want %q", id, realID)
	}
	child.mu.Lock()
	pid := child.session.PID
	child.mu.Unlock()
	if pid != 9090 {
		t.Fatalf("team child pid = %d, want 9090", pid)
	}

	close(endRelease)
	waitForStableDone(t, child)

	evt := findForwardedSessionStart(t, sink)
	if evt.SessionID != realID {
		t.Fatalf("team session.start event = %#v", evt)
	}
}

func TestStableAdoptionDoesNotOverwriteNewerConcurrentAttempt(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-adopt-concurrent.sock", MaxRuntimes: 1})
	child := &childRuntime{
		id:       "rt_concurrent",
		label:    "concurrent",
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
		runID:    sup.runID,
	}
	sup.runtimes[child.id] = child

	older := &stableDeferredProvider{provisionalID: "agy-pending-old", realID: "conv-old-real", backend: "agy", pid: 1111, events: deferredStartEndEvents("conv-old-real", nil)}
	newer := &stableDeferredProvider{provisionalID: "agy-pending-new", realID: "conv-new-real", backend: "agy", pid: 2222, events: deferredStartEndEvents("conv-new-real", nil)}

	// The newer attempt has already become the active child session.
	child.mu.Lock()
	child.provider = newer
	child.session = runtime.Session{SessionID: "agy-pending-new", Backend: "agy", Dir: "/work", PID: 2222}
	child.mu.Unlock()

	var wg sync.WaitGroup
	lateRelease := make(chan struct{})
	// Older attempt's adoption callback fires late, after the newer attempt
	// already updated the aggregate child record.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-lateRelease
		sup.adoptChildSessionID(child, older, "agy-pending-old", "conv-old-real")
	}()
	// Newer attempt's adoption fires first and updates the child.
	sup.adoptChildSessionID(child, newer, "agy-pending-new", "conv-new-real")
	assertChildSessionID(t, child, "conv-new-real")

	close(lateRelease)
	wg.Wait()

	assertChildSessionID(t, child, "conv-new-real")
}

// readSentinel returns the sentinel file content, failing the test if the file
// is missing or empty. Used to assert the terminal sentinel carries the
// adopted conversation id on the normal successful loop/team path.
func readSentinel(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sentinel %q: %v", path, err)
	}
	if len(data) == 0 {
		t.Fatalf("sentinel %q is empty", path)
	}
	return string(data)
}

// assertSentinelHasSession asserts the sentinel content carries the expected
// conversation id on the SESSION= line and the expected status banner.
func assertSentinelHasSession(t *testing.T, content, banner, sessionID string) {
	t.Helper()
	if !strings.HasPrefix(content, banner+"\n") {
		t.Fatalf("sentinel = %q, want banner %q", content, banner)
	}
	if !strings.Contains(content, "SESSION="+sessionID+"\n") {
		t.Fatalf("sentinel = %q, want SESSION=%s", content, sessionID)
	}
}

// TestRunLoopChildPreservesAdoptedSessionAfterPhaseCleanup proves loop
// PhaseAttempt cleanup preserves the authoritative conversation ID in the
// aggregate child record after the phase terminates.
func TestRunLoopChildPreservesAdoptedSessionAfterPhaseCleanup(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-loop-keep-session.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-loop-keep", "conv-loop-keep"
	startRelease := make(chan struct{})
	endRelease := make(chan struct{})
	provider := &stableDeferredProvider{
		provisionalID: provisionalID,
		realID:        realID,
		backend:       "agy",
		pid:           7117,
		events:        deferredGatedEvents(realID, startRelease, endRelease),
	}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

	child := &childRuntime{
		id:          "rt_loop_keep",
		label:       "loop-keep",
		provider:    provider,
		session:     runtime.Session{SessionID: provisionalID, Backend: "agy", Dir: "/work", PID: 7117},
		eventWriter: stableTestSink{},
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		cancelFn:    func() {},
		runID:       sup.runID,
	}
	sup.runtimes[child.id] = child

	cfg := &looprunner.LoopConfig{MaxIterations: 1, Pre: []phaseconfig.Phase{{Name: "work", Prompt: "work"}}}
	go sup.runLoopChild(context.Background(), child, cfg, 0, "", "", "", "", "", "agy")

	waitForStableChild(t, child, func(active, completed bool, phase, phaseLabel string) bool { return active })
	close(startRelease)
	waitForChildSessionID(t, child, realID)

	// Releasing session.end terminates the phase; the deferred cleanup runs
	// before runLoopChild returns. The adopted id must survive cleanup.
	close(endRelease)
	waitForChildProviderCleared(t, child)

	waitForStableDone(t, child)

	assertChildSessionID(t, child, realID)
	assertOnlySessionID(t, child, realID)
}

// TestRunTeamChildPreservesAdoptedSessionAfterPhaseCleanup is the team analogue
// of the loop cleanup-preservation test.
func TestRunTeamChildPreservesAdoptedSessionAfterPhaseCleanup(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-team-keep-session.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-team-keep", "conv-team-keep"
	startRelease := make(chan struct{})
	endRelease := make(chan struct{})
	provider := &stableDeferredProvider{
		provisionalID: provisionalID,
		realID:        realID,
		backend:       "agy",
		pid:           8228,
		events:        deferredGatedEvents(realID, startRelease, endRelease),
	}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

	child := &childRuntime{
		id:          "rt_team_keep",
		label:       "team-keep",
		provider:    provider,
		session:     runtime.Session{SessionID: provisionalID, Backend: "agy", Dir: "/work", PID: 8228},
		eventWriter: stableTestSink{},
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		cancelFn:    func() {},
		runID:       sup.runID,
	}
	sup.runtimes[child.id] = child

	cfg := &teamrunner.TeamConfig{Team: []phaseconfig.Phase{{Name: "work", Prompt: "work"}}}
	go sup.runTeamChild(context.Background(), child, cfg, 0, "", "", "", "", "", "agy")

	waitForStableChild(t, child, func(active, completed bool, phase, phaseLabel string) bool { return active })
	close(startRelease)
	waitForChildSessionID(t, child, realID)

	close(endRelease)
	waitForChildProviderCleared(t, child)

	waitForStableDone(t, child)

	assertChildSessionID(t, child, realID)
	assertOnlySessionID(t, child, realID)
}

// TestRunLoopChildSentinelCarriesAdoptedSessionOnNormalSuccess proves that a
// normal successful loop writes the adopted conversation ID to its terminal
// sentinel. The aggregate RunResult has no SessionID on the max_iterations path.
func TestRunLoopChildSentinelCarriesAdoptedSessionOnNormalSuccess(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-loop-sentinel.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-loop-sent", "conv-loop-sent"
	provider := &stableDeferredProvider{
		provisionalID: provisionalID,
		realID:        realID,
		backend:       "agy",
		pid:           7001,
		events:        deferredStartEndEvents(realID, nil),
	}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

	sentinelPath := filepath.Join(t.TempDir(), "loop-sentinel.env")
	child := &childRuntime{
		id:           "rt_loop_sent",
		label:        "loop-sent",
		provider:     provider,
		session:      runtime.Session{SessionID: provisionalID, Backend: "agy", Dir: "/work", PID: 7001},
		eventWriter:  stableTestSink{},
		sentinelFile: sentinelPath,
		done:         make(chan struct{}),
		promptCh:     make(chan struct{}, 1),
		cancelFn:     func() {},
		runID:        sup.runID,
	}
	sup.runtimes[child.id] = child

	cfg := &looprunner.LoopConfig{MaxIterations: 1, Pre: []phaseconfig.Phase{{Name: "work", Prompt: "work"}}}
	sup.runLoopChild(context.Background(), child, cfg, 0, "", "", "", "", "", "agy")

	waitForStableDone(t, child)
	assertSentinelHasSession(t, readSentinel(t, sentinelPath), "DONE", realID)
}

// TestRunTeamChildSentinelCarriesAdoptedSessionOnNormalSuccess is the team
// analogue of the loop sentinel test.
func TestRunTeamChildSentinelCarriesAdoptedSessionOnNormalSuccess(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-team-sentinel.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-team-sent", "conv-team-sent"
	provider := &stableDeferredProvider{
		provisionalID: provisionalID,
		realID:        realID,
		backend:       "agy",
		pid:           8002,
		events:        deferredStartEndEvents(realID, nil),
	}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }

	sentinelPath := filepath.Join(t.TempDir(), "team-sentinel.env")
	child := &childRuntime{
		id:           "rt_team_sent",
		label:        "team-sent",
		provider:     provider,
		session:      runtime.Session{SessionID: provisionalID, Backend: "agy", Dir: "/work", PID: 8002},
		eventWriter:  stableTestSink{},
		sentinelFile: sentinelPath,
		done:         make(chan struct{}),
		promptCh:     make(chan struct{}, 1),
		cancelFn:     func() {},
		runID:        sup.runID,
	}
	sup.runtimes[child.id] = child

	cfg := &teamrunner.TeamConfig{Team: []phaseconfig.Phase{{Name: "work", Prompt: "work"}}}
	sup.runTeamChild(context.Background(), child, cfg, 0, "", "", "", "", "", "agy")

	waitForStableDone(t, child)
	assertSentinelHasSession(t, readSentinel(t, sentinelPath), "DONE", realID)
}

// TestRunTeamChildStaleAttemptCleanupDoesNotEraseNewerSession proves that
// stale team cleanup preserves a newer child session.
// A newer attempt replaces the provider before deferred cleanup runs.
// The cleanup guard compares runtime.Provider values. A mismatch leaves the
// newer provider and session untouched.
func TestRunTeamChildStaleAttemptCleanupDoesNotEraseNewerSession(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-team-stale-cleanup.sock", MaxRuntimes: 1})
	const provisionalID, realID = "agy-pending-team-stale", "conv-team-stale"
	startRelease := make(chan struct{})
	endRelease := make(chan struct{})
	older := &stableDeferredProvider{
		provisionalID: provisionalID,
		realID:        realID,
		backend:       "agy",
		pid:           6336,
		events:        deferredGatedEvents(realID, startRelease, endRelease),
	}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return older, nil }

	child := &childRuntime{
		id:          "rt_team_stale",
		label:       "team-stale",
		provider:    older,
		session:     runtime.Session{SessionID: provisionalID, Backend: "agy", Dir: "/work", PID: 6336},
		eventWriter: stableTestSink{},
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		cancelFn:    func() {},
		runID:       sup.runID,
	}
	sup.runtimes[child.id] = child

	cfg := &teamrunner.TeamConfig{Team: []phaseconfig.Phase{{Name: "work", Prompt: "work"}}}
	go sup.runTeamChild(context.Background(), child, cfg, 0, "", "", "", "", "", "agy")

	// Wait until the older attempt is active and has adopted the real id.
	waitForStableChild(t, child, func(active, completed bool, phase, phaseLabel string) bool { return active })
	close(startRelease)
	waitForChildSessionID(t, child, realID)

	// Simulate a newer concurrent attempt taking over the aggregate child
	// record while the older attempt's phase is still running (before its
	// deferred cleanup runs).
	newer := &stableDeferredProvider{
		provisionalID: "agy-pending-team-newer",
		realID:        "conv-team-newer",
		backend:       "agy",
		pid:           9449,
		events:        deferredStartEndEvents("conv-team-newer", nil),
	}
	child.mu.Lock()
	child.provider = newer
	child.session = runtime.Session{SessionID: "conv-team-newer", Backend: "agy", Dir: "/work", PID: 9449}
	child.mu.Unlock()

	// Releasing session.end terminates the older attempt; its deferred
	// cleanup guard (child.provider == older) is now false, so the newer
	// provider/session must survive untouched.
	close(endRelease)
	waitForStableDone(t, child)

	// Workflow finalization is authoritative over the transient aggregate slot:
	// the configured phase completed with the older provider's adopted session.
	assertChildSessionID(t, child, realID)
	child.mu.Lock()
	provider := child.provider
	pid := child.session.PID
	child.mu.Unlock()
	if provider != nil || pid != 0 {
		t.Fatalf("completed workflow retained live provider state: provider=%v pid=%d", provider, pid)
	}
}
