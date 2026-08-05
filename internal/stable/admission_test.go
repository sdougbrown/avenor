package stable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stderr pipe: %v", err)
	}
	original := os.Stderr
	os.Stderr = writer
	defer func() {
		os.Stderr = original
		_ = reader.Close()
		_ = writer.Close()
	}()

	fn()
	if err := writer.Close(); err != nil {
		t.Fatalf("close stderr writer: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	return string(data)
}

// treeBudgetStatusValues is test-only polling support for the admission suite.
func (s *Supervisor) treeBudgetStatusValues() (active, capacity int, rootID string) {
	s.treeBudgetMu.Lock()
	defer s.treeBudgetMu.Unlock()
	if s.treeBudget == nil {
		return 0, 0, ""
	}
	return s.treeBudget.Status()
}

func newBlockingScriptedSupervisor(t *testing.T, maxRuntimes, maxTreeBudget int) (*Supervisor, chan struct{}, func()) {
	t.Helper()
	socket := newStableSocketPath(t, "admission-block")
	sup := NewSupervisor(Config{
		ControlSocket:   socket,
		MaxRuntimes:     maxRuntimes,
		MaxTreeBudget:   maxTreeBudget,
		ShutdownTimeout: 0,
	})
	if sup.broker == nil {
		t.Fatal("supervisor did not create a broker")
	}
	release := make(chan struct{})
	var closeOnce sync.Once
	safeClose := func() { closeOnce.Do(func() { close(release) }) }
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return &blockingAdmissionProvider{release: release}, nil
	}
	t.Cleanup(func() {
		safeClose()
		_ = sup.broker.Stop()
		sup.stopReaper()
	})
	return sup, release, safeClose
}

// blockingAdmissionProvider starts a session and blocks Prompt until the
// release channel is closed, then emits a session.end event so the turn
// completes and the runtime parks (releasing its admission slot).
type blockingAdmissionProvider struct {
	mu      sync.Mutex
	release chan struct{}
	session string
	eventCh chan events.Event
}

var admissionSessionSeq int64

func (p *blockingAdmissionProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.session = fmt.Sprintf("ses_block_%d", atomic.AddInt64(&admissionSessionSeq, 1))
	p.eventCh = make(chan events.Event, 4)
	return runtime.Session{SessionID: p.session}, nil
}
func (p *blockingAdmissionProvider) Resume(context.Context, string) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.session == "" {
		p.session = fmt.Sprintf("ses_block_%d", atomic.AddInt64(&admissionSessionSeq, 1))
		p.eventCh = make(chan events.Event, 4)
	}
	return runtime.Session{SessionID: p.session}, nil
}
func (p *blockingAdmissionProvider) Prompt(ctx context.Context, sessionID, _ string) error {
	p.mu.Lock()
	ch := p.eventCh
	p.mu.Unlock()
	if ch == nil {
		ch = make(chan events.Event, 4)
		p.mu.Lock()
		p.eventCh = ch
		p.mu.Unlock()
	}
	select {
	case <-p.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case ch <- events.Event{Event: "session.end", SessionID: sessionID, Fields: map[string]any{"stop_reason": "end_turn"}}:
	default:
	}
	close(ch)
	return nil
}
func (p *blockingAdmissionProvider) Cancel(context.Context, string) error { return nil }
func (p *blockingAdmissionProvider) Events(context.Context, string) (<-chan events.Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.eventCh == nil {
		p.eventCh = make(chan events.Event, 4)
	}
	return p.eventCh, nil
}
func (p *blockingAdmissionProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}
func (p *blockingAdmissionProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}
func (p *blockingAdmissionProvider) Close() error { return nil }

// WaitForChildDone blocks until the given runtime reaches terminal completion.
func WaitForChildDone(t *testing.T, sup *Supervisor, rtID string) {
	t.Helper()
	sup.controlMu.Lock()
	child := sup.runtimes[rtID]
	sup.controlMu.Unlock()
	if child == nil {
		return
	}
	select {
	case <-child.done:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not complete in time")
	}
}

// waitTreeActive polls until the tree budget active count matches want, or
// times out.
func waitTreeActive(t *testing.T, sup *Supervisor, want int) {
	t.Helper()
	for i := 0; i < 100; i++ {
		active, _, _ := sup.treeBudgetStatusValues()
		if active == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	active, _, _ := sup.treeBudgetStatusValues()
	t.Fatalf("tree active = %d, want %d", active, want)
}

func TestSpawnTreeBudgetAcquiresAndReleasesOnPark(t *testing.T) {
	sup, _, releaseDone := newBlockingScriptedSupervisor(t, 4, 8)
	defer releaseDone()

	dir := t.TempDir()
	res, err := sup.spawn(SpawnParams{Prompt: "test", Dir: dir})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	waitTreeActive(t, sup, 1)

	// Release the provider so the turn completes. The runtime parks and
	// releases its tree slot.
	releaseDone()
	waitTreeActive(t, sup, 0)

	// The child is parked (not completed). Cancel to terminate it.
	if err := sup.cancelRuntime(res.RuntimeID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	WaitForChildDone(t, sup, res.RuntimeID)
	waitTreeActive(t, sup, 0)
}

func TestSpawnReleaseOnStartupFailure(t *testing.T) {
	socket := newStableSocketPath(t, "admission-fail")
	sup := NewSupervisor(Config{
		ControlSocket: socket,
		MaxRuntimes:   4,
		MaxTreeBudget: 8,
	})
	if sup.broker == nil {
		t.Fatal("supervisor did not create a broker")
	}
	defer func() { _ = sup.broker.Stop(); sup.stopReaper() }()

	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return nil, errors.New("intentional provider failure")
	}
	_, err := sup.spawn(SpawnParams{Prompt: "fail", Dir: t.TempDir()})
	if err == nil {
		t.Fatal("expected spawn failure")
	}
	active, _, _ := sup.treeBudgetStatusValues()
	if active != 0 {
		t.Fatalf("active = %d, want 0 after startup failure", active)
	}
	if got := sup.activeRuntimeCount(); got != 0 {
		t.Fatalf("local active = %d, want 0 after startup failure", got)
	}
}

func TestSpawnReleaseOnLoopFileFailure(t *testing.T) {
	socket := newStableSocketPath(t, "admission-loop-fail")
	sup := NewSupervisor(Config{
		ControlSocket: socket,
		MaxRuntimes:   4,
		MaxTreeBudget: 8,
	})
	defer func() { _ = sup.broker.Stop(); sup.stopReaper() }()

	_, err := sup.spawn(SpawnParams{LoopFile: "/path/does/not/exist.json"})
	if err == nil {
		t.Fatal("expected loop file failure")
	}
	active, _, _ := sup.treeBudgetStatusValues()
	if active != 0 {
		t.Fatalf("active = %d, want 0 after loop failure", active)
	}
	if got := sup.activeRuntimeCount(); got != 0 {
		t.Fatalf("local active = %d, want 0 after loop failure", got)
	}
}

func TestSpawnTreeExhaustionReturnsTreeCapacityError(t *testing.T) {
	sup, _, releaseDone := newBlockingScriptedSupervisor(t, 8, 3)
	defer releaseDone()

	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		if _, err := sup.spawn(SpawnParams{Prompt: "test", Dir: dir}); err != nil {
			t.Fatalf("spawn %d: %v", i, err)
		}
	}
	waitTreeActive(t, sup, 3)
	_, err := sup.spawn(SpawnParams{Prompt: "overflow", Dir: dir})
	if err == nil {
		t.Fatal("expected tree exhaustion error")
	}
	var ce *admission.CapacityError
	if !errors.As(err, &ce) {
		t.Fatalf("error type = %T, want *admission.CapacityError", err)
	}
	if ce.Source != "tree" {
		t.Fatalf("source = %q, want %q", ce.Source, "tree")
	}
	if ce.Limit != 3 || ce.Active != 3 {
		t.Fatalf("CapacityError = %+v, want limit=3 active=3", ce)
	}
	if !ce.Retryable() {
		t.Fatal("should be retryable")
	}
}

func TestSpawnLocalExhaustionReturnsLocalCapacityError(t *testing.T) {
	sup, _, releaseDone := newBlockingScriptedSupervisor(t, 2, 16)
	defer releaseDone()

	dir := t.TempDir()
	for i := 0; i < 2; i++ {
		if _, err := sup.spawn(SpawnParams{Prompt: "test", Dir: dir}); err != nil {
			t.Fatalf("spawn %d: %v", i, err)
		}
	}
	waitTreeActive(t, sup, 2)
	_, err := sup.spawn(SpawnParams{Prompt: "overflow", Dir: dir})
	if err == nil {
		t.Fatal("expected local exhaustion error")
	}
	var ce *admission.CapacityError
	if !errors.As(err, &ce) {
		t.Fatalf("error type = %T, want *admission.CapacityError", err)
	}
	if ce.Source != "local" {
		t.Fatalf("source = %q, want %q", ce.Source, "local")
	}
	if ce.Limit != 2 || ce.Active != 2 {
		t.Fatalf("capacity error = %+v, want limit=2 active=2", ce)
	}
	// Tree budget should not be consumed for a rejected spawn.
	waitTreeActive(t, sup, 2)
}

func TestFanOutAboveLocalDefaultEight(t *testing.T) {
	// The pre-#143 default was 8. Verify a tree budget of 16 allows a fan-out
	// above 8 when the local limit is also raised.
	sup, _, releaseDone := newBlockingScriptedSupervisor(t, 16, 16)
	defer releaseDone()

	dir := t.TempDir()
	for i := 0; i < 12; i++ {
		if _, err := sup.spawn(SpawnParams{Prompt: "test", Dir: dir}); err != nil {
			t.Fatalf("spawn %d: %v", i, err)
		}
	}
	waitTreeActive(t, sup, 12)
}

func TestConcurrentSiblingSpawnsNoOversubscribeTree(t *testing.T) {
	sup, _, releaseDone := newBlockingScriptedSupervisor(t, 64, 20)
	defer releaseDone()

	dir := t.TempDir()
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	tokens := make(chan string, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := sup.spawn(SpawnParams{Prompt: "test", Dir: dir})
			if err != nil {
				errs <- err
				return
			}
			tokens <- res.RuntimeID
		}()
	}
	wg.Wait()
	close(tokens)
	close(errs)

	acquired := 0
	for range tokens {
		acquired++
	}
	rejected := 0
	for err := range errs {
		var ce *admission.CapacityError
		if !errors.As(err, &ce) || ce.Source != "tree" {
			t.Fatalf("unexpected rejection: %v", err)
		}
		rejected++
	}
	if acquired != 20 {
		t.Fatalf("acquired = %d, want 20 (tree budget)", acquired)
	}
	if rejected != 80 {
		t.Fatalf("rejected = %d, want 80", rejected)
	}
	waitTreeActive(t, sup, 20)
}

func TestNestedSupervisorSharesTreeBudget(t *testing.T) {
	// Simulate a nested supervisor opening the same budget file. The nested
	// supervisor's spawns consume from the shared tree capacity.
	root, _, releaseDone := newBlockingScriptedSupervisor(t, 8, 4)
	defer releaseDone()

	// Root holds 2 slots.
	dir := t.TempDir()
	if _, err := root.spawn(SpawnParams{Prompt: "root1", Dir: dir}); err != nil {
		t.Fatalf("root spawn 1: %v", err)
	}
	if _, err := root.spawn(SpawnParams{Prompt: "root2", Dir: dir}); err != nil {
		t.Fatalf("root spawn 2: %v", err)
	}
	waitTreeActive(t, root, 2)

	// Nested supervisor opens the root's budget file.
	nested := NewSupervisor(Config{
		ControlSocket:   newStableSocketPath(t, "nested"),
		MaxRuntimes:     8,
		TreeBudgetFile:  root.TreeBudgetPath(),
		ShutdownTimeout: 0,
	})
	if nested.broker == nil {
		t.Fatal("nested did not create broker")
	}
	defer func() { _ = nested.broker.Stop(); nested.stopReaper() }()
	nestedRelease := make(chan struct{})
	nested.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return &blockingAdmissionProvider{release: nestedRelease}, nil
	}
	defer close(nestedRelease)

	// Nested should be able to acquire the remaining 2 slots.
	if _, err := nested.spawn(SpawnParams{Prompt: "nested1", Dir: dir}); err != nil {
		t.Fatalf("nested spawn 1: %v", err)
	}
	if _, err := nested.spawn(SpawnParams{Prompt: "nested2", Dir: dir}); err != nil {
		t.Fatalf("nested spawn 2: %v", err)
	}

	// A third nested spawn should fail — the shared tree budget is exhausted.
	_, err := nested.spawn(SpawnParams{Prompt: "nested3", Dir: dir})
	if err == nil {
		t.Fatal("expected tree exhaustion across nested supervisor")
	}
	var ce *admission.CapacityError
	if !errors.As(err, &ce) || ce.Source != "tree" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIndependentRootsDoNotShareBudget(t *testing.T) {
	rootA, _, releaseADone := newBlockingScriptedSupervisor(t, 8, 2)
	defer releaseADone()
	rootB, _, releaseBDone := newBlockingScriptedSupervisor(t, 8, 2)
	defer releaseBDone()

	if rootA.TreeBudgetPath() == rootB.TreeBudgetPath() {
		t.Fatal("independent roots share budget path")
	}

	dir := t.TempDir()
	// Exhaust A's budget.
	if _, err := rootA.spawn(SpawnParams{Prompt: "a1", Dir: dir}); err != nil {
		t.Fatalf("A spawn 1: %v", err)
	}
	if _, err := rootA.spawn(SpawnParams{Prompt: "a2", Dir: dir}); err != nil {
		t.Fatalf("A spawn 2: %v", err)
	}
	if _, err := rootA.spawn(SpawnParams{Prompt: "a3", Dir: dir}); err == nil {
		t.Fatal("A should be exhausted")
	}
	// B is independent and unaffected.
	if _, err := rootB.spawn(SpawnParams{Prompt: "b1", Dir: dir}); err != nil {
		t.Fatalf("B spawn should succeed independently: %v", err)
	}
}

func TestReleaseReleasesAfterCancellation(t *testing.T) {
	sup, _, releaseDone := newBlockingScriptedSupervisor(t, 4, 8)
	defer releaseDone()

	res, err := sup.spawn(SpawnParams{Prompt: "test", Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	waitTreeActive(t, sup, 1)
	if err := sup.cancelRuntime(res.RuntimeID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	WaitForChildDone(t, sup, res.RuntimeID)
	waitTreeActive(t, sup, 0)
}

func TestTreeBudgetStatusViaHandler(t *testing.T) {
	sup, _, releaseDone := newBlockingScriptedSupervisor(t, 4, 8)
	defer releaseDone()

	if _, err := sup.spawn(SpawnParams{Prompt: "test", Dir: t.TempDir()}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	waitTreeActive(t, sup, 1)
	status := sup.TreeBudgetStatus().(map[string]any)
	if status["active"] != 1 || status["capacity"] != 8 {
		t.Fatalf("status = %+v, want active=1 capacity=8", status)
	}
	if status["root_id"] == "" {
		t.Fatal("root_id is empty")
	}
	if status["mode"] != "active" {
		t.Fatalf("mode = %v, want active", status["mode"])
	}
}

func TestWaitForCapacityWakesOnRelease(t *testing.T) {
	sup, _, releaseDone := newBlockingScriptedSupervisor(t, 4, 1)
	defer releaseDone()

	if _, err := sup.spawn(SpawnParams{Prompt: "test", Dir: t.TempDir()}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	waitTreeActive(t, sup, 1)
	// Budget is now full. Start a waiter.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- sup.WaitForCapacity(ctx)
	}()

	// Release the slot; the waiter should wake.
	releaseDone()
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("WaitForCapacity: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("WaitForCapacity did not wake on release")
	}
}

func TestDegradedModeNoBudgetStillEnforcesLocal(t *testing.T) {
	// A supervisor whose Avenor-owned runtime-state directory cannot be
	// created runs in degraded mode with only the local limit. Point HOME at a
	// regular file so creating ~/.avenor/sockets fails.
	unwritable := filepath.Join(t.TempDir(), "home-file")
	if err := os.WriteFile(unwritable, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", unwritable)
	var sup *Supervisor
	stderr := captureStderr(t, func() {
		sup = NewSupervisor(Config{
			ControlSocket:   filepath.Join(t.TempDir(), "control.sock"),
			MaxRuntimes:     1,
			MaxTreeBudget:   8,
			ShutdownTimeout: 0,
		})
	})
	if !strings.Contains(stderr, "tree budget unavailable; using degraded local-only mode") {
		t.Fatalf("stderr = %q, want degraded-mode warning", stderr)
	}
	if sup.broker == nil {
		t.Fatal("supervisor did not create a broker")
	}
	defer func() { _ = sup.broker.Stop() }()
	if sup.treeBudget != nil {
		t.Fatal("expected nil tree budget in degraded mode")
	}
	status := sup.TreeBudgetStatus().(map[string]any)
	if status["mode"] != "degraded" || status["reason"] == "" {
		t.Fatalf("status = %+v, want degraded mode with a reason", status)
	}

	// Install a blocking provider and verify the local limit is enforced even
	// without a tree budget.
	release := make(chan struct{})
	var closeOnce sync.Once
	safeClose := func() { closeOnce.Do(func() { close(release) }) }
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return &blockingAdmissionProvider{release: release}, nil
	}
	defer safeClose()

	dir := t.TempDir()
	if _, err := sup.spawn(SpawnParams{Prompt: "test", Dir: dir}); err != nil {
		t.Fatalf("first spawn in degraded mode: %v", err)
	}
	// The local limit is 1; the second spawn must fail with a local capacity
	// error, not a tree error (no tree budget is active).
	_, err := sup.spawn(SpawnParams{Prompt: "overflow", Dir: dir})
	if err == nil {
		t.Fatal("expected local exhaustion error in degraded mode")
	}
	var ce *admission.CapacityError
	if !errors.As(err, &ce) {
		t.Fatalf("error type = %T, want *admission.CapacityError", err)
	}
	if ce.Source != "local" {
		t.Fatalf("source = %q, want %q (degraded mode has no tree budget)", ce.Source, "local")
	}
}

func TestInheritedBudgetFailureReportsDegradedMode(t *testing.T) {
	socket := newStableSocketPath(t, "degraded-tree-budget")
	var sup *Supervisor
	stderr := captureStderr(t, func() {
		sup = NewSupervisor(Config{
			ControlSocket:  socket,
			MaxRuntimes:    1,
			TreeBudgetFile: filepath.Join(t.TempDir(), "missing.tree-budget"),
		})
	})
	defer func() { _ = sup.broker.Stop() }()
	if !strings.Contains(stderr, "tree budget unavailable; using degraded local-only mode") {
		t.Fatalf("stderr = %q, want degraded-mode warning", stderr)
	}

	status := sup.TreeBudgetStatus().(map[string]any)
	if status["mode"] != "degraded" {
		t.Fatalf("mode = %v, want degraded", status["mode"])
	}
	reason, _ := status["reason"].(string)
	if !strings.Contains(reason, "join tree budget") {
		t.Fatalf("reason = %q, want join failure", reason)
	}

	if err := sup.control.Start(socket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer sup.control.Stop()
	cl, err := client.Dial(socket)
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer cl.Close()
	var publicStatus map[string]any
	if err := cl.Call("tree_budget", nil, &publicStatus); err != nil {
		t.Fatalf("tree_budget: %v", err)
	}
	if publicStatus["mode"] != "degraded" || publicStatus["reason"] != reason {
		t.Fatalf("public status = %+v, want degraded join failure", publicStatus)
	}
}

func TestParkedRuntimeReacquiresOnResume(t *testing.T) {
	releaseSecondTurn := make(chan struct{})

	r1Provider := &stableScriptedProvider{
		attempt: -1,
		scripts: []stableScriptedAttempt{
			// First turn completes immediately.
			{sessionID: "ses_r1_first", events: []stableScriptedEvent{{event: events.Event{
				Event: "session.end", SessionID: "ses_r1_first", Fields: map[string]any{"stop_reason": "end_turn"},
			}}}},
			// Second turn (follow-up) blocks until released, then completes.
			{sessionID: "ses_r1_second", events: []stableScriptedEvent{
				{event: events.Event{Event: "agent.status", SessionID: "ses_r1_second", Fields: map[string]any{"phase": "working"}}},
				{event: events.Event{Event: "session.end", SessionID: "ses_r1_second", Fields: map[string]any{"stop_reason": "end_turn"}}, release: releaseSecondTurn},
			}},
		},
	}

	sup := NewSupervisor(Config{
		ControlSocket:   newStableSocketPath(t, "park-resume"),
		MaxRuntimes:     2,
		MaxTreeBudget:   1,
		ShutdownTimeout: 0,
	})
	if sup.broker == nil {
		t.Fatal("supervisor did not create a broker")
	}
	defer func() { _ = sup.broker.Stop(); sup.stopReaper() }()

	var providerCall int32
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		switch atomic.AddInt32(&providerCall, 1) {
		case 1:
			return r1Provider, nil
		default:
			return &blockingAdmissionProvider{release: make(chan struct{})}, nil
		}
	}

	dir := t.TempDir()
	// Spawn R1. Its first turn completes immediately; it parks.
	r1, err := sup.spawn(SpawnParams{Prompt: "first", Dir: dir})
	if err != nil {
		t.Fatalf("spawn R1: %v", err)
	}
	sup.controlMu.Lock()
	r1Child := sup.runtimes[r1.RuntimeID]
	sup.controlMu.Unlock()
	waitForStableChild(t, r1Child, func(active, completed bool, phase, _ string) bool {
		return !active && !completed && phase == "done"
	})
	waitTreeActive(t, sup, 0) // R1 parked, released its tree slot.

	// Spawn R2 to fill the tree budget (capacity 1).
	r2, err := sup.spawn(SpawnParams{Prompt: "fill", Dir: dir})
	if err != nil {
		t.Fatalf("spawn R2: %v", err)
	}
	waitTreeActive(t, sup, 1) // R2 holds the only tree slot.

	// Send a follow-up prompt to R1. It must re-acquire admission before
	// executing. Since the tree is full (R2 holds it), R1 should block.
	if err := sup.RuntimePrompt(r1.RuntimeID, "second", ""); err != nil {
		t.Fatalf("RuntimePrompt: %v", err)
	}

	// R1 should be waiting for capacity, not running its second turn. Poll the
	// tree budget to confirm R1 has not acquired a slot (only R2 holds one).
	// R1's second turn cannot start until R2 releases, so the active count
	// must remain at 1 for a stable window.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		active, _, _ := sup.treeBudgetStatusValues()
		if active != 1 {
			t.Fatalf("tree active = %d, want 1 (R1 should be blocked on re-acquisition)", active)
		}
		time.Sleep(50 * time.Millisecond)
	}
	waitTreeActive(t, sup, 1) // Still just R2.

	// Cancel R2 to free the tree slot. R1 should wake and re-acquire.
	if err := sup.cancelRuntime(r2.RuntimeID); err != nil {
		t.Fatalf("cancel R2: %v", err)
	}
	WaitForChildDone(t, sup, r2.RuntimeID)

	// R1 re-acquires and starts its second turn (phase=working).
	waitForStableChild(t, r1Child, func(active, completed bool, phase, _ string) bool {
		return active && !completed && phase == "working"
	})
	waitTreeActive(t, sup, 1) // R1 now holds the tree slot.

	// Complete R1's second turn.
	close(releaseSecondTurn)
	waitForStableChild(t, r1Child, func(active, completed bool, phase, _ string) bool {
		return !active && !completed && phase == "done"
	})
	waitTreeActive(t, sup, 0) // R1 parked again.

	// Clean up.
	sup.cancelRuntime(r1.RuntimeID)
	WaitForChildDone(t, sup, r1.RuntimeID)
}
func TestSupervisorReaperReclaimsStaleDescendant(t *testing.T) {
	sup, _, _ := newBlockingScriptedSupervisor(t, 8, 4)
	sup.reaperInterval = 10 * time.Millisecond
	sup.startReaper()

	sup.treeBudgetMu.Lock()
	budget := sup.treeBudget
	sup.treeBudgetMu.Unlock()
	if budget == nil {
		t.Fatal("no tree budget")
	}

	// Acquire a token, then corrupt its reservation to a dead PID directly in
	// the budget file so the reaper can reclaim it.
	tok, err := budget.Acquire("stale-descendant")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	active, _, _ := sup.treeBudgetStatusValues()
	if active != 1 {
		t.Fatalf("active = %d, want 1", active)
	}
	_ = tok // We won't release normally; we simulate a crash.

	// Inject a dead PID into the reservation. We use the same low-level file
	// manipulation as the admission package tests to simulate a crashed
	// descendant supervisor that never released its slot.
	injectDeadPID(t, budget, 999999)

	// The supervisor's reaper goroutine should reclaim the stale reservation.
	waitTreeActive(t, sup, 0)
}

// injectDeadPID rewrites all reservations in the budget file to the given PID.
// This is a test-only helper to simulate a crashed supervisor.
func injectDeadPID(t *testing.T, b *admission.Budget, pid int) {
	t.Helper()
	raw := b.RawFile()
	if raw == nil {
		t.Fatal("budget has no raw file")
	}
	if err := flock(raw); err != nil {
		t.Fatalf("flock: %v", err)
	}
	defer funlock(raw)
	if _, err := raw.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}
	data, err := io.ReadAll(raw)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var bf struct {
		RootID   string `json:"root_id"`
		Capacity int    `json:"capacity"`
		Active   map[string]struct {
			PID    int    `json:"pid"`
			Holder string `json:"holder"`
			At     int64  `json:"at"`
			Token  string `json:"token"`
		} `json:"active"`
	}
	if err := json.Unmarshal(data, &bf); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for k := range bf.Active {
		bf.Active[k] = struct {
			PID    int    `json:"pid"`
			Holder string `json:"holder"`
			At     int64  `json:"at"`
			Token  string `json:"token"`
		}{PID: pid, Holder: bf.Active[k].Holder, At: bf.Active[k].At, Token: bf.Active[k].Token}
	}
	out, _ := json.MarshalIndent(&bf, "", "  ")
	if _, err := raw.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if _, err := raw.Write(out); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = raw.Truncate(int64(len(out)))
	_ = raw.Sync()
}

func flock(f *os.File) error   { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX) }
func funlock(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
