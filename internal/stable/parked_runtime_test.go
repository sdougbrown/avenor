package stable

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// countingCloseProvider records Close calls so tests can verify that a reaped
// parked runtime tears down its provider (and with it the backend process).
type countingCloseProvider struct {
	*stableScriptedProvider
	mu      sync.Mutex
	closes  int
	closeCh chan struct{}
}

func (p *countingCloseProvider) Close() error {
	p.mu.Lock()
	p.closes++
	p.mu.Unlock()
	if p.closeCh != nil {
		select {
		case p.closeCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func (p *countingCloseProvider) closeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closes
}

func newParkedScriptedProvider(sessionID string, attempts int) *stableScriptedProvider {
	scripts := make([]stableScriptedAttempt, 0, attempts)
	for i := 0; i < attempts; i++ {
		scripts = append(scripts, stableScriptedAttempt{
			sessionID: sessionID,
			events: []stableScriptedEvent{{event: events.Event{
				Event:     "session.end",
				SessionID: sessionID,
				Fields:    map[string]any{"stop_reason": "end_turn"},
			}}},
		})
	}
	return &stableScriptedProvider{attempt: -1, scripts: scripts, emitted: make(chan int, 4)}
}

func waitForActiveRuntimeCount(t *testing.T, sup *Supervisor, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if sup.activeRuntimeCount() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("activeRuntimeCount did not reach %d", want)
}

func fetchChild(t *testing.T, sup *Supervisor, rtID string) *childRuntime {
	t.Helper()
	sup.controlMu.Lock()
	defer sup.controlMu.Unlock()
	child, ok := sup.runtimes[rtID]
	if !ok {
		t.Fatalf("runtime %q not found", rtID)
	}
	return child
}

func childCompleted(child *childRuntime) bool {
	child.mu.Lock()
	defer child.mu.Unlock()
	return child.completed
}

func childParked(child *childRuntime) bool {
	child.mu.Lock()
	defer child.mu.Unlock()
	return child.parked
}

// A runtime parked after a successful end_turn must not count against
// MaxRuntimes: the slot belongs to turns in flight, not to parked sessions.
func TestParkedRuntimeDoesNotCountAgainstMaxRuntimes(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:        newStableSocketPath(t, "parked-not-counted"),
		MaxRuntimes:          1,
		ParkedRuntimeTimeout: 0, // disabled: parked runtimes are kept
		ShutdownTimeout:      0,
	})
	t.Cleanup(func() {
		_ = sup.Shutdown("graceful")
		_ = sup.broker.Stop()
	})
	var parkedSpawnSeq int64
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return newParkedScriptedProvider(fmt.Sprintf("ses_parked_not_counted_%d", atomic.AddInt64(&parkedSpawnSeq, 1)), 2), nil
	}
	dir := t.TempDir()

	first, err := sup.spawn(SpawnParams{Prompt: "hello", Dir: dir})
	if err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	waitForActiveRuntimeCount(t, sup, 0)

	if !childParked(fetchChild(t, sup, first.RuntimeID)) {
		t.Fatal("runtime should be parked after end_turn")
	}

	second, err := sup.spawn(SpawnParams{Prompt: "second", Dir: dir})
	if err != nil {
		t.Fatalf("second spawn against cap of 1 while first is parked: %v", err)
	}
	waitForActiveRuntimeCount(t, sup, 0)

	// With reaping disabled (ParkedRuntimeTimeout 0) no reap timer exists,
	// so the parked runtime must stay alive. Hold a bounded window and
	// sample the completed flag throughout to catch an unconditional reap.
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		if childCompleted(fetchChild(t, sup, first.RuntimeID)) {
			t.Fatal("parked runtime with timeout 0 should not be reaped")
		}
		if !childParked(fetchChild(t, sup, first.RuntimeID)) {
			t.Fatal("parked runtime with timeout 0 should remain parked")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = second
}

// A parked runtime reaps itself after ParkedRuntimeTimeout: it completes,
// tears down its provider, leaves the sentinel at its last turn result, and
// stays addressable as a status tombstone.
func TestParkedRuntimeReapedAfterTimeout(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:        newStableSocketPath(t, "parked-reap"),
		MaxRuntimes:          2,
		ParkedRuntimeTimeout: 100 * time.Millisecond,
		ShutdownTimeout:      0,
	})
	t.Cleanup(func() {
		_ = sup.Shutdown("graceful")
		_ = sup.broker.Stop()
	})
	provider := &countingCloseProvider{
		stableScriptedProvider: newParkedScriptedProvider("ses_parked_reap", 1),
		closeCh:                make(chan struct{}, 1),
	}
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return provider, nil
	}
	sentinel := filepath.Join(t.TempDir(), "sentinel.env")
	dir := t.TempDir()

	result, err := sup.spawn(SpawnParams{Prompt: "hello", Dir: dir, SentinelFile: sentinel})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	child := fetchChild(t, sup, result.RuntimeID)

	waitForActiveRuntimeCount(t, sup, 0)
	sentinelDone := func() bool {
		data, err := os.ReadFile(sentinel)
		return err == nil && strings.HasPrefix(string(data), "DONE\n")
	}
	deadline := time.Now().Add(5 * time.Second)
	for !sentinelDone() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !sentinelDone() {
		t.Fatal("sentinel was not written with the successful turn result")
	}

	select {
	case <-child.done:
	case <-time.After(5 * time.Second):
		t.Fatal("parked runtime was not reaped after its timeout")
	}
	if !childCompleted(child) {
		t.Fatal("reaped runtime should be completed")
	}
	if childParked(child) {
		t.Fatal("reaped runtime should no longer be parked")
	}
	if provider.closeCount() == 0 {
		t.Fatal("reaped runtime did not close its provider")
	}

	// The sentinel keeps the turn result; reaping must not rewrite it as
	// cancelled.
	data, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	if !strings.HasPrefix(string(data), "DONE\n") || !strings.Contains(string(data), "STOP_REASON=end_turn") {
		t.Fatalf("sentinel = %q, want the original done/end_turn result", string(data))
	}

	// The completed runtime remains addressable as a tombstone for status and
	// follow-up identity lookups.
	if fetchChild(t, sup, result.RuntimeID) == nil {
		t.Fatal("reaped runtime must stay in the runtime map as a tombstone")
	}
	if got := sup.activeRuntimeCount(); got != 0 {
		t.Fatalf("activeRuntimeCount = %d, want 0 after reaping", got)
	}
}

// A follow-up prompt delivered inside the grace window resumes the parked
// runtime instead of letting the timeout reap it, and the runtime parks again
// after the resumed turn ends.
func TestParkedRuntimePromptWithinGraceResumes(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: newStableSocketPath(t, "parked-resume"),
		MaxRuntimes:   2,
		// Generous grace: the test's 5s wait deadlines must never race the
		// reap timer on a stalled runner.
		ParkedRuntimeTimeout: 30 * time.Second,
		ShutdownTimeout:      0,
	})
	t.Cleanup(func() {
		_ = sup.Shutdown("graceful")
		_ = sup.broker.Stop()
	})
	provider := newParkedScriptedProvider("ses_parked_resume", 2)
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return provider, nil
	}
	dir := t.TempDir()

	result, err := sup.spawn(SpawnParams{Prompt: "hello", Dir: dir})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	child := fetchChild(t, sup, result.RuntimeID)
	waitForActiveRuntimeCount(t, sup, 0)

	// Drain the initial turn's emissions (openSession and each scripted event
	// step both emit the attempt index) so the next read observes only the
	// resumed turn.
	for len(provider.emitted) > 0 {
		<-provider.emitted
	}

	if err := sup.RuntimePrompt(result.RuntimeID, "follow up", ""); err != nil {
		t.Fatalf("RuntimePrompt: %v", err)
	}

	// The resumed turn runs as scripted attempt 1.
	select {
	case idx := <-provider.emitted:
		if idx != 1 {
			t.Fatalf("resumed attempt index = %d, want 1", idx)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("parked runtime did not resume for the follow-up prompt")
	}

	// After the resumed turn the runtime parks again, not completed.
	waitForActiveRuntimeCount(t, sup, 0)
	if childCompleted(child) {
		t.Fatal("runtime resumed within grace should not be completed")
	}
	if !childParked(child) {
		t.Fatal("runtime should park again after the resumed turn")
	}
}

// RuntimePrompt on a reaped runtime reports that the runtime has ended
// instead of silently queueing a prompt that can never run.
func TestRuntimePromptOnReapedRuntimeFails(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:        newStableSocketPath(t, "parked-prompt-ended"),
		MaxRuntimes:          2,
		ParkedRuntimeTimeout: 100 * time.Millisecond,
		ShutdownTimeout:      0,
	})
	t.Cleanup(func() {
		_ = sup.Shutdown("graceful")
		_ = sup.broker.Stop()
	})
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return newParkedScriptedProvider("ses_parked_prompt_ended", 1), nil
	}
	dir := t.TempDir()

	result, err := sup.spawn(SpawnParams{Prompt: "hello", Dir: dir})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	child := fetchChild(t, sup, result.RuntimeID)
	select {
	case <-child.done:
	case <-time.After(5 * time.Second):
		t.Fatal("parked runtime was not reaped")
	}

	err = sup.RuntimePrompt(result.RuntimeID, "too late", "")
	if err == nil || !strings.Contains(err.Error(), "has ended") {
		t.Fatalf("RuntimePrompt on reaped runtime err = %v, want runtime-has-ended", err)
	}
}

// spawnBlockedOnCapacity proves the local cap still rejects a second runtime
// while a first runtime is mid-turn (not parked), guarding against a fix that
// exempts every non-active runtime from the count.
func TestRunningRuntimeStillCountsAgainstMaxRuntimes(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:        newStableSocketPath(t, "parked-running-counts"),
		MaxRuntimes:          1,
		ParkedRuntimeTimeout: 0,
		ShutdownTimeout:      0,
	})
	t.Cleanup(func() {
		_ = sup.Shutdown("kill")
		_ = sup.broker.Stop()
	})
	release := make(chan struct{})
	var closeOnce sync.Once
	safeClose := func() { closeOnce.Do(func() { close(release) }) }
	t.Cleanup(safeClose)
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return &blockingAdmissionProvider{release: release}, nil
	}
	dir := t.TempDir()

	if _, err := sup.spawn(SpawnParams{Prompt: "hello", Dir: dir}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	// The first runtime is mid-turn and must hold the only slot.
	deadline := time.Now().Add(5 * time.Second)
	for sup.activeRuntimeCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := sup.activeRuntimeCount(); got != 1 {
		t.Fatalf("activeRuntimeCount = %d, want 1 while a turn is running", got)
	}
	_, err := sup.spawn(SpawnParams{Prompt: "second", Dir: dir})
	if err == nil {
		t.Fatal("spawn should fail against the cap while a turn is running")
	}
	safeClose()
}

// A follow-up prompt to a parked runtime must wait for a free local slot:
// parking freed the slot, and a later spawn may have claimed it.
func TestParkedRuntimeResumeWaitsForLocalCapacity(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:        newStableSocketPath(t, "parked-resume-capacity"),
		MaxRuntimes:          1,
		ParkedRuntimeTimeout: 0,
		ShutdownTimeout:      0,
	})
	release := make(chan struct{})
	var closeOnce sync.Once
	safeClose := func() { closeOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		safeClose()
		_ = sup.Shutdown("kill")
		_ = sup.broker.Stop()
	})
	provider := newParkedScriptedProvider("ses_parked_resume_cap", 2)
	var providerCalls int32
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		if atomic.AddInt32(&providerCalls, 1) == 1 {
			return provider, nil
		}
		return &blockingAdmissionProvider{release: release}, nil
	}
	dir := t.TempDir()

	first, err := sup.spawn(SpawnParams{Prompt: "hello", Dir: dir})
	if err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	waitForActiveRuntimeCount(t, sup, 0) // first parks, count 0

	// Claim the freed slot with a second runtime blocked mid-turn.
	if _, err := sup.spawn(SpawnParams{Prompt: "fill", Dir: dir}); err != nil {
		t.Fatalf("second spawn: %v", err)
	}
	waitForActiveRuntimeCount(t, sup, 1)
	for len(provider.emitted) > 0 {
		<-provider.emitted
	}

	if err := sup.RuntimePrompt(first.RuntimeID, "second", ""); err != nil {
		t.Fatalf("RuntimePrompt: %v", err)
	}
	// The resumed turn must not start while the only slot is held.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(provider.emitted) > 0 {
			t.Fatal("resumed turn started while the local cap was full")
		}
		if got := sup.activeRuntimeCount(); got > 1 {
			t.Fatalf("activeRuntimeCount = %d, want <= 1", got)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Releasing the second runtime parks it, freeing the slot; the first
	// runtime's follow-up then proceeds.
	safeClose()
	select {
	case <-provider.emitted:
	case <-time.After(5 * time.Second):
		t.Fatal("parked runtime did not resume after a slot freed")
	}
	if got := sup.activeRuntimeCount(); got > 1 {
		t.Fatalf("activeRuntimeCount = %d, want <= 1 while resumed turn runs", got)
	}
	waitForActiveRuntimeCount(t, sup, 0) // parks again
}

// A held admission reservation claims a local slot just like a running
// runtime, so a parked runtime's follow-up must wait for it and resume once
// the reservation is released.
func TestParkedRuntimeResumeWaitsForHeldReservation(t *testing.T) {
	t.Run("tree budget", func(t *testing.T) { testParkedRuntimeResumeWaitsForHeldReservation(t, false) })
	t.Run("local only", func(t *testing.T) { testParkedRuntimeResumeWaitsForHeldReservation(t, true) })
}

func testParkedRuntimeResumeWaitsForHeldReservation(t *testing.T, localOnly bool) {
	sup := NewSupervisor(Config{
		ControlSocket:        newStableSocketPath(t, "parked-resume-reservation"),
		MaxRuntimes:          1,
		ParkedRuntimeTimeout: 0,
		ShutdownTimeout:      0,
	})
	if localOnly {
		sup.treeBudgetMu.Lock()
		sup.treeBudget = nil
		sup.treeBudgetMu.Unlock()
	}
	t.Cleanup(func() {
		_ = sup.Shutdown("kill")
		_ = sup.broker.Stop()
	})
	provider := newParkedScriptedProvider("ses_parked_resume_res", 2)
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return provider, nil
	}

	first, err := sup.spawn(SpawnParams{Prompt: "hello", Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	waitForActiveRuntimeCount(t, sup, 0) // first parks, count 0
	for len(provider.emitted) > 0 {
		<-provider.emitted
	}

	res, err := sup.reserveAdmission()
	if err != nil {
		t.Fatalf("reserveAdmission: %v", err)
	}
	t.Cleanup(res.Release)

	if err := sup.RuntimePrompt(first.RuntimeID, "second", ""); err != nil {
		t.Fatalf("RuntimePrompt: %v", err)
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(provider.emitted) > 0 {
			t.Fatal("resumed turn started while a reservation held the only local slot")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Well inside WaitForCapacity's 2s poll fallback, so the release itself
	// must wake the waiting resume.
	res.Release()
	select {
	case <-provider.emitted:
	case <-time.After(time.Second):
		t.Fatal("parked runtime did not resume promptly after the reservation was released")
	}
	waitForActiveRuntimeCount(t, sup, 0) // parks again
}

func newWaitTestChild() *childRuntime {
	return &childRuntime{
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}
}

// The parked-timeout timer branch must honor a prompt queued before the
// deadline instead of reaping. Queueing without signaling promptCh leaves the
// wait loop parked in its select, so the timer branch is the path that finds
// the prompt. (A prompt queued via RuntimePrompt would usually be dequeued at
// the top of the loop instead; both paths must return the prompt.)
func TestWaitForNextPromptTimerHonorsQueuedPrompt(t *testing.T) {
	c := newWaitTestChild()
	go func() {
		time.Sleep(10 * time.Millisecond)
		c.mu.Lock()
		c.promptQueue = []string{"boundary prompt"}
		c.mu.Unlock()
	}()
	// The queue is populated well before the timer fires: a scheduling
	// delay must not push the populate past the deadline, or the timer
	// branch would correctly see an empty queue and reap.
	prompt, ok := c.waitForNextPrompt(context.Background(), 2*time.Second)
	if !ok || prompt != "boundary prompt" {
		t.Fatalf("waitForNextPrompt = (%q, %v), want the queued prompt honored", prompt, ok)
	}
	c.mu.Lock()
	completed := c.completed
	c.mu.Unlock()
	if completed {
		t.Fatal("runtime that honored a boundary prompt must not be completed")
	}
}

// With no prompt queued, the timer branch reaps: it marks the runtime
// completed in the same critical section, so later RuntimePrompt calls see
// the runtime ended instead of queueing onto a runtime about to tear down.
func TestWaitForNextPromptTimerReapsWithoutPrompt(t *testing.T) {
	c := newWaitTestChild()
	prompt, ok := c.waitForNextPrompt(context.Background(), 50*time.Millisecond)
	if ok || prompt != "" {
		t.Fatalf("waitForNextPrompt = (%q, %v), want a reap with no prompt", prompt, ok)
	}
	c.mu.Lock()
	completed := c.completed
	c.mu.Unlock()
	if !completed {
		t.Fatal("timer branch must mark the runtime completed so late prompts are rejected")
	}
}
