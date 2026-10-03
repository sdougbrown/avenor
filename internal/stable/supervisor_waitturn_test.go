package stable

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// waitTurnTestProvider scripts turns whose session.end events carry
// stop_reason and the complete final_output. Each attempt can gate its
// session.end on a release channel so a test controls exactly when the turn
// settles; a nil gate settles the turn as soon as the attempt runs.
type waitTurnTestProvider struct {
	mu       sync.Mutex
	attempt  int
	gates    []chan struct{}
	channels map[string]chan events.Event
}

func (p *waitTurnTestProvider) open() (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempt++
	index := p.attempt - 1
	sessionID := fmt.Sprintf("ses_turn%d", index)
	ch := make(chan events.Event, 2)
	ch <- events.Event{Event: "agent.status", SessionID: sessionID, Fields: map[string]any{"phase": "working"}}
	end := events.Event{Event: "session.end", SessionID: sessionID, Fields: map[string]any{
		"stop_reason":  "end_turn",
		"final_output": fmt.Sprintf("turn %d output", index),
	}}
	if index < len(p.gates) && p.gates[index] != nil {
		release := p.gates[index]
		// Hold the turn in flight until the gate closes, mimicking an
		// in-flight turn the test controls.
		go func() {
			select {
			case <-release:
			case <-time.After(30 * time.Second):
			}
			ch <- end
			close(ch)
		}()
	} else {
		ch <- end
		close(ch)
	}
	if p.channels == nil {
		p.channels = make(map[string]chan events.Event)
	}
	p.channels[sessionID] = ch
	return runtime.Session{SessionID: sessionID}, nil
}

func (p *waitTurnTestProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	return p.open()
}

func (p *waitTurnTestProvider) Resume(context.Context, string) (runtime.Session, error) {
	return p.open()
}

func (p *waitTurnTestProvider) Prompt(context.Context, string, string) error { return nil }

func (p *waitTurnTestProvider) Events(_ context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch := p.channels[sessionID]
	if ch == nil {
		return nil, fmt.Errorf("no event channel for %s", sessionID)
	}
	return ch, nil
}

func (p *waitTurnTestProvider) Cancel(context.Context, string) error { return nil }

func (p *waitTurnTestProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}

func (p *waitTurnTestProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

func startWaitTurnSupervisor(t *testing.T, provider *waitTurnTestProvider) (*client.Client, *Supervisor) {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "ctrl.sock")
	sup := NewSupervisor(Config{ControlSocket: socketPath, MaxRuntimes: 10})
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		return provider, nil
	}
	if err := sup.control.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(func() { sup.control.Stop() })

	c, err := client.Dial(socketPath)
	if err != nil {
		t.Fatalf("dial control socket: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, sup
}

// waitForTurnWaiterRegistered polls until the runtime has at least one
// wait_turn waiter registered, proving the server-side wait is parked before
// the test triggers a settle.
func waitForTurnWaiterRegistered(t *testing.T, sup *Supervisor, runtimeID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		sup.controlMu.Lock()
		child := sup.runtimes[runtimeID]
		sup.controlMu.Unlock()
		if child != nil {
			child.mu.Lock()
			registered := len(child.turnWaiters) > 0
			child.mu.Unlock()
			if registered {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("wait_turn waiter never registered for runtime %s", runtimeID)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func spawnWaitTurnRuntime(t *testing.T, c *client.Client) string {
	t.Helper()
	spawnResult, err := c.Spawn(map[string]any{"prompt": "turn 0", "dir": ".", "backend": "pony"})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	runtimeID, _ := spawnResult["runtime_id"].(string)
	if runtimeID == "" {
		t.Fatalf("spawn result missing runtime_id: %v", spawnResult)
	}
	return runtimeID
}

func waitForControlStatus(t *testing.T, c *client.Client, runtimeID, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, err := c.Status(runtimeID)
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		if status["status"] == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime %s status never became %q (last: %v)", runtimeID, want, status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Acceptance: wait_turn over an idle runtime must not return on the
// pre-existing idle state. It covers the next prompted turn and reports that
// turn's stop_reason, complete final_output, and session_id.
func TestControlWaitTurnCoversQueuedPromptTurn(t *testing.T) {
	c, _ := startWaitTurnSupervisor(t, &waitTurnTestProvider{gates: []chan struct{}{nil, nil}})
	runtimeID := spawnWaitTurnRuntime(t, c)
	waitForControlStatus(t, c, runtimeID, "idle")

	waited := make(chan map[string]any, 1)
	waitErr := make(chan error, 1)
	go func() {
		result, err := c.WaitTurn(runtimeID, 30*time.Second)
		waited <- result
		waitErr <- err
	}()

	// The pre-existing idle state must not satisfy the wait.
	select {
	case result := <-waited:
		t.Fatalf("wait_turn returned on pre-existing idle state: %v", result)
	case err := <-waitErr:
		t.Fatalf("wait_turn returned on pre-existing idle state: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	if err := c.Prompt(runtimeID, "follow up"); err != nil {
		t.Fatalf("prompt: %v", err)
	}
	var result map[string]any
	select {
	case result = <-waited:
		if err := <-waitErr; err != nil {
			t.Fatalf("wait_turn: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("wait_turn did not return after the prompted turn settled")
	}
	if result["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason = %v, want end_turn", result["stop_reason"])
	}
	if result["final_output"] != "turn 1 output" {
		t.Errorf("final_output = %v, want turn 1 output", result["final_output"])
	}
	if result["session_id"] != "ses_turn1" {
		t.Errorf("session_id = %v, want ses_turn1", result["session_id"])
	}
}

// Acceptance: wait_turn called while the runtime is executing must cover the
// in-flight turn, not skip ahead to a later one.
func TestControlWaitTurnCoversInFlightTurn(t *testing.T) {
	release := make(chan struct{})
	c, sup := startWaitTurnSupervisor(t, &waitTurnTestProvider{gates: []chan struct{}{release}})
	runtimeID := spawnWaitTurnRuntime(t, c)
	waitForControlStatus(t, c, runtimeID, "running")

	waited := make(chan map[string]any, 1)
	waitErr := make(chan error, 1)
	go func() {
		result, err := c.WaitTurn(runtimeID, 30*time.Second)
		waited <- result
		waitErr <- err
	}()

	// Wait until the server-side waiter is parked before releasing the turn,
	// so the settle cannot race the registration.
	waitForTurnWaiterRegistered(t, sup, runtimeID)
	close(release)

	var result map[string]any
	select {
	case result = <-waited:
		if err := <-waitErr; err != nil {
			t.Fatalf("wait_turn: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("wait_turn did not return after the in-flight turn settled")
	}
	if result["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason = %v, want end_turn", result["stop_reason"])
	}
	if result["final_output"] != "turn 0 output" {
		t.Errorf("final_output = %v, want turn 0 output", result["final_output"])
	}
	if result["session_id"] != "ses_turn0" {
		t.Errorf("session_id = %v, want ses_turn0", result["session_id"])
	}
}

// Acceptance: timeout_ms bounds the wait and a timeout surfaces as a typed
// error, not a stuck call.
func TestControlWaitTurnTimesOutWithTypedError(t *testing.T) {
	release := make(chan struct{}) // never closed: the turn never settles
	c, _ := startWaitTurnSupervisor(t, &waitTurnTestProvider{gates: []chan struct{}{release}})
	runtimeID := spawnWaitTurnRuntime(t, c)
	waitForControlStatus(t, c, runtimeID, "running")

	_, err := c.WaitTurn(runtimeID, 150*time.Millisecond)
	if !errors.Is(err, client.ErrWaitTurnTimeout) {
		t.Fatalf("WaitTurn error = %v, want client.ErrWaitTurnTimeout", err)
	}
	// The connection must remain usable after the timed-out wait.
	if err := c.Cancel(runtimeID); err != nil {
		t.Fatalf("cancel after timeout: %v", err)
	}
}
