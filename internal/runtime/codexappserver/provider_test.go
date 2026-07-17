package codexappserver

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestCapabilities(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Backend != "codex-app-server" {
		t.Errorf("Backend = %q, want codex-app-server", caps.Backend)
	}
	if !caps.Permissions {
		t.Error("Permissions should be true")
	}
	if !caps.Resume {
		t.Error("Resume should be true")
	}
	if caps.ExternalServerURL {
		t.Error("ExternalServerURL should be false")
	}
	if caps.SubprocessDiscovery {
		t.Error("SubprocessDiscovery should be false")
	}
	if !caps.ModelSelection {
		t.Error("ModelSelection should be true")
	}
}

func TestResumeNoop(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})
	p.threads["th_existing"] = "th_existing"

	sess, err := p.Resume(context.Background(), "th_existing")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if sess.SessionID != "th_existing" {
		t.Errorf("SessionID = %q, want th_existing", sess.SessionID)
	}
	if sess.Backend != "codex-app-server" {
		t.Errorf("Backend = %q", sess.Backend)
	}
	if sess.Dir != "/work" {
		t.Errorf("Dir = %q, want /work", sess.Dir)
	}
}

func TestResumeEmptyID(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	_, err := p.Resume(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty session id")
	}
}

func TestAnswerPermissionNotStarted(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	err := p.AnswerPermission(context.Background(), "ses", "req", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error for not-started provider")
	}
}

func TestAnswerPermissionEmptyRequestID(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, _, _ := fakeClient()
	defer c.Close()
	p.client = c
	err := p.AnswerPermission(context.Background(), "ses", "", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error for empty request id")
	}
	if !strings.Contains(err.Error(), "permission request id is required") {
		t.Fatalf("error = %v, want permission request id validation", err)
	}
}

func TestEventsNotStarted(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	_, err := p.Events(context.Background(), "ses")
	if err == nil {
		t.Fatal("expected error for not-started provider")
	}
}

func TestCancelNoActiveTurn(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	err := p.Cancel(context.Background(), "ses_unknown")
	if err == nil {
		t.Fatal("expected error for no active turn")
	}
}

func TestCloseNotStarted(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	if err := p.Close(); err != nil {
		t.Fatalf("Close on nil client: %v", err)
	}
}

// TestProviderPipeRoundtrip tests the full Prompt flow via pipe simulation.
// This exercises: request routing, response routing, turn waiter notification.
func TestProviderPipeRoundtrip(t *testing.T) {
	c, wOut, rIn := fakeClient()
	defer c.Close()
	p := NewWithOptions(runtime.StartOptions{})
	p.client = c
	p.threads["th_pipe"] = "th_pipe"

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Prompt(context.Background(), "th_pipe", "hello")
	}()

	// Server side: read request, respond with turn ID, then send turn/completed
	// immediately to exercise the buffered completion path.
	msg, err := readLine(rIn)
	if err != nil {
		t.Fatalf("read turn/start: %v", err)
	}

	writeLine(wOut, map[string]any{
		"id": msg.ID,
		"result": map[string]any{
			"turn": map[string]any{"id": "turn_pipe"},
		},
	})
	writeLine(wOut, map[string]any{
		"method": "turn/completed",
		"params": map[string]any{
			"threadId": "th_pipe",
			"turn": map[string]any{
				"id":     "turn_pipe",
				"status": "completed",
			},
		},
	})

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Prompt")
	}
}

// TestEnsureClientReturnsWithoutDeadlock is the regression guard for the
// self-deadlock: ensureClient must not hold p.mu across the client-start call,
// or the double-checked re-lock deadlocks the calling goroutine against itself.
// The pre-fix code hung here forever on the first call.
func TestEnsureClientReturnsWithoutDeadlock(t *testing.T) {
	c, _, _ := fakeClient()
	p := NewWithOptions(runtime.StartOptions{})
	p.startClient = func(context.Context) (*client, error) { return c, nil }

	done := make(chan struct{})
	go func() {
		got, err := p.ensureClient(context.Background())
		if err != nil {
			t.Errorf("ensureClient: %v", err)
		}
		if got != c {
			t.Errorf("ensureClient returned %p, want %p", got, c)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ensureClient deadlocked")
	}
	_ = c.Close()
}

// TestEnsureClientRaceLossClosesLoser verifies that when another goroutine wins
// the start race (sets p.client while this call is starting its own), the losing
// call closes its now-orphaned client instead of leaking the app-server process.
func TestEnsureClientRaceLossClosesLoser(t *testing.T) {
	winner, _, _ := fakeClient()
	defer winner.Close()
	loser, _, _ := fakeClient()

	p := NewWithOptions(runtime.StartOptions{})
	p.startClient = func(context.Context) (*client, error) {
		// Simulate a concurrent winner publishing its client during our start.
		p.mu.Lock()
		p.client = winner
		p.mu.Unlock()
		return loser, nil
	}

	got, err := p.ensureClient(context.Background())
	if err != nil {
		t.Fatalf("ensureClient: %v", err)
	}
	if got != winner {
		t.Fatalf("ensureClient returned %p, want winner %p", got, winner)
	}
	p.mu.Lock()
	stored := p.client
	p.mu.Unlock()
	if stored != winner {
		t.Fatalf("p.client = %p, want winner %p (loser clobbered stored state)", stored, winner)
	}

	// The loser must have been closed: Close() closes its events channel.
	select {
	case _, ok := <-loser.Events():
		if ok {
			t.Fatal("loser events channel delivered an event; want closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loser client was not closed (events channel still open)")
	}
}

// TestEnsureClientStartError verifies the start-error path: the error
// propagates, no client is stored, and — critically — p.mu was released so a
// subsequent call is not deadlocked by a leaked lock. This guards the branch
// whose old error-path unlock was removed by the deadlock fix.
func TestEnsureClientStartError(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	wantErr := errors.New("boom")
	p.startClient = func(context.Context) (*client, error) { return nil, wantErr }

	if _, err := p.ensureClient(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	p.mu.Lock()
	stored := p.client
	p.mu.Unlock()
	if stored != nil {
		t.Fatalf("p.client = %p, want nil after start error", stored)
	}

	// A second call must retry the start and must not deadlock on a leaked lock.
	c, _, _ := fakeClient()
	p.startClient = func(context.Context) (*client, error) { return c, nil }
	done := make(chan *client, 1)
	go func() {
		got, err := p.ensureClient(context.Background())
		if err != nil {
			t.Errorf("second ensureClient: %v", err)
		}
		done <- got
	}()
	select {
	case got := <-done:
		if got != c {
			t.Fatalf("second ensureClient returned %p, want %p", got, c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second ensureClient deadlocked — error path leaked p.mu")
	}
	_ = c.Close()
}

// TestEnsureClientConcurrentSingleWinner exercises real contention: many
// goroutines racing first-use must converge on exactly one retained client,
// every caller sees that same client, and every other started client is closed
// (no orphaned app-server). Run under -race, this covers the production path the
// deterministic race-loss test only simulates.
func TestEnsureClientConcurrentSingleWinner(t *testing.T) {
	const n = 8
	p := NewWithOptions(runtime.StartOptions{})

	var mu sync.Mutex
	var created []*client
	p.startClient = func(context.Context) (*client, error) {
		c, _, _ := fakeClient()
		mu.Lock()
		created = append(created, c)
		mu.Unlock()
		return c, nil
	}

	results := make([]*client, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := p.ensureClient(context.Background())
			if err != nil {
				t.Errorf("ensureClient: %v", err)
			}
			results[i] = c
		}(i)
	}
	wg.Wait()

	p.mu.Lock()
	kept := p.client
	p.mu.Unlock()
	if kept == nil {
		t.Fatal("no client retained")
	}
	for i, c := range results {
		if c != kept {
			t.Errorf("caller %d got %p, want retained %p", i, c, kept)
		}
	}

	// Every started client except the kept one must have been closed.
	mu.Lock()
	defer mu.Unlock()
	for _, c := range created {
		if c == kept {
			continue
		}
		select {
		case _, ok := <-c.Events():
			if ok {
				t.Error("loser delivered an event; want closed")
			}
		case <-time.After(5 * time.Second):
			t.Error("a losing client was not closed (events channel still open)")
		}
	}
	_ = kept.Close()
}

func TestProviderImplementsInterface(t *testing.T) {
	var _ runtime.Provider = (*Provider)(nil)
}
