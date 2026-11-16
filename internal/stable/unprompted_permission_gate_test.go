package stable

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// TestUnpromptedPermissionRequestGate drives the publication sequence the
// unprompted path shares with the turn loop, and covers the three refusals only
// that path adds: a closing child prepares no claim, a shutting-down supervisor
// prepares no claim (nothing could deliver the answer), and a request whose ID
// another claim still holds is not published, so poo's held replay cannot
// republish a live request invisibly.
func TestUnpromptedPermissionRequestGate(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "idle-gate"), MaxRuntimes: 1})
	request := events.Event{
		Event:     "permission.request",
		SessionID: "session",
		Fields:    map[string]any{"request_id": "a1", "options": []any{map[string]any{"optionId": "allow"}}},
	}
	claimed := func(requestID string) control.PermissionAnswerDelivery {
		return sup.control.DeliverPendingPermission("rt_idle", requestID, "allow", "")
	}
	for _, tc := range []struct {
		name         string
		closing      bool
		shuttingDown bool
		held         bool
		want         bool
		// wantDelivery is what a later answer must see: NoResolver when the
		// unprompted path claimed the request, NotFound when it prepared nothing.
		// A foreign claim keeps its own state, which is never NoResolver.
		wantDelivery control.PermissionAnswerDelivery
		// wantHeld asserts only that a foreign claim was not replaced.
		wantHeld bool
	}{
		{name: "unprompted claims a free request", want: true, wantDelivery: control.PermissionAnswerNoResolver},
		{name: "closing child prepares nothing", closing: true, want: false, wantDelivery: control.PermissionAnswerNotFound},
		{name: "shutting-down supervisor prepares nothing", shuttingDown: true, want: false, wantDelivery: control.PermissionAnswerNotFound},
		{name: "a held request is not republished", held: true, want: false, wantHeld: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh child per case: the closing state must not leak.
			child := &childRuntime{id: "rt_idle", lifecycleCtx: context.Background()}
			requestID := "a1"
			if tc.held {
				// A control resolver already owns this ID: the canonical path
				// refuses to publish a second request over it.
				if !sup.control.PreparePermissionClaim(child.id, requestID, control.PermissionResolverControl, nil) {
					t.Fatal("pre-claim was refused")
				}
			}
			if tc.closing {
				child.lc = lifecycleState{shutdown: shutdownRequested}
			}
			if tc.shuttingDown {
				sup.controlMu.Lock()
				sup.shuttingDown = true
				sup.controlMu.Unlock()
				t.Cleanup(func() {
					sup.controlMu.Lock()
					sup.shuttingDown = false
					sup.controlMu.Unlock()
				})
			}
			publisher := sup.unpromptedPublisher(child, "session", nil)
			// The gate is what this test measures; the log write is not its subject.
			publisher.Write = func(events.Event) error { return nil }
			_, _, err := publisher.Publish(request)
			if publish := err == nil; publish != tc.want {
				t.Fatalf("publish = %v (err %v), want %v", publish, err, tc.want)
			}
			if tc.closing || tc.shuttingDown {
				// A child that cannot answer is not a claim conflict, and
				// saying so would send an operator after the wrong owner.
				if errors.Is(err, cli.ErrPermissionClaimConflict) {
					t.Fatalf("closing drop reported as a claim conflict: %v", err)
				}
			}
			if tc.held && !errors.Is(err, cli.ErrPermissionClaimConflict) {
				t.Fatalf("held request error = %v, want ErrPermissionClaimConflict", err)
			}
			got := claimed(requestID)
			if tc.wantHeld {
				if got == control.PermissionAnswerNoResolver {
					t.Fatalf("delivery state = NoResolver: the unprompted path replaced a held claim")
				}
			} else if got != tc.wantDelivery {
				t.Fatalf("delivery state = %v, want %v", got, tc.wantDelivery)
			}
			sup.control.EndPermissionClaim(child.id, requestID)
		})
	}
}

// TestOutOfTurnCancelLeavesTheRequestAnswerable covers the window an attempt
// start opens: cancelling the out-of-turn resolver hands its claim off, and the
// ask poo still holds must stay answerable until the attempt's subscription
// takes it over. Without the restore, a direct answer in that window fails as a
// request with no registered resolver state.
//
// A client is attached on purpose. A claim with no client is signalled as
// disconnected, and that signal is answered by the fallback branch, which keeps
// the claim whether or not the cancel path restores it.
func TestOutOfTurnCancelLeavesTheRequestAnswerable(t *testing.T) {
	socket := newStableSocketPath(t, "out-of-turn-cancel")
	sup := newTestSupervisor(t, Config{ControlSocket: socket, MaxRuntimes: 1})
	if err := sup.control.Start(socket); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sup.control.Stop() })
	conn, err := client.Dial(socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Identity(); err != nil {
		t.Fatal(err)
	}
	attachOwnerForTest(t, conn, socket)

	child := &childRuntime{id: "rt_cancel", lifecycleCtx: context.Background()}
	options := []any{map[string]any{"optionId": "allow"}}
	request := events.Event{
		Event:     "permission.request",
		SessionID: "session",
		Fields:    map[string]any{"request_id": "a1", "options": options},
	}
	// The state a client resolver waits in, which is where a cancel reaches the
	// handoff branch.
	if !sup.control.PreparePermissionClaim(child.id, "a1", control.PermissionResolverReserved, options) {
		t.Fatal("claim was refused")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cli.ResolvePermissionOutOfTurn(ctx, cli.OutOfTurnPermission{
		ControlServer: sup.control,
		Stderr:        io.Discard,
		SessionID:     "session",
		ClaimScope:    child.id,
	}, request); err != nil {
		t.Fatalf("resolve out of turn: %v", err)
	}
	if got := sup.control.DeliverPendingPermission(child.id, "a1", "allow", ""); got != control.PermissionAnswerNoResolver {
		t.Fatalf("delivery state = %v, want NoResolver: the claim did not survive the cancel", got)
	}
}

// TestOutOfTurnCancelOnAClosingChildRestoresNothing covers the same cancel for a
// child that has started closing: the restore goes through the admission this
// path publishes under, so it is refused like any other claim on a closing
// child and no claim is left that nothing owns.
func TestOutOfTurnCancelOnAClosingChildRestoresNothing(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "cancel-closing"), MaxRuntimes: 1})
	child := &childRuntime{id: "rt_cancel_closing", lifecycleCtx: context.Background(), lc: lifecycleState{shutdown: shutdownRequested}}
	options := []any{map[string]any{"optionId": "allow"}}
	request := events.Event{
		Event:     "permission.request",
		SessionID: "session",
		Fields:    map[string]any{"request_id": "a1", "options": options},
	}
	if !sup.control.PreparePermissionClaim(child.id, "a1", control.PermissionResolverReserved, options) {
		t.Fatal("claim was refused")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cli.ResolvePermissionOutOfTurn(ctx, cli.OutOfTurnPermission{
		ControlServer: sup.control,
		Stderr:        io.Discard,
		SessionID:     "session",
		ClaimScope:    child.id,
		RestoreClaim: func(requestID string, options []any) bool {
			return sup.claimUnpromptedRequest(child)(requestID, control.PermissionResolverNoResolver, options) == nil
		},
	}, request); err != nil {
		t.Fatalf("resolve out of turn: %v", err)
	}
	if got := sup.control.DeliverPendingPermission(child.id, "a1", "allow", ""); got != control.PermissionAnswerNotFound {
		t.Fatalf("delivery state = %v, want NotFound: a claim was restored on a child that cannot answer", got)
	}
}

// TestPublisherReleasesAClaimItsWriteFailed covers the shared publisher's one
// failure that would otherwise leak: a request that never reached the log keeps
// no claim, so a reused request id is not refused forever.
func TestPublisherReleasesAClaimItsWriteFailed(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "publish-write-fail"), MaxRuntimes: 1})
	child := &childRuntime{id: "rt_write", lifecycleCtx: context.Background()}
	request := events.Event{
		Event:     "permission.request",
		SessionID: "session",
		Fields:    map[string]any{"request_id": "a1", "options": []any{map[string]any{"optionId": "allow"}}},
	}
	publisher := sup.unpromptedPublisher(child, "session", nil)
	publisher.Write = func(events.Event) error { return errors.New("log is closed") }
	if _, _, err := publisher.Publish(request); !errors.Is(err, cli.ErrPermissionNotWritten) {
		t.Fatalf("publish error = %v, want ErrPermissionNotWritten", err)
	}
	if got := sup.control.DeliverPendingPermission(child.id, "a1", "allow", ""); got != control.PermissionAnswerNotFound {
		t.Fatalf("delivery state = %v, want NotFound: the claim outlived a request nobody published", got)
	}
}

// TestOutOfTurnFileCancelLeavesTheRequestAnswerable covers the other way an
// out-of-turn resolve ends without an answer: a file-protocol reader that
// declines. The request is recorded as answered by nobody, so it must not
// report a response and must stay answerable.
func TestOutOfTurnFileCancelLeavesTheRequestAnswerable(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "out-of-turn-file-cancel"), MaxRuntimes: 1})
	child := &childRuntime{id: "rt_file_cancel", lifecycleCtx: context.Background()}
	options := []any{map[string]any{"optionId": "allow"}}
	request := events.Event{
		Event:     "permission.request",
		SessionID: "session",
		Fields:    map[string]any{"request_id": "a1", "options": options},
	}
	if !sup.control.PreparePermissionClaim(child.id, "a1", control.PermissionResolverFile, options) {
		t.Fatal("claim was refused")
	}
	base := filepath.Join(t.TempDir(), "perm")
	handler := permission.NewFileHandler(base)
	handler.PollInterval = time.Millisecond
	requestPath, responsePath := permission.FilePaths(base)
	sink := &recordingSink{}
	done := make(chan error, 1)
	go func() {
		done <- cli.ResolvePermissionOutOfTurn(context.Background(), cli.OutOfTurnPermission{
			Provider:      stubProvider{},
			FileHandler:   handler,
			ControlServer: sup.control,
			Writer:        sink,
			Stderr:        io.Discard,
			SessionID:     "session",
			ClaimScope:    child.id,
		}, request)
	}()
	waitFor(t, "permission file request", func() bool {
		_, err := os.Stat(requestPath)
		return err == nil
	})
	if err := os.WriteFile(responsePath, []byte(`{"outcome":"cancelled"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("resolve out of turn: %v", err)
	}
	if written := sink.written(); len(written) != 0 {
		t.Fatalf("recorded %d events, want none: %v", len(written), written)
	}
	if got := sup.control.DeliverPendingPermission(child.id, "a1", "allow", ""); got != control.PermissionAnswerNoResolver {
		t.Fatalf("delivery state = %v, want NoResolver: a declined request stopped being answerable", got)
	}
}

// stubProvider satisfies the provider a file handler requires at entry. A
// request the reader declines never reaches it: the handler answers nobody.
type stubProvider struct{ runtime.Provider }

// recordingSink keeps what a resolver wrote, so a test can prove nothing was.
type recordingSink struct {
	mu     sync.Mutex
	events []events.Event
}

func (r *recordingSink) Write(event events.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	return nil
}

func (r *recordingSink) Close() error { return nil }

func (r *recordingSink) written() []events.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]events.Event(nil), r.events...)
}

// TestPublisherAsksForAClientPerRequest pins the hint to the client state at
// publication, not at session start: a client that connects while a turn runs
// must be the resolver the next request is published for.
func TestPublisherAsksForAClientPerRequest(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "publish-per-request"), MaxRuntimes: 1})
	child := &childRuntime{id: "rt_hint", lifecycleCtx: context.Background()}
	connected := false
	var hints []string
	publisher := sup.unpromptedPublisher(child, "session", nil)
	publisher.HasControlClient = func() bool { return connected }
	publisher.Claim = func(string, control.PermissionResolverState, []any) error { return nil }
	publisher.Write = func(event events.Event) error {
		hint, _ := event.Fields["resolver"].(string)
		hints = append(hints, hint)
		return nil
	}
	request := func(requestID string) events.Event {
		return events.Event{
			Event:     "permission.request",
			SessionID: "session",
			Fields:    map[string]any{"request_id": requestID, "options": []any{map[string]any{"optionId": "allow"}}},
		}
	}
	if _, _, err := publisher.Publish(request("a1")); err != nil {
		t.Fatal(err)
	}
	connected = true
	if _, _, err := publisher.Publish(request("a2")); err != nil {
		t.Fatal(err)
	}
	want := []string{"none", "reserved"}
	if len(hints) != len(want) || hints[0] != want[0] || hints[1] != want[1] {
		t.Fatalf("resolver hints = %v, want %v", hints, want)
	}
}

// TestPublisherReturnsTheEventItWrote pins the event a caller must resolve: the
// one carrying the hint, cloned away from the map the backend published. The
// file protocol copies these fields into its request, so a resolver reading the
// original would write a request no reader could match to the log.
func TestPublisherReturnsTheEventItWrote(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "publish-returns"), MaxRuntimes: 1})
	child := &childRuntime{id: "rt_returns", lifecycleCtx: context.Background()}
	original := events.Event{
		Event:     "permission.request",
		SessionID: "session",
		Fields:    map[string]any{"request_id": "a1"},
	}
	publisher := sup.unpromptedPublisher(child, "session", nil)
	publisher.Claim = func(string, control.PermissionResolverState, []any) error { return nil }
	publisher.Write = func(events.Event) error { return nil }
	published, state, err := publisher.Publish(original)
	if err != nil {
		t.Fatal(err)
	}
	if hint, _ := published.Fields["resolver"].(string); hint != state.String() {
		t.Fatalf("published resolver = %q, want %q", hint, state.String())
	}
	if _, mutated := original.Fields["resolver"]; mutated {
		t.Fatal("the backend's own event was mutated: the hint belongs on the copy")
	}
}

// TestAnUnpromptedAnswerRunsOnItsTurn pins the glue that keeps an out-of-turn
// answer inside the provider lifecycle: the answer is a call on the turn the
// watcher admitted, so a close that has already begun refuses it instead of the
// answer reaching the provider untracked while the provider is ending.
func TestAnUnpromptedAnswerRunsOnItsTurn(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "out-of-turn-turn"), MaxRuntimes: 1})
	request := events.Event{
		Event:     "permission.request",
		SessionID: "session",
		Fields:    map[string]any{"request_id": "a1", "options": []any{map[string]any{"optionId": "allow", "kind": "allow"}}},
	}
	answer := func(provider runtime.Provider, turn *cli.ProviderTurn) error {
		return cli.ResolvePermissionOutOfTurn(context.Background(), cli.OutOfTurnPermission{
			Provider:      provider,
			Turn:          turn,
			ControlServer: sup.control,
			Writer:        &recordingSink{},
			Stderr:        io.Discard,
			SessionID:     "session",
			ClaimScope:    "rt_turn",
			AutoApprove:   true,
		}, request)
	}

	provider := &answeringProvider{Provider: stubProvider{}}
	live := cli.NewProviderLifecycle(provider)
	if err := answer(provider, live.NewTurn()); err != nil {
		t.Fatalf("answer on a live turn: %v", err)
	}
	if got := provider.answers(); got != 1 {
		t.Fatalf("provider answered %d calls, want 1", got)
	}

	closing := cli.NewProviderLifecycle(provider)
	turn := closing.NewTurn()
	closing.RequestClose()
	if err := answer(provider, turn); err == nil {
		t.Fatal("an answer on a closed turn reached the provider")
	}
	if got := provider.answers(); got != 1 {
		t.Fatalf("provider answered %d calls, want still 1: a closing provider was written to", got)
	}
}

// answeringProvider records the answers that reach it, so a test can prove one
// did or did not.
type answeringProvider struct {
	runtime.Provider
	mu   sync.Mutex
	sent int
}

func (p *answeringProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent++
	return nil
}

func (p *answeringProvider) answers() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent
}

// TestIdleWatcherAnswersAParkedAsk drives the wiring this PR adds without a
// backend: a provider that emits a permission request while the child is parked
// must reach the resolver through the watcher, and its answer must land on the
// fanout writer the log and the control surface share.
func TestIdleWatcherAnswersAParkedAsk(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "watcher-answers"), MaxRuntimes: 1})
	log := &recordingSink{}
	provider := &askingProvider{sink: log}
	child := &childRuntime{
		id:           "rt_watcher",
		lifecycleCtx: context.Background(),
		provider:     provider,
		eventWriter:  log,
		session:      runtime.Session{SessionID: "session"},
		autoApprove:  true,
	}
	writer := newRuntimeFanoutWriter(sup, child, nil, nil)
	lifecycle := sup.ensureChildProviderLifecycle(child)
	if lifecycle == nil {
		t.Fatal("no provider lifecycle")
	}
	turn, admitted := sup.beginChildProviderTurn(child, lifecycle)
	if !admitted {
		t.Fatal("turn was not admitted")
	}
	sup.startIdleWatcher(child, provider, turn, "session", writer)
	t.Cleanup(func() { sup.stopIdleWatcher(child) })

	waitFor(t, "permission response from the watcher", func() bool {
		for _, event := range log.written() {
			if event.Event != "permission.response" {
				continue
			}
			if source, _ := event.Fields["source"].(string); source == "avenor" {
				return true
			}
		}
		return false
	})
	if got := provider.answers(); got != 1 {
		t.Fatalf("provider answered %d calls, want 1", got)
	}
}

// askingProvider emits one permission request on demand, then waits for the
// watcher's context, and records what it was asked to answer.
type askingProvider struct {
	runtime.Provider
	sink       *recordingSink
	mu         sync.Mutex
	sent       int
	askRequest events.Event
}

func (p *askingProvider) Events(ctx context.Context, _ string) (<-chan events.Event, error) {
	out := make(chan events.Event, 1)
	out <- events.Event{
		Event:     "permission.request",
		SessionID: "session",
		Fields: map[string]any{
			"request_id": "a1",
			"options":    []any{map[string]any{"optionId": "allow", "kind": "allow"}},
		},
	}
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out, nil
}

func (p *askingProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent++
	return nil
}

func (p *askingProvider) answers() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent
}
