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
