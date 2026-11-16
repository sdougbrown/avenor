package stable

import (
	"context"
	"io"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
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
