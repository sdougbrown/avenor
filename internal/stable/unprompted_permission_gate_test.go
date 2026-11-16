package stable

import (
	"context"
	"testing"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
)

// TestUnpromptedPermissionRequestGate covers the two refusals the unprompted
// path must share with the canonical one: a closing child prepares no claim
// (nothing could answer it), and a request whose ID another claim still holds
// is not published, so poo's held replay cannot republish a live request
// invisibly.
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
		name    string
		closing bool
		held    bool
		want    bool
		// wantDelivery is what a later answer must see: NoResolver when the
		// unprompted path claimed the request, NotFound when it prepared nothing.
		// A foreign claim keeps its own state, which is never NoResolver.
		wantDelivery control.PermissionAnswerDelivery
		// wantHeld asserts only that a foreign claim was not replaced.
		wantHeld bool
	}{
		{name: "unprompted claims a free request", want: true, wantDelivery: control.PermissionAnswerNoResolver},
		{name: "closing child prepares nothing", closing: true, want: false, wantDelivery: control.PermissionAnswerNotFound},
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
			_, _, publish := sup.prepareUnpromptedRequest(child, request)
			if publish != tc.want {
				t.Fatalf("publish = %v, want %v", publish, tc.want)
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
