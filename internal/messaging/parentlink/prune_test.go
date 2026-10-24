package parentlink

import (
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestPruneLockedDropsRoutesPastTheWaitHorizon pins the route store's
// hygiene: a route record older than the messaging wait horizon can no
// longer be reached by any operation, so the prune drops it, and a record
// from inside the horizon survives for its wait or reply to follow.
func TestPruneLockedDropsRoutesPastTheWaitHorizon(t *testing.T) {
	now := time.Now()
	link := &Link{
		asks: map[askKey]askRoute{
			{Sender: "run-a", MessageID: "msg-old"}: {generation: "gen-a", recordedAt: now.Add(-messaging.MaxWait - time.Second)},
			{Sender: "run-a", MessageID: "msg-new"}: {generation: "gen-a", recordedAt: now.Add(-time.Second)},
		},
		relays: map[string]relayRoute{
			"msg-relay-old": {askerRunID: "run-a", edgeGeneration: "gen-a", recordedAt: now.Add(-messaging.MaxWait - time.Second)},
			"msg-relay-new": {askerRunID: "run-a", edgeGeneration: "gen-a", recordedAt: now.Add(-time.Second)},
		},
	}

	link.pruneLocked(now)

	if _, ok := link.asks[askKey{Sender: "run-a", MessageID: "msg-old"}]; ok {
		t.Fatal("the expired ask route stayed, want it dropped past the horizon")
	}
	if _, ok := link.asks[askKey{Sender: "run-a", MessageID: "msg-new"}]; !ok {
		t.Fatal("the live ask route was dropped inside the horizon")
	}
	if _, ok := link.relays["msg-relay-old"]; ok {
		t.Fatal("the expired relay route stayed, want it dropped past the horizon")
	}
	if _, ok := link.relays["msg-relay-new"]; !ok {
		t.Fatal("the live relay route was dropped inside the horizon")
	}
}
