package parentlink

import (
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestRelayedAskCarriesTheAskersDisplayName pins that the asker's name travels
// with the relay. The receiving branch has no record of the asking run, so the
// name cannot be resolved there: the run ID it is handed is not a name, and a run
// ID both branches reserve resolves to the receiving branch's own run instead.
func TestRelayedAskCarriesTheAskersDisplayName(t *testing.T) {
	parent := startBranch(t, "", messaging.SupervisorRunID)
	child := startBranch(t, parent.url(), messaging.SupervisorRunID)
	asker := child.key(messaging.SupervisorRunID)
	awaitRegistered(t, parent, asker)

	// Both branches name their own supervisor differently, which is what an
	// operator does and what a run ID cannot tell apart.
	if err := parent.broker.SetHarnessName(messaging.SupervisorRunID, "root-supervisor"); err != nil {
		t.Fatalf("name the parent's supervisor: %v", err)
	}
	if err := child.broker.SetHarnessName(messaging.SupervisorRunID, "child-supervisor"); err != nil {
		t.Fatalf("name the child's supervisor: %v", err)
	}
	awaitUntil(t, "the parent to publish the child supervisor's name", func() bool {
		hop, ok := parent.link.Registry().Hop(asker)
		return ok && hop.Label == "child-supervisor"
	})

	msgID := newMsgID(t)
	if status, body := ask(t, parent, messaging.SupervisorRunID, asker, msgID); status != http.StatusOK ||
		body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("ask to the child supervisor = (%d, %v), want 200 sent", status, body)
	}
	queued := queuedAgentMessages(t, child.broker, messaging.SupervisorRunID)
	if len(queued) != 1 {
		t.Fatalf("the child queued %d messages for its supervisor, want the relayed ask", len(queued))
	}
	if queued[0].Sender != "root-supervisor" {
		t.Fatalf("relayed ask sender = %q, want the asker's own name %q (the receiving branch's %q is a different run)",
			queued[0].Sender, "root-supervisor", "child-supervisor")
	}
	if queued[0].FromRunID != messaging.SupervisorRunID {
		t.Fatalf("relayed ask from_run_id = %q, want the canonical run the asker knows itself by", queued[0].FromRunID)
	}
}
