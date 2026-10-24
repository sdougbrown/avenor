package parentlink

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// crossBranch starts one parent broker and two branches that registered with it,
// and waits until the parent adopted both branches' runs.
func crossBranch(t *testing.T) (parent, first, second *branch) {
	t.Helper()
	parent = startBranch(t, "")
	first = startBranch(t, parent.url(), "asker")
	second = startBranch(t, parent.url(), "target")
	awaitRegistered(t, parent, first.key("asker"), second.key("target"))
	return parent, first, second
}

// TestCrossBranchAskAdmitsOneEdgeAtTheParent pins the delivery boundary: a
// cross-branch ask is admitted once, in the parent's ask store, relayed into the
// target's queue without a second edge, and answered along the route each hop
// recorded.
func TestCrossBranchAskAdmitsOneEdgeAtTheParent(t *testing.T) {
	parent, first, second := crossBranch(t)

	msgID := newMsgID(t)
	status, body := ask(t, first, "asker", second.key("target"), msgID)
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("cross-branch ask = (%d, %v), want 200 sent", status, body)
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 1 {
		t.Fatalf("parent holds %d ask edges, want exactly 1", edges)
	}
	if edges := first.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("the asking branch admitted %d edges, want 0", edges)
	}
	if edges := second.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("the target branch admitted %d edges, want 0", edges)
	}

	queued := queuedAgentMessages(t, second.broker, "target")
	if len(queued) != 1 {
		t.Fatalf("target queue = %d messages, want exactly one relayed ask", len(queued))
	}
	// The asker travels as the parent addresses it, so the reply the target sends
	// back names a run the parent can resolve.
	if queued[0].ID != msgID || queued[0].FromRunID != first.key("asker") {
		t.Fatalf("relayed ask = %+v, want the canonical id and the parent's name for the asker", queued[0])
	}

	// The asker is named the way the parent relayed the ask: the parent's own
	// address for the asking branch's run.
	if status, body = reply(t, second, "target", first.key("asker"), msgID, "pong"); status != http.StatusOK ||
		body["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("cross-branch reply = (%d, %v), want 200 replied", status, body)
	}
	status, body = waitReply(t, first, "asker", msgID, 5000)
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("cross-branch wait = (%d, %v), want 200 replied", status, body)
	}
	payload, ok := body["payload"].(map[string]any)
	if !ok || payload["message"] != "pong" {
		t.Fatalf("wait payload = %v, want the reply body", body["payload"])
	}
	if fromRunID, _ := body["from_run_id"].(string); fromRunID != second.key("target") {
		t.Fatalf("reply from_run_id = %q, want the run the asker addressed", fromRunID)
	}

	// The reply completed the asker's edge; it was not queued a second time as a
	// fire-and-forget delivery, and the parent holds no edge afterwards.
	if queued := queuedAgentMessages(t, first.broker, "asker"); len(queued) != 0 {
		t.Fatalf("asker queue = %d messages, want the reply to complete the edge only", len(queued))
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("parent still holds %d ask edges after the reply", edges)
	}
}

// TestWithdrawalRetractsOnlyTheParentEdge pins the best-effort withdrawal: the
// parent's edge is retracted, and the ask already relayed into the target's
// queue is not recalled.
func TestWithdrawalRetractsOnlyTheParentEdge(t *testing.T) {
	parent, first, second := crossBranch(t)

	msgID := newMsgID(t)
	if status, body := ask(t, first, "asker", second.key("target"), msgID); status != http.StatusOK {
		t.Fatalf("cross-branch ask = (%d, %v), want 200", status, body)
	}
	if queued := queuedAgentMessages(t, second.broker, "target"); len(queued) != 1 {
		t.Fatalf("target queue = %d messages, want the relayed ask", len(queued))
	}

	status, body := withdraw(t, first, "asker", msgID)
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeWithdrawn) {
		t.Fatalf("withdraw = (%d, %v), want 200 withdrawn", status, body)
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("parent still holds %d ask edges after the withdrawal", edges)
	}
	if queued := queuedAgentMessages(t, second.broker, "target"); len(queued) != 1 {
		t.Fatalf("target queue = %d messages after the withdrawal, want the forwarded ask kept", len(queued))
	}

	// A wait after the withdrawal reads the parent's terminal fence.
	status, body = waitReply(t, first, "asker", msgID, 1000)
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeWithdrawn) {
		t.Fatalf("wait after withdrawal = (%d, %v), want 200 withdrawn", status, body)
	}
}

// TestRefusedDescendantRetiresItsBrokerRecord pins that a run whose branch
// refused a relay stops being a target entirely: the claim and the broker's run
// record go together, so a later ask is refused instead of queued for a run
// nobody serves.
func TestRefusedDescendantRetiresItsBrokerRecord(t *testing.T) {
	parent, first, second := crossBranch(t)
	target := second.key("target")
	if parent.broker.GetRun(target) == nil {
		t.Fatal("the parent never adopted the target run the branch registered")
	}

	// The target branch answers as a replacement, so the relay is refused.
	second.broker.Store().Reset()
	status, body := ask(t, first, "asker", second.key("target"), newMsgID(t))
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("ask to a refused branch = (%d, %v), want 404 target-unavailable", status, body)
	}
	if parent.broker.GetRun(target) != nil {
		t.Fatal("a refused descendant kept its broker record")
	}

	// A later ask cannot fall back to serving the run locally.
	status, body = ask(t, first, "asker", second.key("target"), newMsgID(t))
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("ask after the record was retired = (%d, %v), want 404 target-unavailable", status, body)
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("the parent holds %d ask edges for a retired descendant", edges)
	}
	if st := parent.broker.GetRun(target); st != nil {
		st.Mu.Lock()
		queued := len(st.ControlQueue)
		st.Mu.Unlock()
		if queued != 0 {
			t.Fatalf("the parent queued %d messages for a retired descendant", queued)
		}
	}
}

// TestRelayToAGoneBranchRetractsTheParentEdge pins that a relay which never
// reaches the branch leaves no edge behind.
func TestRelayToAGoneBranchRetractsTheParentEdge(t *testing.T) {
	parent, first, second := crossBranch(t)

	if err := second.broker.Stop(); err != nil {
		t.Fatalf("stop target branch broker: %v", err)
	}
	status, body := ask(t, first, "asker", second.key("target"), newMsgID(t))
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("ask to a gone branch = (%d, %v), want 404 target-unavailable", status, body)
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("parent holds %d ask edges for a relay that never landed", edges)
	}
}

// TestRelayToAReplacedBranchIsNotReplayed pins that a branch which answers as a
// replacement is refused: the relay is not retried into it and the edge the
// parent admitted for it is retracted.
func TestRelayToAReplacedBranchIsNotReplayed(t *testing.T) {
	parent, first, second := crossBranch(t)

	// The target branch's ask store is replaced: it now answers as a different
	// generation than the one it registered with.
	second.broker.Store().Reset()

	status, body := ask(t, first, "asker", second.key("target"), newMsgID(t))
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("ask to a replaced branch = (%d, %v), want 404 target-unavailable", status, body)
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("parent holds %d ask edges for a refused relay", edges)
	}
	if parent.link.Registry().Owns(second.key("target")) {
		t.Fatal("a branch that answers as a replacement is still a registered target")
	}
}

// TestReplacedParentIsNeverReplayed pins the reply-generation guard: an ask
// admitted by a parent whose generation has since changed is lost, not
// forwarded again.
func TestReplacedParentIsNeverReplayed(t *testing.T) {
	parent, first, second := crossBranch(t)

	msgID := newMsgID(t)
	if status, body := ask(t, first, "asker", second.key("target"), msgID); status != http.StatusOK {
		t.Fatalf("cross-branch ask = (%d, %v), want 200", status, body)
	}
	// The parent's ask store is replaced: a new generation, and no edge from the
	// old one can exist.
	parent.broker.Store().Reset()

	status, body := waitReply(t, first, "asker", msgID, 2000)
	if status != http.StatusGone || body["outcome"] != string(messaging.OutcomeLost) {
		t.Fatalf("wait against a replaced parent = (%d, %v), want 410 lost", status, body)
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("replaced parent holds %d ask edges, want none replayed", edges)
	}
}

// TestReplyGenerationGuardRefusesAReplacedParent pins the server-side
// no-replay invariant: a reply that names the generation its edge was admitted
// under is refused by a replacement parent instead of degrading into a delivery
// nobody waits for.
func TestReplyGenerationGuardRefusesAReplacedParent(t *testing.T) {
	parent, first, second := crossBranch(t)

	msgID := newMsgID(t)
	if status, body := ask(t, first, "asker", second.key("target"), msgID); status != http.StatusOK {
		t.Fatalf("cross-branch ask = (%d, %v), want 200", status, body)
	}
	admitted := parent.broker.Store().Generation()
	parent.broker.Store().Reset()

	status, body := postJSON(t, second.broker.Addr(), "/send", "target", second.token(t, "target"), map[string]any{
		"from_run_id": "target",
		"to_run_id":   "asker",
		"type":        "agent_message",
		"reply_to":    msgID,
		"generation":  admitted,
		"payload":     map[string]any{"message": "pong"},
	})
	if status != http.StatusConflict || body["code"] != "generation-mismatch" {
		t.Fatalf("reply to a replaced parent = (%d, %v), want 409 generation-mismatch", status, body)
	}
	if queued := queuedAgentMessages(t, first.broker, "asker"); len(queued) != 0 {
		t.Fatalf("asker queue = %d messages, want no delivery for a refused reply", len(queued))
	}
}

// TestReplyRouteIsDroppedWhenTheParentWasReplaced pins the link's own guard: once
// a branch knows its parent was replaced, it stops transmitting replies onto the
// old generation instead of asking the replacement to refuse them.
func TestReplyRouteIsDroppedWhenTheParentWasReplaced(t *testing.T) {
	parent, first, second := crossBranch(t)

	msgID := newMsgID(t)
	if status, body := ask(t, first, "asker", second.key("target"), msgID); status != http.StatusOK {
		t.Fatalf("cross-branch ask = (%d, %v), want 200", status, body)
	}
	parent.broker.Store().Reset()
	if err := second.link.up.Sync(context.Background()); err != nil {
		t.Fatalf("re-register the target branch: %v", err)
	}

	outcome, err := second.link.up.Reply(context.Background(), messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "target"},
		ToRunID:   first.key("asker"),
		MessageID: msgID,
		Body:      `{"message":"pong"}`,
	})
	if err != nil {
		t.Fatalf("reply to a replaced parent = (%v, %v), want a clean lost verdict", outcome, err)
	}
	if outcome != messaging.OutcomeLost {
		t.Fatalf("reply to a replaced parent = %v, want lost", outcome)
	}
	if queued := queuedAgentMessages(t, first.broker, "asker"); len(queued) != 0 {
		t.Fatalf("asker queue = %d messages, want no delivery for a dropped reply route", len(queued))
	}
}

// TestParentPeerExportHidesSiblingBranches pins the export rule: a branch sees
// its own runs and the parent's own runs, never a sibling branch's runs, while
// the parent's own callers see the descendants it owns.
func TestParentPeerExportHidesSiblingBranches(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	first := startBranch(t, parent.url(), "asker")
	sibling := startBranch(t, parent.url(), "target")
	awaitRegistered(t, parent, first.key("asker"), sibling.key("target"))

	status, body := getSessions(t, first.broker.Addr(), "asker", first.token(t, "asker"))
	if status != http.StatusOK {
		t.Fatalf("sessions = (%d, %v), want 200", status, body)
	}
	listed := sessionRunIDs(t, body)
	if listed["asker"] != 1 || listed["orchestrator"] != 1 {
		t.Fatalf("branch listing = %v, want its own run and the parent's own run, once each", listed)
	}
	if listed[sibling.key("target")] != 0 {
		t.Fatalf("branch listing = %v, want no sibling branch run", listed)
	}

	status, body = getSessions(t, parent.broker.Addr(), "orchestrator", parent.token(t, "orchestrator"))
	if status != http.StatusOK {
		t.Fatalf("parent sessions = (%d, %v), want 200", status, body)
	}
	listed = sessionRunIDs(t, body)
	if listed["orchestrator"] != 1 || listed[first.key("asker")] != 1 || listed[sibling.key("target")] != 1 {
		t.Fatalf("parent listing = %v, want its own run and its owned descendants, once each", listed)
	}
}

// TestBranchRetiresItsClaimOnClose pins that a closed branch stops being a
// routing target at its parent.
func TestBranchRetiresItsClaimOnClose(t *testing.T) {
	parent := startBranch(t, "")
	child := startBranch(t, parent.url(), "child-run")
	key := child.key("child-run")
	awaitRegistered(t, parent, key)

	if err := child.link.Close(); err != nil {
		t.Fatalf("close branch link: %v", err)
	}
	if parent.link.Registry().Owns(key) {
		t.Fatal("the closed branch is still a registered routing target")
	}
	if parent.broker.GetRun(key) != nil {
		t.Fatal("the closed branch's run record is still at the parent")
	}
}

// sessionRunIDs counts the rows a /sessions listing carries for each run ID, so
// a run listed twice reads as two rather than as present.
func sessionRunIDs(t *testing.T, body map[string]any) map[string]int {
	t.Helper()
	sessions, ok := body["sessions"].([]any)
	if !ok {
		t.Fatalf("sessions body = %v, want a sessions array", body)
	}
	listed := make(map[string]int, len(sessions))
	for _, entry := range sessions {
		record, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if runID, ok := record["run_id"].(string); ok {
			listed[runID]++
		}
	}
	return listed
}

// TestRenamedRunIsRepublishedToTheParent pins that a run rename reaches the
// parent's run records: the branch re-publishes its run set when the label
// changes, instead of the parent showing the pre-rename name until an unrelated
// run-set change.
func TestRenamedRunIsRepublishedToTheParent(t *testing.T) {
	parent, _, second := crossBranch(t)

	target := second.key("target")
	if err := second.broker.RenameRun("target", "renamed-target"); err != nil {
		t.Fatalf("rename run: %v", err)
	}
	deadline := time.Now().Add(awaitDeadline)
	label := func() string {
		for _, info := range parent.broker.Sessions() {
			if info.RunID == target {
				return info.Label
			}
		}
		return ""
	}
	for label() != "renamed-target" {
		if time.Now().After(deadline) {
			t.Fatalf("parent run label = %q, want the renamed name republished", label())
		}
		time.Sleep(2 * time.Millisecond)
	}
}
