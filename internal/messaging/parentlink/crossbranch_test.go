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
	awaitRegistered(t, parent, "asker", "target")
	return parent, first, second
}

// TestCrossBranchAskAdmitsOneEdgeAtTheParent pins the delivery boundary: a
// cross-branch ask is admitted once, in the parent's ask store, relayed into the
// target's queue without a second edge, and answered along the route each hop
// recorded.
func TestCrossBranchAskAdmitsOneEdgeAtTheParent(t *testing.T) {
	parent, first, second := crossBranch(t)

	msgID := newMsgID(t)
	status, body := ask(t, first, "asker", "target", msgID)
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
	if queued[0].ID != msgID || queued[0].FromRunID != "asker" {
		t.Fatalf("relayed ask = %+v, want the canonical id and asker", queued[0])
	}

	if status, body = reply(t, second, "target", "asker", msgID, "pong"); status != http.StatusOK ||
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
	if fromRunID, _ := body["from_run_id"].(string); fromRunID != "target" {
		t.Fatalf("reply from_run_id = %q, want the canonical target run", fromRunID)
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
	if status, body := ask(t, first, "asker", "target", msgID); status != http.StatusOK {
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
	if parent.broker.GetRun("target") == nil {
		t.Fatal("the parent never adopted the target run the branch registered")
	}

	// The target branch answers as a replacement, so the relay is refused.
	second.broker.Store().Reset()
	status, body := ask(t, first, "asker", "target", newMsgID(t))
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("ask to a refused branch = (%d, %v), want 404 target-unavailable", status, body)
	}
	if parent.broker.GetRun("target") != nil {
		t.Fatal("a refused descendant kept its broker record")
	}

	// A later ask cannot fall back to serving the run locally.
	status, body = ask(t, first, "asker", "target", newMsgID(t))
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("ask after the record was retired = (%d, %v), want 404 target-unavailable", status, body)
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("the parent holds %d ask edges for a retired descendant", edges)
	}
}

// TestRelayToAGoneBranchRetractsTheParentEdge pins that a relay which never
// reaches the branch leaves no edge behind.
func TestRelayToAGoneBranchRetractsTheParentEdge(t *testing.T) {
	parent, first, second := crossBranch(t)

	if err := second.broker.Stop(); err != nil {
		t.Fatalf("stop target branch broker: %v", err)
	}
	status, body := ask(t, first, "asker", "target", newMsgID(t))
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

	status, body := ask(t, first, "asker", "target", newMsgID(t))
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("ask to a replaced branch = (%d, %v), want 404 target-unavailable", status, body)
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("parent holds %d ask edges for a refused relay", edges)
	}
	if parent.link.Registry().Owns("target") {
		t.Fatal("a branch that answers as a replacement is still a registered target")
	}
}

// TestReplacedParentIsNeverReplayed pins the reply-generation guard: an ask
// admitted by a parent whose generation has since changed is lost, not
// forwarded again.
func TestReplacedParentIsNeverReplayed(t *testing.T) {
	parent, first, _ := crossBranch(t)

	msgID := newMsgID(t)
	if status, body := ask(t, first, "asker", "target", msgID); status != http.StatusOK {
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
	if status, body := ask(t, first, "asker", "target", msgID); status != http.StatusOK {
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
	if status, body := ask(t, first, "asker", "target", msgID); status != http.StatusOK {
		t.Fatalf("cross-branch ask = (%d, %v), want 200", status, body)
	}
	parent.broker.Store().Reset()
	if err := second.link.up.Sync(context.Background()); err != nil {
		t.Fatalf("re-register the target branch: %v", err)
	}

	outcome, err := second.link.up.Reply(context.Background(), messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "target"},
		ToRunID:   "asker",
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
	_ = startBranch(t, parent.url(), "target")
	awaitRegistered(t, parent, "asker", "target")

	status, body := getSessions(t, first.broker.Addr(), "asker", first.token(t, "asker"))
	if status != http.StatusOK {
		t.Fatalf("sessions = (%d, %v), want 200", status, body)
	}
	listed := sessionRunIDs(t, body)
	if !listed["asker"] || !listed["orchestrator"] {
		t.Fatalf("branch listing = %v, want its own run and the parent's own run", listed)
	}
	if listed["target"] {
		t.Fatalf("branch listing = %v, want no sibling branch run", listed)
	}

	status, body = getSessions(t, parent.broker.Addr(), "orchestrator", parent.token(t, "orchestrator"))
	if status != http.StatusOK {
		t.Fatalf("parent sessions = (%d, %v), want 200", status, body)
	}
	listed = sessionRunIDs(t, body)
	if !listed["orchestrator"] || !listed["asker"] || !listed["target"] {
		t.Fatalf("parent listing = %v, want its own run and its owned descendants", listed)
	}
}

// TestBranchDoesNotListItsOwnSupervisorTwice pins the peer join across the
// link: a branch's supervisor registers at its parent under a namespaced key,
// but the branch lists the run locally under its canonical ID, so the parent's
// listing must not hand the alias back — the branch would otherwise see a
// phantom remote peer for itself and address it through the parent.
func TestBranchDoesNotListItsOwnSupervisorTwice(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	childID := newTestChildID()
	child := newBranchWith(t, parent.url(), childID, DefaultRefreshInterval, true, []string{messaging.SupervisorRunID})
	awaitRegistered(t, parent, "supervisor@"+childID)

	status, body := getSessions(t, child.broker.Addr(), messaging.SupervisorRunID, child.token(t, messaging.SupervisorRunID))
	if status != http.StatusOK {
		t.Fatalf("sessions = (%d, %v), want 200", status, body)
	}
	listed := sessionRunIDs(t, body)
	if !listed[messaging.SupervisorRunID] || !listed["orchestrator"] {
		t.Fatalf("branch listing = %v, want its own supervisor and the parent's own run", listed)
	}
	if listed["supervisor@"+childID] {
		t.Fatalf("branch listing = %v, want no namespaced alias of its own supervisor", listed)
	}
}

// TestBranchRetiresItsClaimOnClose pins that a closed branch stops being a
// routing target at its parent.
func TestBranchRetiresItsClaimOnClose(t *testing.T) {
	parent := startBranch(t, "")
	child := startBranch(t, parent.url(), "child-run")
	awaitRegistered(t, parent, "child-run")

	if err := child.link.Close(); err != nil {
		t.Fatalf("close branch link: %v", err)
	}
	if parent.link.Registry().Owns("child-run") {
		t.Fatal("the closed branch is still a registered routing target")
	}
	if parent.broker.GetRun("child-run") != nil {
		t.Fatal("the closed branch's run record is still at the parent")
	}
}

func sessionRunIDs(t *testing.T, body map[string]any) map[string]bool {
	t.Helper()
	sessions, ok := body["sessions"].([]any)
	if !ok {
		t.Fatalf("sessions body = %v, want a sessions array", body)
	}
	listed := make(map[string]bool, len(sessions))
	for _, entry := range sessions {
		record, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if runID, ok := record["run_id"].(string); ok {
			listed[runID] = true
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

	if err := second.broker.RenameRun("target", "renamed-target"); err != nil {
		t.Fatalf("rename run: %v", err)
	}
	deadline := time.Now().Add(awaitDeadline)
	label := func() string {
		st := parent.broker.GetRun("target")
		if st == nil {
			return ""
		}
		st.Mu.Lock()
		defer st.Mu.Unlock()
		if st.Info == nil {
			return ""
		}
		return st.Info.Label
	}
	for label() != "renamed-target" {
		if time.Now().After(deadline) {
			t.Fatalf("parent run label = %q, want the renamed name republished", label())
		}
		time.Sleep(2 * time.Millisecond)
	}
}
