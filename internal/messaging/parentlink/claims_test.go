package parentlink

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestSiblingBranchesThatMintTheSameRunIDsBothRegister pins the branch identity
// on a claim: every branch mints run IDs from its own counter, so two siblings
// claim the same literal run IDs for runs that are not the same run. Both claims
// are adopted — a sibling is never left invisible — and each stays its own
// routing target.
func TestSiblingBranchesThatMintTheSameRunIDsBothRegister(t *testing.T) {
	parent := startBranch(t, "")
	first := startBranch(t, parent.url(), "rt_1", messaging.SupervisorRunID)
	second := startBranch(t, parent.url(), "rt_1", messaging.SupervisorRunID)

	firstRun, secondRun := first.key("rt_1"), second.key("rt_1")
	if firstRun == secondRun {
		t.Fatalf("both branches claim %q, want one run per branch", firstRun)
	}
	awaitRegistered(t, parent, firstRun, secondRun)

	// An ask to one sibling's rt_1 is queued in that branch and nowhere else.
	msgID := newMsgID(t)
	if status, body := ask(t, first, messaging.SupervisorRunID, secondRun, msgID); status != http.StatusOK ||
		body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("ask to a sibling's rt_1 = (%d, %v), want 200 sent", status, body)
	}
	if queued := queuedAgentMessages(t, second.broker, "rt_1"); len(queued) != 1 {
		t.Fatalf("the target branch queued %d messages for its rt_1, want the relayed ask", len(queued))
	}
	if queued := queuedAgentMessages(t, first.broker, "rt_1"); len(queued) != 0 {
		t.Fatalf("the asking branch queued %d messages for its own rt_1, want none", len(queued))
	}
}

// TestChildClaimLeavesTheParentsOwnRunIntact pins that a claim never takes a run
// this broker owns itself: the parent and a child branch may both have an rt_1,
// the parent's record keeps its own credential, and an ask to it is served here
// instead of being relayed into the branch that claimed the same run ID.
func TestChildClaimLeavesTheParentsOwnRunIntact(t *testing.T) {
	parent := startBranch(t, "", "orchestrator", "rt_1")
	child := startBranch(t, parent.url(), "rt_1")
	childRun := child.key("rt_1")
	awaitRegistered(t, parent, childRun)

	owned, _ := parent.broker.RunToken("rt_1")
	if owned != parent.token(t, "rt_1") {
		t.Fatal("the child's claim re-pointed the parent's own run credential")
	}

	if status, body := ask(t, parent, "orchestrator", "rt_1", newMsgID(t)); status != http.StatusOK ||
		body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("ask to the parent's own rt_1 = (%d, %v), want 200 sent", status, body)
	}
	if queued := queuedAgentMessages(t, parent.broker, "rt_1"); len(queued) != 1 {
		t.Fatalf("the parent queued %d messages for its own rt_1, want the ask kept local", len(queued))
	}
	if queued := queuedAgentMessages(t, child.broker, "rt_1"); len(queued) != 0 {
		t.Fatalf("the child branch queued %d messages for a run it does not own here", len(queued))
	}

	// The child's own rt_1 stays its own routing target.
	if status, body := ask(t, parent, "orchestrator", childRun, newMsgID(t)); status != http.StatusOK ||
		body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("ask to the child's rt_1 = (%d, %v), want 200 sent", status, body)
	}
	if queued := queuedAgentMessages(t, child.broker, "rt_1"); len(queued) != 1 {
		t.Fatalf("the child branch queued %d messages for its own rt_1, want the relayed ask", len(queued))
	}
}

// TestClaimOnARecordTheParentOwnsUnderTheKeyIsRefused pins the register guard: a
// record this broker minted itself under a claim key is not a child's to take, so
// the registration is refused whole and the record keeps its credential.
func TestClaimOnARecordTheParentOwnsUnderTheKeyIsRefused(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	const childID = "branch-collide"
	key := ParentRunKey(childID, "rt_1")
	owned, err := parent.broker.CreateRun(key, "parent-owned")
	if err != nil {
		t.Fatalf("create the parent's own record under the claim key: %v", err)
	}

	status, body := postRegistration(t, parent, childID, "http://127.0.0.1:1", "gen-claim", testLinkToken,
		messaging.RunClaim{RunID: "rt_1", Label: "child-owned", Token: "child-token"})
	if status != http.StatusConflict {
		t.Fatalf("claim on a record the parent owns = (%d, %v), want 409", status, body)
	}
	if token, ok := parent.broker.RunToken(key); !ok || token != owned {
		t.Fatalf("run record credential = (%q, %v), want the parent's own", token, ok)
	}
	if parent.link.Registry().Owns(key) {
		t.Fatal("a refused claim was adopted anyway")
	}
	if label := parent.broker.DisplayName(key); label != "parent-owned" {
		t.Fatalf("run record label = %q, want the parent's own", label)
	}
}

// TestParentRoutesRefuseWithoutTheLinkCredential pins the parent-route
// authorization: a registration without the configured link credential cannot
// replace a live branch's claim, and a broker with no configured credential
// serves no parent routes at all.
func TestParentRoutesRefuseWithoutTheLinkCredential(t *testing.T) {
	parent := startBranch(t, "")
	child := startBranch(t, parent.url(), "target")
	key := child.key("target")
	awaitRegistered(t, parent, key)

	// No credential at all on a broker that requires one.
	status, body := postRegistration(t, parent, child.childID, "http://127.0.0.1:1", "gen-hijack", "",
		messaging.RunClaim{RunID: "target", Label: "hijack", Token: "hijack-token"})
	if status != http.StatusUnauthorized {
		t.Fatalf("claim with no link credential = (%d, %v), want 401", status, body)
	}
	if owner, ok := parent.link.Registry().Owner(key); !ok || owner != child.childID {
		t.Fatalf("owner of the live claim = (%q, %v), want the branch that registered", owner, ok)
	}
	if token, _ := parent.broker.RunToken(key); token != child.token(t, "target") {
		t.Fatal("an unauthenticated claim re-pointed the live branch's credential")
	}

	// A broker with no configured credential does not fall back to trusting the
	// loopback caller.
	open := startBranch(t, "")
	open.broker.InstallParentLink(open.link, "", open.link.RunChanged)
	status, body = postRegistration(t, open, "branch-open", "http://127.0.0.1:1", "gen-open", testLinkToken,
		messaging.RunClaim{RunID: "target", Token: "open-token"})
	if status != http.StatusUnauthorized {
		t.Fatalf("claim on a broker with no link credential = (%d, %v), want 401", status, body)
	}
	if open.link.Registry().Owns(ParentRunKey("branch-open", "target")) {
		t.Fatal("a broker with no link credential adopted a claim")
	}
}

// TestDescendantRecordsStayOutOfNameResolution pins that a run a child broker
// owns is neither resolved nor renamed here: its display name belongs to the
// branch that owns it, and a rename written to this broker's copy would be
// reverted by the next claim.
func TestDescendantRecordsStayOutOfNameResolution(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	child := startBranch(t, parent.url(), "target")
	key := child.key("target")
	awaitRegistered(t, parent, key)

	if matches := parent.broker.MatchLabel("target"); len(matches) != 0 {
		t.Fatalf("MatchLabel(target) = %v, want the descendant's record excluded", matches)
	}
	if matches := parent.broker.MatchLabel("orchestrator"); len(matches) != 1 || matches[0] != "orchestrator" {
		t.Fatalf("MatchLabel(orchestrator) = %v, want this broker's own run", matches)
	}

	if err := parent.broker.RenameRun(key, "renamed-here"); err == nil {
		t.Fatal("the parent renamed a run a child broker owns")
	}
	if label := parent.broker.DisplayName(key); label != "target" {
		t.Fatalf("descendant label = %q, want the name the owning branch publishes", label)
	}
}

// postRegistration publishes one child's claim the way a branch's link does and
// reports the parent's answer. An empty credential sends no Authorization
// header, which is what a caller that never learned the link credential presents.
func postRegistration(t *testing.T, parent *branch, childID, endpoint, generation, credential string, runs ...messaging.RunClaim) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"child_id":   childID,
		"endpoint":   endpoint,
		"generation": generation,
		"runs":       runs,
	})
	if err != nil {
		t.Fatalf("marshal registration: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, parent.url()+pathRegister, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build registration: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post registration: %v", err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}
