package parentlink

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestChildClaimLeavesTheParentsOwnRunIntact pins that a claim never takes a run
// this broker owns itself: the parent and a child branch may both have an rt_1,
// but the branch's claim of the locally owned run ID is refused whole, the
// parent's record keeps its own credential, and an ask to it is served here
// instead of being relayed into the branch that claimed the same run ID.
func TestChildClaimLeavesTheParentsOwnRunIntact(t *testing.T) {
	parent := startBranch(t, "", "orchestrator", "rt_1")
	child := startBranchTolerant(t, parent.url(), "rt_1")

	// The child's claim of rt_1 is refused: the run ID is locally owned here.
	status, body := postRegistration(t, parent, child.childID, child.url(), "gen-child", "",
		messaging.RunClaim{RunID: "rt_1", Label: "rt_1", Token: "child-token"})
	if status != http.StatusConflict {
		t.Fatalf("claim of a locally owned run = (%d, %v), want 409", status, body)
	}
	if parent.link.Registry().Owns(child.key("rt_1")) {
		t.Fatal("a refused claim was adopted anyway")
	}

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
		t.Fatalf("the child branch queued %d messages for a run the parent refused to route", len(queued))
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

	status, body := postRegistration(t, parent, childID, "http://127.0.0.1:1", "gen-claim", "",
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

// postRegistration publishes one child's claim the way a branch's link does and
// reports the parent's answer. A broker without a configured link credential
// trusts the loopback caller, so a request without an Authorization header is
// what a same-host test branch presents.
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
