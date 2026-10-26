package parentlink

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestBranchIDDerivesDurableIdentity pins the identity a branch registers
// under: the same input always derives the same identity, two identities whose
// sanitized stems collide stay distinct, the readable stem is sanitized, and
// the same socket path on two different hosts stays distinct.
func TestBranchIDDerivesDurableIdentity(t *testing.T) {
	cases := []struct {
		identity string
		host     string
		prefix   string // the sanitized stem the identity must start with
	}{
		{identity: "test/avenor.sock", host: "host-a", prefix: "avenor"},
		{identity: "/x/avenor", host: "host-a", prefix: "avenor"},
		{identity: "/x/avenor.bak", host: "host-a", prefix: "avenor"},
		{identity: "dir/my branch!.sock", host: "host-a", prefix: "my-branch"},
		{identity: "dir/!!avenor!!.sock", host: "host-a", prefix: "avenor"},
		{identity: "dir/---.sock", host: "host-a", prefix: "branch"},
	}
	seen := map[string]string{}
	for _, tc := range cases {
		id := BranchID(tc.identity, tc.host)
		if id == "" || !strings.HasPrefix(id, tc.prefix+"-") {
			t.Fatalf("BranchID(%q, %q) = %q, want the sanitized stem %q", tc.identity, tc.host, id, tc.prefix)
		}
		if other, dup := seen[id]; dup {
			t.Fatalf("BranchID(%q) = BranchID(%q) = %q, want distinct identities", tc.identity, other, id)
		}
		seen[id] = tc.identity
		if again := BranchID(tc.identity, tc.host); again != id {
			t.Fatalf("BranchID(%q) derived %q then %q, want the same identity every time", tc.identity, id, again)
		}
	}
	// A branch on another host that mounts the same socket path is a different
	// branch: the registry would otherwise key both claims under one identity.
	first := BranchID("/home/dev/.avenor/agent.sock", "host-a")
	second := BranchID("/home/dev/.avenor/agent.sock", "host-b")
	if first == second {
		t.Fatal("the same socket path on two hosts derived one identity")
	}
	// The empty host is not a free pass: the path alone still derives an identity.
	if id := BranchID("/x/avenor.sock", ""); id == "" {
		t.Fatal("an empty host derived no identity")
	}
	if id := BranchID("", "host-a"); id != "" {
		t.Fatalf("BranchID(\"\") = %q, want no identity", id)
	}
}

// TestUncleanRestartReclaimsTheRunsUnderItsDurableIdentity pins the recovery
// path: a branch that dies without retracting its claim comes back under the
// same durable identity, reclaims the runs it still owns, retires the ones it
// no longer owns, and leaves no duplicate run records behind.
func TestUncleanRestartReclaimsTheRunsUnderItsDurableIdentity(t *testing.T) {
	parent := startBranch(t, "")

	// The first instance registers and then dies: its broker stops serving and
	// its claim is never retracted.
	first := newBranchWith(t, parent.url(), "branch-alpha", time.Hour, true, []string{"asker", "stale"})
	awaitRegistered(t, parent, "asker", "stale")
	if err := first.broker.Stop(); err != nil {
		t.Fatalf("stop the first instance uncleanly: %v", err)
	}
	if !parent.link.Registry().Owns("stale") {
		t.Fatal("the dead branch's claim did not survive the test's unclean stop")
	}

	// The restarted instance keeps "asker", dropped "stale" and added "fresh".
	restarted := newBranchWith(t, parent.url(), "branch-alpha", time.Hour, true, []string{"asker", "fresh"})
	awaitUntil(t, "the restarted branch to reclaim its runs", func() bool {
		return parent.link.Registry().Owns("asker") && parent.link.Registry().Owns("fresh")
	})

	if parent.link.Registry().Owns("stale") {
		t.Fatal("the restarted branch still owns the run it stopped owning")
	}
	if st := parent.broker.GetRun("stale"); st != nil {
		t.Fatal("the parent still serves a run the restarted branch stopped owning")
	}
	if owner, ok := parent.link.Registry().Owner("asker"); !ok || owner != "branch-alpha" {
		t.Fatalf("owner of the reclaimed run = (%q, %v), want the durable branch identity", owner, ok)
	}
	// One record per claimed run: the replacement replaced, it did not add.
	if count := parent.broker.RunCount(); count != 2 {
		t.Fatalf("the parent holds %d run records, want the two the restarted branch claims", count)
	}
	if token, _ := parent.broker.RunToken("asker"); token != restarted.token(t, "asker") {
		t.Fatal("the reclaimed run kept the dead instance's credential")
	}
}

// TestReplacementClaimRefreshesTheRunCredential pins that accepting a
// replacement claim adopts the credential it presents: both directions keep
// authenticating after a branch comes back with rotated credentials.
func TestReplacementClaimRefreshesTheRunCredential(t *testing.T) {
	parent := startBranch(t, "")
	token, err := parent.broker.CreateRun("orchestrator", "orchestrator")
	if err != nil {
		t.Fatalf("create the parent's own asker run: %v", err)
	}
	parent.tokens["orchestrator"] = token
	first := newBranchWith(t, parent.url(), "branch-beta", time.Hour, true, []string{"target"})
	awaitRegistered(t, parent, "target")
	staleToken, _ := parent.broker.RunToken("target")
	if staleToken != first.token(t, "target") {
		t.Fatal("the parent did not adopt the first instance's credential")
	}

	replacement := newBranchWith(t, parent.url(), "branch-beta", time.Hour, true, []string{"target"})
	rotated := replacement.token(t, "target")
	if rotated == staleToken {
		t.Fatal("the replacement branch reused the dead instance's credential")
	}
	awaitUntil(t, "the parent to adopt the rotated credential", func() bool {
		token, ok := parent.broker.RunToken("target")
		return ok && token == rotated
	})

	// Downward: the parent relays into the replacement branch with the stored
	// credential, so the branch that came back receives the ask.
	msgID := newMsgID(t)
	if status, body := ask(t, parent, "orchestrator", "target", msgID); status != http.StatusOK {
		t.Fatalf("ask to the replaced branch = (%d, %v), want 200", status, body)
	}
	awaitUntil(t, "the relayed ask to reach the replacement branch", func() bool {
		return len(queuedAgentMessages(t, replacement.broker, "target")) == 1
	})

	// Upward: the replacement branch answers with the rotated credential and
	// the parent completes the edge it admitted.
	if status, body := reply(t, replacement, "target", "orchestrator", msgID, "pong"); status != http.StatusOK {
		t.Fatalf("reply from the replacement branch = (%d, %v), want 200", status, body)
	}
	status, body := waitReply(t, parent, "orchestrator", msgID, 5000)
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("wait for the replacement branch's reply = (%d, %v), want 200 replied", status, body)
	}
}
