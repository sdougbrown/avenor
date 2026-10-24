package parentlink

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestBranchIdentityIsDerivedFromTheControlSocketPath derives the expectation
// from the identity itself instead of calling BranchID, so an identity that moves
// with process state, drops the digest, or cannot tell two paths with the same
// stem apart fails here instead of silently defeating restart reclamation in
// tests that compute their expectations through the same function.
func TestBranchIdentityIsDerivedFromTheControlSocketPath(t *testing.T) {
	identities := []string{
		"/run/user/500/avenor/branch-alpha.sock",
		"/home/agent/.local/state/avenor/branch-alpha.sock",
		"state/branch-alpha.sock",
	}
	seen := make(map[string]string, len(identities))
	for _, identity := range identities {
		digest := sha256.Sum256([]byte(identity))
		want := "branch-alpha-" + hex.EncodeToString(digest[:4])
		if got := BranchID(identity); got != want {
			t.Fatalf("BranchID(%q) = %q, want the sanitized stem and the identity's digest %q", identity, got, want)
		}
		if again := BranchID(identity); again != want {
			t.Fatalf("BranchID(%q) = %q on a second call, want the same durable identity %q", identity, again, want)
		}
		if other, collides := seen[want]; collides {
			t.Fatalf("%q and %q share the branch identity %q", other, identity, want)
		}
		seen[want] = identity
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
	asker, stale := first.key("asker"), first.key("stale")
	awaitRegistered(t, parent, asker, stale)
	if err := first.broker.Stop(); err != nil {
		t.Fatalf("stop the first instance uncleanly: %v", err)
	}
	if !parent.link.Registry().Owns(stale) {
		t.Fatal("the dead branch's claim did not survive the test's unclean stop")
	}

	// The restarted instance keeps "asker", dropped "stale" and added "fresh".
	restarted := newBranchWith(t, parent.url(), "branch-alpha", time.Hour, true, []string{"asker", "fresh"})
	awaitUntil(t, "the restarted branch to reclaim its runs", func() bool {
		return parent.link.Registry().Owns(asker) && parent.link.Registry().Owns(restarted.key("fresh"))
	})

	if parent.link.Registry().Owns(stale) {
		t.Fatal("the restarted branch still owns the run it stopped owning")
	}
	if st := parent.broker.GetRun(stale); st != nil {
		t.Fatal("the parent still serves a run the restarted branch stopped owning")
	}
	if owner, ok := parent.link.Registry().Owner(asker); !ok || owner != "branch-alpha" {
		t.Fatalf("owner of the reclaimed run = (%q, %v), want the durable branch identity", owner, ok)
	}
	// One record per claimed run: the replacement replaced, it did not add.
	if count := parent.broker.RunCount(); count != 2 {
		t.Fatalf("the parent holds %d run records, want the two the restarted branch claims", count)
	}
	if token, _ := parent.broker.RunToken(asker); token != restarted.token(t, "asker") {
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
	target := first.key("target")
	awaitRegistered(t, parent, target)
	staleToken, _ := parent.broker.RunToken(target)
	if staleToken != first.token(t, "target") {
		t.Fatal("the parent did not adopt the first instance's credential")
	}

	replacement := newBranchWith(t, parent.url(), "branch-beta", time.Hour, true, []string{"target"})
	rotated := replacement.token(t, "target")
	if rotated == staleToken {
		t.Fatal("the replacement branch reused the dead instance's credential")
	}
	awaitUntil(t, "the parent to adopt the rotated credential", func() bool {
		token, ok := parent.broker.RunToken(target)
		return ok && token == rotated
	})

	// Downward: the parent relays into the replacement branch with the stored
	// credential, so the branch that came back receives the ask.
	msgID := newMsgID(t)
	if status, body := ask(t, parent, "orchestrator", target, msgID); status != http.StatusOK {
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
