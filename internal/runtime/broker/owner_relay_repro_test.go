package broker

import (
	"context"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/parentlink"
)

// This test models issue #508: a control-owner send (the OpenCode host path,
// which claims the supervisor identity) addressed to a run a child branch owns.
// The parent is the root broker the host connected to; the child is the branch
// whose broker owns the Claude run.
func TestOwnerTellToAChildOwnedRun(t *testing.T) {
	child := New("")
	if err := child.Start(); err != nil {
		t.Fatalf("start child broker: %v", err)
	}
	t.Cleanup(func() { _ = child.Stop() })
	if _, err := child.CreateRun("claude-x", "claude-x"); err != nil {
		t.Fatalf("create child run: %v", err)
	}

	parent := New("")
	if err := parent.Start(); err != nil {
		t.Fatalf("start parent broker: %v", err)
	}
	t.Cleanup(func() { _ = parent.Stop() })

	// The root: no parent of its own, but it serves the descendant registry and
	// routes through the same seam the supervisor installs.
	rootMessenger, err := parentlink.New(parentlink.Options{
		ChildID:    "root",
		Generation: func() string { return parent.Store().Generation() },
	}, parent.LocalMessenger())
	if err != nil {
		t.Fatalf("build root messenger: %v", err)
	}
	parent.SetMessenger(rootMessenger)
	parent.InstallParentLink(rootMessenger, "parent-token", rootMessenger.RunChanged)

	// The child branch: it registers its runs at the root, the way the stable
	// supervisor's installParentLink wires it.
	childLink, err := parentlink.New(parentlink.Options{
		ParentURL:  "http://" + parent.Addr(),
		Token:      "parent-token",
		Endpoint:   "http://" + child.Addr(),
		ChildID:    "child-1",
		Runs:       func() []messaging.RunClaim { return child.ParentRunClaims() },
		Generation: func() string { return child.Store().Generation() },
		DisplayName: func(runID string) string {
			if st := child.GetRun(runID); st != nil {
				return runID
			}
			return runID
		},
		RetireRun: func(runID, token string) { child.DeleteRunIfToken(runID, token) },
	}, child.LocalMessenger())
	if err != nil {
		t.Fatalf("build child link: %v", err)
	}
	child.SetMessenger(childLink)
	child.InstallParentLink(childLink, "parent-token", childLink.RunChanged)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := childLink.Start(ctx); err != nil {
		t.Fatalf("child registration: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for !rootMessenger.Owns(parentlink.ParentRunKey("child-1", "claude-x")) {
		if time.Now().After(deadline) {
			t.Fatal("the child's claim never landed at the root")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The control owner sends as the supervisor identity, both target spellings
	// the resolution could produce.
	for _, target := range []string{"claude-x", parentlink.ParentRunKey("child-1", "claude-x")} {
		res, err := rootMessenger.Send(ctx, messaging.SendRequest{
			From:      messaging.Identity{RunID: "supervisor"},
			To:        messaging.Identity{RunID: target},
			MessageID: "av1.owner.1",
			Body:      `{"message":"hello from the host"}`,
			NoReply:   true,
		})
		t.Logf("tell to %q: outcome=%q err=%v", target, res.Outcome, err)
	}

	// And the ask shape, for completeness.
	res, err := rootMessenger.Send(ctx, messaging.SendRequest{
		From:      messaging.Identity{RunID: "supervisor"},
		To:        messaging.Identity{RunID: parentlink.ParentRunKey("child-1", "claude-x")},
		MessageID: "av1.owner.2",
		Body:      `{"message":"ask from the host"}`,
	})
	t.Logf("ask: outcome=%q err=%v", res.Outcome, err)
}

// The live system's second failure shape: the host and the target live on the
// SAME branch broker, but the target's peer consumer stopped polling, so the
// record reads unavailable. The seam then forwards the send to the parent even
// though the target is a run this broker owns — and a parent cannot route a
// plain run id it only holds behind a namespaced claim key. This test pins
// what happens today so the fix has a red to turn green.
func TestOwnerTellToAnIdleLocalRunIsNotForwardedUpward(t *testing.T) {
	parent := New("")
	if err := parent.Start(); err != nil {
		t.Fatalf("start parent broker: %v", err)
	}
	t.Cleanup(func() { _ = parent.Stop() })
	rootMessenger, err := parentlink.New(parentlink.Options{
		ChildID:    "root",
		Generation: func() string { return parent.Store().Generation() },
	}, parent.LocalMessenger())
	if err != nil {
		t.Fatalf("build root messenger: %v", err)
	}
	parent.SetMessenger(rootMessenger)
	parent.InstallParentLink(rootMessenger, "parent-token", rootMessenger.RunChanged)

	child := New("")
	if err := child.Start(); err != nil {
		t.Fatalf("start child broker: %v", err)
	}
	t.Cleanup(func() { _ = child.Stop() })
	if _, err := child.CreateRun("claude-y", "claude-y"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	childLink, err := parentlink.New(parentlink.Options{
		ParentURL:  "http://" + parent.Addr(),
		Token:      "parent-token",
		Endpoint:   "http://" + child.Addr(),
		ChildID:    "child-1",
		Runs:       func() []messaging.RunClaim { return child.ParentRunClaims() },
		Generation: func() string { return child.Store().Generation() },
		RetireRun:  func(runID, token string) { child.DeleteRunIfToken(runID, token) },
	}, child.LocalMessenger())
	if err != nil {
		t.Fatalf("build child link: %v", err)
	}
	child.SetMessenger(childLink)
	child.InstallParentLink(childLink, "parent-token", childLink.RunChanged)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := childLink.Start(ctx); err != nil {
		t.Fatalf("child registration: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !rootMessenger.Owns(parentlink.ParentRunKey("child-1", "claude-y")) {
		if time.Now().After(deadline) {
			t.Fatal("the child's claim never landed at the root")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The target's peer stops polling: the record reads unavailable, the way an
	// idle Claude session does after the parked timeout.
	parkUnavailable(t, child, "claude-y")

	res, err := childLink.Send(ctx, messaging.SendRequest{
		From:      messaging.Identity{RunID: "supervisor"},
		To:        messaging.Identity{RunID: "claude-y"},
		MessageID: "av1.owner.3",
		Body:      `{"message":"hello from the host"}`,
		NoReply:   true,
	})
	t.Logf("tell to an idle local run: outcome=%q err=%v", res.Outcome, err)
}

// parkUnavailable marks runID's peer unavailable on its own broker, the way a
// missed lease deadline does, without waiting one out.
func parkUnavailable(t *testing.T, b *Broker, runID string) {
	t.Helper()
	st := b.GetRun(runID)
	if st == nil {
		t.Fatalf("run %q is not registered", runID)
	}
	st.Mu.Lock()
	st.peer = true
	st.peerDeadline = time.Now().Add(-time.Minute)
	st.Mu.Unlock()
}
