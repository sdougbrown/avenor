package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/messagetest"
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
	// the resolution could produce, each pinning the store's generation.
	generation := parent.Store().Generation()
	now := time.Now()
	for _, target := range []string{"claude-x", parentlink.ParentRunKey("child-1", "claude-x")} {
		id, err := messaging.NewMsgID(now, time.Time{})
		if err != nil {
			t.Fatalf("mint message id: %v", err)
		}
		res, err := rootMessenger.Send(ctx, messaging.SendRequest{
			From:       messaging.Identity{RunID: "supervisor"},
			To:         messaging.Identity{RunID: target},
			MessageID:  id,
			Generation: generation,
			Body:       `{"message":"hello from the host"}`,
			NoReply:    true,
		})
		t.Logf("tell to %q: outcome=%q err=%v", target, res.Outcome, err)
	}

	// The ask shape, for completeness.
	askID, err := messaging.NewMsgID(now, time.Time{})
	if err != nil {
		t.Fatalf("mint ask id: %v", err)
	}
	res, err := rootMessenger.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: parentlink.ParentRunKey("child-1", "claude-x")},
		MessageID:  askID,
		Generation: generation,
		Body:       `{"message":"ask from the host"}`,
	})
	t.Logf("ask: outcome=%q err=%v", res.Outcome, err)
}

// The live system's second failure shape: the host and the target live on the
// SAME branch broker, but the target's peer consumer stopped polling, so the
// record reads unavailable. The send must stay here — answered or refused by
// this broker's own store — and never travel to the parent, which cannot route
// a plain run id it only holds behind a namespaced claim key.
func TestOwnerTellToAnIdleLocalRunIsNotForwardedUpward(t *testing.T) {
	parent := New("")
	if err := parent.Start(); err != nil {
		t.Fatalf("start parent broker: %v", err)
	}
	t.Cleanup(func() { _ = parent.Stop() })
	rootLocal := &messagetest.Recorder{Local: parent.LocalMessenger()}
	rootMessenger, err := parentlink.New(parentlink.Options{
		ChildID:    "root",
		Generation: func() string { return parent.Store().Generation() },
	}, rootLocal)
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
	childLocal := &messagetest.Recorder{Local: child.LocalMessenger()}
	childLink, err := parentlink.New(parentlink.Options{
		ParentURL:  "http://" + parent.Addr(),
		Token:      "parent-token",
		Endpoint:   "http://" + child.Addr(),
		ChildID:    "child-1",
		Runs:       func() []messaging.RunClaim { return child.ParentRunClaims() },
		Generation: func() string { return child.Store().Generation() },
		RetireRun:  func(runID, token string) { child.DeleteRunIfToken(runID, token) },
	}, childLocal)
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

	tellID, err := messaging.NewMsgID(time.Now(), time.Time{})
	if err != nil {
		t.Fatalf("mint tell id: %v", err)
	}
	res, err := childLink.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: "claude-y"},
		MessageID:  tellID,
		Generation: child.Store().Generation(),
		Body:       `{"message":"hello from the host"}`,
		NoReply:    true,
	})
	if err == nil {
		t.Fatalf("tell to an idle local run = (%+v, nil), want a local refusal", res)
	}
	if !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("tell to an idle local run err = %v, want ErrTargetUnavailable", err)
	}
	if res.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("tell outcome = %q, want %q", res.Outcome, messaging.OutcomeNotSent)
	}
	// The send stayed here: the local seam served it and the parent never saw it.
	for _, sent := range childLocal.Sends() {
		if sent.Request.To.RunID != "claude-y" {
			t.Fatalf("the local seam was asked for %q, want the idle local target only", sent.Request.To.RunID)
		}
	}
	if len(childLocal.Sends()) != 1 {
		t.Fatalf("the local seam saw %d sends, want the one idle local send", len(childLocal.Sends()))
	}
	if len(rootLocal.Sends()) != 0 {
		t.Fatalf("the parent was asked for %d sends, want nothing forwarded", len(rootLocal.Sends()))
	}
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
