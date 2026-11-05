package broker

import (
	"context"
	"encoding/json"
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
	hub := startHubAndBranch(t, "claude-x")
	parent := hub.parent
	child := hub.child
	rootMessenger := hub.rootMessenger

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// The control owner sends as the supervisor identity, both target spellings
	// the resolution could produce, each pinning the store's generation.
	generation := parent.Store().Generation()
	now := time.Now()
	// A plain target id names no run the root routes: the claim key is the
	// namespaced spelling, so the bare id is refused and nothing is relayed.
	res, err := rootMessenger.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: "claude-x"},
		MessageID:  mustMsgID(t, now),
		Generation: generation,
		Body:       `{"message":"hello from the host"}`,
		NoReply:    true,
	})
	if res.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("tell to the bare id = (%+v, %v), want not-sent", res, err)
	}
	if !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("tell to the bare id err = %v, want ErrTargetUnavailable", err)
	}

	// The claim-key tell relays into the child's queue.
	res, err = rootMessenger.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: parentlink.ParentRunKey("child-1", "claude-x")},
		MessageID:  mustMsgID(t, now),
		Generation: generation,
		Body:       `{"message":"hello from the host"}`,
		NoReply:    true,
	})
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("tell to the claim key = (%+v, %v), want sent", res, err)
	}

	// And the ask shape: the edge admits at the root and the relay delivers.
	res, err = rootMessenger.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: parentlink.ParentRunKey("child-1", "claude-x")},
		MessageID:  mustMsgID(t, now),
		Generation: generation,
		Body:       `{"message":"ask from the host"}`,
	})
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("ask = (%+v, %v), want sent", res, err)
	}

	// Both relayed messages reached the child's queue — the "not lost" half of
	// the issue this branch pins.
	if got := countAgentMessages(t, child, "claude-x"); got != 2 {
		t.Fatalf("child queue = %d messages, want the relayed tell and ask", got)
	}
}

// The live system's second failure shape: the host and the target live on the
// SAME branch broker, but the target's peer consumer stopped polling, so the
// record reads unavailable. The send must stay here — answered or refused by
// this broker's own store — and never travel to the parent, which cannot route
// a plain run id it only holds behind a namespaced claim key.
func TestOwnerTellToAnIdleLocalRunIsNotForwardedUpward(t *testing.T) {
	hub := startHubAndBranch(t, "claude-y")
	child := hub.child
	rootLocal := hub.rootLocal
	childLocal := hub.childLocal

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// The target's peer stops polling: the record reads unavailable, the way an
	// idle Claude session does after the parked timeout.
	parkUnavailable(t, child, "claude-y")

	tellID, err := messaging.NewMsgID(time.Now(), time.Time{})
	if err != nil {
		t.Fatalf("mint tell id: %v", err)
	}
	res, err := hub.childLink.Send(ctx, messaging.SendRequest{
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
	// And the ask shape: the routing decision is the same for an ask, and the
	// parked target refuses it locally without admitting an edge.
	askRes, askErr := hub.childLink.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: "claude-y"},
		MessageID:  mustMsgID(t, time.Now()),
		Generation: child.Store().Generation(),
		Body:       `{"message":"ask to an idle run"}`,
	})
	if !errors.Is(askErr, messaging.ErrTargetUnavailable) || askRes.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("ask to an idle local run = (%+v, %v), want a local refusal", askRes, askErr)
	}
	// And the tell shape: the fire-and-forget path routes the same way, and the
	// parked target refuses it locally without it ever reaching the parent.
	if err := hub.childLink.Tell(ctx, messaging.TellRequest{
		FromRunID: "supervisor",
		ToRunID:   "claude-y",
		Kind:      "agent_message",
		Payload:   json.RawMessage(`{"message":"tell to an idle run"}`),
	}); !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("tell to an idle local run err = %v, want ErrTargetUnavailable", err)
	}

	// The tells and sends stayed here: the local seam served them and the parent
	// never saw any. The local-seam counts below are what fail when a message is
	// forwarded — the parent's own reachability gate would 404 the forward before
	// its seam ever ran, so a parent-side assertion cannot carry this pin.
	for _, sent := range childLocal.Sends() {
		if sent.Request.To.RunID != "claude-y" {
			t.Fatalf("the local seam was asked for %q, want the idle local target only", sent.Request.To.RunID)
		}
	}
	if len(childLocal.Sends()) != 2 {
		t.Fatalf("the local seam saw %d sends, want the send-backed tell and the ask served locally", len(childLocal.Sends()))
	}
	for _, told := range childLocal.Tells() {
		if told.ToRunID != "claude-y" {
			t.Fatalf("the local seam was told for %q, want the idle local target only", told.ToRunID)
		}
	}
	if len(childLocal.Tells()) != 1 {
		t.Fatalf("the local seam saw %d tells, want the tell served locally", len(childLocal.Tells()))
	}
	if len(rootLocal.Sends()) != 0 {
		t.Fatalf("the parent was asked for %d sends, want nothing forwarded", len(rootLocal.Sends()))
	}
	if len(rootLocal.Tells()) != 0 {
		t.Fatalf("the parent was told for %d, want nothing forwarded", len(rootLocal.Tells()))
	}
}

// hubBranch is one root-and-child broker pair wired the way the stable
// supervisor installs it: the child branch registers its runs at the root, and
// each broker's messaging runs through a recorder over its own local seam.
type hubBranch struct {
	parent, child *Broker
	rootMessenger, childLink *parentlink.Messenger
	rootLocal, childLocal    *messagetest.Recorder
}

// startHubAndBranch builds the topology both repro tests share: a root broker
// with no parent, and a child branch that registers its one run at the root.
// The child's claim is waited for at the root before the caller proceeds.
func startHubAndBranch(t *testing.T, runID string) *hubBranch {
	t.Helper()
	parent := New("")
	if err := parent.Start(); err != nil {
		t.Fatalf("start parent broker: %v", err)
	}
	t.Cleanup(func() { _ = parent.Stop() })

	// The root: no parent of its own, but it serves the descendant registry and
	// routes through the same seam the supervisor installs.
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
	if _, err := child.CreateRun(runID, runID); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// The child branch: it registers its runs at the root, the way the stable
	// supervisor's installParentLink wires it.
	childLocal := &messagetest.Recorder{Local: child.LocalMessenger()}
	childLink, err := parentlink.New(parentlink.Options{
		ParentURL:   "http://" + parent.Addr(),
		Token:       "parent-token",
		Endpoint:    "http://" + child.Addr(),
		ChildID:     "child-1",
		Runs:        func() []messaging.RunClaim { return child.ParentRunClaims() },
		Generation:  func() string { return child.Store().Generation() },
		DisplayName: func(runID string) string { return runID },
		RetireRun:   func(runID, token string) { child.DeleteRunIfToken(runID, token) },
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
	for !rootMessenger.Owns(parentlink.ParentRunKey("child-1", runID)) {
		if time.Now().After(deadline) {
			t.Fatal("the child's claim never landed at the root")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return &hubBranch{
		parent:        parent,
		child:         child,
		rootMessenger: rootMessenger,
		childLink:     childLink,
		rootLocal:     rootLocal,
		childLocal:    childLocal,
	}
}

// parkUnavailable marks runID's peer unavailable on its own broker, the way a
// missed lease deadline does, without waiting one out. It asserts the state it
// produced, so a later lifecycle change cannot silently hollow the helper out.
func parkUnavailable(t *testing.T, b *Broker, runID string) {
	t.Helper()
	st := b.GetRun(runID)
	if st == nil {
		t.Fatalf("run %q is not registered", runID)
	}
	st.Mu.Lock()
	st.peer = true
	st.peerDeadline = time.Now().Add(-time.Minute)
	if st.peerUnavailableLocked(time.Now()) == "" {
		st.Mu.Unlock()
		t.Fatalf("parking %q did not make the peer unavailable", runID)
	}
	st.Mu.Unlock()
}

// mustMsgID mints one versioned message id with the default acceptance
// horizon, the way every production caller does.
func mustMsgID(t *testing.T, now time.Time) string {
	t.Helper()
	id, err := messaging.NewMsgID(now, time.Time{})
	if err != nil {
		t.Fatalf("mint message id: %v", err)
	}
	return id
}
