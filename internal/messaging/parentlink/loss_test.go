package parentlink

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// awaitUntil polls until condition reports true or the deadline passes.
func awaitUntil(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(awaitDeadline)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestUnreachableParentSurfacesAsTargetUnavailable pins the parent-loss
// fallback: a branch whose parent is not listening forwards nothing, reports an
// ask for a target it cannot reach as target-unavailable instead of hanging, and
// reports a wait with no forwarded route as edge-absent. The listing such a
// branch still keeps is pinned separately by
// TestParentLossKeepsThePartialListingAndReportsIt.
func TestUnreachableParentSurfacesAsTargetUnavailable(t *testing.T) {
	gone := startBranch(t, "")
	goneURL := gone.url()
	if err := gone.broker.Stop(); err != nil {
		t.Fatalf("stop the branch that stands in for the parent: %v", err)
	}
	branch := startBranchTolerant(t, goneURL, "asker")
	if branch.link == nil {
		t.Fatal("branch link missing")
	}

	status, body := ask(t, branch, "asker", "elsewhere", newMsgID(t))
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("ask through an unreachable parent = (%d, %v), want 404 target-unavailable", status, body)
	}

	// A wait for an ask this branch never forwarded stays local.
	status, body = waitReply(t, branch, "asker", newMsgID(t), 200)
	if status != http.StatusNotFound || body["outcome"] != string(messaging.OutcomeEdgeAbsent) {
		t.Fatalf("wait with no forwarded route = (%d, %v), want 404 edge-absent", status, body)
	}
}

// TestParentLossKeepsThePartialListingAndReportsIt pins the listing contract: a
// branch whose parent stopped answering still returns its own sessions, the local
// generation and the reason the listing is partial.
func TestParentLossKeepsThePartialListingAndReportsIt(t *testing.T) {
	parent, first, _ := crossBranch(t)

	if err := parent.broker.Stop(); err != nil {
		t.Fatalf("stop parent broker: %v", err)
	}
	status, body := getSessions(t, first.broker.Addr(), "asker", first.token(t, "asker"))
	if status != http.StatusOK {
		t.Fatalf("sessions after parent loss = (%d, %v), want 200", status, body)
	}
	if listed := sessionRunIDs(t, body); !listed["asker"] {
		t.Fatalf("partial listing = %v, want the branch's own run", listed)
	}
	if generation, _ := body["generation"].(string); generation == "" {
		t.Fatalf("partial listing = %v, want the local generation", body)
	}
	if peersError, _ := body["peers_error"].(string); peersError == "" {
		t.Fatalf("partial listing = %v, want peers_error beside it", body)
	}
}

// TestCallerLossRetiresTheParentEdge pins the caller-loss path: a run deleted in
// its own branch stops being a routing target at the parent, and the edge it left
// open there is completed instead of waiting out its timeout.
func TestCallerLossRetiresTheParentEdge(t *testing.T) {
	parent, first, _ := crossBranch(t)

	if status, body := ask(t, first, "asker", "target", newMsgID(t)); status != http.StatusOK {
		t.Fatalf("cross-branch ask = (%d, %v), want 200", status, body)
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 1 {
		t.Fatalf("parent holds %d ask edges, want 1", edges)
	}

	// The asking run is gone, so its branch re-publishes a run set without it.
	// The registry change and the adopted record's deletion land in two steps,
	// so wait for the whole server-side transition before asserting.
	first.broker.DeleteRun("asker")
	awaitUntil(t, "the parent to retire the deleted caller", func() bool {
		return !parent.link.Registry().Owns("asker") &&
			parent.broker.GetRun("asker") == nil &&
			parent.broker.Store().ActiveCount() == 0
	})
}

// TestConcurrentCrossBranchAsksAndWithdrawals drives the routed reply, wait and
// withdraw paths concurrently, so the route maps, the registry and the link's
// registration are exercised under contention.
func TestConcurrentCrossBranchAsksAndWithdrawals(t *testing.T) {
	parent, first, second := crossBranch(t)
	const asks = 8

	done := make(chan struct{}, asks)
	for i := 0; i < asks; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			ctx := context.Background()
			now := time.Now().UTC()
			msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
			if err != nil {
				t.Errorf("mint message id: %v", err)
				return
			}
			sent, err := first.link.Send(ctx, messaging.SendRequest{
				From:       messaging.Identity{RunID: "asker", Sender: "asker"},
				To:         messaging.Identity{RunID: "target"},
				MessageID:  msgID,
				Generation: first.broker.Store().Generation(),
				Body:       `{"message":"ping"}`,
			}, true)
			if err != nil || sent.Outcome != messaging.OutcomeSent {
				t.Errorf("ask = (%+v, %v), want sent", sent, err)
				return
			}
			if i%2 == 0 {
				withdrawn, err := first.link.Withdraw(ctx, messaging.WithdrawRequest{
					Sender:     messaging.Identity{RunID: "asker"},
					MessageID:  msgID,
					Generation: first.broker.Store().Generation(),
				})
				if err != nil || withdrawn.Outcome != messaging.OutcomeWithdrawn {
					t.Errorf("withdraw = (%+v, %v), want withdrawn", withdrawn, err)
				}
				return
			}
			// The reply arm: the target's reply races this branch's wait on
			// the same edge, and the verdict is Replied under either
			// interleaving: a wait admitted first blocks on the live edge until
			// the reply completes it, and a reply that completes the edge first
			// leaves it for this wait to read, because this wait is the only
			// consumer. A withdrawal never touches this edge: that path is the
			// other arm's.
			replied := make(chan messaging.Outcome, 1)
			go func() {
				outcome, err := second.link.Reply(ctx, messaging.ReplyRequest{
					From:      messaging.Identity{RunID: "target"},
					ToRunID:   "asker",
					MessageID: msgID,
					Body:      `{"message":"pong"}`,
				})
				if err != nil || outcome != messaging.OutcomeReplied {
					t.Errorf("reply = (%v, %v), want replied", outcome, err)
				}
				replied <- outcome
			}()
			waited, err := first.link.Wait(ctx, messaging.WaitRequest{
				Sender:     messaging.Identity{RunID: "asker"},
				MessageID:  msgID,
				Generation: first.broker.Store().Generation(),
				Timeout:    5 * time.Second,
			})
			if err != nil || waited.Outcome != messaging.OutcomeReplied {
				t.Errorf("wait = (%+v, %v), want replied", waited, err)
			} else if string(waited.Reply.Payload) != `{"message":"pong"}` {
				t.Errorf("wait reply payload = %v, want the reply the target sent", waited.Reply.Payload)
			}
			<-replied
		}(i)
	}
	for i := 0; i < asks; i++ {
		<-done
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("parent holds %d ask edges after every ask settled", edges)
	}
}

// TestWaitInFlightReturnsWhenTheBranchCloses pins teardown: a wait that is in
// flight when its branch closes returns a verdict instead of hanging.
func TestWaitInFlightReturnsWhenTheBranchCloses(t *testing.T) {
	_, first, second := crossBranch(t)

	msgID := newMsgID(t)
	if status, body := ask(t, first, "asker", "target", msgID); status != http.StatusOK {
		t.Fatalf("cross-branch ask = (%d, %v), want 200", status, body)
	}
	if queued := queuedAgentMessages(t, second.broker, "target"); len(queued) != 1 {
		t.Fatalf("target queue = %d messages, want the relayed ask", len(queued))
	}

	waited := make(chan messaging.WaitResult, 1)
	go func() {
		result, err := first.link.Wait(context.Background(), messaging.WaitRequest{
			Sender:     messaging.Identity{RunID: "asker"},
			MessageID:  msgID,
			Generation: first.broker.Store().Generation(),
			Timeout:    messaging.MaxWait,
		})
		if err != nil {
			result = messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}
		}
		waited <- result
	}()
	// The wait is registered at the parent once the ask's edge is still open and
	// the branch is closed underneath it.
	if err := first.link.Close(); err != nil {
		t.Fatalf("close branch link: %v", err)
	}
	select {
	case result := <-waited:
		if result.Outcome == messaging.OutcomeReplied {
			t.Fatalf("wait returned %q for an ask nobody answered", result.Outcome)
		}
	case <-time.After(awaitDeadline):
		t.Fatal("a wait in flight did not return after its branch closed")
	}
}
