package stable

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/parentlink"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// testLinkToken is the credential a parent broker in these tests requires of
// the branches that register with it, and the one those branches present.
const testLinkToken = "stable-test-link-token"

// startTestParentBroker starts a broker that serves the parent-link routes and
// relays into the branches that register with it, wired the way the supervisor
// wires a parent broker.
func startTestParentBroker(t *testing.T) (*broker.Broker, *parentlink.Messenger) {
	t.Helper()
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start parent broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	link, err := parentlink.New(parentlink.Options{
		Generation:  b.Store().Generation,
		RunToken:    b.RunToken,
		DisplayName: b.DisplayName,
		RetireRun:   b.DeleteRunIfToken,
	}, b.LocalMessenger())
	if err != nil {
		t.Fatalf("build parent messenger: %v", err)
	}
	b.SetParentLink(link)
	b.SetParentLinkToken(testLinkToken)
	b.SetMessenger(link)
	t.Cleanup(func() { _ = link.Close() })
	return b, link
}

// startTestBranch starts a broker whose runs register with parentBroker, the
// way a nested supervisor is wired.
func startTestBranch(t *testing.T, parentBroker *broker.Broker, runs ...string) (*broker.Broker, map[string]string) {
	t.Helper()
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start branch broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	tokens := make(map[string]string, len(runs))
	for _, runID := range runs {
		token, err := b.CreateRun(runID, runID)
		if err != nil {
			t.Fatalf("create run %q: %v", runID, err)
		}
		tokens[runID] = token
	}
	var link *parentlink.Messenger
	link, err := parentlink.New(parentlink.Options{
		ParentURL:  "http://" + parentBroker.Addr(),
		Token:      testLinkToken,
		ChildID:    parentlink.BranchID("test/" + b.Addr()),
		Endpoint:   "http://" + b.Addr(),
		Generation: b.Store().Generation,
		Runs: func() []messaging.RunClaim {
			claims := make([]messaging.RunClaim, 0, len(runs))
			for _, info := range b.Sessions() {
				if link.Owns(info.RunID) {
					continue
				}
				token, _ := b.RunToken(info.RunID)
				claims = append(claims, messaging.RunClaim{RunID: info.RunID, Label: info.Label, Token: token})
			}
			return claims
		},
		RunToken: b.RunToken,
		// A relay carries the asker's resolved name, which the receiving branch
		// cannot resolve for itself.
		DisplayName: b.DisplayName,
		RetireRun:   b.DeleteRunIfToken,
	}, b.LocalMessenger())
	if err != nil {
		t.Fatalf("build branch messenger: %v", err)
	}
	b.SetParentLink(link)
	b.SetParentLinkToken(testLinkToken)
	b.SetMessenger(link)
	b.SetRunObserver(link.RunChanged)
	t.Cleanup(func() { _ = link.Close() })
	if err := link.Start(context.Background()); err != nil {
		t.Fatalf("register branch with parent: %v", err)
	}
	return b, tokens
}

// TestSupervisorParentBrokerLink pins the supervisor's opt-in wiring: with a
// parent broker configured, the supervisor registers its own run there, an ask
// no local run can answer is admitted once at the parent and relayed into the
// branch that owns the target, and the reply correlates back through the
// supervisor's own transport.
func TestSupervisorParentBrokerLink(t *testing.T) {
	parentBroker, parentLink := startTestParentBroker(t)
	peerBroker, peerTokens := startTestBranch(t, parentBroker, "rt_remote")
	if !parentLink.Registry().Owns(branchKey(peerBroker, "rt_remote")) {
		t.Fatal("the parent never adopted the peer branch's run")
	}

	sup := newTestSupervisor(t, Config{
		ControlSocket:     newStableSocketPath(t, "parent"),
		MaxRuntimes:       2,
		BrokerParentURL:   "http://" + parentBroker.Addr(),
		BrokerParentToken: testLinkToken,
	})
	t.Cleanup(func() { _ = sup.Shutdown("kill") })
	if sup.parentLink == nil {
		t.Fatal("a supervisor with a parent broker URL has no parent link")
	}
	// The supervisor registers its own run under its branch identity: every
	// branch reserves the supervisor run ID, so the parent addresses it there.
	if branchSupervisorRun := messaging.SupervisorRunID + "@" + parentlink.BranchID(sup.config.ControlSocket);
		!parentLink.Registry().Owns(branchSupervisorRun) {
		t.Fatalf("the supervisor did not register its own run with its parent broker as %q", branchSupervisorRun)
	}

	ctx := context.Background()
	pre, err := sup.messaging.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}

	// A local target is answered here: the parent admits nothing for it.
	token, err := sup.broker.CreateRun("rt_local", "local")
	if err != nil {
		t.Fatalf("create local run: %v", err)
	}
	_ = token
	localMsgID := newStableMsgID(t)
	sent, err := sup.messaging.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: messaging.SupervisorRunID, Sender: "supervisor"},
		To:         messaging.Identity{RunID: "rt_local"},
		MessageID:  localMsgID,
		Generation: pre.Generation,
		Body:       `{"message":"local ping"}`,
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("local ask = (%+v, %v), want sent", sent, err)
	}
	if edges := parentBroker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("a local ask left %d edges at the parent broker", edges)
	}

	// A target in another branch is admitted once at the parent and relayed.
	msgID := newStableMsgID(t)
	sent, err = sup.messaging.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: messaging.SupervisorRunID, Sender: "supervisor"},
		To:         messaging.Identity{RunID: branchKey(peerBroker, "rt_remote")},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       `{"message":"cross-branch ping"}`,
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("cross-branch ask = (%+v, %v), want sent", sent, err)
	}
	if edges := parentBroker.Store().ActiveCount(); edges != 1 {
		t.Fatalf("parent holds %d ask edges for the cross-branch ask, want exactly 1", edges)
	}
	if edges := peerBroker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("the target branch admitted %d edges, want 0", edges)
	}

	// The remote run answers over its own broker surface. The asker it names is
	// the run ID the ask arrived with, which is the parent's address for this
	// branch's supervisor.
	asker := deliveredAsker(t, peerBroker, "rt_remote", msgID)
	if want := messaging.SupervisorRunID + "@" + parentlink.BranchID(sup.config.ControlSocket); asker != want {
		t.Fatalf("delivered asker = %q, want the supervisor's branch-scoped run ID %q", asker, want)
	}
	if status, body := parentLinkedPost(t, peerBroker.Addr(), "/send", "rt_remote", peerTokens["rt_remote"], map[string]any{
		"from_run_id": "rt_remote",
		"to_run_id":   asker,
		"type":        "agent_message",
		"reply_to":    msgID,
		"payload":     map[string]any{"message": "cross-branch pong"},
	}); status != http.StatusOK {
		t.Fatalf("remote reply = (%d, %v), want 200", status, body)
	}

	waited, err := sup.messaging.Wait(ctx, messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: messaging.SupervisorRunID},
		MessageID:  msgID,
		Generation: pre.Generation,
		Timeout:    5 * time.Second,
	})
	if err != nil || waited.Outcome != messaging.OutcomeReplied || waited.Reply == nil {
		t.Fatalf("cross-branch wait = (%+v, %v), want the reply", waited, err)
	}
	if waited.Reply.FromRunID != branchKey(peerBroker, "rt_remote") {
		t.Fatalf("reply from_run_id = %q, want the run the supervisor addressed", waited.Reply.FromRunID)
	}
	if edges := parentBroker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("parent still holds %d ask edges after the reply", edges)
	}
}

// TestSupervisorParentAcceptsSupervisorChildren pins the branch-supervisor
// identity: every branch reserves the same supervisor run ID, so a supervisor
// parent and the supervisors that register with it keep distinct routing
// targets. The parent's own supervisor run survives its children's claims, each
// child's supervisor is addressed under its branch identity, and an ask to one
// of them still relays into that branch and correlates its reply.
func TestSupervisorParentAcceptsSupervisorChildren(t *testing.T) {
	parentSup := newTestSupervisor(t, Config{
		ControlSocket:     newStableSocketPath(t, "root"),
		MaxRuntimes:       2,
		BrokerParentToken: "branch-token",
	})
	t.Cleanup(func() { _ = parentSup.Shutdown("kill") })
	if parentSup.parentLink == nil {
		t.Fatal("a supervisor with a parent token serves no parent routes")
	}
	parentURL := "http://" + parentSup.broker.Addr()

	children := make([]*Supervisor, 0, 2)
	childKeys := make([]string, 0, 2)
	for _, name := range []string{"child-one", "child-two"} {
		socket := newStableSocketPath(t, name)
		child := newTestSupervisor(t, Config{
			ControlSocket:     socket,
			MaxRuntimes:       2,
			BrokerParentURL:   parentURL,
			BrokerParentToken: "branch-token",
		})
		t.Cleanup(func() { _ = child.Shutdown("kill") })
		children = append(children, child)
		childKeys = append(childKeys, messaging.SupervisorRunID+"@"+parentlink.BranchID(socket))
	}
	for _, key := range childKeys {
		if !parentSup.parentLink.Registry().Owns(key) {
			t.Fatalf("the parent did not adopt the child supervisor run %q", key)
		}
	}
	// The parent's own supervisor run is still its own: a child's claim never
	// overwrites it, so the parent's credential still authenticates it.
	if parentSup.parentLink.Registry().Owns(messaging.SupervisorRunID) {
		t.Fatal("a child supervisor claim took the parent's own supervisor run ID")
	}
	if token, ok := parentSup.broker.RunToken(messaging.SupervisorRunID); !ok || token != parentSup.brokerToken {
		t.Fatal("the parent's own supervisor record was overwritten by a child claim")
	}

	// An ask the parent's supervisor sends to one child's supervisor is admitted
	// once here and relayed into that branch's own supervisor queue.
	ctx := context.Background()
	pre, err := parentSup.messaging.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	msgID := newStableMsgID(t)
	sent, err := parentSup.messaging.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: messaging.SupervisorRunID, Sender: "supervisor"},
		To:         messaging.Identity{RunID: childKeys[0]},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       `{"message":"branch ping"}`,
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("ask to a child supervisor = (%+v, %v), want sent", sent, err)
	}
	if edges := parentSup.broker.Store().ActiveCount(); edges != 1 {
		t.Fatalf("the parent holds %d ask edges, want exactly the one cross-branch ask", edges)
	}
	queued := parentSup.broker.GetRun(childKeys[0])
	if queued == nil {
		t.Fatal("the parent has no record for the child supervisor run")
	}

	// The child's supervisor answers over its own broker surface, and the reply
	// follows the hop the ask arrived on.
	if status, body := parentLinkedPost(t, children[0].broker.Addr(), "/send", messaging.SupervisorRunID,
		children[0].brokerToken, map[string]any{
			"from_run_id": messaging.SupervisorRunID,
			"to_run_id":   messaging.SupervisorRunID,
			"type":        "agent_message",
			"reply_to":    msgID,
			"payload":     map[string]any{"message": "branch pong"},
		}); status != http.StatusOK {
		t.Fatalf("child supervisor reply = (%d, %v), want 200", status, body)
	}
	waited, err := parentSup.messaging.Wait(ctx, messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: messaging.SupervisorRunID},
		MessageID:  msgID,
		Generation: pre.Generation,
		Timeout:    5 * time.Second,
	})
	if err != nil || waited.Outcome != messaging.OutcomeReplied || waited.Reply == nil {
		t.Fatalf("wait for the child supervisor's reply = (%+v, %v), want the reply", waited, err)
	}
	if edges := parentSup.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("the parent still holds %d ask edges after the reply", edges)
	}
}

// TestSupervisorWithoutParentBrokerKeepsLocalMessaging pins that an unconfigured
// supervisor installs nothing: no parent link and no parent routes.
func TestSupervisorWithoutParentBrokerKeepsLocalMessaging(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "noparent"), MaxRuntimes: 2})
	t.Cleanup(func() { _ = sup.Shutdown("kill") })
	if sup.parentLink != nil {
		t.Fatal("a supervisor without a parent broker URL installed a parent link")
	}
	status, body := parentLinkedPost(t, sup.broker.Addr(), "/parent/register", messaging.SupervisorRunID,
		sup.brokerToken, map[string]any{"child_id": "other", "endpoint": "http://127.0.0.1:1", "generation": "g"})
	if status != http.StatusNotFound {
		t.Fatalf("standalone broker parent register = (%d, %v), want 404", status, body)
	}
}

// branchKey reports the run ID a test parent broker addresses one of a branch
// broker's runs by.
func branchKey(b *broker.Broker, runID string) string {
	return parentlink.ParentRunKey(parentlink.BranchID("test/"+b.Addr()), runID)
}

// deliveredAsker reports the run ID the relayed ask for msgID arrived from, the
// one a reply has to name for the edge to complete.
func deliveredAsker(t *testing.T, b *broker.Broker, runID, msgID string) string {
	t.Helper()
	st := b.GetRun(runID)
	if st == nil {
		t.Fatalf("run %q is not in this broker", runID)
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	for _, msg := range st.ControlQueue {
		if msg.ID == msgID {
			return msg.FromRunID
		}
	}
	t.Fatalf("message %q was never delivered to run %q", msgID, runID)
	return ""
}

func newStableMsgID(t *testing.T) string {
	t.Helper()
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("mint message id: %v", err)
	}
	return msgID
}

func parentLinkedPost(t *testing.T, addr, path, runID, token string, body map[string]any) (int, map[string]any) {
	t.Helper()
	body["run_id"] = runID
	body["token"] = token
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	resp, err := http.Post("http://"+addr+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}
