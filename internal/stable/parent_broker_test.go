package stable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/parentlink"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

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
		Generation: b.Store().Generation,
	}, b.LocalMessenger())
	if err != nil {
		t.Fatalf("build parent messenger: %v", err)
	}
	b.SetParentLink(link)
	b.SetMessenger(link)
	t.Cleanup(func() { _ = link.Close() })
	return b, link
}

// startTestBranch starts a broker whose runs register with parentBroker, the
// way a nested supervisor is wired. It returns the branch's link so a test can
// inspect the descendant registry the branch serves.
func startTestBranch(t *testing.T, parentBroker *broker.Broker, runs ...string) (*broker.Broker, *parentlink.Messenger, map[string]string) {
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
	var (
		link *parentlink.Messenger
		err  error
	)
	link, err = parentlink.Install(parentlink.Options{
		ParentURL:  "http://" + parentBroker.Addr(),
		Endpoint:   "http://" + b.Addr(),
		Generation: b.Store().Generation,
		Runs: func() []messaging.RunClaim { return b.ParentRunClaims() },
	}, b)
	if err != nil {
		t.Fatalf("build branch messenger: %v", err)
	}
	t.Cleanup(func() { _ = link.Close() })
	if err := link.Start(context.Background()); err != nil {
		t.Fatalf("register branch with parent: %v", err)
	}
	return b, link, tokens
}

// awaitRegistryOwner polls until a registry has adopted runID.
func awaitRegistryOwner(t *testing.T, what string, registry *parentlink.Registry, runID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !registry.Owns(runID) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestThreeTierBranchAttribution pins claim attribution across three tiers: a
// grandchild's run is attributed to the child branch that registered it — at
// the middle tier, which is the registry that saw the registration — and the
// middle's claim to its own parent never carries the grandchild's run, so the
// grandparent's registry records no route for it at all.
func TestThreeTierBranchAttribution(t *testing.T) {
	grandparentBroker, grandparentLink := startTestParentBroker(t)
	middleBroker, middleLink, _ := startTestBranch(t, grandparentBroker, "mid-run")
	childBroker, _, _ := startTestBranch(t, middleBroker, "child-run")
	middleURL := "http://" + middleBroker.Addr()
	childURL := "http://" + childBroker.Addr()

	awaitRegistryOwner(t, "the grandparent to adopt the middle branch's run", grandparentLink.Registry(), "mid-run")
	if hop, ok := grandparentLink.Registry().Hop("mid-run"); !ok || hop.Endpoint != middleURL {
		t.Fatalf("the grandparent routes mid-run to %+v, want the middle branch at %s", hop, middleURL)
	}
	awaitRegistryOwner(t, "the middle branch to adopt the child branch's run", middleLink.Registry(), "child-run")
	if hop, ok := middleLink.Registry().Hop("child-run"); !ok || hop.Endpoint != childURL {
		t.Fatalf("the middle tier routes child-run to %+v, want the child branch at %s", hop, childURL)
	}

	// The middle's re-publish after the child registered is asynchronous, so
	// give the middle a run-set change of its own and wait for that change to
	// reach the grandparent: once the grandparent has adopted the later run, a
	// claim computed after the child registered has landed, and it must not
	// carry the child's run.
	if _, err := middleBroker.CreateRun("mid-run-2", "mid-run-2"); err != nil {
		t.Fatalf("create the middle branch's later run: %v", err)
	}
	awaitRegistryOwner(t, "the grandparent to adopt the middle branch's later run", grandparentLink.Registry(), "mid-run-2")
	if grandparentLink.Registry().Owns("child-run") {
		t.Fatal("the grandparent's registry holds the child's run in the middle branch's claim, want it left to the child branch alone")
	}
}

// TestSupervisorParentTokenWithoutURL pins the token-only hub mode: a
// supervisor configured with a parent token and no parent URL still installs
// the parent link — its broker serves the parent routes and requires that
// credential from every child that registers — while it registers upward
// nowhere.
func TestSupervisorParentTokenWithoutURL(t *testing.T) {
	sup := newTestSupervisor(t, Config{
		ControlSocket:     newStableSocketPath(t, "parent-token"),
		MaxRuntimes:       2,
		BrokerParentToken: "hub-token",
	})
	t.Cleanup(func() { _ = sup.Shutdown("kill") })
	if sup.parentLink == nil {
		t.Fatal("a supervisor with only a parent token has no parent link")
	}

	register := func(token string) int {
		body := fmt.Sprintf(`{"child_id":"child","endpoint":"http://child","generation":%q,"runs":[]}`, sup.broker.Store().Generation())
		req, err := http.NewRequest(http.MethodPost, "http://"+sup.broker.Addr()+"/parent/register", strings.NewReader(body))
		if err != nil {
			t.Fatalf("build register request: %v", err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post /parent/register: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if code := register(""); code != http.StatusUnauthorized {
		t.Fatalf("register without a credential = %d, want 401", code)
	}
	if code := register("wrong"); code != http.StatusUnauthorized {
		t.Fatalf("register with a wrong credential = %d, want 401", code)
	}
	if code := register("hub-token"); code == http.StatusUnauthorized {
		t.Fatalf("register with the hub credential = %d, want it accepted", code)
	}
}

// TestSupervisorParentBrokerLink pins the supervisor's opt-in wiring: with a
// parent broker configured, the supervisor registers its own run there, an ask
// no local run can answer is admitted once at the parent and relayed into the
// branch that owns the target, and the reply correlates back through the
// supervisor's own transport.
func TestSupervisorParentBrokerLink(t *testing.T) {
	parentBroker, parentLink := startTestParentBroker(t)
	peerBroker, _, peerTokens := startTestBranch(t, parentBroker, "rt_remote")
	if !parentLink.Registry().Owns("rt_remote") {
		t.Fatal("the parent never adopted the peer branch's run")
	}

	sup := newTestSupervisor(t, Config{
		ControlSocket:   newStableSocketPath(t, "parent"),
		MaxRuntimes:     2,
		BrokerParentURL: "http://" + parentBroker.Addr(),
	})
	t.Cleanup(func() { _ = sup.Shutdown("kill") })
	if sup.parentLink == nil {
		t.Fatal("a supervisor with a parent broker URL has no parent link")
	}
	if !parentLink.Registry().Owns(messaging.SupervisorRunID) {
		t.Fatal("the supervisor did not register its own run with its parent broker")
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
		To:         messaging.Identity{RunID: "rt_remote"},
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

	// The remote run answers over its own broker surface.
	if status, body := parentLinkedPost(t, peerBroker.Addr(), "/send", "rt_remote", peerTokens["rt_remote"], map[string]any{
		"from_run_id": "rt_remote",
		"to_run_id":   messaging.SupervisorRunID,
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
	if waited.Reply.FromRunID != "rt_remote" {
		t.Fatalf("reply from_run_id = %q, want the canonical target run", waited.Reply.FromRunID)
	}
	if edges := parentBroker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("parent still holds %d ask edges after the reply", edges)
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
