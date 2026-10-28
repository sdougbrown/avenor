package stable

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/parentlink"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// testHost reports the host component the supervisor mixes into its branch
// identity, so a test that derives an expected identity matches the real one.
func testHost() string {
	host, _ := os.Hostname()
	return host
}

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
		Generation: b.Store().Generation,
		RetireRun:  func(runID, token string) { b.DeleteRunIfToken(runID, token) },
	}, b.LocalMessenger())
	if err != nil {
		t.Fatalf("build parent messenger: %v", err)
	}
	b.InstallParentLink(link, testLinkToken, nil)
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
	link, err = parentlink.New(parentlink.Options{
		ParentURL:  "http://" + parentBroker.Addr(),
		Token:      testLinkToken,
		ChildID:    parentlink.BranchID("test/"+b.Addr(), testHost()),
		Endpoint:   "http://" + b.Addr(),
		Generation: b.Store().Generation,
		Runs: func() []messaging.RunClaim {
			return b.ParentRunClaims()
		},
		// A relay carries the asker's resolved name, which the receiving branch
		// cannot resolve for itself.
		DisplayName: b.DisplayName,
		// A run this branch stops owning loses its broker record too, so a
		// refused descendant stops being served locally.
		RetireRun: func(runID, token string) { b.DeleteRunIfToken(runID, token) },
	}, b.LocalMessenger())
	if err != nil {
		t.Fatalf("build branch messenger: %v", err)
	}
	b.InstallParentLink(link, testLinkToken, link.RunChanged)
	b.SetMessenger(link)
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

	awaitRegistryOwner(t, "the grandparent to adopt the middle branch's run", grandparentLink.Registry(), branchKey(middleBroker, "mid-run"))
	if hop, ok := grandparentLink.Registry().Hop(branchKey(middleBroker, "mid-run")); !ok || hop.Endpoint != middleURL {
		t.Fatalf("the grandparent routes mid-run to %+v, want the middle branch at %s", hop, middleURL)
	}
	awaitRegistryOwner(t, "the middle branch to adopt the child branch's run", middleLink.Registry(), branchKey(childBroker, "child-run"))
	if hop, ok := middleLink.Registry().Hop(branchKey(childBroker, "child-run")); !ok || hop.Endpoint != childURL {
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
	awaitRegistryOwner(t, "the grandparent to adopt the middle branch's later run", grandparentLink.Registry(), branchKey(middleBroker, "mid-run-2"))
	if grandparentLink.Registry().Owns(branchKey(childBroker, "child-run")) {
		t.Fatal("the grandparent's registry holds the child's run in the middle branch's claim, want it left to the child branch alone")
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
	if branchSupervisorRun := messaging.SupervisorRunID + "@" + parentlink.BranchID(sup.config.ControlSocket, testHost()); !parentLink.Registry().Owns(branchSupervisorRun) {
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
	if want := messaging.SupervisorRunID + "@" + parentlink.BranchID(sup.config.ControlSocket, testHost()); asker != want {
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
		childKeys = append(childKeys, messaging.SupervisorRunID+"@"+parentlink.BranchID(socket, testHost()))
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
	st := parentSup.broker.GetRun(messaging.SupervisorRunID)
	if st == nil || st.Token != parentSup.brokerToken {
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
	return parentlink.ParentRunKey(parentlink.BranchID("test/"+b.Addr(), testHost()), runID)
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

// TestAdvertisedBrokerURLIsTheParentRelayEndpoint pins the routing side of an
// advertised address: the branch registers the URL a TLS terminator publishes
// rather than the address it bound, and a relay the parent posts to that URL
// still reaches the run. The terminator here is a reverse proxy in front of the
// bound address, so a regression back to the bound address leaves the proxy
// unused.
func TestAdvertisedBrokerURLIsTheParentRelayEndpoint(t *testing.T) {
	parentBroker, parentLink := startTestParentBroker(t)
	bindAddr := reserveStableLoopbackAddr(t)

	var relays atomic.Int64
	terminator := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: bindAddr})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/relay" {
			relays.Add(1)
		}
		terminator.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)

	socketPath := newStableSocketPath(t, "advertised-relay")
	sup := newTestSupervisor(t, Config{
		ControlSocket:      socketPath,
		MaxRuntimes:        2,
		BrokerAddr:         bindAddr,
		BrokerAdvertiseURL: proxy.URL,
		BrokerParentURL:    "http://" + parentBroker.Addr(),
		BrokerParentToken:  testLinkToken,
	})
	t.Cleanup(func() { _ = sup.Shutdown("kill") })

	branchID := parentlink.BranchID(socketPath, testHost())
	supervisorKey := messaging.SupervisorRunID + "@" + branchID
	awaitRegistryOwner(t, "the parent to adopt the advertised branch", parentLink.Registry(), supervisorKey)
	hop, ok := parentLink.Registry().Hop(supervisorKey)
	if !ok {
		t.Fatalf("the parent holds no hop for %q", supervisorKey)
	}
	if hop.Endpoint != proxy.URL {
		t.Fatalf("registered endpoint = %q, want the advertised %q", hop.Endpoint, proxy.URL)
	}

	if _, err := sup.broker.CreateRun("rt_advertised", "advertised"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	runKey := "rt_advertised@" + branchID
	awaitRegistryOwner(t, "the parent to adopt the advertised branch's run", parentLink.Registry(), runKey)

	if _, err := parentBroker.CreateRun("rt_asker", "asker"); err != nil {
		t.Fatalf("create the asker run: %v", err)
	}
	transport := parentBroker.MessengerTransport()
	t.Cleanup(func() { _ = transport.Close() })
	ctx := context.Background()
	pre, err := transport.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	msgID := newStableMsgID(t)
	sent, err := transport.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "rt_asker", Sender: "asker"},
		To:         messaging.Identity{RunID: runKey},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       `{"message":"through the terminator"}`,
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("cross-branch ask = (%+v, %v), want sent", sent, err)
	}
	if asker := deliveredAsker(t, sup.broker, "rt_advertised", msgID); asker != "rt_asker" {
		t.Fatalf("delivered asker = %q, want the asker the parent relayed to", asker)
	}
	if got := relays.Load(); got != 1 {
		t.Fatalf("the terminator forwarded %d relay requests, want exactly 1", got)
	}
}
