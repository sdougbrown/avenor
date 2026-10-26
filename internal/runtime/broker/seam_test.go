package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/messaging/messagetest"
)

// countAgentMessages reports how many agent messages are queued for runID.
// Publication happens before the handler answers, so a caller observes a
// settled queue without waiting.
func countAgentMessages(t *testing.T, b *Broker, runID string) int {
	t.Helper()
	st := b.GetRun(runID)
	if st == nil {
		t.Fatalf("run %s is not registered", runID)
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	count := 0
	for _, msg := range st.ControlQueue {
		if msg.Type == "agent_message" {
			count++
		}
	}
	return count
}

func startSeamBroker(t *testing.T) (*Broker, string, string) {
	t.Helper()
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	senderToken, err := b.CreateRun("sender", "sender")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("target", "target")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	return b, senderToken, targetToken
}

// TestLocalMessagingSeamServesTheHTTPChildPaths pins that the authenticated
// local HTTP router publishes every messaging path through the broker's seam:
// a tell and an absent-edge reply as deliveries that allocate no ask edge, a
// live-edge reply as an edge completion, an ask under the legacy contract, and
// a withdrawal. Each call keeps its existing wire status and outcome.
func TestLocalMessagingSeamServesTheHTTPChildPaths(t *testing.T) {
	b, senderToken, targetToken := startSeamBroker(t)
	rec := &messagetest.Recorder{Local: b.LocalMessenger()}
	b.SetMessenger(rec)

	status, body := postAuthBroker(t, b.Addr(), "/send", "sender", senderToken, map[string]any{
		"from_run_id": "sender",
		"to_run_id":   "target",
		"type":        "agent_message",
		"payload":     map[string]any{"message": "notice"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("tell = (%d, %v), want 200 sent", status, body)
	}
	delivered := rec.Tells()
	if len(delivered) != 1 || delivered[0].ToRunID != "target" || delivered[0].Kind != "agent_message" {
		t.Fatalf("deliveries = %+v, want the tell through the seam", delivered)
	}
	if edges := b.Store().ActiveCount(); edges != 0 {
		t.Fatalf("tell allocated %d ask edges, want 0", edges)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 1 {
		t.Fatalf("target queue = %d messages, want the single tell", queued)
	}

	// A legacy caller names an unversioned message id without a generation.
	status, body = postAuthBroker(t, b.Addr(), "/send", "sender", senderToken, map[string]any{
		"from_run_id":   "sender",
		"to_run_id":     "target",
		"type":          "agent_message",
		"message_id":    "legacy-ask-1",
		"expects_reply": true,
		"payload":       map[string]any{"message": "ping"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("legacy ask = (%d, %v), want 200 sent", status, body)
	}
	sent := rec.Sends()
	if len(sent) != 1 || !sent[0].Request.Legacy || sent[0].Request.MessageID != "legacy-ask-1" {
		t.Fatalf("sends = %+v, want one legacy ask through the seam", sent)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 2 {
		t.Fatalf("target queue = %d messages, want the tell and the ask", queued)
	}
	if edges := b.Store().ActiveCount(); edges != 1 {
		t.Fatalf("active ask edges = %d, want the legacy ask", edges)
	}
	// A replied edge is retained for duplicate detection, so the reply
	// completes it instead of allocating another one.
	edgesAfterAsk := b.Store().ActiveCount()

	// The ask's target answers the live edge.
	status, body = postAuthBroker(t, b.Addr(), "/send", "target", targetToken, map[string]any{
		"from_run_id": "target",
		"to_run_id":   "sender",
		"type":        "agent_message",
		"reply_to":    "legacy-ask-1",
		"payload":     map[string]any{"message": "all good"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("live reply = (%d, %v), want 200 replied", status, body)
	}
	if replies := rec.Replies(); len(replies) != 1 || replies[0].MessageID != "legacy-ask-1" {
		t.Fatalf("replies = %+v, want the live reply through the seam", replies)
	}
	if edges := b.Store().ActiveCount(); edges != 1 {
		t.Fatalf("active ask edges = %d, want the completed ask", edges)
	}

	// A reply with no live edge degrades to a delivery instead of an edge.
	status, body = postAuthBroker(t, b.Addr(), "/send", "target", targetToken, map[string]any{
		"from_run_id": "target",
		"to_run_id":   "sender",
		"type":        "agent_message",
		"reply_to":    "edge-already-retired",
		"payload":     map[string]any{"message": "late"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeEdgeAbsent) {
		t.Fatalf("absent-edge reply = (%d, %v), want 200 edge-absent", status, body)
	}
	if delivered := rec.Tells(); len(delivered) != 2 || delivered[1].ToRunID != "sender" {
		t.Fatalf("deliveries = %+v, want the absent-edge fallback through the seam", delivered)
	}
	if edges := b.Store().ActiveCount(); edges != edgesAfterAsk {
		t.Fatalf("absent-edge reply allocated an ask edge (%d, want %d)", edges, edgesAfterAsk)
	}
	if queued := countAgentMessages(t, b, "sender"); queued != 1 {
		t.Fatalf("sender queue = %d messages, want the fallback delivery", queued)
	}

	// A withdrawal reaches the seam and retires the edge it names.
	status, body = postAuthBroker(t, b.Addr(), "/send", "sender", senderToken, map[string]any{
		"from_run_id":   "sender",
		"to_run_id":     "target",
		"type":          "agent_message",
		"message_id":    "legacy-ask-2",
		"expects_reply": true,
		"payload":       map[string]any{"message": "cancel me"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("second legacy ask = (%d, %v), want 200 sent", status, body)
	}
	status, body = postAuthBroker(t, b.Addr(), "/cancel_message", "sender", senderToken, map[string]any{
		"cancel_message_id": "legacy-ask-2",
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeWithdrawn) {
		t.Fatalf("withdraw = (%d, %v), want 200 withdrawn", status, body)
	}
	if withdraws := rec.Withdraws(); len(withdraws) != 1 || withdraws[0].MessageID != "legacy-ask-2" {
		t.Fatalf("withdrawals = %+v, want the withdrawal through the seam", withdraws)
	}
	if edges := b.Store().ActiveCount(); edges != 1 {
		t.Fatalf("active ask edges = %d after the withdrawal, want only the completed ask", edges)
	}
}

// TestLocalMessagingSeamAnswersAskTargetLookup pins that the ask target
// pre-check asks the seam, so a host that can reach a target the broker does
// not own decides its reachability.
func TestLocalMessagingSeamAnswersAskTargetLookup(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)
	rec := &messagetest.Recorder{
		Local:         b.LocalMessenger(),
		ReachableFunc: func(string) bool { return false },
	}
	b.SetMessenger(rec)

	status, body := postAuthBroker(t, b.Addr(), "/send", "sender", senderToken, map[string]any{
		"from_run_id":   "sender",
		"to_run_id":     "target",
		"type":          "agent_message",
		"message_id":    "legacy-ask-1",
		"expects_reply": true,
		"payload":       map[string]any{"message": "ping"},
	})
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("ask with an unreachable target = (%d, %v), want 404 target-unavailable", status, body)
	}
	if sends := rec.Sends(); len(sends) != 0 {
		t.Fatalf("sends = %+v, want none for an unreachable target", sends)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 0 {
		t.Fatalf("target queue = %d messages, want none", queued)
	}
}

// seamPeerSource overrides only peer listing, so a test can put a peer the
// broker does not own in front of the /sessions listing while every other
// operation stays local. Its generation closure reports the local endpoint's
// current generation at call time, so a listing that resets the store
// mid-call resolves the post-reset generation instead of an empty or stale
// one.
type seamPeerSource struct {
	messaging.Seam
	peers      []messaging.Peer
	err        error
	generation func() string
	before     func()
}

func (s seamPeerSource) Peers(context.Context, messaging.PeersRequest) (messaging.PeersResult, error) {
	if s.before != nil {
		s.before()
	}
	var generation string
	if s.generation != nil {
		generation = s.generation()
	}
	return messaging.PeersResult{Peers: s.peers, Generation: generation}, s.err
}

// TestSessionsListsSeamPeersForHTTPChildren pins that the authenticated
// /sessions listing a local HTTP child reads comes from the installed
// messenger: it carries the peers only that messenger can see while every
// local peer keeps its recorded metadata.
func TestSessionsListsSeamPeersForHTTPChildren(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)
	b.UpdateSessionInfo("target", &SessionInfo{
		Backend: "seam-backend", Model: "seam-model", Dir: "/work", Status: "thinking", Kind: "agent",
	})

	local, err := b.LocalMessenger().Peers(context.Background(), messaging.PeersRequest{Generation: b.Store().Generation()})
	if err != nil {
		t.Fatalf("local peers: %v", err)
	}
	remoteSession, err := json.Marshal(SessionInfo{RunID: "remote-peer", Label: "remote", LastSeen: 1700000000})
	if err != nil {
		t.Fatalf("remote session: %v", err)
	}
	b.SetMessenger(seamPeerSource{
		Seam:       b.LocalMessenger(),
		peers:      append(local.Peers, messaging.Peer{RunID: "remote-peer", Session: remoteSession}),
		generation: func() string { return b.Store().Generation() },
	})
	t.Cleanup(func() { b.SetMessenger(nil) })

	child, err := localhttp.New(b.Addr(), messaging.LocalCredentials{RunID: "sender", Token: senderToken})
	if err != nil {
		t.Fatalf("local transport: %v", err)
	}
	defer child.Close()

	ctx := context.Background()
	pre, err := child.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	listed, err := child.Peers(ctx, messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: "sender"},
		Generation: pre.Generation,
	})
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	byRunID := make(map[string]SessionInfo, len(listed.Peers))
	for _, peer := range listed.Peers {
		var info SessionInfo
		if err := json.Unmarshal(peer.Session, &info); err != nil {
			t.Fatalf("peer %s session = %s (%v)", peer.RunID, peer.Session, err)
		}
		byRunID[peer.RunID] = info
	}
	remote, ok := byRunID["remote-peer"]
	if !ok {
		t.Fatalf("peers = %+v, want the messenger-provided remote peer", listed.Peers)
	}
	if remote.Label != "remote" || remote.LastSeen != 1700000000 {
		t.Fatalf("remote peer = %+v, want the record the messenger reported", remote)
	}
	target, ok := byRunID["target"]
	if !ok {
		t.Fatalf("peers = %+v, want the local target", listed.Peers)
	}
	if target.Label != "target" || target.Kind != "agent" || target.Backend != "seam-backend" ||
		target.Model != "seam-model" || target.Dir != "/work" || target.Status != "thinking" {
		t.Fatalf("target peer = %+v, want its recorded metadata", target)
	}
	if target.LastSeen <= 0 {
		t.Fatalf("target last_seen = %d, want the local record's timestamp", target.LastSeen)
	}
}

// TestSessionsReportsAnIncompletePeerListing pins that a peer listing the
// installed messenger could not complete is reported instead of reading as a
// complete list, while the generation and protocol marker a preflight needs
// stay available.
func TestSessionsReportsAnIncompletePeerListing(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)
	b.SetMessenger(seamPeerSource{
		Seam:       b.LocalMessenger(),
		peers:      []messaging.Peer{{RunID: "only-peer"}},
		err:        errors.New("parent link down"),
		generation: func() string { return b.Store().Generation() },
	})
	t.Cleanup(func() { b.SetMessenger(nil) })

	child, err := localhttp.New(b.Addr(), messaging.LocalCredentials{RunID: "sender", Token: senderToken})
	if err != nil {
		t.Fatalf("local transport: %v", err)
	}
	defer child.Close()

	ctx := context.Background()
	pre, err := child.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if pre.Generation == "" || pre.Generation != b.Store().Generation() {
		t.Fatalf("preflight generation = %q, want %q", pre.Generation, b.Store().Generation())
	}
	listed, err := child.Peers(ctx, messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: "sender"},
		Generation: pre.Generation,
	})
	if !errors.Is(err, messaging.ErrIncompleteListing) {
		t.Fatalf("peers = (%+v, %v), want ErrIncompleteListing for the incomplete listing", listed, err)
	}
	if listed.Generation != pre.Generation {
		t.Fatalf("peers generation = %q, want %q", listed.Generation, pre.Generation)
	}
	if len(listed.Peers) == 0 {
		t.Fatalf("peers = %+v, want the retained partial peer", listed.Peers)
	}
	var record messaging.PeerRecord
	retained := false
	for _, peer := range listed.Peers {
		if peer.RunID != "only-peer" {
			continue
		}
		retained = true
		if err := json.Unmarshal(peer.Session, &record); err != nil {
			t.Fatalf("partial peer session = %s (%v)", peer.Session, err)
		}
	}
	if !retained || record.RunID != "only-peer" {
		t.Fatalf("partial peer session = %+v, want only-peer", listed.Peers)
	}
	if strings.Contains(err.Error(), "parent link down") {
		t.Fatalf("peers error = %v, want the seam's own failure text kept out of the served listing", err)
	}
}

// TestSessionsDropsAnUnreadablePeerRecord pins that a peer whose session
// record cannot be read is dropped from the listing and the listing reports
// that it could not read every peer, so a partial list never reads as a
// complete one.
func TestSessionsDropsAnUnreadablePeerRecord(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)
	b.SetMessenger(seamPeerSource{
		Seam:       b.LocalMessenger(),
		generation: func() string { return b.Store().Generation() },
		peers: []messaging.Peer{
			{RunID: "readable-peer", Session: json.RawMessage(`{"run_id":"readable-peer"}`)},
			{RunID: "unreadable-peer", Session: json.RawMessage(`not json`)},
		},
	})
	t.Cleanup(func() { b.SetMessenger(nil) })

	child, err := localhttp.New(b.Addr(), messaging.LocalCredentials{RunID: "sender", Token: senderToken})
	if err != nil {
		t.Fatalf("local transport: %v", err)
	}
	defer child.Close()

	ctx := context.Background()
	pre, err := child.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	listed, err := child.Peers(ctx, messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: "sender"},
		Generation: pre.Generation,
	})
	if err == nil {
		t.Fatalf("peers = %+v, want the incomplete listing reported", listed.Peers)
	}
	if !strings.Contains(err.Error(), "could not read every peer") {
		t.Fatalf("peers error = %v, want the listing's own reason", err)
	}
	for _, peer := range listed.Peers {
		if peer.RunID == "unreadable-peer" {
			t.Fatalf("peers = %+v, want the unreadable peer dropped", listed.Peers)
		}
	}
}

// TestSessionsReportsTheListingGeneration pins that the /sessions
// generation is the one the peer listing resolved, not the one the handler
// read before it asked: a seam that resets the store while listing reports
// the post-reset generation.
func TestSessionsReportsTheListingGeneration(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)

	pre := b.Store().Generation()
	var listed string
	b.SetMessenger(seamPeerSource{
		Seam:  b.LocalMessenger(),
		peers: []messaging.Peer{{RunID: "only-peer"}},
		generation: func() string {
			listed = b.Store().Generation()
			return listed
		},
		before: func() { b.Store().Reset() },
	})
	t.Cleanup(func() { b.SetMessenger(nil) })

	resp, err := getSessions(b.Addr(), "sender", senderToken)
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sessions status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Generation string `json:"generation"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if listed == "" || listed == pre {
		t.Fatalf("the seam resolved generation %q, want the post-reset generation distinct from %q", listed, pre)
	}
	if body.Generation != listed {
		t.Fatalf("sessions generation = %q, want the listing's %q", body.Generation, listed)
	}
}

// TestRoutedOwnerNoReplySendDeliversWithoutAnEdge pins the owner's routed
// no-reply send: the message is delivered once, allocates no ask edge, and a
// wait on it reports an absent edge instead of waiting for a reply nobody
// asked for.
func TestRoutedOwnerNoReplySendDeliversWithoutAnEdge(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	transport := b.MessengerTransport()
	t.Cleanup(func() { _ = transport.Close() })

	ctx := context.Background()
	pre, err := transport.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	sent, err := transport.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: messaging.SupervisorRunID},
		To:         messaging.Identity{RunID: "target"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       `{"message":"notice"}`,
		NoReply:    true,
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("no-reply send = (%+v, %v), want sent", sent, err)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 1 {
		t.Fatalf("target queue = %d messages, want the single no-reply delivery", queued)
	}
	if edges := b.Store().ActiveCount(); edges != 0 {
		t.Fatalf("no-reply send allocated %d ask edges, want 0", edges)
	}
	waited, err := transport.Wait(ctx, messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: messaging.SupervisorRunID},
		MessageID:  msgID,
		Generation: pre.Generation,
		Timeout:    time.Millisecond,
	})
	if err != nil || waited.Outcome != messaging.OutcomeEdgeAbsent {
		t.Fatalf("wait = (%+v, %v), want an absent edge", waited, err)
	}
}

// TestLocalMessengerKeepsAskAdmissionContracts pins the one admission
// difference between the broker's two local callers: the legacy HTTP router
// admits an unversioned message id, and a versioned caller does not.
func TestLocalMessengerKeepsAskAdmissionContracts(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	ctx := context.Background()
	req := messaging.SendRequest{
		From:      messaging.Identity{RunID: "sender", Sender: "sender"},
		To:        messaging.Identity{RunID: "target", Sender: "target"},
		MessageID: "unversioned-ask",
		Body:      `{"message":"ping"}`,
	}

	res, err := b.LocalMessenger().Send(ctx, req)
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("versioned send of an unversioned id = (%+v, %v), want a generation mismatch", res, err)
	}

	req.Legacy = true
	res, err = b.LocalMessenger().Send(ctx, req)
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("legacy send of an unversioned id = (%+v, %v), want sent", res, err)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 1 {
		t.Fatalf("target queue = %d messages, want the single legacy ask", queued)
	}
}

// TestLocalMessengerReplyReportsTheStoreGeneration pins the direct seam's
// reply result: it carries the store's current generation, including after a
// reset rotates it, so a reply result never reads as an edge no generation
// ever admitted.
func TestLocalMessengerReplyReportsTheStoreGeneration(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	ctx := context.Background()

	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	gen := b.Store().Generation()
	if _, err := b.LocalMessenger().Send(ctx, messaging.SendRequest{
		From:      messaging.Identity{RunID: "sender", Sender: "sender"},
		To:        messaging.Identity{RunID: "target", Sender: "target"},
		MessageID: msgID, Generation: gen, Body: `{"message":"ping"}`,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	replied, err := b.LocalMessenger().Reply(ctx, messaging.ReplyRequest{
		From: messaging.Identity{RunID: "target", Sender: "target"}, ToRunID: "sender", MessageID: msgID, Body: `{"message":"pong"}`,
	})
	if err != nil || replied.Outcome != messaging.OutcomeReplied {
		t.Fatalf("Reply = (%+v, %v), want the ask answered", replied, err)
	}
	if replied.Generation != gen {
		t.Fatalf("Reply generation = %q, want the store's %q", replied.Generation, gen)
	}

	// After a reset the edge is gone, but the result still carries the
	// store's rotated generation rather than an emptied one.
	b.Store().Reset()
	rotated := b.Store().Generation()
	replied, err = b.LocalMessenger().Reply(ctx, messaging.ReplyRequest{
		From: messaging.Identity{RunID: "target", Sender: "target"}, ToRunID: "sender", MessageID: msgID, Body: `{"message":"pong"}`,
	})
	if err != nil || replied.Outcome != messaging.OutcomeEdgeAbsent {
		t.Fatalf("Reply after reset = (%+v, %v), want edge-absent", replied, err)
	}
	if replied.Generation != rotated {
		t.Fatalf("Reply generation after reset = %q, want the store's rotated %q", replied.Generation, rotated)
	}
}

// TestLocalMessengerReceiveDrainsBoundedAgentMessages pins the direct
// seam's bounded drain: a named limit returns the head of the agent queue
// with its identities, payloads and order intact and leaves the remainder
// queued, and an absent limit clamps to the default bound.
func TestLocalMessengerReceiveDrainsBoundedAgentMessages(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	ctx := context.Background()

	const total = 40
	for i := 0; i < total; i++ {
		payload, err := json.Marshal(AgentMessage{
			ID: fmt.Sprintf("drain-%02d", i), From: "sender", FromRunID: "sender", ToRunID: "target", Message: fmt.Sprintf("body-%02d", i),
		})
		if err != nil {
			t.Fatalf("payload: %v", err)
		}
		if err := b.SendTo("sender", "target", "agent_message", payload, fmt.Sprintf("drain-%02d", i)); err != nil {
			t.Fatalf("queue message: %v", err)
		}
	}
	if queued := countAgentMessages(t, b, "target"); queued != total {
		t.Fatalf("target queue = %d messages, want %d", queued, total)
	}

	res, err := b.LocalMessenger().Receive(ctx, messaging.ReceiveRequest{
		Sender: messaging.Identity{RunID: "target"},
		Limit:  5,
	})
	if err != nil {
		t.Fatalf("bounded receive: %v", err)
	}
	if len(res.Messages) != 5 {
		t.Fatalf("bounded receive = %d messages, want 5", len(res.Messages))
	}
	for i, msg := range res.Messages {
		if msg.MessageID != fmt.Sprintf("drain-%02d", i) || msg.FromRunID != "sender" || msg.Sender != "sender" || msg.Kind != "agent_message" {
			t.Fatalf("drained[%d] = %+v, want the %dth queued message in order", i, msg, i)
		}
		var am AgentMessage
		if err := json.Unmarshal(msg.Payload, &am); err != nil || am.Message != fmt.Sprintf("body-%02d", i) {
			t.Fatalf("drained[%d] payload = %s (%v), want body-%02d", i, msg.Payload, err, i)
		}
	}
	if res.Generation != b.Store().Generation() {
		t.Fatalf("receive generation = %q, want %q", res.Generation, b.Store().Generation())
	}
	if queued := countAgentMessages(t, b, "target"); queued != total-5 {
		t.Fatalf("target queue after the bounded drain = %d messages, want the %d remainder", queued, total-5)
	}

	// An absent limit clamps to the default bound: 35 queued, 32 drained, 3 left.
	res, err = b.LocalMessenger().Receive(ctx, messaging.ReceiveRequest{Sender: messaging.Identity{RunID: "target"}})
	if err != nil {
		t.Fatalf("default receive: %v", err)
	}
	if len(res.Messages) != messaging.DefaultReceiveLimit {
		t.Fatalf("default receive = %d messages, want the %d clamp", len(res.Messages), messaging.DefaultReceiveLimit)
	}
	for i, msg := range res.Messages {
		if msg.MessageID != fmt.Sprintf("drain-%02d", 5+i) {
			t.Fatalf("drained[%d] = %q, want drain-%02d in order", i, msg.MessageID, 5+i)
		}
	}
	if queued := countAgentMessages(t, b, "target"); queued != total-5-messaging.DefaultReceiveLimit {
		t.Fatalf("target queue after the default drain = %d messages, want the remainder", queued)
	}
}

// TestLocalMessengerPreflightReportsTheStoreGeneration pins that the direct
// seam's preflight reports the shared protocol and the store's actual
// generation, following a reset that rotates it.
func TestLocalMessengerPreflightReportsTheStoreGeneration(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	ctx := context.Background()

	pre, err := b.LocalMessenger().Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if pre.Protocol != messaging.ProtocolVersion {
		t.Fatalf("preflight protocol = %d, want %d", pre.Protocol, messaging.ProtocolVersion)
	}
	if pre.Generation == "" || pre.Generation != b.Store().Generation() {
		t.Fatalf("preflight generation = %q, want the store's %q", pre.Generation, b.Store().Generation())
	}

	b.Store().Reset()
	if b.Store().Generation() == pre.Generation {
		t.Fatalf("reset did not rotate the generation: %q", pre.Generation)
	}

	post, err := b.LocalMessenger().Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight after reset: %v", err)
	}
	if post.Protocol != messaging.ProtocolVersion {
		t.Fatalf("preflight protocol after reset = %d, want %d", post.Protocol, messaging.ProtocolVersion)
	}
	if post.Generation != b.Store().Generation() || post.Generation == pre.Generation {
		t.Fatalf("preflight after reset = %q, want the store's rotated %q", post.Generation, b.Store().Generation())
	}
}

// TestLocalMessengerCloseLeavesTheBrokerUsable pins that closing the direct
// seam releases nothing: the store and the queues stay usable.
func TestLocalMessengerCloseLeavesTheBrokerUsable(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	ctx := context.Background()

	if err := b.LocalMessenger().Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	payload, err := json.Marshal(AgentMessage{ID: "after-close", From: "sender", FromRunID: "sender", ToRunID: "target", Message: "still works"})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	if err := b.LocalMessenger().Tell(ctx, messaging.TellRequest{FromRunID: "sender", ToRunID: "target", Kind: "agent_message", Payload: payload}); err != nil {
		t.Fatalf("tell after close: %v", err)
	}
	res, err := b.LocalMessenger().Receive(ctx, messaging.ReceiveRequest{Sender: messaging.Identity{RunID: "target"}, Limit: 1})
	if err != nil || len(res.Messages) != 1 || res.Messages[0].MessageID != "after-close" {
		t.Fatalf("receive after close = (%+v, %v), want the queued message", res, err)
	}
}

// TestMessengerTransportResolvesTheSeamPerCall pins that the in-process
// transport built for the owner enters the messenger installed after it was
// built, and that its ask lifecycle stays joined to the store's generation.
func TestMessengerTransportResolvesTheSeamPerCall(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	transport := b.MessengerTransport()
	t.Cleanup(func() { _ = transport.Close() })

	rec := &messagetest.Recorder{Local: b.LocalMessenger()}
	b.SetMessenger(rec)

	ctx := context.Background()
	pre, err := transport.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	res, err := transport.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "sender"},
		To:         messaging.Identity{RunID: "target"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       `{"message":"ping"}`,
	})
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("send = (%+v, %v), want sent", res, err)
	}
	sent := rec.Sends()
	if len(sent) != 1 || sent[0].Request.MessageID != msgID || sent[0].Request.Legacy {
		t.Fatalf("sends = %+v, want one versioned ask through the seam", sent)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 1 {
		t.Fatalf("target queue = %d messages, want the single ask", queued)
	}

	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	waited, err := transport.Wait(waitCtx, messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: "sender"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Timeout:    50 * time.Millisecond,
	})
	if err != nil || waited.Outcome != messaging.OutcomeTimedOut {
		t.Fatalf("wait = (%+v, %v), want a bounded timeout", waited, err)
	}
	if waits := rec.Waits(); len(waits) != 1 || waits[0].MessageID != msgID {
		t.Fatalf("waits = %+v, want the wait through the seam", waits)
	}
}
