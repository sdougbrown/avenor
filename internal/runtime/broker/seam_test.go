package broker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/inprocess"
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
	delivered := rec.Delivers()
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
	if len(sent) != 1 || !sent[0].Legacy || sent[0].Request.MessageID != "legacy-ask-1" {
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
	if delivered := rec.Delivers(); len(delivered) != 2 || delivered[1].ToRunID != "sender" {
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
// operation stays local.
type seamPeerSource struct {
	messaging.Messenger
	peers []messaging.Peer
	err   error
}

func (s seamPeerSource) Peers(context.Context) ([]messaging.Peer, error) { return s.peers, s.err }

// storePeerSource lists its peers through the public ask store, the way a host
// seam that composes the broker's local peers with the ones it reaches would.
type storePeerSource struct {
	messaging.Messenger
	broker *Broker
}

func (s storePeerSource) Peers(context.Context) ([]messaging.Peer, error) {
	return s.broker.Store().Peers()
}

// TestStorePeersDoNotRecurseThroughTheSeam pins that the ask store's peer
// source is the broker's own registry. A seam that composes its local peers
// from the public store would otherwise call the store back into itself.
func TestStorePeersDoNotRecurseThroughTheSeam(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	b.SetMessenger(storePeerSource{Messenger: b.LocalMessenger(), broker: b})
	t.Cleanup(func() { b.SetMessenger(nil) })

	peers, err := b.Store().Peers()
	if err != nil {
		t.Fatalf("store peers: %v", err)
	}
	if len(peers) == 0 {
		t.Fatalf("store peers = %+v, want the broker's own runs", peers)
	}
}

// TestSessionsPublishesTheSeamPeerID pins that the identity a peer listing
// publishes is the id the seam named for it. A session record's own run_id is
// the record's content, so a peer that names another run in its record is
// still listed under the id the seam gave it.
func TestSessionsPublishesTheSeamPeerID(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)
	spoofed, err := json.Marshal(SessionInfo{RunID: "target", Label: "spoofed"})
	if err != nil {
		t.Fatalf("spoofed session: %v", err)
	}
	b.SetMessenger(seamPeerSource{
		Messenger: b.LocalMessenger(),
		peers:     []messaging.Peer{{RunID: "real-peer", Session: spoofed}},
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
	found := 0
	for _, peer := range listed.Peers {
		if peer.RunID != "real-peer" {
			continue
		}
		found++
		var record messaging.PeerRecord
		if err := json.Unmarshal(peer.Session, &record); err != nil {
			t.Fatalf("peer session = %s (%v)", peer.Session, err)
		}
		if record.RunID != "real-peer" {
			t.Fatalf("published peer record run id = %q, want the seam's id", record.RunID)
		}
	}
	if found != 1 {
		t.Fatalf("peers = %+v, want the seam's peer listed once", listed.Peers)
	}
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

	local, err := b.LocalMessenger().Peers(context.Background())
	if err != nil {
		t.Fatalf("local peers: %v", err)
	}
	remoteSession, err := json.Marshal(SessionInfo{RunID: "remote-peer", Label: "remote", LastSeen: 1700000000})
	if err != nil {
		t.Fatalf("remote session: %v", err)
	}
	b.SetMessenger(seamPeerSource{
		Messenger: b.LocalMessenger(),
		peers:     append(local, messaging.Peer{RunID: "remote-peer", Session: remoteSession}),
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
		Messenger: b.LocalMessenger(),
		peers:     []messaging.Peer{{RunID: "only-peer"}},
		err:       errors.New("parent link down"),
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
	if !strings.Contains(err.Error(), "could not read every peer") {
		t.Fatalf("peers error = %v, want the listing's own reason", err)
	}
}

// TestSessionsKeepsTheBrokersOwnRuns pins that the listing carries the runs
// this broker owns even when the installed messenger reports only the peers
// it reaches. A child that addresses a local run by id must still find it.
func TestSessionsKeepsTheBrokersOwnRuns(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)
	b.SetMessenger(seamPeerSource{
		Messenger: b.LocalMessenger(),
		peers:     []messaging.Peer{{RunID: "remote-peer"}},
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
	byRunID := make(map[string]bool, len(listed.Peers))
	for _, peer := range listed.Peers {
		byRunID[peer.RunID] = true
	}
	for _, want := range []string{"sender", "target", "remote-peer"} {
		if !byRunID[want] {
			t.Fatalf("peers = %+v, want %s listed", listed.Peers, want)
		}
	}
}

// TestRoutedTransportListsTheBrokersOwnRuns pins that the transport the
// broker serves peers through names the runs the broker owns beside the peers
// the seam can see, so every listing the broker serves names the same
// participants.
func TestRoutedTransportListsTheBrokersOwnRuns(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	b.SetMessenger(seamPeerSource{
		Messenger: b.LocalMessenger(),
		peers:     []messaging.Peer{{RunID: "remote-peer"}},
	})
	t.Cleanup(func() { b.SetMessenger(nil) })

	transport := b.MessengerTransport()
	t.Cleanup(func() { _ = transport.Close() })

	ctx := context.Background()
	pre, err := transport.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	listed, err := transport.Peers(ctx, messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: "sender"},
		Generation: pre.Generation,
	})
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	byRunID := make(map[string]bool, len(listed.Peers))
	for _, peer := range listed.Peers {
		byRunID[peer.RunID] = true
	}
	for _, want := range []string{"sender", "target", "remote-peer"} {
		if !byRunID[want] {
			t.Fatalf("peers = %+v, want %s listed", listed.Peers, want)
		}
	}
}

// TestStoreOnlyTransportAndLocalMessengerAgree pins that the transport that
// serves messaging from the store alone and the broker's own messenger answer
// the same ask the same way, so the store's ask contract has one owner.
func TestStoreOnlyTransportAndLocalMessengerAgree(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	transport := inprocess.New(b.Store())
	t.Cleanup(func() { _ = transport.Close() })
	messenger := b.LocalMessenger()

	ctx := context.Background()
	gen := b.Store().Generation()
	now := time.Now().UTC()

	outcomes := func(t *testing.T, ask string) ([3]messaging.Outcome, string) {
		t.Helper()
		msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
		if err != nil {
			t.Fatalf("message id: %v", err)
		}
		sent, err := transport.Send(ctx, messaging.SendRequest{
			From: messaging.Identity{RunID: "sender"}, To: messaging.Identity{RunID: "target"},
			MessageID: msgID, Generation: gen, Body: `{"message":"ping"}`,
		})
		if err != nil {
			t.Fatalf("%s send: %v", ask, err)
		}
		replied, err := transport.Reply(ctx, messaging.ReplyRequest{
			From: messaging.Identity{RunID: "target"}, ToRunID: "sender", MessageID: msgID, Body: `{"message":"pong"}`,
		})
		if err != nil {
			t.Fatalf("%s reply: %v", ask, err)
		}
		waited, err := transport.Wait(ctx, messaging.WaitRequest{
			Sender: messaging.Identity{RunID: "sender"}, MessageID: msgID, Generation: gen, Timeout: time.Second,
		})
		if err != nil {
			t.Fatalf("%s wait: %v", ask, err)
		}
		payload := ""
		if waited.Reply != nil {
			payload = string(waited.Reply.Payload)
		}
		return [3]messaging.Outcome{sent.Outcome, replied.Outcome, waited.Outcome}, payload
	}

	seamOutcomes := func(t *testing.T, ask string) ([3]messaging.Outcome, string) {
		t.Helper()
		msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
		if err != nil {
			t.Fatalf("message id: %v", err)
		}
		sent, err := messenger.Send(ctx, messaging.SendRequest{
			From: messaging.Identity{RunID: "sender"}, To: messaging.Identity{RunID: "target"},
			MessageID: msgID, Generation: gen, Body: `{"message":"ping"}`,
		}, false)
		if err != nil {
			t.Fatalf("%s send: %v", ask, err)
		}
		replied, err := messenger.Reply(ctx, messaging.ReplyRequest{
			From: messaging.Identity{RunID: "target"}, ToRunID: "sender", MessageID: msgID, Body: `{"message":"pong"}`,
		})
		if err != nil {
			t.Fatalf("%s reply: %v", ask, err)
		}
		waited, err := messenger.Wait(ctx, messaging.WaitRequest{
			Sender: messaging.Identity{RunID: "sender"}, MessageID: msgID, Generation: gen, Timeout: time.Second,
		})
		if err != nil {
			t.Fatalf("%s wait: %v", ask, err)
		}
		payload := ""
		if waited.Reply != nil {
			payload = string(waited.Reply.Payload)
		}
		return [3]messaging.Outcome{sent.Outcome, replied, waited.Outcome}, payload
	}

	fromStore, storePayload := outcomes(t, "store-only")
	fromSeam, seamPayload := seamOutcomes(t, "local messenger")
	if fromStore != fromSeam {
		t.Fatalf("store-only outcomes = %v, local messenger outcomes = %v, want the same", fromStore, fromSeam)
	}
	if storePayload != seamPayload {
		t.Fatalf("store-only payload = %q, local messenger payload = %q, want the same", storePayload, seamPayload)
	}
	if storePayload != `{"message":"pong"}` {
		t.Fatalf("reply payload = %q, want the reply body the caller sent", storePayload)
	}
	if fromStore[0] != messaging.OutcomeSent || fromStore[1] != messaging.OutcomeReplied ||
		fromStore[2] != messaging.OutcomeReplied {
		t.Fatalf("outcomes = %v, want sent, replied, replied", fromStore)
	}
}

// TestSessionsDropsAnUnreadablePeerRecord pins that a peer whose session
// record cannot be read is dropped from the listing and the listing reports
// that it could not read every peer, so a partial list never reads as a
// complete one.
func TestSessionsDropsAnUnreadablePeerRecord(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)
	b.SetMessenger(seamPeerSource{
		Messenger: b.LocalMessenger(),
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

// TestRoutedOwnerNoReplySendDeliversWithoutAnEdge pins the owner's routed
// no-reply send: the message is delivered once, allocates no ask edge, and a
// wait on it reports an absent edge instead of waiting for a reply nobody
// asked for.
// answeringWaiter answers a wait at once, so the broker's wait path is
// exercised without waiting for a reply that will not come.
type answeringWaiter struct {
	messaging.Messenger
	waited messaging.WaitRequest
}

func (w *answeringWaiter) Wait(_ context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	w.waited = req
	return messaging.WaitResult{
		Outcome: messaging.OutcomeReplied,
		Reply: &messaging.InboundMessage{
			MessageID: req.MessageID,
			FromRunID: "target",
			Sender:    "target",
			Kind:      "agent_message",
			Payload:   json.RawMessage(`{"message":"pong"}`),
		},
	}, nil
}

// TestLocalMessagingSeamServesTheHTTPWaitPath pins that the authenticated
// /wait_reply path waits through the broker's seam, so an ask the seam routed
// is the ask the caller waits for.
func TestLocalMessagingSeamServesTheHTTPWaitPath(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)
	waiter := &answeringWaiter{}
	rec := &messagetest.Recorder{Local: waiter}
	b.SetMessenger(rec)
	t.Cleanup(func() { b.SetMessenger(nil) })

	status, body := postAuthBroker(t, b.Addr(), "/wait_reply", "sender", senderToken, map[string]any{
		"waiting_for": "legacy-ask-1",
		"generation":  b.Store().Generation(),
		"timeout_ms":  1000,
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("wait = (%d, %v), want 200 replied", status, body)
	}
	waits := rec.Waits()
	if len(waits) != 1 || waits[0].MessageID != "legacy-ask-1" {
		t.Fatalf("waits = %+v, want the wait through the seam", waits)
	}
	if waits[0].Sender.RunID != "sender" {
		t.Fatalf("wait sender = %+v, want the calling run", waits[0].Sender)
	}
	if waiter.waited.MessageID != "legacy-ask-1" {
		t.Fatalf("seam wait = %+v, want the message id the caller waited for", waiter.waited)
	}
}

// TestSetMessengerRestoresTheLocalMessenger pins that a nil messenger restores
// the broker's own implementation, so a host that takes its seam back leaves
// the broker serving its own runs.
func TestSetMessengerRestoresTheLocalMessenger(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)
	rec := &messagetest.Recorder{Local: b.LocalMessenger()}
	b.SetMessenger(rec)
	b.SetMessenger(nil)

	status, body := postAuthBroker(t, b.Addr(), "/send", "sender", senderToken, map[string]any{
		"from_run_id": "sender",
		"to_run_id":   "target",
		"type":        "agent_message",
		"payload":     map[string]any{"message": "notice"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("tell = (%d, %v), want 200 sent", status, body)
	}
	if delivers := rec.Delivers(); len(delivers) != 0 {
		t.Fatalf("deliveries = %+v, want the local messenger back", delivers)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 1 {
		t.Fatalf("target queue = %d messages, want the single tell", queued)
	}
}

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

	res, err := b.LocalMessenger().Send(ctx, req, false)
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("versioned send of an unversioned id = (%+v, %v), want a generation mismatch", res, err)
	}

	res, err = b.LocalMessenger().Send(ctx, req, true)
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("legacy send of an unversioned id = (%+v, %v), want sent", res, err)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 1 {
		t.Fatalf("target queue = %d messages, want the single legacy ask", queued)
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
	if len(sent) != 1 || sent[0].Request.MessageID != msgID || sent[0].Legacy {
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
