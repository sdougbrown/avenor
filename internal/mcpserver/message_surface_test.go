package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// recordingControlClient is a ControlClient double for the message surface. It
// records every control call so a test can assert the adapter forwarded the
// exact request and converted the result back to the #317 types.
type recordingControlClient struct {
	*fakeClient

	sent      messaging.SendRequest
	sentRes   messaging.SendResult
	sentErr   error
	waited    messaging.WaitRequest
	waitRes   messaging.WaitResult
	waitErr   error
	replied   messaging.ReplyRequest
	replyRes  messaging.SendResult
	replyErr  error
	withdrew  messaging.WithdrawRequest
	withdrawR messaging.WithdrawResult
	withdrawE error
	received  messaging.ReceiveRequest
	receiveR  messaging.ReceiveResult
	receiveE  error
	peered    messaging.PeersRequest
	peerRes   messaging.PeersResult
	peerErr   error
	preflight messaging.PreflightResult
	preflErr  error
	resumeReq messaging.ResumeRequest
	resumeRes messaging.ResumeResult
	resumeErr error
	renameID  string
	renameNew string
	renameRes messaging.RenameResult
	renameErr error
	lastID    string
	lastRes   messaging.PeerLast
	lastErr   error
}

func newRecordingControlClient() *recordingControlClient {
	return &recordingControlClient{fakeClient: &fakeClient{}}
}

func (c *recordingControlClient) MessageSend(req messaging.SendRequest) (messaging.SendResult, error) {
	c.sent = req
	return c.sentRes, c.sentErr
}

func (c *recordingControlClient) MessageWait(req messaging.WaitRequest) (messaging.WaitResult, error) {
	c.waited = req
	return c.waitRes, c.waitErr
}

func (c *recordingControlClient) MessageReply(req messaging.ReplyRequest) (messaging.SendResult, error) {
	c.replied = req
	return c.replyRes, c.replyErr
}

func (c *recordingControlClient) MessageWithdraw(req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	c.withdrew = req
	return c.withdrawR, c.withdrawE
}

func (c *recordingControlClient) MessageReceive(req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	c.received = req
	return c.receiveR, c.receiveE
}

func (c *recordingControlClient) MessagePeers(req messaging.PeersRequest) (messaging.PeersResult, error) {
	c.peered = req
	return c.peerRes, c.peerErr
}

func (c *recordingControlClient) MessagePreflight() (messaging.PreflightResult, error) {
	return c.preflight, c.preflErr
}

func (c *recordingControlClient) Resume(req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	c.resumeReq = req
	return c.resumeRes, c.resumeErr
}

func (c *recordingControlClient) SessionRename(sessionID, name string) (messaging.RenameResult, error) {
	c.renameID, c.renameNew = sessionID, name
	return c.renameRes, c.renameErr
}

func (c *recordingControlClient) SessionLast(runtimeID string) (messaging.PeerLast, error) {
	c.lastID = runtimeID
	return c.lastRes, c.lastErr
}

func TestControlTransportForwardsEveryVerb(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	cl.sentRes = messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: "msg_1"}
	cl.waitRes = messaging.WaitResult{Outcome: messaging.OutcomeReplied}
	cl.replyRes = messaging.SendResult{Outcome: messaging.OutcomeReplied}
	cl.withdrawR = messaging.WithdrawResult{Outcome: messaging.OutcomeWithdrawn}
	cl.receiveR = messaging.ReceiveResult{Generation: "gen-7"}
	cl.peerRes = messaging.PeersResult{Generation: "gen-7"}
	tr := newControlTransport(cl)
	ctx := context.Background()

	send := messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: "rt_1"},
		MessageID:  "msg_1",
		Generation: "gen-7",
		Body:       `{"message":"hi"}`,
		NoReply:    true,
	}
	got, err := tr.Send(ctx, send)
	if err != nil || got.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = %+v, %v", got, err)
	}
	if cl.sent.NoReply != true || cl.sent.To.RunID != "rt_1" || cl.sent.MessageID != "msg_1" {
		t.Fatalf("forwarded send = %+v", cl.sent)
	}
	wait := messaging.WaitRequest{Sender: messaging.Identity{RunID: "supervisor"}, MessageID: "msg_1", Generation: "gen-7"}
	if _, err := tr.Wait(ctx, wait); err != nil {
		t.Fatal(err)
	}
	if cl.waited.MessageID != "msg_1" || cl.waited.Generation != "gen-7" {
		t.Fatalf("forwarded wait = %+v", cl.waited)
	}
	reply := messaging.ReplyRequest{From: messaging.Identity{RunID: "supervisor"}, ToRunID: "rt_1", MessageID: "msg_1"}
	if _, err := tr.Reply(ctx, reply); err != nil {
		t.Fatal(err)
	}
	if cl.replied.ToRunID != "rt_1" || cl.replied.MessageID != "msg_1" {
		t.Fatalf("forwarded reply = %+v", cl.replied)
	}
	withdraw := messaging.WithdrawRequest{Sender: messaging.Identity{RunID: "supervisor"}, MessageID: "msg_1", Generation: "gen-7"}
	if _, err := tr.Withdraw(ctx, withdraw); err != nil {
		t.Fatal(err)
	}
	if cl.withdrew.MessageID != "msg_1" {
		t.Fatalf("forwarded withdraw = %+v", cl.withdrew)
	}
	receive := messaging.ReceiveRequest{Sender: messaging.Identity{RunID: "supervisor"}, Generation: "gen-7", Limit: 3}
	if _, err := tr.Receive(ctx, receive); err != nil {
		t.Fatal(err)
	}
	if cl.received.Limit != 3 {
		t.Fatalf("forwarded receive = %+v", cl.received)
	}
	peers := messaging.PeersRequest{Sender: messaging.Identity{RunID: "supervisor"}, Generation: "gen-7"}
	if _, err := tr.Peers(ctx, peers); err != nil {
		t.Fatal(err)
	}
	if cl.peered.Generation != "gen-7" {
		t.Fatalf("forwarded peers = %+v", cl.peered)
	}
	pre, err := tr.Preflight(ctx)
	if err != nil || pre.Generation != "gen-7" {
		t.Fatalf("Preflight = %+v, %v", pre, err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

func TestControlTransportMapsMessageErrorData(t *testing.T) {
	cl := newRecordingControlClient()
	cl.sentErr = &client.RPCError{
		Code:    -32000,
		Message: "credentials rejected",
		Data:    json.RawMessage(`{"code":"auth-rejected","wire_code":"auth-rejected","outcome":"auth-rejected"}`),
	}
	_, err := newControlTransport(cl).Send(context.Background(), messaging.SendRequest{Generation: "gen-7"})
	if !errors.Is(err, messaging.ErrAuthRejected) {
		t.Fatalf("Send error = %v, want ErrAuthRejected", err)
	}
}

func TestMCPMessageProviderResolveLadder(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.registry.Store(&RunInfo{RunID: "run-a", RuntimeID: "rt_a", Label: "alpha", SupervisorID: "supervisor-socket"}); err != nil {
		t.Fatal(err)
	}
	cl.peerRes = messaging.PeersResult{
		Generation: "gen-7",
		Peers: []messaging.Peer{
			{RunID: "rt_a", Session: json.RawMessage(`{"run_id":"rt_a","label":"alpha"}`)},
			{RunID: "rt_b", Session: json.RawMessage(`{"run_id":"rt_b","label":"mesh"}`)},
			{RunID: "rt_c", Session: json.RawMessage(`{"run_id":"rt_c","label":"mesh"}`)},
		},
	}
	p, err := newMCPMessageProvider(s, cl, "supervisor-socket")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	self, err := p.Resolve(ctx, "supervisor")
	if err != nil || self.Match != messaging.MatchUnique || self.RunID != "supervisor" {
		t.Fatalf("self = %+v, %v", self, err)
	}
	publicID, err := p.Resolve(ctx, "run-a")
	if err != nil || publicID.RunID != "rt_a" || publicID.PublicRunID != "run-a" || publicID.Label != "alpha" {
		t.Fatalf("public run id = %+v, %v", publicID, err)
	}
	label, err := p.Resolve(ctx, "alpha")
	if err != nil || label.RunID != "rt_a" || label.Label != "alpha" {
		t.Fatalf("label = %+v, %v", label, err)
	}
	ambiguous, err := p.Resolve(ctx, "mesh")
	if err != nil || ambiguous.Match != messaging.MatchAmbiguous || len(ambiguous.Candidates) != 2 {
		t.Fatalf("ambiguous = %+v, %v", ambiguous, err)
	}
	none, err := p.Resolve(ctx, "ghost")
	if err != nil || none.Match != messaging.MatchNone {
		t.Fatalf("none = %+v, %v", none, err)
	}
}

func TestMCPMessageProviderRunState(t *testing.T) {
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "avenor-run-x.done")
	if err := os.WriteFile(sentinel, []byte("DONE\nSESSION=ses_1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cl := newRecordingControlClient()
	cl.statusFunc = func(string) (map[string]any, error) {
		return map[string]any{"status": "ended", "phase": "done", "session_id": "ses_1", "sentinel_file": sentinel}, nil
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	p, err := newMCPMessageProvider(s, cl, "sock")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := p.RunState(context.Background(), "rt_1")
	if err != nil {
		t.Fatal(err)
	}
	if facts.Status != "done" || facts.Live || !facts.Resumable {
		t.Fatalf("facts = %+v", facts)
	}
}

func TestResumeRegistersRunByLabelAndReadsEvents(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	cl.resumeRes = messaging.ResumeResult{
		RunID: "run-new", Label: "resumed", RuntimeID: "rt_new", SessionID: "ses_new",
		SentinelFile: "/tmp/avenor-run-run-new.done", OnEvent: "/tmp/avenor-run-run-new.log",
	}
	cl.statusFunc = func(string) (map[string]any, error) {
		return map[string]any{"status": "running", "agent": "worker", "model": "sonnet"}, nil
	}
	cl.peerRes = messaging.PeersResult{Generation: "gen-7", Peers: []messaging.Peer{
		{RunID: "rt_new", Session: json.RawMessage(`{"run_id":"rt_new","label":"resumed","kind":"agent"}`)},
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	p, err := newMCPMessageProvider(s, cl, "sock")
	if err != nil {
		t.Fatal(err)
	}
	res, err := p.Resume(context.Background(), messaging.ResumeRequest{Target: "rt_old", Message: "continue"})
	if err != nil {
		t.Fatal(err)
	}
	if cl.resumeReq.Target != "rt_old" || cl.resumeReq.Message != "continue" || cl.resumeReq.RunID == "" {
		t.Fatalf("resume request = %+v", cl.resumeReq)
	}
	if !strings.HasPrefix(cl.resumeReq.SentinelFile, os.TempDir()) || cl.resumeReq.EventLog == "" {
		t.Fatalf("resume minted paths = %+v", cl.resumeReq)
	}
	resolved, err := p.Resolve(context.Background(), "resumed")
	if err != nil || resolved.RunID != "rt_new" {
		t.Fatalf("resolve by label = %+v, %v", resolved, err)
	}
	info := s.registry.Lookup("sock", res.RunID)
	if info == nil || info.Label != "resumed" || info.RuntimeID != "rt_new" || info.Agent != "worker" {
		t.Fatalf("registry entry = %+v", info)
	}
	logPath := filepath.Join(t.TempDir(), "events.log")
	if err := os.WriteFile(logPath, []byte(`{"event":"session.end","ts":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	info.EventLogPath = logPath
	_, out, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: res.RunID})
	if err != nil {
		t.Fatal(err)
	}
	events, _ := out.(map[string]any)["events"].([]map[string]any)
	if len(events) != 1 || events[0]["event"] != "session.end" {
		t.Fatalf("events = %#v", out)
	}
}

func TestResumeRegistrationRefusalNamesBothIDs(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	cl.resumeRes = messaging.ResumeResult{RunID: "run-new", Label: "taken", RuntimeID: "rt_new"}
	cl.peerRes = messaging.PeersResult{Generation: "gen-7", Peers: []messaging.Peer{
		{RunID: "rt_new", Session: json.RawMessage(`{"run_id":"rt_new","label":"taken"}`)},
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.registry.Store(&RunInfo{RunID: "other", RuntimeID: "rt_other", Label: "taken"}); err != nil {
		t.Fatal(err)
	}
	p, err := newMCPMessageProvider(s, cl, "sock")
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Resume(context.Background(), messaging.ResumeRequest{Target: "rt_old", Message: "continue"})
	var toolErr *router.ToolError
	if !errors.As(err, &toolErr) || toolErr.Code != "refused" {
		t.Fatalf("resume error = %v, want refused tool error", err)
	}
	if !strings.Contains(toolErr.Message, "run-new") || !strings.Contains(toolErr.Message, "rt_new") {
		t.Fatalf("refusal text %q does not name both ids", toolErr.Message)
	}
	if info := s.registry.Lookup("sock", "run-new"); info != nil {
		t.Fatalf("refused resume stored a registry entry: %+v", info)
	}
	addressable, err := p.Resolve(context.Background(), "rt_new")
	if err != nil || addressable.RunID != "rt_new" {
		t.Fatalf("runtime id is not addressable: %+v, %v", addressable, err)
	}
}

func TestAvenorPeersLastTextIsTheRouterText(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	cl.peerRes = messaging.PeersResult{Generation: "gen-7", Peers: []messaging.Peer{
		{RunID: "rt_7", Session: json.RawMessage(`{"run_id":"rt_7","label":"mesh-a"}`)},
	}}
	cl.lastRes = messaging.PeerLast{RunID: "rt_7", Label: "mesh-a", HasRecord: true, Text: "OK", StopReason: "end_turn"}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	res, out, err := s.handleAvenorPeers(context.Background(), nil, map[string]any{"action": "last", "target": "rt_7"})
	if err != nil {
		t.Fatal(err)
	}
	if cl.lastID != "rt_7" {
		t.Fatalf("session_last target = %q, want rt_7", cl.lastID)
	}
	content, _ := res.Content[0].(*mcpsdk.TextContent)
	text := content.Text
	wantText := `**Last message from "mesh-a"** (run "rt_7", stop reason: end_turn)` + "\n\nOK"
	if text != wantText {
		t.Fatalf("last content = %q, want %q", text, wantText)
	}
	details, _ := out.(map[string]any)
	last, _ := details["last"].(map[string]any)
	if last["run_id"] != "rt_7" || last["runtime_id"] != "rt_7" || last["label"] != "mesh-a" || last["final_output"] != "OK" || last["stop_reason"] != "end_turn" {
		t.Fatalf("last details = %#v", details)
	}
}
