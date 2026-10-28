package mcpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// recordingControlClient is a ControlClient double for the message surface. It
// records every control call so a test can assert the adapter forwarded the
// exact request and converted the result back to the #317 types.
type recordingControlClient struct {
	*fakeClient

	sent       messaging.SendRequest
	sentRes    messaging.SendResult
	sentErr    error
	waited     messaging.WaitRequest
	waitRes    messaging.WaitResult
	waitErr    error
	replied    messaging.ReplyRequest
	replyRes   messaging.SendResult
	replyErr   error
	withdrew   messaging.WithdrawRequest
	withdrawR  messaging.WithdrawResult
	withdrawE  error
	received   messaging.ReceiveRequest
	receiveR   messaging.ReceiveResult
	receiveE   error
	peered     messaging.PeersRequest
	peerRes    messaging.PeersResult
	peerErr    error
	peersCalls int
	preflight  messaging.PreflightResult
	preflErr   error
	resumeReq  messaging.ResumeRequest
	resumeRes  messaging.ResumeResult
	resumeErr  error
	renameID   string
	renameNew  string
	renameRes  messaging.RenameResult
	renameErr  error
	lastID     string
	lastRes    messaging.PeerLast
	lastErr    error
	ctxs       []context.Context
}

func newRecordingControlClient() *recordingControlClient {
	return &recordingControlClient{fakeClient: &fakeClient{}}
}

func (c *recordingControlClient) MessageSendContext(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	c.ctxs = append(c.ctxs, ctx)
	c.sent = req
	return c.sentRes, c.sentErr
}

func (c *recordingControlClient) MessageWaitContext(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	c.ctxs = append(c.ctxs, ctx)
	c.waited = req
	return c.waitRes, c.waitErr
}

func (c *recordingControlClient) MessageReplyContext(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	c.ctxs = append(c.ctxs, ctx)
	c.replied = req
	return c.replyRes, c.replyErr
}

func (c *recordingControlClient) MessageWithdrawContext(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	c.ctxs = append(c.ctxs, ctx)
	c.withdrew = req
	return c.withdrawR, c.withdrawE
}

func (c *recordingControlClient) MessageReceiveContext(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	c.ctxs = append(c.ctxs, ctx)
	c.received = req
	return c.receiveR, c.receiveE
}

func (c *recordingControlClient) MessagePeersContext(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	c.ctxs = append(c.ctxs, ctx)
	c.peered = req
	c.peersCalls++
	return c.peerRes, c.peerErr
}

func (c *recordingControlClient) MessagePreflightContext(ctx context.Context) (messaging.PreflightResult, error) {
	c.ctxs = append(c.ctxs, ctx)
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

func (c *recordingControlClient) ResumeContext(_ context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	return c.Resume(req)
}

func (c *recordingControlClient) SessionRenameContext(_ context.Context, sessionID, name string) (messaging.RenameResult, error) {
	return c.SessionRename(sessionID, name)
}

func (c *recordingControlClient) SessionLastContext(_ context.Context, runtimeID string) (messaging.PeerLast, error) {
	return c.SessionLast(runtimeID)
}

func (c *recordingControlClient) SessionLastTurnContext(_ context.Context, runtimeID, _ string) (messaging.PeerLast, error) {
	return c.SessionLast(runtimeID)
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
			// The shadow row carries the reserved alias as a label, so only a guard
			// that runs before the listing can keep it from answering "supervisor".
			{RunID: "rt_shadow", Session: json.RawMessage(`{"run_id":"rt_shadow","label":"supervisor"}`)},
			// The bare row has no session blob, pinning the decoder's empty-session
			// guard, its run-id fallback and the resolution's label fallback.
			{RunID: "rt_bare"},
		},
	}
	p, err := newMCPMessageProvider(context.Background(), s, cl, "supervisor-socket")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	self, err := p.Resolve(ctx, "supervisor")
	if err != nil || self.Match != messaging.MatchUnique || self.RunID != "supervisor" {
		t.Fatalf("self = %+v, %v", self, err)
	}
	if cl.peersCalls != 0 {
		t.Fatalf("reserved alias issued %d peer listings, want none", cl.peersCalls)
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
	bare, err := p.Resolve(ctx, "rt_bare")
	if err != nil || bare.Match != messaging.MatchUnique || bare.RunID != "rt_bare" || bare.Label != "rt_bare" {
		t.Fatalf("bare peer row = %+v, %v", bare, err)
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
	p, err := newMCPMessageProvider(context.Background(), s, cl, "sock")
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
	p, err := newMCPMessageProvider(context.Background(), s, cl, "sock")
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

func TestResumeRegistrationFailureNamesBothIDs(t *testing.T) {
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
	p, err := newMCPMessageProvider(context.Background(), s, cl, "sock")
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Resume(context.Background(), messaging.ResumeRequest{Target: "rt_old", Message: "continue"})
	// The resume already ran, so a registration failure is a server-side error,
	// not a typed refusal that denies an action that was performed.
	var toolErr *router.ToolError
	if errors.As(err, &toolErr) {
		t.Fatalf("registration failure reported a typed refusal %q", toolErr.Code)
	}
	if err == nil {
		t.Fatal("resume with a colliding label returned no error")
	}
	if !strings.Contains(err.Error(), "run-new") || !strings.Contains(err.Error(), "rt_new") {
		t.Fatalf("error text %q does not name both ids", err.Error())
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
	res, err := s.handleAvenorPeers(context.Background(), toolRequest(t, map[string]any{"action": "last", "target": "rt_7"}))
	if err != nil {
		t.Fatal(err)
	}
	if cl.lastID != "rt_7" {
		t.Fatalf("session_last target = %q, want rt_7", cl.lastID)
	}
	content, _ := res.Content[0].(*mcpsdk.TextContent)
	text := content.Text
	wantText := lastRenderText(t, "rt_7", "mesh-a", "end_turn") + "\n\nOK"
	if text != wantText {
		t.Fatalf("last content = %q, want %q", text, wantText)
	}
	details, _ := res.StructuredContent.(map[string]any)
	last, _ := details["last"].(map[string]any)
	if last["run_id"] != "rt_7" || last["runtime_id"] != "rt_7" || last["label"] != "mesh-a" || last["final_output"] != "OK" || last["stop_reason"] != "end_turn" {
		t.Fatalf("last details = %#v", details)
	}
}

// toolRequest builds the raw-arguments call request the low-level MCP handler
// receives, so a test drives exactly the arguments the wire carries.
func toolRequest(t *testing.T, args map[string]any) *mcpsdk.CallToolRequest {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	return &mcpsdk.CallToolRequest{Params: &mcpsdk.CallToolParamsRaw{Arguments: raw}}
}

// lastRenderText renders the peers `last` header from the shared contract
// fixture and the router's own fact construction, so a test pins the renderer's
// use instead of restating the template or its escaping rule.
func lastRenderText(t *testing.T, runID, label, stopReason string) string {
	t.Helper()
	return messaging.LastRender("header", router.LastFacts(runID, label, stopReason))
}

// TestMCPMessageProviderRunStateReadsBrokerOnlySupervisor proves the one run
// the host cannot read through the runtime status verb is read through the
// peer listing the surface already uses.
func TestMCPMessageProviderRunStateReadsBrokerOnlySupervisor(t *testing.T) {
	cl := newRecordingControlClient()
	cl.statusFunc = func(runtimeID string) (map[string]any, error) {
		return nil, errors.New(`runtime "supervisor" not found`)
	}
	cl.peerRes = messaging.PeersResult{Generation: "gen-7", Peers: []messaging.Peer{
		{RunID: "supervisor", Session: json.RawMessage(`{"run_id":"supervisor","label":"supervisor","kind":"supervisor","status":"running"}`)},
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	p, err := newMCPMessageProvider(context.Background(), s, cl, "sock")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := p.RunState(context.Background(), messaging.SupervisorRunID)
	if err != nil {
		t.Fatal(err)
	}
	if !facts.Live || facts.Resumable || facts.Status != "running" {
		t.Fatalf("supervisor facts = %+v, want live and not resumable", facts)
	}
}

// TestAvenorMessageTargetSelfReachesTheSupervisor proves the published `self`
// alias works on this surface: the tell resolves to the supervisor session and
// its state comes from the peer listing, not the runtime status verb.
func TestAvenorMessageTargetSelfReachesTheSupervisor(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	cl.statusFunc = func(runtimeID string) (map[string]any, error) {
		return nil, errors.New(`runtime "supervisor" not found`)
	}
	cl.peerRes = messaging.PeersResult{Generation: "gen-7", Peers: []messaging.Peer{
		{RunID: "supervisor", Session: json.RawMessage(`{"run_id":"supervisor","label":"supervisor","kind":"supervisor","status":"running"}`)},
	}}
	cl.sentRes = messaging.SendResult{Outcome: messaging.OutcomeSent}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "tell", "target": "self", "message": "hi",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("tell to self reported isError: %#v", res.StructuredContent)
	}
	if cl.sent.To.RunID != messaging.SupervisorRunID {
		t.Fatalf("tell target = %q, want supervisor", cl.sent.To.RunID)
	}
	object, _ := res.StructuredContent.(map[string]any)
	if object["outcome"] != "sent" {
		t.Fatalf("tell result = %#v", object)
	}
}

// TestMCPMessageRouterOwnsAdmission drives the two merged tools over the MCP
// wire and proves the router, not the SDK's schema validation, answers an
// inadmissible call with the contract's typed error on this surface too.
func TestMCPMessageRouterOwnsAdmission(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	clientSession := connectTestClient(t, s)
	defer clientSession.Close()
	ctx := context.Background()

	cases := []struct {
		tool string
		args map[string]any
		code string
		text string
	}{
		{
			tool: "message",
			args: map[string]any{"action": "bogus"},
			code: "invalid_action",
		},
		{
			tool: "message",
			args: map[string]any{"action": "tell", "target": "x", "message": "m", "extra": "field"},
			code: "invalid_request",
			text: messaging.Repair("invalid_request", map[string]string{
				"action": messaging.Quote("tell"),
				"detail": messaging.Repair("unknown_property", map[string]string{"name": messaging.Quote("extra")}),
			}),
		},
		{
			tool: "peers",
			args: map[string]any{"action": "list", "name": "x"},
			code: "invalid_request",
			text: messaging.Repair("invalid_request", map[string]string{
				"action": messaging.Quote("list"),
				"detail": messaging.Repair("unknown_property", map[string]string{"name": messaging.Quote("name")}),
			}),
		},
	}
	for _, tc := range cases {
		result, err := clientSession.CallTool(ctx, &mcpsdk.CallToolParams{Name: tc.tool, Arguments: tc.args})
		if err != nil {
			t.Fatalf("%s %v: call: %v", tc.tool, tc.args, err)
		}
		if !result.IsError {
			t.Fatalf("%s %v: IsError = false, want a tool error", tc.tool, tc.args)
		}
		structured, _ := result.StructuredContent.(map[string]any)
		if structured["code"] != tc.code {
			t.Fatalf("%s %v: code = %v, want %s (%#v)", tc.tool, tc.args, structured["code"], tc.code, structured)
		}
		if tc.text != "" && structured["text"] != tc.text {
			t.Fatalf("%s %v: text = %v, want %q", tc.tool, tc.args, structured["text"], tc.text)
		}
		content, _ := result.Content[0].(*mcpsdk.TextContent)
		if strings.Contains(content.Text, "validating") || strings.Contains(content.Text, "schemas/message.input.json") {
			t.Fatalf("%s %v: SDK schema validation answered first: %q", tc.tool, tc.args, content.Text)
		}
	}
}

// TestMCPMessageToolErrorIsAnErrorResult proves a router refusal is flagged the
// way the child surfaces flag it, with the same structured payload.
func TestMCPMessageToolErrorIsAnErrorResult(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "tell", "target": "ghost", "message": "hi",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatalf("tool error IsError = false, want true (result %#v)", res)
	}
	object, _ := res.StructuredContent.(map[string]any)
	wantText := messaging.Repair("unknown_target", map[string]string{"target": messaging.Quote("ghost")})
	if object["code"] != "unknown_target" || object["text"] != wantText {
		t.Fatalf("tool error object = %#v, want unknown_target and %q", object, wantText)
	}
	content, _ := res.Content[0].(*mcpsdk.TextContent)
	if content.Text != wantText {
		t.Fatalf("tool error content = %q, want %q", content.Text, wantText)
	}
}

// TestMCPMessageProviderResolvesPublicRunIDAfterRestart proves the id-only
// rehydrating rung still resolves a host public run id when the MCP server's
// in-process registry is empty, without turning labels into a registry plane.
func TestMCPMessageProviderResolvesPublicRunIDAfterRestart(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	cl.listResult = []map[string]any{{
		"runtime_id":    "rt_restored",
		"sentinel_file": filepath.Join(t.TempDir(), "avenor-run-11111111-1111-1111-1111-111111111111.done"),
		"label":         "watched",
		"status":        "ended",
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	p, err := newMCPMessageProvider(context.Background(), s, cl, "sock")
	if err != nil {
		t.Fatal(err)
	}
	res, err := p.Resolve(context.Background(), "11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if res.Match != messaging.MatchUnique || res.RunID != "rt_restored" || res.PublicRunID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("public run id after restart = %+v", res)
	}
	// The rehydrating rung resolves public ids only: a label still needs the
	// broker's listing.
	label, err := p.Resolve(context.Background(), "watched")
	if err != nil {
		t.Fatal(err)
	}
	if label.Match != messaging.MatchNone {
		t.Fatalf("label resolved through the id rung: %+v", label)
	}
}

// TestPeersLastReportsOneIDSpace proves the same run reports one run_id no
// matter which address resolved it, so the Stage 8 mesh can compare it with the
// public ids spawn and status report.
func TestPeersLastReportsOneIDSpace(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	cl.peerRes = messaging.PeersResult{Generation: "gen-7", Peers: []messaging.Peer{
		{RunID: "rt_a", Session: json.RawMessage(`{"run_id":"rt_a","label":"alpha"}`)},
	}}
	cl.lastRes = messaging.PeerLast{RunID: "rt_a", Label: "alpha", HasRecord: true, Text: "OK", StopReason: "end_turn"}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.registry.Store(&RunInfo{RunID: "run-a", RuntimeID: "rt_a", Label: "alpha", SupervisorID: "sock", SentinelPath: "/tmp/test-turn-a"}); err != nil {
		t.Fatal(err)
	}
	p, err := newMCPMessageProvider(context.Background(), s, cl, "sock")
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"run-a", "rt_a", "alpha"} {
		result, err := router.ExecutePeers(context.Background(), p, json.RawMessage(`{"action":"last","target":"`+target+`"}`))
		if err != nil {
			t.Fatalf("last %s: %v", target, err)
		}
		if result.RunID != "run-a" || result.RuntimeID != "rt_a" {
			t.Fatalf("last %s run ids = %q/%q, want run-a/rt_a", target, result.RunID, result.RuntimeID)
		}
	}
}

// TestTwoSupervisorsWithTheSameRuntimeIDStayDistinct proves the message surface
// answers for the supervisor the call is bound to. Runtime ids are minted per
// supervisor, so the first run on every supervisor is rt_1; an identity lookup
// that scans every supervisor answers for whichever one the map iteration
// reached first.
func TestTwoSupervisorsWithTheSameRuntimeIDStayDistinct(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range []*RunInfo{
		{RunID: "run-a", RuntimeID: "rt_1", Label: "alpha", SupervisorID: "/sock/a"},
		{RunID: "run-b", RuntimeID: "rt_1", Label: "beta", SupervisorID: "/sock/b"},
	} {
		if err := s.registry.Store(info); err != nil {
			t.Fatal(err)
		}
	}
	alpha, err := newMCPMessageProvider(context.Background(), s, cl, "/sock/a")
	if err != nil {
		t.Fatal(err)
	}
	beta, err := newMCPMessageProvider(context.Background(), s, cl, "/sock/b")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if got := alpha.peerResolution("rt_1", "alpha").PublicRunID; got != "run-a" {
			t.Fatalf("a's rt_1 reported run_id %q, want run-a", got)
		}
		if got := beta.peerResolution("rt_1", "beta").PublicRunID; got != "run-b" {
			t.Fatalf("b's rt_1 reported run_id %q, want run-b", got)
		}
	}

	// A runtime id one supervisor owns still binds the call to that supervisor,
	// while an id both own resolves to neither and leaves the default binding.
	if err := s.registry.Store(&RunInfo{RunID: "run-c", RuntimeID: "rt_9", Label: "gamma", SupervisorID: "/sock/b"}); err != nil {
		t.Fatal(err)
	}
	if got := s.messageSupervisorPath(json.RawMessage(`{"action":"last","target":"rt_9"}`)); got != "/sock/b" {
		t.Fatalf("unique runtime id bound to %q, want /sock/b", got)
	}
	for i := 0; i < 200; i++ {
		if got := s.messageSupervisorPath(json.RawMessage(`{"action":"last","target":"rt_1"}`)); got != "" {
			t.Fatalf("runtime id on two supervisors bound to %q, want the default supervisor", got)
		}
	}
}

// TestPeerResolutionResolvesSharedRuntimeToCurrentTurn proves a runtime
// shared by two registered turns resolves to the turn whose sentinel the
// runtime currently holds, so the turn guard pins operations to the current
// turn. A runtime whose sentinel matches no registered turn stays unresolved:
// restart ambiguity is never broken by picking an entry.
func TestPeerResolutionResolvesSharedRuntimeToCurrentTurn(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	cl.statusResult = map[string]any{"status": "running", "sentinel_file": "rt_1/current.done"}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range []*RunInfo{
		{RunID: "run-old", RuntimeID: "rt_1", SupervisorID: "sock", SentinelPath: "rt_1/old.done"},
		{RunID: "run-new", RuntimeID: "rt_1", SupervisorID: "sock", SentinelPath: "rt_1/current.done"},
	} {
		if err := s.registry.Store(info); err != nil {
			t.Fatal(err)
		}
	}
	p, err := newMCPMessageProvider(context.Background(), s, cl, "sock")
	if err != nil {
		t.Fatal(err)
	}
	res := p.peerResolution("rt_1", "warm")
	if res.PublicRunID != "run-new" {
		t.Fatalf("shared runtime public run id = %q, want run-new", res.PublicRunID)
	}
	if p.resolvedRun == nil || p.resolvedRun.RunID != "run-new" {
		t.Fatalf("resolved run = %#v, want run-new", p.resolvedRun)
	}

	// A runtime whose current sentinel matches no registered turn answers
	// with no public run and no resolved turn.
	cl.statusResult = map[string]any{"status": "running", "sentinel_file": "rt_1/none.done"}
	p2, err := newMCPMessageProvider(context.Background(), s, cl, "sock")
	if err != nil {
		t.Fatal(err)
	}
	if got := p2.peerResolution("rt_1", "warm"); got.PublicRunID != "" || p2.resolvedRun != nil {
		t.Fatalf("unmatched sentinel resolved %+v / %#v, want nothing", got, p2.resolvedRun)
	}

	// A runtime whose status cannot be read keeps the ambiguity.
	cl.statusErr = errors.New("runtime gone")
	cl.statusResult = nil
	p3, err := newMCPMessageProvider(context.Background(), s, cl, "sock")
	if err != nil {
		t.Fatal(err)
	}
	if got := p3.peerResolution("rt_1", "warm"); got.PublicRunID != "" || p3.resolvedRun != nil {
		t.Fatalf("unreadable runtime resolved %+v / %#v, want nothing", got, p3.resolvedRun)
	}
}

// TestAuthorityToolErrorMapsWireCodesToRefused proves a control failure whose
// code is a wire class, not a router code, is published as a refusal.
func TestAuthorityToolErrorMapsWireCodesToRefused(t *testing.T) {
	cases := []struct {
		data string
		want string
	}{
		{`{"code":"invalid-request"}`, "refused"},
		{`{"code":"auth-rejected"}`, "refused"},
		{`{"code":"not_permitted"}`, "not_permitted"},
		{`{}`, "refused"},
	}
	for _, tc := range cases {
		err := authorityToolError(&client.RPCError{Code: -32000, Message: "broker not available", Data: json.RawMessage(tc.data)})
		var toolErr *router.ToolError
		if !errors.As(err, &toolErr) || toolErr.Code != tc.want {
			t.Fatalf("authorityToolError(%s) = %v, want code %s", tc.data, err, tc.want)
		}
	}
}

// connectTestClient connects an in-memory MCP client to the server, so a test
// can call a tool over the real wire instead of the handler directly.
func connectTestClient(t *testing.T, s *Server) *mcpsdk.ClientSession {
	t.Helper()
	ctx := context.Background()
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serverSession, err := s.mcpServer.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("connect server: %v", err)
	}
	t.Cleanup(func() { serverSession.Close() })
	mcpClient := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "dev"}, nil)
	clientSession, err := mcpClient.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("connect client: %v", err)
	}
	return clientSession
}

// controlCall is one request a recording supervisor received.
type controlCall struct {
	method string
	params map[string]any
}

// startRecordingSupervisor serves the control verbs a resume needs and records
// every request, so a test can prove which supervisor received it.
func startRecordingSupervisor(t *testing.T, name, eventLog string) (string, func() []controlCall, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "ext")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	cap := publishOwnerRecordForStub(t, path)

	var mu sync.Mutex
	var calls []controlCall
	resoOne := func(req client.Request) controlCall {
		var params map[string]any
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params, &params)
		}
		call := controlCall{method: req.Method, params: params}
		mu.Lock()
		calls = append(calls, call)
		mu.Unlock()
		return call
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					var req client.Request
					if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
						continue
					}
					call := resoOne(req)
					resp := client.Response{JSONRPC: "2.0", ID: req.ID}
					switch call.method {
					case "authenticate_owner":
						resp = ownerAuthResponse(req, cap)
					case "message_preflight":
						resp.Result, _ = json.Marshal(map[string]any{
							"messaging_protocol": messaging.ProtocolVersion, "generation": "gen-ext",
						})
					case "status":
						if call.params["runtime_id"] == "rt_ext" {
							resp.Result, _ = json.Marshal(map[string]any{
								"status": "ended", "phase": "done", "session_id": "ses_ext",
							})
						} else {
							resp.Result, _ = json.Marshal(map[string]any{"status": "running", "agent": "worker"})
						}
					case "list":
						resp.Result, _ = json.Marshal([]map[string]any{{
							"runtime_id": "rt_ext", "label": "watched", "status": "ended",
						}})
					case "message_peers":
						resp.Result, _ = json.Marshal(map[string]any{
							"generation": "gen-ext",
							"peers": []map[string]any{{
								"run_id": "rt_ext",
								"session": map[string]any{
									"run_id": "rt_ext", "label": "watched", "kind": "agent", "status": "ended",
								},
							}},
						})
					case "resume":
						runID, _ := call.params["run_id"].(string)
						resp.Result, _ = json.Marshal(map[string]any{
							"run_id": runID, "label": "resumed", "runtime_id": "rt_new",
							"session_id": "ses_new", "on_event": eventLog,
						})
					default:
						resp.Error = &client.RespError{Code: -32601, Message: "method not found"}
					}
					data, _ := json.Marshal(resp)
					_, _ = c.Write(append(data, '\n'))
				}
			}(conn)
		}
	}()
	recorder := func() []controlCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]controlCall(nil), calls...)
	}
	cleanup := func() {
		ln.Close()
		os.Remove(path)
		os.Remove(control.OwnerRecordPath(path))
		os.RemoveAll(dir)
	}
	return path, recorder, cleanup
}

// TestAvenorMessageResumesRunOnExternallyWatchedSupervisor is the plan's
// "resume an externally watched run" regression for this surface: the run's
// owning supervisor receives the resume, the default supervisor does not, and
// the resumed run is resolvable and its events readable.
func TestAvenorMessageResumesRunOnExternallyWatchedSupervisor(t *testing.T) {
	eventLog := filepath.Join(t.TempDir(), "events.log")
	if err := os.WriteFile(eventLog, []byte(`{"event":"session.end","ts":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	supervisorPath, calls, cleanup := startRecordingSupervisor(t, "avenor-mcp-extern.sock", eventLog)
	defer cleanup()
	stubSocketsRoot(t, filepath.Dir(supervisorPath))

	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.registry.Store(&RunInfo{
		RunID: "run-ext", Label: "watched", RuntimeID: "rt_ext", SupervisorID: supervisorPath,
	}); err != nil {
		t.Fatal(err)
	}

	res, err := s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "resume", "target": "watched", "message": "go",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("resume reported isError: %#v", res.StructuredContent)
	}
	var resumed controlCall
	found := false
	for _, call := range calls() {
		if call.method == "resume" {
			resumed, found = call, true
		}
	}
	if !found {
		t.Fatalf("external supervisor received no resume: %v", calls())
	}
	if resumed.params["target"] != "rt_ext" || resumed.params["message"] != "go" {
		t.Fatalf("external resume = %#v", resumed.params)
	}
	if cl.resumeReq != (messaging.ResumeRequest{}) {
		t.Fatalf("default supervisor resume = %+v, want the default supervisor never asked to resume", cl.resumeReq)
	}
	object, _ := res.StructuredContent.(map[string]any)
	runID, _ := object["run_id"].(string)
	info := s.registry.LookupUnique(runID)
	if info == nil || info.SupervisorID != supervisorPath || info.Label != "resumed" {
		t.Fatalf("resumed registry entry = %+v, want label resumed on %s", info, supervisorPath)
	}

	_, out, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	events, _ := out.(map[string]any)["events"].([]map[string]any)
	if len(events) != 1 || events[0]["event"] != "session.end" {
		t.Fatalf("resumed run events = %#v", out)
	}
}

// TestControlTransportForwardsContext proves every transport verb hands the
// caller's context to the control client, so a cancelled tool call stops the
// transport instead of waiting out the server's timeout.
func TestControlTransportForwardsContext(t *testing.T) {
	cl := newRecordingControlClient()
	cl.preflight = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}
	tr := newControlTransport(cl)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _ = tr.Send(ctx, messaging.SendRequest{Generation: "gen-7"})
	_, _ = tr.Wait(ctx, messaging.WaitRequest{Generation: "gen-7"})
	_, _ = tr.Reply(ctx, messaging.ReplyRequest{})
	_, _ = tr.Withdraw(ctx, messaging.WithdrawRequest{Generation: "gen-7"})
	_, _ = tr.Receive(ctx, messaging.ReceiveRequest{Generation: "gen-7"})
	_, _ = tr.Peers(ctx, messaging.PeersRequest{Generation: "gen-7"})
	_, _ = tr.Preflight(ctx)

	if len(cl.ctxs) != 7 {
		t.Fatalf("recorded %d contexts, want 7", len(cl.ctxs))
	}
	for i, seen := range cl.ctxs {
		if seen != ctx {
			t.Fatalf("verb %d received a different context", i)
		}
	}
}
