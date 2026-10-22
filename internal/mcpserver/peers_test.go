package mcpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// fix06DefaultSocket is the configured default supervisor for these tests. The
// fake ControlClient answers it, so the path never has to be dialable.
const fix06DefaultSocket = "/tmp/avenor-mcp-fix06-default.sock"

// connectToolSession connects an in-memory MCP client to s, so a test calls the
// registered tools the way a client does instead of calling the handlers.
func connectToolSession(t *testing.T, s *Server) *mcpsdk.ClientSession {
	t.Helper()
	ctx := context.Background()
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serverSession, err := s.mcpServer.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("connect server: %v", err)
	}
	t.Cleanup(func() { serverSession.Close() })
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "dev"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("connect client: %v", err)
	}
	t.Cleanup(func() { clientSession.Close() })
	return clientSession
}

// newToolServer builds a server whose default supervisor is a fake client.
func newToolServer(t *testing.T, cl ControlClient) *Server {
	t.Helper()
	s, err := NewServer(Options{
		Transport:        "stdio",
		NoAutostart:      true,
		SupervisorSocket: fix06DefaultSocket,
		ControlClient:    cl,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// toolResultText returns the single text content of a tool result.
func toolResultText(t *testing.T, res *mcpsdk.CallToolResult) string {
	t.Helper()
	if len(res.Content) != 1 {
		t.Fatalf("content parts = %d, want 1", len(res.Content))
	}
	text, ok := res.Content[0].(*mcpsdk.TextContent)
	if !ok {
		t.Fatalf("content = %T, want text content", res.Content[0])
	}
	return text.Text
}

// runningStatus is the status a live target reports, so a tell passes the
// router's state row.
func runningStatus() map[string]any {
	return map[string]any{"status": "running", "phase": "active", "session_id": "sess-1"}
}

// TestMessageToolTellUsesDefaultSupervisor drives the registered message tool
// through a tell and pins both halves of its rendering: the rendered sentence
// as content, the result object as structured content, and the call on the
// bound supervisor's client.
func TestMessageToolTellUsesDefaultSupervisor(t *testing.T) {
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{statusResult: runningStatus()}}
	var sent []messaging.SendRequest
	cl.sendFunc = func(_ context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
		sent = append(sent, req)
		return messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: req.MessageID, Generation: req.Generation}, nil
	}
	s := newToolServer(t, cl)
	if err := s.registry.Store(&RunInfo{
		RunID:        "run-1",
		RuntimeID:    "rt-1",
		Label:        "worker",
		SupervisorID: fix06DefaultSocket,
	}); err != nil {
		t.Fatalf("registry.Store: %v", err)
	}

	res, err := connectToolSession(t, s).CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name:      "message",
		Arguments: map[string]any{"action": "tell", "target": "run-1", "message": "hello"},
	})
	if err != nil {
		t.Fatalf("call message: %v", err)
	}
	if res.IsError {
		t.Fatalf("message result is an error: %s", toolResultText(t, res))
	}
	if len(sent) != 1 {
		t.Fatalf("send calls = %d, want 1", len(sent))
	}
	if sent[0].To.RunID != "rt-1" {
		t.Fatalf("send target = %q, want rt-1", sent[0].To.RunID)
	}

	structured, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content = %T, want an object", res.StructuredContent)
	}
	text := toolResultText(t, res)
	if text != structured["text"] {
		t.Fatalf("content text = %q, want the result's rendered text %q", text, structured["text"])
	}
	if strings.HasPrefix(text, "{") {
		t.Fatalf("content text = %q, want a rendered sentence, not the result object", text)
	}
	if structured["action"] != "tell" || structured["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("structured content = %#v, want a sent tell", structured)
	}
}

// TestPeersToolDefaultsToList pins the omitted-arguments path: no arguments
// object at all still lists peers on the default supervisor.
func TestPeersToolDefaultsToList(t *testing.T) {
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{}}
	peerCalls := 0
	cl.peersFunc = func(context.Context, messaging.PeersRequest) (messaging.PeersResult, error) {
		peerCalls++
		return messaging.PeersResult{Generation: "gen-1"}, nil
	}
	s := newToolServer(t, cl)

	res, err := connectToolSession(t, s).CallTool(context.Background(), &mcpsdk.CallToolParams{Name: "peers"})
	if err != nil {
		t.Fatalf("call peers: %v", err)
	}
	if peerCalls != 1 {
		t.Fatalf("peer listings = %d, want 1", peerCalls)
	}
	structured, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content = %T, want an object", res.StructuredContent)
	}
	if _, listed := structured["peers"]; !listed {
		t.Fatalf("structured content = %#v, want the list action's peers key", structured)
	}
	if text := toolResultText(t, res); text != router.RenderPeers(nil) {
		t.Fatalf("content text = %q, want the empty list block", text)
	}
}

// TestPeersToolRendersRouterRefusal pins the refusal envelope: the router's
// sentence as content, its object as structured content, and the error flag.
// The refusal is produced the way the control plane produces one — a typed code
// on the RPC error — so the provider's authority mapping runs too.
func TestPeersToolRendersRouterRefusal(t *testing.T) {
	const refusal = "rename refused: bad name"
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{}}
	cl.renameFunc = func(context.Context, string, string) (messaging.RenameResult, error) {
		return messaging.RenameResult{}, &client.RPCError{
			Code:    -32000,
			Message: refusal,
			Data:    json.RawMessage(`{"code":"invalid_name"}`),
		}
	}
	s := newToolServer(t, cl)

	res, err := connectToolSession(t, s).CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name:      "peers",
		Arguments: map[string]any{"action": "rename", "session_id": "run-1", "name": "renamed"},
	})
	if err != nil {
		t.Fatalf("call peers: %v", err)
	}
	if !res.IsError {
		t.Fatal("peers refusal result is not flagged as an error")
	}
	if text := toolResultText(t, res); text != refusal {
		t.Fatalf("content text = %q, want %q", text, refusal)
	}
	want := (&router.ToolError{Code: "invalid_name", Message: refusal}).Output()
	if !reflect.DeepEqual(res.StructuredContent, want) {
		t.Fatalf("structured content = %#v, want %#v", res.StructuredContent, want)
	}
}

// TestMessageToolPassesNonToolErrorToJSONRPC proves a failure that is not a
// router refusal is not turned into an isError envelope. A provider that cannot
// bind a generation fails at router construction, which is the one failure the
// router does not classify.
func TestMessageToolPassesNonToolErrorToJSONRPC(t *testing.T) {
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{}}
	cl.preflightFunc = func(context.Context) (messaging.PreflightResult, error) {
		return messaging.PreflightResult{}, errors.New("preflight boom")
	}
	s := newToolServer(t, cl)

	_, err := connectToolSession(t, s).CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name:      "message",
		Arguments: map[string]any{"action": "tell", "target": "run-1", "message": "hello"},
	})
	if err == nil {
		t.Fatal("call message error = nil, want the non-refusal failure")
	}
	if !strings.Contains(err.Error(), "preflight boom") {
		t.Fatalf("call message error = %v, want the provider failure", err)
	}

	// The envelope helper itself keeps its hand off a non-refusal.
	if res, err := toolErrorResult(errors.New("boom")); res != nil || err == nil {
		t.Fatalf("toolErrorResult(plain error) = %#v, %v; want nil, the error", res, err)
	}
}

// messageStubSupervisor is a control store that speaks the verbs the merged
// message and peers tools call and records what it received, so a test can tell
// which supervisor served a call.
type messageStubSupervisor struct {
	mu      sync.Mutex
	methods []string
	sends   []control.MessageSendParams
	renames []control.SessionRenameParams
}

func (s *messageStubSupervisor) record(req client.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.methods = append(s.methods, req.Method)
	switch req.Method {
	case "message_send":
		var params control.MessageSendParams
		_ = json.Unmarshal(req.Params, &params)
		s.sends = append(s.sends, params)
	case "session_rename":
		var params control.SessionRenameParams
		_ = json.Unmarshal(req.Params, &params)
		s.renames = append(s.renames, params)
	}
}

func (s *messageStubSupervisor) recordedSends() []control.MessageSendParams {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]control.MessageSendParams{}, s.sends...)
}

func (s *messageStubSupervisor) recordedRenames() []control.SessionRenameParams {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]control.SessionRenameParams{}, s.renames...)
}

func stubResult(req client.Request, result any) client.Response {
	resp := client.Response{JSONRPC: "2.0", ID: req.ID}
	resp.Result, _ = json.Marshal(result)
	return resp
}

// serve answers one connection. Every response carries a marker only this stub
// produces, so a result that reaches the test came from this supervisor.
func (s *messageStubSupervisor) serve(conn net.Conn, cap control.OwnerCapability) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var req client.Request
		if json.Unmarshal(scanner.Bytes(), &req) != nil {
			continue
		}
		s.record(req)
		var resp client.Response
		switch req.Method {
		case "authenticate_owner":
			resp = ownerAuthResponse(req, cap)
		case "message_preflight":
			resp = stubResult(req, control.MessagePreflightResult{
				Protocol:   messaging.ProtocolVersion,
				Generation: "gen-stub",
			})
		case "status":
			resp = stubResult(req, runningStatus())
		case "message_send":
			var params control.MessageSendParams
			_ = json.Unmarshal(req.Params, &params)
			resp = stubResult(req, control.MessageSendResult{
				Outcome:    string(messaging.OutcomeSent),
				MessageID:  params.MessageID,
				Generation: "gen-stub",
			})
		case "session_rename":
			resp = stubResult(req, control.SessionRenameResult{
				RunID:         "rt-remote",
				Label:         "renamed-remote",
				PreviousLabel: "remote",
			})
		case "message_peers":
			resp = stubResult(req, control.MessagePeersResult{Generation: "gen-stub"})
		default:
			resp = client.Response{JSONRPC: "2.0", ID: req.ID, Error: &client.RespError{Code: -32601, Message: "method not found"}}
		}
		data, _ := json.Marshal(resp)
		if _, err := conn.Write(append(data, '\n')); err != nil {
			return
		}
	}
}

// startMessageStubSupervisor serves the merged tools' verbs on a socket inside
// root, publishing the owner record the owner attach needs.
func startMessageStubSupervisor(t *testing.T, root, name string) (*messageStubSupervisor, string) {
	t.Helper()
	socketPath := filepath.Join(root, name)
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen %s: %v", socketPath, err)
	}
	cap := publishOwnerRecordForStub(t, socketPath)
	stub := &messageStubSupervisor{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go stub.serve(conn, cap)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		os.Remove(socketPath)
		os.Remove(control.OwnerRecordPath(socketPath))
	})
	return stub, socketPath
}

// TestMessageToolBindsNonDefaultSupervisor proves a tell to a run the registry
// stores on another supervisor reaches that supervisor's client, not the
// default one.
func TestMessageToolBindsNonDefaultSupervisor(t *testing.T) {
	root := stubSocketsRoot(t, t.TempDir())
	stub, socketPath := startMessageStubSupervisor(t, root, "avenor-mcp-fix06-remote.sock")

	cl := &fakeMessageControlClient{fakeClient: &fakeClient{statusResult: runningStatus()}}
	var defaultSends int
	cl.sendFunc = func(_ context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
		defaultSends++
		return messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: req.MessageID, Generation: req.Generation}, nil
	}
	s := newToolServer(t, cl)
	if err := s.registry.Store(&RunInfo{
		RunID:        "run-remote",
		RuntimeID:    "rt-remote",
		Label:        "remote",
		SupervisorID: socketPath,
	}); err != nil {
		t.Fatalf("registry.Store: %v", err)
	}

	res, err := connectToolSession(t, s).CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name:      "message",
		Arguments: map[string]any{"action": "tell", "target": "run-remote", "message": "hello"},
	})
	if err != nil {
		t.Fatalf("call message: %v", err)
	}
	if res.IsError {
		t.Fatalf("message result is an error: %s", toolResultText(t, res))
	}
	sends := stub.recordedSends()
	if len(sends) != 1 {
		t.Fatalf("non-default supervisor sends = %d, want 1", len(sends))
	}
	if sends[0].ToRunID != "rt-remote" {
		t.Fatalf("non-default supervisor send target = %q, want rt-remote", sends[0].ToRunID)
	}
	if defaultSends != 0 {
		t.Fatalf("default supervisor sends = %d, want none", defaultSends)
	}
	if len(cl.statusCapturedRuntimeIDs) != 0 {
		t.Fatalf("default supervisor status reads = %v, want none", cl.statusCapturedRuntimeIDs)
	}
}

// TestPeersToolBindsNonDefaultSupervisor is the peers twin: a rename whose
// session_id names a run on another supervisor is written there.
func TestPeersToolBindsNonDefaultSupervisor(t *testing.T) {
	root := stubSocketsRoot(t, t.TempDir())
	stub, socketPath := startMessageStubSupervisor(t, root, "avenor-mcp-fix06-remote-peers.sock")

	cl := &fakeMessageControlClient{fakeClient: &fakeClient{}}
	s := newToolServer(t, cl)
	if err := s.registry.Store(&RunInfo{
		RunID:        "run-remote",
		RuntimeID:    "rt-remote",
		Label:        "remote",
		SupervisorID: socketPath,
	}); err != nil {
		t.Fatalf("registry.Store: %v", err)
	}

	res, err := connectToolSession(t, s).CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name:      "peers",
		Arguments: map[string]any{"action": "rename", "session_id": "run-remote", "name": "renamed"},
	})
	if err != nil {
		t.Fatalf("call peers: %v", err)
	}
	renames := stub.recordedRenames()
	if len(renames) != 1 {
		t.Fatalf("non-default supervisor renames = %d, want 1", len(renames))
	}
	if renames[0].SessionID != "run-remote" || renames[0].Name != "renamed" {
		t.Fatalf("non-default supervisor rename = %#v, want run-remote/renamed", renames[0])
	}
	if len(cl.renameCalls) != 0 {
		t.Fatalf("default supervisor renames = %v, want none", cl.renameCalls)
	}
	structured, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content = %T, want an object", res.StructuredContent)
	}
	renamed, _ := structured["renamed"].(map[string]any)
	if renamed["label"] != "renamed-remote" {
		t.Fatalf("renamed label = %v, want the non-default supervisor's answer", renamed["label"])
	}
}

// TestPeersToolAmbiguousRunUsesDefaultSupervisor proves a key that identifies
// more than one run binds nothing: the call answers on the default supervisor
// instead of the entry a map iteration reached first.
func TestPeersToolAmbiguousRunUsesDefaultSupervisor(t *testing.T) {
	root := stubSocketsRoot(t, t.TempDir())
	stub, socketPath := startMessageStubSupervisor(t, root, "avenor-mcp-fix06-ambiguous.sock")
	dials := withCountedDials(t)

	cl := &fakeMessageControlClient{fakeClient: &fakeClient{}}
	cl.renameFunc = func(_ context.Context, sessionID, name string) (messaging.RenameResult, error) {
		return messaging.RenameResult{RunID: sessionID, Label: "default-renamed", PreviousLabel: "dup-a"}, nil
	}
	s := newToolServer(t, cl)
	for _, stored := range []struct{ supervisor, label string }{
		{socketPath, "dup-a"},
		{"/tmp/avenor-mcp-fix06-other.sock", "dup-b"},
	} {
		if err := s.registry.Store(&RunInfo{
			RunID:        "run-dup",
			RuntimeID:    "rt-dup",
			Label:        stored.label,
			SupervisorID: stored.supervisor,
		}); err != nil {
			t.Fatalf("registry.Store: %v", err)
		}
	}

	res, err := connectToolSession(t, s).CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name:      "peers",
		Arguments: map[string]any{"action": "rename", "session_id": "run-dup", "name": "renamed"},
	})
	if err != nil {
		t.Fatalf("call peers: %v", err)
	}
	if len(cl.renameCalls) != 1 || cl.renameCalls[0].sessionID != "run-dup" {
		t.Fatalf("default supervisor renames = %v, want one rename of run-dup", cl.renameCalls)
	}
	if renames := stub.recordedRenames(); len(renames) != 0 {
		t.Fatalf("ambiguous key bound a supervisor: %#v", renames)
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("dials = %d, want none for an ambiguous key", got)
	}
	structured, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content = %T, want an object", res.StructuredContent)
	}
	renamed, _ := structured["renamed"].(map[string]any)
	if renamed["label"] != "default-renamed" {
		t.Fatalf("renamed label = %v, want the default supervisor's answer", renamed["label"])
	}
}
