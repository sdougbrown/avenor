package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/messaging"
)

// bindingClient answers the conversation bind and records what a spawn sent, so
// a test can read both the parent a child was told and the identities the server
// bound.
type bindingClient struct {
	fakeClient
	mu     sync.Mutex
	binds  []messaging.MessageBindRequest
	result messaging.MessageBindResult
	err    error
}

func (c *bindingClient) MessageBindContext(_ context.Context, req messaging.MessageBindRequest) (messaging.MessageBindResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.binds = append(c.binds, req)
	return c.result, c.err
}

func (c *bindingClient) bindCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.binds)
}

func (c *bindingClient) setResult(res messaging.MessageBindResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.result = res
}

func (c *bindingClient) spawnedParams() map[string]any {
	return c.spawnCapturedParams
}

// readOnlyClient is a control connection without the bind verb, such as an
// injected client from a surface that never opened an owner connection.
type readOnlyClient struct{ fakeClient }

// boundResult is one conversation's bound identity on a local broker.
func boundResult(runID, token string) messaging.MessageBindResult {
	return messaging.MessageBindResult{
		BrokerURL:  "http://127.0.0.1:1",
		RunID:      runID,
		RunToken:   token,
		Generation: "gen-1",
		Created:    true,
	}
}

// writeCloserSpy records whether the handoff pipe was closed and whether the
// launch bounded its write.
type writeCloserSpy struct {
	mu          sync.Mutex
	writes      int
	closed      bool
	err         error
	deadlineErr error
	deadlineOK  bool
}

func (w *writeCloserSpy) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes++
	if w.err != nil {
		return 0, w.err
	}
	return len(p), nil
}

func (w *writeCloserSpy) SetWriteDeadline(time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadlineOK = true
	return w.deadlineErr
}

func (w *writeCloserSpy) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *writeCloserSpy) state() (writes int, closed bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes, w.closed
}

// declaredServer builds a server that serves one declared host conversation over
// the injected control client, capturing its diagnostics.
func declaredServer(t *testing.T, cl ControlClient, host, session string, handoff io.WriteCloser) (*Server, *bytes.Buffer) {
	t.Helper()
	if handoff == nil {
		handoff = &writeCloserSpy{}
	}
	diag := &bytes.Buffer{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: cl,
		BindHost:      host,
		BindSession:   session,
		BindHandoff:   handoff,
		Stderr:        diag,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return s, diag
}

func TestCloseBeforeServingClosesTheHandoff(t *testing.T) {
	pipe := &writeCloserSpy{}
	s, _ := declaredServer(t, &bindingClient{}, "poo", "ses_close", pipe)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if writes, closed := pipe.state(); writes != 0 || !closed {
		t.Fatalf("after close: writes = %d, pipe closed = %v; want no writes and a closed pipe", writes, closed)
	}
}

// bindDeclared runs the launch's one bind, the step ServeTransport performs
// before it serves anything.
func bindDeclared(t *testing.T, s *Server) {
	t.Helper()
	if err := s.bindHostConversation(context.Background()); err != nil {
		t.Fatalf("conversation bind: %v", err)
	}
}

// serveDeclared serves the server over an in-memory MCP transport and returns
// the client session a caller drives it with.
func serveDeclared(t *testing.T, ctx context.Context, s *Server) (*mcpsdk.ClientSession, error) {
	t.Helper()
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serveCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.ServeTransport(serveCtx, serverTransport) }()
	t.Cleanup(func() {
		_ = s.Close()
		cancel()
		select {
		case <-serveErr:
		case <-time.After(5 * time.Second):
			t.Error("ServeTransport did not return after cancel")
		}
	})
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "bind-test", Version: "1"}, nil)
	cs, err := client.Connect(serveCtx, clientTransport, nil)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, nil
}

// bindLine reads the one JSON line a handoff wrote and returns the decoded
// identity.
func bindLine(t *testing.T, line []byte) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(line), &decoded); err != nil {
		t.Fatalf("handoff line is not one JSON object: %v (%q)", err, line)
	}
	return decoded
}

// TestHostBindDeclaration pins what a launch may declare: both halves together,
// a per-conversation host of the bind policy, a valid native session id, a
// transport that serves one client, and the handoff pipe the identity is written
// to.
func TestHostBindDeclaration(t *testing.T) {
	for _, tc := range []struct {
		name       string
		transport  string
		host       string
		session    string
		handoff    bool
		wantErr    string
		wantDeclar bool
	}{
		{name: "none", transport: "stdio", wantDeclar: false},
		{name: "host without session", transport: "stdio", host: "poo", wantErr: "needs a host, a session id and a handoff pipe together"},
		{name: "session without host", transport: "stdio", session: "ses_poo", wantErr: "needs a host, a session id and a handoff pipe together"},
		{name: "handoff without conversation", transport: "stdio", handoff: true, wantErr: "needs a host, a session id and a handoff pipe together"},
		{name: "conversation without handoff", transport: "stdio", host: "poo", session: "ses_poo", wantErr: "needs a host, a session id and a handoff pipe together"},
		{name: "shared transport", transport: "http", host: "poo", session: "ses_poo", handoff: true, wantErr: "declared only on the stdio transport"},
		{name: "unserved host", transport: "stdio", host: "claude", session: "ses_1", handoff: true, wantErr: `host "claude" is not a native host the conversation bind serves`},
		{name: "reuse-only host", transport: "stdio", host: "pi", session: "ses_pi", handoff: true, wantErr: "binds the supervisor's own run"},
		{name: "invalid session", transport: "stdio", host: "poo", session: "bad/session", handoff: true, wantErr: "session id must be 1-200 bytes"},
		{name: "declared", transport: "stdio", host: "poo", session: "ses_poo", handoff: true, wantDeclar: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var handoff io.WriteCloser
			if tc.handoff {
				handoff = &writeCloserSpy{}
			}
			s, err := NewServer(Options{
				Transport:     tc.transport,
				NoAutostart:   true,
				AuthToken:     "token",
				ControlClient: &bindingClient{},
				BindHost:      tc.host,
				BindSession:   tc.session,
				BindHandoff:   handoff,
			})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("NewServer error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			defer s.Close()
			if declared := s.hostBind != nil; declared != tc.wantDeclar {
				t.Fatalf("declared = %v, want %v", declared, tc.wantDeclar)
			}
		})
	}
}

// TestHostBindHandsOverOneLineAndClosesThePipe pins the handoff itself: the
// launch binds once, writes the existing bind result as one JSON line on the
// pipe, closes the pipe, and keeps the credentials out of its diagnostics while
// remembering only the run the children must name.
func TestHostBindHandsOverOneLineAndClosesThePipe(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	cl := &bindingClient{}
	cl.setResult(boundResult("run_poo", "tok_secret"))
	s, diag := declaredServer(t, cl, "poo", "ses_poo", write)
	defer s.Close()

	bindDeclared(t, s)

	line, err := io.ReadAll(read)
	if err != nil {
		t.Fatalf("read handoff pipe: %v", err)
	}
	if n := bytes.Count(line, []byte("\n")); n != 1 || !bytes.HasSuffix(line, []byte("\n")) {
		t.Fatalf("handoff = %q, want exactly one newline-terminated line", line)
	}
	decoded := bindLine(t, line)
	want := map[string]any{
		"broker_url": "http://127.0.0.1:1",
		"run_id":     "run_poo",
		"run_token":  "tok_secret",
		"generation": "gen-1",
		"created":    true,
	}
	if len(decoded) != len(want) {
		t.Fatalf("handoff fields = %v, want exactly %v", decoded, want)
	}
	for key, value := range want {
		if decoded[key] != value {
			t.Fatalf("handoff[%q] = %v, want %v", key, decoded[key], value)
		}
	}
	// The pipe is closed after the handoff, so the host reads EOF and never
	// waits for more.
	if _, err := read.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read after handoff = %v, want EOF from a closed pipe", err)
	}
	if strings.Contains(diag.String(), "tok_secret") {
		t.Fatalf("diagnostics leaked the credential: %s", diag.String())
	}
	if cl.bindCount() != 1 {
		t.Fatalf("binds = %d, want the launch's one bind", cl.bindCount())
	}
	if path, runID := s.hostBind.identity(); runID != "run_poo" {
		t.Fatalf("identity = (%q, %q), want the bound run", path, runID)
	}
}

// TestHandoffFailureFailsTheLaunch pins that the handoff is the launch's proof
// the host holds the identity: a pipe that cannot take the line fails the launch
// before any tool call, and the pipe is closed either way.
func TestHandoffFailureFailsTheLaunch(t *testing.T) {
	cl := &bindingClient{}
	cl.setResult(boundResult("run_poo", "tok_secret"))
	handoff := &writeCloserSpy{err: io.ErrClosedPipe}
	s, diag := declaredServer(t, cl, "poo", "ses_poo", handoff)
	defer s.Close()

	err := s.bindHostConversation(context.Background())
	if err == nil || !strings.Contains(err.Error(), "hand off to the launch pipe") {
		t.Fatalf("bind = %v, want the handoff failure", err)
	}
	writes, closed := handoff.state()
	if writes != 1 || !closed {
		t.Fatalf("handoff writes = %d, closed = %v, want one write and a close", writes, closed)
	}
	if strings.Contains(err.Error(), "tok_secret") || strings.Contains(diag.String(), "tok_secret") {
		t.Fatalf("a failed handoff leaked the credential: %v / %s", err, diag.String())
	}
	if _, runID := s.hostBind.identity(); runID != "" {
		t.Fatalf("a failed handoff recorded run %q; the launch has no identity to serve", runID)
	}
}

// TestHandoffDoesNotWaitOnAFullPipe pins the startup bound: a pipe whose reader
// is not draining must fail the launch rather than hang it.
func TestHandoffDoesNotWaitOnAFullPipe(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	// Fill the pipe so the handoff's own line cannot land.
	if err := write.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("set fill deadline: %v", err)
	}
	if _, err := write.Write(make([]byte, 1<<20)); err == nil {
		t.Fatal("filling the pipe did not fill it")
	}

	oldTimeout := hostHandoffTimeout
	hostHandoffTimeout = 50 * time.Millisecond
	t.Cleanup(func() { hostHandoffTimeout = oldTimeout })

	cl := &bindingClient{}
	cl.setResult(boundResult("run_poo", "tok_secret"))
	s, _ := declaredServer(t, cl, "poo", "ses_poo", write)
	defer s.Close()

	done := make(chan error, 1)
	go func() { done <- s.bindHostConversation(context.Background()) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "hand off to the launch pipe") {
			t.Fatalf("bind = %v, want the handoff timeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handoff waited on a reader that never drained the pipe")
	}
}

// TestLaunchFailsWhenTheBindFails pins the removed fallback: a launch whose bind
// cannot land does not serve unbound and does not bind again later.
func TestLaunchFailsWhenTheBindFails(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	cl := &bindingClient{}
	cl.err = errors.New("supervisor is restarting")
	s, _ := declaredServer(t, cl, "poo", "ses_poo", write)
	defer s.Close()

	serverTransport, _ := mcpsdk.NewInMemoryTransports()
	err = s.ServeTransport(context.Background(), serverTransport)
	if err == nil || !strings.Contains(err.Error(), "supervisor is restarting") {
		t.Fatalf("ServeTransport = %v, want the bind failure before serving", err)
	}
	if cl.bindCount() != 1 {
		t.Fatalf("binds = %d, want the launch's one failed attempt", cl.bindCount())
	}
	if line, _ := io.ReadAll(read); len(line) != 0 {
		t.Fatalf("handoff = %q, want nothing when the bind fails", line)
	}
}

// TestLaunchFailsWithoutABindableConnection pins the control-connection
// requirement: a client that cannot bind fails the launch instead of serving a
// conversation nothing routes.
func TestLaunchFailsWithoutABindableConnection(t *testing.T) {
	s, _ := declaredServer(t, &readOnlyClient{}, "poo", "ses_poo", nil)
	defer s.Close()

	err := s.bindHostConversation(context.Background())
	if err == nil || !strings.Contains(err.Error(), "cannot bind a conversation") {
		t.Fatalf("bind = %v, want the control-connection refusal", err)
	}
}

// TestServedSpawnNamesTheBoundConversationAsParent pins the order the launch
// promises and the parent every spawn names: the bind and handoff complete
// before the first tool call is served, and each spawn this server forwards to
// its own supervisor carries the conversation's run as parent_run_id without a
// runtime parent or a second bind.
func TestServedSpawnNamesTheBoundConversationAsParent(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	cl := &bindingClient{}
	cl.spawnResult = map[string]any{"runtime_id": "rt_1", "session_id": "ses_1"}
	cl.setResult(boundResult("run_poo", "tok_poo"))
	s, diag := declaredServer(t, cl, "poo", "ses_poo", write)

	cs, err := serveDeclared(t, context.Background(), s)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	// The identity is already on the pipe before the client can call a tool.
	if line, _ := io.ReadAll(read); len(line) == 0 {
		t.Fatal("the server served a client before handing the identity over")
	}

	for i := 0; i < 2; i++ {
		res, err := cs.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: "spawn", Arguments: map[string]any{"dir": t.TempDir(), "prompt": "work"}})
		if err != nil {
			t.Fatalf("spawn tool: %v", err)
		}
		if res.IsError {
			t.Fatalf("spawn tool error: %+v", res.Content)
		}
		params := cl.spawnedParams()
		if got, _ := params["parent_run_id"].(string); got != "run_poo" {
			t.Fatalf("spawn parent_run_id = %q, want the conversation's bound run", got)
		}
		for _, key := range []string{"parent_id", "runtime_id"} {
			if _, ok := params[key]; ok {
				t.Fatalf("spawn sent %q; the runtime parent is the supervisor's own bookkeeping", key)
			}
		}
		if cl.bindCount() != 1 {
			t.Fatalf("binds = %d, want the launch's one bind", cl.bindCount())
		}
	}
	req := cl.binds[0]
	if req.Host != "poo" || req.SessionID != "ses_poo" {
		t.Fatalf("bind request = %+v, want the declared conversation", req)
	}
	if diag.Len() != 0 {
		t.Fatalf("unexpected diagnostics: %s", diag)
	}
}

// TestSpawnOnAnotherSupervisorNamesNoParent pins the ownership rule: a spawn
// aimed at a supervisor other than the one the conversation binds on gets no
// parent, is never asked to bind the conversation, and reports the missing route.
func TestSpawnOnAnotherSupervisorNamesNoParent(t *testing.T) {
	cl := &bindingClient{}
	cl.setResult(boundResult("run_poo", "tok_poo"))
	s, diag := declaredServer(t, cl, "poo", "ses_poo", nil)
	defer s.Close()
	s.defaultSupervisorPath = "/run/own.sock"
	bindDeclared(t, s)

	if got := s.boundParentRun("/run/own.sock"); got != "run_poo" {
		t.Fatalf("bound parent on its supervisor = %q, want the bound run", got)
	}
	if got := s.boundParentRun("/run/other.sock"); got != "" {
		t.Fatalf("bound parent on another supervisor = %q, want none", got)
	}
	if cl.bindCount() != 1 {
		t.Fatalf("binds = %d, want only the launch's own bind", cl.bindCount())
	}
	if !strings.Contains(diag.String(), "parent run run_poo lives on /run/own.sock") {
		t.Fatalf("diagnostics did not name the run's supervisor: %s", diag)
	}
}

// TestHandoffWithoutADeadlineFailsTheLaunch pins the bound the handoff needs: a
// writer that cannot take a write deadline is refused before the write, never
// written to without a bound.
func TestHandoffWithoutADeadlineFailsTheLaunch(t *testing.T) {
	cl := &bindingClient{}
	cl.setResult(boundResult("run_poo", "tok_secret"))
	handoff := &noDeadlineWriter{}
	s, _ := declaredServer(t, cl, "poo", "ses_poo", handoff)
	defer s.Close()

	err := s.bindHostConversation(context.Background())
	if err == nil || !strings.Contains(err.Error(), "cannot bound its write") {
		t.Fatalf("bind = %v, want the unbounded-writer refusal", err)
	}
	if handoff.writes != 0 {
		t.Fatalf("writes = %d, want none without a bound", handoff.writes)
	}
	if !handoff.closed {
		t.Fatal("the handoff pipe was left open")
	}
}

// TestHandoffDeadlineSetupErrorFailsTheLaunch pins that a failed deadline setup
// is reported, not swallowed: without it the write it was meant to bound would
// run unbounded.
func TestHandoffDeadlineSetupErrorFailsTheLaunch(t *testing.T) {
	cl := &bindingClient{}
	cl.setResult(boundResult("run_poo", "tok_secret"))
	handoff := &writeCloserSpy{deadlineErr: errors.New("descriptor is blocking")}
	s, _ := declaredServer(t, cl, "poo", "ses_poo", handoff)
	defer s.Close()

	err := s.bindHostConversation(context.Background())
	if err == nil || !strings.Contains(err.Error(), "bound the handoff write") {
		t.Fatalf("bind = %v, want the deadline setup failure", err)
	}
	writes, closed := handoff.state()
	if writes != 0 || !closed {
		t.Fatalf("writes = %d, closed = %v, want no write and a closed pipe", writes, closed)
	}
}

// noDeadlineWriter is a writer with no deadline support at all.
type noDeadlineWriter struct {
	writes int
	closed bool
}

func (w *noDeadlineWriter) Write(p []byte) (int, error) {
	w.writes++
	return len(p), nil
}

func (w *noDeadlineWriter) Close() error {
	w.closed = true
	return nil
}

// TestUnboundServerLeavesSpawnParamsAlone pins the preserved default: a server
// whose launch declared no conversation binds nothing, writes nothing, and adds
// no parent.
func TestUnboundServerLeavesSpawnParamsAlone(t *testing.T) {
	cl := &bindingClient{}
	cl.spawnResult = map[string]any{"runtime_id": "rt_1"}
	cl.setResult(boundResult("run_unused", "tok_unused"))
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl, Stderr: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.bindHostConversation(context.Background()); err != nil {
		t.Fatalf("an unbound launch bound something: %v", err)
	}

	if _, _, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{Dir: t.TempDir(), Prompt: "work"}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if _, ok := cl.spawnedParams()["parent_run_id"]; ok {
		t.Fatal("an unbound spawn must not name a parent")
	}
	if cl.bindCount() != 0 {
		t.Fatal("an unbound server must not bind a conversation")
	}
}
