package mcpserver

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"

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

// declaredServer builds a server that serves one declared host conversation over
// the injected control client, capturing its diagnostics.
func declaredServer(t *testing.T, cl ControlClient, host, session string) (*Server, *bytes.Buffer) {
	t.Helper()
	diag := &bytes.Buffer{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: cl,
		BindHost:      host,
		BindSession:   session,
		Stderr:        diag,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return s, diag
}

// spawnOnce drives one MCP spawn through the tool handler.
func spawnOnce(t *testing.T, s *Server, args spawnArgs) {
	t.Helper()
	if _, _, err := s.handleAvenorSpawn(context.Background(), nil, args); err != nil {
		t.Fatalf("spawn: %v", err)
	}
}

// TestHostBindDeclaration pins what a launch may declare: both halves together,
// a per-conversation host of the bind policy, a valid native session id, and a
// transport that serves one client.
func TestHostBindDeclaration(t *testing.T) {
	for _, tc := range []struct {
		name       string
		transport  string
		host       string
		session    string
		wantErr    string
		wantDeclar bool
	}{
		{name: "none", transport: "stdio", wantDeclar: false},
		{name: "host without session", transport: "stdio", host: "poo", wantErr: "needs a host and a session id together"},
		{name: "session without host", transport: "stdio", session: "ses_poo", wantErr: "needs a host and a session id together"},
		{name: "shared transport", transport: "http", host: "poo", session: "ses_poo", wantErr: "declared only on the stdio transport"},
		{name: "unserved host", transport: "stdio", host: "claude", session: "ses_1", wantErr: `host "claude" is not a native host the conversation bind serves`},
		{name: "reuse-only host", transport: "stdio", host: "pi", session: "ses_pi", wantErr: "binds the supervisor's own run"},
		{name: "invalid session", transport: "stdio", host: "poo", session: "bad/session", wantErr: "session id must be 1-200 bytes"},
		{name: "declared", transport: "stdio", host: "poo", session: "ses_poo", wantDeclar: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewServer(Options{
				Transport:     tc.transport,
				NoAutostart:   true,
				AuthToken:     "token",
				ControlClient: &bindingClient{},
				BindHost:      tc.host,
				BindSession:   tc.session,
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
			if declared := s.hostBind != nil; declared != tc.wantDeclar {
				t.Fatalf("declared = %v, want %v", declared, tc.wantDeclar)
			}
		})
	}
}

// TestSpawnNamesTheDeclaredConversationAsParent pins the same-supervisor spawn
// path: the child is told the conversation's bound run is its broker parent,
// the runtime parent is left to the supervisor, and the conversation's identity
// is the one the launch bound, reused by every later spawn.
func TestSpawnNamesTheDeclaredConversationAsParent(t *testing.T) {
	cl := &bindingClient{}
	cl.spawnResult = map[string]any{"runtime_id": "rt_1", "session_id": "ses_1"}
	cl.setResult(boundResult("run_poo", "tok_poo"))
	s, diag := declaredServer(t, cl, "poo", "ses_poo")
	defer s.Close()

	spawnOnce(t, s, spawnArgs{Dir: t.TempDir(), Prompt: "work"})
	params := cl.spawnedParams()
	if got, _ := params["parent_run_id"].(string); got != "run_poo" {
		t.Fatalf("spawn parent_run_id = %q, want the conversation's bound run %q", got, "run_poo")
	}
	for _, key := range []string{"parent_id", "runtime_id"} {
		if _, ok := params[key]; ok {
			t.Fatalf("spawn sent %q; the runtime parent is the supervisor's own bookkeeping", key)
		}
	}
	if cl.bindCount() != 1 {
		t.Fatalf("binds = %d, want one bind for the launch", cl.bindCount())
	}
	req := cl.binds[0]
	if req.Host != "poo" || req.SessionID != "ses_poo" {
		t.Fatalf("bind request = %+v, want the declared conversation", req)
	}

	// A second spawn reuses the bound run instead of binding again: the host is
	// still using the identity it was given.
	spawnOnce(t, s, spawnArgs{Dir: t.TempDir(), Prompt: "more work"})
	if got, _ := cl.spawnedParams()["parent_run_id"].(string); got != "run_poo" {
		t.Fatalf("second spawn parent_run_id = %q, want the same bound run", got)
	}
	if cl.bindCount() != 1 {
		t.Fatalf("binds after a second spawn = %d, want the launch's one bind", cl.bindCount())
	}
	if diag.Len() != 0 {
		t.Fatalf("unexpected diagnostics: %s", diag)
	}
}

// TestSpawnOnAnotherSupervisorNamesNoParent pins the ownership rule: a spawn
// aimed at a supervisor other than the one the conversation binds on gets no
// parent, and that supervisor is never asked to bind the conversation.
func TestSpawnOnAnotherSupervisorNamesNoParent(t *testing.T) {
	cl := &bindingClient{}
	cl.setResult(boundResult("run_poo", "tok_poo"))
	s, diag := declaredServer(t, cl, "poo", "ses_poo")
	defer s.Close()

	other := &bindingClient{}
	if got := s.boundParentRun(context.Background(), other, "/run/other.sock"); got != "" {
		t.Fatalf("bound parent on another supervisor = %q, want none", got)
	}
	if other.bindCount() != 0 {
		t.Fatal("the other supervisor must not be asked to bind the conversation")
	}
	if !strings.Contains(diag.String(), "so the child starts without a parent route") {
		t.Fatalf("diagnostics did not report the missing parent route: %s", diag)
	}
}

// TestBoundConversationStaysOnItsSupervisor pins the binding's ownership after
// it is established: a later spawn aimed elsewhere names no parent even though
// the launch has already bound the conversation.
func TestBoundConversationStaysOnItsSupervisor(t *testing.T) {
	cl := &bindingClient{}
	cl.setResult(boundResult("run_poo", "tok_poo"))
	s, diag := declaredServer(t, cl, "poo", "ses_poo")
	defer s.Close()

	if got := s.boundParentRun(context.Background(), cl, ""); got != "run_poo" {
		t.Fatalf("bound parent = %q, want the bound run", got)
	}
	if got := s.boundParentRun(context.Background(), cl, "/run/other.sock"); got != "" {
		t.Fatalf("bound parent on another supervisor = %q, want none", got)
	}
	if cl.bindCount() != 1 {
		t.Fatalf("binds = %d, want the launch's one bind", cl.bindCount())
	}
	if !strings.Contains(diag.String(), "parent run run_poo lives on") {
		t.Fatalf("diagnostics did not name the run's supervisor: %s", diag)
	}
}

// TestBindFailureLeavesTheSpawnUnbound pins the availability boundary: a bind
// that cannot land is reported, leaves no parent route, and does not fail the
// spawn.
func TestBindFailureLeavesTheSpawnUnbound(t *testing.T) {
	cl := &bindingClient{}
	cl.spawnResult = map[string]any{"runtime_id": "rt_1"}
	cl.err = io.ErrUnexpectedEOF
	s, diag := declaredServer(t, cl, "poo", "ses_poo")
	defer s.Close()

	spawnOnce(t, s, spawnArgs{Dir: t.TempDir(), Prompt: "work"})
	if _, ok := cl.spawnedParams()["parent_run_id"]; ok {
		t.Fatal("a failed bind must not name a parent")
	}
	if !strings.Contains(diag.String(), "conversation bind failed") {
		t.Fatalf("diagnostics did not report the failed bind: %s", diag)
	}
}

// TestRecoveredBindIsUsedOnce pins recovery from a bind that failed at startup:
// the next spawn retries, and from then on the launch reuses the run it got.
func TestRecoveredBindIsUsedOnce(t *testing.T) {
	cl := &bindingClient{}
	cl.spawnResult = map[string]any{"runtime_id": "rt_1"}
	cl.err = io.ErrUnexpectedEOF
	s, _ := declaredServer(t, cl, "poo", "ses_poo")
	defer s.Close()

	spawnOnce(t, s, spawnArgs{Dir: t.TempDir(), Prompt: "work"})
	if _, ok := cl.spawnedParams()["parent_run_id"]; ok {
		t.Fatal("a failed bind must not name a parent")
	}
	cl.mu.Lock()
	cl.err, cl.result = nil, boundResult("run_poo", "tok_poo")
	cl.mu.Unlock()
	spawnOnce(t, s, spawnArgs{Dir: t.TempDir(), Prompt: "work"})
	if got, _ := cl.spawnedParams()["parent_run_id"].(string); got != "run_poo" {
		t.Fatalf("parent_run_id after recovery = %q, want the bound run", got)
	}
	if cl.bindCount() != 2 {
		t.Fatalf("binds = %d, want the failed attempt plus the recovered one", cl.bindCount())
	}
}

// TestUnboundServerLeavesSpawnParamsAlone pins the preserved default: a server
// whose launch declared no conversation binds nothing and adds no parent.
func TestUnboundServerLeavesSpawnParamsAlone(t *testing.T) {
	cl := &bindingClient{}
	cl.spawnResult = map[string]any{"runtime_id": "rt_1"}
	cl.setResult(boundResult("run_unused", "tok_unused"))
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl, Stderr: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	spawnOnce(t, s, spawnArgs{Dir: t.TempDir(), Prompt: "work"})
	if _, ok := cl.spawnedParams()["parent_run_id"]; ok {
		t.Fatal("an unbound spawn must not name a parent")
	}
	if cl.bindCount() != 0 {
		t.Fatal("an unbound server must not bind a conversation")
	}
}
