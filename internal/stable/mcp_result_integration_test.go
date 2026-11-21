// This file holds the deliberate cross-boundary integration tests in the
// stable package: they import internal/mcpserver (a surface) so stable's test
// binary links a surface binary. They prove two properties of the MCP surface
// against a real supervisor and a real control socket:
//
//   - a runtime result carries the per-turn sentinel_file through control to
//     the MCP result tool: a run spawned with its own sentinel file and
//     per-turn event log answers with its complete output and no truncation
//     flag, even after the event log is moved out from under the runtime;
//   - the MCP spawn tool refuses a roster entry's launch fields.
//
// New stable tests must not import internal/mcpserver; keep the surface
// boundary here.
package stable

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/mcpserver"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

type resultAuthTransport struct{}

func (resultAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer test-token")
	return http.DefaultTransport.RoundTrip(req)
}

func TestMCPResultUsesRealSupervisorTurnOwnership(t *testing.T) {
	const runID = "11111111-1111-1111-1111-111111111111"
	output := strings.Repeat("complete output ", 3000)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "avenor-run-"+runID+".done")
	eventPath := filepath.Join(dir, "events.ndjson")
	provider := newItestProvider(itestAttempt("ses_mcp", itestSessionEnd("ses_mcp", "end_turn", output)))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))
	rt := h.spawn(map[string]any{"sentinel_file": sentinel, "on_event": eventPath, "label": "mcp-result"})
	h.waitTurn(rt)
	// waitTurn returns when the turn settles, which is before the success
	// presentation sets the terminal phase: the settle clears the phase and
	// the presentation sets it again. Wait for the settled status so the
	// result tool reads a completed run rather than a mid-transition one.
	h.awaitSettledStatus(rt)
	if err := os.Remove(eventPath); err != nil {
		t.Fatal(err)
	}
	cl := h.connectClient()
	server, err := mcpserver.NewServer(mcpserver.Options{Transport: "http", Addr: "127.0.0.1:0", AuthToken: "test-token", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	httpServer := httptest.NewServer(server.HTTPHandler())
	defer httpServer.Close()
	c := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil)
	session, err := c.Connect(context.Background(), &mcpsdk.StreamableClientTransport{Endpoint: httpServer.URL, HTTPClient: &http.Client{Transport: resultAuthTransport{}}, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: "result", Arguments: map[string]any{"run_id": runID, "wait": false}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("tool error: %#v", result.Content)
	}
	data, ok := result.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content: %T", result.StructuredContent)
	}
	got, _ := data["output"].(string)
	if got != output || data["output_truncated"] == true {
		t.Fatalf("result output length = %d, truncated = %v", len(got), data["output_truncated"])
	}
}

// resultToolText returns the single text content of a tool result.
func resultToolText(t *testing.T, res *mcpsdk.CallToolResult) string {
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

// TestMCPToolSpawnRefusesCallerChosenLaunch drives the surface the reviewed
// bypass used end to end: the MCP spawn tool, over the real control socket,
// with a roster file the caller wrote. The tool takes the gated verb for every
// request now, so the roster entry's command is refused and no provider is
// built.
func TestMCPToolSpawnRefusesCallerChosenLaunch(t *testing.T) {
	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search","command":"/opt/bin/evil-poo"}}`)
	var created atomic.Int32
	h := newItestHarness(t, Config{}, func(runtime.StartOptions, string) (runtime.Provider, error) {
		created.Add(1)
		return nil, errors.New("a refused spawn must not build a provider")
	})
	cl := h.connectClient()
	server, err := mcpserver.NewServer(mcpserver.Options{Transport: "http", Addr: "127.0.0.1:0", AuthToken: "test-token", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	httpServer := httptest.NewServer(server.HTTPHandler())
	defer httpServer.Close()
	c := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil)
	session, err := c.Connect(context.Background(), &mcpsdk.StreamableClientTransport{Endpoint: httpServer.URL, HTTPClient: &http.Client{Transport: resultAuthTransport{}}, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	result, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: "spawn", Arguments: map[string]any{
		"dir":          dir,
		"prompt":       "hi",
		"roster_file":  rosterPath,
		"roster_entry": "tool",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("spawn tool = success, want the launch-field refusal")
	}
	if got := resultToolText(t, result); !strings.Contains(got, `roster entry "tool": launch fields are disabled for MCP spawns (command)`) {
		t.Fatalf("spawn tool error = %q, want the launch-field refusal", got)
	}
	if n := created.Load(); n != 0 {
		t.Fatalf("provider creations = %d, want 0", n)
	}
}

// conversationChild is the fake child backend for the declared-conversation
// drive: on its first prompt it asks the parent run named in the spawn note and
// records the answer, so the test observes the return path a real child uses.
type conversationChild struct {
	*stableFakeProvider
	mu       sync.Mutex
	runID    string
	broker   *broker.Broker
	token    string
	note     string
	parent   string
	reply    string
	outcome  messaging.Outcome
	askErr   error
	finished chan struct{}
}

func newConversationChild() *conversationChild {
	return &conversationChild{
		stableFakeProvider: &stableFakeProvider{events: make(chan events.Event, 1)},
		finished:           make(chan struct{}),
	}
}

func (c *conversationChild) Start(_ context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	c.mu.Lock()
	c.runID, c.broker, c.token = opts.RuntimeID, opts.Broker, opts.BrokerToken
	c.mu.Unlock()
	return runtime.Session{SessionID: "ses_" + opts.RuntimeID}, nil
}

func (c *conversationChild) Prompt(ctx context.Context, _, prompt string) error {
	c.mu.Lock()
	c.note = prompt
	c.mu.Unlock()
	defer close(c.finished)
	c.record(askParentRun(ctx, c))
	p := c.snapshot()
	c.events <- events.Event{Event: "session.end", SessionID: "ses_" + p.runID, Fields: map[string]any{"stop_reason": "end_turn", "run_id": p.runID}}
	return nil
}

// askParentRun sends an ask to the parent run the spawn note named and waits for
// the reply. A child whose note names no parent records why instead.
func askParentRun(ctx context.Context, c *conversationChild) (parent, reply string, outcome messaging.Outcome, err error) {
	me := c.snapshot()
	parent = noteParentRun(me.note)
	if parent == "" {
		return "", "", "", errors.New("the spawn note named no parent run")
	}
	tr, err := localhttp.New(me.broker.Addr(), messaging.LocalCredentials{RunID: me.runID, Token: me.token})
	if err != nil {
		return parent, "", "", err
	}
	defer tr.Close()
	pre, err := tr.Preflight(ctx)
	if err != nil {
		return parent, "", "", err
	}
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(2*time.Minute))
	if err != nil {
		return parent, "", "", err
	}
	body, err := messaging.AskBody(msgID, me.runID, parent, "ready?", "agent")
	if err != nil {
		return parent, "", "", err
	}
	sent, err := tr.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: me.runID},
		To:         messaging.Identity{RunID: parent},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       string(body),
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		return parent, "", sent.Outcome, fmt.Errorf("child ask = (%+v, %v), want sent", sent, err)
	}
	waited, err := tr.Wait(ctx, messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: me.runID},
		MessageID:  msgID,
		Generation: pre.Generation,
		Timeout:    10 * time.Second,
	})
	if err != nil {
		return parent, "", waited.Outcome, err
	}
	if waited.Reply != nil {
		reply = string(waited.Reply.Payload)
	}
	return parent, reply, waited.Outcome, nil
}

func (c *conversationChild) record(parent, reply string, outcome messaging.Outcome, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.parent, c.reply, c.outcome, c.askErr = parent, reply, outcome, err
}

// conversationChildState is one child's settled facts, read without its lock.
type conversationChildState struct {
	runID   string
	broker  *broker.Broker
	token   string
	note    string
	parent  string
	reply   string
	outcome messaging.Outcome
	askErr  error
}

func (c *conversationChild) snapshot() conversationChildState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return conversationChildState{
		runID:   c.runID,
		broker:  c.broker,
		token:   c.token,
		note:    c.note,
		parent:  c.parent,
		reply:   c.reply,
		outcome: c.outcome,
		askErr:  c.askErr,
	}
}

// noteParentRun reads the quoted parent run id out of a spawn note.
func noteParentRun(note string) string {
	const marker = "broker run ID is "
	_, rest, ok := strings.Cut(note, marker)
	if !ok {
		return ""
	}
	_, quoted, ok := strings.Cut(rest, `"`)
	if !ok {
		return ""
	}
	parent, _, ok := strings.Cut(quoted, `"`)
	if !ok {
		return ""
	}
	return parent
}

// connectMCP declares one host conversation on a server over its supervisor and
// returns the session a client drives it with.
func connectMCP(t *testing.T, ctx context.Context, cl mcpserver.ControlClient, host, session string) *mcpsdk.ClientSession {
	t.Helper()
	srv, err := mcpserver.NewServer(mcpserver.Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: cl,
		BindHost:      host,
		BindSession:   session,
		Stderr:        os.Stderr,
	})
	if err != nil {
		t.Fatalf("mcp server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serveCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() { _ = srv.ServeTransport(serveCtx, serverTransport) }()
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "poo-test", Version: "1"}, nil)
	cs, err := client.Connect(serveCtx, clientTransport, nil)
	if err != nil {
		t.Fatalf("connect mcp client: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// spawnOverMCP drives the spawn tool and returns its structured result.
func spawnOverMCP(t *testing.T, ctx context.Context, cs *mcpsdk.ClientSession, args map[string]any) {
	t.Helper()
	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{Name: "spawn", Arguments: args})
	if err != nil {
		t.Fatalf("spawn tool: %v", err)
	}
	if res.IsError {
		t.Fatalf("spawn tool error: %s", resultToolText(t, res))
	}
}

// answerAsBoundRun receives one inbound message as the bound conversation and
// answers it, so the child's ask gets a reply.
func answerAsBoundRun(t *testing.T, ctx context.Context, bound messaging.MessageBindResult) string {
	t.Helper()
	tr, err := localhttp.New("", messaging.LocalCredentials{RunID: bound.RunID, Token: bound.RunToken, BaseURL: bound.BrokerURL})
	if err != nil {
		t.Fatalf("bound transport: %v", err)
	}
	defer tr.Close()
	pre, err := tr.Preflight(ctx)
	if err != nil {
		t.Fatalf("bound preflight: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		rec, err := tr.Receive(ctx, messaging.ReceiveRequest{
			Sender:     messaging.Identity{RunID: bound.RunID},
			Generation: pre.Generation,
			Timeout:    time.Second,
			Limit:      8,
		})
		if err != nil {
			t.Fatalf("bound receive: %v", err)
		}
		for _, msg := range rec.Messages {
			if msg.FromRunID == bound.RunID {
				continue
			}
			if _, err := tr.Reply(ctx, messaging.ReplyRequest{
				From:          messaging.Identity{RunID: bound.RunID},
				ToRunID:       msg.FromRunID,
				MessageID:     msg.MessageID,
				CorrelationID: msg.MessageID,
				Body:          `{"message":"pong","role":"host"}`,
			}); err != nil {
				t.Fatalf("bound reply: %v", err)
			}
			return msg.FromRunID
		}
		if time.Now().After(deadline) {
			t.Fatal("the bound conversation received nothing from the child")
		}
	}
}

// TestMCPDeclaredConversationIsTheChildParent drives the seam end to end: two
// declared host conversations on one real supervisor, each spawning a child
// through the real MCP spawn tool. Each child is told its own conversation's
// bound run as its broker parent, sends as a distinct peer, and gets an answer
// from that bound inbox alone.
func TestMCPDeclaredConversationIsTheChildParent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	children := make(chan *conversationChild, 4)
	h := newItestHarness(t, Config{}, func(runtime.StartOptions, string) (runtime.Provider, error) {
		child := newConversationChild()
		children <- child
		return child, nil
	})
	cl := h.connectClient()

	bound := map[string]messaging.MessageBindResult{}
	for _, session := range []string{"ses_poo_a", "ses_poo_b"} {
		res, err := cl.MessageBind(messaging.MessageBindRequest{Host: "poo", SessionID: session})
		if err != nil {
			t.Fatalf("message_bind %s: %v", session, err)
		}
		if !res.Created {
			t.Fatalf("bind %s reported created=false, want the conversation's own run", session)
		}
		bound[session] = res
		cs := connectMCP(t, ctx, cl, "poo", session)
		spawnOverMCP(t, ctx, cs, map[string]any{"dir": t.TempDir(), "prompt": "report", "backend": "pony"})
	}
	if bound["ses_poo_a"].RunID == bound["ses_poo_b"].RunID {
		t.Fatal("two conversations must not share one bound run")
	}

	answered := map[string]string{}
	for i := 0; i < 2; i++ {
		var child *conversationChild
		select {
		case child = <-children:
		case <-ctx.Done():
			t.Fatal("the spawn built no child provider")
		}
		snapshot := child.snapshot()
		parent := noteParentRun(snapshot.note)
		if parent == "" {
			t.Fatalf("spawn note named no parent run: %q", snapshot.note)
		}
		var session string
		for name, res := range bound {
			if res.RunID == parent {
				session = name
			}
		}
		if session == "" {
			t.Fatalf("child was told parent %q, which is neither declared conversation's run", parent)
		}
		answered[answerAsBoundRun(t, ctx, bound[session])] = session
		select {
		case <-child.finished:
		case <-ctx.Done():
			t.Fatal("the child never finished its ask")
		}
		done := child.snapshot()
		if done.askErr != nil {
			t.Fatalf("child ask: %v", done.askErr)
		}
		if done.outcome != messaging.OutcomeReplied {
			t.Fatalf("child ask outcome = %q, want replied", done.outcome)
		}
		if !strings.Contains(done.reply, "pong") {
			t.Fatalf("child reply = %q, want the bound conversation's answer", done.reply)
		}
		if answered[done.runID] != session {
			t.Fatalf("child %s was answered by conversation %s, want %s", done.runID, answered[done.runID], session)
		}
	}

	// Each child speaks as its own distinct peer, and the bound conversations are
	// peers of the same broker.
	pre, err := cl.MessagePreflight()
	if err != nil {
		t.Fatalf("message_preflight: %v", err)
	}
	peers, err := cl.MessagePeers(messaging.PeersRequest{Sender: messaging.Identity{RunID: messaging.SupervisorRunID}, Generation: pre.Generation})
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	visible := map[string]bool{}
	for _, peer := range peers.Peers {
		visible[peer.RunID] = true
	}
	for session, res := range bound {
		if !visible[res.RunID] {
			t.Fatalf("bound run %s of %s is not a peer", res.RunID, session)
		}
	}
	for childRunID := range answered {
		if !visible[childRunID] {
			t.Fatalf("child run %s is not a distinct peer", childRunID)
		}
	}
}

// awaitNote waits for one child's spawn note, which its prompt delivers
// asynchronously.
func awaitNote(t *testing.T, ctx context.Context, child *conversationChild) string {
	t.Helper()
	for {
		if note := child.snapshot().note; note != "" {
			return note
		}
		select {
		case <-ctx.Done():
			t.Fatal("the child never received a prompt")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestMCPDeclaredConversationSkipsAnAlternateSupervisor pins the ownership rule
// on real supervisors: a spawn aimed at another supervisor runs there without a
// parent route, and that supervisor is never asked to bind the conversation, so
// no parent record is minted in a broker nothing consumes from.
func TestMCPDeclaredConversationSkipsAnAlternateSupervisor(t *testing.T) {
	// A short avenor home keeps the alternate supervisor's tool-addressable
	// socket path under the unix socket length limit.
	home, err := os.MkdirTemp("/tmp", "avh-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	sockets := filepath.Join(home, "sockets")
	if err := os.MkdirAll(sockets, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AVENOR_HOME", home)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	main := newItestHarness(t, Config{}, func(runtime.StartOptions, string) (runtime.Provider, error) {
		return newConversationChild(), nil
	})
	alternateChildren := make(chan *conversationChild, 1)
	alternate := newItestHarness(t, Config{
		ControlSocket: filepath.Join(sockets, "avenor-mcp-alternate.sock"),
	}, func(runtime.StartOptions, string) (runtime.Provider, error) {
		child := newConversationChild()
		alternateChildren <- child
		return child, nil
	})
	cl := main.connectClient()
	bound, err := cl.MessageBind(messaging.MessageBindRequest{Host: "poo", SessionID: "ses_poo"})
	if err != nil {
		t.Fatalf("message_bind: %v", err)
	}

	cs := connectMCP(t, ctx, cl, "poo", "ses_poo")
	spawnOverMCP(t, ctx, cs, map[string]any{
		"dir":           t.TempDir(),
		"prompt":        "report",
		"backend":       "pony",
		"supervisor_id": alternate.socket,
	})

	var alternateChild *conversationChild
	select {
	case alternateChild = <-alternateChildren:
	case <-ctx.Done():
		t.Fatal("the alternate supervisor built no child provider")
	}
	if note := awaitNote(t, ctx, alternateChild); note != "report" {
		t.Fatalf("the child on the alternate supervisor was told %q, want its prompt alone with no parent route", note)
	}
	if record := alternate.sup.broker.GetRun(bound.RunID); record != nil {
		t.Fatalf("the alternate supervisor minted a parent record for %q", bound.RunID)
	}
	if record := main.sup.broker.GetRun(bound.RunID); record == nil {
		t.Fatalf("the bound run %q is missing from its own supervisor", bound.RunID)
	}
}
