// This file holds the deliberate cross-boundary integration tests in the
// stable package: they import internal/mcpserver (a surface) so stable's test
// binary links a surface binary. They prove three properties of the MCP surface
// against a real supervisor and a real control socket:
//
//   - a runtime result carries the per-turn sentinel_file through control to
//     the MCP result tool: a run spawned with its own sentinel file and
//     per-turn event log answers with its complete output and no truncation
//     flag, even after the event log is moved out from under the runtime;
//   - the MCP spawn tool refuses a roster entry's launch fields;
//   - a bound conversation's avenor://inbox carries a child's ask to the MCP
//     client and the reply it sends back through the message tool reaches that
//     child, on both the resources/subscribe and subscriptions/listen revisions,
//     and it shares one consumer lock with `avenor feed broker` in both
//     directions.
//
// New stable tests must not import internal/mcpserver; keep the surface
// boundary here.
package stable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/feed"
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

// connectMCPOptions parameterizes a bound MCP connection. The zero value is the
// launch that declares a private handoff pipe and connects an SDK client with
// its defaults; a caller that drives the inbox itself passes what it needs.
type connectMCPOptions struct {
	// noHandoff declares the conversation without a pipe. The launch then keeps
	// the identity in memory, which is what a server that serves its own inbox
	// does: nothing else needs the credentials.
	noHandoff bool
	// client and session configure the MCP client.
	client  *mcpsdk.ClientOptions
	session *mcpsdk.ClientSessionOptions
}

// connectMCP declares one host conversation on a server over its supervisor and
// returns the session a client drives it with, plus the identity the launch
// handed over on the private pipe. The line is read before the client connects:
// the launch writes it before it serves anything.
func connectMCP(t *testing.T, ctx context.Context, opts mcpserver.Options, host, session string) (*mcpsdk.ClientSession, messaging.MessageBindResult) {
	t.Helper()
	cs, bound, _ := connectMCPWith(t, ctx, opts, host, session, connectMCPOptions{})
	return cs, bound
}

// connectMCPWith connects one client to a bound MCP server over an in-memory
// transport. It also returns a waiter for the served transport's exit, so a test
// can observe what the server releases when its client goes away. The returned
// identity is empty for a launch that declared no pipe: that launch hands its
// credentials to nobody.
func connectMCPWith(t *testing.T, ctx context.Context, opts mcpserver.Options, host, session string, extra connectMCPOptions) (*mcpsdk.ClientSession, messaging.MessageBindResult, func() error) {
	t.Helper()
	opts.Transport, opts.NoAutostart = "stdio", true
	opts.BindHost, opts.BindSession = host, session
	var handoff *os.File
	if !extra.noHandoff {
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = read.Close() })
		handoff = read
		opts.BindHandoff = write
	}
	if opts.Stderr == nil {
		opts.Stderr = os.Stderr
	}
	srv, err := mcpserver.NewServer(opts)
	if err != nil {
		t.Fatalf("mcp server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serveCtx, cancel := context.WithCancel(ctx)
	var (
		serveOnce sync.Once
		serveErr  error
	)
	serveDone := make(chan error, 1)
	awaitServe := func() error {
		serveOnce.Do(func() {
			select {
			case serveErr = <-serveDone:
			case <-time.After(15 * time.Second):
				t.Error("ServeTransport did not return")
			}
		})
		return serveErr
	}
	t.Cleanup(func() {
		cancel()
		awaitServe()
	})
	go func() { serveDone <- srv.ServeTransport(serveCtx, serverTransport) }()

	var handed messaging.MessageBindResult
	if handoff != nil {
		line, err := io.ReadAll(handoff)
		if err != nil {
			t.Fatalf("read handoff: %v", err)
		}
		if len(line) == 0 {
			t.Fatal("the launch handed over no identity: it failed before serving")
		}
		var handedControl control.MessageBindResult
		if err := json.Unmarshal(line, &handedControl); err != nil {
			t.Fatalf("handoff line: %v", err)
		}
		handed = handedControl.MessageBindResult()
		if handed.RunID == "" || handed.RunToken == "" || handed.BrokerURL == "" || handed.Generation == "" {
			t.Fatalf("handoff identity = %+v, want the bind result's five fields", handed)
		}
	}

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "poo-test", Version: "1"}, extra.client)
	cs, err := client.Connect(serveCtx, clientTransport, extra.session)
	if err != nil {
		t.Fatalf("connect mcp client: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, handed, awaitServe
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
		cs, identity := connectMCP(t, ctx, mcpserver.Options{ControlClient: cl}, "poo", session)
		if !identity.Created {
			t.Fatalf("bind %s reported created=false, want the conversation's own run", session)
		}
		bound[session] = identity
		spawnOverMCP(t, ctx, cs, map[string]any{"dir": t.TempDir(), "prompt": "report", "backend": "poo"})
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
	cs, bound := connectMCP(t, ctx, mcpserver.Options{ControlClient: cl}, "poo", "ses_poo")
	spawnOverMCP(t, ctx, cs, map[string]any{
		"dir":           t.TempDir(),
		"prompt":        "report",
		"backend":       "poo",
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

// TestMCPDeclaredConversationRefusesAStaleBinding drives the restart case end to
// end: an MCP server binds its conversation at launch, the supervisor that
// minted that binding is replaced on the same socket, and the server keeps
// serving with the identity it handed over. A spawn must then be refused with
// the reconnect instruction — never rebound, and never given a parent record the
// replacement minted by that id — so no child starts against a parent nothing
// consumes.
func TestMCPDeclaredConversationRefusesAStaleBinding(t *testing.T) {
	// A short avenor home keeps the tool-addressable socket path under the unix
	// socket length limit, and the replacement needs the same path.
	home, err := os.MkdirTemp("/tmp", "avb-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	sockets := filepath.Join(home, "sockets")
	if err := os.MkdirAll(sockets, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AVENOR_HOME", home)
	socket := filepath.Join(sockets, "avenor-mcp-restart.sock")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var firstChildren atomic.Int32
	first := newItestHarness(t, Config{ControlSocket: socket}, func(runtime.StartOptions, string) (runtime.Provider, error) {
		firstChildren.Add(1)
		return newConversationChild(), nil
	})
	cs, bound := connectMCP(t, ctx, mcpserver.Options{SupervisorSocket: socket}, "poo", "ses_poo")

	// Replace the supervisor that minted the binding on the same socket.
	first.shutdown("kill")
	if err := first.sup.control.Stop(); err != nil {
		t.Fatalf("stop first supervisor: %v", err)
	}
	var replacementChildren atomic.Int32
	replacement := newItestHarness(t, Config{ControlSocket: socket}, func(runtime.StartOptions, string) (runtime.Provider, error) {
		replacementChildren.Add(1)
		return newConversationChild(), nil
	})
	if _, err := replacement.sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "poo", SessionID: "ses_poo"}); err != nil {
		t.Fatalf("message_bind on the replacement: %v", err)
	}

	// The MCP server's cached connection died with the supervisor, so the first
	// call may still report the transport; every call must be refused, and the
	// supervisor's own refusal names the reconnect.
	refused := false
	var lastText string
	for attempt := 0; attempt < 4 && !refused; attempt++ {
		res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{Name: "spawn", Arguments: map[string]any{
			"dir":     t.TempDir(),
			"prompt":  "report",
			"backend": "poo",
		}})
		if err != nil {
			t.Fatalf("spawn tool call: %v", err)
		}
		if !res.IsError {
			t.Fatalf("spawn after a supervisor replacement succeeded; a stale binding must be refused")
		}
		lastText = resultToolText(t, res)
		refused = strings.Contains(lastText, "reconnect the host conversation")
	}
	if !refused {
		t.Fatalf("spawn refusal = %q, want the reconnect instruction", lastText)
	}
	if n := firstChildren.Load(); n != 0 {
		t.Fatalf("the first supervisor started %d children", n)
	}
	if n := replacementChildren.Load(); n != 0 {
		t.Fatalf("the replacement started %d children for a stale binding", n)
	}
	if record := replacement.sup.broker.GetRun(bound.RunID); record != nil {
		t.Fatalf("the replacement minted a ghost parent record for %q", bound.RunID)
	}
}

// inboxAsk is the part of one avenor://inbox message a reply needs. The JSON
// shape is the resource's contract; this package sees it only through the wire.
type inboxAsk struct {
	ID           string `json:"id"`
	From         string `json:"from"`
	Text         string `json:"text"`
	ExpectsReply bool   `json:"expects_reply"`
	Answerable   bool   `json:"answerable"`
}

// decodeInboxPayload reads the messages one inbox read reports.
func decodeInboxPayload(t *testing.T, text string) []inboxAsk {
	t.Helper()
	var payload struct {
		Messages []inboxAsk `json:"messages"`
	}
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		t.Fatalf("decode the inbox payload: %v (%q)", err, text)
	}
	return payload.Messages
}

// answerableAsk returns the one ask a delivered batch carries, failing if the
// conversation was told about a message it cannot answer.
func answerableAsk(t *testing.T, messages []inboxAsk) inboxAsk {
	t.Helper()
	for _, message := range messages {
		if !message.ExpectsReply {
			continue
		}
		if !message.Answerable {
			t.Fatalf("the delivered ask %+v reports no route for its reply", message)
		}
		return message
	}
	t.Fatalf("the inbox reported no ask: %+v", messages)
	return inboxAsk{}
}

// inboxDriver is the protocol-specific half of the inbox round trip: how the
// conversation subscribes, spawns, learns about the ask and answers it.
type inboxDriver interface {
	// subscribe opens the conversation's subscription to its own inbox.
	subscribe(t *testing.T)
	// spawn starts one child over the MCP spawn tool.
	spawn(t *testing.T, dir string)
	// awaitAsk waits for the inbox to be reported changed and returns the ask it
	// holds.
	awaitAsk(t *testing.T) inboxAsk
	// reply answers one ask through the MCP message tool.
	reply(t *testing.T, askID, text string)
}

// legacyInboxDriver drives the resources/subscribe revision through the SDK
// client, which reports resource updates through a handler.
type legacyInboxDriver struct {
	ctx     context.Context
	cs      *mcpsdk.ClientSession
	updates chan struct{}
}

func (d legacyInboxDriver) subscribe(t *testing.T) {
	t.Helper()
	if err := d.cs.Subscribe(d.ctx, &mcpsdk.SubscribeParams{URI: mcpserver.InboxResourceURI}); err != nil {
		t.Fatalf("subscribe to %s: %v", mcpserver.InboxResourceURI, err)
	}
}

func (d legacyInboxDriver) spawn(t *testing.T, dir string) {
	t.Helper()
	spawnOverMCP(t, d.ctx, d.cs, map[string]any{"dir": dir, "prompt": "report", "backend": "poo"})
}

func (d legacyInboxDriver) awaitAsk(t *testing.T) inboxAsk {
	t.Helper()
	select {
	case <-d.updates:
	case <-d.ctx.Done():
		t.Fatal("the conversation was never told its inbox changed")
	}
	res, err := d.cs.ReadResource(d.ctx, &mcpsdk.ReadResourceParams{URI: mcpserver.InboxResourceURI})
	if err != nil {
		t.Fatalf("read %s: %v", mcpserver.InboxResourceURI, err)
	}
	if len(res.Contents) != 1 {
		t.Fatalf("the read returned %d contents, want 1", len(res.Contents))
	}
	return answerableAsk(t, decodeInboxPayload(t, res.Contents[0].Text))
}

func (d legacyInboxDriver) reply(t *testing.T, askID, text string) {
	t.Helper()
	res, err := d.cs.CallTool(d.ctx, &mcpsdk.CallToolParams{Name: "message", Arguments: map[string]any{
		"action": "reply", "reply_to": askID, "message": text,
	}})
	if err != nil {
		t.Fatalf("message reply: %v", err)
	}
	if res.IsError {
		t.Fatalf("message reply tool error: %s", resultToolText(t, res))
	}
}

// rawMCPStream drives one MCP server over newline-delimited JSON-RPC pipes. The
// SDK client runs subscriptions/listen in a background goroutine and returns only
// when that stream ends, so the revision without resources/subscribe has to be
// driven frame by frame.
type rawMCPStream struct {
	t      *testing.T
	to     *io.PipeWriter
	frames chan map[string]any
}

// serveRawMCP serves one bound MCP server over pipes and starts reading its
// frames.
func serveRawMCP(t *testing.T, ctx context.Context, opts mcpserver.Options) *rawMCPStream {
	t.Helper()
	opts.Transport, opts.NoAutostart = "stdio", true
	opts.BindHost, opts.BindSession, opts.BindHandoff = "poo", "ses_poo", nil
	if opts.Stderr == nil {
		opts.Stderr = os.Stderr
	}
	srv, err := mcpserver.NewServer(opts)
	if err != nil {
		t.Fatalf("mcp server: %v", err)
	}
	clientToServer, fromClient := io.Pipe()
	toClient, serverToClient := io.Pipe()
	serveCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- srv.ServeTransport(serveCtx, &mcpsdk.IOTransport{Reader: clientToServer, Writer: serverToClient})
	}()
	stream := &rawMCPStream{t: t, to: fromClient, frames: make(chan map[string]any, 64)}
	go func() {
		defer close(stream.frames)
		decoder := json.NewDecoder(toClient)
		for {
			var frame map[string]any
			if err := decoder.Decode(&frame); err != nil {
				return
			}
			stream.frames <- frame
		}
	}()
	t.Cleanup(func() {
		_ = fromClient.Close()
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("ServeTransport did not return")
		}
		_ = srv.Close()
	})
	return stream
}

// send writes one frame.
func (s *rawMCPStream) send(frame map[string]any) {
	s.t.Helper()
	frame["jsonrpc"] = "2.0"
	encoded, err := json.Marshal(frame)
	if err != nil {
		s.t.Fatalf("encode frame %+v: %v", frame, err)
	}
	if _, err := s.to.Write(append(encoded, '\n')); err != nil {
		s.t.Fatalf("write frame %+v: %v", frame, err)
	}
}

// call sends one request under the SEP-2575 per-request metadata every call in
// this revision carries.
func (s *rawMCPStream) call(id int, method string, params map[string]any) {
	s.t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = map[string]any{
		"io.modelcontextprotocol/protocolVersion":    "2026-07-28",
		"io.modelcontextprotocol/clientInfo":         map[string]any{"name": "stable-raw", "version": "1"},
		"io.modelcontextprotocol/clientCapabilities": map[string]any{},
	}
	s.send(map[string]any{"id": id, "method": method, "params": params})
}

// await returns the next frame matching match.
func (s *rawMCPStream) await(what string, match func(map[string]any) bool) map[string]any {
	s.t.Helper()
	deadline := time.After(30 * time.Second)
	var seen []map[string]any
	for {
		select {
		case frame, ok := <-s.frames:
			if !ok {
				s.t.Fatalf("the server closed the stream while waiting for %s; frames seen: %#v", what, seen)
			}
			seen = append(seen, frame)
			if match(frame) {
				return frame
			}
		case <-deadline:
			s.t.Fatalf("timed out waiting for %s; frames seen: %#v", what, seen)
		}
	}
}

// result returns the result object of a response frame.
func (s *rawMCPStream) result(frame map[string]any) map[string]any {
	s.t.Helper()
	result, ok := frame["result"].(map[string]any)
	if !ok {
		s.t.Fatalf("frame %#v carries no result", frame)
	}
	return result
}

// mcpFrameWithID matches the response to one request.
func mcpFrameWithID(id float64) func(map[string]any) bool {
	return func(frame map[string]any) bool {
		frameID, ok := frame["id"].(float64)
		return ok && frameID == id
	}
}

// modernInboxDriver drives the subscriptions/listen revision over the raw stream.
type modernInboxDriver struct {
	stream *rawMCPStream
}

func (d modernInboxDriver) subscribe(t *testing.T) {
	t.Helper()
	d.stream.call(10, "subscriptions/listen", map[string]any{
		"notifications": map[string]any{"resourceSubscriptions": []string{mcpserver.InboxResourceURI}},
	})
	d.stream.await("subscriptions/acknowledged", func(frame map[string]any) bool {
		return frame["method"] == "notifications/subscriptions/acknowledged"
	})
}

func (d modernInboxDriver) spawn(t *testing.T, dir string) {
	t.Helper()
	d.stream.call(11, "tools/call", map[string]any{
		"name":      "spawn",
		"arguments": map[string]any{"dir": dir, "prompt": "report", "backend": "poo"},
	})
	res := d.stream.result(d.stream.await("the spawn result", mcpFrameWithID(11)))
	if isError, _ := res["isError"].(bool); isError {
		t.Fatalf("spawn tool error: %#v", res)
	}
}

func (d modernInboxDriver) awaitAsk(t *testing.T) inboxAsk {
	t.Helper()
	d.stream.await("notifications/resources/updated", func(frame map[string]any) bool {
		return frame["method"] == "notifications/resources/updated"
	})
	d.stream.call(20, "resources/read", map[string]any{"uri": mcpserver.InboxResourceURI})
	read := d.stream.result(d.stream.await("the inbox read", mcpFrameWithID(20)))
	contents, _ := read["contents"].([]any)
	if len(contents) != 1 {
		t.Fatalf("the read returned %d contents, want 1", len(contents))
	}
	content, _ := contents[0].(map[string]any)
	text, _ := content["text"].(string)
	return answerableAsk(t, decodeInboxPayload(t, text))
}

func (d modernInboxDriver) reply(t *testing.T, askID, text string) {
	t.Helper()
	d.stream.call(30, "tools/call", map[string]any{
		"name": "message",
		"arguments": map[string]any{
			"action": "reply", "reply_to": askID, "message": text,
		},
	})
	res := d.stream.result(d.stream.await("the reply result", mcpFrameWithID(30)))
	if isError, _ := res["isError"].(bool); isError {
		t.Fatalf("message reply tool error: %#v", res)
	}
}

// awaitConversationChild returns the provider the spawn built.
func awaitConversationChild(t *testing.T, ctx context.Context, children <-chan *conversationChild) *conversationChild {
	t.Helper()
	select {
	case child := <-children:
		return child
	case <-ctx.Done():
		t.Fatal("the spawn built no child provider")
	}
	return nil
}

// assertChildAnswered waits for the child's own ask to settle and checks what it
// received back.
func assertChildAnswered(t *testing.T, ctx context.Context, child *conversationChild, want string) {
	t.Helper()
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
	if !strings.Contains(done.reply, want) {
		t.Fatalf("child reply = %q, want %q", done.reply, want)
	}
}

// driveInboxConversation runs the round trip one protocol revision at a time: the
// conversation subscribes to its own inbox, a child spawned over MCP asks it
// something, and the answer the conversation sends through the MCP message tool
// reaches that child.
func driveInboxConversation(t *testing.T, ctx context.Context, driver inboxDriver, children <-chan *conversationChild) {
	t.Helper()
	driver.subscribe(t)
	driver.spawn(t, t.TempDir())
	child := awaitConversationChild(t, ctx, children)
	ask := driver.awaitAsk(t)
	if ask.From == "" || ask.Text == "" {
		t.Fatalf("the delivered ask = %+v, want the child's question and its sender", ask)
	}
	driver.reply(t, ask.ID, "pong")
	assertChildAnswered(t, ctx, child, "pong")
}

// TestMCPInboxDeliversChildAskAndRepliesOverMCP proves the inbox round trip
// against a real supervisor and a real broker on both protocol revisions: each
// subscribes by its own RPC, is woken by its own notification, drains the same
// resource, and answers with the same message tool.
func TestMCPInboxDeliversChildAskAndRepliesOverMCP(t *testing.T) {
	for _, version := range []string{"2025-11-25", "2026-07-28"} {
		t.Run(version, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			t.Setenv("XDG_STATE_HOME", t.TempDir())
			children := make(chan *conversationChild, 1)
			h := newItestHarness(t, Config{}, func(runtime.StartOptions, string) (runtime.Provider, error) {
				child := newConversationChild()
				children <- child
				return child, nil
			})
			cl := h.connectClient()

			if version == "2026-07-28" {
				// The SDK client cannot drive a listen stream synchronously, so this
				// revision speaks the protocol over pipes.
				driveInboxConversation(t, ctx, modernInboxDriver{stream: serveRawMCP(t, ctx, mcpserver.Options{ControlClient: cl})}, children)
				return
			}
			updates := make(chan struct{}, 4)
			cs, _, _ := connectMCPWith(t, ctx, mcpserver.Options{ControlClient: cl}, "poo", "ses_poo", connectMCPOptions{
				noHandoff: true,
				client: &mcpsdk.ClientOptions{ResourceUpdatedHandler: func(_ context.Context, req *mcpsdk.ResourceUpdatedNotificationRequest) {
					if req.Params != nil && req.Params.URI == mcpserver.InboxResourceURI {
						select {
						case updates <- struct{}{}:
						default:
						}
					}
				}},
				session: &mcpsdk.ClientSessionOptions{ProtocolVersion: version},
			})
			driveInboxConversation(t, ctx, legacyInboxDriver{ctx: ctx, cs: cs, updates: updates}, children)
		})
	}
}

// feedRecord is one NDJSON line `avenor feed broker` printed.
type feedRecord struct {
	Text string `json:"text"`
	From string `json:"from"`
	ID   string `json:"id"`
}

// firstFeedRecord decodes the first line the feed printed.
func firstFeedRecord(t *testing.T, out string) feedRecord {
	t.Helper()
	line, _, _ := strings.Cut(out, "\n")
	var record feedRecord
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		t.Fatalf("feed record %q: %v", line, err)
	}
	return record
}

// TestMCPInboxReplyWorksForFeedDeliveredAsk pins the shared claim directory: an
// ask the *feed* delivered is answerable from the MCP surface, because both write
// and read the one record the launch resolved for the conversation.
func TestMCPInboxReplyWorksForFeedDeliveredAsk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	children := make(chan *conversationChild, 1)
	h := newItestHarness(t, Config{}, func(runtime.StartOptions, string) (runtime.Provider, error) {
		child := newConversationChild()
		children <- child
		return child, nil
	})
	cl := h.connectClient()
	cs, bound := connectMCP(t, ctx, mcpserver.Options{ControlClient: cl}, "poo", "ses_poo")

	// The feed consumes the inbox instead of the MCP resource, using the identity
	// the launch handed over.
	var records, feedLog syncBuffer
	feedCtx, stopFeed := context.WithCancel(ctx)
	defer stopFeed()
	go func() {
		feed.Broker{
			RunID:          bound.RunID,
			Token:          bound.RunToken,
			BrokerURL:      bound.BrokerURL,
			LockDir:        feed.DefaultLockDir(appdir.StateHome()),
			Out:            &records,
			Log:            &feedLog,
			RequestTimeout: 2 * time.Second,
		}.Run(feedCtx)
	}()

	spawnOverMCP(t, ctx, cs, map[string]any{"dir": t.TempDir(), "prompt": "report", "backend": "poo"})
	child := awaitConversationChild(t, ctx, children)
	h.awaitCondition("the feed to record the child's ask", func() bool {
		return strings.Contains(records.String(), "\n")
	})
	record := firstFeedRecord(t, records.String())
	if record.ID == "" || record.From == "" {
		t.Fatalf("feed record = %+v, want the child's ask", record)
	}

	claimDir, err := askclaim.DefaultDir(appdir.StateHome(), bound.BrokerURL, bound.RunID)
	if err != nil {
		t.Fatalf("resolve the claim directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(claimDir, record.ID+".json")); err != nil {
		t.Fatalf("the feed-delivered ask left no claim under %s: %v", claimDir, err)
	}

	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{Name: "message", Arguments: map[string]any{
		"action": "reply", "reply_to": record.ID, "message": "pong-from-mcp",
	}})
	if err != nil {
		t.Fatalf("message reply: %v", err)
	}
	if res.IsError {
		t.Fatalf("message reply tool error: %s", resultToolText(t, res))
	}
	assertChildAnswered(t, ctx, child, "pong-from-mcp")
}

// TestMCPInboxSharesLockWithFeedBroker pins the mutual exclusion between the two
// consumers of one run's inbox: while the MCP server subscribes, `avenor feed
// broker` exits without consuming, and once the subscription is released the feed
// takes the inbox over and delivers.
func TestMCPInboxSharesLockWithFeedBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	children := make(chan *conversationChild, 1)
	h := newItestHarness(t, Config{}, func(runtime.StartOptions, string) (runtime.Provider, error) {
		child := newConversationChild()
		children <- child
		return child, nil
	})
	cl := h.connectClient()
	// The legacy revision subscribes synchronously, so the lock is held by the
	// time Subscribe returns.
	cs, bound, _ := connectMCPWith(t, ctx, mcpserver.Options{ControlClient: cl}, "poo", "ses_poo", connectMCPOptions{
		session: &mcpsdk.ClientSessionOptions{ProtocolVersion: "2025-11-25"},
	})
	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: mcpserver.InboxResourceURI}); err != nil {
		t.Fatalf("subscribe to %s: %v", mcpserver.InboxResourceURI, err)
	}

	var feedLog syncBuffer
	contended := feed.Broker{
		RunID:          bound.RunID,
		Token:          bound.RunToken,
		BrokerURL:      bound.BrokerURL,
		LockDir:        feed.DefaultLockDir(appdir.StateHome()),
		Out:            io.Discard,
		Log:            &feedLog,
		RequestTimeout: 2 * time.Second,
	}
	if code := contended.Run(ctx); code != 0 {
		t.Fatalf("feed broker exit = %d while the MCP inbox was subscribed, want 0: losing the lock is not a failure", code)
	}
	if !strings.Contains(feedLog.String(), "consumer lock is held") {
		t.Fatalf("feed broker log = %q, want the lock-held line", feedLog.String())
	}

	if err := cs.Unsubscribe(ctx, &mcpsdk.UnsubscribeParams{URI: mcpserver.InboxResourceURI}); err != nil {
		t.Fatalf("unsubscribe from %s: %v", mcpserver.InboxResourceURI, err)
	}

	var records syncBuffer
	feedCtx, stopFeed := context.WithCancel(ctx)
	defer stopFeed()
	go func() {
		contended.Out = &records
		contended.Run(feedCtx)
	}()
	spawnOverMCP(t, ctx, cs, map[string]any{"dir": t.TempDir(), "prompt": "report", "backend": "poo"})
	awaitConversationChild(t, ctx, children)
	h.awaitCondition("the released feed to deliver the child's ask", func() bool {
		return strings.Contains(records.String(), "\n")
	})
}
