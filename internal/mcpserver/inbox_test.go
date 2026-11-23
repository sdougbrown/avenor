package mcpserver

import (
	"bytes"
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
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/feed"
	"github.com/sdougbrown/avenor/internal/messaging"
)

// The conversation these tests bind: one run on a fake local broker, with a
// token distinctive enough that its appearance anywhere is a leak.
const (
	inboxTestRunID      = "run_inbox"
	inboxTestToken      = "tok_inbox_secret"
	inboxTestGeneration = "gen-inbox"
	// inboxChildRunID is the stamped sender of every message the fake broker
	// hands the bound run.
	inboxChildRunID = "run_inbox_child"
	// The two protocol revisions the inbox serves: the legacy subscription RPC
	// and the SEP-2575 listen stream.
	inboxLegacyVersion = "2025-11-25"
	inboxModernVersion = "2026-07-28"
)

// inboxRow is one message waiting at the fake broker.
type inboxRow struct {
	ID           string
	From         string
	Text         string
	ExpectsReply bool
}

// inboxBroker is a local broker answering the shared messaging protocol for one
// run. A test queues the rows it wants delivered and may replace the poll
// handler, so every observation a test makes is a channel, a count the broker
// recorded itself, or a log line the consumer wrote — never a sleep used as an
// observation window.
type inboxBroker struct {
	server *httptest.Server

	mu sync.Mutex
	// rows wait for a poll. perPoll caps one response, which is what lets a
	// backpressure test count hand-outs one at a time.
	rows    []inboxRow
	perPoll int
	// wake is closed and replaced whenever rows are queued, so an idle poll
	// blocks like a real long-poll instead of spinning.
	wake chan struct{}
	// polls counts every /poll-control request, and handed signals each row the
	// broker has given away.
	polls  int
	handed chan inboxRow
	// status, when non-zero, makes /poll-control answer with that HTTP status.
	status int
	// onPoll, when set, answers one /poll-control request instead of the queue.
	// The request body is already drained, so it may wait on r.Context().Done()
	// to observe a consumer that stopped polling.
	onPoll func(w http.ResponseWriter, r *http.Request)
}

// newInboxBroker starts a fake broker on loopback.
func newInboxBroker(t *testing.T) *inboxBroker {
	t.Helper()
	b := &inboxBroker{wake: make(chan struct{}), handed: make(chan inboxRow, 256)}
	b.server = httptest.NewServer(b)
	t.Cleanup(b.server.Close)
	return b
}

// URL is the broker URL the bind resolves to.
func (b *inboxBroker) URL() string { return b.server.URL }

// queue makes rows available to the next poll. One call is one future batch,
// so a test that expects two messages in one read queues them together.
func (b *inboxBroker) queue(rows ...inboxRow) {
	b.mu.Lock()
	b.rows = append(b.rows, rows...)
	close(b.wake)
	b.wake = make(chan struct{})
	b.mu.Unlock()
}

// pollCount reports how many /poll-control requests the broker has served.
func (b *inboxBroker) pollCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.polls
}

// refuse makes /poll-control answer with an HTTP status instead of rows.
func (b *inboxBroker) refuse(status int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status = status
}

// setOnPoll replaces the poll handler for tests that must observe the request
// itself, such as one that waits for the consumer to stop polling.
func (b *inboxBroker) setOnPoll(hook func(w http.ResponseWriter, r *http.Request)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onPoll = hook
}

func (b *inboxBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/sessions" {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"messaging_protocol":%d,"generation":%q,"sessions":[]}`, messaging.ProtocolVersion, inboxTestGeneration)
		return
	}
	if r.URL.Path != "/poll-control" {
		http.NotFound(w, r)
		return
	}
	// The request body must reach EOF before the handler can observe a client
	// that disconnected mid-poll.
	_, _ = io.Copy(io.Discard, r.Body)
	b.mu.Lock()
	b.polls++
	hook, status := b.onPoll, b.status
	b.mu.Unlock()
	if hook != nil {
		hook(w, r)
		return
	}
	if status != 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `{"error":"refused"}`)
		return
	}
	b.serveQueued(w, r)
}

// serveQueued long-polls: it answers with the queued rows, or blocks until one
// is queued or the consumer goes away.
func (b *inboxBroker) serveQueued(w http.ResponseWriter, r *http.Request) {
	for {
		b.mu.Lock()
		if len(b.rows) > 0 {
			count := len(b.rows)
			if b.perPoll > 0 && count > b.perPoll {
				count = b.perPoll
			}
			batch := append([]inboxRow(nil), b.rows[:count]...)
			b.rows = b.rows[count:]
			b.mu.Unlock()
			for _, row := range batch {
				b.handed <- row
			}
			writeInboxRows(w, batch)
			return
		}
		wake := b.wake
		b.mu.Unlock()
		select {
		case <-wake:
		case <-r.Context().Done():
			return
		}
	}
}

// writeInboxRows encodes rows as the poll response the local transport decodes.
func writeInboxRows(w http.ResponseWriter, rows []inboxRow) {
	entries := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, map[string]any{
			"id":          row.ID,
			"type":        "agent_message",
			"from_run_id": row.From,
			"sender":      row.From,
			"payload": map[string]any{
				"id":            row.ID,
				"from_run_id":   row.From,
				"message":       row.Text,
				"expects_reply": row.ExpectsReply,
			},
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entries)
}

// syncBuffer collects the inbox worker's diagnostics, so a test can read them
// while the worker is still running.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// awaitContains waits for a diagnostic line to appear. It is a positive barrier:
// it can only pass once the line was written.
func (b *syncBuffer) awaitContains(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if strings.Contains(b.String(), want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("diagnostics never contained %q: %s", want, b.String())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// bindInboxServer declares one host conversation on a server and runs the
// launch's one bind against brokerURL, so the server serves that run's inbox.
// The state home is isolated, so the consumer lock and the ask claims belong to
// this test alone.
func bindInboxServer(t *testing.T, brokerURL string) (*Server, *syncBuffer) {
	t.Helper()
	return bindInboxServerIn(t, t.TempDir(), brokerURL)
}

// bindInboxServerIn is bindInboxServer with the state home chosen by the caller,
// for a test that must arrange what the bind resolves before the launch runs.
func bindInboxServerIn(t *testing.T, stateHome, brokerURL string) (*Server, *syncBuffer) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", stateHome)
	cl := &bindingClient{}
	cl.setResult(messaging.MessageBindResult{
		BrokerURL:  brokerURL,
		RunID:      inboxTestRunID,
		RunToken:   inboxTestToken,
		Generation: inboxTestGeneration,
	})
	diag := &syncBuffer{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: cl,
		BindHost:      "poo",
		BindSession:   "ses_inbox",
		Stderr:        diag,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	bindDeclared(t, s)
	return s, diag
}

// boundAsksDir reports the claim directory the launch resolved for the bound
// conversation.
func boundAsksDir(t *testing.T, s *Server) string {
	t.Helper()
	bound, ok := s.hostBind.boundState()
	if !ok {
		t.Fatal("the launch's bind recorded no identity")
	}
	return bound.asksDir
}

// inboxServeWaiter waits for one ServeTransport call exactly once, so a test and
// a cleanup can both ask for its result.
type inboxServeWaiter struct {
	t    *testing.T
	err  chan error
	once sync.Once
	last error
}

func (w *inboxServeWaiter) wait() error {
	w.once.Do(func() {
		select {
		case w.last = <-w.err:
		case <-time.After(10 * time.Second):
			w.t.Error("ServeTransport did not return")
		}
	})
	return w.last
}

// serveInboxClient serves one bound server over an in-memory MCP transport and
// connects a client pinned to the given protocol version. The client is closed
// by cleanup; a test that ends the session itself closes it first.
func serveInboxClient(t *testing.T, ctx context.Context, s *Server, clientOpts *mcpsdk.ClientOptions, version string) (*mcpsdk.ClientSession, *inboxServeWaiter) {
	t.Helper()
	if clientOpts == nil {
		clientOpts = &mcpsdk.ClientOptions{}
	}
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serveCtx, cancel := context.WithCancel(ctx)
	waiter := &inboxServeWaiter{t: t, err: make(chan error, 1)}
	go func() { waiter.err <- s.ServeTransport(serveCtx, serverTransport) }()
	t.Cleanup(func() {
		cancel()
		waiter.wait()
	})
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "inbox-test", Version: "1"}, clientOpts)
	cs, err := client.Connect(serveCtx, clientTransport, &mcpsdk.ClientSessionOptions{ProtocolVersion: version})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, waiter
}

// awaitInboxUpdated waits for one resource-updated notification naming the
// inbox. Notifications arrive before the read that drains them, so the wait is
// a positive barrier rather than a probe.
func awaitInboxUpdated(t *testing.T, updates <-chan struct{}) {
	t.Helper()
	select {
	case <-updates:
	case <-time.After(10 * time.Second):
		t.Fatal("the client was never told the inbox changed")
	}
}

// inboxUpdates returns the channel a test waits on for each inbox wake, and the
// handler that fills it.
func inboxUpdates() (chan struct{}, func(context.Context, *mcpsdk.ResourceUpdatedNotificationRequest)) {
	return inboxUpdatesOf(8)
}

// inboxUpdatesOf is inboxUpdates with room for a test that counts every wake.
func inboxUpdatesOf(capacity int) (chan struct{}, func(context.Context, *mcpsdk.ResourceUpdatedNotificationRequest)) {
	updates := make(chan struct{}, capacity)
	handler := func(_ context.Context, req *mcpsdk.ResourceUpdatedNotificationRequest) {
		if req.Params == nil || req.Params.URI != InboxResourceURI {
			return
		}
		select {
		case updates <- struct{}{}:
		default:
		}
	}
	return updates, handler
}

// readInbox drains the inbox and decodes the messages it reports.
func readInbox(t *testing.T, ctx context.Context, cs *mcpsdk.ClientSession) []inboxMessage {
	t.Helper()
	res, err := cs.ReadResource(ctx, &mcpsdk.ReadResourceParams{URI: InboxResourceURI})
	if err != nil {
		t.Fatalf("read avenor://inbox: %v", err)
	}
	if len(res.Contents) != 1 {
		t.Fatalf("read returned %d contents, want 1", len(res.Contents))
	}
	content := res.Contents[0]
	if content.URI != InboxResourceURI || content.MIMEType != InboxMIMEType {
		t.Fatalf("read content = (%q, %q), want the inbox resource and its MIME type", content.URI, content.MIMEType)
	}
	var payload struct {
		Messages *[]inboxMessage `json:"messages"`
	}
	if err := json.Unmarshal([]byte(content.Text), &payload); err != nil {
		t.Fatalf("decode the read payload: %v (%q)", err, content.Text)
	}
	if payload.Messages == nil {
		t.Fatalf("read payload %q carries no messages array", content.Text)
	}
	return *payload.Messages
}

// awaitInboxMessages reads until the inbox reports something, and returns what it
// took. It is the wait for a message a poll has already handed over but the
// worker has not queued yet; the caller states what it expects to find.
func awaitInboxMessages(t *testing.T, ctx context.Context, cs *mcpsdk.ClientSession, what string) []inboxMessage {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		messages := readInbox(t, ctx, cs)
		if len(messages) > 0 {
			return messages
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never reached the inbox", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// inboxLockFree reports whether the inbox consumer lock for the bound run is
// free right now. It never waits: a released lock is the state a caller can act
// on immediately.
func inboxLockFree(t *testing.T, brokerURL string) bool {
	t.Helper()
	unlock, held, err := feed.AcquireLock(feed.DefaultLockDir(appdir.StateHome()), brokerURL, inboxTestRunID)
	if err != nil {
		t.Fatalf("take the inbox consumer lock: %v", err)
	}
	if held {
		return false
	}
	if err := unlock(); err != nil {
		t.Fatalf("release the inbox consumer lock: %v", err)
	}
	return true
}

// TestUnboundServerServesNoInbox pins the preserved default: a server whose
// launch declared no conversation advertises no resources capability, lists no
// resources and answers the inbox URI as not found.
func TestUnboundServerServesNoInbox(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: &bindingClient{}, Stderr: io.Discard})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	cs, _ := serveInboxClient(t, context.Background(), s, nil, inboxLegacyVersion)

	if caps := cs.InitializeResult().Capabilities; caps == nil || caps.Resources != nil {
		t.Fatalf("capabilities = %+v, want no resources capability on an unbound server", caps)
	}
	listed, err := cs.ListResources(context.Background(), nil)
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}
	if len(listed.Resources) != 0 {
		t.Fatalf("an unbound server listed %d resources, want none", len(listed.Resources))
	}
	if _, err := cs.ReadResource(context.Background(), &mcpsdk.ReadResourceParams{URI: InboxResourceURI}); err == nil || !strings.Contains(err.Error(), "Resource not found") {
		t.Fatalf("read avenor://inbox on an unbound server = %v, want resource-not-found", err)
	}
	if err := cs.Subscribe(context.Background(), &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err == nil {
		t.Fatal("an unbound server accepted an inbox subscription")
	}
}

// TestInboxLegacyReadDrainsWhatWasQueuedBeforeSubscribe pins the documented
// initial-drain contract: a message the broker queued before anyone subscribed is
// delivered by the first read, because the worker polls as soon as it starts.
func TestInboxLegacyReadDrainsWhatWasQueuedBeforeSubscribe(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	broker.queue(inboxRow{ID: "msg_before", From: inboxChildRunID, Text: "queued first"})
	s, _ := bindInboxServer(t, broker.URL())
	updates, handler := inboxUpdates()
	cs, _ := serveInboxClient(t, ctx, s, &mcpsdk.ClientOptions{ResourceUpdatedHandler: handler}, inboxLegacyVersion)

	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	awaitInboxUpdated(t, updates)
	messages := readInbox(t, ctx, cs)
	if len(messages) != 1 || messages[0].ID != "msg_before" || messages[0].Text != "queued first" {
		t.Fatalf("first read = %+v, want the message queued before the subscribe", messages)
	}
}

// TestInboxLegacySubscribeDeliversAndDrains drives one real subscription over the
// legacy protocol: a tell and an ask queued after the subscribe wake the client,
// one read reports both with the raw text and the ask bits, the second read is
// empty, and the ask's claim is recorded under the bound conversation's own
// directory so a reply from this surface can claim it.
func TestInboxLegacySubscribeDeliversAndDrains(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	s, diag := bindInboxServer(t, broker.URL())
	updates, handler := inboxUpdates()
	cs, _ := serveInboxClient(t, ctx, s, &mcpsdk.ClientOptions{ResourceUpdatedHandler: handler}, inboxLegacyVersion)

	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	broker.queue(
		inboxRow{ID: "msg_tell", From: inboxChildRunID, Text: "a tell"},
		inboxRow{ID: "msg_ask", From: inboxChildRunID, Text: "an ask", ExpectsReply: true},
	)
	awaitInboxUpdated(t, updates)

	messages := readInbox(t, ctx, cs)
	if len(messages) != 2 {
		t.Fatalf("read returned %d messages, want the tell and the ask: %+v", len(messages), messages)
	}
	tell, ask := messages[0], messages[1]
	if tell.ID != "msg_tell" || tell.From != inboxChildRunID || tell.Text != "a tell" || tell.ExpectsReply || !tell.Answerable {
		t.Fatalf("tell = %+v, want the raw tell from %s", tell, inboxChildRunID)
	}
	if ask.ID != "msg_ask" || ask.Text != "an ask" || !ask.ExpectsReply || !ask.Answerable {
		t.Fatalf("ask = %+v, want an answerable ask carrying its raw text", ask)
	}
	if rest := readInbox(t, ctx, cs); len(rest) != 0 {
		t.Fatalf("second read = %+v, want an empty inbox: a read is the only way a message leaves", rest)
	}

	claim := filepath.Join(boundAsksDir(t, s), "msg_ask"+".json")
	if _, err := os.Stat(claim); err != nil {
		t.Fatalf("the delivered ask left no claim at %s: %v", claim, err)
	}
	if strings.Contains(diag.String(), inboxTestToken) {
		t.Fatalf("diagnostics leaked the run token: %s", diag.String())
	}
}

// TestInboxLegacyRepeatSubscribeKeepsDelivering pins idempotence: the legacy
// protocol lets a client subscribe twice, and the second call must neither fail
// nor take the inbox away from the first.
func TestInboxLegacyRepeatSubscribeKeepsDelivering(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	s, _ := bindInboxServer(t, broker.URL())
	updates, handler := inboxUpdates()
	cs, _ := serveInboxClient(t, ctx, s, &mcpsdk.ClientOptions{ResourceUpdatedHandler: handler}, inboxLegacyVersion)

	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("repeat subscribe: %v, want idempotent success", err)
	}
	broker.queue(inboxRow{ID: "msg_after", From: inboxChildRunID, Text: "still delivered"})
	awaitInboxUpdated(t, updates)
	messages := readInbox(t, ctx, cs)
	if len(messages) != 1 || messages[0].ID != "msg_after" {
		t.Fatalf("read after a repeat subscribe = %+v, want the message queued after it", messages)
	}
}

// TestInboxUnsubscribeReleasesTheLockImmediately pins the teardown order: the
// unsubscribe returns only once the worker has released the consumer lock, so a
// second consumer can take it without polling for the release.
func TestInboxUnsubscribeReleasesTheLockImmediately(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	entered := make(chan struct{}, 4)
	broker.setOnPoll(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	})
	s, _ := bindInboxServer(t, broker.URL())
	cs, _ := serveInboxClient(t, ctx, s, nil, inboxLegacyVersion)

	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the inbox worker never polled the broker")
	}
	if inboxLockFree(t, broker.URL()) {
		t.Fatal("the consumer lock was free while the worker was polling")
	}
	if err := cs.Unsubscribe(ctx, &mcpsdk.UnsubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	if !inboxLockFree(t, broker.URL()) {
		t.Fatal("the inbox consumer lock outlived the unsubscribe that stopped the worker")
	}
}

// TestInboxEOFReleasesTheLock pins the disconnect path with no unsubscribe at
// all: a client that goes away takes its poller with it, in-flight long-poll
// included, so the lock is free the moment the served transport returns.
func TestInboxEOFReleasesTheLock(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	entered := make(chan struct{}, 4)
	broker.setOnPoll(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	})
	s, _ := bindInboxServer(t, broker.URL())
	cs, serve := serveInboxClient(t, ctx, s, nil, inboxLegacyVersion)

	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the inbox worker never polled the broker")
	}
	// Closing the session ends the served transport without unsubscribing.
	if err := cs.Close(); err != nil {
		t.Fatalf("close the client: %v", err)
	}
	serve.wait()
	if !inboxLockFree(t, broker.URL()) {
		t.Fatal("a disconnected client left its inbox consumer lock behind")
	}
}

// rawInboxClient speaks newline-delimited JSON-RPC over a pair of pipes. The SDK
// client runs subscriptions/listen in the background and returns only when the
// stream ends, so a test that must see the acknowledgement, a refusal and the
// stream's own result frame has to drive the protocol itself.
type rawInboxClient struct {
	t  *testing.T
	to *io.PipeWriter

	frames chan map[string]any
	mu     sync.Mutex
	seen   []map[string]any
}

// newRawInboxClient serves s over raw pipes and starts reading its frames.
func newRawInboxClient(t *testing.T, ctx context.Context, s *Server) (*rawInboxClient, *inboxServeWaiter) {
	t.Helper()
	clientToServer, fromClient := io.Pipe()
	toClient, serverToClient := io.Pipe()
	transport := &mcpsdk.IOTransport{Reader: clientToServer, Writer: serverToClient}
	serveCtx, cancel := context.WithCancel(ctx)
	waiter := &inboxServeWaiter{t: t, err: make(chan error, 1)}
	go func() { waiter.err <- s.ServeTransport(serveCtx, transport) }()
	c := &rawInboxClient{t: t, to: fromClient, frames: make(chan map[string]any, 64)}
	go func() {
		defer close(c.frames)
		decoder := json.NewDecoder(toClient)
		for {
			var frame map[string]any
			if err := decoder.Decode(&frame); err != nil {
				return
			}
			c.frames <- frame
		}
	}()
	t.Cleanup(func() {
		_ = fromClient.Close()
		cancel()
		waiter.wait()
	})
	return c, waiter
}

// send writes one frame.
func (c *rawInboxClient) send(frame map[string]any) {
	c.t.Helper()
	frame["jsonrpc"] = "2.0"
	encoded, err := json.Marshal(frame)
	if err != nil {
		c.t.Fatalf("encode frame %+v: %v", frame, err)
	}
	if _, err := c.to.Write(append(encoded, '\n')); err != nil {
		c.t.Fatalf("write frame %+v: %v", frame, err)
	}
}

// rawRequestMeta is the SEP-2575 per-request `_meta` triple every call carries.
func rawRequestMeta() map[string]any {
	return map[string]any{
		"io.modelcontextprotocol/protocolVersion":    inboxModernVersion,
		"io.modelcontextprotocol/clientInfo":         map[string]any{"name": "raw-test", "version": "1"},
		"io.modelcontextprotocol/clientCapabilities": map[string]any{},
	}
}

// call sends one request carrying the per-request metadata.
func (c *rawInboxClient) call(id int, method string, params map[string]any) {
	c.t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = rawRequestMeta()
	c.send(map[string]any{"id": id, "method": method, "params": params})
}

// notify sends one notification.
func (c *rawInboxClient) notify(method string, params map[string]any) {
	c.t.Helper()
	c.send(map[string]any{"method": method, "params": params})
}

// await returns the next frame matching match, remembering every frame it read.
func (c *rawInboxClient) await(what string, match func(map[string]any) bool) map[string]any {
	c.t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case frame, ok := <-c.frames:
			if !ok {
				c.t.Fatalf("the server closed the stream while waiting for %s", what)
			}
			c.mu.Lock()
			c.seen = append(c.seen, frame)
			c.mu.Unlock()
			if match(frame) {
				return frame
			}
		case <-deadline:
			c.mu.Lock()
			seen := append([]map[string]any(nil), c.seen...)
			c.mu.Unlock()
			c.t.Fatalf("timed out waiting for %s; frames seen: %#v", what, seen)
		}
	}
}

// frames copies every frame read so far, so a test can assert that some shape
// never appeared once a later positive frame has arrived.
func (c *rawInboxClient) framesSeen() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.seen...)
}

// frameWithID matches a response to one request.
func frameWithID(id float64) func(map[string]any) bool {
	return func(frame map[string]any) bool {
		responseID, ok := frame["id"].(float64)
		return ok && responseID == id
	}
}

// notification matches one notification method.
func notification(method string) func(map[string]any) bool {
	return func(frame map[string]any) bool {
		return frame["method"] == method
	}
}

// subscriptionID reads the listen request id a modern notification carries.
func subscriptionID(frame map[string]any) float64 {
	params, _ := frame["params"].(map[string]any)
	meta, _ := params["_meta"].(map[string]any)
	id, _ := meta["io.modelcontextprotocol/subscriptionId"].(float64)
	return id
}

// rawResult returns the result object of a response frame.
func rawResult(t *testing.T, frame map[string]any) map[string]any {
	t.Helper()
	result, ok := frame["result"].(map[string]any)
	if !ok {
		t.Fatalf("frame %#v carries no result", frame)
	}
	return result
}

// rawAcknowledged asserts one acknowledgement frame agrees to the subscription
// the test asked for.
func rawAcknowledged(t *testing.T, frame map[string]any, id float64) {
	t.Helper()
	if got := subscriptionID(frame); got != id {
		t.Fatalf("acknowledged subscriptionId = %v, want %v", got, id)
	}
	params, _ := frame["params"].(map[string]any)
	notifications, _ := params["notifications"].(map[string]any)
	uris, _ := notifications["resourceSubscriptions"].([]any)
	if len(uris) != 1 || uris[0] != InboxResourceURI {
		t.Fatalf("acknowledged resourceSubscriptions = %v, want [%s]", uris, InboxResourceURI)
	}
}

// beginModernListen discovers the server and opens one listen stream, returning
// once the server has acknowledged it.
func beginModernListen(t *testing.T, c *rawInboxClient, id int) {
	t.Helper()
	c.call(1, "server/discover", nil)
	discovered := rawResult(t, c.await("server/discover", frameWithID(1)))
	capabilities, _ := discovered["capabilities"].(map[string]any)
	resources, _ := capabilities["resources"].(map[string]any)
	if subscribe, _ := resources["subscribe"].(bool); !subscribe {
		t.Fatalf("server/discover capabilities = %#v, want resources.subscribe", capabilities)
	}
	c.call(id, "subscriptions/listen", map[string]any{
		"notifications": map[string]any{"resourceSubscriptions": []string{InboxResourceURI}},
	})
	ack := c.await("subscriptions/acknowledged", notification("notifications/subscriptions/acknowledged"))
	rawAcknowledged(t, ack, float64(id))
}

// TestInboxModernListenAcknowledgesAndDelivers drives the SEP-2575 path over the
// wire: discover advertises the subscription capability, listen is acknowledged
// with the subscription id, a broker message wakes the stream, and the read
// reports the drained messages with the uncacheable cache hint the resource sets.
func TestInboxModernListenAcknowledgesAndDelivers(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	s, _ := bindInboxServer(t, broker.URL())
	c, _ := newRawInboxClient(t, ctx, s)

	beginModernListen(t, c, 10)
	broker.queue(
		inboxRow{ID: "msg_tell", From: inboxChildRunID, Text: "a tell"},
		inboxRow{ID: "msg_ask", From: inboxChildRunID, Text: "an ask", ExpectsReply: true},
	)
	updated := c.await("notifications/resources/updated", notification("notifications/resources/updated"))
	if got := subscriptionID(updated); got != 10 {
		t.Fatalf("resources/updated subscriptionId = %v, want the listen that asked for it", got)
	}
	params, _ := updated["params"].(map[string]any)
	if params["uri"] != InboxResourceURI {
		t.Fatalf("resources/updated uri = %v, want %s", params["uri"], InboxResourceURI)
	}

	c.call(20, "resources/read", map[string]any{"uri": InboxResourceURI})
	read := rawResult(t, c.await("resources/read", frameWithID(20)))
	if ttl, ok := read["ttlMs"].(float64); !ok || ttl != 0 {
		t.Fatalf("read ttlMs = %v, want 0: a drained queue is never cacheable", read["ttlMs"])
	}
	if read["cacheScope"] != "private" {
		t.Fatalf("read cacheScope = %v, want private", read["cacheScope"])
	}
	contents, _ := read["contents"].([]any)
	if len(contents) != 1 {
		t.Fatalf("read returned %d contents, want 1", len(contents))
	}
	content, _ := contents[0].(map[string]any)
	var payload struct {
		Messages []inboxMessage `json:"messages"`
	}
	text, _ := content["text"].(string)
	if strings.Contains(text, inboxTestToken) {
		t.Fatalf("the read frame leaked the run token: %q", text)
	}
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		t.Fatalf("decode the read payload: %v (%q)", err, text)
	}
	if len(payload.Messages) != 2 || payload.Messages[0].ID != "msg_tell" || payload.Messages[1].ID != "msg_ask" {
		t.Fatalf("read messages = %#v, want the tell and the ask", payload.Messages)
	}
	if !payload.Messages[1].ExpectsReply || !payload.Messages[1].Answerable {
		t.Fatalf("ask = %+v, want an answerable ask", payload.Messages[1])
	}
}

// TestInboxModernRefusesOverlappingListen pins the per-session exclusivity the
// SDK cannot enforce: a second listen on the same session for the same resource
// is refused with an error frame and no acknowledgement, and the first listen
// keeps receiving.
func TestInboxModernRefusesOverlappingListen(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	s, _ := bindInboxServer(t, broker.URL())
	c, _ := newRawInboxClient(t, ctx, s)

	beginModernListen(t, c, 10)
	c.call(11, "subscriptions/listen", map[string]any{
		"notifications": map[string]any{"resourceSubscriptions": []string{InboxResourceURI}},
	})
	refused := c.await("the refusal of the overlapping listen", frameWithID(11))
	if _, isError := refused["error"]; !isError {
		t.Fatalf("overlapping listen = %#v, want an error frame", refused)
	}

	// The first listen is untouched: a message queued now still wakes it.
	broker.queue(inboxRow{ID: "msg_after", From: inboxChildRunID, Text: "still listened"})
	updated := c.await("resources/updated for the first listen", notification("notifications/resources/updated"))
	if got := subscriptionID(updated); got != 10 {
		t.Fatalf("resources/updated subscriptionId = %v, want the listen that survived", got)
	}
	for _, frame := range c.framesSeen() {
		if frame["method"] != "notifications/subscriptions/acknowledged" {
			continue
		}
		if subscriptionID(frame) == 11 {
			t.Fatalf("the refused listen was acknowledged: %#v", frame)
		}
	}
}

// TestInboxModernCancellationReleasesTheLock pins the modern teardown: cancelling
// the listen returns its result frame even though the handler's context is
// already done, and the worker has released the consumer lock by then.
func TestInboxModernCancellationReleasesTheLock(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	s, _ := bindInboxServer(t, broker.URL())
	c, _ := newRawInboxClient(t, ctx, s)

	beginModernListen(t, c, 10)
	if inboxLockFree(t, broker.URL()) {
		t.Fatal("the consumer lock was free while the listen was active")
	}
	c.notify("notifications/cancelled", map[string]any{"requestId": 10})
	result := c.await("the cancelled listen's result", frameWithID(10))
	if _, isError := result["error"]; isError {
		t.Fatalf("cancelled listen = %#v, want a result frame", result)
	}
	if !inboxLockFree(t, broker.URL()) {
		t.Fatal("a cancelled listen left its inbox consumer lock behind")
	}
}

// TestInboxModernRefusesAHeldLock pins the shared exclusivity on the modern path:
// a lock another consumer holds is a refusal, delivered as an error frame with no
// acknowledgement and no poll.
func TestInboxModernRefusesAHeldLock(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	s, _ := bindInboxServer(t, broker.URL())
	c, _ := newRawInboxClient(t, ctx, s)

	unlock, held, err := feed.AcquireLock(feed.DefaultLockDir(appdir.StateHome()), broker.URL(), inboxTestRunID)
	if err != nil || held {
		t.Fatalf("pre-acquire the consumer lock: held = %v, err = %v", held, err)
	}
	defer func() { _ = unlock() }()

	c.call(1, "server/discover", nil)
	c.await("server/discover", frameWithID(1))
	c.call(10, "subscriptions/listen", map[string]any{
		"notifications": map[string]any{"resourceSubscriptions": []string{InboxResourceURI}},
	})
	refused := c.await("the lock-held refusal", frameWithID(10))
	errorObject, _ := refused["error"].(map[string]any)
	message, _ := errorObject["message"].(string)
	if !strings.Contains(message, "consumer lock is held") {
		t.Fatalf("listen on a held lock = %#v, want the lock-held refusal", refused)
	}
	if strings.Contains(message, inboxTestToken) {
		t.Fatalf("the lock-held refusal leaked the run token: %q", message)
	}
	for _, frame := range c.framesSeen() {
		if frame["method"] == "notifications/subscriptions/acknowledged" {
			t.Fatalf("a refused listen was acknowledged: %#v", frame)
		}
	}
	if got := broker.pollCount(); got != 0 {
		t.Fatalf("a refused listen polled the broker %d times, want none", got)
	}
}

// TestInboxModernEOFReleasesTheLock pins the modern disconnect path: a client
// that closes the stream without cancelling still takes its poller with it.
func TestInboxModernEOFReleasesTheLock(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	entered := make(chan struct{}, 4)
	broker.setOnPoll(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	})
	s, _ := bindInboxServer(t, broker.URL())
	c, serve := newRawInboxClient(t, ctx, s)

	beginModernListen(t, c, 10)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the inbox worker never polled the broker")
	}
	if err := c.to.Close(); err != nil {
		t.Fatalf("close the client stream: %v", err)
	}
	serve.wait()
	if !inboxLockFree(t, broker.URL()) {
		t.Fatal("a disconnected listen left its inbox consumer lock behind")
	}
}

// TestInboxSubscribeRefusesAHeldLock pins the shared lock from the legacy side: a
// lock another consumer holds refuses the subscribe before any poll is issued,
// and the same subscribe succeeds once that consumer lets go.
func TestInboxSubscribeRefusesAHeldLock(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	s, _ := bindInboxServer(t, broker.URL())
	cs, _ := serveInboxClient(t, ctx, s, nil, inboxLegacyVersion)

	unlock, held, err := feed.AcquireLock(feed.DefaultLockDir(appdir.StateHome()), broker.URL(), inboxTestRunID)
	if err != nil || held {
		t.Fatalf("pre-acquire the consumer lock: held = %v, err = %v", held, err)
	}
	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err == nil || !strings.Contains(err.Error(), "consumer lock is held") {
		t.Fatalf("subscribe on a held lock = %v, want the lock-held refusal", err)
	}
	if got := broker.pollCount(); got != 0 {
		t.Fatalf("a refused subscribe polled the broker %d times, want none", got)
	}
	if err := unlock(); err != nil {
		t.Fatalf("release the consumer lock: %v", err)
	}
	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe after the lock was released: %v", err)
	}
}

// TestInboxHoldsTheLockAgainstTheFeedBroker pins the mutual exclusion the lock
// exists for: while the inbox is subscribed, `avenor feed broker` for the same run
// exits without consuming anything.
func TestInboxHoldsTheLockAgainstTheFeedBroker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	broker := newInboxBroker(t)
	entered := make(chan struct{}, 4)
	broker.setOnPoll(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	})
	s, _ := bindInboxServer(t, broker.URL())
	cs, _ := serveInboxClient(t, ctx, s, nil, inboxLegacyVersion)

	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the inbox worker never polled the broker")
	}
	polls := broker.pollCount()

	var log bytes.Buffer
	code := make(chan int, 1)
	go func() {
		code <- feed.Broker{
			RunID:     inboxTestRunID,
			Token:     inboxTestToken,
			BrokerURL: broker.URL(),
			LockDir:   feed.DefaultLockDir(appdir.StateHome()),
			Out:       io.Discard,
			Log:       &log,
		}.Run(ctx)
	}()
	select {
	case got := <-code:
		if got != 0 {
			t.Fatalf("feed.Broker.Run exit = %d, want 0: losing the lock is not a failure", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("feed.Broker.Run waited on a lock the inbox holds")
	}
	if !strings.Contains(log.String(), "consumer lock is held") {
		t.Fatalf("feed.Broker.Run log = %q, want the lock-held line", log.String())
	}
	if got := broker.pollCount(); got != polls {
		t.Fatalf("the feed broker polled the inbox %d times, want none: it holds no lock", got-polls)
	}
}

// TestInboxConcurrentStopAndRestart pins the retirement boundary: a stop racing a
// resubscribe, and a stop racing a Close, both settle without deadlock and without
// a false lock contention against the retiring worker, and the lock is free once
// the inbox is finally stopped.
func TestInboxConcurrentStopAndRestart(t *testing.T) {
	ctx := context.Background()

	t.Run("unsubscribe and resubscribe", func(t *testing.T) {
		broker := newInboxBroker(t)
		entered := make(chan struct{}, 8)
		broker.setOnPoll(func(_ http.ResponseWriter, r *http.Request) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-r.Context().Done()
		})
		s, _ := bindInboxServer(t, broker.URL())
		cs, _ := serveInboxClient(t, ctx, s, nil, inboxLegacyVersion)

		if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("the inbox worker never polled the broker")
		}

		var wg sync.WaitGroup
		errors := make([]error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			errors[0] = cs.Unsubscribe(ctx, &mcpsdk.UnsubscribeParams{URI: InboxResourceURI})
		}()
		go func() {
			defer wg.Done()
			errors[1] = cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI})
		}()
		finished := make(chan struct{})
		go func() { wg.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Fatal("a stop racing a resubscribe deadlocked")
		}
		for i, err := range errors {
			if err != nil {
				t.Fatalf("concurrent call %d = %v, want no failure: the retiring worker must not look like a held lock", i, err)
			}
		}
		if err := cs.Unsubscribe(ctx, &mcpsdk.UnsubscribeParams{URI: InboxResourceURI}); err != nil {
			t.Fatalf("final unsubscribe: %v", err)
		}
		if !inboxLockFree(t, broker.URL()) {
			t.Fatal("the inbox consumer lock outlived the final stop")
		}
	})

	t.Run("unsubscribe and close", func(t *testing.T) {
		broker := newInboxBroker(t)
		entered := make(chan struct{}, 8)
		broker.setOnPoll(func(_ http.ResponseWriter, r *http.Request) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-r.Context().Done()
		})
		s, _ := bindInboxServer(t, broker.URL())
		cs, _ := serveInboxClient(t, ctx, s, nil, inboxLegacyVersion)

		if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("the inbox worker never polled the broker")
		}

		var wg sync.WaitGroup
		var unsubscribeErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			unsubscribeErr = cs.Unsubscribe(ctx, &mcpsdk.UnsubscribeParams{URI: InboxResourceURI})
		}()
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
		finished := make(chan struct{})
		go func() { wg.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Fatal("a stop racing a Close deadlocked")
		}
		// Close may end the connection before the unsubscribe is written, which
		// is a connection error and not a lock contention.
		if unsubscribeErr != nil && !errors.Is(unsubscribeErr, mcpsdk.ErrConnectionClosed) {
			t.Fatalf("unsubscribe racing Close = %v, want no failure other than the closed connection", unsubscribeErr)
		}
		if !inboxLockFree(t, broker.URL()) {
			t.Fatal("the inbox consumer lock outlived a Close that raced an unsubscribe")
		}
	})
}

// TestInboxBackpressurePausesThePollThenWakes pins the queue bound: once the
// inbox holds maxInboxMessages the worker stops polling, so the broker keeps the
// excess, and the next read wakes it to deliver the rest.
func TestInboxBackpressurePausesThePollThenWakes(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	broker.perPoll = 1
	rows := make([]inboxRow, 0, maxInboxMessages+1)
	for i := 0; i < maxInboxMessages+1; i++ {
		rows = append(rows, inboxRow{ID: fmt.Sprintf("msg_%03d", i), From: inboxChildRunID, Text: fmt.Sprintf("body %03d", i)})
	}
	broker.queue(rows...)
	s, _ := bindInboxServer(t, broker.URL())
	updates, handler := inboxUpdatesOf(maxInboxMessages + 8)
	cs, _ := serveInboxClient(t, ctx, s, &mcpsdk.ClientOptions{ResourceUpdatedHandler: handler}, inboxLegacyVersion)

	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// The broker hands the queue exactly as many messages as the inbox holds, and
	// each delivery wakes the client after the message is queued, so the last wake
	// is the point at which the queue is full.
	for i := 0; i < maxInboxMessages; i++ {
		select {
		case <-broker.handed:
		case <-time.After(10 * time.Second):
			t.Fatalf("the broker handed out only %d of %d messages", i, maxInboxMessages)
		}
		awaitInboxUpdated(t, updates)
	}
	if got := broker.pollCount(); got != maxInboxMessages {
		t.Fatalf("polls before the read = %d, want one per message", got)
	}
	// A full queue stops the poll, so the excess stays at the broker.
	select {
	case <-broker.handed:
		t.Fatal("the worker polled past a full inbox: backpressure never reached the broker")
	case <-time.After(250 * time.Millisecond):
	}

	messages := readInbox(t, ctx, cs)
	if len(messages) != maxInboxMessages {
		t.Fatalf("the read drained %d messages, want the %d the queue held", len(messages), maxInboxMessages)
	}
	if messages[0].ID != "msg_000" || messages[len(messages)-1].ID != fmt.Sprintf("msg_%03d", maxInboxMessages-1) {
		t.Fatalf("drained messages run %s..%s, want arrival order", messages[0].ID, messages[len(messages)-1].ID)
	}
	// The read is what woke the poll: the message the broker held back is handed
	// over, and the worker queues it on the next loop.
	select {
	case row := <-broker.handed:
		if want := fmt.Sprintf("msg_%03d", maxInboxMessages); row.ID != want {
			t.Fatalf("the poll after the read handed out %s, want %s", row.ID, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the read did not wake the paused poll")
	}
	rest := awaitInboxMessages(t, ctx, cs, "the message the broker held back")
	if len(rest) != 1 || rest[0].ID != fmt.Sprintf("msg_%03d", maxInboxMessages) {
		t.Fatalf("the read after the wake returned %+v, want the message held back at the broker", rest)
	}
}

// TestInboxAskWithoutASenderIsUnanswerable pins the verdict a delivered ask
// carries when no reply can be routed: the ask bit stays set, the answer bit is
// cleared, and the text a reader sees is the sender's own.
func TestInboxAskWithoutASenderIsUnanswerable(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	broker.queue(inboxRow{ID: "msg_stray", Text: "no sender", ExpectsReply: true})
	s, _ := bindInboxServer(t, broker.URL())
	updates, handler := inboxUpdates()
	cs, _ := serveInboxClient(t, ctx, s, &mcpsdk.ClientOptions{ResourceUpdatedHandler: handler}, inboxLegacyVersion)

	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	awaitInboxUpdated(t, updates)
	messages := readInbox(t, ctx, cs)
	if len(messages) != 1 {
		t.Fatalf("read returned %+v, want the stray ask", messages)
	}
	ask := messages[0]
	if !ask.ExpectsReply || ask.Answerable {
		t.Fatalf("stray ask = %+v, want expects_reply true and answerable false", ask)
	}
	if ask.Text != "no sender" || ask.From != "unknown" {
		t.Fatalf("stray ask = %+v, want the raw text and no named sender", ask)
	}
	if _, err := os.Stat(filepath.Join(boundAsksDir(t, s), "msg_stray"+".json")); err == nil {
		t.Fatal("an unanswerable ask was recorded as a claim")
	}
}

// TestInboxAskWithAnUnwritableClaimDirectoryIsUnanswerable pins the other way a
// reply cannot be routed: the ask names its sender but its claim cannot be
// recorded, so the message is still delivered, reported unanswerable, and the
// failure is diagnosed without the run token.
func TestInboxAskWithAnUnwritableClaimDirectoryIsUnanswerable(t *testing.T) {
	ctx := context.Background()
	stateHome := t.TempDir()
	// A regular file where the claim root belongs makes every record fail.
	if err := os.MkdirAll(filepath.Join(stateHome, "avenor"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateHome, "avenor", "feed-asks"), []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}

	broker := newInboxBroker(t)
	broker.queue(inboxRow{ID: "msg_unwritable", From: inboxChildRunID, Text: "an unrecorded ask", ExpectsReply: true})
	s, diag := bindInboxServerIn(t, stateHome, broker.URL())
	updates, handler := inboxUpdates()
	cs, _ := serveInboxClient(t, ctx, s, &mcpsdk.ClientOptions{ResourceUpdatedHandler: handler}, inboxLegacyVersion)

	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	awaitInboxUpdated(t, updates)
	messages := readInbox(t, ctx, cs)
	if len(messages) != 1 {
		t.Fatalf("read returned %+v, want the ask whose claim failed", messages)
	}
	ask := messages[0]
	if !ask.ExpectsReply || ask.Answerable {
		t.Fatalf("ask with an unwritable claim = %+v, want expects_reply true and answerable false", ask)
	}
	if ask.Text != "an unrecorded ask" || ask.From != inboxChildRunID {
		t.Fatalf("ask = %+v, want the raw text and named sender", ask)
	}
	diag.awaitContains(t, `failed to record ask id "msg_unwritable"`)
	if strings.Contains(diag.String(), inboxTestToken) {
		t.Fatalf("a failed claim leaked the run token: %s", diag.String())
	}
}

// TestInboxTerminalRefusalParksAndCloseExits pins the failure a subscribe cannot
// report: a broker that refuses the run's credentials is diagnosed once, without
// the credential, and the server still closes cleanly and releases the lock.
func TestInboxTerminalRefusalParksAndCloseExits(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	broker.refuse(http.StatusUnauthorized)
	s, diag := bindInboxServer(t, broker.URL())
	cs, _ := serveInboxClient(t, ctx, s, nil, inboxLegacyVersion)

	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: InboxResourceURI}); err != nil {
		t.Fatalf("subscribe: %v, want the worker to start and report the refusal itself", err)
	}
	diag.awaitContains(t, "mcp inbox: terminal broker refusal (HTTP 401)")
	log := diag.String()
	if strings.Contains(log, inboxTestToken) {
		t.Fatalf("the terminal refusal leaked the run token: %s", log)
	}
	if n := strings.Count(log, "terminal broker refusal"); n != 1 {
		t.Fatalf("the terminal refusal was reported %d times, want one line before parking: %s", n, log)
	}

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close after a terminal refusal: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close waited on an inbox worker parked by a terminal refusal")
	}
	if !inboxLockFree(t, broker.URL()) {
		t.Fatal("a parked inbox worker kept its consumer lock through Close")
	}
}

// TestInboxUnknownResource pins the resource boundary: the server serves one URI,
// and every other URI is not found on both the read and the subscribe path.
func TestInboxUnknownResource(t *testing.T) {
	ctx := context.Background()
	broker := newInboxBroker(t)
	s, _ := bindInboxServer(t, broker.URL())
	cs, _ := serveInboxClient(t, ctx, s, nil, inboxLegacyVersion)

	const other = "avenor://other"
	if _, err := cs.ReadResource(ctx, &mcpsdk.ReadResourceParams{URI: other}); err == nil || !strings.Contains(err.Error(), "Resource not found") {
		t.Fatalf("read %s = %v, want resource-not-found", other, err)
	}
	if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: other}); err == nil || !strings.Contains(err.Error(), "Resource not found") {
		t.Fatalf("subscribe %s = %v, want resource-not-found", other, err)
	}
	if got := broker.pollCount(); got != 0 {
		t.Fatalf("a refused URI started %d polls, want none", got)
	}
}
