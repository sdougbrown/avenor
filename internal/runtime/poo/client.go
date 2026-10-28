package poo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/launcher"
	"github.com/sdougbrown/poo/wire/protocol"
)

// The bounds the client enforces on the process it owns.
const (
	// closeAnswerTimeout is how long session/close may take to answer. The
	// server's own close path budgets four 5 s steps, so this is a ceiling,
	// not the expected time.
	closeAnswerTimeout = 30 * time.Second
	// exitGrace is how long the process has to exit after stdin closes.
	exitGrace = 30 * time.Second
	// killGrace is how long SIGTERM gets before SIGKILL.
	killGrace = 5 * time.Second
	// readDrain is how long teardown waits for the stdout reader to reach EOF.
	readDrain = 2 * time.Second
	// inspectWaitMS is one run/inspect wait. A wait that runs out is not a
	// failure: the client asks again.
	inspectWaitMS = 30000
	// heldEventLimit bounds the events buffered for a session before a
	// subscriber attaches or while a prompt submission is being acknowledged.
	heldEventLimit = 4096
	// stderrLines is how many of the child's last stderr lines are kept as the
	// exit cause.
	stderrLines = 200
)

// rpcReply is one answer to a client request: a result or a server-refused
// failure, never both.
type rpcReply struct {
	result json.RawMessage
	err    *protocol.Error
}

// client is one poo process and its line-delimited JSON-RPC connection.
type client struct {
	proc   *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser

	stdinMu sync.Mutex

	mu          sync.Mutex
	pending     map[string]chan rpcReply
	subs        map[string][]chan events.Event
	held        map[string][]events.Event
	holds       map[string]int
	submissions map[string][]events.Event
	observe     func(sessionEvent)

	stderr *rollingBuffer

	stdoutDone chan struct{}
	waitDone   chan struct{}
	waitErr    error
	closeOnce  sync.Once
}

// startClient spawns the resolved binary and returns the connected client. The
// argv is the recorded launch identity, so the process is exactly the one the
// session owns.
func startClient(ctx context.Context, opts runtime.StartOptions, observe func(sessionEvent)) (*client, error) {
	argv := Argv(opts.Launch)
	if len(argv) == 0 || argv[0] == "" {
		return nil, errors.New("no poo executable was resolved for this launch")
	}
	proc := launcher.Command(ctx, opts.LaunchWrapper, argv[0], argv[1:]...)
	proc.Dir = opts.Dir
	// The child receives this launch's identity and none of the parent's, the
	// same scrub every backend process goes through. It is given no broker
	// credentials: nothing in a poo binary spends them.
	proc.Env = runtime.LaunchEnv(proc.Environ(), opts.Provenance)

	stdin, err := proc.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("poo stdin pipe: %w", err)
	}
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("poo stdout pipe: %w", err)
	}
	stderr, err := proc.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("poo stderr pipe: %w", err)
	}
	if err := proc.Start(); err != nil {
		return nil, fmt.Errorf("start poo: %w", err)
	}

	c := &client{
		proc:        proc,
		stdin:       stdin,
		stdout:      stdout,
		pending:     map[string]chan rpcReply{},
		subs:        map[string][]chan events.Event{},
		held:        map[string][]events.Event{},
		holds:       map[string]int{},
		submissions: map[string][]events.Event{},
		stderr:      newRollingBuffer(stderrLines),
		stdoutDone:  make(chan struct{}),
		waitDone:    make(chan struct{}),
		observe:     observe,
	}
	go c.wait()
	go c.drainStderr(stderr)
	go c.readLoop()
	return c, nil
}

func (c *client) wait() {
	err := c.proc.Wait()
	c.mu.Lock()
	c.waitErr = err
	c.mu.Unlock()
	close(c.waitDone)
}

func (c *client) pid() int {
	if c.proc != nil && c.proc.Process != nil {
		return c.proc.Process.Pid
	}
	return 0
}

// initialize performs the handshake and checks the agreed protocol version.
func (c *client) initialize(ctx context.Context, versions []int) (protocol.InitializeResult, error) {
	params := protocol.InitializeParams{
		ProtocolVersions: versions,
		Client: protocol.Client{
			Name:    "avenor",
			Version: "1",
			Capabilities: protocol.ClientCapabilities{
				PermissionAnswers: true,
			},
		},
	}
	raw, rpcErr, err := c.call(ctx, protocol.MethodInitialize, params)
	if err != nil {
		return protocol.InitializeResult{}, err
	}
	if rpcErr != nil {
		return protocol.InitializeResult{}, handshakeError(rpcErr)
	}
	var result protocol.InitializeResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return protocol.InitializeResult{}, fmt.Errorf("decode poo/initialize result: %w", err)
	}
	if !containsVersion(versions, result.ProtocolVersion) {
		return protocol.InitializeResult{}, fmt.Errorf("poo agreed on protocol version %d, which this client does not speak (it offered %v)", result.ProtocolVersion, versions)
	}
	return result, nil
}

// handshakeError names a refused handshake the way the operator has to read it:
// a version the binary does not speak is not an unknown method.
func handshakeError(rpcErr *protocol.Error) error {
	if dataCode(rpcErr) == protocol.DataProtocolVersionUnsupported {
		return fmt.Errorf("poo supports no protocol version this client speaks (%s): %s", protocol.DataProtocolVersionUnsupported, rpcErr.Message)
	}
	return rpcError(protocol.MethodInitialize, rpcErr)
}

func containsVersion(versions []int, want int) bool {
	for _, version := range versions {
		if version == want {
			return true
		}
	}
	return false
}

// createSession opens a logged session, or an ephemeral one when the caller
// asked for it.
func (c *client) createSession(ctx context.Context, params protocol.CreateParams) (protocol.SessionInfo, error) {
	raw, rpcErr, err := c.call(ctx, protocol.MethodSessionCreate, params)
	if err != nil {
		return protocol.SessionInfo{}, err
	}
	if rpcErr != nil {
		return protocol.SessionInfo{}, &refusedError{method: protocol.MethodSessionCreate, rpc: rpcErr}
	}
	var info protocol.SessionInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return protocol.SessionInfo{}, fmt.Errorf("decode session/create result: %w", err)
	}
	if info.SessionID == "" {
		return protocol.SessionInfo{}, errors.New("session/create returned no session id")
	}
	return info, nil
}

// call sends one request and waits for its answer.
func (c *client) call(ctx context.Context, method string, params any) (json.RawMessage, *protocol.Error, error) {
	id := newRequestID()
	request := struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      string          `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params,omitempty"`
	}{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
	}
	if params != nil {
		encoded, err := json.Marshal(params)
		if err != nil {
			return nil, nil, fmt.Errorf("encode %s params: %w", method, err)
		}
		request.Params = encoded
	}
	line, err := json.Marshal(request)
	if err != nil {
		return nil, nil, fmt.Errorf("encode %s request: %w", method, err)
	}

	// A response echoes the id as JSON, so the pending table is keyed by the
	// encoded form rather than the bare string.
	key := string(mustMarshal(id))

	reply := make(chan rpcReply, 1)
	c.mu.Lock()
	if c.pending == nil {
		c.mu.Unlock()
		return nil, nil, errors.New("poo client is closed")
	}
	c.pending[key] = reply
	c.mu.Unlock()

	if err := c.writeLine(append(line, '\n')); err != nil {
		c.forget(key)
		return nil, nil, fmt.Errorf("send %s: %w", method, err)
	}

	select {
	case answer := <-reply:
		return answer.result, answer.err, nil
	case <-ctx.Done():
		c.forget(key)
		return nil, nil, ctx.Err()
	case <-c.stdoutDone:
		// The reader dispatches a final line before it reports EOF, so an
		// answer that arrived just before the exit is already queued.
		select {
		case answer := <-reply:
			return answer.result, answer.err, nil
		default:
		}
		c.forget(key)
		return nil, nil, fmt.Errorf("poo exited before answering %s: %s", method, c.exitCause())
	}
}

func (c *client) forget(key string) {
	c.mu.Lock()
	delete(c.pending, key)
	c.mu.Unlock()
}

func (c *client) writeLine(line []byte) error {
	c.stdinMu.Lock()
	defer c.stdinMu.Unlock()
	_, err := c.stdin.Write(line)
	return err
}

// closeSession asks the backend to close one session. The answer is bounded:
// poo's own teardown path can spend its record budget before replying.
func (c *client) closeSession(sessionID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), closeAnswerTimeout)
	defer cancel()
	_, rpcErr, err := c.call(ctx, protocol.MethodSessionClose, protocol.CloseParams{SessionID: sessionID})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("session/close did not answer within %s", closeAnswerTimeout)
		}
		return err
	}
	if rpcErr == nil {
		return nil
	}
	switch dataCode(rpcErr) {
	case protocol.DataSessionNotFound, protocol.DataSessionClosed:
		// Closing a session that is already gone is not a failure.
		return nil
	default:
		return fmt.Errorf("session/close: %s", describeRefusal(rpcErr))
	}
}

// shutdown closes stdin and reaps the process: exitGrace for a clean exit, then
// SIGTERM, then SIGKILL killGrace later.
func (c *client) shutdown() error {
	c.closeOnce.Do(func() {
		_ = c.stdin.Close()
		if !c.waitForExit(exitGrace) {
			if c.proc.Process != nil {
				_ = c.proc.Process.Signal(syscall.SIGTERM)
			}
			if !c.waitForExit(killGrace) {
				if c.proc.Process != nil {
					_ = c.proc.Process.Kill()
				}
				<-c.waitDone
			}
		}
		timer := time.NewTimer(readDrain)
		defer timer.Stop()
		select {
		case <-c.stdoutDone:
		case <-timer.C:
		}
		c.mu.Lock()
		c.pending = nil
		c.subs = nil
		c.held = nil
		c.mu.Unlock()
	})
	return nil
}

func (c *client) waitForExit(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-c.waitDone:
		return true
	case <-timer.C:
		return false
	}
}

// exitCause is the process's own last word: the stderr tail it printed before
// it went away, or the exit status when it printed nothing.
func (c *client) exitCause() string {
	if tail := strings.TrimSpace(c.stderr.String()); tail != "" {
		return tail
	}
	c.mu.Lock()
	err := c.waitErr
	c.mu.Unlock()
	if err != nil {
		return err.Error()
	}
	return "the process exited"
}

func (c *client) readLoop() {
	defer close(c.stdoutDone)
	scanner := bufio.NewScanner(c.stdout)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		c.dispatchLine(line)
	}
	if err := scanner.Err(); err != nil {
		c.stderr.Append(fmt.Sprintf("read loop error: %v", err))
	}
}

// envelope is any line the server may send: a response, a notification or a
// server-initiated request. The method decides which.
type envelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *protocol.Error `json:"error,omitempty"`
}

func (c *client) dispatchLine(line []byte) {
	var msg envelope
	if err := json.Unmarshal(line, &msg); err != nil {
		c.stderr.Append(fmt.Sprintf("dropped malformed JSON line: %v", err))
		return
	}
	if msg.Method != "" {
		if hasID(msg.ID) {
			c.routeServerRequest(msg)
			return
		}
		c.routeNotification(msg)
		return
	}
	if !hasID(msg.ID) {
		c.stderr.Append("dropped a response with no id")
		return
	}
	c.mu.Lock()
	reply, ok := c.pending[string(msg.ID)]
	delete(c.pending, string(msg.ID))
	c.mu.Unlock()
	if !ok {
		return
	}
	reply <- rpcReply{result: msg.Result, err: msg.Error}
}

func hasID(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

func (c *client) routeNotification(msg envelope) {
	if msg.Method != protocol.MethodEvent {
		return
	}
	ev, err := decodeSessionEvent(msg.Params)
	if err != nil {
		c.stderr.Append(fmt.Sprintf("dropped a malformed event: %v", err))
		return
	}
	for _, translated := range translateEvent(ev) {
		c.deliver(translated)
	}
	if c.observe != nil {
		c.observe(ev)
	}
}

// decodeSessionEvent reads the event notification's flat params object: the
// envelope fields come out, and the rest stays the event's own data.
func decodeSessionEvent(raw json.RawMessage) (sessionEvent, error) {
	var flat map[string]any
	if err := json.Unmarshal(raw, &flat); err != nil {
		return sessionEvent{}, err
	}
	ev := sessionEvent{SessionID: stringField(flat, "session_id")}
	if ev.SessionID == "" {
		return sessionEvent{}, errors.New("event has no session id")
	}
	if value, ok := events.Int64(flat["seq"]); ok {
		ev.Seq = value
	}
	ev.Type, _ = flat["type"].(string)
	delete(flat, "session_id")
	delete(flat, "seq")
	delete(flat, "type")
	delete(flat, "at")
	ev.Fields = flat
	return ev, nil
}

// routeServerRequest publishes a permission ask to the session's observers. The
// answer itself is not implemented: the ask id is the request id an answer
// will carry.
func (c *client) routeServerRequest(msg envelope) {
	if msg.Method != protocol.MethodPermissionAsk {
		c.stderr.Append(fmt.Sprintf("ignored unknown server request %q", msg.Method))
		return
	}
	var params protocol.AskParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		c.stderr.Append(fmt.Sprintf("dropped a malformed permission ask: %v", err))
		return
	}
	if params.SessionID == "" {
		return
	}
	c.deliver(events.Event{
		Event:     "permission.request",
		SessionID: params.SessionID,
		Fields: map[string]any{
			"request_id":  params.AskID,
			"kind":        "confirm",
			"question":    params.Summary,
			"description": params.Summary,
			"tool":        params.Tool,
			"class":       params.Class,
			"reason":      params.Reason,
			"run_id":      params.RunID,
			"options": []any{
				map[string]any{"optionId": protocol.DecisionAllow, "kind": "allow"},
				map[string]any{"optionId": protocol.DecisionDeny, "kind": "reject"},
			},
		},
	})
}

// deliver fans an event out to the session's subscribers, or holds it until a
// subscriber attaches or the prompt's acceptance is recorded.
func (c *client) deliver(ev events.Event) {
	c.mu.Lock()
	if c.subs == nil {
		c.mu.Unlock()
		return
	}
	// A session's first events are emitted before the create response that
	// names it, and a subscriber attaches only after Start returns: both are
	// held here and flushed when someone is listening.
	if c.holds[ev.SessionID] > 0 || len(c.subs[ev.SessionID]) == 0 {
		c.held[ev.SessionID] = appendBounded(c.held[ev.SessionID], ev)
		c.mu.Unlock()
		return
	}
	subs := append([]chan events.Event(nil), c.subs[ev.SessionID]...)
	c.mu.Unlock()
	publish(subs, ev, c)
}

func appendBounded(buffer []events.Event, ev events.Event) []events.Event {
	if len(buffer) >= heldEventLimit {
		return append(buffer[1:], ev)
	}
	return append(buffer, ev)
}

func publish(subs []chan events.Event, ev events.Event, c *client) {
	for _, ch := range subs {
		if !trySend(ch, ev) {
			c.stderr.Append(fmt.Sprintf("dropped event %q for session %q: subscriber buffer full", ev.Event, ev.SessionID))
		}
	}
}

func trySend(ch chan events.Event, ev events.Event) (sent bool) {
	defer func() { recover() }()
	select {
	case ch <- ev:
		return true
	default:
		return false
	}
}

func (c *client) subscribe(sessionID string, ch chan events.Event) {
	c.mu.Lock()
	if c.subs == nil {
		c.mu.Unlock()
		return
	}
	c.subs[sessionID] = append(c.subs[sessionID], ch)
	held := c.held[sessionID]
	c.held[sessionID] = nil
	c.mu.Unlock()
	for _, ev := range held {
		trySend(ch, ev)
	}
}

func (c *client) unsubscribe(sessionID string, ch chan events.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	chans := c.subs[sessionID]
	for i, sub := range chans {
		if sub != ch {
			continue
		}
		c.subs[sessionID] = append(chans[:i], chans[i+1:]...)
		return
	}
}

// pauseEvents holds a session's events until the returned release runs. It lets
// a prompt's acceptance be published ahead of anything the backend emitted
// before it answered.
func (c *client) pauseEvents(sessionID string) func(*events.Event) {
	c.mu.Lock()
	c.holds[sessionID]++
	c.mu.Unlock()

	var once sync.Once
	return func(submitted *events.Event) {
		once.Do(func() {
			c.mu.Lock()
			if submitted != nil {
				c.submissions[sessionID] = append(c.submissions[sessionID], *submitted)
			}
			c.holds[sessionID]--
			if c.holds[sessionID] > 0 {
				c.mu.Unlock()
				return
			}
			delete(c.holds, sessionID)
			submissions := c.submissions[sessionID]
			delete(c.submissions, sessionID)
			held := c.held[sessionID]
			delete(c.held, sessionID)
			subs := append([]chan events.Event(nil), c.subs[sessionID]...)
			c.mu.Unlock()
			for _, ev := range submissions {
				publish(subs, ev, c)
			}
			for _, ev := range held {
				publish(subs, ev, c)
			}
		})
	}
}

func (c *client) drainStderr(r io.ReadCloser) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		c.stderr.Append(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		c.stderr.Append(fmt.Sprintf("stderr drain error: %v", err))
	}
	_ = r.Close()
}

// refusedError is the server's refusal of one request, kept typed so a caller
// can branch on the data code while the message stays readable.
type refusedError struct {
	method string
	rpc    *protocol.Error
}

func (e *refusedError) Error() string {
	return e.method + ": " + describeRefusal(e.rpc)
}

// refusedCode returns the data code a refusal carries, or "".
func refusedCode(err error) string {
	var refused *refusedError
	if errors.As(err, &refused) {
		return dataCode(refused.rpc)
	}
	return ""
}

func stringField(fields map[string]any, key string) string {
	value, _ := fields[key].(string)
	return value
}

// dataCode returns the machine-readable code an rpc error carries.
func dataCode(rpcErr *protocol.Error) string {
	if rpcErr == nil || rpcErr.Data == nil {
		return ""
	}
	return rpcErr.Data.Code
}

func rpcError(method string, rpcErr *protocol.Error) error {
	if rpcErr == nil {
		return nil
	}
	return fmt.Errorf("%s: %s", method, describeRefusal(rpcErr))
}

// describeRefusal renders a server refusal with the code a caller branches on
// and the server's own text.
func describeRefusal(rpcErr *protocol.Error) string {
	if rpcErr == nil {
		return ""
	}
	code := dataCode(rpcErr)
	switch {
	case code != "" && rpcErr.Message != "":
		return fmt.Sprintf("%s: %s", code, rpcErr.Message)
	case code != "":
		return code
	default:
		return rpcErr.Message
	}
}

// isRefusal reports whether an rpc error carries one of the codes.
func isRefusal(rpcErr *protocol.Error, codes ...string) bool {
	code := dataCode(rpcErr)
	for _, want := range codes {
		if code == want {
			return true
		}
	}
	return false
}

// rollingBuffer keeps the child's most recent stderr lines.
type rollingBuffer struct {
	mu    sync.Mutex
	lines []string
	cap   int
}

func newRollingBuffer(capacity int) *rollingBuffer {
	return &rollingBuffer{cap: capacity}
}

func (b *rollingBuffer) Append(line string) {
	b.mu.Lock()
	b.lines = append(b.lines, line)
	if len(b.lines) > b.cap {
		b.lines = b.lines[len(b.lines)-b.cap:]
	}
	b.mu.Unlock()
}

func (b *rollingBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Join(b.lines, "\n")
}

// mustMarshal encodes a value whose shape cannot fail (a string), so a request
// id can be keyed by exactly what the server echoes.
func mustMarshal(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return encoded
}

var requestIDCounter int64

func newRequestID() string {
	return fmt.Sprintf("avenor-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&requestIDCounter, 1))
}
