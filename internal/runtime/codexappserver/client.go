package codexappserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

const stderrCap = 400 // max lines of stderr retained in rolling buffer

type pendingApproval struct {
	threadID string
	turnID   string
	id       json.RawMessage
}

type client struct {
	proc   *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser

	stdinMu   sync.Mutex // serializes writes to stdin
	mu        sync.Mutex
	pending   map[string]chan json.RawMessage // requestID → response result
	turns     map[string]chan events.Event    // turnID → completion event
	turnDone  map[string]events.Event         // turnID → buffered completion event
	subs      map[string][]chan events.Event  // threadID → Events subscribers
	approvals map[string]pendingApproval      // requestID → pending approval

	stderr *rollingBuffer

	eventsCh chan events.Event // output channel for all events (notifications + approvals)
	done     chan struct{}     // closed when reader goroutine exits
}

// writeStdin serializes writes to the stdin pipe to prevent interleaving.
func (c *client) writeStdin(data []byte) error {
	c.stdinMu.Lock()
	defer c.stdinMu.Unlock()
	_, err := c.stdin.Write(data)
	return err
}

func newClient(proc *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser, stderr io.ReadCloser) *client {
	c := &client{
		proc:      proc,
		stdin:     stdin,
		stdout:    stdout,
		pending:   map[string]chan json.RawMessage{},
		turns:     map[string]chan events.Event{},
		turnDone:  map[string]events.Event{},
		subs:      map[string][]chan events.Event{},
		approvals: map[string]pendingApproval{},
		stderr:    newRollingBuffer(stderrCap),
		eventsCh:  make(chan events.Event, 4096),
		done:      make(chan struct{}),
	}
	go c.drainStderr(stderr)
	go c.readLoop()
	return c
}

// StartClient spawns `codex app-server` and performs the initialize handshake.
func StartClient(ctx context.Context) (*client, error) {
	proc := exec.CommandContext(ctx, "codex", "app-server")
	stdin, err := proc.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("codex stdin pipe: %w", err)
	}
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("codex stdout pipe: %w", err)
	}
	stderr, err := proc.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("codex stderr pipe: %w", err)
	}
	if err := proc.Start(); err != nil {
		return nil, fmt.Errorf("start codex app-server: %w", err)
	}

	c := newClient(proc, stdin, stdout, stderr)

	// Initialize handshake
	initParams := map[string]any{
		"clientInfo": map[string]any{
			"name":    "avenor",
			"version": "0.0.0",
		},
		"capabilities": map[string]any{
			"experimentalApi": true,
		},
	}
	_, err = c.request(ctx, "initialize", initParams)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("codex initialize: %w", err)
	}
	if err := c.notify("initialized", nil); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("codex initialized: %w", err)
	}
	return c, nil
}

func (c *client) Close() error {
	// Close stdin and stdout to unblock the readLoop.
	if c.stdin != nil {
		_ = c.stdin.Close()
	}
	if c.stdout != nil {
		_ = c.stdout.Close()
	}

	// Wait for the process if there is one.
	if c.proc != nil {
		done := make(chan error, 1)
		go func() {
			done <- c.proc.Wait()
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = c.proc.Process.Kill()
			<-done
		}
	}

	// Wait for reader goroutine.
	<-c.done

	// Close all pending request channels so blocked callers unblock.
	c.mu.Lock()
	for _, ch := range c.pending {
		close(ch)
	}
	c.pending = nil
	for _, ch := range c.turns {
		close(ch)
	}
	c.turns = nil
	c.turnDone = nil
	c.mu.Unlock()

	close(c.eventsCh)
	return nil
}

func (c *client) Events() <-chan events.Event { return c.eventsCh }

func (c *client) Stderr() string { return c.stderr.String() }

// request sends a JSON-RPC request and blocks until the response arrives.
// If ctx is cancelled or the client is closed, it returns an error.
func (c *client) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := newRequestID()
	ch := make(chan json.RawMessage, 1)

	c.mu.Lock()
	c.pending[id] = ch
	c.mu.Unlock()

	pb, err := json.Marshal(params)
	if err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("marshal params: %w", err)
	}

	idBytes, _ := json.Marshal(id)
	msg := rpcMessage{
		ID:     json.RawMessage(idBytes),
		Method: method,
		Params: pb,
	}
	data, _ := json.Marshal(msg)
	data = append(data, '\n')

	if err := c.writeStdin(data); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("write request: %w", err)
	}

	select {
	case result, ok := <-ch:
		if !ok {
			return nil, errors.New("client closed while waiting for response")
		}
		if err := rpcErrorFromResult(result); err != nil {
			return nil, err
		}
		return result, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	case <-c.done:
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, errors.New("client closed while waiting for response")
	}
}

func rpcErrorFromResult(result json.RawMessage) error {
	var envelope struct {
		Error *rpcError `json:"error"`
	}
	if err := json.Unmarshal(result, &envelope); err != nil || envelope.Error == nil {
		return nil
	}
	return fmt.Errorf("rpc error %d: %s", envelope.Error.Code, envelope.Error.Message)
}

func (c *client) notify(method string, params any) error {
	pb, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal notify params: %w", err)
	}

	msg := rpcMessage{
		Method: method,
		Params: pb,
	}
	data, _ := json.Marshal(msg)
	data = append(data, '\n')

	if err := c.writeStdin(data); err != nil {
		return fmt.Errorf("write notify: %w", err)
	}
	return nil
}

func (c *client) answerApproval(requestID string, allow bool) error {
	c.mu.Lock()
	approval, ok := c.approvals[requestID]
	if ok {
		delete(c.approvals, requestID)
	}
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("approval request %q not found", requestID)
	}

	return c.writeApprovalResponse(requestID, approval.id, allow)
}

// answerApprovalThreaded validates that the approval belongs to the given thread
// before answering. Used by the provider to enforce session-scoped permission isolation.
func (c *client) answerApprovalThreaded(requestID, threadID string, allow bool) error {
	c.mu.Lock()
	approval, ok := c.approvals[requestID]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("approval request %q not found", requestID)
	}
	if approval.threadID != threadID {
		c.mu.Unlock()
		return fmt.Errorf("approval request %q belongs to thread %q, not %q", requestID, approval.threadID, threadID)
	}
	delete(c.approvals, requestID)
	c.mu.Unlock()

	return c.writeApprovalResponse(requestID, approval.id, allow)
}

func (c *client) writeApprovalResponse(requestID string, id json.RawMessage, allow bool) error {
	if len(id) == 0 {
		id, _ = json.Marshal(requestID)
	}
	decision := "decline"
	if allow {
		decision = "accept"
	}
	msg := rpcMessage{
		ID:     id,
		Result: mustRaw(map[string]any{"decision": decision}),
	}
	data, _ := json.Marshal(msg)
	data = append(data, '\n')

	if err := c.writeStdin(data); err != nil {
		return err
	}
	return nil
}

// subscribe registers a channel to receive events for a given threadID.
func (c *client) subscribe(threadID string, ch chan events.Event) {
	c.mu.Lock()
	c.subs[threadID] = append(c.subs[threadID], ch)
	c.mu.Unlock()
}

// unsubscribe removes a subscriber channel.
func (c *client) unsubscribe(threadID string, ch chan events.Event) {
	c.mu.Lock()
	chans := c.subs[threadID]
	for i, sub := range chans {
		if sub == ch {
			c.subs[threadID] = append(chans[:i], chans[i+1:]...)
			break
		}
	}
	c.mu.Unlock()
}

// registerTurn registers a buffered channel to receive the completion event for a turn.
func (c *client) registerTurn(turnID string) chan events.Event {
	ch := make(chan events.Event, 1)
	c.mu.Lock()
	c.turns[turnID] = ch
	if ev, ok := c.turnDone[turnID]; ok {
		delete(c.turnDone, turnID)
		select {
		case ch <- ev:
		default:
		}
	}
	c.mu.Unlock()
	return ch
}

// unregisterTurn removes a turn waiter.
func (c *client) unregisterTurn(turnID string) {
	c.mu.Lock()
	delete(c.turns, turnID)
	delete(c.turnDone, turnID)
	c.mu.Unlock()
}

// readLoop is the single goroutine that reads lines from stdout.
func (c *client) readLoop() {
	defer close(c.done)
	scanner := bufio.NewScanner(c.stdout)
	// Allow up to 50MB per line (codex can emit large JSON payloads).
	scanner.Buffer(make([]byte, 1024*1024), 50*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		c.dispatchLine(line)
	}
	if err := scanner.Err(); err != nil {
		// Log stderr? For now, drain is best-effort.
		_ = err
	}
}

// dispatchLine classifies and routes a single JSON-RPC line from stdout.
func (c *client) dispatchLine(line []byte) {
	var msg rpcMessage
	if err := json.Unmarshal(line, &msg); err != nil {
		c.stderr.Append(fmt.Sprintf("dropped malformed JSON-RPC line: %v", err))
		return
	}

	hasID := len(msg.ID) > 0 && string(msg.ID) != "null"
	hasMethod := msg.Method != ""

	if hasID && !hasMethod {
		// RPC response: route to pending request.
		c.routeResponse(msg.ID, msg.Result, msg.Error)
		return
	}

	if hasMethod && !hasID {
		// Notification: translate and route to subscribers.
		c.routeNotification(msg.Method, msg.Params)
		return
	}

	if hasID && hasMethod {
		// Server-initiated request: store approval, translate, route to subscribers.
		c.routeApprovalRequest(msg.ID, msg.Method, msg.Params)
		return
	}
}

func (c *client) routeResponse(id json.RawMessage, result json.RawMessage, rpcErr *rpcError) {
	idStr := string(id)
	// Strip quotes from string-encoded IDs.
	if len(idStr) >= 2 && idStr[0] == '"' && idStr[len(idStr)-1] == '"' {
		idStr = idStr[1 : len(idStr)-1]
	}

	c.mu.Lock()
	ch, ok := c.pending[idStr]
	delete(c.pending, idStr)
	c.mu.Unlock()

	if !ok {
		return
	}
	if rpcErr != nil {
		ch <- mustRaw(map[string]any{"error": rpcErr})
	} else {
		ch <- result
	}
}

func (c *client) routeNotification(method string, params json.RawMessage) {
	evs := translateNotification(method, params)
	if len(evs) == 0 {
		return
	}

	// Handle turn/completed: signal turn waiters.
	if method == "turn/completed" {
		var n turnCompletedNotification
		if err := json.Unmarshal(params, &n); err == nil {
			terminal := evs[0]
			for _, ev := range evs {
				if ev.Event == "session.end" {
					terminal = ev
					break
				}
			}
			c.mu.Lock()
			ch, ok := c.turns[n.Turn.ID]
			if !ok {
				c.turnDone[n.Turn.ID] = terminal
			}
			c.clearApprovalsForTurnLocked(n.ThreadID, n.Turn.ID)
			c.mu.Unlock()
			if ok {
				select {
				case ch <- terminal:
				default:
				}
			}
		}
	}

	for i := range evs {
		c.fanout(&evs[i])
	}
}

func (c *client) routeApprovalRequest(id json.RawMessage, method string, params json.RawMessage) {
	idStr := string(id)
	if len(idStr) >= 2 && idStr[0] == '"' && idStr[len(idStr)-1] == '"' {
		idStr = idStr[1 : len(idStr)-1]
	}

	_, knownApprovalMethod := approvalKind(method)
	ev, ar := translateApprovalRequest(method, params)
	if ev == nil {
		if knownApprovalMethod {
			_ = c.writeRPCError(id, -32602, fmt.Sprintf("invalid params for app-server request method %q", method))
		} else {
			_ = c.writeRPCError(id, -32601, fmt.Sprintf("unsupported app-server request method %q", method))
		}
		return
	}

	// Attach request_id so AnswerPermission can find it.
	ev.Fields["request_id"] = idStr

	c.mu.Lock()
	c.approvals[idStr] = pendingApproval{threadID: ev.SessionID, turnID: ar.TurnID, id: append(json.RawMessage(nil), id...)}
	c.mu.Unlock()

	c.fanout(ev)
}

func (c *client) fanout(ev *events.Event) {
	if ev == nil {
		return
	}
	select {
	case c.eventsCh <- *ev:
	default:
		c.stderr.Append(fmt.Sprintf("dropped event %q for session %q: global event buffer full", ev.Event, ev.SessionID))
	}

	c.mu.Lock()
	chans := c.subs[ev.SessionID]
	for _, ch := range chans {
		if isCriticalEvent(ev.Event) {
			ch <- *ev
		} else {
			select {
			case ch <- *ev:
			default:
				c.stderr.Append(fmt.Sprintf("dropped event %q for session %q: subscriber buffer full", ev.Event, ev.SessionID))
			}
		}
	}
	c.mu.Unlock()
}

func (c *client) writeRPCError(id json.RawMessage, code int, message string) error {
	msg := rpcMessage{
		ID: id,
		Error: &rpcError{
			Code:    code,
			Message: message,
		},
	}
	data, _ := json.Marshal(msg)
	data = append(data, '\n')
	return c.writeStdin(data)
}

func (c *client) clearApprovalsForTurnLocked(threadID, turnID string) {
	if turnID == "" {
		return
	}
	for requestID, approval := range c.approvals {
		if approval.threadID == threadID && approval.turnID == turnID {
			delete(c.approvals, requestID)
		}
	}
}

func isCriticalEvent(event string) bool {
	switch event {
	case "permission.request", "session.end":
		return true
	default:
		return false
	}
}

func (c *client) drainStderr(r io.ReadCloser) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		c.stderr.Append(scanner.Text())
	}
	_ = r.Close()
}

// rollingBuffer keeps the last N lines of text.
type rollingBuffer struct {
	mu    sync.Mutex
	lines []string
	cap   int
}

func newRollingBuffer(cap int) *rollingBuffer {
	return &rollingBuffer{cap: cap}
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
	var sb strings.Builder
	for _, line := range b.lines {
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	return sb.String()
}

var requestIDCounter int64

func newRequestID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), atomic.AddInt64(&requestIDCounter, 1))
}

func mustRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`null`)
	}
	return b
}
