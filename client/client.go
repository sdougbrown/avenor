package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const maxSendToParentMessageBytes = 64 * 1024

type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RespError      `json:"error,omitempty"`
}

type RespError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type Notification struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// RPCError preserves structured RPC error payloads (e.g. a failed broker ask's
// message_id in Data) instead of flattening them into a string.
type RPCError struct {
	Code    int
	Message string
	Data    json.RawMessage
}

func (e *RPCError) Error() string { return fmt.Sprintf("rpc error [%d]: %s", e.Code, e.Message) }

type Event struct {
	Event     string         `json:"event"`
	SessionID string         `json:"session_id,omitempty"`
	RuntimeID string         `json:"runtime_id,omitempty"`
	Raw       map[string]any `json:"-"`
}

func (e *Event) UnmarshalJSON(data []byte) error {
	e.Raw = map[string]any{}
	if err := json.Unmarshal(data, &e.Raw); err != nil {
		return err
	}
	if v, ok := e.Raw["event"].(string); ok {
		e.Event = v
	}
	if v, ok := e.Raw["session_id"].(string); ok {
		e.SessionID = v
	}
	if v, ok := e.Raw["runtime_id"].(string); ok {
		e.RuntimeID = v
	}
	return nil
}

type Client struct {
	conn    net.Conn
	mu      sync.Mutex
	reader  *bufio.Reader
	nextID  int
	started bool

	readMu    sync.Mutex
	pending   map[int]chan Response
	eventCh   chan Event
	eventOnce sync.Once
	dropped   int // events discarded due to full eventCh; surfaced as client.lagged

	subMu      sync.Mutex
	subscribed bool

	subsMu      sync.Mutex
	runtimeSubs map[string]map[chan Event]struct{}

	// closed records a terminal connection state: explicit Close, a readLoop
	// exit (EOF or read error), or a failed request write. A closed client
	// refuses new calls instead of parking them on the response timeout.
	closed   atomic.Bool
	closedCh chan struct{}
}

func Dial(socketPath string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial control socket: %w", err)
	}
	c := &Client{
		conn:        conn,
		reader:      bufio.NewReader(conn),
		pending:     map[int]chan Response{},
		eventCh:     make(chan Event, 256),
		runtimeSubs: map[string]map[chan Event]struct{}{},
		closedCh:    make(chan struct{}),
	}
	// Start reading immediately so an idle EOF (a supervisor that exited
	// right after the dial) is observable via Closed() before the first RPC.
	c.eventOnce.Do(func() {
		go c.readLoop()
	})
	return c, nil
}

// Closed reports whether the connection is terminally gone: Close was
// called, the readLoop exited (EOF or read error), or a request write failed.
// An RPC error or response timeout does not close the client; the connection
// may still be usable.
func (c *Client) Closed() bool { return c.closed.Load() }

// markClosed sets the closed flag and closes closedCh exactly once, so the
// three close sites (Close, readLoop exit, failed write) can race safely.
// Clients constructed without Dial have a nil channel and only get the flag.
func (c *Client) markClosed() {
	if c.closed.CompareAndSwap(false, true) {
		if c.closedCh != nil {
			close(c.closedCh)
		}
	}
}

func (c *Client) Close() error {
	c.markClosed()
	return c.conn.Close()
}

func (c *Client) Call(method string, params any, result any) error {
	return c.call(method, params, result, 0)
}

// call issues a request and waits for the response. A positive wait bounds
// the response wait; zero uses the 30s default; a negative wait waits
// indefinitely (for server-side long-poll methods such as wait_turn).
func (c *Client) call(method string, params any, result any, wait time.Duration) error {
	if method == "subscribe" {
		// An explicit subscribe (global or per-runtime) satisfies the
		// ensureSubscribed contract; auto-subscribing again would register a
		// second subscriber and duplicate event delivery. Marking subscribed
		// before the round trip means a failed explicit subscribe is never
		// silently retried behind the caller's back; the error is the caller's
		// to handle.
		c.subMu.Lock()
		c.subscribed = true
		c.subMu.Unlock()
	}
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	req := Request{JSONRPC: "2.0", ID: id, Method: method}
	if params != nil {
		data, err := json.Marshal(params)
		if err != nil {
			c.mu.Unlock()
			return fmt.Errorf("marshal params: %w", err)
		}
		req.Params = data
	}

	data, err := json.Marshal(req)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("marshal request: %w", err)
	}
	if c.closed.Load() {
		c.mu.Unlock()
		return fmt.Errorf("call %s: connection closed", method)
	}
	// Register the response waiter before writing so an already-running
	// readLoop cannot receive and discard a fast response.
	respCh := make(chan Response, 1)
	c.pending[id] = respCh

	// Ensure readLoop is running.
	c.eventOnce.Do(func() {
		go c.readLoop()
	})

	data = append(data, '\n')
	if _, err := c.conn.Write(data); err != nil {
		// A failed write means the connection is dead; mark it so callers can
		// redial instead of waiting out the response timeout.
		c.markClosed()
		delete(c.pending, id)
		c.mu.Unlock()
		return fmt.Errorf("write request: %w", err)
	}
	c.mu.Unlock()

	if wait == 0 {
		wait = 30 * time.Second
	}
	var timerC <-chan time.Time
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		timerC = timer.C
	}

	var resp Response
	select {
	case r, ok := <-respCh:
		if !ok {
			return fmt.Errorf("read response: connection closed")
		}
		resp = r
	case <-timerC:
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return fmt.Errorf("read response: timeout")
	}

	if resp.Error != nil {
		return &RPCError{Code: resp.Error.Code, Message: resp.Error.Message, Data: resp.Error.Data}
	}
	if result != nil && len(resp.Result) > 0 {
		if err := json.Unmarshal(resp.Result, result); err != nil {
			return fmt.Errorf("unmarshal result: %w", err)
		}
	}
	return nil
}

func (c *Client) subscribe() error {
	var result struct {
		Subscribed bool `json:"subscribed"`
	}
	return c.Call("subscribe", nil, &result)
}

// ensureSubscribed issues the global "subscribe" call at most once so the
// control server registers this connection in its subscriber set and delivers
// events. A caller that already sent its own subscribe (via Call) takes over:
// the auto-subscribe is skipped so its subscription mode is preserved. An
// auto-subscribe failure is never retried and is not reported here; it
// surfaces as no event delivery. Callers that need the subscribe error must
// call subscribe themselves (via Call) and check it.
func (c *Client) ensureSubscribed() {
	c.subMu.Lock()
	if c.subscribed {
		c.subMu.Unlock()
		return
	}
	c.subscribed = true
	c.subMu.Unlock()
	_ = c.subscribe()
}

// Events returns a channel of server-sent events. Only one subscriber is
// supported. The first Events() or SubscribeRuntime() call sends the global
// subscribe request to the server.
func (c *Client) Events() <-chan Event {
	c.eventOnce.Do(func() {
		go c.readLoop()
	})
	c.ensureSubscribed()
	return c.eventCh
}

// readLoop is the single goroutine that reads newline-delimited frames,
// dispatching responses to pending Call() waiters and events to the
// Events() channel. Only one readLoop runs per Client.
func (c *Client) readLoop() {
	if c.reader == nil {
		c.reader = bufio.NewReader(c.conn)
	}
	defer close(c.eventCh)
	defer func() {
		// Mark the connection closed before draining so a Call() goroutine
		// woken by the channel close can observe Closed().
		c.markClosed()
		// Drain pending channels so Call() goroutines don't hang
		// on the 30s timeout after connection drop.
		c.mu.Lock()
		for id, ch := range c.pending {
			delete(c.pending, id)
			close(ch)
		}
		c.mu.Unlock()
	}()
	for {
		line, err := c.reader.ReadBytes('\n')
		if err != nil {
			return
		}
		// Try parsing as a Response (has "id" field). ReadBytes has no
		// token-size ceiling, which lets explicit result replies remain exact.
		var resp Response
		if err := json.Unmarshal(line, &resp); err != nil {
			continue
		}
		if resp.ID != nil {
			// Extract numeric ID.
			id, ok := idToInt(resp.ID)
			if ok {
				c.mu.Lock()
				ch := c.pending[id]
				delete(c.pending, id)
				c.mu.Unlock()
				if ch != nil {
					ch <- resp
				}
			}
			continue
		}
		// No id field — try parsing as a Notification.
		var n Notification
		if err := json.Unmarshal(line, &n); err != nil {
			continue
		}
		if n.Method != "event" {
			continue
		}
		var ev Event
		if err := json.Unmarshal(n.Params, &ev); err != nil {
			continue
		}
		// Non-blocking send: mirrors the server's subscriber.loop() ordering —
		// emit client.lagged *before* the next real event so consumers see the
		// lag notification before post-drop events (not after).
		// If the lagged notification itself can't be sent, leave dropped
		// accumulating; we do not drop the real event because of it.
		if c.dropped > 0 {
			lag := c.dropped
			c.dropped = 0
			select {
			case c.eventCh <- Event{Event: "client.lagged", Raw: map[string]any{"event": "client.lagged", "dropped_count": lag}}:
			default:
				// Channel still full; roll the count back so it accumulates
				// and we retry on the next successful delivery.
				c.dropped += lag
			}
		}
		select {
		case c.eventCh <- ev:
		default:
			c.dropped++
		}
		c.publishRuntimeEvent(ev)
	}
}

func idToInt(v any) (int, bool) {
	switch id := v.(type) {
	case float64:
		return int(id), true
	case int:
		return id, true
	default:
		return 0, false
	}
}

// Identity returns the per-process control-server token, when configured.
// It is used internally to distinguish a newly spawned supervisor from a
// replacement that reused its socket path.
func (c *Client) Identity() (string, error) {
	var result struct {
		Token string `json:"token"`
	}
	if err := c.Call("identity", nil, &result); err != nil {
		return "", err
	}
	return result.Token, nil
}

// ErrWaitTurnTimeout reports that a wait_turn request timed out before the
// runtime's turn settled (control.codeWaitTurnTimeout).
var ErrWaitTurnTimeout = errors.New("wait_turn: timeout before the runtime's turn settled")

// codeWaitTurnTimeout mirrors control.codeWaitTurnTimeout; the client cannot
// import the control package without an import cycle.
const codeWaitTurnTimeout = -32020

// WaitTurn blocks until the runtime's next running→idle transition settles:
// an in-flight turn is waited out, and a turn prompted while the runtime is
// idle is covered too. A non-positive timeout waits indefinitely. The result
// carries the turn's stop_reason, the complete final_output (not the bounded
// status preview), and session_id.
func (c *Client) WaitTurn(runtimeID string, timeout time.Duration) (map[string]any, error) {
	params := map[string]any{"runtime_id": runtimeID}
	wait := time.Duration(-1)
	if timeout > 0 {
		params["timeout_ms"] = timeout.Milliseconds()
		// The server ends the wait at the timeout; allow it delivery margin.
		wait = timeout + 10*time.Second
	}
	var result map[string]any
	err := c.call("wait_turn", params, &result, wait)
	if rpcErr, ok := err.(*RPCError); ok && rpcErr.Code == codeWaitTurnTimeout {
		return result, ErrWaitTurnTimeout
	}
	return result, err
}

// Status returns the snapshot for the one-shot run or a runtime if runtimeID is set.
func (c *Client) Status(runtimeID string) (map[string]any, error) {
	var params any
	if runtimeID != "" {
		params = map[string]string{"runtime_id": runtimeID}
	}
	var result map[string]any
	err := c.Call("status", params, &result)
	return result, err
}

// Result returns the complete terminal reply. Unlike Status, this explicit
// retrieval path is not bounded for lifecycle presentation.
func (c *Client) Result(runtimeID string) (map[string]any, error) {
	var params any
	if runtimeID != "" {
		params = map[string]string{"runtime_id": runtimeID}
	}
	var result map[string]any
	err := c.Call("result", params, &result)
	return result, err
}

// Cancel cancels the one-shot run or a specific runtime if runtimeID is set.
func (c *Client) Cancel(runtimeID string) error {
	var params any
	if runtimeID != "" {
		params = map[string]string{"runtime_id": runtimeID}
	}
	return c.Call("cancel", params, nil)
}

// Prompt sends a follow-up prompt to the one-shot session or a runtime.
func (c *Client) Prompt(runtimeID, text string) error {
	return c.PromptWithRequestID(runtimeID, text, "")
}

// PromptWithRequestID sends a follow-up prompt with an optional child-question request ID.
func (c *Client) PromptWithRequestID(runtimeID, text, requestID string) error {
	params := map[string]string{"text": text}
	if runtimeID != "" {
		params["runtime_id"] = runtimeID
	}
	if requestID != "" {
		params["request_id"] = requestID
	}
	return c.Call("prompt", params, nil)
}

// AnswerPermission answers a pending permission request.
func (c *Client) AnswerPermission(runtimeID, requestID, optionID string) error {
	return c.AnswerPermissionWithMessage(runtimeID, requestID, optionID, "")
}

// AnswerPermissionWithMessage answers a pending permission request with an
// optional write-in message. Only non-empty messages are included in the
// JSON payload, preserving backward compatibility.
func (c *Client) AnswerPermissionWithMessage(runtimeID, requestID, optionID, message string) error {
	params := map[string]string{
		"request_id": requestID,
		"option_id":  optionID,
	}
	if runtimeID != "" {
		params["runtime_id"] = runtimeID
	}
	if message != "" {
		params["message"] = message
	}
	return c.Call("answer_permission", params, nil)
}

// SubscribeRuntime returns a channel of events filtered to a specific runtime.
// The caller must drain the channel. The channel closes when ctx is done or
// the underlying event channel is closed. Calling SubscribeRuntime ensures the
// readLoop is started and the global subscribe request is sent.
func (c *Client) SubscribeRuntime(ctx context.Context, runtimeID string) <-chan Event {
	_ = c.Events() // ensure readLoop is running and subscription is registered
	out := make(chan Event, 256)
	c.subsMu.Lock()
	if c.runtimeSubs[runtimeID] == nil {
		c.runtimeSubs[runtimeID] = map[chan Event]struct{}{}
	}
	c.runtimeSubs[runtimeID][out] = struct{}{}
	c.subsMu.Unlock()
	go func() {
		<-ctx.Done()
		c.subsMu.Lock()
		if subs := c.runtimeSubs[runtimeID]; subs != nil {
			delete(subs, out)
			if len(subs) == 0 {
				delete(c.runtimeSubs, runtimeID)
			}
		}
		c.subsMu.Unlock()
		close(out)
	}()
	return out
}

func (c *Client) publishRuntimeEvent(ev Event) {
	keys := []string{ev.RuntimeID}
	if ev.SessionID != "" && ev.SessionID != ev.RuntimeID {
		keys = append(keys, ev.SessionID)
	}

	c.subsMu.Lock()
	targets := make([]chan Event, 0)
	for _, key := range keys {
		for ch := range c.runtimeSubs[key] {
			targets = append(targets, ch)
		}
	}
	c.subsMu.Unlock()

	for _, ch := range targets {
		select {
		case ch <- ev:
		default:
		}
	}
}

// List returns all active runtimes from the stable supervisor.
func (c *Client) List() ([]map[string]any, error) {
	var result []map[string]any
	err := c.Call("list", nil, &result)
	return result, err
}

// Spawn creates a new runtime in the stable supervisor.
func (c *Client) Spawn(params map[string]any) (map[string]any, error) {
	var result map[string]any
	err := c.Call("spawn", params, &result)
	return result, err
}

// Shutdown shuts down the stable supervisor.
func (c *Client) Shutdown(mode string) error {
	return c.Call("shutdown", map[string]string{"mode": mode}, nil)
}

// InterruptAndPrompt cancels the current turn and starts a new prompt.
func (c *Client) InterruptAndPrompt(runtimeID, text string, keepQueue bool) error {
	params := map[string]any{
		"text":       text,
		"keep_queue": keepQueue,
	}
	if runtimeID != "" {
		params["runtime_id"] = runtimeID
	}
	return c.Call("interrupt_and_prompt", params, nil)
}

// WorkflowCreate stores a workflow template given as raw template JSON.
func (c *Client) WorkflowCreate(template json.RawMessage) (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.create", template, &result)
	return result, err
}

// WorkflowInstantiate creates a workflow instance from a stored template.
// params supplies the instance parameters the template declares; an empty
// params is omitted from the wire.
func (c *Client) WorkflowInstantiate(templateID, templateVersion string, metadata map[string]any, params map[string]string) (map[string]any, error) {
	req := map[string]any{
		"template_id":      templateID,
		"template_version": templateVersion,
	}
	if metadata != nil {
		req["metadata"] = metadata
	}
	if len(params) > 0 {
		req["params"] = params
	}
	var result map[string]any
	err := c.Call("workflow.instantiate", req, &result)
	return result, err
}

// WorkflowStatus returns the lightweight status for a workflow instance.
func (c *Client) WorkflowStatus(workflowID string) (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.status", map[string]string{"workflow_id": workflowID}, &result)
	return result, err
}

// WorkflowWait polls until the workflow is terminal or timeout elapses.
// A timeout <= 0 returns after the first poll; the server defaults to 5s.
func (c *Client) WorkflowWait(workflowID string, timeout time.Duration) (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.wait", map[string]any{
		"workflow_id": workflowID,
		"timeout_ms":  int64(timeout.Milliseconds()),
	}, &result)
	return result, err
}

// WorkflowInspect returns the full instance detail for a workflow.
func (c *Client) WorkflowInspect(workflowID string) (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.inspect", map[string]string{"workflow_id": workflowID}, &result)
	return result, err
}

// WorkflowEvents returns log events with Sequence > afterSeq, capped at limit.
func (c *Client) WorkflowEvents(workflowID string, afterSeq int64, limit int) (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.events", map[string]any{
		"workflow_id": workflowID,
		"after_seq":   afterSeq,
		"limit":       limit,
	}, &result)
	return result, err
}

// WorkflowCommand sends a raw command payload to a workflow instance.
func (c *Client) WorkflowCommand(workflowID string, command json.RawMessage) (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.command", map[string]any{
		"workflow_id": workflowID,
		"command":     command,
	}, &result)
	return result, err
}

// workflowCommand builds a workflow.command payload with the given op
// discriminator and field map and routes it through WorkflowCommand. Fields
// are passed through verbatim; the server-side command handler validates
// them.
func (c *Client) workflowCommand(workflowID, op string, fields map[string]any) (map[string]any, error) {
	payload := make(map[string]any, len(fields)+1)
	for k, v := range fields {
		payload[k] = v
	}
	payload["op"] = op
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("workflow %s command: %w", op, err)
	}
	return c.WorkflowCommand(workflowID, data)
}

// WorkflowComplete atomically completes a machine/external handoff
// activation (op "complete").
func (c *Client) WorkflowComplete(workflowID string, fields map[string]any) (map[string]any, error) {
	return c.workflowCommand(workflowID, "complete", fields)
}

// WorkflowGate records a gate decision on a parked awaiting_gate activation
// (op "gate"); fields carry node_id, activation_id, gate_id, operation, and
// the operation's required fields (actor/reason/evidence for human ops, the
// external result envelope for external_result).
func (c *Client) WorkflowGate(workflowID string, fields map[string]any) (map[string]any, error) {
	return c.workflowCommand(workflowID, "gate", fields)
}

// WorkflowSkip waives every unsatisfied required gate on a parked
// awaiting_gate activation (op "skip"); fields carry node_id, actor,
// reason, evidence_ids, and optionally activation_id.
func (c *Client) WorkflowSkip(workflowID string, fields map[string]any) (map[string]any, error) {
	return c.workflowCommand(workflowID, "skip", fields)
}

// WorkflowUnblock returns a blocked activation to ready (op "unblock");
// fields carry node_id, actor, reason, and optionally activation_id.
func (c *Client) WorkflowUnblock(workflowID string, fields map[string]any) (map[string]any, error) {
	return c.workflowCommand(workflowID, "unblock", fields)
}

// WorkflowControllerCreate registers a workflow-controller given as raw JSON.
func (c *Client) WorkflowControllerCreate(request json.RawMessage) (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.controller.create", request, &result)
	return result, err
}

// WorkflowControllerEnable enables a workflow-controller.
func (c *Client) WorkflowControllerEnable(controllerID string) (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.controller.enable", map[string]string{"controller_id": controllerID}, &result)
	return result, err
}

// WorkflowControllerDisable disables a workflow-controller with a reason.
func (c *Client) WorkflowControllerDisable(controllerID, reason string) (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.controller.disable", map[string]string{
		"controller_id": controllerID,
		"reason":        reason,
	}, &result)
	return result, err
}

// WorkflowControllerStatus returns the status for a workflow-controller.
func (c *Client) WorkflowControllerStatus(controllerID string) (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.controller.status", map[string]string{"controller_id": controllerID}, &result)
	return result, err
}

// WorkflowControllerList lists all workflow-controllers.
func (c *Client) WorkflowControllerList() (map[string]any, error) {
	var result map[string]any
	err := c.Call("workflow.controller.list", nil, &result)
	return result, err
}

// WorkflowReady reports readiness for a workflow-controller. A limit <= 0
// omits the field so the server applies its default.
func (c *Client) WorkflowReady(controllerID string, limit int) (map[string]any, error) {
	params := map[string]any{"controller_id": controllerID}
	if limit > 0 {
		params["limit"] = limit
	}
	var result map[string]any
	err := c.Call("workflow.ready", params, &result)
	return result, err
}

// SendToParent sends a message from a child runtime to its parent.
func (c *Client) SendToParent(runtimeID, message string) error {
	if runtimeID == "" {
		return fmt.Errorf("runtime_id is required")
	}
	if message == "" {
		return fmt.Errorf("message is required")
	}
	if len(message) > maxSendToParentMessageBytes {
		return fmt.Errorf("message exceeds %d bytes", maxSendToParentMessageBytes)
	}
	params := map[string]any{
		"runtime_id": runtimeID,
		"message":    message,
	}
	return c.Call("send_to_parent", params, nil)
}
