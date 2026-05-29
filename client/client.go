package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
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
	scan    *bufio.Scanner
	nextID  int
	started bool

	readMu    sync.Mutex
	pending   map[int]chan Response
	eventCh   chan Event
	eventOnce sync.Once
	dropped   int // events discarded due to full eventCh; surfaced as client.lagged

	subsMu      sync.Mutex
	runtimeSubs map[string]map[chan Event]struct{}
}

func Dial(socketPath string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial control socket: %w", err)
	}
	return &Client{
		conn:    conn,
		scan:    bufio.NewScanner(conn),
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 256),
		runtimeSubs: map[string]map[chan Event]struct{}{},
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Call(method string, params any, result any) error {
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
		delete(c.pending, id)
		c.mu.Unlock()
		return fmt.Errorf("write request: %w", err)
	}
	c.mu.Unlock()

	var resp Response
	select {
	case r, ok := <-respCh:
		if !ok {
			return fmt.Errorf("read response: connection closed")
		}
		resp = r
	case <-time.After(30 * time.Second):
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return fmt.Errorf("read response: timeout")
	}

	if resp.Error != nil {
		return fmt.Errorf("rpc error [%d]: %s", resp.Error.Code, resp.Error.Message)
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

// Events returns a channel of server-sent events. Only one subscriber is
// supported. Call before any Call() to avoid missing events.
func (c *Client) Events() <-chan Event {
	c.eventOnce.Do(func() {
		go c.readLoop()
	})
	return c.eventCh
}

// readLoop is the single goroutine that reads from the bufio.Scanner,
// dispatching responses to pending Call() waiters and events to the
// Events() channel. Only one readLoop runs per Client.
func (c *Client) readLoop() {
	defer close(c.eventCh)
	defer func() {
		// Drain pending channels so Call() goroutines don't hang
		// on the 30s timeout after connection drop.
		c.mu.Lock()
		for id, ch := range c.pending {
			delete(c.pending, id)
			close(ch)
		}
		c.mu.Unlock()
	}()
	for c.scan.Scan() {
		line := c.scan.Bytes()
		// Try parsing as a Response (has "id" field).
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
	params := map[string]string{
		"request_id": requestID,
		"option_id":  optionID,
	}
	if runtimeID != "" {
		params["runtime_id"] = runtimeID
	}
	return c.Call("answer_permission", params, nil)
}

// SubscribeRuntime returns a channel of events filtered to a specific runtime.
// The caller must drain the channel. The channel closes when ctx is done or
// the underlying event channel is closed. Calling SubscribeRuntime ensures the
// readLoop is started.
func (c *Client) SubscribeRuntime(ctx context.Context, runtimeID string) <-chan Event {
	_ = c.Events() // ensure readLoop is running
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
