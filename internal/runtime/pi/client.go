package pi

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

const stderrCap = 400

type pendingApproval struct {
	id        string
	method    string
	rawID     string
	sessionID string
}

type client struct {
	proc   *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser

	stdinMu   sync.Mutex
	mu        sync.Mutex
	pending   map[string]chan json.RawMessage
	subs      map[string][]chan events.Event
	approvals map[string]pendingApproval

	stderr    *rollingBuffer
	eventsCh  chan events.Event
	done      chan struct{}
	closeOnce sync.Once
	sessionID string
	sessionCh chan struct{}
}

func newClient(proc *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser, stderr io.ReadCloser) *client {
	c := &client{
		proc:      proc,
		stdin:     stdin,
		stdout:    stdout,
		pending:   map[string]chan json.RawMessage{},
		subs:      map[string][]chan events.Event{},
		approvals: map[string]pendingApproval{},
		stderr:    newRollingBuffer(stderrCap),
		eventsCh:  make(chan events.Event, 4096),
		done:      make(chan struct{}),
		sessionCh: make(chan struct{}, 1),
	}
	go c.drainStderr(stderr)
	go c.readLoop()
	return c
}

var piExecCommandContext = exec.CommandContext

func StartClient(ctx context.Context, provider string, model string, sessionDir string) (*client, error) {
	return StartClientWithAgentProfileAndDir(ctx, provider, model, sessionDir, "", "", "")
}

func StartClientWithAgent(ctx context.Context, provider string, model string, sessionDir string, agent string) (*client, error) {
	return StartClientWithAgentProfileAndDir(ctx, provider, model, sessionDir, agent, "", "")
}

func StartClientWithAgentAndDir(ctx context.Context, provider string, model string, sessionDir string, agent string, cwd string) (*client, error) {
	return StartClientWithAgentProfileAndDir(ctx, provider, model, sessionDir, agent, "", cwd)
}

func StartClientWithAgentProfileAndDir(ctx context.Context, provider string, model string, sessionDir string, agent string, agentProfile string, cwd string) (*client, error) {
	args := []string{"--mode", "rpc", "--no-session"}
	if provider != "" {
		args = append(args, "--provider", provider)
	}
	if model != "" {
		args = append(args, "--model", model)
	}
	if sessionDir != "" {
		args = append(args, "--session-dir", sessionDir)
	}

	proc := piExecCommandContext(ctx, "pi", args...)
	proc.Dir = cwd
	if agent != "" || agentProfile != "" {
		env := proc.Environ()
		filtered := env[:0]
		for _, e := range env {
			if !strings.HasPrefix(e, "PI_AGENT=") &&
				!strings.HasPrefix(e, "PI_AGENT_PROFILE=") {
				filtered = append(filtered, e)
			}
		}
		if agent != "" {
			filtered = append(filtered, "PI_AGENT="+agent)
		}
		if agentProfile != "" {
			filtered = append(filtered, "PI_AGENT_PROFILE="+agentProfile)
		}
		proc.Env = filtered
	}
	stdin, err := proc.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("pi stdin pipe: %w", err)
	}
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("pi stdout pipe: %w", err)
	}
	stderr, err := proc.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("pi stderr pipe: %w", err)
	}
	if err := proc.Start(); err != nil {
		return nil, fmt.Errorf("start pi rpc: %w", err)
	}

	c := newClient(proc, stdin, stdout, stderr)
	return c, nil
}

func (c *client) Close() error {
	if c.stdin != nil {
		_ = c.stdin.Close()
	}
	if c.stdout != nil {
		_ = c.stdout.Close()
	}

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

	<-c.done

	c.closeOnce.Do(func() {
		c.mu.Lock()
		for _, ch := range c.pending {
			close(ch)
		}
		c.pending = nil
		c.approvals = nil
		c.mu.Unlock()

		close(c.eventsCh)
	})
	return nil
}

func (c *client) Events() <-chan events.Event { return c.eventsCh }

func (c *client) Stderr() string { return c.stderr.String() }

func (c *client) writeStdin(data []byte) error {
	c.stdinMu.Lock()
	defer c.stdinMu.Unlock()
	_, err := c.stdin.Write(data)
	return err
}

func (c *client) sendCommand(ctx context.Context, cmd map[string]any) (json.RawMessage, error) {
	id := newRequestID()
	cmd["id"] = id
	ch := make(chan json.RawMessage, 1)

	c.mu.Lock()
	c.pending[id] = ch
	c.mu.Unlock()

	data, err := json.Marshal(cmd)
	if err != nil {
		c.mu.Lock()
		if c.pending != nil {
			delete(c.pending, id)
		}
		c.mu.Unlock()
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	data = append(data, '\n')

	if err := c.writeStdin(data); err != nil {
		c.mu.Lock()
		if c.pending != nil {
			delete(c.pending, id)
		}
		c.mu.Unlock()
		return nil, fmt.Errorf("write command: %w", err)
	}

	select {
	case result := <-ch:
		return result, nil
	case <-ctx.Done():
		c.mu.Lock()
		if c.pending != nil {
			delete(c.pending, id)
		}
		c.mu.Unlock()
		return nil, ctx.Err()
	case <-c.done:
		c.mu.Lock()
		if c.pending != nil {
			delete(c.pending, id)
		}
		c.mu.Unlock()
		return nil, errors.New("client closed while waiting for response")
	}
}

func (c *client) answerExtensionUI(rawID any, method string, response map[string]any) error {
	msg := map[string]any{
		"type": "extension_ui_response",
		"id":   rawID,
	}
	msg["method"] = method

	cancelled, _ := response["cancelled"].(bool)
	switch method {
	case "select":
		if !cancelled {
			msg["value"] = response["value"]
		} else {
			msg["cancelled"] = true
		}
	case "confirm":
		if cancelled {
			msg["cancelled"] = true
		} else {
			msg["confirmed"] = response["confirmed"]
		}
	case "input", "editor":
		if cancelled {
			msg["cancelled"] = true
		} else {
			msg["value"] = response["value"]
		}
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal extension UI response: %w", err)
	}
	data = append(data, '\n')
	return c.writeStdin(data)
}

func (c *client) subscribe(sessionID string, ch chan events.Event) {
	c.mu.Lock()
	c.subs[sessionID] = append(c.subs[sessionID], ch)
	c.mu.Unlock()
}

func (c *client) unsubscribe(sessionID string, ch chan events.Event) {
	c.mu.Lock()
	chans := c.subs[sessionID]
	for i, sub := range chans {
		if sub == ch {
			n := len(chans)
			copy(chans[i:], chans[i+1:])
			chans[n-1] = nil
			c.subs[sessionID] = chans[:n-1]
			break
		}
	}
	c.mu.Unlock()
}

func (c *client) setSessionID(sid string) {
	c.mu.Lock()
	c.sessionID = sid
	if c.sessionCh != nil {
		select {
		case c.sessionCh <- struct{}{}:
		default:
		}
	}
	c.mu.Unlock()
}

func (c *client) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

func (c *client) readLoop() {
	defer close(c.done)
	scanner := bufio.NewScanner(c.stdout)
	scanner.Buffer(make([]byte, 1024*1024), 50*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		line = []byte(strings.TrimSuffix(string(line), "\r"))
		if len(line) == 0 {
			continue
		}
		c.dispatchLine(line)
	}
	if err := scanner.Err(); err != nil {
		c.stderr.Append(fmt.Sprintf("read loop error: %v", err))
	}
}

func (c *client) dispatchLine(line []byte) {
	var payload map[string]any
	if err := json.Unmarshal(line, &payload); err != nil {
		c.stderr.Append(fmt.Sprintf("dropped malformed JSON line: %v", err))
		return
	}

	evtType, _ := payload["type"].(string)

	switch {
	case evtType == "response":
		c.routeResponse(payload)
	case evtType == "extension_ui_request":
		c.routeExtensionUI(payload)
	default:
		c.routeEvent(payload)
	}
}

func (c *client) routeResponse(payload map[string]any) {
	id, _ := payload["id"].(string)
	if id == "" {
		return
	}

	c.mu.Lock()
	ch, ok := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()

	if !ok {
		return
	}

	data, err := json.Marshal(payload)
	if err != nil {
		c.stderr.Append(fmt.Sprintf("marshal response for id %s: %v", id, err))
		close(ch)
		return
	}
	ch <- json.RawMessage(data)
}

func (c *client) routeExtensionUI(payload map[string]any) {
	id, _ := payload["id"].(string)
	method, _ := payload["method"].(string)

	isDialog := dialogMethods[method]

	if isDialog {
		sessionID := c.SessionID()
		ev, _ := translateExtensionUI(payload, sessionID)
		if ev == nil {
			return
		}

		ev.Fields["request_id"] = id
		ev.Fields["ui_request_id"] = id

		rawID := fmt.Sprint(payload["id"])

		c.mu.Lock()
		c.approvals[id] = pendingApproval{
			id:        id,
			method:    method,
			rawID:     rawID,
			sessionID: sessionID,
		}
		c.mu.Unlock()

		c.fanout(ev)
		return
	}

	if ev, _ := translateExtensionUI(payload, c.SessionID()); ev != nil {
		c.fanout(ev)
	}
}

func (c *client) routeEvent(payload map[string]any) {
	sessionID := c.SessionID()
	evtType, _ := payload["type"].(string)

	if evtType == "response" {
		return
	}

	if evtType == "extension_ui_request" {
		return
	}

	if evtType == "agent_end" {
		if messages, ok := payload["messages"]; ok {
			if msgArr, ok := messages.([]any); ok && len(msgArr) > 0 {
				lastMsg, _ := msgArr[len(msgArr)-1].(map[string]any)
				if lastMsg != nil {
					if sessID, ok := lastMsg["sessionId"]; ok {
						if sessIDStr, ok := sessID.(string); ok && sessIDStr != "" {
							sessionID = sessIDStr
						}
					}
				}
			}
		}
	}

	events := translateNotification(payload, sessionID)
	for i := range events {
		c.fanout(&events[i])
	}
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
	chans := make([]chan events.Event, len(c.subs[ev.SessionID]))
	copy(chans, c.subs[ev.SessionID])
	c.mu.Unlock()

	for _, ch := range chans {
		if isCriticalEvent(ev.Event) {
			if !trySend(ch, *ev) {
				c.stderr.Append(fmt.Sprintf("dropped critical event %q for session %q: subscriber buffer full", ev.Event, ev.SessionID))
			}
		} else {
			if !trySend(ch, *ev) {
				c.stderr.Append(fmt.Sprintf("dropped event %q for session %q: subscriber buffer full", ev.Event, ev.SessionID))
			}
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
	if err := scanner.Err(); err != nil {
		c.stderr.Append(fmt.Sprintf("stderr drain error: %v", err))
	}
	_ = r.Close()
}

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
	return fmt.Sprintf("avenor-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&requestIDCounter, 1))
}
