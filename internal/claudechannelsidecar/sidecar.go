package claudechannelsidecar

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askclient"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
)

type Options struct {
	RunID string
	// Sender is the run's registered display name. An empty value leaves the
	// name to the broker, which reports the run ID.
	Sender     string
	Token      string
	BrokerURL  string
	Stdin      io.Reader
	Stdout     io.Writer
	Stderr     io.Writer
	HTTPClient *http.Client
}

type Server struct {
	opts        Options
	client      *http.Client
	ask         *askclient.Client
	outMu       sync.Mutex
	initialized chan struct{}
	initOnce    sync.Once
}

func Run(ctx context.Context, opts Options) error {
	if opts.RunID == "" || opts.Token == "" || opts.BrokerURL == "" {
		return errors.New("--run-id, --token, and --broker-url are required")
	}
	if opts.Stdin == nil {
		opts.Stdin = os.Stdin
	}
	if opts.Stdout == nil {
		opts.Stdout = os.Stdout
	}
	if opts.Stderr == nil {
		opts.Stderr = os.Stderr
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	transport, err := localhttp.New("", messaging.LocalCredentials{
		RunID:   opts.RunID,
		Token:   opts.Token,
		BaseURL: opts.BrokerURL,
	})
	if err != nil {
		return fmt.Errorf("local broker messaging: %w", err)
	}
	defer func() { _ = transport.Close() }()

	s := &Server{
		opts:        opts,
		client:      opts.HTTPClient,
		ask:         askclient.New(transport, askclient.Options{}),
		initialized: make(chan struct{}),
	}
	if err := s.brokerPost(ctx, "/register", map[string]any{}); err != nil {
		return fmt.Errorf("register with broker: %w", err)
	}
	fmt.Fprintln(opts.Stderr, "avenor claude-channel: registered with broker")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.heartbeatLoop(ctx)
	go s.pollControlLoop(ctx)

	scanner := bufio.NewScanner(opts.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if err := s.handleLine(ctx, scanner.Bytes()); err != nil {
			fmt.Fprintf(opts.Stderr, "avenor claude-channel: %v\n", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func (s *Server) handleLine(ctx context.Context, line []byte) error {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil
	}
	var msg rpcMessage
	if err := json.Unmarshal(line, &msg); err != nil {
		return fmt.Errorf("decode json-rpc: %w", err)
	}
	if len(msg.ID) == 0 {
		return s.handleNotification(ctx, msg.Method, msg.Params)
	}
	result, err := s.handleRequest(ctx, msg.Method, msg.Params)
	if err != nil {
		return s.writeError(msg.ID, -32603, err.Error())
	}
	return s.writeResult(msg.ID, result)
}

func (s *Server) handleRequest(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case "initialize":
		s.initOnce.Do(func() { close(s.initialized) })
		return map[string]any{
			"protocolVersion": "2024-11-05",
			"serverInfo": map[string]any{
				"name":    "avenor",
				"version": "dev",
			},
			"capabilities": map[string]any{
				"experimental": map[string]any{
					"claude/channel":            map[string]any{},
					"claude/channel/permission": map[string]any{},
				},
				"tools": map[string]any{},
			},
			"instructions": `Messages arrive as <channel source="avenor" ...>. Reply with avenor_reply, report progress with avenor_report, and finish with avenor_finish.`,
		}, nil
	case "tools/list":
		return map[string]any{"tools": toolSchemas()}, nil
	case "tools/call":
		var req struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, fmt.Errorf("decode tool call: %w", err)
		}
		return s.callTool(ctx, req.Name, req.Arguments)
	default:
		return nil, fmt.Errorf("unsupported method %q", method)
	}
}

func (s *Server) handleNotification(ctx context.Context, method string, params json.RawMessage) error {
	switch method {
	case "notifications/initialized", "notifications/cancelled":
		if method == "notifications/initialized" {
			s.initOnce.Do(func() { close(s.initialized) })
		}
		return nil
	case "notifications/claude/channel/permission_request":
		var p struct {
			RequestID    string `json:"request_id"`
			ToolName     string `json:"tool_name"`
			Description  string `json:"description"`
			InputPreview string `json:"input_preview"`
		}
		if err := json.Unmarshal(params, &p); err != nil {
			return fmt.Errorf("decode permission_request: %w", err)
		}
		return s.brokerPost(ctx, "/permission_request", map[string]any{
			"request_id":    p.RequestID,
			"tool_name":     p.ToolName,
			"description":   p.Description,
			"input_preview": p.InputPreview,
		})
	default:
		return nil
	}
}

func (s *Server) callTool(ctx context.Context, name string, args json.RawMessage) (any, error) {
	switch name {
	case "avenor_report":
		var p struct {
			State   string          `json:"state"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(args, &p); err != nil {
			return nil, err
		}
		if err := s.brokerPost(ctx, "/report", map[string]any{"state": p.State, "payload": rawOrObject(p.Payload)}); err != nil {
			return nil, err
		}
	case "avenor_finish":
		var p struct {
			Status       string          `json:"status"`
			Summary      string          `json:"summary"`
			FilesChanged []string        `json:"files_changed"`
			Details      json.RawMessage `json:"details"`
		}
		if err := json.Unmarshal(args, &p); err != nil {
			return nil, err
		}
		if err := s.brokerPost(ctx, "/finish", map[string]any{
			"status":        p.Status,
			"summary":       p.Summary,
			"files_changed": p.FilesChanged,
			"payload":       rawOrObject(p.Details),
		}); err != nil {
			return nil, err
		}
	case "avenor_reply":
		var p struct {
			To      string          `json:"to"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(args, &p); err != nil {
			return nil, err
		}
		if err := s.brokerPost(ctx, "/reply", map[string]any{"to": p.To, "payload": rawOrObject(p.Payload)}); err != nil {
			return nil, err
		}
	case "avenor_send":
		var p struct {
			ToRunID string `json:"to_run_id"`
			Message string `json:"message"`
			Role    string `json:"role"`
		}
		if err := json.Unmarshal(args, &p); err != nil {
			return nil, err
		}
		if p.ToRunID == "" || p.Message == "" {
			return nil, fmt.Errorf("to_run_id and message are required")
		}
		role := p.Role
		if role == "" {
			role = "agent"
		}
		body, err := messaging.SendBody(s.opts.RunID, p.ToRunID, p.Message, role)
		if err != nil {
			return nil, err
		}
		sent, err := s.ask.Send(ctx, messaging.SendRequest{
			From:    messaging.Identity{RunID: s.opts.RunID, Sender: s.opts.Sender},
			To:      messaging.Identity{RunID: p.ToRunID, Sender: p.ToRunID},
			Body:    string(body),
			NoReply: true,
		})
		if err != nil {
			return nil, fmt.Errorf("send to %s: %w", p.ToRunID, err)
		}
		if sent.Outcome != messaging.OutcomeSent {
			return nil, fmt.Errorf("send to %s: %s", p.ToRunID, sent.Outcome)
		}
		return map[string]any{"content": []map[string]any{{"type": "text", "text": fmt.Sprintf("sent message to run %q", p.ToRunID)}}}, nil
	case "avenor_ask":
		var p struct {
			ToRunID string `json:"to_run_id"`
			Message string `json:"message"`
			Role    string `json:"role,omitempty"`
		}
		if err := json.Unmarshal(args, &p); err != nil {
			return nil, err
		}
		if p.ToRunID == "" || p.Message == "" {
			return nil, fmt.Errorf("to_run_id and message are required")
		}
		role := p.Role
		if role == "" {
			role = "agent"
		}
		sender := messaging.Identity{RunID: s.opts.RunID, Sender: s.opts.Sender}
		send, wait, err := s.ask.AskRequest(sender, s.opts.RunID, p.ToRunID, p.Message, role)
		if err != nil {
			return nil, err
		}
		result, err := s.ask.Ask(ctx, send, wait)
		if err != nil {
			return nil, result.Failure(err)
		}
		replyText, err := result.Text()
		if err != nil {
			return nil, err
		}
		return map[string]any{"content": []map[string]any{{"type": "text", "text": fmt.Sprintf("Reply from %s:\n%s", p.ToRunID, replyText)}}}, nil
	case "avenor_peers":
		var sessionsResult map[string]any
		if err := s.brokerGet(ctx, "/sessions", &sessionsResult); err != nil {
			return nil, err
		}
		sessions, _ := sessionsResult["sessions"].([]any)
		var lines []string
		for _, s := range sessions {
			if entry, ok := s.(map[string]any); ok {
				id, _ := entry["run_id"].(string)
				label, _ := entry["label"].(string)
				status, _ := entry["status"].(string)
				backend, _ := entry["backend"].(string)
				model, _ := entry["model"].(string)
				line := "  " + id
				if label != "" {
					line = fmt.Sprintf("  %s (%s)", label, id)
				}
				if status != "" {
					line += " [" + status
					if backend != "" {
						line += ", " + backend
					}
					if model != "" {
						line += ", " + model
					}
					line += "]"
				}
				lines = append(lines, line)
			}
		}
		if len(lines) == 0 {
			return map[string]any{"content": []map[string]any{{"type": "text", "text": "No other agent sessions connected."}}}, nil
		}
		result := "**Active sessions:**\n" + strings.Join(lines, "\n")
		return map[string]any{"content": []map[string]any{{"type": "text", "text": result}}}, nil
	case "avenor_cancel":
		var p struct {
			MessageID string `json:"message_id"`
		}
		if err := json.Unmarshal(args, &p); err != nil {
			return nil, err
		}
		if p.MessageID == "" {
			return nil, fmt.Errorf("message_id is required")
		}
		withdrawn, err := s.ask.Withdraw(ctx, messaging.WithdrawRequest{
			Sender:    messaging.Identity{RunID: s.opts.RunID, Sender: s.opts.Sender},
			MessageID: p.MessageID,
		})
		if err != nil {
			return nil, fmt.Errorf("cancel %s: %w", p.MessageID, err)
		}
		if withdrawn.Outcome == messaging.OutcomeEdgeAbsent {
			return nil, fmt.Errorf("no pending ask for %s", p.MessageID)
		}
		return map[string]any{"content": []map[string]any{{"type": "text", "text": fmt.Sprintf("Cancellation requested for %s", p.MessageID)}}}, nil
	default:
		return nil, fmt.Errorf("unknown tool %q", name)
	}
	return map[string]any{"content": []map[string]any{{"type": "text", "text": "ok"}}}, nil
}

func (s *Server) brokerGet(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.opts.BrokerURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set(messaging.HeaderRunID, s.opts.RunID)
	req.Header.Set(messaging.HeaderRunToken, s.opts.Token)
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		text, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("broker %s: %s: %s", path, resp.Status, bytes.TrimSpace(text))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func rawOrObject(raw json.RawMessage) any {
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]any{}
	}
	return raw
}

func (s *Server) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.brokerPost(ctx, "/heartbeat", map[string]any{}); err != nil {
				fmt.Fprintf(s.opts.Stderr, "avenor claude-channel heartbeat: %v\n", err)
			}
		}
	}
}

func (s *Server) pollControlLoop(ctx context.Context) {
	select {
	case <-s.initialized:
	case <-ctx.Done():
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		var msgs []controlMessage
		if err := s.brokerPostDecode(ctx, "/poll-control", map[string]any{}, &msgs); err != nil {
			fmt.Fprintf(s.opts.Stderr, "avenor claude-channel poll-control: %v\n", err)
			sleepContext(ctx, 2*time.Second)
			continue
		}
		for _, msg := range msgs {
			_ = s.writeNotification("notifications/claude/channel", map[string]any{
				"content": renderControlMessage(msg),
				"meta": map[string]string{
					"run_id":      msg.RunID,
					"ctrl_id":     msg.ID,
					"type":        msg.Type,
					"from_run_id": msg.FromRunID,
				},
			})
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

type controlMessage struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	RunID     string          `json:"run_id"`
	FromRunID string          `json:"from_run_id,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

func renderControlMessage(msg controlMessage) string {
	payload := string(bytes.TrimSpace(msg.Payload))
	if payload == "" {
		payload = "{}"
	}
	switch msg.Type {
	case "continue":
		// Extract the message from the payload and present it as a natural prompt.
		var p struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(msg.Payload, &p); err == nil && p.Message != "" {
			return p.Message
		}
		return "Continue working on the supplied task."
	case "add_context":
		return "Incorporate the following context:\n" + payload
	case "request_status":
		return fmt.Sprintf("Status requested. Reply by calling avenor_reply with to=%q.", msg.ID)
	case "cancel":
		return "Stop work. Call avenor_finish(status=blocked|failed) if possible. Avoid starting new tool calls."
	case "permission_decision":
		return "Permission decision: " + payload
	case "agent_message":
		// Only the broker-set FromRunID is trusted for attribution.
		var agent struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(msg.Payload, &agent); err == nil && agent.Message != "" {
			return fmt.Sprintf("Message from run %s:\n%s\n\nReply by calling avenor_send with to_run_id=%q.",
				msg.FromRunID, agent.Message, msg.FromRunID)
		}
		// Fallback render if payload doesn't parse
		return "Message from another agent:\n" + payload
	default:
		return "Unhandled control type: " + msg.Type
	}
}

func joinLines(lines []string) string {
	var buf bytes.Buffer
	for i, line := range lines {
		if i > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(line)
	}
	return buf.String()
}

func (s *Server) brokerPost(ctx context.Context, path string, body map[string]any) error {
	return s.brokerPostDecode(ctx, path, body, nil)
}

func (s *Server) brokerPostDecode(ctx context.Context, path string, body map[string]any, out any) error {
	body["run_id"] = s.opts.RunID
	body["token"] = s.opts.Token
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.opts.BrokerURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		text, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("broker %s: %s: %s", path, resp.Status, bytes.TrimSpace(text))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (s *Server) writeResult(id json.RawMessage, result any) error {
	return s.write(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(id), "result": result})
}

func (s *Server) writeError(id json.RawMessage, code int, message string) error {
	return s.write(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

func (s *Server) writeNotification(method string, params any) error {
	return s.write(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func (s *Server) write(v any) error {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	enc := json.NewEncoder(s.opts.Stdout)
	return enc.Encode(v)
}

func toolSchemas() []map[string]any {
	return []map[string]any{
		{
			"name":        "avenor_report",
			"description": "Report progress back to Avenor",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"state": map[string]any{
						"type": "string",
						"enum": []string{"started", "thinking", "working", "blocked", "checkpoint", "permission_requested", "error"},
					},
					"payload": map[string]any{"type": "object"},
				},
				"required": []string{"state", "payload"},
			},
		},
		{
			"name":        "avenor_finish",
			"description": "Signal run completion",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"status":        map[string]any{"type": "string", "enum": []string{"done", "failed", "blocked"}},
					"summary":       map[string]any{"type": "string"},
					"files_changed": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					"details":       map[string]any{"type": "object"},
				},
				"required": []string{"status", "summary"},
			},
		},
		{
			"name":        "avenor_reply",
			"description": "Reply to a specific control message",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"to":      map[string]any{"type": "string"},
					"payload": map[string]any{"type": "object"},
				},
				"required": []string{"to"},
			},
		},
		{
			"name":        "avenor_send",
			"description": "Send a message to another agent run",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"to_run_id": map[string]any{"type": "string"},
					"message":   map[string]any{"type": "string"},
					"role":      map[string]any{"type": "string", "description": "Role to display on the receiving side (defaults to agent)"},
				},
				"required": []string{"to_run_id", "message"},
			},
		},
		{
			"name":        "avenor_ask",
			"description": "Ask another agent run a question and wait for a reply (blocks until answer arrives or times out)",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"to_run_id": map[string]any{"type": "string", "description": "Target run ID"},
					"message":   map[string]any{"type": "string", "description": "Question to ask"},
					"role":      map[string]any{"type": "string", "description": "Role to display (defaults to agent)"},
				},
				"required": []string{"to_run_id", "message"},
			},
		},
		{
			"name":        "avenor_peers",
			"description": "List all connected agent sessions with their status, backend, and model",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "avenor_cancel",
			"description": "Cancel a pending ask message by its message ID",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"message_id": map[string]any{"type": "string", "description": "Message ID of the pending ask to cancel"},
				},
				"required": []string{"message_id"},
			},
		},
	}
}
