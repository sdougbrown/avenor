package channeltools

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
	"sync"
	"time"
)

type Options struct {
	RunID      string
	Token      string
	BrokerURL  string
	Stdin      io.Reader
	Stdout     io.Writer
	Stderr     io.Writer
	HTTPClient *http.Client
}

type Server struct {
	opts   Options
	client *http.Client
	outMu  sync.Mutex
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
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

	s := &Server{opts: opts, client: opts.HTTPClient}
	scanner := bufio.NewScanner(opts.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if err := s.handleLine(ctx, scanner.Bytes()); err != nil {
			fmt.Fprintf(opts.Stderr, "avenor channel-tools: %v\n", err)
		}
	}
	return scanner.Err()
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
		return nil
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
		return map[string]any{
			"protocolVersion": "2024-11-05",
			"serverInfo":      map[string]any{"name": "avenor-channel-tools", "version": "dev"},
			"capabilities":    map[string]any{"tools": map[string]any{}},
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

func (s *Server) callTool(ctx context.Context, name string, args json.RawMessage) (any, error) {
	switch name {
	case "avenor_send", "avenor_upsend":
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
		if err := s.brokerPost(ctx, "/send", map[string]any{
			"from_run_id": s.opts.RunID,
			"to_run_id":   p.ToRunID,
			"type":        "agent_message",
			"payload":     map[string]any{"from": s.opts.RunID, "from_run_id": s.opts.RunID, "message": p.Message, "role": role},
		}); err != nil {
			return nil, err
		}
		resultText := fmt.Sprintf("sent message to run %q", p.ToRunID)
		if name == "avenor_upsend" {
			resultText = fmt.Sprintf("sent upward message to run %q", p.ToRunID)
		}
		return map[string]any{"content": []map[string]any{{"type": "text", "text": resultText}}}, nil
	default:
		return nil, fmt.Errorf("unknown tool %q", name)
	}
}

func toolSchemas() []map[string]any {
	return []map[string]any{
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
			"name":        "avenor_upsend",
			"description": "Send a message upward to your parent or supervisor agent.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"to_run_id": map[string]any{"type": "string", "description": "Target parent/supervisor run ID"},
					"message":   map[string]any{"type": "string", "description": "Message content"},
					"role":      map[string]any{"type": "string", "description": "Role to display (e.g., reviewer, implementer)"},
				},
				"required": []string{"to_run_id", "message"},
			},
		},
	}
}

func (s *Server) brokerPost(ctx context.Context, path string, body map[string]any) error {
	requestBody := make(map[string]any, len(body)+2)
	for key, value := range body {
		requestBody[key] = value
	}
	requestBody["run_id"] = s.opts.RunID
	requestBody["token"] = s.opts.Token
	payload, err := json.Marshal(requestBody)
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
	return nil
}

func (s *Server) writeResult(id json.RawMessage, result any) error {
	return s.write(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(id), "result": result})
}

func (s *Server) writeError(id json.RawMessage, code int, message string) error {
	return s.write(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"error":   map[string]any{"code": code, "message": message},
	})
}

func (s *Server) write(v any) error {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	return json.NewEncoder(s.opts.Stdout).Encode(v)
}
