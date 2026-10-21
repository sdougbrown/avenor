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

	"github.com/sdougbrown/avenor/internal/childmessaging"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
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
	opts     Options
	provider *childmessaging.MessageProvider
	outMu    sync.Mutex
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
	provider, err := childmessaging.NewMessageProvider(messaging.SurfaceChanneltools, childmessaging.Options{
		RunID:      opts.RunID,
		Token:      opts.Token,
		BrokerURL:  opts.BrokerURL,
		HTTPClient: opts.HTTPClient,
	})
	if err != nil {
		return fmt.Errorf("local broker messaging: %w", err)
	}
	defer func() { _ = provider.Close() }()

	s := &Server{opts: opts, provider: provider}
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
	case "avenor_message":
		return s.message(ctx, args)
	case "avenor_peers":
		return s.peers(ctx, args)
	default:
		return nil, fmt.Errorf("unknown tool %q", name)
	}
}

// messageRouter binds the shared child provider to one generation and hands the
// call to the one router all Go surfaces use.
func (s *Server) messageRouter(ctx context.Context) (*router.Router, error) {
	if err := s.provider.Prepare(ctx); err != nil {
		return nil, err
	}
	return router.NewRouter(s.provider)
}

func (s *Server) message(ctx context.Context, args json.RawMessage) (any, error) {
	rt, err := s.messageRouter(ctx)
	if err != nil {
		return nil, err
	}
	result, err := rt.Execute(ctx, args)
	if err != nil {
		var toolErr *router.ToolError
		if errors.As(err, &toolErr) {
			return structuredToolResult(toolErr.Output(), toolErr.Message, true), nil
		}
		return nil, err
	}
	return structuredToolResult(result.Output(), result.Text, false), nil
}

func (s *Server) peers(ctx context.Context, args json.RawMessage) (any, error) {
	if err := s.provider.Prepare(ctx); err != nil {
		return nil, err
	}
	result, err := router.ExecutePeers(ctx, s.provider, args)
	if err != nil {
		var toolErr *router.ToolError
		if errors.As(err, &toolErr) {
			return structuredToolResult(toolErr.Output(), toolErr.Message, true), nil
		}
		return nil, err
	}
	return structuredToolResult(router.PeersDetails(result), router.PeersContent(result), false), nil
}

// structuredToolResult wraps one tool outcome in the MCP envelope: content
// carries the rendered sentence as its single text part, structuredContent
// carries the shared snake_case result object, and isError marks a router
// refusal.
func structuredToolResult(object map[string]any, text string, isError bool) map[string]any {
	return map[string]any{
		"content":           []map[string]any{{"type": "text", "text": text}},
		"structuredContent": object,
		"isError":           isError,
	}
}

func toolSchemas() []map[string]any {
	return []map[string]any{
		{
			"name":        "avenor_message",
			"description": "Tell, ask or withdraw a message between agent runs; reply, resume and control replies are not available on this surface",
			"inputSchema": messaging.MessageInputFlatSchema(),
		},
		{
			"name":        "avenor_peers",
			"description": "List connected agent sessions, rename a run's display name, or read a peer's last agent message",
			"inputSchema": messaging.PeersInputFlatSchema(),
		},
	}
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
