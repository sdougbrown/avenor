package mcpserver

import (
	"context"
	"fmt"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/client"
)

type ControlClient interface {
	Status(runtimeID string) (map[string]any, error)
	List() ([]map[string]any, error)
	Close() error
}

type Options struct {
	Transport        string
	ControlSocket    string
	SupervisorSocket string
	NoAutostart      bool
	IdleTimeout      time.Duration
	ControlClient    ControlClient
}

type Server struct {
	opts          Options
	mcpServer     *mcp.Server
	controlClient ControlClient
	lifecycle     *supervisorLifecycle
}

type statusArgs struct {
	RunID string `json:"run_id,omitempty" jsonschema:"optional run ID to query"`
}

func NewServer(opts Options) (*Server, error) {
	if opts.Transport == "" {
		return nil, fmt.Errorf("transport is required")
	}
	if opts.Transport != "stdio" {
		return nil, fmt.Errorf("unsupported transport: %s", opts.Transport)
	}
	if opts.NoAutostart && opts.SupervisorSocket == "" && opts.ControlClient == nil {
		return nil, fmt.Errorf("--no-autostart requires --supervisor-socket")
	}

	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    "avenor",
		Version: "dev",
	}, nil)

	s := &Server{
		opts:          opts,
		mcpServer:     mcpServer,
		controlClient: opts.ControlClient,
	}

	if opts.SupervisorSocket != "" && opts.ControlClient == nil {
		cl, err := client.Dial(opts.SupervisorSocket)
		if err != nil {
			return nil, fmt.Errorf("dial supervisor socket: %w", err)
		}
		s.controlClient = cl
	}

	if opts.SupervisorSocket == "" && !opts.NoAutostart && opts.ControlClient == nil {
		lc, err := startSupervisor("", opts.IdleTimeout)
		if err != nil {
			return nil, fmt.Errorf("autostart supervisor: %w", err)
		}
		s.lifecycle = lc
		s.controlClient = lc.client
	}

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_status",
		Description: "Get status of avenor runs",
	}, s.handleAvenorStatus)

	return s, nil
}

func (s *Server) Close() error {
	var firstErr error

	if s.controlClient != nil {
		if err := s.controlClient.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.controlClient = nil
	}

	if s.lifecycle != nil {
		if err := s.lifecycle.Shutdown(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.lifecycle = nil
	}

	return firstErr
}

func (s *Server) handleAvenorStatus(ctx context.Context, req *mcp.CallToolRequest, args statusArgs) (*mcp.CallToolResult, any, error) {
	if s.controlClient == nil {
		return nil, nil, fmt.Errorf("control client not available")
	}

	if args.RunID != "" {
		result, err := s.controlClient.Status(args.RunID)
		if err != nil {
			return nil, nil, fmt.Errorf("status: %w", err)
		}
		return nil, result, nil
	}

	results, err := s.controlClient.List()
	if err != nil {
		return nil, nil, fmt.Errorf("list runs: %w", err)
	}
	return nil, results, nil
}

func (s *Server) Run() error {
	return s.mcpServer.Run(context.Background(), &mcp.StdioTransport{})
}
