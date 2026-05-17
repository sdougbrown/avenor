package mcpserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/client"
)

type ControlClient interface {
	Status(runtimeID string) (map[string]any, error)
	List() ([]map[string]any, error)
	Spawn(params map[string]any) (map[string]any, error)
	Shutdown(mode string) error
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
	opts                  Options
	mcpServer             *mcp.Server
	controlClient         ControlClient
	lifecycle             *supervisorLifecycle
	registry              *RunRegistry
	defaultSupervisorPath string
}

type statusArgs struct {
	RunID        string `json:"run_id,omitempty" jsonschema:"optional run ID or label to query"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type spawnArgs struct {
	Agent        string `json:"agent" jsonschema:"required agent to run"`
	RepoDir      string `json:"repo_dir" jsonschema:"required path to the repository"`
	Prompt       string `json:"prompt,omitempty" jsonschema:"optional initial prompt"`
	PromptFile   string `json:"prompt_file,omitempty" jsonschema:"optional path to file containing the initial prompt"`
	Label        string `json:"label,omitempty" jsonschema:"optional label for the run"`
	Timeout      string `json:"timeout,omitempty" jsonschema:"optional timeout in seconds (numeric string)"`
	Model        string `json:"model,omitempty" jsonschema:"optional model to use"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type shutdownArgs struct {
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
	Force        bool   `json:"force,omitempty" jsonschema:"optional force kill instead of graceful shutdown"`
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
		registry:      NewRunRegistry(),
	}

	if opts.SupervisorSocket != "" && opts.ControlClient == nil {
		cl, err := client.Dial(opts.SupervisorSocket)
		if err != nil {
			return nil, fmt.Errorf("dial supervisor socket: %w", err)
		}
		s.controlClient = cl
		s.defaultSupervisorPath = opts.SupervisorSocket
	}

	if opts.SupervisorSocket == "" && !opts.NoAutostart && opts.ControlClient == nil {
		lc, err := startSupervisor("", opts.IdleTimeout)
		if err != nil {
			return nil, fmt.Errorf("autostart supervisor: %w", err)
		}
		s.lifecycle = lc
		s.controlClient = lc.client
		s.defaultSupervisorPath = lc.socketPath
	}

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_status",
		Description: "Get status of avenor runs",
	}, s.handleAvenorStatus)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_spawn",
		Description: "Spawn a new avenor run",
	}, s.handleAvenorSpawn)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_shutdown",
		Description: "Shutdown the avenor supervisor and clean up run artifacts",
	}, s.handleAvenorShutdown)

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
	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	if args.RunID == "" {
		results, err := cl.List()
		if err != nil {
			return nil, nil, fmt.Errorf("list runs: %w", err)
		}
		translated := make([]map[string]any, 0, len(results))
		for _, entry := range results {
			runtimeID, _ := entry["runtime_id"].(string)
			ri := s.findRegistryByRuntimeID(runtimeID)
			var sentinelPath string
			if ri != nil {
				sentinelPath = ri.SentinelPath
			}
			ts := translateStatus(entry, sentinelPath)
			if len(ts) > 0 {
				if ri != nil {
					ts["run_id"] = ri.RunID
					ts["label"] = ri.Label
				}
				translated = append(translated, ts)
			}
		}
		return nil, translated, nil
	}

	ri := s.registry.Lookup(args.RunID)
	if ri != nil {
		result, err := cl.Status(ri.RuntimeID)
		if err != nil {
			return nil, nil, fmt.Errorf("status: %w", err)
		}
		ts := translateStatus(result, ri.SentinelPath)
		ts["run_id"] = ri.RunID
		ts["label"] = ri.Label
		return nil, ts, nil
	}

	result, err := cl.Status(args.RunID)
	if err != nil {
		return nil, nil, fmt.Errorf("status: %w", err)
	}
	ts := translateStatus(result, "")
	return nil, ts, nil
}

func (s *Server) handleAvenorSpawn(ctx context.Context, req *mcp.CallToolRequest, args spawnArgs) (*mcp.CallToolResult, any, error) {
	if args.Agent == "" {
		return nil, nil, fmt.Errorf("agent is required")
	}
	if args.RepoDir == "" {
		return nil, nil, fmt.Errorf("repo_dir is required")
	}

	runID := uuid.New().String()
	label := args.Label
	if label == "" {
		label = runID
	}

	sentinelPath := filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.done", runID))
	eventLogPath := filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.log", runID))

	params := map[string]any{
		"dir":           args.RepoDir,
		"agent":         args.Agent,
		"label":         label,
		"sentinel_file": sentinelPath,
		"on_event":      eventLogPath,
	}

	if args.Prompt != "" {
		params["prompt"] = args.Prompt
	}
	if args.PromptFile != "" {
		params["prompt_file"] = args.PromptFile
	}
	if args.Model != "" {
		params["model"] = args.Model
	}
	if args.Timeout != "" {
		secs, err := strconv.Atoi(args.Timeout)
		if err != nil {
			return nil, nil, fmt.Errorf("timeout must be a number of seconds, got: %s", args.Timeout)
		}
		params["timeout"] = secs
	}

	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	result, err := cl.Spawn(params)
	if err != nil {
		return nil, nil, fmt.Errorf("spawn: %w", err)
	}

	runtimeID, _ := result["runtime_id"].(string)
	sessionID, _ := result["session_id"].(string)

	supervisorPath := s.getSupervisorPath(args.SupervisorID)

	s.registry.Store(&RunInfo{
		RunID:        runID,
		Label:        label,
		RuntimeID:    runtimeID,
		SessionID:    sessionID,
		SupervisorID: supervisorPath,
		SentinelPath: sentinelPath,
		EventLogPath: eventLogPath,
		Agent:        args.Agent,
		Dir:          args.RepoDir,
		CreatedAt:    time.Now(),
	})

	return nil, map[string]any{
		"run_id":        runID,
		"label":         label,
		"supervisor_id": supervisorPath,
	}, nil
}

func (s *Server) handleAvenorShutdown(ctx context.Context, req *mcp.CallToolRequest, args shutdownArgs) (*mcp.CallToolResult, any, error) {
	mode := "graceful"
	if args.Force {
		mode = "kill"
	}

	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	supervisorPath := s.getSupervisorPath(args.SupervisorID)

	if err := cl.Shutdown(mode); err != nil {
		return nil, nil, fmt.Errorf("shutdown: %w", err)
	}

	var cleanedUp []string
	for _, ri := range s.registry.All() {
		if ri.SupervisorID == supervisorPath {
			s.registry.Remove(ri.RunID)
			if _, statErr := os.Stat(ri.SentinelPath); statErr == nil {
				if rmErr := os.Remove(ri.SentinelPath); rmErr == nil {
					cleanedUp = append(cleanedUp, ri.SentinelPath)
				}
			}
			if _, statErr := os.Stat(ri.EventLogPath); statErr == nil {
				if rmErr := os.Remove(ri.EventLogPath); rmErr == nil {
					cleanedUp = append(cleanedUp, ri.EventLogPath)
				}
			}
		}
	}

	if args.SupervisorID == "" && s.lifecycle != nil {
		_ = s.lifecycle.Shutdown()
		s.lifecycle = nil
		s.controlClient = nil
	}

	return nil, map[string]any{
		"ok":         true,
		"cleaned_up": cleanedUp,
	}, nil
}

func (s *Server) getClientForSupervisor(supervisorID string) (ControlClient, func(), error) {
	if supervisorID == "" {
		if s.controlClient == nil {
			return nil, nil, fmt.Errorf("control client not available")
		}
		return s.controlClient, func() {}, nil
	}
	cl, err := client.Dial(supervisorID)
	if err != nil {
		return nil, nil, fmt.Errorf("dial supervisor socket %s: %w", supervisorID, err)
	}
	return cl, func() { cl.Close() }, nil
}

func (s *Server) getSupervisorPath(supervisorID string) string {
	if supervisorID != "" {
		return supervisorID
	}
	return s.defaultSupervisorPath
}

func (s *Server) findRegistryByRuntimeID(runtimeID string) *RunInfo {
	for _, ri := range s.registry.All() {
		if ri.RuntimeID == runtimeID {
			return ri
		}
	}
	return nil
}

func (s *Server) Run() error {
	return s.mcpServer.Run(context.Background(), &mcp.StdioTransport{})
}
