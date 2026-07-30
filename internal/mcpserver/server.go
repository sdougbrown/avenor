package mcpserver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type ControlClient interface {
	Status(runtimeID string) (map[string]any, error)
	List() ([]map[string]any, error)
	Spawn(params map[string]any) (map[string]any, error)
	Shutdown(mode string) error
	Close() error
	AnswerPermission(runtimeID, requestID, optionID string) error
}

type messagePermissionControlClient interface {
	AnswerPermissionWithMessage(runtimeID, requestID, optionID, message string) error
}

type Options struct {
	Transport        string
	ControlSocket    string
	SupervisorSocket string
	NoAutostart      bool
	IdleTimeout      time.Duration
	Addr             string
	AuthToken        string
	ControlClient    ControlClient
}

type Server struct {
	opts                  Options
	mcpServer             *mcp.Server
	controlClient         ControlClient
	lifecycle             *supervisorLifecycle
	registry              *RunRegistry
	defaultSupervisorPath string
	toolNames             []string
	clock                 func() time.Time

	// supervisorMu serializes lazy default-supervisor acquisition. The
	// persistent client is also the connection that owns mutating operations
	// in the control plane, so concurrent MCP calls must share it.
	supervisorMu sync.Mutex
	closed       bool
}

type statusArgs struct {
	RunID        string `json:"run_id,omitempty" jsonschema:"optional run ID or label to query"`
	View         string `json:"view,omitempty" jsonschema:"optional response detail: lifecycle or full"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type resultArgs struct {
	RunID        string `json:"run_id" jsonschema:"required run ID or label"`
	Wait         *bool  `json:"wait,omitempty" jsonschema:"optional wait for a terminal result (default true)"`
	Timeout      string `json:"timeout,omitempty" jsonschema:"optional maximum time to wait (for example 30s or 5m)"`
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
	Backend      string `json:"backend,omitempty" jsonschema:"optional runtime backend (for example agy, pi, opencode-acp, or codex-app-server)"`
	ServerURL    string `json:"server_url,omitempty" jsonschema:"optional opencode serve URL for opencode-http backend"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
	AutoApprove  bool   `json:"auto_approve,omitempty" jsonschema:"optional auto-approve all permission requests so the run executes unattended (no answer_permission needed)"`
}

type shutdownArgs struct {
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
	Force        bool   `json:"force,omitempty" jsonschema:"optional force kill instead of graceful shutdown"`
}

type permissionArgs struct {
	RunID        string `json:"run_id" jsonschema:"required run ID or label"`
	OptionID     string `json:"option_id" jsonschema:"required option ID to select"`
	RequestID    string `json:"request_id,omitempty" jsonschema:"optional request ID (auto-detected if omitted)"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
	Message      string `json:"message,omitempty" jsonschema:"optional write-in response text"`
}

func (p *permissionArgs) UnmarshalJSON(data []byte) error {
	var wire struct {
		RunID        string          `json:"run_id"`
		OptionID     string          `json:"option_id"`
		RequestID    string          `json:"request_id"`
		SupervisorID string          `json:"supervisor_id"`
		Message      json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	message, err := runtime.DecodePermissionMessageJSON(wire.Message)
	if err != nil {
		return err
	}
	p.RunID, p.OptionID, p.Message = wire.RunID, wire.OptionID, message
	p.RequestID, p.SupervisorID = wire.RequestID, wire.SupervisorID
	return nil
}

type eventsArgs struct {
	RunID        string   `json:"run_id" jsonschema:"required run ID or label"`
	Types        []string `json:"types,omitempty" jsonschema:"optional event types to filter by"`
	Limit        int      `json:"limit,omitempty" jsonschema:"optional max events to return (default 50)"`
	SupervisorID string   `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type followUpArgs struct {
	RunID        string `json:"run_id" jsonschema:"required prior run ID or label"`
	Message      string `json:"message" jsonschema:"required follow-up message"`
	Label        string `json:"label,omitempty" jsonschema:"optional label for the new run (defaults to <prior-label>-followup)"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

func NewServer(opts Options) (*Server, error) {
	if opts.Transport == "" {
		return nil, fmt.Errorf("transport is required")
	}
	if opts.Transport != "stdio" && opts.Transport != "http" {
		return nil, fmt.Errorf("unsupported transport: %s", opts.Transport)
	}
	if opts.Transport == "http" && strings.TrimSpace(opts.AuthToken) == "" {
		return nil, fmt.Errorf("--transport http requires MCP_AUTH_TOKEN or --auth-token")
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
		clock:         time.Now,
		toolNames: []string{
			"avenor_status",
			"avenor_result",
			"avenor_spawn",
			"avenor_shutdown",
			"avenor_answer_permission",
			"avenor_events",
			"avenor_follow_up",
		},
	}

	// Explicit sockets retain their eager, no-autostart dial semantics. The
	// default autostart path is intentionally acquired by the first tool call
	// so constructing an MCP server does not race other constructors.
	if opts.SupervisorSocket != "" {
		s.defaultSupervisorPath = opts.SupervisorSocket
	}
	if opts.SupervisorSocket != "" && opts.ControlClient == nil {
		cl, err := client.Dial(opts.SupervisorSocket)
		if err != nil {
			return nil, fmt.Errorf("dial supervisor socket: %w", err)
		}
		s.controlClient = cl
	}

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_status",
		Description: "Get lifecycle status of avenor runs; use lifecycle view for compact polling",
	}, s.handleAvenorStatus)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_result",
		Description: "Wait for a run to finish and return its complete final output",
	}, s.handleAvenorResult)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_spawn",
		Description: "Spawn a new avenor run",
	}, s.handleAvenorSpawn)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_shutdown",
		Description: "Shutdown the avenor supervisor and clean up run artifacts",
	}, s.handleAvenorShutdown)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_answer_permission",
		Description: "Answer a pending permission request for a run",
	}, s.handleAvenorAnswerPermission)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_events",
		Description: "Read recent events from a run's event log",
	}, s.handleAvenorEvents)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_follow_up",
		Description: "Spawn a follow-up run continuing a prior session",
	}, s.handleAvenorFollowUp)

	return s, nil
}

func (s *Server) Close() error {
	s.supervisorMu.Lock()
	lifecycle := s.lifecycle
	controlClient := s.controlClient
	s.lifecycle = nil
	s.controlClient = nil
	s.closed = true
	s.supervisorMu.Unlock()

	if lifecycle != nil {
		return lifecycle.Close()
	}
	if controlClient != nil {
		return controlClient.Close()
	}
	return nil
}

func (s *Server) handleAvenorStatus(ctx context.Context, req *mcp.CallToolRequest, args statusArgs) (*mcp.CallToolResult, any, error) {
	if args.View != "" && args.View != "lifecycle" && args.View != "full" {
		return nil, nil, fmt.Errorf("view must be lifecycle or full")
	}

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
				translated = append(translated, shapeStatusForView(ts, args.View))
			}
		}
		return nil, translated, nil
	}

	ts, err := s.queryRunStatus(cl, args.RunID)
	if err != nil {
		return nil, nil, err
	}
	return nil, shapeStatusForView(ts, args.View), nil
}

func (s *Server) queryRunStatus(cl ControlClient, runID string) (map[string]any, error) {
	ri := s.registry.Lookup(runID)
	if ri != nil {
		result, err := cl.Status(ri.RuntimeID)
		if err != nil {
			return nil, fmt.Errorf("status: %w", err)
		}
		ts := translateStatus(result, ri.SentinelPath)
		ts["run_id"] = ri.RunID
		ts["label"] = ri.Label
		return ts, nil
	}

	result, err := cl.Status(runID)
	if err != nil {
		return nil, fmt.Errorf("status: %w", err)
	}
	ts := translateStatus(result, "")
	ts["run_id"] = runID
	if _, ok := ts["label"]; !ok {
		ts["label"] = runID
	}
	return ts, nil
}

func shapeStatusForView(status map[string]any, view string) map[string]any {
	if view == "" || view == "full" {
		return status
	}

	result := make(map[string]any)
	for _, key := range []string{"run_id", "label", "status", "runtime_id", "phase", "phase_label", "pending_permission", "latest_seq"} {
		if value, ok := status[key]; ok {
			result[key] = value
		}
	}
	return result
}

func resultFromStatus(status map[string]any, timedOut bool) map[string]any {
	state, _ := status["status"].(string)
	ready := state == "done" || state == "failed" || state == "timeout" || state == "killed"
	result := map[string]any{
		"run_id": status["run_id"],
		"label":  status["label"],
		"status": state,
		"ready":  ready,
	}
	for _, key := range []string{"runtime_id", "session_id", "stop_reason", "pending_permission"} {
		if value, ok := status[key]; ok {
			result[key] = value
		}
	}
	if ready {
		if output, ok := status["final_output"]; ok {
			result["output"] = output
		}
		if truncated, _ := status["final_output_truncated"].(bool); truncated {
			result["output_truncated"] = true
			if eventPath, _ := status["event_path"].(string); eventPath != "" {
				result["output_event_path"] = eventPath
			}
		}
	}
	if timedOut {
		result["timed_out"] = true
	}
	return result
}

// recoverFinalOutput reads the durable terminal event when an older control
// server does not implement the explicit result method.
func (s *Server) recoverFinalOutput(runID string) (string, bool) {
	ri := s.registry.Lookup(runID)
	if ri == nil || ri.EventLogPath == "" {
		return "", false
	}
	output, found, err := readFinalOutput(ri.EventLogPath)
	if err != nil {
		return "", false
	}
	return output, found
}

func (s *Server) resultSupervisorID(runID, requestedSupervisorID string) string {
	if requestedSupervisorID != "" {
		return requestedSupervisorID
	}
	if ri := s.registry.Lookup(runID); ri != nil {
		return ri.SupervisorID
	}
	return ""
}

func (s *Server) handleAvenorResult(ctx context.Context, req *mcp.CallToolRequest, args resultArgs) (*mcp.CallToolResult, any, error) {
	if args.RunID == "" {
		return nil, nil, fmt.Errorf("run_id is required")
	}

	wait := args.Wait == nil || *args.Wait
	var deadline time.Time
	if args.Timeout != "" {
		seconds, err := parseTimeoutSeconds(args.Timeout)
		if err != nil {
			return nil, nil, err
		}
		deadline = s.clock().Add(time.Duration(seconds) * time.Second)
	}

	supervisorID := s.resultSupervisorID(args.RunID, args.SupervisorID)
	cl, cleanup, err := s.getClientForSupervisor(supervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	var pollTimer *time.Timer
	defer func() {
		if pollTimer != nil && !pollTimer.Stop() {
			select {
			case <-pollTimer.C:
			default:
			}
		}
	}()

	for {
		status, err := s.queryRunStatus(cl, args.RunID)
		if err != nil {
			return nil, nil, err
		}
		state, _ := status["status"].(string)
		terminal := state == "done" || state == "failed" || state == "timeout" || state == "killed"
		if terminal || state == "waiting" || !wait {
			if terminal {
				// Status intentionally carries only a bounded preview. Ask the
				// control plane's explicit result method for the lossless reply.
				fullResultRetrieved := false
				if results, ok := cl.(interface {
					Result(string) (map[string]any, error)
				}); ok {
					runtimeID, _ := status["runtime_id"].(string)
					if result, err := results.Result(runtimeID); err == nil {
						if output, ok := result["final_output"].(string); ok {
							status["final_output"] = output
							status["final_output_truncated"] = false
							fullResultRetrieved = true
						}
					}
				}
				if !fullResultRetrieved {
					if output, found := s.recoverFinalOutput(args.RunID); found {
						status["final_output"] = output
						status["final_output_truncated"] = false
						fullResultRetrieved = true
					}
				}
				// Older supervisors have no result RPC and did not report whether
				// their status preview was bounded. Never present that fallback as
				// complete when neither lossless source was available.
				if !fullResultRetrieved {
					if _, ok := status["final_output"].(string); ok {
						status["final_output_truncated"] = true
					}
				}
			}
			return nil, resultFromStatus(status, false), nil
		}
		if !deadline.IsZero() && !s.clock().Before(deadline) {
			return nil, resultFromStatus(status, true), nil
		}

		delay := time.Second
		if !deadline.IsZero() {
			remaining := deadline.Sub(s.clock())
			if remaining < delay {
				delay = remaining
			}
		}
		if delay <= 0 {
			return nil, resultFromStatus(status, true), nil
		}

		if pollTimer == nil {
			pollTimer = time.NewTimer(delay)
		} else {
			pollTimer.Reset(delay)
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-pollTimer.C:
		}
	}
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
	if args.Backend != "" {
		params["backend"] = args.Backend
	}
	if args.AutoApprove {
		params["auto_approve"] = true
	}
	if args.ServerURL != "" {
		params["server_url"] = args.ServerURL
	}
	if args.Timeout != "" {
		secs, err := parseTimeoutSeconds(args.Timeout)
		if err != nil {
			return nil, nil, err
		}
		params["timeout"] = secs
	}

	cl, cleanup, supervisorPath, err := s.getClientForSupervisorWithPath(args.SupervisorID)
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

	if err := s.registry.Store(&RunInfo{
		RunID:        runID,
		Label:        label,
		RuntimeID:    runtimeID,
		SessionID:    sessionID,
		SupervisorID: supervisorPath,
		SentinelPath: sentinelPath,
		EventLogPath: eventLogPath,
		Agent:        args.Agent,
		Backend:      args.Backend,
		Dir:          args.RepoDir,
		AutoApprove:  args.AutoApprove,
		CreatedAt:    time.Now(),
	}); err != nil {
		return nil, nil, fmt.Errorf("registry store: %w", err)
	}

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

	// Acquire lazily before deciding whether this is our owned lifecycle. This
	// makes shutdown on a cold MCP server behave like the other tools while
	// still allowing the owner connection to perform the shutdown.
	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	s.supervisorMu.Lock()
	lifecycle := s.lifecycle
	defaultSupervisorPath := s.defaultSupervisorPath
	supervisorPath := args.SupervisorID
	if supervisorPath == "" {
		supervisorPath = defaultSupervisorPath
	}
	s.supervisorMu.Unlock()
	useLifecycle := lifecycle != nil &&
		(args.SupervisorID == "" || supervisorPath == defaultSupervisorPath)
	if useLifecycle {
		// Shutdown may report an RPC error after it has closed the client and
		// waited for its child. Retire the owned lifecycle either way so a
		// closed connection is never retained for another tool call.
		shutdownErr := lifecycle.ShutdownWithMode(mode)
		s.supervisorMu.Lock()
		s.lifecycle = nil
		s.controlClient = nil
		s.closed = true
		s.supervisorMu.Unlock()
		if shutdownErr != nil {
			return nil, nil, fmt.Errorf("shutdown: %w", shutdownErr)
		}
	} else if err := cl.Shutdown(mode); err != nil {
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

	return nil, map[string]any{
		"ok":         true,
		"cleaned_up": cleanedUp,
	}, nil
}

func (s *Server) handleAvenorAnswerPermission(ctx context.Context, req *mcp.CallToolRequest, args permissionArgs) (*mcp.CallToolResult, any, error) {
	if err := runtime.ValidatePermissionMessage(args.Message); err != nil {
		return nil, nil, err
	}
	ri := s.registry.Lookup(args.RunID)
	supervisorID := args.SupervisorID
	if supervisorID == "" && ri != nil {
		supervisorID = ri.SupervisorID
	}

	cl, cleanup, err := s.getClientForSupervisor(supervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	runtimeID := ""
	if ri != nil {
		runtimeID = ri.RuntimeID
	} else {
		runtimeID, err = resolveRuntimeIDFromList(cl, args.RunID)
		if err != nil {
			return nil, nil, err
		}
	}

	requestID := args.RequestID
	if requestID == "" {
		statusResult, err := cl.Status(runtimeID)
		if err != nil {
			return nil, nil, fmt.Errorf("status: %w", err)
		}
		// The supervisor reports pending_permission as a bool and carries the
		// request details (including request_id) in a separate "permission" map.
		if pending, _ := statusResult["pending_permission"].(bool); !pending {
			return nil, nil, fmt.Errorf("no pending permission request")
		}
		perm, ok := statusResult["permission"].(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("pending permission has no request details in status")
		}
		requestID, _ = perm["request_id"].(string)
		if requestID == "" {
			return nil, nil, fmt.Errorf("pending permission missing request_id")
		}
	}

	var answerErr error
	if args.Message == "" {
		answerErr = cl.AnswerPermission(runtimeID, requestID, args.OptionID)
	} else if messageClient, ok := cl.(messagePermissionControlClient); ok {
		answerErr = messageClient.AnswerPermissionWithMessage(runtimeID, requestID, args.OptionID, args.Message)
	} else {
		answerErr = errors.New("control client does not support permission write-ins")
	}
	if answerErr != nil {
		return nil, nil, fmt.Errorf("answer_permission: %w", answerErr)
	}

	return nil, map[string]any{"ok": true}, nil
}

func resolveRuntimeIDFromList(cl ControlClient, runID string) (string, error) {
	results, err := cl.List()
	if err != nil {
		return "", fmt.Errorf("list runs: %w", err)
	}
	for _, entry := range results {
		runtimeID, _ := entry["runtime_id"].(string)
		label, _ := entry["label"].(string)
		sessionID, _ := entry["session_id"].(string)
		if runID == runtimeID || runID == label || runID == sessionID {
			if runtimeID == "" {
				return "", fmt.Errorf("run %q matched but has no runtime_id", runID)
			}
			return runtimeID, nil
		}
	}
	return "", fmt.Errorf("run %q not found", runID)
}

func (s *Server) handleAvenorEvents(ctx context.Context, req *mcp.CallToolRequest, args eventsArgs) (*mcp.CallToolResult, any, error) {
	ri := s.registry.Lookup(args.RunID)
	if ri == nil {
		return nil, nil, fmt.Errorf("run not found in registry")
	}

	limit := args.Limit
	if limit <= 0 {
		limit = 50
	}

	events, err := readEvents(ri.EventLogPath, args.Types, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("read events: %w", err)
	}
	if events == nil {
		events = []map[string]any{}
	}

	return nil, map[string]any{"events": events}, nil
}

func (s *Server) handleAvenorFollowUp(ctx context.Context, req *mcp.CallToolRequest, args followUpArgs) (*mcp.CallToolResult, any, error) {
	ri := s.registry.Lookup(args.RunID)
	if ri == nil {
		return nil, nil, fmt.Errorf("run not found in registry")
	}

	supervisorID := args.SupervisorID
	if supervisorID == "" {
		supervisorID = ri.SupervisorID
	}

	sessionID, err := readSentinelSession(ri.SentinelPath)
	if err != nil {
		// A supervisor returns session_id when it creates the runtime, before a
		// terminal sentinel exists. Preserve failed/non-resumable sentinels as
		// errors, but allow that durable registry value to cover a missing file.
		if !errors.Is(err, os.ErrNotExist) || ri.SessionID == "" {
			return nil, nil, fmt.Errorf("read sentinel session: %w", err)
		}
		sessionID = ri.SessionID
	}

	runID := uuid.New().String()
	followupLabel := args.Label
	if followupLabel == "" {
		followupLabel = ri.Label + "-followup"
	}

	sentinelPath := filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.done", runID))
	eventLogPath := filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.log", runID))

	params := map[string]any{
		"dir":           ri.Dir,
		"agent":         ri.Agent,
		"prompt":        args.Message,
		"label":         followupLabel,
		"session_id":    sessionID,
		"sentinel_file": sentinelPath,
		"on_event":      eventLogPath,
	}
	if ri.Backend != "" {
		params["backend"] = ri.Backend
	}
	if ri.AutoApprove {
		params["auto_approve"] = true
	}

	cl, cleanup, err := s.getClientForSupervisor(supervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	result, err := cl.Spawn(params)
	if err != nil {
		return nil, nil, fmt.Errorf("spawn: %w", err)
	}

	runtimeID, _ := result["runtime_id"].(string)
	newSessionID, _ := result["session_id"].(string)

	supervisorPath := s.getSupervisorPath(supervisorID)

	if err := s.registry.Store(&RunInfo{
		RunID:        runID,
		Label:        followupLabel,
		RuntimeID:    runtimeID,
		SessionID:    newSessionID,
		SupervisorID: supervisorPath,
		SentinelPath: sentinelPath,
		EventLogPath: eventLogPath,
		Agent:        ri.Agent,
		Backend:      ri.Backend,
		Dir:          ri.Dir,
		AutoApprove:  ri.AutoApprove,
		CreatedAt:    time.Now(),
	}); err != nil {
		return nil, nil, fmt.Errorf("registry store: %w", err)
	}

	return nil, map[string]any{
		"run_id": runID,
		"label":  followupLabel,
	}, nil
}

var startSupervisorFunc = startSupervisor

// beforeSupervisorLock is a no-op production hook used to coordinate callers
// at the lazy-supervisor lock boundary in concurrency tests.
var beforeSupervisorLock = func() {}

func (s *Server) getClientForSupervisor(supervisorID string) (ControlClient, func(), error) {
	cl, cleanup, _, err := s.getClientForSupervisorWithPath(supervisorID)
	return cl, cleanup, err
}

func (s *Server) getClientForSupervisorWithPath(supervisorID string) (ControlClient, func(), string, error) {
	// An explicit supervisor_id that resolves to the autostarted/default
	// supervisor must reuse the persistent owner connection. Ownership is
	// per-connection (first mutator wins), and spawn claimed it on
	// s.controlClient — dialing a fresh connection here would fail ensureOwner
	// on mutating calls (answer_permission, prompt, cancel, follow_up), since
	// those resolve supervisor_id from the registry and would otherwise arrive
	// on a non-owner connection. Mirrors handleAvenorShutdown's path check.
	beforeSupervisorLock()
	s.supervisorMu.Lock()
	if s.closed {
		s.supervisorMu.Unlock()
		return nil, nil, "", fmt.Errorf("control client not available")
	}
	isDefault := supervisorID == "" || supervisorID == s.defaultSupervisorPath
	if !isDefault {
		s.supervisorMu.Unlock()
		cl, err := client.Dial(supervisorID)
		if err != nil {
			return nil, nil, "", fmt.Errorf("dial supervisor socket %s: %w", supervisorID, err)
		}
		return cl, func() { cl.Close() }, supervisorID, nil
	}
	defer s.supervisorMu.Unlock()

	if s.controlClient == nil {
		if s.opts.NoAutostart {
			return nil, nil, "", fmt.Errorf("control client not available")
		}
		lc, err := startSupervisorFunc(s.opts.ControlSocket, s.opts.IdleTimeout)
		if err != nil {
			return nil, nil, "", fmt.Errorf("autostart supervisor: %w", err)
		}
		s.lifecycle = lc
		s.controlClient = lc.client
		s.defaultSupervisorPath = lc.socketPath
	}
	return s.controlClient, func() {}, s.defaultSupervisorPath, nil
}

func (s *Server) getSupervisorPath(supervisorID string) string {
	if supervisorID != "" {
		return supervisorID
	}
	s.supervisorMu.Lock()
	defer s.supervisorMu.Unlock()
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

func (s *Server) RunHTTP(addr string) error {
	return http.ListenAndServe(addr, s.HTTPHandler())
}

func (s *Server) HTTPHandler() http.Handler {
	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return s.mcpServer
	}, &mcp.StreamableHTTPOptions{Stateless: true})
	return s.authenticatedHTTPHandler(handler)
}

func (s *Server) authenticatedHTTPHandler(next http.Handler) http.Handler {
	token := strings.TrimSpace(s.opts.AuthToken)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isAllowedHTTPHost(r.Host) || !isAllowedHTTPOrigin(r.Header.Get("Origin")) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if !bearerTokenMatches(r.Header.Get("Authorization"), token) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

var timeoutRE = regexp.MustCompile(`^(\d+)([smh]?)$`)

func parseTimeoutSeconds(value string) (int, error) {
	trimmed := strings.TrimSpace(value)
	match := timeoutRE.FindStringSubmatch(trimmed)
	if match == nil {
		return 0, fmt.Errorf("invalid timeout: %s", value)
	}
	amount, err := strconv.Atoi(match[1])
	if err != nil || amount <= 0 {
		return 0, fmt.Errorf("invalid timeout: %s", value)
	}
	switch match[2] {
	case "m":
		return amount * 60, nil
	case "h":
		return amount * 3600, nil
	default:
		return amount, nil
	}
}

func bearerTokenMatches(header, want string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(strings.ToLower(header), strings.ToLower(prefix)) {
		return false
	}
	got := strings.TrimSpace(header[len(prefix):])
	if got == "" || want == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func isAllowedHTTPOrigin(origin string) bool {
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return u.Scheme == "http" && isLoopbackHost(u.Hostname())
}

func isAllowedHTTPHost(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	return isLoopbackHost(strings.Trim(host, "[]"))
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) RegisteredToolNames() []string {
	names := make([]string, len(s.toolNames))
	copy(names, s.toolNames)
	return names
}
