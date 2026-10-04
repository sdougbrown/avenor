package mcpserver

import (
	"bytes"
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
	"github.com/sdougbrown/avenor/internal/spawnselection"
)

type ControlClient interface {
	Status(runtimeID string) (map[string]any, error)
	List() ([]map[string]any, error)
	Spawn(params map[string]any) (map[string]any, error)
	Shutdown(mode string) error
	Close() error
	Closed() bool
	AnswerPermission(runtimeID, requestID, optionID string) error
	WorkflowStatus(workflowID string) (map[string]any, error)
	WorkflowWait(workflowID string, timeout time.Duration) (map[string]any, error)
	WorkflowInspect(workflowID string) (map[string]any, error)
	WorkflowEvents(workflowID string, afterSeq int64, limit int) (map[string]any, error)
	WorkflowComplete(workflowID string, fields map[string]any) (map[string]any, error)
	WorkflowGate(workflowID string, fields map[string]any) (map[string]any, error)
	WorkflowControllerStatus(controllerID string) (map[string]any, error)
	WorkflowControllerList() (map[string]any, error)
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
	MaxWait          time.Duration
	AllowedHosts     []string
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
	sleep                 func(context.Context, time.Duration) error

	// supervisorMu serializes lazy default-supervisor acquisition. The
	// persistent client is also the connection that owns mutating operations
	// in the control plane, so concurrent MCP calls must share it.
	supervisorMu sync.Mutex
	closed       bool
}

type statusArgs struct {
	RunID        string `json:"run_id,omitempty" jsonschema:"optional run ID or label to query"`
	View         string `json:"view,omitempty" jsonschema:"optional response detail: lifecycle or full"`
	WaitFor      string `json:"wait_for,omitempty" jsonschema:"optional wait condition: terminal, phase_change, turn_complete, or permission"`
	Timeout      string `json:"timeout,omitempty" jsonschema:"optional maximum wait time, such as 30s, 5m, or 1h"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type resultArgs struct {
	RunID        string `json:"run_id" jsonschema:"required run ID or label"`
	Wait         *bool  `json:"wait,omitempty" jsonschema:"optional wait for a terminal result (default true)"`
	Timeout      string `json:"timeout,omitempty" jsonschema:"optional maximum time to wait (for example 30s or 5m)"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type spawnArgs struct {
	Agent        string `json:"agent,omitempty" jsonschema:"optional agent name; omission uses the selected model or runtime defaults"`
	RepoDir      string `json:"repo_dir" jsonschema:"required path to the repository"`
	Prompt       string `json:"prompt,omitempty" jsonschema:"optional initial prompt"`
	PromptFile   string `json:"prompt_file,omitempty" jsonschema:"optional path to file containing the initial prompt"`
	Label        string `json:"label,omitempty" jsonschema:"optional label for the run"`
	Timeout      string `json:"timeout,omitempty" jsonschema:"optional timeout in seconds (numeric string)"`
	Model        string `json:"model,omitempty" jsonschema:"optional model to use"`
	Thinking     string `json:"thinking,omitempty" jsonschema:"optional thinking level (off, minimal, low, medium, high, xhigh, max); omission uses the backend default"`
	Backend      string `json:"backend,omitempty" jsonschema:"optional runtime backend (for example agy, pi, opencode-acp, or codex-app-server)"`
	RosterFile   string `json:"roster_file,omitempty" jsonschema:"optional path to the roster map"`
	RosterEntry  string `json:"roster_entry,omitempty" jsonschema:"optional roster entry key"`
	ServerURL    string `json:"server_url,omitempty" jsonschema:"optional opencode serve URL for opencode-http backend"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
	AutoApprove  bool   `json:"auto_approve,omitempty" jsonschema:"optional auto-approve all permission requests so the run executes unattended (no answer_permission needed)"`
}

// UnmarshalJSON makes the Go MCP spawn tool a strict raw boundary: unknown
// keys — including misspelled or deferred selector keys such as "rosterFile"
// or "system" — are refused instead of silently ignored. Declared provider
// options (thinking, server_url, timeout, ...) remain covered by the struct
// tags and are unaffected.
func (a *spawnArgs) UnmarshalJSON(data []byte) error {
	type alias spawnArgs
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var out alias
	if err := dec.Decode(&out); err != nil {
		return err
	}
	*a = spawnArgs(out)
	return nil
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
	AfterSeq     *int64   `json:"after_seq,omitempty" jsonschema:"optional sequence cursor: return only events with seq > after_seq, oldest-first"`
	SupervisorID string   `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type followUpArgs struct {
	RunID        string `json:"run_id" jsonschema:"required prior run ID or label"`
	Message      string `json:"message" jsonschema:"required follow-up message"`
	Label        string `json:"label,omitempty" jsonschema:"optional label for the new run (defaults to <prior-label>-followup)"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type workflowStatusArgs struct {
	WorkflowID   string `json:"workflow_id" jsonschema:"required workflow ID"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type workflowWaitArgs struct {
	WorkflowID   string `json:"workflow_id" jsonschema:"required workflow ID"`
	Timeout      string `json:"timeout,omitempty" jsonschema:"optional maximum wait time, such as 30s, 5m, or 1h (default 30s)"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type workflowInspectArgs struct {
	WorkflowID   string `json:"workflow_id" jsonschema:"required workflow ID"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type workflowEventsArgs struct {
	WorkflowID   string `json:"workflow_id" jsonschema:"required workflow ID"`
	AfterSeq     int64  `json:"after_seq,omitempty" jsonschema:"optional only events with sequence greater than this value"`
	Limit        int    `json:"limit,omitempty" jsonschema:"optional max events to return (0 for server default)"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type workflowCompleteArgs struct {
	WorkflowID   string          `json:"workflow_id" jsonschema:"required workflow ID"`
	NodeID       string          `json:"node_id" jsonschema:"required node ID"`
	ActivationID string          `json:"activation_id" jsonschema:"required activation ID"`
	AttemptID    string          `json:"attempt_id" jsonschema:"required attempt ID"`
	LeaseID      string          `json:"lease_id" jsonschema:"required lease ID"`
	OwnerToken   string          `json:"owner_token" jsonschema:"required lease owner token"`
	Outcome      string          `json:"outcome" jsonschema:"required declared outcome"`
	Outputs      json.RawMessage `json:"outputs,omitempty" jsonschema:"optional declared outputs as JSON, for example [{\"definition_id\":\"o1\",\"value\":...}]"`
	Artifacts    json.RawMessage `json:"artifacts,omitempty" jsonschema:"optional staged evidence artifacts as JSON, for example [{\"src_path\":\"...\",\"stored_path\":\"...\"}]"`
	SupervisorID string          `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type workflowGateArgs struct {
	WorkflowID   string          `json:"workflow_id" jsonschema:"required workflow ID"`
	NodeID       string          `json:"node_id" jsonschema:"required node ID"`
	GateID       string          `json:"gate_id" jsonschema:"required gate ID"`
	ActivationID string          `json:"activation_id" jsonschema:"required activation ID"`
	Operation    string          `json:"operation" jsonschema:"required gate operation (satisfy, reject, waive, or external_result)"`
	Actor        string          `json:"actor,omitempty" jsonschema:"optional actor id for human gate operations"`
	Reason       string          `json:"reason,omitempty" jsonschema:"optional reason for human gate operations"`
	Outcome      string          `json:"outcome,omitempty" jsonschema:"optional outcome for the gate decision (used to branch reject/fail cases)"`
	Subject      json.RawMessage `json:"subject,omitempty" jsonschema:"optional bound exact subject as JSON, for example {\"type\":\"pr\",\"repository\":\"org/repo\",\"pull_request\":12,\"revision\":\"abc\"}"`
	PollID       string          `json:"poll_id,omitempty" jsonschema:"optional external poll id (external_result)"`
	Source       string          `json:"source,omitempty" jsonschema:"optional external result source (external_result)"`
	Result       string          `json:"result,omitempty" jsonschema:"optional external result status (external_result)"`
	ResponseHash string          `json:"response_hash,omitempty" jsonschema:"optional response hash (external_result)"`
	ObservedAt   string          `json:"observed_at,omitempty" jsonschema:"optional observed-at RFC3339 timestamp (external_result)"`
	EvidenceIDs  []string        `json:"evidence_ids,omitempty" jsonschema:"optional evidence IDs"`
	SupervisorID string          `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

type workflowControllerStatusArgs struct {
	ControllerID string `json:"controller_id,omitempty" jsonschema:"optional controller ID; omit to list all controllers"`
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
		return nil, fmt.Errorf("--transport http requires MCP_AUTH_TOKEN, --auth-token, or --auth-token-file")
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
		sleep:         sleepWithContext,
		toolNames: []string{
			"avenor_status",
			"avenor_result",
			"avenor_spawn",
			"avenor_shutdown",
			"avenor_answer_permission",
			"avenor_events",
			"avenor_follow_up",
			"avenor_workflow_status",
			"avenor_workflow_wait",
			"avenor_workflow_inspect",
			"avenor_workflow_events",
			"avenor_workflow_complete",
			"avenor_workflow_gate",
			"avenor_workflow_controller_status",
		},
	}

	// Explicit sockets keep their socket path for lazy acquisition: the first
	// default-supervisor tool call dials, and redials after a dead connection.
	// The default autostart path is intentionally acquired by the first tool
	// call so constructing an MCP server does not race other constructors.
	if opts.SupervisorSocket != "" {
		s.defaultSupervisorPath = opts.SupervisorSocket
	}

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_status",
		Description: "Get lifecycle status of avenor runs; optionally wait for terminal, phase_change, turn_complete, or permission. Without run_id, returns an object with runs (array of status objects) and count. The wait budget is approximate: a single underlying status poll may add up to its own timeout.",
	}, s.handleAvenorStatus)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_result",
		Description: "Wait for a run to finish and return its complete final output. The wait budget is approximate: a single underlying status poll may add up to its own timeout.",
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

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_workflow_status",
		Description: "Get lightweight status for a workflow instance",
	}, s.handleAvenorWorkflowStatus)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_workflow_wait",
		Description: "Wait for a workflow to reach a terminal state or until timeout. The wait budget is approximate: a single underlying wait may add up to its own timeout.",
	}, s.handleAvenorWorkflowWait)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_workflow_inspect",
		Description: "Return the full instance detail for a workflow",
	}, s.handleAvenorWorkflowInspect)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_workflow_events",
		Description: "Read log events from a workflow instance's event log",
	}, s.handleAvenorWorkflowEvents)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_workflow_complete",
		Description: "Atomically complete a machine/external handoff activation",
	}, s.handleAvenorWorkflowComplete)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_workflow_gate",
		Description: "Record a gate decision on a parked awaiting_gate activation",
	}, s.handleAvenorWorkflowGate)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "avenor_workflow_controller_status",
		Description: "Show workflow controller status. With controller_id, returns that controller's full status; without it, lists all controllers with summary state. Create, enable, and disable are CLI-only (avenor workflow controller ...).",
	}, s.handleAvenorWorkflowControllerStatus)

	return s, nil
}

// clampWait applies the configured wait budget. A requested duration of 0
// means unbounded. Returns the effective wait and whether the budget bit.
func (s *Server) clampWait(requested time.Duration) (time.Duration, bool) {
	if s.opts.MaxWait <= 0 {
		return requested, false
	}
	if requested == 0 || requested > s.opts.MaxWait {
		return s.opts.MaxWait, true
	}
	return requested, false
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

func (s *Server) handleAvenorStatus(ctx context.Context, req *mcp.CallToolRequest, args statusArgs) (*mcp.CallToolResult, statusToolOutput, error) {
	if args.View != "" && args.View != "lifecycle" && args.View != "full" {
		return nil, statusToolOutput{}, fmt.Errorf("view must be lifecycle or full")
	}
	condition, err := parseWaitCondition(args.WaitFor)
	if err != nil {
		return nil, statusToolOutput{}, err
	}
	if condition == "" && args.Timeout != "" {
		return nil, statusToolOutput{}, fmt.Errorf("timeout requires wait_for")
	}
	if condition != "" && args.RunID == "" {
		return nil, statusToolOutput{}, fmt.Errorf("run_id is required when wait_for is set")
	}

	var requested time.Duration
	if args.Timeout != "" {
		seconds, err := parseTimeoutSeconds(args.Timeout)
		if err != nil {
			return nil, statusToolOutput{}, err
		}
		requested = time.Duration(seconds) * time.Second
	}
	effective, clamped := s.clampWait(requested)
	var deadline time.Time
	if effective > 0 {
		deadline = s.clock().Add(effective)
	}

	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, statusToolOutput{}, err
	}
	defer cleanup()
	supervisorPath := s.getSupervisorPath(args.SupervisorID)

	if args.RunID == "" {
		results, err := cl.List()
		if err != nil {
			return nil, statusToolOutput{}, fmt.Errorf("list runs: %w", err)
		}
		translated := make([]map[string]any, 0, len(results))
		seenRegistryRuns := make(map[string]bool)
		for _, entry := range results {
			runtimeID, _ := entry["runtime_id"].(string)
			ri := s.findRegistryByRuntimeID(supervisorPath, runtimeID)
			var sentinelPath string
			if ri != nil {
				sentinelPath = ri.SentinelPath
			}
			ts := translateStatus(entry, sentinelPath)
			if len(ts) > 0 {
				if ri != nil {
					seenRegistryRuns[ri.RunID] = true
					applyRunInfoIdentity(ts, ri)
					ts["run_id"] = ri.RunID
					ts["label"] = ri.Label
				}
				translated = append(translated, shapeStatusForView(ts, args.View))
			}
		}
		for _, ri := range s.registry.All() {
			if seenRegistryRuns[ri.RunID] || ri.SupervisorID != supervisorPath {
				continue
			}
			if ts, ok := terminalStatusFromRunInfo(ri); ok {
				translated = append(translated, shapeStatusForView(ts, args.View))
			}
		}
		runs := make([]statusRun, 0, len(translated))
		for _, ts := range translated {
			runs = append(runs, statusRunFromMap(ts))
		}
		count := len(runs)
		return nil, statusToolOutput{Runs: &runs, Count: &count}, nil
	}

	ri, err := s.lookupRun(cl, supervisorPath, args.RunID)
	if err != nil {
		return nil, statusToolOutput{}, err
	}
	key := args.RunID
	if ri != nil {
		key = ri.RunID
	}
	var timedOut bool
	var ts map[string]any
	if condition != "" {
		ts, timedOut, err = s.waitForRun(ctx, cl, supervisorPath, key, condition, deadline)
	} else {
		ts, err = s.queryRunStatus(cl, supervisorPath, key)
	}
	if err != nil {
		return nil, statusToolOutput{}, err
	}
	if timedOut {
		ts["timed_out"] = true
		if clamped {
			ts["wait_clamped"] = true
		}
	}
	return nil, statusToolOutput{statusRun: statusRunFromMap(shapeStatusForView(ts, args.View))}, nil
}

func terminalStatusFromRunInfo(info *RunInfo) (map[string]any, bool) {
	if info == nil || info.SentinelPath == "" {
		return nil, false
	}
	sd, err := readSentinel(info.SentinelPath)
	if err != nil {
		return nil, false
	}
	switch strings.ToUpper(sd.Status) {
	case "DONE", "FAILED", "BLOCKED", "TIMEOUT", "KILLED":
	default:
		return nil, false
	}
	status := make(map[string]any)
	applySentinelStatus(status, sd)
	applyRunInfoIdentity(status, info)
	if info.RuntimeID != "" {
		status["runtime_id"] = info.RuntimeID
	}
	if info.Dir != "" {
		status["dir"] = info.Dir
	}
	if info.Thinking != "" {
		status["thinking"] = info.Thinking
	}
	status["run_id"] = info.RunID
	status["label"] = info.Label
	return status, true
}

// lookupRun resolves key (an MCP run UUID or a label) against the selected
// supervisor, rehydrating the registry from the supervisor's list on a miss.
// A nil result with a nil error means the list contained no match; callers
// keep their existing not-found behavior. List errors propagate as errors.
func (s *Server) lookupRun(cl ControlClient, supervisorPath, key string) (*RunInfo, error) {
	if ri := s.registry.Lookup(supervisorPath, key); ri != nil {
		return ri, nil
	}
	entries, err := cl.List()
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	var (
		sentinelMatch = -1
		runtimeMatch  = -1
		labelMatches  []int
	)
	for i := range entries {
		entry := entries[i]
		sentinelFile, _ := entry["sentinel_file"].(string)
		if key != "" && filepath.Base(sentinelFile) == "avenor-run-"+key+".done" {
			if sentinelMatch == -1 {
				sentinelMatch = i
			}
			continue
		}
		runtimeID, _ := entry["runtime_id"].(string)
		if key != "" && runtimeID == key {
			if runtimeMatch == -1 {
				runtimeMatch = i
			}
			continue
		}
		label, _ := entry["label"].(string)
		if key != "" && label != "" && label == key {
			labelMatches = append(labelMatches, i)
		}
	}
	switch {
	case sentinelMatch != -1:
		return s.runInfoFromListEntry(entries[sentinelMatch], supervisorPath)
	case runtimeMatch != -1:
		return s.runInfoFromListEntry(entries[runtimeMatch], supervisorPath)
	case len(labelMatches) == 1:
		return s.runInfoFromListEntry(entries[labelMatches[0]], supervisorPath)
	case len(labelMatches) > 1:
		ids := make([]string, 0, len(labelMatches))
		for _, i := range labelMatches {
			id, _ := entries[i]["runtime_id"].(string)
			ids = append(ids, id)
		}
		return nil, fmt.Errorf("ambiguous label %s: matches runtimes %v", key, ids)
	}
	return nil, nil
}

// runIDFromSentinel extracts the MCP run UUID from a sentinel basename shaped
// avenor-run-<uuid>.done, or returns empty for any other name.
func runIDFromSentinel(sentinelFile string) string {
	base := filepath.Base(sentinelFile)
	id := strings.TrimSuffix(strings.TrimPrefix(base, "avenor-run-"), ".done")
	if id == "" || id == base {
		return ""
	}
	if _, err := uuid.Parse(id); err != nil {
		return ""
	}
	return id
}

// runInfoFromListEntry converts a stable supervisor list entry into a registry
// entry scoped to supervisorPath. The supervisor-wide run_id is never used as
// the MCP run ID.
func (s *Server) runInfoFromListEntry(entry map[string]any, supervisorPath string) (*RunInfo, error) {
	runtimeID, _ := entry["runtime_id"].(string)
	sentinelFile, _ := entry["sentinel_file"].(string)
	runID := runIDFromSentinel(sentinelFile)
	if runID == "" {
		runID = runtimeID
	}
	label, _ := entry["label"].(string)
	info := &RunInfo{
		RunID:        runID,
		Label:        label,
		RuntimeID:    runtimeID,
		SupervisorID: supervisorPath,
		SentinelPath: sentinelFile,
		Dir:          stringField(entry, "dir"),
		EventLogPath: stringField(entry, "on_event"),
		Thinking:     stringField(entry, "thinking"),
	}
	info.SessionID = stringField(entry, "session_id")
	info.AutoApprove, _ = entry["auto_approve"].(bool)
	if ms, ok := entry["started_at"].(float64); ok && ms > 0 {
		info.CreatedAt = time.UnixMilli(int64(ms))
	}
	var identity resolvedSpawnIdentity
	applySpawnIdentity(entry, &identity)
	info.Agent = identity.EffectiveAgent
	info.Model = identity.EffectiveModel
	info.Backend = identity.EffectiveBackend
	info.RosterFile = identity.RosterFile
	info.RosterEntry = identity.RosterEntry
	info.EffectiveAgent = identity.EffectiveAgent
	info.EffectiveModel = identity.EffectiveModel
	info.EffectiveBackend = identity.EffectiveBackend
	info.AgentProfile = identity.AgentProfile
	if err := s.registry.Store(info); err != nil {
		return nil, fmt.Errorf("registry store: %w", err)
	}
	return info, nil
}

func stringField(entry map[string]any, key string) string {
	value, _ := entry[key].(string)
	return value
}

func (s *Server) queryRunStatus(cl ControlClient, supervisorPath, runID string) (map[string]any, error) {
	ri := s.registry.Lookup(supervisorPath, runID)
	if ri != nil {
		result, err := cl.Status(ri.RuntimeID)
		if err != nil {
			if ts, ok := terminalStatusFromRunInfo(ri); ok {
				return ts, nil
			}
			return nil, fmt.Errorf("status: %w", err)
		}
		ts := translateStatus(result, ri.SentinelPath)
		applyRunInfoIdentity(ts, ri)
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
	for _, key := range []string{"run_id", "label", "status", "runtime_id", "phase", "phase_label", "pending_permission", "permission", "latest_seq", "timed_out", "wait_clamped"} {
		if value, ok := status[key]; ok {
			result[key] = value
		}
	}
	return result
}

func resultFromStatus(status map[string]any, timedOut bool) map[string]any {
	state, _ := status["status"].(string)
	ready := isTerminalStatus(status) && !hasPendingPermission(status)
	result := map[string]any{
		"run_id": status["run_id"],
		"label":  status["label"],
		"status": state,
		"ready":  ready,
	}
	for _, key := range []string{"runtime_id", "session_id", "stop_reason", "pending_permission", "permission"} {
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
func (s *Server) recoverFinalOutput(supervisorPath, runID string) (string, bool) {
	ri := s.registry.Lookup(supervisorPath, runID)
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
	if ri := s.registry.LookupUnique(runID); ri != nil {
		return ri.SupervisorID
	}
	return ""
}

func (s *Server) retrieveFinalOutput(cl ControlClient, supervisorPath, runID string, status map[string]any) {
	// Status provides a bounded preview only. Call Result for full output when
	// the control plane supports it.
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
		if output, found := s.recoverFinalOutput(supervisorPath, runID); found {
			status["final_output"] = output
			status["final_output_truncated"] = false
			fullResultRetrieved = true
		}
	}
	// Older supervisors lack Result and do not specify whether previews are
	// bounded. Do not present a preview as complete without a full source.
	if !fullResultRetrieved {
		if _, ok := status["final_output"].(string); ok {
			status["final_output_truncated"] = true
		}
	}
}

func (s *Server) handleAvenorResult(ctx context.Context, req *mcp.CallToolRequest, args resultArgs) (*mcp.CallToolResult, any, error) {
	if args.RunID == "" {
		return nil, nil, fmt.Errorf("run_id is required")
	}

	wait := args.Wait == nil || *args.Wait
	var requested time.Duration
	if args.Timeout != "" {
		seconds, err := parseTimeoutSeconds(args.Timeout)
		if err != nil {
			return nil, nil, err
		}
		requested = time.Duration(seconds) * time.Second
	}
	effective, clamped := s.clampWait(requested)
	var deadline time.Time
	if effective > 0 {
		deadline = s.clock().Add(effective)
	}

	supervisorID := s.resultSupervisorID(args.RunID, args.SupervisorID)
	cl, cleanup, err := s.getClientForSupervisor(supervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	supervisorPath := s.getSupervisorPath(supervisorID)

	ri, err := s.lookupRun(cl, supervisorPath, args.RunID)
	if err != nil {
		return nil, nil, err
	}
	key := args.RunID
	if ri != nil {
		key = ri.RunID
	}

	var status map[string]any
	var timedOut bool
	if wait {
		status, timedOut, err = s.waitForRun(ctx, cl, supervisorPath, key, waitTurnComplete, deadline)
	} else {
		status, err = s.queryRunStatus(cl, supervisorPath, key)
	}
	if err != nil {
		return nil, nil, err
	}

	if isTerminalStatus(status) && !hasPendingPermission(status) {
		s.retrieveFinalOutput(cl, supervisorPath, key, status)
	}
	result := resultFromStatus(status, timedOut)
	if clamped && timedOut {
		result["wait_clamped"] = true
	}
	return nil, result, nil
}

type resolvedSpawnIdentity struct {
	Agent            string
	Model            string
	Backend          string
	RosterFile       string
	RosterEntry      string
	EffectiveAgent   string
	EffectiveModel   string
	EffectiveBackend string
	AgentProfile     string
}

func applySpawnIdentity(raw map[string]any, identity *resolvedSpawnIdentity) {
	if raw == nil {
		return
	}
	if value, ok := raw["roster_file"].(string); ok && value != "" {
		identity.RosterFile = value
	}
	if value, ok := raw["roster_entry"].(string); ok && value != "" {
		identity.RosterEntry = value
	}
	if value, ok := raw["effective_agent"].(string); ok && value != "" {
		identity.EffectiveAgent = value
	}
	if value, ok := raw["effective_model"].(string); ok && value != "" {
		identity.EffectiveModel = value
	}
	if value, ok := raw["effective_backend"].(string); ok && value != "" {
		identity.EffectiveBackend = value
	}
	if value, ok := raw["agent_profile"].(string); ok && value != "" {
		identity.AgentProfile = value
	}
	if value, ok := raw["agent"].(string); ok && value != "" {
		identity.Agent = value
	}
	if value, ok := raw["model"].(string); ok && value != "" {
		identity.Model = value
	}
	if value, ok := raw["backend"].(string); ok && value != "" {
		identity.Backend = value
	}
	if identity.EffectiveAgent == "" {
		identity.EffectiveAgent = identity.Agent
	}
	if identity.EffectiveModel == "" {
		identity.EffectiveModel = identity.Model
	}
	if identity.EffectiveBackend == "" {
		identity.EffectiveBackend = identity.Backend
	}
}

func resolveSpawnIdentity(cl ControlClient, runtimeID string, result map[string]any, args spawnArgs) resolvedSpawnIdentity {
	identity := resolvedSpawnIdentity{
		Agent:            args.Agent,
		Model:            args.Model,
		Backend:          args.Backend,
		RosterFile:       args.RosterFile,
		RosterEntry:      args.RosterEntry,
		EffectiveAgent:   args.Agent,
		EffectiveModel:   args.Model,
		EffectiveBackend: args.Backend,
	}
	applySpawnIdentity(result, &identity)
	// SpawnResult is intentionally small on older supervisors. Query status
	// when available so registry metadata records the supervisor's resolved
	// backend/identity rather than re-resolving a roster in this server.
	if runtimeID != "" {
		if status, err := cl.Status(runtimeID); err == nil {
			applySpawnIdentity(status, &identity)
		}
	}
	return identity
}

func (s *Server) handleAvenorSpawn(ctx context.Context, req *mcp.CallToolRequest, args spawnArgs) (*mcp.CallToolResult, any, error) {
	if args.RepoDir == "" {
		return nil, nil, fmt.Errorf("repo_dir is required")
	}
	if err := spawnselection.Validate(spawnselection.Input{
		Agent:       args.Agent,
		Model:       args.Model,
		Backend:     args.Backend,
		RosterFile:  args.RosterFile,
		RosterEntry: args.RosterEntry,
	}, false); err != nil {
		return nil, nil, err
	}
	if err := runtime.ValidateThinking(args.Thinking); err != nil {
		return nil, nil, err
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
		"label":         label,
		"sentinel_file": sentinelPath,
		"on_event":      eventLogPath,
	}

	if args.Agent != "" {
		params["agent"] = args.Agent
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
	if args.Thinking != "" {
		params["thinking"] = args.Thinking
	}
	if args.Backend != "" {
		params["backend"] = args.Backend
	}
	if args.RosterFile != "" {
		params["roster_file"] = args.RosterFile
	}
	if args.RosterEntry != "" {
		params["roster_entry"] = args.RosterEntry
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
	identity := resolveSpawnIdentity(cl, runtimeID, result, args)

	if err := s.registry.Store(&RunInfo{
		RunID:            runID,
		Label:            label,
		RuntimeID:        runtimeID,
		SessionID:        sessionID,
		SupervisorID:     supervisorPath,
		SentinelPath:     sentinelPath,
		EventLogPath:     eventLogPath,
		Agent:            identity.EffectiveAgent,
		Model:            identity.EffectiveModel,
		Backend:          identity.EffectiveBackend,
		RosterFile:       identity.RosterFile,
		RosterEntry:      identity.RosterEntry,
		EffectiveAgent:   identity.EffectiveAgent,
		EffectiveModel:   identity.EffectiveModel,
		EffectiveBackend: identity.EffectiveBackend,
		AgentProfile:     identity.AgentProfile,
		Thinking:         args.Thinking,
		Dir:              args.RepoDir,
		AutoApprove:      args.AutoApprove,
		CreatedAt:        time.Now(),
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
		// closed connection is never retained for another tool call. The
		// server itself stays live: the next default-supervisor tool call
		// autostarts a replacement. Resetting the default path drops the
		// retired autostarted socket instead of reusing it.
		shutdownErr := lifecycle.ShutdownWithMode(mode)
		s.supervisorMu.Lock()
		s.lifecycle = nil
		s.controlClient = nil
		s.defaultSupervisorPath = s.opts.SupervisorSocket
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
			s.registry.Remove(ri.SupervisorID, ri.RunID)
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
	ri := s.registry.LookupUnique(args.RunID)
	supervisorID := args.SupervisorID
	if supervisorID == "" && ri != nil {
		supervisorID = ri.SupervisorID
	}

	cl, cleanup, err := s.getClientForSupervisor(supervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	ri, err = s.lookupRun(cl, s.getSupervisorPath(supervisorID), args.RunID)
	if err != nil {
		return nil, nil, err
	}

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
	supervisorPath := s.getSupervisorPath(args.SupervisorID)
	ri := s.registry.Lookup(supervisorPath, args.RunID)
	if ri == nil {
		// The registry fast path only covers a scoped run ID; any other key
		// needs the supervisor to establish a unique match before the event
		// log can be read locally.
		cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
		if err != nil {
			return nil, nil, err
		}
		defer cleanup()
		ri, err = s.lookupRun(cl, supervisorPath, args.RunID)
		if err != nil {
			return nil, nil, err
		}
		if ri == nil {
			return nil, nil, fmt.Errorf("run not found in registry")
		}
	}

	limit := args.Limit
	if limit <= 0 {
		limit = 50
	}

	events, latestSeq, err := readEvents(ri.EventLogPath, args.Types, limit, args.AfterSeq)
	if err != nil {
		return nil, nil, fmt.Errorf("read events: %w", err)
	}
	if events == nil {
		events = []map[string]any{}
	}

	return nil, map[string]any{"events": events, "latest_seq": latestSeq}, nil
}

func (s *Server) handleAvenorFollowUp(ctx context.Context, req *mcp.CallToolRequest, args followUpArgs) (*mcp.CallToolResult, any, error) {
	ri := s.registry.LookupUnique(args.RunID)
	supervisorID := args.SupervisorID
	if supervisorID == "" && ri != nil {
		supervisorID = ri.SupervisorID
	}

	// Resolve the supervisor's control client once and store it in cl.
	// The status lookup and follow-up Spawn call both use cl.
	// They therefore reuse one control-client connection.
	cl, cleanup, err := s.getClientForSupervisor(supervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	ri, err = s.lookupRun(cl, s.getSupervisorPath(supervisorID), args.RunID)
	if err != nil {
		return nil, nil, err
	}
	if ri == nil {
		return nil, nil, fmt.Errorf("run not found in registry")
	}

	sessionID, err := readSentinelSession(ri.SentinelPath)
	if err != nil {
		// Return errors for failed or non-resumable sentinel files unchanged. A
		// missing file may mean the run is not terminal. Query supervisor status for
		// the adopted session ID. If status has no usable ID, re-read the sentinel
		// before using the spawn-time registry value.
		if !errors.Is(err, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("read sentinel session: %w", err)
		}
		if ri.RuntimeID != "" {
			if status, statusErr := cl.Status(ri.RuntimeID); statusErr == nil {
				if currentID, _ := status["session_id"].(string); currentID != "" {
					sessionID = currentID
				}
			}
		}
		if sessionID == "" {
			// If status yields no session ID, retry the sentinel because the
			// supervisor may have written it since the initial read.
			retryID, retryErr := readSentinelSession(ri.SentinelPath)
			if retryErr == nil {
				sessionID = retryID
			} else if !errors.Is(retryErr, os.ErrNotExist) {
				return nil, nil, fmt.Errorf("read sentinel session: %w", retryErr)
			}
		}
		if sessionID == "" {
			if ri.SessionID == "" {
				return nil, nil, fmt.Errorf("read sentinel session: %w", err)
			}
			sessionID = ri.SessionID
		}
	}

	identity := resolvedSpawnIdentity{
		Agent: ri.Agent, Model: ri.Model, Backend: ri.Backend,
		RosterFile: ri.RosterFile, RosterEntry: ri.RosterEntry,
		EffectiveAgent: ri.EffectiveAgent, EffectiveModel: ri.EffectiveModel,
		EffectiveBackend: ri.EffectiveBackend, AgentProfile: ri.AgentProfile,
	}
	// A completed workflow's authoritative identity belongs to its final phase,
	// not its run-level selector. Refresh from the stable tombstone before
	// follow-up and never reread the roster file.
	var liveIdentityStatus map[string]any
	if ri.RuntimeID != "" {
		if status, statusErr := cl.Status(ri.RuntimeID); statusErr == nil {
			liveIdentityStatus = status
			applySpawnIdentity(status, &identity)
			if currentID, _ := status["session_id"].(string); currentID != "" {
				sessionID = currentID
			}
		}
	}

	runID := uuid.New().String()
	followupLabel := args.Label
	if followupLabel == "" {
		followupLabel = ri.Label + "-followup"
	}

	sentinelPath := filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.done", runID))
	eventLogPath := filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.log", runID))

	effectiveAgent := identity.EffectiveAgent
	if effectiveAgent == "" {
		effectiveAgent = identity.Agent
	}
	effectiveModel := identity.EffectiveModel
	if effectiveModel == "" {
		effectiveModel = identity.Model
	}
	effectiveBackend := identity.EffectiveBackend
	if effectiveBackend == "" {
		effectiveBackend = identity.Backend
	}
	agentProfile := identity.AgentProfile
	// Presence is authoritative even for an empty string. This clears stale
	// run-level fields when the final workflow phase is agent-only/model-only.
	if liveIdentityStatus != nil {
		exact := func(effectiveKey, directKey string) (string, bool) {
			if value, ok := liveIdentityStatus[effectiveKey].(string); ok {
				return value, true
			}
			value, ok := liveIdentityStatus[directKey].(string)
			return value, ok
		}
		if value, ok := exact("effective_agent", "agent"); ok {
			effectiveAgent = value
		}
		if value, ok := exact("effective_model", "model"); ok {
			effectiveModel = value
		}
		if value, ok := exact("effective_backend", "backend"); ok {
			effectiveBackend = value
		}
		if value, ok := liveIdentityStatus["agent_profile"].(string); ok {
			agentProfile = value
		}
	}
	params := map[string]any{
		"dir":           ri.Dir,
		"prompt":        args.Message,
		"label":         followupLabel,
		"session_id":    sessionID,
		"sentinel_file": sentinelPath,
		"on_event":      eventLogPath,
	}
	if effectiveAgent != "" {
		params["agent"] = effectiveAgent
	}
	if effectiveModel != "" {
		params["model"] = effectiveModel
	}
	if effectiveBackend != "" {
		params["backend"] = effectiveBackend
	}
	if ri.Thinking != "" {
		params["thinking"] = ri.Thinking
	}
	if agentProfile != "" {
		params["agent_profile"] = agentProfile
	}
	if ri.AutoApprove {
		params["auto_approve"] = true
	}

	result, err := cl.Spawn(params)
	if err != nil {
		return nil, nil, fmt.Errorf("spawn: %w", err)
	}

	runtimeID, _ := result["runtime_id"].(string)
	newSessionID, _ := result["session_id"].(string)

	supervisorPath := s.getSupervisorPath(supervisorID)

	if err := s.registry.Store(&RunInfo{
		RunID:            runID,
		Label:            followupLabel,
		RuntimeID:        runtimeID,
		SessionID:        newSessionID,
		SupervisorID:     supervisorPath,
		SentinelPath:     sentinelPath,
		EventLogPath:     eventLogPath,
		Agent:            effectiveAgent,
		Model:            effectiveModel,
		Backend:          effectiveBackend,
		RosterFile:       identity.RosterFile,
		RosterEntry:      identity.RosterEntry,
		EffectiveAgent:   effectiveAgent,
		EffectiveModel:   effectiveModel,
		EffectiveBackend: effectiveBackend,
		AgentProfile:     agentProfile,
		Thinking:         ri.Thinking,
		Dir:              ri.Dir,
		AutoApprove:      ri.AutoApprove,
		CreatedAt:        time.Now(),
	}); err != nil {
		return nil, nil, fmt.Errorf("registry store: %w", err)
	}

	return nil, map[string]any{
		"run_id": runID,
		"label":  followupLabel,
	}, nil
}

// handleAvenorWorkflowStatus mirrors the workflow.status control verb against
// the typed WorkflowStatus client method. The workflow ID is passed through
// unchanged; no identifier is remapped or rewritten.
func (s *Server) handleAvenorWorkflowStatus(ctx context.Context, req *mcp.CallToolRequest, args workflowStatusArgs) (*mcp.CallToolResult, any, error) {
	if args.WorkflowID == "" {
		return nil, nil, fmt.Errorf("workflow_id is required")
	}
	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	result, err := cl.WorkflowStatus(args.WorkflowID)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow status: %w", err)
	}
	return nil, result, nil
}

// handleAvenorWorkflowWait waits for a workflow to reach a terminal state or
// until the bounded timeout elapses. It returns the current workflow snapshot.
func (s *Server) handleAvenorWorkflowWait(ctx context.Context, req *mcp.CallToolRequest, args workflowWaitArgs) (*mcp.CallToolResult, any, error) {
	if args.WorkflowID == "" {
		return nil, nil, fmt.Errorf("workflow_id is required")
	}
	var requested time.Duration = 30 * time.Second
	if args.Timeout != "" {
		seconds, err := parseTimeoutSeconds(args.Timeout)
		if err != nil {
			return nil, nil, err
		}
		requested = time.Duration(seconds) * time.Second
	}
	effective, clamped := s.clampWait(requested)
	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	result, err := cl.WorkflowWait(args.WorkflowID, effective)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow wait: %w", err)
	}
	if clamped {
		timedOut, _ := result["timed_out"].(bool)
		terminal, _ := result["terminal"].(bool)
		if timedOut && !terminal {
			result["wait_clamped"] = true
		}
	}
	return nil, result, nil
}

// handleAvenorWorkflowInspect returns the full instance detail for a workflow.
func (s *Server) handleAvenorWorkflowInspect(ctx context.Context, req *mcp.CallToolRequest, args workflowInspectArgs) (*mcp.CallToolResult, any, error) {
	if args.WorkflowID == "" {
		return nil, nil, fmt.Errorf("workflow_id is required")
	}
	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	result, err := cl.WorkflowInspect(args.WorkflowID)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow inspect: %w", err)
	}
	return nil, result, nil
}

// handleAvenorWorkflowEvents reads log events from a workflow instance's event
// store with Sequence > afterSeq, capped at limit.
func (s *Server) handleAvenorWorkflowEvents(ctx context.Context, req *mcp.CallToolRequest, args workflowEventsArgs) (*mcp.CallToolResult, any, error) {
	if args.WorkflowID == "" {
		return nil, nil, fmt.Errorf("workflow_id is required")
	}
	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	result, err := cl.WorkflowEvents(args.WorkflowID, args.AfterSeq, args.Limit)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow events: %w", err)
	}
	return nil, result, nil
}

// handleAvenorWorkflowComplete atomically completes a machine/external handoff
// activation. Inputs are aligned with the Stage 11 complete contract: the
// lease/owner-token pair, the declared outcome, and optional outputs/artifacts.
func (s *Server) handleAvenorWorkflowComplete(ctx context.Context, req *mcp.CallToolRequest, args workflowCompleteArgs) (*mcp.CallToolResult, any, error) {
	if args.WorkflowID == "" {
		return nil, nil, fmt.Errorf("workflow_id is required")
	}
	if args.NodeID == "" {
		return nil, nil, fmt.Errorf("node_id is required")
	}
	if args.ActivationID == "" {
		return nil, nil, fmt.Errorf("activation_id is required")
	}
	if args.AttemptID == "" {
		return nil, nil, fmt.Errorf("attempt_id is required")
	}
	if args.LeaseID == "" {
		return nil, nil, fmt.Errorf("lease_id is required")
	}
	if args.OwnerToken == "" {
		return nil, nil, fmt.Errorf("owner_token is required")
	}
	if args.Outcome == "" {
		return nil, nil, fmt.Errorf("outcome is required")
	}
	fields := map[string]any{
		"node_id":       args.NodeID,
		"activation_id": args.ActivationID,
		"attempt_id":    args.AttemptID,
		"lease_id":      args.LeaseID,
		"owner_token":   args.OwnerToken,
		"outcome":       args.Outcome,
	}
	if len(args.Outputs) > 0 {
		var outputs any
		if err := json.Unmarshal(args.Outputs, &outputs); err != nil {
			return nil, nil, fmt.Errorf("outputs must be valid JSON: %w", err)
		}
		fields["outputs"] = outputs
	}
	if len(args.Artifacts) > 0 {
		var artifacts any
		if err := json.Unmarshal(args.Artifacts, &artifacts); err != nil {
			return nil, nil, fmt.Errorf("artifacts must be valid JSON: %w", err)
		}
		fields["artifacts"] = artifacts
	}
	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	result, err := cl.WorkflowComplete(args.WorkflowID, fields)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow complete: %w", err)
	}
	return nil, result, nil
}

// handleAvenorWorkflowGate records a gate decision on a parked awaiting_gate
// activation. The operation is validated against the Stage 12 closed enum
// before any server call so an unknown value fails fast without mutating state.
func (s *Server) handleAvenorWorkflowGate(ctx context.Context, req *mcp.CallToolRequest, args workflowGateArgs) (*mcp.CallToolResult, any, error) {
	if args.WorkflowID == "" {
		return nil, nil, fmt.Errorf("workflow_id is required")
	}
	if args.NodeID == "" {
		return nil, nil, fmt.Errorf("node_id is required")
	}
	if args.GateID == "" {
		return nil, nil, fmt.Errorf("gate_id is required")
	}
	if args.ActivationID == "" {
		return nil, nil, fmt.Errorf("activation_id is required")
	}
	switch args.Operation {
	case "satisfy", "reject", "waive", "external_result":
	default:
		return nil, nil, fmt.Errorf("unknown gate operation %q (allowed: satisfy, reject, waive, external_result)", args.Operation)
	}
	fields := map[string]any{
		"node_id":       args.NodeID,
		"activation_id": args.ActivationID,
		"gate_id":       args.GateID,
		"operation":     args.Operation,
	}
	for key, value := range map[string]any{
		"actor":         args.Actor,
		"reason":        args.Reason,
		"outcome":       args.Outcome,
		"poll_id":       args.PollID,
		"source":        args.Source,
		"result":        args.Result,
		"response_hash": args.ResponseHash,
	} {
		if value.(string) != "" {
			fields[key] = value
		}
	}
	if args.ObservedAt != "" {
		if _, err := time.Parse(time.RFC3339, args.ObservedAt); err != nil {
			return nil, nil, fmt.Errorf("observed_at must be an RFC3339 timestamp: %w", err)
		}
		fields["observed_at"] = args.ObservedAt
	}
	if len(args.Subject) > 0 {
		var subject any
		if err := json.Unmarshal(args.Subject, &subject); err != nil {
			return nil, nil, fmt.Errorf("subject must be valid JSON: %w", err)
		}
		fields["subject"] = subject
	}
	if len(args.EvidenceIDs) > 0 {
		fields["evidence_ids"] = args.EvidenceIDs
	}
	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	result, err := cl.WorkflowGate(args.WorkflowID, fields)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow gate: %w", err)
	}
	return nil, result, nil
}

// handleAvenorWorkflowControllerStatus mirrors the workflow.controller.status
// and workflow.controller.list control verbs against the typed
// WorkflowControllerStatus and WorkflowControllerList client methods. With a
// controller ID it returns that controller's full status; without one it
// lists all controllers. The controller ID is passed through unchanged; no
// identifier is remapped.
func (s *Server) handleAvenorWorkflowControllerStatus(ctx context.Context, req *mcp.CallToolRequest, args workflowControllerStatusArgs) (*mcp.CallToolResult, any, error) {
	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	if args.ControllerID == "" {
		result, err := cl.WorkflowControllerList()
		if err != nil {
			return nil, nil, fmt.Errorf("workflow controller list: %w", err)
		}
		return nil, result, nil
	}
	result, err := cl.WorkflowControllerStatus(args.ControllerID)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow controller status: %w", err)
	}
	return nil, result, nil
}

var startSupervisorFunc = startSupervisor

// dialSupervisorClient is the seam for dialing a supervisor control socket;
// tests replace it to count or substitute dials.
var dialSupervisorClient = client.Dial

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
		cl, err := dialSupervisorClient(supervisorID)
		if err != nil {
			return nil, nil, "", fmt.Errorf("dial supervisor socket %s: %w", supervisorID, err)
		}
		return cl, func() { cl.Close() }, supervisorID, nil
	}
	defer s.supervisorMu.Unlock()

	if s.opts.SupervisorSocket != "" {
		// Explicit-socket deployment: dial lazily, redial after a dead
		// connection, and never fall through to autostart. Holding the lock
		// during the dial makes concurrent acquisitions share one attempt.
		if s.controlClient == nil || s.controlClient.Closed() {
			cl, err := dialSupervisorClient(s.opts.SupervisorSocket)
			if err != nil {
				s.controlClient = nil
				return nil, nil, "", fmt.Errorf("supervisor unavailable at %s: %w", s.opts.SupervisorSocket, err)
			}
			s.controlClient = cl
		}
		return s.controlClient, func() {}, s.defaultSupervisorPath, nil
	}

	if s.controlClient == nil {
		if s.opts.NoAutostart {
			return nil, nil, "", fmt.Errorf("no supervisor running: autostart disabled")
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

func (s *Server) findRegistryByRuntimeID(supervisorPath, runtimeID string) *RunInfo {
	for _, ri := range s.registry.All() {
		if ri.SupervisorID == supervisorPath && ri.RuntimeID == runtimeID {
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
		if !s.isAllowedHTTPHost(r.Host) || !s.isAllowedHTTPOrigin(r.Header.Get("Origin")) {
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

func (s *Server) isAllowedHTTPOrigin(origin string) bool {
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if u.Scheme == "http" && isLoopbackHost(u.Hostname()) {
		return true
	}
	if u.Scheme == "http" || u.Scheme == "https" {
		hostname := u.Hostname()
		if !isASCIIHost(hostname) {
			return false
		}
		for _, entry := range s.opts.AllowedHosts {
			if strings.EqualFold(hostname, entry) {
				return true
			}
		}
	}
	return false
}

func (s *Server) isAllowedHTTPHost(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	host = strings.Trim(host, "[]")
	if isLoopbackHost(host) {
		return true
	}
	if !isASCIIHost(host) {
		return false
	}
	for _, entry := range s.opts.AllowedHosts {
		if strings.EqualFold(host, entry) {
			return true
		}
	}
	return false
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// isASCIIHost reports whether s contains only ASCII runes. Non-ASCII hosts are
// rejected before EqualFold because Unicode simple folding (e.g. the KELVIN
// SIGN U+212A folding to 'k') could otherwise let a non-ASCII host match an
// allowlist entry.
func isASCIIHost(s string) bool {
	for _, r := range s {
		if r > 0x7F {
			return false
		}
	}
	return true
}

func (s *Server) RegisteredToolNames() []string {
	names := make([]string, len(s.toolNames))
	copy(names, s.toolNames)
	return names
}
