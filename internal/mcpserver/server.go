package mcpserver

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runstate"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/spawnselection"
)

type ControlClient interface {
	Status(runtimeID string) (map[string]any, error)
	List() ([]map[string]any, error)
	Spawn(params map[string]any) (map[string]any, error)
	SpawnMCP(params map[string]any) (map[string]any, error)
	Shutdown(mode string) error
	Cancel(runtimeID string) error
	Close() error
	Closed() bool
	AnswerPermission(runtimeID, requestID, optionID string) error
	WorkflowStatus(workflowID string) (map[string]any, error)
	WorkflowWait(workflowID string, timeout time.Duration) (map[string]any, error)
	WorkflowInspect(workflowID string) (map[string]any, error)
	WorkflowEvents(workflowID string, afterSeq int64, limit int) (map[string]any, error)
	WorkflowComplete(workflowID string, fields map[string]any) (map[string]any, error)
	WorkflowGate(workflowID string, fields map[string]any) (map[string]any, error)
	WorkflowCancel(workflowID, actor, reason string) (map[string]any, error)
	WorkflowControllerStatus(controllerID string) (map[string]any, error)
	WorkflowControllerList() (map[string]any, error)

	MessageSendContext(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error)
	MessageWaitContext(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error)
	MessageReplyContext(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error)
	MessageWithdrawContext(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error)
	MessageReceiveContext(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error)
	MessagePeersContext(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error)
	MessagePreflightContext(ctx context.Context) (messaging.PreflightResult, error)
	ResumeContext(ctx context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error)
	SessionRenameContext(ctx context.Context, sessionID, name string) (messaging.RenameResult, error)
	SessionLastContext(ctx context.Context, runtimeID string) (messaging.PeerLast, error)
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

	// Stderr is the writer unrecognized-status diagnostics are reported to.
	// Nil means os.Stderr. Tests inject a buffer to capture output.
	Stderr io.Writer

	// MCPLaunchWrapperPolicyPath is forwarded to a supervisor this server
	// autostarts, giving that supervisor its MCP launch wrapper allowlist.
	// Attaching to an already-running supervisor never changes its policy:
	// the path is ignored on attach.
	MCPLaunchWrapperPolicyPath string
}

type Server struct {
	opts                  Options
	mcpServer             *mcp.Server
	controlClient         ControlClient
	lifecycle             *supervisorLifecycle
	registry              *RunRegistry
	defaultSupervisorPath string
	diag                  *statusDiagnostics
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
	Agent          string   `json:"agent,omitempty" jsonschema:"optional agent name; omission uses the selected model or runtime defaults"`
	RepoDir        string   `json:"repo_dir" jsonschema:"required path to the repository"`
	Prompt         string   `json:"prompt,omitempty" jsonschema:"optional initial prompt"`
	PromptFile     string   `json:"prompt_file,omitempty" jsonschema:"optional path to file containing the initial prompt"`
	Label          string   `json:"label,omitempty" jsonschema:"optional label for the run"`
	Timeout        string   `json:"timeout,omitempty" jsonschema:"optional timeout in seconds (numeric string)"`
	Model          string   `json:"model,omitempty" jsonschema:"optional model to use"`
	Thinking       string   `json:"thinking,omitempty" jsonschema:"optional thinking level (off, minimal, low, medium, high, xhigh, max); omission uses the backend default"`
	Backend        string   `json:"backend,omitempty" jsonschema:"optional runtime backend (for example agy, pi, opencode-acp, or codex-app-server)"`
	RosterFile     string   `json:"roster_file,omitempty" jsonschema:"optional path to the roster map"`
	RosterEntry    string   `json:"roster_entry,omitempty" jsonschema:"optional roster entry key"`
	ServerURL      string   `json:"server_url,omitempty" jsonschema:"optional opencode serve URL for opencode-http backend"`
	SupervisorID   string   `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
	IdempotencyKey string   `json:"idempotency_key,omitempty" jsonschema:"optional idempotency key; a retry with the same key and parameters returns the original run instead of spawning a second one"`
	LaunchWrapper  []string `json:"launch_wrapper,omitempty" jsonschema:"optional argv prefix; disabled unless the selected supervisor enables an operator allowlist. Must exactly match an approved argv array. No shell expansion."`
	AutoApprove    bool     `json:"auto_approve,omitempty" jsonschema:"optional auto-approve all permission requests so the run executes unattended (no answer_permission needed)"`
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

type killArgs struct {
	RunID        string `json:"run_id" jsonschema:"required run ID or label"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
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

type workflowCancelArgs struct {
	WorkflowID   string `json:"workflow_id" jsonschema:"required workflow ID"`
	Reason       string `json:"reason" jsonschema:"required reason for the cancel; recorded on the workflow.canceled event"`
	Actor        string `json:"actor,omitempty" jsonschema:"optional actor id recorded as the canceling operator (default mcp)"`
	SupervisorID string `json:"supervisor_id,omitempty" jsonschema:"optional supervisor socket path"`
}

// stderrOr resolves the server's diagnostic writer: the configured writer
// when set, else the process stderr.
func stderrOr(w io.Writer) io.Writer {
	if w != nil {
		return w
	}
	return os.Stderr
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
			"status",
			"result",
			"spawn",
			"shutdown",
			"answer_permission",
			"message",
			"peers",
			"events",
			"kill",
			"workflow_status",
			"workflow_wait",
			"workflow_inspect",
			"workflow_events",
			"workflow_complete",
			"workflow_gate",
			"workflow_cancel",
			"workflow_controller_status",
		},
		diag: &statusDiagnostics{stderr: stderrOr(opts.Stderr)},
	}

	// Explicit sockets keep their socket path for lazy acquisition: the first
	// default-supervisor tool call dials, and redials after a dead connection.
	// The default autostart path is intentionally acquired by the first tool
	// call so constructing an MCP server does not race other constructors.
	if opts.SupervisorSocket != "" {
		s.defaultSupervisorPath = opts.SupervisorSocket
	}

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "status",
		Description: "Get lifecycle status of avenor runs; optionally wait for terminal, phase_change, turn_complete, or permission. Without run_id, returns an object with runs (array of status objects) and count. The wait budget is approximate: a single underlying status poll may add up to its own timeout.",
	}, s.handleAvenorStatus)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "result",
		Description: "Wait for a run to finish and return its complete final output. The wait budget is approximate: a single underlying status poll may add up to its own timeout.",
	}, s.handleAvenorResult)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "spawn",
		Description: "Spawn a new avenor run",
	}, s.handleAvenorSpawn)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "shutdown",
		Description: "Shutdown the avenor supervisor and clean up run artifacts",
	}, s.handleAvenorShutdown)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "kill",
		Description: "Terminate a single run without shutting down the supervisor. Other runs and the supervisor itself keep running.",
	}, s.handleAvenorKill)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "answer_permission",
		Description: "Answer a pending permission request for a run",
	}, s.handleAvenorAnswerPermission)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "events",
		Description: "Read recent events from a run's event log",
	}, s.handleAvenorEvents)

	// The merged tools publish the flat object schema compiled from the shared
	// contract's per-action union, and register through the raw-arguments
	// handler: admission belongs to the router on every surface, so the SDK's own
	// schema validation must not answer first with an error shape the other
	// surfaces never produce.
	mcpServer.AddTool(&mcp.Tool{
		Name:        "message",
		Description: "Send, ask, reply to, resume, withdraw or answer a control message between agent runs. The call is bound to the supervisor this server knows the addressed run on, so a run on a non-default supervisor is addressable only once this server has registered it: call status with that supervisor_id first, for example after an MCP restart.",
		InputSchema: messaging.MessageInputFlatSchema(),
	}, s.handleAvenorMessage)

	mcpServer.AddTool(&mcp.Tool{
		Name:        "peers",
		Description: "List connected agent sessions, rename a run's display name, or read a peer's last agent message. A peer on a non-default supervisor is addressable only once this server has registered a run there (call status with that supervisor_id).",
		InputSchema: messaging.PeersInputFlatSchema(),
	}, s.handleAvenorPeers)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "workflow_status",
		Description: "Get lightweight status for a workflow instance",
	}, s.handleAvenorWorkflowStatus)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "workflow_wait",
		Description: "Wait for a workflow to reach a terminal state or until timeout. The wait budget is approximate: a single underlying wait may add up to its own timeout.",
	}, s.handleAvenorWorkflowWait)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "workflow_inspect",
		Description: "Return the full instance detail for a workflow",
	}, s.handleAvenorWorkflowInspect)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "workflow_events",
		Description: "Read log events from a workflow instance's event log",
	}, s.handleAvenorWorkflowEvents)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "workflow_complete",
		Description: "Atomically complete a machine/external handoff activation",
	}, s.handleAvenorWorkflowComplete)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "workflow_gate",
		Description: "Record a gate decision on a parked awaiting_gate activation",
	}, s.handleAvenorWorkflowGate)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "workflow_cancel",
		Description: "Cancel a workflow instance: records the attributed cancel event (actor, reason), releases leases, parks non-terminal activations, and terminates the instance's in-flight attempt runtimes. The instance becomes terminal (canceled); re-canceling is an idempotent no-op, and a completed or failed instance cannot be canceled.",
	}, s.handleAvenorWorkflowCancel)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "workflow_controller_status",
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

	cl, cleanup, err := s.getReadClientForSupervisor(args.SupervisorID)
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
		// Index the scope's registry entries by runtime ID once so the
		// per-entry merge is a single map lookup instead of a full registry
		// scan per entry.
		registryByRuntimeID := make(map[string]*RunInfo)
		for _, ri := range s.registry.All() {
			if ri.SupervisorID == supervisorPath {
				if _, ok := registryByRuntimeID[ri.RuntimeID]; !ok {
					registryByRuntimeID[ri.RuntimeID] = ri
				}
			}
		}
		for _, entry := range results {
			runtimeID, _ := entry["runtime_id"].(string)
			ri := registryByRuntimeID[runtimeID]
			var sentinelPath string
			if ri != nil {
				sentinelPath = ri.SentinelPath
			}
			ts := translateStatus(s.diag, runtimeID, entry, sentinelPath)
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
	sd, err := runstate.ReadSentinel(info.SentinelPath)
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
	matched := -1
	switch {
	case sentinelMatch != -1:
		matched = sentinelMatch
	case runtimeMatch != -1:
		matched = runtimeMatch
	case len(labelMatches) == 1:
		matched = labelMatches[0]
	case len(labelMatches) > 1:
		ids := make([]string, 0, len(labelMatches))
		for _, i := range labelMatches {
			id, _ := entries[i]["runtime_id"].(string)
			ids = append(ids, id)
		}
		return nil, fmt.Errorf("ambiguous label %s: matches runtimes %v", key, ids)
	}
	if matched != -1 {
		s.reapStaleLabel(supervisorPath, entries, entries[matched])
		return s.runInfoFromListEntry(entries[matched], supervisorPath)
	}
	// The run's runtime left the live list (e.g. a supervisor restart) but
	// the label is still cached: resolve it to the cached entry so the dead
	// run stays reachable by label for its persisted result and event log.
	if ri := s.registry.LookupLabel(supervisorPath, key); ri != nil {
		return ri, nil
	}
	return nil, nil
}

// lookupRunByPublicID rehydrates the registry from the supervisor's list on a
// miss and resolves one public run id only. Unlike lookupRun it never matches a
// label, a runtime id or a sentinel path fragment, so the message provider
// keeps the two resolution planes while still resolving a public run id that
// outlived this MCP server process. It reaps a stale label mapping before the
// store exactly as lookupRun does, so a supervisor restart cannot leave the
// rehydrated entry rejected by a pre-restart mapping of the same label.
func (s *Server) lookupRunByPublicID(cl ControlClient, supervisorPath, runID string) (*RunInfo, error) {
	// Public run ids are UUIDs the surface minted; anything else is a label or
	// a runtime id and belongs to the broker listing, so no list is issued.
	if _, err := uuid.Parse(runID); err != nil {
		return nil, nil
	}
	if ri := s.registry.Lookup(supervisorPath, runID); ri != nil {
		return ri, nil
	}
	entries, err := cl.List()
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	for _, entry := range entries {
		sentinelFile, _ := entry["sentinel_file"].(string)
		if runstate.RunIDFromSentinel(sentinelFile) != runID {
			continue
		}
		s.reapStaleLabel(supervisorPath, entries, entry)
		return s.runInfoFromListEntry(entry, supervisorPath)
	}
	return nil, nil
}

// reapStaleLabel unlinks a cached label mapping that claims the same label
// under the same supervisor but whose runtime is no longer in the live list.
// The live supervisor's list is authoritative for liveness: a supervisor
// restart at the same socket path orphans its pre-restart entries, and the
// live list re-points the colliding label to the live run so the subsequent
// store succeeds. The stale mapping is unlinked so the label re-points, while
// the dead run stays discoverable by run ID for its persisted terminal result
// and event log. Entries whose runtime is still live, entries of other
// supervisors, and non-colliding labels are left untouched. The known race (a
// spawn storing an entry between the list and the reap) is acceptable and
// self-healing: the next lookup miss re-discovers it.
func (s *Server) reapStaleLabel(supervisorPath string, liveEntries []map[string]any, entry map[string]any) {
	label, _ := entry["label"].(string)
	if label == "" {
		return
	}
	old := s.registry.LookupLabel(supervisorPath, label)
	if old == nil {
		return
	}
	runtimeID, _ := entry["runtime_id"].(string)
	sentinelFile, _ := entry["sentinel_file"].(string)
	newRunID := runstate.RunIDFromSentinel(sentinelFile)
	if newRunID == "" {
		newRunID = runtimeID
	}
	if old.RunID == newRunID {
		return
	}
	liveRuntimes := make(map[string]bool, len(liveEntries))
	for _, live := range liveEntries {
		if rid, ok := live["runtime_id"].(string); ok {
			liveRuntimes[rid] = true
		}
	}
	if liveRuntimes[old.RuntimeID] {
		return
	}
	s.registry.UnlinkLabel(old.SupervisorID, old.Label)
}

// checkLabelAvailable rejects a spawn whose explicit label is already claimed
// by a non-terminal run in the supervisor's list, by a live run on another
// supervisor (the registry enforces global label uniqueness), and reaps a
// stale registry mapping left behind by a supervisor restart (the live list
// is authoritative for liveness). Only non-terminal runs hold a label: an
// ended run's label is free to re-claim, so terminal entries are excluded
// from the claim scan, and a terminal cross-supervisor holder's mapping is
// reaped so the spawn re-points the label. A list error never blocks the
// spawn: the registry's own collision check remains the backstop.
func (s *Server) checkLabelAvailable(cl ControlClient, supervisorPath, label string) error {
	entries, err := cl.List()
	if err != nil {
		return nil
	}
	live := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		status, _ := entry["status"].(string)
		if runstate.IsTerminalStatus(runstate.RuntimeStatus(status)) {
			continue
		}
		live = append(live, entry)
	}
	for _, entry := range live {
		if l, _ := entry["label"].(string); l == label {
			runtimeID, _ := entry["runtime_id"].(string)
			return fmt.Errorf("label already in use: %s (runtime %s)", label, runtimeID)
		}
	}
	if holder := s.registry.LabelHolder(label); holder != nil && holder.SupervisorID != supervisorPath {
		if _, terminal := terminalStatusFromRunInfo(holder); terminal {
			// An ended cross-supervisor run no longer holds the label:
			// unlink its stale mapping so the spawn's store re-points it.
			s.registry.UnlinkLabel(holder.SupervisorID, label)
		} else {
			return fmt.Errorf("label already in use: %s (mapped to supervisor %s runtime %s)",
				label, holder.SupervisorID, holder.RuntimeID)
		}
	}
	s.reapStaleLabel(supervisorPath, live, map[string]any{"label": label})
	return nil
}

// runInfoBase carries the identity and base fields every registry entry build
// site supplies. Callers set the path-specific fields (Thinking, Dir,
// AutoApprove, resume status fields) on the returned entry.
type runInfoBase struct {
	RunID        string
	Label        string
	RuntimeID    string
	SessionID    string
	SupervisorID string
	SentinelPath string
	EventLogPath string
	Identity     resolvedSpawnIdentity
	CreatedAt    time.Time
}

// newRunInfo builds a registry entry from base so every identity field is
// copied from base.Identity in one place. An unset CreatedAt defaults to now.
func newRunInfo(base runInfoBase) *RunInfo {
	info := &RunInfo{
		RunID:            base.RunID,
		Label:            base.Label,
		RuntimeID:        base.RuntimeID,
		SessionID:        base.SessionID,
		SupervisorID:     base.SupervisorID,
		SentinelPath:     base.SentinelPath,
		EventLogPath:     base.EventLogPath,
		Agent:            base.Identity.EffectiveAgent,
		Model:            base.Identity.EffectiveModel,
		Backend:          base.Identity.EffectiveBackend,
		RosterFile:       base.Identity.RosterFile,
		RosterEntry:      base.Identity.RosterEntry,
		EffectiveAgent:   base.Identity.EffectiveAgent,
		EffectiveModel:   base.Identity.EffectiveModel,
		EffectiveBackend: base.Identity.EffectiveBackend,
		AgentProfile:     base.Identity.AgentProfile,
		CreatedAt:        base.CreatedAt,
	}
	if info.CreatedAt.IsZero() {
		info.CreatedAt = time.Now()
	}
	return info
}

// runInfoFromListEntry converts a stable supervisor list entry into a registry
// entry scoped to supervisorPath. The supervisor-wide run_id is never used as
// the MCP run ID.
func (s *Server) runInfoFromListEntry(entry map[string]any, supervisorPath string) (*RunInfo, error) {
	runtimeID, _ := entry["runtime_id"].(string)
	sentinelFile, _ := entry["sentinel_file"].(string)
	runID := runstate.RunIDFromSentinel(sentinelFile)
	if runID == "" {
		runID = runtimeID
	}
	label, _ := entry["label"].(string)
	var identity resolvedSpawnIdentity
	applySpawnIdentity(entry, &identity)
	info := newRunInfo(runInfoBase{
		RunID:        runID,
		Label:        label,
		RuntimeID:    runtimeID,
		SessionID:    stringField(entry, "session_id"),
		SupervisorID: supervisorPath,
		SentinelPath: sentinelFile,
		EventLogPath: stringField(entry, "on_event"),
		Identity:     identity,
	})
	info.Dir = stringField(entry, "dir")
	info.Thinking = stringField(entry, "thinking")
	info.AutoApprove, _ = entry["auto_approve"].(bool)
	if ms, ok := entry["started_at"].(float64); ok && ms > 0 {
		info.CreatedAt = time.UnixMilli(int64(ms))
	}
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
		ts := translateStatus(s.diag, ri.RuntimeID, result, ri.SentinelPath)
		applyRunInfoIdentity(ts, ri)
		ts["run_id"] = ri.RunID
		ts["label"] = ri.Label
		return ts, nil
	}

	result, err := cl.Status(runID)
	if err != nil {
		return nil, fmt.Errorf("status: %w", err)
	}
	runtimeID := stringField(result, "runtime_id")
	if runtimeID == "" {
		runtimeID = runID
	}
	ts := translateStatus(s.diag, runtimeID, result, "")
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
	for _, key := range []string{"run_id", "label", "status", "runtime_id", "phase", "phase_label", "pending_permission", "permission", "permission_decider", "permission_model_decider", "permission_decider_exhausted", "latest_seq", "timed_out", "wait_clamped"} {
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

// resultSupervisorID resolves the supervisor a status or result call should
// bind to: the caller's explicit choice, else the one supervisor that owns the
// run id or label. The run-key ladder is shared with the peers and message
// surface; runtime ids are deliberately not a key here, because these tools
// document run_id as a run id or label.
func (s *Server) resultSupervisorID(runID, requestedSupervisorID string) string {
	if requestedSupervisorID != "" {
		return requestedSupervisorID
	}
	return s.supervisorForRunKey(runID)
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
	cl, cleanup, err := s.getReadClientForSupervisor(supervisorID)
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
	if err := runtime.ValidateLaunchWrapper(args.LaunchWrapper); err != nil {
		return nil, nil, fmt.Errorf("launch_wrapper: %w", err)
	}
	if len(args.IdempotencyKey) > 256 {
		return nil, nil, fmt.Errorf("idempotency_key too long (max 256 bytes)")
	}

	runID := uuid.New().String()
	labelDerived := args.Label == ""
	label := args.Label
	if label == "" {
		label = runID
	}

	sentinelPath, eventLogPath := runArtifactPaths(runID)

	params := map[string]any{
		"dir":           args.RepoDir,
		"label":         label,
		"sentinel_file": sentinelPath,
		"on_event":      eventLogPath,
	}

	if args.IdempotencyKey != "" {
		params["idempotency_key"] = "spawn:" + args.IdempotencyKey
		if labelDerived {
			params["label_derived"] = true
		}
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
	if len(args.LaunchWrapper) > 0 {
		params["launch_wrapper"] = args.LaunchWrapper
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

	if args.Label != "" && args.IdempotencyKey == "" {
		// Keyed spawns skip the pre-check: a retry under the same key must be
		// allowed to reach the idempotency gate and return the stored result,
		// even though the original run still holds the label.
		if err := s.checkLabelAvailable(cl, supervisorPath, args.Label); err != nil {
			return nil, nil, err
		}
	}

	var result map[string]any
	if len(args.LaunchWrapper) > 0 {
		// Wrapped requests must use the gated spawn_mcp verb so the
		// supervisor-side launch-wrapper allowlist applies.
		result, err = cl.SpawnMCP(params)
	} else {
		// Unwrapped requests use the plain spawn verb, which also works on
		// supervisors predating spawn_mcp.
		result, err = cl.Spawn(params)
	}
	if err != nil {
		// Fail closed for older supervisors without the spawn_mcp verb: a
		// wrapped MCP request must never fall back to the ungated spawn path.
		if isMethodNotFound(err) && len(args.LaunchWrapper) > 0 {
			return nil, nil, errors.New("launch_wrapper: disabled for MCP on this supervisor")
		}
		return nil, nil, fmt.Errorf("spawn: %w", err)
	}

	if args.IdempotencyKey != "" {
		if sent, ok := result["sentinel_file"].(string); ok && sent != "" {
			sentinelPath = sent
			if id := runstate.RunIDFromSentinel(sent); id != "" {
				runID = id
			}
		}
		if ev, ok := result["on_event"].(string); ok && ev != "" {
			eventLogPath = ev
		}
		if labelDerived {
			// The response label is the original run's ID, not the retry's
			// freshly generated UUID (D4).
			label = runID
		}
	}

	runtimeID, _ := result["runtime_id"].(string)
	sessionID, _ := result["session_id"].(string)
	identity := resolveSpawnIdentity(cl, runtimeID, result, args)

	info := newRunInfo(runInfoBase{
		RunID:        runID,
		Label:        label,
		RuntimeID:    runtimeID,
		SessionID:    sessionID,
		SupervisorID: supervisorPath,
		SentinelPath: sentinelPath,
		EventLogPath: eventLogPath,
		Identity:     identity,
	})
	info.Thinking = args.Thinking
	info.Dir = args.RepoDir
	info.AutoApprove = args.AutoApprove
	if err := s.registry.Store(info); err != nil {
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

func (s *Server) handleAvenorKill(ctx context.Context, req *mcp.CallToolRequest, args killArgs) (*mcp.CallToolResult, any, error) {
	args.RunID = strings.TrimSpace(args.RunID)
	if args.RunID == "" {
		return nil, nil, fmt.Errorf("run_id is required")
	}
	supervisorID := s.resultSupervisorID(args.RunID, args.SupervisorID)

	cl, cleanup, err := s.getClientForSupervisor(supervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	ri, err := s.lookupRun(cl, s.getSupervisorPath(supervisorID), args.RunID)
	if err != nil {
		return nil, nil, err
	}

	// The live list is authoritative when registry metadata is stale: a
	// registry entry without a runtime_id resolves like an unknown run.
	runtimeID := ""
	if ri != nil && ri.RuntimeID != "" {
		runtimeID = ri.RuntimeID
	} else {
		runtimeID, err = resolveRuntimeIDFromList(cl, args.RunID)
		if err != nil {
			return nil, nil, err
		}
	}

	// Cancel without a runtime_id targets the supervisor's root run, so an
	// empty resolution must fail instead of reaching the RPC.
	if runtimeID == "" {
		return nil, nil, fmt.Errorf("run %q resolved to no runtime_id", args.RunID)
	}

	if err := cl.Cancel(runtimeID); err != nil {
		return nil, nil, fmt.Errorf("cancel: %w", err)
	}

	return nil, map[string]any{
		"killed":     true,
		"run_id":     args.RunID,
		"runtime_id": runtimeID,
	}, nil
}

func (s *Server) handleAvenorAnswerPermission(ctx context.Context, req *mcp.CallToolRequest, args permissionArgs) (*mcp.CallToolResult, any, error) {
	if err := runtime.ValidatePermissionMessage(args.Message); err != nil {
		return nil, nil, err
	}
	supervisorID := s.resultSupervisorID(args.RunID, args.SupervisorID)

	cl, cleanup, err := s.getClientForSupervisor(supervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	ri, err := s.lookupRun(cl, s.getSupervisorPath(supervisorID), args.RunID)
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
			return nil, nil, fmt.Errorf("no pending permission request for runtime %s; include request_id only when retrying a previously answered request", runtimeID)
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

// isMethodNotFound reports whether err is a control-protocol "method not
// found" failure (code -32601), as returned when the supervisor predates the
// spawn_mcp verb.
func isMethodNotFound(err error) bool {
	var rpcErr *client.RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr.Code == -32601
	}
	return strings.Contains(err.Error(), "method not found")
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
		runEntryID, _ := entry["run_id"].(string)
		if runID == runtimeID || runID == label || runID == sessionID || runID == runEntryID {
			if runtimeID == "" {
				return "", fmt.Errorf("run %q matched but has no runtime_id", runID)
			}
			return runtimeID, nil
		}
	}
	return "", fmt.Errorf("run %q not found", runID)
}

func (s *Server) handleAvenorEvents(ctx context.Context, req *mcp.CallToolRequest, args eventsArgs) (*mcp.CallToolResult, any, error) {
	// The registry fast path only covers a run ID that resolves to a unique
	// cached entry; any other key needs the supervisor to establish a match
	// before the event log can be read locally.
	ri := s.registry.LookupUnique(args.RunID)
	if ri != nil && args.SupervisorID != "" && ri.SupervisorID != args.SupervisorID {
		ri = nil
	}
	if ri == nil {
		// Acquire the client before resolving the supervisor path: the
		// acquisition may autostart the default supervisor, and a rehydrated
		// entry must be scoped to the path it was found on.
		cl, cleanup, err := s.getReadClientForSupervisor(args.SupervisorID)
		if err != nil {
			return nil, nil, err
		}
		defer cleanup()
		supervisorPath := s.getSupervisorPath(args.SupervisorID)
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

// registerResumed records the run a resume produced in the same registry the
// spawn and status paths read, so the resumed run is resolvable by label and
// its event log is readable. Identity comes from the new runtime's status, the
// same way a follow-up read it; a status miss leaves the identity empty instead
// of failing a resume that already succeeded. The run is never killed here.
func (s *Server) registerResumed(cl ControlClient, supervisorPath string, res messaging.ResumeResult) error {
	identity := resolvedSpawnIdentity{}
	var status map[string]any
	if res.RuntimeID != "" {
		if live, err := cl.Status(res.RuntimeID); err == nil {
			status = live
			applySpawnIdentity(live, &identity)
		}
	}
	info := newRunInfo(runInfoBase{
		RunID:        res.RunID,
		Label:        res.Label,
		RuntimeID:    res.RuntimeID,
		SessionID:    res.SessionID,
		SupervisorID: supervisorPath,
		SentinelPath: res.SentinelFile,
		EventLogPath: res.OnEvent,
		Identity:     identity,
	})
	if status != nil {
		info.Thinking, _ = status["thinking"].(string)
		info.Dir, _ = status["dir"].(string)
		info.AutoApprove, _ = status["auto_approve"].(bool)
	}
	return s.registry.Store(info)
}

// handleAvenorWorkflowStatus mirrors the workflow.status control verb against
// the typed WorkflowStatus client method. The workflow ID is passed through
// unchanged; no identifier is remapped or rewritten.
func (s *Server) handleAvenorWorkflowStatus(ctx context.Context, req *mcp.CallToolRequest, args workflowStatusArgs) (*mcp.CallToolResult, any, error) {
	if args.WorkflowID == "" {
		return nil, nil, fmt.Errorf("workflow_id is required")
	}
	cl, cleanup, err := s.getReadClientForSupervisor(args.SupervisorID)
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
	cl, cleanup, err := s.getReadClientForSupervisor(args.SupervisorID)
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
	cl, cleanup, err := s.getReadClientForSupervisor(args.SupervisorID)
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
	cl, cleanup, err := s.getReadClientForSupervisor(args.SupervisorID)
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

// handleAvenorWorkflowCancel cancels a whole workflow instance. The reason is
// required: it is the durable audit trail of the workflow.canceled event. The
// actor is optional and defaults to "cli" at the CLI surface; the manager
// requires an explicit actor, so the tool supplies the caller's value or "mcp".
func (s *Server) handleAvenorWorkflowCancel(ctx context.Context, req *mcp.CallToolRequest, args workflowCancelArgs) (*mcp.CallToolResult, any, error) {
	if args.WorkflowID == "" {
		return nil, nil, fmt.Errorf("workflow_id is required")
	}
	if args.Reason == "" {
		return nil, nil, fmt.Errorf("reason is required")
	}
	actor := args.Actor
	if actor == "" {
		actor = "mcp"
	}
	cl, cleanup, err := s.getClientForSupervisor(args.SupervisorID)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	result, err := cl.WorkflowCancel(args.WorkflowID, actor, args.Reason)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow cancel: %w", err)
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
	cl, cleanup, err := s.getReadClientForSupervisor(args.SupervisorID)
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

// dialSupervisorClient and attachSupervisorClient are the two seams for
// acquiring a supervisor control client. Read-only handlers dial anonymously
// so they never read or transmit the owner record; owner-gated handlers attach
// as the socket's published owner. Tests replace them to count or substitute
// acquisitions.
var (
	dialSupervisorClient   = client.Dial
	attachSupervisorClient = client.AttachOwner
)

// supervisorAccess selects what a supervisor client presents.
type supervisorAccess int

const (
	// readAccess acquires a credential-free connection.
	readAccess supervisorAccess = iota
	// ownerAccess authenticates as the socket's published owner.
	ownerAccess
)

// beforeSupervisorLock is a no-op production hook used to coordinate callers
// at the lazy-supervisor lock boundary in concurrency tests.
var beforeSupervisorLock = func() {}

// persistentControlClientForTest returns the currently dialed persistent
// control client under the supervisor lock. Test-only accessor for
// asserting disconnect/redial behavior without reading unexported fields.
func (s *Server) persistentControlClientForTest() ControlClient {
	s.supervisorMu.Lock()
	defer s.supervisorMu.Unlock()
	return s.controlClient
}

func (s *Server) getClientForSupervisor(supervisorID string) (ControlClient, func(), error) {
	cl, cleanup, _, err := s.acquireSupervisor(supervisorID, ownerAccess)
	return cl, cleanup, err
}

// getReadClientForSupervisor acquires the credential-free client the read-only
// tools use.
func (s *Server) getReadClientForSupervisor(supervisorID string) (ControlClient, func(), error) {
	cl, cleanup, _, err := s.acquireSupervisor(supervisorID, readAccess)
	return cl, cleanup, err
}

func (s *Server) getClientForSupervisorWithPath(supervisorID string) (ControlClient, func(), string, error) {
	return s.acquireSupervisor(supervisorID, ownerAccess)
}

func (s *Server) acquireSupervisor(supervisorID string, access supervisorAccess) (ControlClient, func(), string, error) {
	dial := dialSupervisorClient
	if access == ownerAccess {
		dial = attachSupervisorClient
	}
	// An explicit supervisor_id that resolves to the autostarted/default
	// supervisor must reuse the persistent owner connection: ownership is
	// granted per connection by authenticate_owner, and spawn authenticated on
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
		// Tool-arg supervisor_ids must live in the sockets root; the configured
		// --supervisor-socket (and the autostarted default) are exempt via
		// isDefault. Blocks dialing arbitrary local sockets on caller say-so.
		// Validation and dial both run outside the lock: neither touches
		// lock-protected state, and holding supervisorMu across filesystem
		// syscalls would serialize default-path acquisitions behind them.
		if err := validateSupervisorSocketPath(supervisorID); err != nil {
			return nil, nil, "", err
		}
		cl, err := dial(supervisorID)
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
			cl, err := dial(s.opts.SupervisorSocket)
			if err != nil {
				s.controlClient = nil
				return nil, nil, "", fmt.Errorf("supervisor unavailable at %s: %w", s.opts.SupervisorSocket, err)
			}
			if access == readAccess {
				// The persistent client is the owner connection, so a read-only
				// call closes its own connection instead of caching it.
				return cl, func() { cl.Close() }, s.defaultSupervisorPath, nil
			}
			s.controlClient = cl
		}
		return s.controlClient, func() {}, s.defaultSupervisorPath, nil
	}

	if s.controlClient == nil || s.controlClient.Closed() {
		if s.opts.NoAutostart {
			return nil, nil, "", fmt.Errorf("no supervisor running: autostart disabled")
		}
		lc, err := startSupervisorFunc(s.opts.ControlSocket, s.opts.IdleTimeout, s.opts.MCPLaunchWrapperPolicyPath)
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
