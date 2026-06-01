package pony

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/pony/model"
	"github.com/sdougbrown/avenor/internal/runtime/pony/tools"
)

// backendID is the backend identifier.
const backendID = "pony"

// RuntimeIDKey is the context key for the current session's runtime ID.
// The pony provider injects this during Prompt; tools read it for
// parent-child routing (e.g., send_to_parent).
type runtimeIDKey struct{}

var RuntimeIDKey = &runtimeIDKey{}

// Config configures the pony provider.
type Config struct {
	Adapter                model.Adapter
	Model                  string
	MaxTokens              int
	SystemPrompt           string
	InitialPrompt          string
	Executor               OrchestratorExecutor // nil when OrchTools is false
	LocalTools             bool
	OrchTools              bool
	WorkingDir             string // for AGENTS.md discovery
	InjectAgentsMD         bool
	AllowedReadDirs        []string        // additional directories read tools may access (e.g. skills)
	AllowedWriteDirs       []string        // additional directories write tools may access
	ToolApproval           map[string]bool // tool name → requires approval
	ShellConfig     *ShellConfig     // overrides for shell tool
	FileReadConfig         *FileReadConfig  // overrides for file_read tool
	Context                int              // model context window in tokens; 0 = use default compaction
	CompactionPrompt       string           // override LLM compaction task prompt
	// Registry is internal; built from tool config.
	toolRegistry *tools.Registry

	// StopConditions for the loop.
	StopConditions []StopCondition
}

// OrchestratorExecutor wraps the control-socket client for orchestration tools.
type OrchestratorExecutor interface {
	SpawnAgent(ctx context.Context, params map[string]any) (sessionID string, err error)
	SendPrompt(ctx context.Context, sessionID, prompt, requestID string) error
	GetStatus(ctx context.Context, sessionID string) (map[string]any, error)
	WaitForDone(ctx context.Context, sessionID string) (*runtime.AgentResult, error)
	SendToParent(ctx context.Context, runtimeID, message string) error
}

// Provider implements runtime.Provider for the pony backend.
type Provider struct {
	cfg Config
	startErr error

	mu             sync.Mutex
	sessions       map[string]*sessionState
	sessionCounter atomic.Int64
	permCounter    atomic.Int64
}

var _ runtime.Provider = (*Provider)(nil)

// globalSessions provides cross-provider session access so that
// Resume + Prompt work across phase boundaries where a new provider
// is created for each phase attempt.
var (
	globalSessions   = make(map[string]*sessionState)
	globalSessionsMu sync.Mutex
)

const localToolInstructions = "Tool use: use tools when they are the direct way to inspect or change local state. Prefer structured tools first: use list_dir instead of ls, glob for path discovery, grep for content search, and file_read/file_edit/file_write for file access. Use shell only when no structured tool fits. For shell, prefer cmd plus args for exact argv execution; cmd must be only the executable name, with subcommands and flags in args. Examples: use {\"cmd\":\"git\",\"args\":[\"diff\",\"--stat\",\"base...head\"]} rather than putting diff or flags into cmd, and use {\"cmd\":\"ls\",\"args\":[\"packages\"]} rather than repeating ls in args. Do not use pipes, redirects, command chaining, command substitution, or backticks in legacy command strings."

const orchestrationToolInstructions = "Orchestration: use spawn_agents for independent parallel child work, send_prompt for follow-ups, and wait_for_done to collect child results."

const compactionInstructions = "Context management: to keep your context window from overflowing, tool results from older turns are compacted into summaries (marked with [compacted: ...] or [tool result too large: ...]). The most recent ~16K tokens of history are always kept intact. If you need content from a compacted result, re-read the file at a different offset. file_read also returns a reminder instead of re-reading content you've already seen more than twice."

func systemPromptWithToolInstructions(base string, localTools, orchTools bool) string {
	var parts []string
	if strings.TrimSpace(base) != "" {
		parts = append(parts, base)
	}
	if localTools {
		parts = append(parts, localToolInstructions)
		parts = append(parts, compactionInstructions)
	}
	if orchTools {
		parts = append(parts, orchestrationToolInstructions)
	}
	return strings.Join(parts, "\n\n")
}

// New creates a new pony provider with the given config.
func New(cfg Config) *Provider {
	// Derive compaction threshold from context limit (tokens → bytes).
	// bytesPerToken is conservative — code averages ~3.5, natural language ~4.
	// Leaves room for output budget and tool definition overhead.
	const bytesPerToken = 3
	if cfg.Context > 0 {
		SetCompactionThreshold(cfg.Context * bytesPerToken)
	} else {
		SetCompactionThreshold(0) // reset to default
	}
	SetCompactionTaskPrompt(cfg.CompactionPrompt)

	// Build tool registry based on config
	var toolList []tools.Tool
	if cfg.LocalTools {
		toolList = append(toolList,
			tools.NewFileReadToolWithConfig(cfg.FileReadConfig),
			tools.NewFileWriteTool(),
			tools.NewFileEditTool(),
			tools.NewGlobTool(),
			tools.NewGrepTool(),
			tools.NewListDirTool(),
			tools.NewShellToolWithConfig(cfg.ShellConfig),
		)
	}
	// report_finding is always available — it emits structured events
	toolList = append(toolList, tools.NewReportFindingTool())
	if cfg.OrchTools && cfg.Executor != nil {
		toolList = append(toolList, newOrchestrationTools(cfg.Executor)...)
	}
	cfg.toolRegistry = tools.NewRegistry(toolList)
	if len(cfg.AllowedReadDirs) > 0 {
		cfg.toolRegistry.SetAllowedReadDirs(cfg.AllowedReadDirs)
	}
	if len(cfg.AllowedWriteDirs) > 0 {
		cfg.toolRegistry.SetAllowedWriteDirs(cfg.AllowedWriteDirs)
	}

	return &Provider{
		cfg:      cfg,
		sessions: make(map[string]*sessionState),
	}
}

func newWithStartErr(cfg Config, err error) *Provider {
	return &Provider{
		cfg:      cfg,
		startErr: err,
		sessions: make(map[string]*sessionState),
	}
}

// NewWithOptions creates a provider from StartOptions (used by factory).
// The factory requires a zero-arg or StartOptions-only constructor pattern.
// This is a thin wrapper — the actual config is loaded by the CLI and stored
// in a package-level variable before factory call.
var (
	globalConfigMu sync.Mutex
	globalConfig   *Config
	globalProfiles map[string]Config
	globalDefaultAgent string
)

// SetGlobalConfig stores the pony config for the factory path.
func SetGlobalConfig(cfg *Config) {
	globalConfigMu.Lock()
	globalConfig = cfg
	globalProfiles = nil
	globalDefaultAgent = ""
	globalConfigMu.Unlock()
}

// SetGlobalProfiles stores pony profile configs for per-attempt agent resolution.
func SetGlobalProfiles(defaultAgent string, profiles map[string]Config) {
	globalConfigMu.Lock()
	globalConfig = nil
	globalDefaultAgent = defaultAgent
	if profiles == nil {
		globalProfiles = nil
	} else {
		globalProfiles = make(map[string]Config, len(profiles))
		for name, cfg := range profiles {
			globalProfiles[name] = cfg
		}
	}
	globalConfigMu.Unlock()
}

func NewWithOptions(opts runtime.StartOptions) *Provider {
	globalConfigMu.Lock()
	cfg := globalConfig
	profiles := globalProfiles
	defaultAgent := globalDefaultAgent
	globalConfigMu.Unlock()
	if len(profiles) > 0 {
		agent := opts.Agent
		if agent == "" {
			agent = defaultAgent
		}
		selected, ok := profiles[agent]
		if !ok {
			if opts.Agent == "" && defaultAgent != "" {
				if fallback, ok := profiles[defaultAgent]; ok {
					selected = fallback
					ok = true
				}
			}
			if !ok && opts.Agent != "" {
				return newWithStartErr(Config{WorkingDir: opts.Dir}, fmt.Errorf("pony config: unknown profile %q", opts.Agent))
			}
		}
		if ok {
			pcfg := selected
			pcfg.WorkingDir = opts.Dir
			if opts.Model != "" {
				pcfg.Model = opts.Model
			}
			return New(pcfg)
		}
	}
	if cfg == nil {
		return New(Config{
			Model:      opts.Model,
			WorkingDir: opts.Dir,
		})
	}
	// Clone config and apply StartOptions overrides
	pcfg := *cfg
	// Deep copy mutable fields
	if len(cfg.StopConditions) > 0 {
		pcfg.StopConditions = make([]StopCondition, len(cfg.StopConditions))
		copy(pcfg.StopConditions, cfg.StopConditions)
	}
	pcfg.WorkingDir = opts.Dir
	if opts.Model != "" {
		pcfg.Model = opts.Model
	}
	return New(pcfg)
}

func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	if p.startErr != nil {
		return runtime.Session{}, p.startErr
	}
	// Generate a unique session ID
	id := p.sessionCounter.Add(1)
	sessionID := fmt.Sprintf("pony_%d", id)

	ss := newSessionState()
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.runtimeID = opts.RuntimeID

	// 1. System prompt
	if systemPrompt := systemPromptWithToolInstructions(p.cfg.SystemPrompt, p.cfg.LocalTools, p.cfg.OrchTools && p.cfg.Executor != nil); systemPrompt != "" {
		ss.history = append(ss.history, model.Message{
			Role:    model.RoleSystem,
			Content: systemPrompt,
		})
	}

	// 2. AGENTS.md (first-turn user message, before initial prompt)
	if p.cfg.InjectAgentsMD {
		wd := p.cfg.WorkingDir
		if opts.Dir != "" {
			wd = opts.Dir
		}
		if content := LoadAgentsMD(wd); content != "" {
			ss.history = append(ss.history, model.Message{
				Role:    model.RoleUser,
				Content: "## Project conventions\n\n" + content,
			})
		}
	}

	// 3. Initial prompt (first-turn user message)
	if p.cfg.InitialPrompt != "" {
		ss.history = append(ss.history, model.Message{
			Role:    model.RoleUser,
			Content: p.cfg.InitialPrompt,
		})
	}

	ss.initialised = true

	p.mu.Lock()
	p.sessions[sessionID] = ss
	p.mu.Unlock()

	globalSessionsMu.Lock()
	globalSessions[sessionID] = ss
	globalSessionsMu.Unlock()

	return runtime.Session{
		SessionID: sessionID,
		Backend:   backendID,
		Dir:       opts.Dir,
	}, nil
}

func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	p.mu.Lock()
	_, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		globalSessionsMu.Lock()
		_, ok = globalSessions[sessionID]
		globalSessionsMu.Unlock()
	}
	if !ok {
		return runtime.Session{}, fmt.Errorf("unknown session: %s", sessionID)
	}
	return runtime.Session{
		SessionID: sessionID,
		Backend:   backendID,
	}, nil
}

func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	p.mu.Lock()
	ss, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		// Session may have been created by a previous provider (cross-phase resume).
		// Look it up in the global store and adopt it.
		globalSessionsMu.Lock()
		ss, ok = globalSessions[sessionID]
		globalSessionsMu.Unlock()
		if !ok {
			return fmt.Errorf("unknown session: %s", sessionID)
		}
		p.mu.Lock()
		p.sessions[sessionID] = ss
		p.mu.Unlock()
	}

	ss.mu.Lock()
	// Append task prompt as user message
	ss.history = append(ss.history, model.Message{
		Role:    model.RoleUser,
		Content: prompt,
	})

	// Create cancellable context
	promptCtx, cancel := context.WithCancel(ctx)
	ss.cancel = cancel
	history := ss.history
	ss.mu.Unlock()

	// Ensure cancel is cleaned up when Prompt returns
	defer cancel()

	// Inject runtime ID into context for tool access (e.g., send_to_parent).
	promptCtx = context.WithValue(promptCtx, RuntimeIDKey, ss.runtimeID)

	// Build event channel — emit through session state
	eventCh := make(chan events.Event, 256)
	go func() {
		for evt := range eventCh {
			ss.emit(evt)
		}
	}()

	// Run the loop
	var approval ApprovalChecker
	if len(p.cfg.ToolApproval) > 0 {
		approval = p.makeApprovalChecker(ss)
	}
	finalHistory, stopReason, err := LoopWithRetry(
		promptCtx,
		p.cfg.Adapter,
		p.cfg.Model,
		p.cfg.MaxTokens,
		history,
		p.cfg.toolRegistry,
		p.cfg.WorkingDir,
		eventCh,
		p.cfg.StopConditions,
		approval,
	)

	close(eventCh)

	// Emit session.end
	fields := map[string]any{"stop_reason": stopReason}
	if err != nil {
		fields["error_message"] = err.Error()
	}
	ss.emit(events.Event{
		Event:     "session.end",
		SessionID: sessionID,
		Fields:    fields,
	})

	// Store updated history for follow-up prompts
	ss.mu.Lock()
	ss.history = finalHistory
	ss.cancel = nil
	ss.mu.Unlock()

	return err
}

func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	p.mu.Lock()
	ss, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown session: %s", sessionID)
	}
	ss.mu.Lock()
	cancel := ss.cancel
	ss.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (p *Provider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	ss, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown session: %s", sessionID)
	}

	out := make(chan events.Event, 128)
	ss.addSubscriber(out)

	go func() {
		<-ctx.Done()
		ss.removeSubscriber(out)
		close(out)
	}()

	return out, nil
}

// makeApprovalChecker returns an ApprovalChecker that blocks until the
// permission request is answered via AnswerPermission.
func (p *Provider) makeApprovalChecker(ss *sessionState) ApprovalChecker {
	return func(ctx context.Context, toolName string, eventCh chan<- events.Event) (bool, error) {
		requires, ok := p.cfg.ToolApproval[toolName]
		if !ok || !requires {
			return true, nil
		}

		respond := make(chan runtime.PermissionResponse, 1)
		permID := p.permCounter.Add(1)
		requestID := fmt.Sprintf("perm_%s_%d", toolName, permID)

		ss.mu.Lock()
		ss.pendingPerm.requestID = requestID
		ss.pendingPerm.respond = respond
		ss.mu.Unlock()

		defer func() {
			ss.mu.Lock()
			ss.pendingPerm.respond = nil
			ss.mu.Unlock()
		}()

		emit(eventCh, "permission.request", map[string]any{
			"request_id": requestID,
			"tool":       toolName,
			"question":   fmt.Sprintf("Allow %s to execute?", toolName),
			"options": []map[string]any{
				{"optionId": "allow", "kind": "allow"},
				{"optionId": "deny", "kind": "deny"},
			},
		})

		select {
		case resp := <-respond:
			return resp.Allow, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

func (p *Provider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	p.mu.Lock()
	ss, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown session: %s", sessionID)
	}

	ss.mu.Lock()
	respond := ss.pendingPerm.respond
	pendingID := ss.pendingPerm.requestID
	ss.mu.Unlock()

	if respond == nil {
		return fmt.Errorf("no pending permission request for session %s", sessionID)
	}
	if requestID != pendingID {
		return fmt.Errorf("requestID mismatch: got %q, pending %q", requestID, pendingID)
	}

	// Non-blocking send: if the approval checker has already consumed the
	// response (or its context was cancelled and it stopped listening), the
	// send fails without blocking. This prevents goroutine leaks from
	// duplicate or stale AnswerPermission calls.
	select {
	case respond <- response:
		return nil
	default:
		return fmt.Errorf("permission request %q is no longer pending", requestID)
	}
}

func (p *Provider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{
		Backend:             backendID,
		Permissions:         len(p.cfg.ToolApproval) > 0,
		Resume:              false,
		ExternalServerURL:   true,
		SubprocessDiscovery: false,
		ModelSelection:      true,
	}, nil
}
