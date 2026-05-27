package pony

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/pony/model"
	"github.com/sdougbrown/avenor/internal/runtime/pony/tools"
)

// backendID is the backend identifier.
const backendID = "pony"

// Config configures the pony provider.
type Config struct {
	Adapter        model.Adapter
	Model          string
	MaxTokens      int
	SystemPrompt   string
	InitialPrompt  string
	Executor       OrchestratorExecutor // nil when OrchTools is false
	LocalTools     bool
	OrchTools      bool
	WorkingDir     string // for AGENTS.md discovery
	InjectAgentsMD bool
	// Registry is internal; built from tool config.
	toolRegistry *tools.Registry

	// StopConditions for the loop.
	StopConditions []StopCondition
}

// OrchestratorExecutor wraps the control-socket client for orchestration tools.
type OrchestratorExecutor interface {
	SpawnAgent(ctx context.Context, params map[string]any) (sessionID string, err error)
	SendPrompt(ctx context.Context, sessionID, prompt string) error
	GetStatus(ctx context.Context, sessionID string) (map[string]any, error)
	WaitForDone(ctx context.Context, sessionID string) error
}

// Provider implements runtime.Provider for the pony backend.
type Provider struct {
	cfg Config

	mu             sync.Mutex
	sessions       map[string]*sessionState
	sessionCounter atomic.Int64
}

var _ runtime.Provider = (*Provider)(nil)

// New creates a new pony provider with the given config.
func New(cfg Config) *Provider {
	// Build tool registry based on config
	var toolList []tools.Tool
	if cfg.LocalTools {
		toolList = append(toolList,
			tools.NewFileReadTool(),
			tools.NewFileWriteTool(),
			tools.NewFileEditTool(),
			tools.NewGlobTool(),
			tools.NewGrepTool(),
			tools.NewListDirTool(),
			tools.NewShellTool(),
		)
	}
	if cfg.OrchTools && cfg.Executor != nil {
		toolList = append(toolList, newOrchestrationTools(cfg.Executor)...)
	}
	cfg.toolRegistry = tools.NewRegistry(toolList)

	return &Provider{
		cfg:      cfg,
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
)

// SetGlobalConfig stores the pony config for the factory path.
func SetGlobalConfig(cfg *Config) {
	globalConfigMu.Lock()
	globalConfig = cfg
	globalConfigMu.Unlock()
}

func NewWithOptions(opts runtime.StartOptions) *Provider {
	globalConfigMu.Lock()
	cfg := globalConfig
	globalConfigMu.Unlock()
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
	// Generate a unique session ID
	id := p.sessionCounter.Add(1)
	sessionID := fmt.Sprintf("pony_%d", id)

	ss := newSessionState()
	ss.mu.Lock()
	defer ss.mu.Unlock()

	// 1. System prompt
	if p.cfg.SystemPrompt != "" {
		ss.history = append(ss.history, model.Message{
			Role:    model.RoleSystem,
			Content: p.cfg.SystemPrompt,
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

	return runtime.Session{
		SessionID: sessionID,
		Backend:   backendID,
		Dir:       opts.Dir,
	}, nil
}

func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	return runtime.Session{}, errors.New("resume not supported by pony backend")
}

func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	p.mu.Lock()
	ss, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown session: %s", sessionID)
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

	// Build event channel — emit through session state
	eventCh := make(chan events.Event, 256)
	go func() {
		for evt := range eventCh {
			ss.emit(evt)
		}
	}()

	// Run the loop
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

func (p *Provider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	return errors.New("permissions not supported by pony backend")
}

func (p *Provider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{
		Backend:             backendID,
		Permissions:         false,
		Resume:              false,
		ExternalServerURL:   true,
		SubprocessDiscovery: false,
		ModelSelection:      true,
	}, nil
}
