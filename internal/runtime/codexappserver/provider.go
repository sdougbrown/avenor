package codexappserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

const backendID = "codex-app-server"

// Provider implements runtime.Provider for Codex's app-server JSON-RPC protocol.
type Provider struct {
	opts runtime.StartOptions

	mu      sync.Mutex
	client  *client
	threads map[string]string // sessionID (=threadID) → threadID
	turns   map[string]string // sessionID → current turnID
	efforts map[string]string // sessionID → explicit turn/start effort override

	// startClient constructs a new app-server client. Overridable in tests;
	// defaults to StartClient when nil.
	startClient func(context.Context) (*client, error)

	// Broker fields for channel-wrapped prompt injection.
	broker          *broker.Broker
	runIDs          map[string]string // sessionID → runID
	pollCancels     map[string]context.CancelFunc
	pendingMu       sync.Mutex
	pendingMessages map[string][]string // sessionID → queued wrapped prompts
}

func NewWithOptions(opts runtime.StartOptions) *Provider {
	return &Provider{
		opts:            opts,
		threads:         map[string]string{},
		turns:           map[string]string{},
		efforts:         map[string]string{},
		runIDs:          map[string]string{},
		pollCancels:     map[string]context.CancelFunc{},
		pendingMessages: map[string][]string{},
	}
}

var _ runtime.Provider = (*Provider)(nil)

func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := runtime.MergeStartOptions(p.opts, opts)
	c, err := p.ensureClient(ctx)
	if err != nil {
		return runtime.Session{}, err
	}

	cwd := merged.Dir
	model := merged.Model

	params := threadStartParams{CWD: cwd, Model: model}
	result, err := c.request(ctx, "thread/start", params)
	if err != nil {
		return runtime.Session{}, fmt.Errorf("thread/start: %w", err)
	}

	var tsr threadStartResult
	if err := json.Unmarshal(result, &tsr); err != nil {
		return runtime.Session{}, fmt.Errorf("thread/start result: %w", err)
	}
	threadID := tsr.Thread.ID

	// Register a broker run for this session.
	p.mu.Lock()
	broker := p.broker
	p.mu.Unlock()
	var runID string
	if broker != nil {
		runID = uuid.New().String()
		if _, err := broker.CreateRun(runID); err != nil {
			runID = ""
		}
	}
	p.mu.Lock()
	p.threads[threadID] = threadID
	p.efforts[threadID] = merged.Thinking
	p.runIDs[threadID] = runID
	p.mu.Unlock()

	// Start the broker message polling goroutine.
	if runID != "" {
		pollCtx, cancel := context.WithCancel(ctx)
		p.mu.Lock()
		p.pollCancels[threadID] = cancel
		p.mu.Unlock()
		go p.broker.PollAgentMessages(pollCtx, runID, func(wrapped string) {
			p.pendingMu.Lock()
			p.pendingMessages[threadID] = append(p.pendingMessages[threadID], wrapped)
			p.pendingMu.Unlock()
		})
	}

	return runtime.Session{
		SessionID: threadID,
		Backend:   backendID,
		Dir:       cwd,
	}, nil
}

func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	return p.resume(ctx, sessionID, "")
}

func (p *Provider) ResumeWithOptions(ctx context.Context, sessionID string, opts runtime.StartOptions) (runtime.Session, error) {
	return p.resume(ctx, sessionID, opts.Thinking)
}

func (p *Provider) resume(ctx context.Context, sessionID, effort string) (runtime.Session, error) {
	if sessionID == "" {
		return runtime.Session{}, errors.New("session id is required")
	}

	p.mu.Lock()
	if _, ok := p.threads[sessionID]; ok {
		p.efforts[sessionID] = effort
		p.mu.Unlock()
		return runtime.Session{SessionID: sessionID, Backend: backendID, Dir: p.opts.Dir}, nil
	}
	p.mu.Unlock()

	c, err := p.ensureClient(ctx)
	if err != nil {
		return runtime.Session{}, err
	}

	params := threadResumeParams{ThreadID: sessionID}
	result, err := c.request(ctx, "thread/resume", params)
	if err != nil {
		return runtime.Session{}, fmt.Errorf("thread/resume: %w", err)
	}

	var tsr threadStartResult
	if err := json.Unmarshal(result, &tsr); err != nil {
		return runtime.Session{}, fmt.Errorf("thread/resume result: %w", err)
	}
	threadID := tsr.Thread.ID

	p.mu.Lock()
	p.threads[threadID] = threadID
	p.efforts[threadID] = effort
	p.mu.Unlock()

	return runtime.Session{
		SessionID: threadID,
		Backend:   backendID,
		Dir:       p.opts.Dir,
	}, nil
}

func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	if _, err := p.sessionThread(sessionID); err != nil {
		return err
	}

	c, err := p.getClient()
	if err != nil {
		return err
	}

	p.mu.Lock()
	effort := p.efforts[sessionID]
	p.mu.Unlock()

	params := turnStartParams{
		ThreadID: sessionID,
		Effort:   effort,
		Input: []inputPart{
			{Type: "text", Text: prompt},
		},
	}
	result, err := c.request(ctx, "turn/start", params)
	if err != nil {
		if effort != "" {
			return fmt.Errorf("%s thinking %q: turn/start: %w", backendID, effort, err)
		}
		return fmt.Errorf("turn/start: %w", err)
	}

	var tsr turnStartResult
	if err := json.Unmarshal(result, &tsr); err != nil {
		return fmt.Errorf("turn/start result: %w", err)
	}
	turnID := tsr.Turn.ID

	turnCh := c.registerTurn(turnID)
	p.mu.Lock()
	p.turns[sessionID] = turnID
	p.mu.Unlock()

	defer func() {
		c.unregisterTurn(turnID)
		p.mu.Lock()
		delete(p.turns, sessionID)
		p.mu.Unlock()
	}()

	select {
	case ev, ok := <-turnCh:
		if !ok {
			return errors.New("client closed while waiting for turn completion")
		}
		reason, _ := ev.Fields["stop_reason"].(string)
		switch reason {
		case "end_turn":
			return nil
		case "cancelled":
			return fmt.Errorf("turn cancelled")
		case "error":
			msg, _ := ev.Fields["error_message"].(string)
			if msg == "" {
				msg = "unknown error"
			}
			return fmt.Errorf("turn failed: %s", msg)
		default:
			return fmt.Errorf("turn ended: %s", reason)
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	p.mu.Lock()
	turnID := p.turns[sessionID]
	p.mu.Unlock()
	if turnID == "" {
		return fmt.Errorf("no active turn for session %q", sessionID)
	}

	c, err := p.getClient()
	if err != nil {
		return err
	}

	params := turnInterruptParams{
		ThreadID: sessionID,
		TurnID:   turnID,
	}
	if err := c.notify("turn/interrupt", params); err != nil {
		return fmt.Errorf("turn/interrupt: %w", err)
	}
	return nil
}

func (p *Provider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	c, err := p.getClient()
	if err != nil {
		return nil, err
	}

	out := make(chan events.Event, 128)
	c.subscribe(sessionID, out)
	go func() {
		<-ctx.Done()
		c.unsubscribe(sessionID, out)
		close(out)
	}()
	return out, nil
}

func (p *Provider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	c, err := p.getClient()
	if err != nil {
		return err
	}
	if requestID == "" {
		return errors.New("permission request id is required")
	}
	return c.answerApprovalThreaded(requestID, sessionID, response.Allow)
}

func (p *Provider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{
		Backend:             backendID,
		Permissions:         true,
		Resume:              true,
		ExternalServerURL:   false,
		SubprocessDiscovery: false,
		ModelSelection:      true,
	}, nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	c := p.client
	cancels := make([]context.CancelFunc, 0, len(p.pollCancels))
	for _, cancel := range p.pollCancels {
		cancels = append(cancels, cancel)
	}
	p.client = nil
	p.pollCancels = map[string]context.CancelFunc{}
	p.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	p.mu.Lock()
	p.runIDs = nil
	p.mu.Unlock()
	p.pendingMu.Lock()
	p.pendingMessages = map[string][]string{}
	p.pendingMu.Unlock()
	if c == nil {
		return nil
	}
	return c.Close()
}

func (p *Provider) ensureClient(ctx context.Context) (*client, error) {
	p.mu.Lock()
	if p.client != nil {
		c := p.client
		p.mu.Unlock()
		return c, nil
	}
	// Release p.mu before starting a client. sync.Mutex is not reentrant, so
	// keeping it held here would make the re-lock below (for the double-checked
	// store) deadlock this goroutine against itself. Re-check p.client after
	// re-acquiring.
	p.mu.Unlock()

	starter := p.startClient
	if starter == nil {
		starter = StartClient
	}
	c, err := starter(ctx)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.client != nil {
		// Lost the race: another goroutine started a client first. Close ours
		// to avoid orphaning the codex app-server process it spawned.
		existing := p.client
		p.mu.Unlock()
		_ = c.Close()
		return existing, nil
	}
	// Store broker reference if provided.
	if p.opts.Broker != nil {
		p.broker = p.opts.Broker
	}
	p.client = c
	p.mu.Unlock()
	return c, nil
}

func (p *Provider) sessionThread(sessionID string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	threadID, ok := p.threads[sessionID]
	if !ok {
		return "", fmt.Errorf("session %q not found", sessionID)
	}
	return threadID, nil
}

// getClient returns the current client, or an error if the provider has not been started or was closed.
func (p *Provider) getClient() (*client, error) {
	p.mu.Lock()
	c := p.client
	p.mu.Unlock()
	if c == nil {
		return nil, errors.New("provider has not been started")
	}
	return c, nil
}

// injectPendingMessages injects queued channel-wrapped messages for a session
// at a turn boundary.
func (p *Provider) injectPendingMessages(ctx context.Context, sessionID string) {
	p.pendingMu.Lock()
	msgs := p.pendingMessages[sessionID]
	p.pendingMessages[sessionID] = nil
	p.pendingMu.Unlock()

	if len(msgs) == 0 {
		return
	}
	for _, msg := range msgs {
		if err := p.Prompt(ctx, sessionID, msg); err != nil {
			// Non-fatal: continue with remaining messages.
		}
	}
}
