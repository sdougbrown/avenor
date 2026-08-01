package pi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

const backendID = "pi"

type Provider struct {
	opts        runtime.StartOptions
	mu          sync.Mutex
	client      *client
	sessions    map[string]struct{}
	startClient func(context.Context, runtime.StartOptions) (*client, error)
}

func NewWithOptions(opts runtime.StartOptions) *Provider {
	return &Provider{
		opts:     opts,
		sessions: map[string]struct{}{},
	}
}

var _ runtime.Provider = (*Provider)(nil)

func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := runtime.MergeStartOptions(p.opts, opts)
	if err := runtime.ValidateThinkingForBackend(backendID, merged.Thinking); err != nil {
		return runtime.Session{}, err
	}
	c, fresh, err := p.ensureClient(ctx, merged)
	if err != nil {
		return runtime.Session{}, err
	}
	if !fresh && merged.Thinking != "" {
		if err := p.setThinkingLevel(ctx, c, merged.Thinking); err != nil {
			return runtime.Session{}, err
		}
	}

	stateResult, err := c.sendCommand(ctx, map[string]any{
		"type": "get_state",
	})
	if err != nil {
		return runtime.Session{}, fmt.Errorf("get_state: %w", err)
	}

	var stateMap map[string]any
	if err := json.Unmarshal(stateResult, &stateMap); err != nil {
		return runtime.Session{}, fmt.Errorf("get_state result: %w", err)
	}

	sessionID, _ := stateMap["sessionId"].(string)
	if sessionID == "" {
		if data, ok := stateMap["data"].(map[string]any); ok {
			sessionID, _ = data["sessionId"].(string)
		}
	}
	if sessionID == "" {
		sessionID, _ = stateMap["state"].(string)
	}
	if sessionID == "" {
		sessionID = "pi-" + newRequestID()
		c.stderr.Append("session ID not found in get_state response, generating new session ID")
	}

	p.mu.Lock()
	if len(p.sessions) > 0 {
		p.mu.Unlock()
		return runtime.Session{}, errors.New("pi rpc supports only one active session per provider")
	}
	p.sessions[sessionID] = struct{}{}
	p.mu.Unlock()

	c.setSessionID(sessionID)

	return runtime.Session{
		SessionID: sessionID,
		Backend:   backendID,
		Dir:       merged.Dir,
		PID:       c.Pid(),
	}, nil
}

func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	return p.resume(ctx, sessionID, runtime.StartOptions{}, false)
}

func (p *Provider) ResumeWithOptions(ctx context.Context, sessionID string, opts runtime.StartOptions) (runtime.Session, error) {
	return p.resume(ctx, sessionID, opts, true)
}

func (p *Provider) resume(ctx context.Context, sessionID string, opts runtime.StartOptions, applyThinking bool) (runtime.Session, error) {
	if sessionID == "" {
		return runtime.Session{}, errors.New("session id is required")
	}
	merged := runtime.MergeStartOptions(p.opts, opts)
	if !applyThinking {
		merged.Thinking = ""
	}
	if err := runtime.ValidateThinkingForBackend(backendID, merged.Thinking); err != nil {
		return runtime.Session{}, err
	}

	p.mu.Lock()
	_, exists := p.sessions[sessionID]
	c := p.client
	p.mu.Unlock()
	if exists {
		if merged.Thinking != "" {
			if c == nil {
				return runtime.Session{}, errors.New("provider has not been started")
			}
			if err := p.setThinkingLevel(ctx, c, merged.Thinking); err != nil {
				return runtime.Session{}, err
			}
		}
		var pid int
		if c != nil {
			pid = c.Pid()
		}
		return runtime.Session{SessionID: sessionID, Backend: backendID, Dir: merged.Dir, PID: pid}, nil
	}

	c, fresh, err := p.ensureClient(ctx, merged)
	if err != nil {
		return runtime.Session{}, err
	}
	if !fresh && merged.Thinking != "" {
		if err := p.setThinkingLevel(ctx, c, merged.Thinking); err != nil {
			return runtime.Session{}, err
		}
	}

	p.mu.Lock()
	if len(p.sessions) > 0 {
		p.mu.Unlock()
		return runtime.Session{}, errors.New("pi rpc supports only one active session per provider")
	}
	p.sessions[sessionID] = struct{}{}
	p.mu.Unlock()

	c.setSessionID(sessionID)

	return runtime.Session{
		SessionID: sessionID,
		Backend:   backendID,
		Dir:       merged.Dir,
		PID:       c.Pid(),
	}, nil
}

func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	if _, err := p.sessionExists(sessionID); err != nil {
		return err
	}

	c, err := p.getClient()
	if err != nil {
		return err
	}

	turnCh := make(chan events.Event, 1)

	subCh := make(chan events.Event, 128)
	c.subscribe(sessionID, subCh)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range subCh {
			if ev.Event == "session.end" {
				select {
				case turnCh <- ev:
				default:
				}
				return
			}
		}
	}()

	cmd := map[string]any{
		"type":    "prompt",
		"message": prompt,
		"id":      newRequestID(),
	}
	_, err = c.sendCommand(ctx, cmd)
	if err != nil {
		c.unsubscribe(sessionID, subCh)
		close(subCh)
		<-done
		return fmt.Errorf("send prompt: %w", err)
	}

	select {
	case ev := <-turnCh:
		c.unsubscribe(sessionID, subCh)
		close(subCh)
		<-done

		reason, _ := ev.Fields["stop_reason"]
		switch reason {
		case "end_turn", "end_of_turn":
			return nil
		case "cancelled":
			return fmt.Errorf("turn cancelled for session %s", sessionID)
		case "error":
			msg, _ := ev.Fields["error_message"].(string)
			if msg == "" {
				msg = "unknown error"
			}
			return fmt.Errorf("turn failed for session %s: %s", sessionID, msg)
		default:
			return nil
		}
	case <-ctx.Done():
		c.unsubscribe(sessionID, subCh)
		close(subCh)
		<-done
		return ctx.Err()
	}
}

func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	if _, err := p.sessionExists(sessionID); err != nil {
		return err
	}

	c, err := p.getClient()
	if err != nil {
		return err
	}

	_, err = c.sendCommand(ctx, map[string]any{
		"type": "abort",
	})
	if err != nil {
		return fmt.Errorf("send abort: %w", err)
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

	c.mu.Lock()
	approval, ok := c.approvals[requestID]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("extension UI request %q not found", requestID)
	}
	if approval.sessionID != "" && approval.sessionID != sessionID {
		c.mu.Unlock()
		return fmt.Errorf("approval request %q belongs to session %q, not %q", requestID, approval.sessionID, sessionID)
	}
	delete(c.approvals, requestID)
	c.mu.Unlock()

	resp := map[string]any{
		"cancelled": false,
	}

	if !response.Allow {
		resp["cancelled"] = true
	} else {
		switch approval.method {
		case "select":
			resp["value"] = response.OptionID
			if response.Message != "" {
				resp["message"] = response.Message
			}
		case "confirm":
			resp["confirmed"] = true
		case "input", "editor":
			resp["value"] = response.Message
			if response.Message == "" {
				resp["value"] = response.OptionID
			}
		}
	}

	return c.answerExtensionUI(approval.rawID, approval.method, resp)
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
	p.client = nil
	p.mu.Unlock()
	if c == nil {
		return nil
	}
	return c.Close()
}

func (p *Provider) ensureClient(ctx context.Context, opts runtime.StartOptions) (*client, bool, error) {
	p.mu.Lock()
	if p.client != nil {
		c := p.client
		p.mu.Unlock()
		return c, false, nil
	}
	if opts.Thinking != "" {
		if err := checkThinkingCapability(ctx); err != nil {
			p.mu.Unlock()
			return nil, false, fmt.Errorf("%w: %v", runtime.NewUnsupportedThinkingError(backendID), err)
		}
	}

	starter := p.startClient
	if starter == nil {
		starter = func(ctx context.Context, opts runtime.StartOptions) (*client, error) {
			return StartClientWithAgentProfileThinkingAndDir(
				ctx, "", opts.Model, "", opts.Agent, opts.AgentProfile, opts.Thinking, opts.Dir,
			)
		}
	}
	c, err := starter(ctx, opts)
	if err != nil {
		p.mu.Unlock()
		return nil, false, err
	}
	p.client = c
	p.mu.Unlock()
	return c, true, nil
}

func (p *Provider) setThinkingLevel(ctx context.Context, c *client, level string) error {
	result, err := c.sendCommand(ctx, map[string]any{
		"type":  "set_thinking_level",
		"level": level,
	})
	if err != nil {
		return fmt.Errorf("pi set_thinking_level %q: %w", level, err)
	}
	var response struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return fmt.Errorf("decode pi set_thinking_level %q response: %w", level, err)
	}
	if !response.Success {
		if response.Error == "" {
			response.Error = "unsuccessful response"
		}
		return fmt.Errorf("pi set_thinking_level %q rejected: %s", level, response.Error)
	}
	return nil
}

func (p *Provider) sessionExists(sessionID string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.sessions[sessionID]
	if !ok {
		return "", fmt.Errorf("session %q not found", sessionID)
	}
	return sessionID, nil
}

func (p *Provider) getClient() (*client, error) {
	p.mu.Lock()
	c := p.client
	p.mu.Unlock()
	if c == nil {
		return nil, errors.New("provider has not been started")
	}
	return c, nil
}
