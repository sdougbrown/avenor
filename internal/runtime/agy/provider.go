package agy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

var _ runtime.Provider = (*Provider)(nil)

// ClientFactory is a function that creates a new agy client.
type ClientFactory func() *client

// defaultClientFactory creates a client using newClient with nil pipes.
func defaultClientFactory() *client {
	return newClient(nil, nil, nil, nil)
}

// Provider implements the runtime.Provider interface for agy-based sessions.
type Provider struct {
	opts          runtime.StartOptions
	mu            sync.Mutex
	sessions      map[string]*sessionState
	version       string
	versionOnce   sync.Once
	clientFactory ClientFactory
}

// NewWithOptions creates a new Provider with the given start options.
func NewWithOptions(opts runtime.StartOptions) *Provider {
	return &Provider{
		opts:          opts,
		sessions:      make(map[string]*sessionState),
		clientFactory: defaultClientFactory,
	}
}

// WithClientFactory returns a provider that uses the given factory to create clients.
// This is primarily useful for testing.
func (p *Provider) WithClientFactory(factory ClientFactory) *Provider {
	p.clientFactory = factory
	return p
}

type sessionState struct {
	client       *client
	sessionID    string
	startOpts    runtime.StartOptions
	initCache    map[string]any
	startEmitted bool
	firstTurn    bool
	running      bool
	subs         []chan events.Event
	subsMu       sync.Mutex
}

// ensureVersion caches the agy version via sync.Once.
// In test mode, the version is set by the test client.
func (p *Provider) ensureVersion(ctx context.Context) {
	p.versionOnce.Do(func() {
		c := p.clientFactory()
		c.ensureVersion(ctx)
		p.mu.Lock()
		p.version = c.version
		p.mu.Unlock()
	})
}

// Start creates a new agy subprocess and registers a session.
// The session.start event is cached and published on first Prompt, not here.
func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	if opts.Model == "" && opts.Agent == "" && opts.Dir == "" {
		opts = p.opts
	}

	c := p.clientFactory()
	c.version = p.version

	// Detect test mode: if client already has pipes, use pipe-based startup.
	c.mu.Lock()
	hasPipes := c.stdin != nil || c.stdout != nil
	c.mu.Unlock()

	if hasPipes {
		c.mu.Lock()
		c.testMode = true
		c.mu.Unlock()
	}

	p.ensureVersion(ctx)

	var info sessionInfo
	var err error
	if hasPipes {
		// Test mode: use pre-existing pipes.
		c.mu.Lock()
		stdin := c.stdin
		stdout := c.stdout
		c.mu.Unlock()
		info, err = c.StartWithPipes(ctx, stdin, stdout, nil)
		if err != nil {
			return runtime.Session{}, err
		}
	} else {
		info, err = c.Start(ctx, opts.Model, opts.Agent)
		if err != nil {
			return runtime.Session{}, err
		}
	}

	s := &sessionState{
		client:       c,
		sessionID:    info.ConversationID,
		startOpts:    opts,
		firstTurn:    true,
		startEmitted: false,
		running:      false,
	}
	// Copy initCache from client (populated by readLoop from init event).
	s.subsMu.Lock()
	if c.initCache != nil {
		s.initCache = make(map[string]any, len(c.initCache))
		for k, v := range c.initCache {
			s.initCache[k] = v
		}
	}
	s.subsMu.Unlock()

	p.mu.Lock()
	p.sessions[info.ConversationID] = s
	p.mu.Unlock()

	return runtime.Session{
		SessionID: info.ConversationID,
		Backend:   BackendID,
		Dir:       opts.Dir,
	}, nil
}

// Resume registers a logical session with no active process.
// The next Prompt will start a resumed agy process.
func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	if sessionID == "" {
		return runtime.Session{}, errors.New("session id is required")
	}

	p.mu.Lock()
	if s, ok := p.sessions[sessionID]; ok {
		p.mu.Unlock()
		return runtime.Session{
			SessionID: sessionID,
			Backend:   BackendID,
			Dir:       s.startOpts.Dir,
		}, nil
	}

	s := &sessionState{
		sessionID:    sessionID,
		startOpts:    p.opts,
		initCache:    make(map[string]any),
		firstTurn:    true,
		startEmitted: false,
		running:      false,
	}
	s.initCache["conversation_id"] = sessionID
	p.sessions[sessionID] = s
	p.mu.Unlock()

	return runtime.Session{
		SessionID: sessionID,
		Backend:   BackendID,
		Dir:       p.opts.Dir,
	}, nil
}

// Prompt executes a prompt on the given session.
// On first turn, publishes cached session.start then calls FirstPrompt.
// On subsequent turns, creates a new client and calls ResumePrompt.
func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	s := p.getSession(sessionID)
	if s == nil {
		return fmt.Errorf("session %q not found", sessionID)
	}

	s.subsMu.Lock()
	if s.running {
		s.subsMu.Unlock()
		return fmt.Errorf("prompt already active for session %q", sessionID)
	}
	s.running = true
	s.subsMu.Unlock()

	defer func() {
		s.subsMu.Lock()
		s.running = false
		s.subsMu.Unlock()
	}()

	if s.firstTurn {
		s.subsMu.Lock()
		if !s.startEmitted {
			startEvt := events.Event{
				Event:     "session.start",
				SessionID: sessionID,
				Fields: map[string]any{
					"backend":         BackendID,
					"model":           s.initCache["model"],
					"agy_version":     s.initCache["agy_version"],
					"conversation_id": s.initCache["conversation_id"],
				},
			}
			for _, ch := range s.subs {
				select {
				case ch <- startEvt:
				default:
				}
			}
			s.startEmitted = true
		}
		s.subsMu.Unlock()

		err := s.client.FirstPrompt(ctx, prompt)
		if err != nil {
			return err
		}

		s.subsMu.Lock()
		s.firstTurn = false
		s.subsMu.Unlock()
	} else {
		c := p.clientFactory()
		c.version = p.version

		// Detect test mode: if client already has pipes, use pipe-based resume.
		c.mu.Lock()
		hasPipes := c.stdin != nil || c.stdout != nil
		c.mu.Unlock()

		var err error
		if hasPipes {
			c.mu.Lock()
			c.testMode = true
			c.mu.Unlock()
			err = c.validateAndReadResult(ctx, s.sessionID, prompt)
		} else {
			err = c.ResumePrompt(ctx, s.sessionID, prompt, s.startOpts.Model, s.startOpts.Agent)
		}
		if err != nil {
			_ = c.Close()
			return err
		}

		s.subsMu.Lock()
		s.client = c
		s.initCache = c.initCache
		s.subsMu.Unlock()
	}

	// Clean up and deliver terminal session.end event.
	termEvt := p.buildTerminalEvent(s)
	if termEvt != nil {
		s.subsMu.Lock()
		for _, ch := range s.subs {
			select {
			case ch <- *termEvt:
			default:
			}
		}
		s.subsMu.Unlock()
	}

	_ = s.client.Close()
	s.subsMu.Lock()
	s.client = nil
	s.subsMu.Unlock()

	return nil
}

// Cancel cancels the active process for a session and emits session.end.
func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	s := p.getSession(sessionID)
	if s == nil {
		return fmt.Errorf("session %q not found", sessionID)
	}

	var cancelErr error
	s.subsMu.Lock()
	if s.client != nil {
		cancelErr = s.client.Cancel(ctx)
	}
	s.subsMu.Unlock()

	endEvt := events.Event{
		Event:     "session.end",
		SessionID: sessionID,
		Fields: map[string]any{
			"stop_reason": "cancelled",
		},
	}
	s.subsMu.Lock()
	for _, ch := range s.subs {
		select {
		case ch <- endEvt:
		default:
		}
	}
	s.running = false
	s.subsMu.Unlock()

	return cancelErr
}

// Events subscribes to events for a session.
// Returns a buffered channel (128 capacity). When the context is cancelled,
// the subscriber is removed and the channel is closed.
func (p *Provider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	s := p.getSession(sessionID)
	if s == nil {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	ch := make(chan events.Event, 128)
	s.subsMu.Lock()
	s.subs = append(s.subs, ch)
	s.subsMu.Unlock()

	go func() {
		defer func() {
			s.subsMu.Lock()
			for i, sub := range s.subs {
				if sub == ch {
					s.subs = append(s.subs[:i], s.subs[i+1:]...)
					break
				}
			}
			s.subsMu.Unlock()
			close(ch)
		}()

		s.subsMu.Lock()
		cl := s.client
		s.subsMu.Unlock()

		for cl != nil {
			select {
			case evt, ok := <-cl.Events():
				if !ok {
					return
				}
				select {
				case ch <- evt:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// AnswerPermission is not supported in agy headless mode.
func (p *Provider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	return errors.New("AnswerPermission is not supported in agy headless mode")
}

// Capabilities returns the capabilities of the agy backend.
func (p *Provider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{
		Backend:             BackendID,
		Permissions:         false,
		Resume:              true,
		ExternalServerURL:   false,
		SubprocessDiscovery: false,
		ModelSelection:      true,
	}, nil
}

// Close shuts down the provider and all sessions.
func (p *Provider) Close() error {
	p.mu.Lock()
	sessions := make(map[string]*sessionState)
	for id, s := range p.sessions {
		sessions[id] = s
	}
	p.sessions = make(map[string]*sessionState)
	p.mu.Unlock()

	var firstErr error
	for _, s := range sessions {
		s.subsMu.Lock()
		cl := s.client
		subs := make([]chan events.Event, len(s.subs))
		copy(subs, s.subs)
		s.subsMu.Unlock()

		if cl != nil {
			if cl.proc != nil && cl.proc.Process != nil {
				if err := cl.Cancel(context.Background()); err != nil {
					if firstErr == nil {
						firstErr = fmt.Errorf("cancel session %q: %w", s.sessionID, err)
					}
				}
			}
			if err := cl.Close(); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("close session %q: %w", s.sessionID, err)
				}
			}
		}

		for _, ch := range subs {
			close(ch)
		}
	}

	return firstErr
}

func (p *Provider) getSession(sessionID string) *sessionState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sessions[sessionID]
}

func (p *Provider) buildTerminalEvent(s *sessionState) *events.Event {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	return &events.Event{
		Event:     "session.end",
		SessionID: s.sessionID,
		Fields: map[string]any{
			"stop_reason": "end_turn",
		},
	}
}
