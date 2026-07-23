package agy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

var _ runtime.Provider = (*Provider)(nil)

const subscriberBuf = 128

type clientFactory func() *client

// defaultClientFactory creates a client using newClient with nil pipes.
func defaultClientFactory() *client {
	return newClient(nil, nil, nil, nil)
}

// Provider implements the runtime.Provider interface for agy-based sessions.
type Provider struct {
	opts              runtime.StartOptions
	mu                sync.Mutex
	sessions          map[string]*sessionState
	version           string
	versionErr        error
	versionOnce       sync.Once
	clientFactory     clientFactory
	ptyRPCHostFactory ptyRPCHostFactory // Stage 18 selects this seam; Phase 1 never invokes it.
}

// NewWithOptions creates a new Provider with the given start options.
func NewWithOptions(opts runtime.StartOptions) *Provider {
	return &Provider{
		opts:              opts,
		sessions:          make(map[string]*sessionState),
		clientFactory:     defaultClientFactory,
		ptyRPCHostFactory: defaultPTYRPCHostFactory,
	}
}

// sessionState holds the state for one logical conversation session.
type sessionState struct {
	mu sync.Mutex

	client    *client
	rpcHost   *ptyRPCHost // Reserved for an already-probed Stage 18 RPC selection.
	sessionID string
	startOpts runtime.StartOptions
	initCache map[string]any

	startEmitted bool
	firstTurn    bool
	running      bool
	closed       bool
	externalID   string

	subs   []*subscriber
	subsMu sync.Mutex

	// cancelled and terminalEmitted are reset at the start of each turn.
	cancelled       bool
	terminalEmitted bool
}

type subscriber struct {
	ch        chan events.Event
	ctxDone   <-chan struct{}
	stop      chan struct{}
	closeOnce sync.Once
	mu        sync.RWMutex
	closed    bool
}

func newSubscriber(ctx context.Context) *subscriber {
	return &subscriber{
		ch:      make(chan events.Event, subscriberBuf),
		ctxDone: ctx.Done(),
		stop:    make(chan struct{}),
	}
}

func (sub *subscriber) send(evt events.Event) {
	sub.mu.RLock()
	defer sub.mu.RUnlock()
	if sub.closed {
		return
	}
	select {
	case sub.ch <- evt:
	case <-sub.ctxDone:
	case <-sub.stop:
	}
}

func (sub *subscriber) close() {
	sub.closeOnce.Do(func() {
		close(sub.stop)
		sub.mu.Lock()
		sub.closed = true
		close(sub.ch)
		sub.mu.Unlock()
	})
}

// emit preserves event order for every live subscriber. A subscriber that is
// no longer consuming is released by either its context or provider shutdown.
func (s *sessionState) emit(evt events.Event) {
	s.subsMu.Lock()
	subs := append([]*subscriber(nil), s.subs...)
	s.subsMu.Unlock()
	for _, sub := range subs {
		sub.send(evt)
	}
}

func (s *sessionState) emitTerminal(evt events.Event) bool {
	s.mu.Lock()
	if s.terminalEmitted {
		s.mu.Unlock()
		return false
	}
	s.terminalEmitted = true
	s.mu.Unlock()
	s.emit(evt)
	return true
}

func (s *sessionState) addSubscriber(sub *subscriber) {
	s.subsMu.Lock()
	s.subs = append(s.subs, sub)
	s.subsMu.Unlock()
}

func (s *sessionState) removeSubscriber(target *subscriber) {
	s.subsMu.Lock()
	for i, sub := range s.subs {
		if sub == target {
			s.subs = append(s.subs[:i], s.subs[i+1:]...)
			break
		}
	}
	s.subsMu.Unlock()
	target.close()
}

func (s *sessionState) closeSubscribers() {
	s.subsMu.Lock()
	subs := s.subs
	s.subs = nil
	s.subsMu.Unlock()
	for _, sub := range subs {
		sub.close()
	}
}

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

func (p *Provider) ensureVersion(ctx context.Context) error {
	p.versionOnce.Do(func() {
		c := p.clientFactory()
		c.ensureVersion(ctx)
		p.mu.Lock()
		p.version = c.version
		p.versionErr = c.versionErr
		p.mu.Unlock()
	})
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.versionErr
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

// Start registers a resource-free provisional session. agy allocates its
// external conversation ID only after --print receives the first prompt, so
// Prompt launches the process and atomically adopts that external ID.
func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := runtime.MergeStartOptions(p.opts, opts)
	if err := p.ensureVersion(ctx); err != nil {
		return runtime.Session{}, err
	}

	provisionalID := "agy-pending-" + uuid.New().String()
	s := &sessionState{
		sessionID: provisionalID,
		startOpts: merged,
		initCache: map[string]any{
			"model":       merged.Model,
			"agy_version": p.version,
		},
		firstTurn: true,
	}

	p.mu.Lock()
	p.sessions[provisionalID] = s
	p.mu.Unlock()

	return runtime.Session{
		SessionID: provisionalID,
		Backend:   BackendID,
		Dir:       merged.Dir,
	}, nil
}

// ---------------------------------------------------------------------------
// Resume
// ---------------------------------------------------------------------------

// Resume registers a logical session with no active process.
// The next Prompt will start a resumed agy process.
// Resume sets firstTurn=true but client=nil so that Prompt uses the resumed
// command construction path rather than the "write to pending process stdin" path.
func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	if sessionID == "" {
		return runtime.Session{}, errors.New("session id is required")
	}
	if err := p.ensureVersion(ctx); err != nil {
		return runtime.Session{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if s, ok := p.sessions[sessionID]; ok {
		return runtime.Session{
			SessionID: sessionID,
			Backend:   BackendID,
			Dir:       s.startOpts.Dir,
		}, nil
	}

	s := &sessionState{
		sessionID:  sessionID,
		externalID: sessionID,
		startOpts:  p.opts,
		initCache: map[string]any{
			"conversation_id": sessionID,
			"model":           p.opts.Model,
			"agy_version":     p.version,
		},
		firstTurn: true,
	}
	p.sessions[sessionID] = s

	return runtime.Session{
		SessionID: sessionID,
		Backend:   BackendID,
		Dir:       p.opts.Dir,
	}, nil
}

// ---------------------------------------------------------------------------
// Prompt
// ---------------------------------------------------------------------------

// Prompt executes a turn on the given session.
//
// First turn from Start: publishes the cached session.start, writes the prompt
// to the pending process's stdin, closes stdin, then relays all parsed events
// to subscribers until a terminal session.end.
//
// Resumed first turn (Resume + Prompt): creates a new agy process with
// --conversation <id> --print <prompt>, validates the init, then relays all
// events to subscribers without emitting a duplicate session.start.
//
// Subsequent turns: same as resumed first turn (creates a new process).
//
// Prompt blocks until a terminal session.end arrives, process failure,
// context cancellation, or explicit Cancel. Exactly one session.end is
// emitted per turn.
func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	s := p.getSession(sessionID)
	if s == nil {
		return fmt.Errorf("session %q not found", sessionID)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("session %q is closed", sessionID)
	}
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("prompt already active for session %q", sessionID)
	}
	s.running = true
	s.cancelled = false
	s.terminalEmitted = false
	s.mu.Unlock()

	// On return, mark the session as no longer running, clean up the turn
	// process, and remove the provisional alias after successful ID adoption.
	defer func() {
		s.mu.Lock()
		s.running = false
		cl := s.client
		s.client = nil
		currentID := s.sessionID
		s.mu.Unlock()
		if cl != nil {
			_ = cl.Close()
		}
		if currentID != sessionID {
			p.mu.Lock()
			if p.sessions[sessionID] == s {
				delete(p.sessions, sessionID)
			}
			p.mu.Unlock()
		}
	}()

	s.mu.Lock()
	firstTurn := s.firstTurn
	externalID := s.externalID
	s.mu.Unlock()

	var err error
	switch {
	case firstTurn && externalID == "":
		err = p.runInitialPrompt(ctx, s, sessionID, prompt)
	case firstTurn:
		err = p.runResumedPrompt(ctx, s, externalID, prompt, true)
	default:
		err = p.runResumedPrompt(ctx, s, externalID, prompt, false)
	}

	if err != nil {
		s.mu.Lock()
		cancelled := s.cancelled
		s.mu.Unlock()
		if !cancelled {
			stopReason := "error"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				stopReason = "cancelled"
			}
			s.mu.Lock()
			currentID := s.sessionID
			s.mu.Unlock()
			s.emitTerminal(events.Event{
				Event:     "session.end",
				SessionID: currentID,
				Fields: map[string]any{
					"stop_reason": stopReason,
					"error":       err.Error(),
				},
			})
		}
	}
	return err
}

// runInitialPrompt launches agy with the first prompt, adopts the external
// conversation ID from init, and then relays the turn.
func (p *Provider) runInitialPrompt(ctx context.Context, s *sessionState, provisionalID, prompt string) error {
	cl := p.clientFactory()
	cl.version = p.version

	s.mu.Lock()
	s.client = cl
	startOpts := s.startOpts
	s.mu.Unlock()

	cl.mu.Lock()
	testMode := cl.testMode
	hasPipes := cl.procState.stdout != nil
	cl.mu.Unlock()

	var (
		info sessionInfo
		err  error
	)
	if hasPipes || testMode {
		cl.testMode = true
		info, err = cl.waitForInit(ctx)
		if err == nil {
			cl.mu.Lock()
			cl.mode = "running"
			cl.mu.Unlock()
		}
	} else {
		info, err = cl.LaunchInitial(ctx, prompt, startOpts.Model, startOpts.Agent, startOpts.Dir)
	}
	if err != nil {
		return err
	}
	if info.ConversationID == "" {
		return errors.New("agy: init did not include conversation_id")
	}
	if info.Model == "" {
		info.Model = startOpts.Model
	}
	if info.AgyVersion == "" {
		info.AgyVersion = p.version
	}

	if err := p.adoptSessionID(provisionalID, info, s); err != nil {
		return err
	}

	s.mu.Lock()
	if s.cancelled {
		s.mu.Unlock()
		return context.Canceled
	}
	startEvt := s.buildSessionStart()
	s.startEmitted = true
	s.mu.Unlock()
	s.emit(startEvt)

	termEvt, err := relayEvents(ctx, s, cl)
	if err != nil {
		_ = cl.Close()
		return cl.withStderr(err)
	}
	s.mu.Lock()
	s.firstTurn = false
	s.mu.Unlock()
	if termEvt != nil {
		s.emitTerminal(*termEvt)
	}
	return nil
}

// runResumedPrompt starts a fresh agy process for an existing conversation,
// validates its init event, and relays the new turn without replaying history.
func (p *Provider) runResumedPrompt(ctx context.Context, s *sessionState, sessionID, prompt string, isFirstResumed bool) error {
	s.mu.Lock()
	oldClient := s.client
	s.client = nil
	startOpts := s.startOpts
	s.mu.Unlock()
	if oldClient != nil {
		_ = oldClient.Close()
	}

	cl := p.clientFactory()
	cl.version = p.version

	s.mu.Lock()
	s.client = cl // Publish before launch so concurrent Cancel can stop it.
	s.mu.Unlock()

	cl.mu.Lock()
	testMode := cl.testMode
	hasPipes := cl.procState.stdout != nil
	cl.mu.Unlock()

	var err error
	if hasPipes || testMode {
		cl.testMode = true
		err = cl.validateInit(ctx, sessionID)
	} else {
		err = cl.LaunchResumed(ctx, sessionID, prompt, startOpts.Model, startOpts.Agent, startOpts.Dir)
	}
	if err != nil {
		_ = cl.Close()
		return err
	}

	var startEvt *events.Event
	s.mu.Lock()
	if s.cancelled {
		s.mu.Unlock()
		_ = cl.Close()
		return context.Canceled
	}
	if isFirstResumed && !s.startEmitted {
		evt := s.buildSessionStart()
		startEvt = &evt
		s.startEmitted = true
	}
	s.mu.Unlock()
	if startEvt != nil {
		s.emit(*startEvt)
	}

	termEvt, err := relayEvents(ctx, s, cl)
	if err != nil {
		_ = cl.Close()
		return cl.withStderr(err)
	}

	s.mu.Lock()
	s.firstTurn = false
	s.mu.Unlock()
	if termEvt != nil {
		s.emitTerminal(*termEvt)
	}
	return nil
}

// relayEvents reads from the client's events channel and forwards every
// event to the session's subscribers. It returns when it encounters a
// session.end event, the context is cancelled, or the events channel is
// closed (process exit).
//
// Exactly the returned session.end (if any) should be emitted by the caller.
// Returns nil for context cancellation and process-death paths.
func relayEvents(ctx context.Context, s *sessionState, cl *client) (*events.Event, error) {
	handle := func(evt events.Event) *events.Event {
		if evt.Event == "session.end" {
			return &evt
		}
		s.emit(evt)
		return nil
	}

	for {
		select {
		case evt, ok := <-cl.Events():
			if !ok {
				return nil, cl.processError(ErrProcessDied)
			}
			if terminal := handle(evt); terminal != nil {
				return terminal, nil
			}
		case <-cl.doneCh:
			// readLoop closes doneCh only after it has enqueued every parsed
			// event, so drain the channel before classifying an early exit.
			for {
				select {
				case evt, ok := <-cl.Events():
					if !ok {
						return nil, cl.processError(ErrProcessDied)
					}
					if terminal := handle(evt); terminal != nil {
						return terminal, nil
					}
				default:
					return nil, cl.processError(ErrProcessDied)
				}
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// buildSessionStart constructs the session.start event from the cached init
// metadata. Does not emit it.
func (s *sessionState) buildSessionStart() events.Event {
	model, _ := s.initCache["model"].(string)
	agyVersion, _ := s.initCache["agy_version"].(string)
	convID, _ := s.initCache["conversation_id"].(string)

	return events.Event{
		Event:     "session.start",
		SessionID: s.sessionID,
		Fields: map[string]any{
			"backend":         BackendID,
			"model":           model,
			"agy_version":     agyVersion,
			"conversation_id": convID,
		},
	}
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

// Cancel interrupts the active process for the given session and emits a
// cancelled session.end event. Cancel is idempotent per session — subsequent
// calls are no-ops.
func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	s := p.getSession(sessionID)
	if s == nil {
		return fmt.Errorf("session %q not found", sessionID)
	}

	s.mu.Lock()
	cl := s.client
	if s.cancelled {
		s.mu.Unlock()
		return nil
	}
	s.cancelled = true
	s.mu.Unlock()

	var cancelErr error
	if cl != nil {
		cancelErr = cl.Cancel(ctx)
		if errors.Is(cancelErr, ErrClientClosed) {
			cancelErr = nil
		}
	}

	endEvt := events.Event{
		Event:     "session.end",
		SessionID: sessionID,
		Fields: map[string]any{
			"stop_reason": "cancelled",
		},
	}
	s.emitTerminal(endEvt)
	return cancelErr
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

// Events subscribes to events for a session. The returned channel receives all
// events produced during Prompt calls (including session.start on first turn
// and session.end at completion).
//
// The channel is buffered to 128. When the provided context is cancelled, the
// subscriber is removed and the channel is closed.
func (p *Provider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	s := p.getSession(sessionID)
	if s == nil {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	sub := newSubscriber(ctx)
	s.addSubscriber(sub)

	go func() {
		<-ctx.Done()
		s.removeSubscriber(sub)
	}()

	return sub.ch, nil
}

// ---------------------------------------------------------------------------
// AnswerPermission
// ---------------------------------------------------------------------------

// AnswerPermission is not supported in agy headless mode.
func (p *Provider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	return errors.New("AnswerPermission is not supported in agy headless mode")
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

// Close shuts down the provider and all sessions. Idempotent.
func (p *Provider) Close() error {
	p.mu.Lock()
	sessions := make([]*sessionState, 0, len(p.sessions))
	seen := make(map[*sessionState]struct{}, len(p.sessions))
	for _, s := range p.sessions {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		sessions = append(sessions, s)
	}
	p.sessions = make(map[string]*sessionState)
	p.mu.Unlock()

	var firstErr error
	for _, s := range sessions {
		s.mu.Lock()
		s.closed = true
		cl := s.client
		s.client = nil
		rpc := s.rpcHost
		s.rpcHost = nil
		s.mu.Unlock()

		if cl != nil {
			if err := cl.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("close session %q: %w", s.sessionID, err)
			}
		}
		if rpc != nil {
			if err := rpc.Close(context.Background()); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("close RPC session: %w", err)
			}
		}

		s.closeSubscribers()
	}

	return firstErr
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (p *Provider) adoptSessionID(provisionalID string, info sessionInfo, s *sessionState) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.sessions[info.ConversationID]; existing != nil && existing != s {
		return fmt.Errorf("agy: conversation_id %q is already active", info.ConversationID)
	}

	s.mu.Lock()
	s.sessionID = info.ConversationID
	s.externalID = info.ConversationID
	s.initCache["conversation_id"] = info.ConversationID
	s.initCache["model"] = info.Model
	s.initCache["agy_version"] = info.AgyVersion
	s.mu.Unlock()

	// Keep the provisional alias until the in-flight first Prompt returns so
	// timeout/cancel paths that have not yet observed session.start still work.
	p.sessions[provisionalID] = s
	p.sessions[info.ConversationID] = s
	return nil
}

func (p *Provider) getSession(sessionID string) *sessionState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sessions[sessionID]
}
