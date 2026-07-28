package agy

import (
	"context"
	"errors"
	"fmt"
	"os"
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
	closeMu           sync.Mutex
	operations        sync.WaitGroup
	sessions          map[string]*sessionState
	cleanupHosts      []rpcSessionHost
	closed            bool
	lifetimeCtx       context.Context
	lifetimeCancel    context.CancelFunc
	version           string
	versionErr        error
	versionOnce       sync.Once
	clientFactory     clientFactory
	ptyRPCHostFactory ptyRPCHostFactory // Production factory for selected RPC sessions.
	getenv            func(string) string
	rpcHostFactory    rpcSessionHostFactory // Optional narrow test seam.
}

// NewWithOptions creates a new Provider with the given start options.
func NewWithOptions(opts runtime.StartOptions) *Provider {
	lifetimeCtx, lifetimeCancel := context.WithCancel(context.Background())
	return &Provider{
		opts:              opts,
		lifetimeCtx:       lifetimeCtx,
		lifetimeCancel:    lifetimeCancel,
		sessions:          make(map[string]*sessionState),
		clientFactory:     defaultClientFactory,
		ptyRPCHostFactory: defaultPTYRPCHostFactory,
		getenv:            os.Getenv,
	}
}

// sessionState holds the state for one logical conversation session.
type sessionState struct {
	mu sync.Mutex

	client     *client
	rpcHost    rpcSessionHost // Owned for RPC sessions from selection until Close.
	transport  transportKind
	diagnostic string
	sessionID  string
	startOpts  runtime.StartOptions
	initCache  map[string]any

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
	promptCancel    context.CancelFunc
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

// Start selects a transport before registration. Headless keeps its
// resource-free provisional identity; RPC registers the host's already
// validated external conversation identity directly.
func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := runtime.MergeStartOptions(p.opts, opts)
	opCtx, finish, err := p.beginOperation(ctx)
	if err != nil {
		return runtime.Session{}, err
	}
	defer finish()
	if err := p.ensureVersion(opCtx); err != nil {
		return runtime.Session{}, err
	}
	version := p.validatedVersion()
	selection, diagnostic, err := p.selectTransport(opCtx, merged, "", version)
	if err != nil {
		if selection.rpc != nil {
			p.retainCleanupHost(selection.rpc)
		}
		return runtime.Session{}, err
	}

	sessionID := "agy-pending-" + uuid.New().String()
	if selection.kind == transportRPC {
		sessionID = selection.rpc.ConversationID()
		if sessionID == "" {
			if err := p.closeSelectedRPCHost(selection.rpc); err != nil {
				return runtime.Session{}, err
			}
			return runtime.Session{}, errors.New("agy RPC host did not provide a conversation")
		}
	}
	s := &sessionState{
		rpcHost:    selection.rpc,
		transport:  selection.kind,
		diagnostic: diagnostic,
		sessionID:  sessionID,
		startOpts:  merged,
		initCache: map[string]any{
			"conversation_id": sessionID,
			"model":           merged.Model,
			"agy_version":     version,
		},
		firstTurn: true,
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		if err := p.closeSelectedRPCHost(selection.rpc); err != nil {
			return runtime.Session{}, err
		}
		return runtime.Session{}, errors.New("agy provider is closed")
	}
	if existing := p.sessions[sessionID]; existing != nil {
		p.mu.Unlock()
		if err := p.closeSelectedRPCHost(selection.rpc); err != nil {
			return runtime.Session{}, err
		}
		return runtime.Session{}, errors.New("agy conversation is already active")
	}
	p.sessions[sessionID] = s
	p.mu.Unlock()

	return runtime.Session{SessionID: sessionID, Backend: BackendID, Dir: merged.Dir}, nil
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
	opCtx, finish, err := p.beginOperation(ctx)
	if err != nil {
		return runtime.Session{}, err
	}
	defer finish()
	if err := p.ensureVersion(opCtx); err != nil {
		return runtime.Session{}, err
	}

	// Check before probing so an already-owned session never starts a second
	// PTY. A second check below closes a concurrently-probed losing host.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return runtime.Session{}, errors.New("agy provider is closed")
	}
	if s := p.sessions[sessionID]; s != nil {
		p.mu.Unlock()
		return runtime.Session{SessionID: sessionID, Backend: BackendID, Dir: s.startOpts.Dir}, nil
	}
	p.mu.Unlock()

	version := p.validatedVersion()
	selection, diagnostic, err := p.selectTransport(opCtx, p.opts, sessionID, version)
	if err != nil {
		if selection.rpc != nil {
			p.retainCleanupHost(selection.rpc)
		}
		return runtime.Session{}, err
	}
	if selection.kind == transportRPC && selection.rpc.ConversationID() != sessionID {
		if err := p.closeSelectedRPCHost(selection.rpc); err != nil {
			return runtime.Session{}, err
		}
		return runtime.Session{}, errors.New("agy RPC host resumed the wrong conversation")
	}
	s := &sessionState{
		rpcHost:    selection.rpc,
		transport:  selection.kind,
		diagnostic: diagnostic,
		sessionID:  sessionID,
		externalID: sessionID,
		startOpts:  p.opts,
		initCache: map[string]any{
			"conversation_id": sessionID,
			"model":           p.opts.Model,
			"agy_version":     version,
		},
		firstTurn: true,
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		if err := p.closeSelectedRPCHost(selection.rpc); err != nil {
			return runtime.Session{}, err
		}
		return runtime.Session{}, errors.New("agy provider is closed")
	}
	if existing := p.sessions[sessionID]; existing != nil {
		p.mu.Unlock()
		if err := p.closeSelectedRPCHost(selection.rpc); err != nil {
			return runtime.Session{}, err
		}
		return runtime.Session{SessionID: sessionID, Backend: BackendID, Dir: existing.startOpts.Dir}, nil
	}
	p.sessions[sessionID] = s
	p.mu.Unlock()

	return runtime.Session{SessionID: sessionID, Backend: BackendID, Dir: p.opts.Dir}, nil
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

	turnCtx, turnCancel := context.WithCancel(ctx)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		turnCancel()
		return fmt.Errorf("session %q is closed", sessionID)
	}
	if s.running {
		s.mu.Unlock()
		turnCancel()
		return fmt.Errorf("prompt already active for session %q", sessionID)
	}
	s.running = true
	s.cancelled = false
	s.terminalEmitted = false
	s.promptCancel = turnCancel
	s.mu.Unlock()

	// On return, mark the session as no longer running, clean up the turn
	// process, and remove the provisional alias after successful ID adoption.
	defer func() {
		s.mu.Lock()
		if s.rpcHost != nil {
			s.rpcHost.DisarmCancellation()
		}
		s.running = false
		s.promptCancel = nil
		cl := s.client
		s.client = nil
		currentID := s.sessionID
		s.mu.Unlock()
		if cl != nil {
			_ = cl.Close()
		}
		turnCancel()
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
	transport := s.transport
	s.mu.Unlock()

	var err error
	if transport == transportRPC {
		err = p.runRPCPrompt(turnCtx, s, prompt)
	} else {
		switch {
		case firstTurn && externalID == "":
			err = p.runInitialPrompt(turnCtx, s, sessionID, prompt)
		case firstTurn:
			err = p.runResumedPrompt(turnCtx, s, externalID, prompt, true)
		default:
			err = p.runResumedPrompt(turnCtx, s, externalID, prompt, false)
		}
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
	s.emitTransportDiagnostic()

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

// runRPCPrompt uses the already-probed persistent host selected at Start or
// Resume. It deliberately has no fallback path: a registered RPC session stays
// RPC for its entire lifetime.
func (p *Provider) runRPCPrompt(ctx context.Context, s *sessionState, prompt string) error {
	s.mu.Lock()
	host := s.rpcHost
	first := s.firstTurn
	model := s.startOpts.Model
	cancelled := s.cancelled
	if host == nil {
		s.mu.Unlock()
		return errors.New("agy RPC host is unavailable")
	}
	if cancelled {
		s.mu.Unlock()
		return context.Canceled
	}
	if first && !s.startEmitted {
		start := s.buildSessionStart()
		s.startEmitted = true
		s.mu.Unlock()
		s.emit(start)
		s.emitTransportDiagnostic()
	} else {
		s.mu.Unlock()
	}

	err := host.RunTurn(ctx, prompt, model, func(evt events.Event) {
		// The selected host's validated conversation is the only logical ID
		// exposed by this session. Preserve canonical event fields, but enforce
		// that identity at the provider boundary.
		s.mu.Lock()
		evt.SessionID = s.sessionID
		s.mu.Unlock()
		if evt.Event == "session.end" {
			s.emitTerminal(evt)
			return
		}
		s.emit(evt)
	})
	if err == nil {
		s.mu.Lock()
		s.firstTurn = false
		s.mu.Unlock()
	}
	return err
}

func (s *sessionState) emitTransportDiagnostic() {
	s.mu.Lock()
	code := s.diagnostic
	s.diagnostic = ""
	sessionID := s.sessionID
	s.mu.Unlock()
	if code == "" {
		return
	}
	s.emit(events.Event{Event: "avenor.agy.transport", SessionID: sessionID, Fields: map[string]any{"code": code}})
}

// runResumedPrompt starts a fresh headless process for an existing
// conversation, validates its init event, and relays the new turn without
// replaying history.
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
		s.emitTransportDiagnostic()
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
		// The init event has already been adopted before relay begins. Enforce
		// that logical identity here because the client's read loop can parse
		// later events before adoption updates its local conversion cache.
		s.mu.Lock()
		evt.SessionID = s.sessionID
		s.mu.Unlock()
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
	rpc := s.rpcHost
	transport := s.transport
	promptCancel := s.promptCancel
	running := s.running
	terminalEmitted := s.terminalEmitted
	if s.cancelled {
		s.mu.Unlock()
		return nil
	}
	s.cancelled = true
	s.mu.Unlock()

	var cancelErr error
	if transport == transportRPC {
		if rpc != nil {
			if running && !terminalEmitted {
				rpc.ArmCancellation()
			}
			cancelErr = rpc.Cancel(ctx)
		}
		if promptCancel != nil {
			promptCancel()
		}
		// ptyRPCHost normally forwards its observed cancelled terminal through
		// RunTurn. Emit the provider fallback only when that event was absent.
		s.mu.Lock()
		terminal := s.terminalEmitted
		s.mu.Unlock()
		if !terminal {
			s.emitTerminal(events.Event{Event: "session.end", SessionID: sessionID, Fields: map[string]any{"stop_reason": "cancelled"}})
		}
		return cancelErr
	}
	if promptCancel != nil {
		promptCancel()
	}
	if cl != nil {
		cancelErr = cl.Cancel(ctx)
		if errors.Is(cancelErr, ErrClientClosed) {
			cancelErr = nil
		}
	}

	// Preserve the Phase 1 cancellation event exactly.
	s.emitTerminal(events.Event{Event: "session.end", SessionID: sessionID, Fields: map[string]any{"stop_reason": "cancelled"}})
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

// AnswerPermission delegates only to a session selected for RPC. Headless
// keeps its Phase 1 unsupported error.
func (p *Provider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	s := p.getSession(sessionID)
	if s == nil {
		return errors.New("AnswerPermission is not supported in agy headless mode")
	}
	s.mu.Lock()
	host, transport := s.rpcHost, s.transport
	s.mu.Unlock()
	if transport != transportRPC || host == nil {
		return errors.New("AnswerPermission is not supported in agy headless mode")
	}
	return host.AnswerPermission(ctx, requestID, response)
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

// Capabilities returns the capabilities of the agy backend.
func (p *Provider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	// auto is deliberately conservative in Stage 18: its probe may select
	// headless, so only explicit rpc advertises interactive permissions.
	permissions := p.transportPolicy() == "rpc"
	return runtime.Capabilities{
		Backend:             BackendID,
		Permissions:         permissions,
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
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	p.mu.Lock()
	p.closed = true
	if p.lifetimeCancel != nil {
		p.lifetimeCancel()
	}
	p.mu.Unlock()
	p.operations.Wait()

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
	cleanupHosts := p.cleanupHosts
	p.cleanupHosts = nil
	p.mu.Unlock()

	var firstErr error
	for _, s := range sessions {
		s.mu.Lock()
		s.closed = true
		cl := s.client
		rpc := s.rpcHost
		s.mu.Unlock()

		clientClosed := true
		if cl != nil {
			if err := cl.Close(); err != nil {
				clientClosed = false
				if firstErr == nil {
					firstErr = errors.New("agy session close failed")
				}
			}
		}
		rpcClosed := true
		if rpc != nil {
			if err := rpc.Close(context.Background()); err != nil {
				rpcClosed = false
				if firstErr == nil {
					firstErr = errors.New("agy RPC session close failed")
				}
			}
		}

		s.mu.Lock()
		if clientClosed && s.client == cl {
			s.client = nil
		}
		if rpcClosed && s.rpcHost == rpc {
			s.rpcHost = nil
		}
		s.mu.Unlock()
		if clientClosed && rpcClosed {
			p.mu.Lock()
			for id, candidate := range p.sessions {
				if candidate == s {
					delete(p.sessions, id)
				}
			}
			p.mu.Unlock()
		}
		s.closeSubscribers()
	}
	for _, host := range cleanupHosts {
		if err := host.Close(context.Background()); err != nil {
			if retryErr := host.Close(context.Background()); retryErr != nil {
				p.retainCleanupHost(host)
				if firstErr == nil {
					firstErr = errors.New("agy RPC host cleanup failed")
				}
			}
		}
	}

	return firstErr
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (p *Provider) beginOperation(ctx context.Context) (context.Context, func(), error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, nil, errors.New("agy provider is closed")
	}
	lifetime := p.lifetimeCtx
	p.operations.Add(1)
	p.mu.Unlock()

	opCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifetime, cancel)
	finish := func() {
		stop()
		cancel()
		p.operations.Done()
	}
	return opCtx, finish, nil
}

func (p *Provider) retainCleanupHost(host rpcSessionHost) {
	if host == nil {
		return
	}
	p.mu.Lock()
	p.cleanupHosts = append(p.cleanupHosts, host)
	p.mu.Unlock()
}

func (p *Provider) closeSelectedRPCHost(host rpcSessionHost) error {
	if host == nil {
		return nil
	}
	if err := host.Close(context.Background()); err != nil {
		if retryErr := host.Close(context.Background()); retryErr != nil {
			p.retainCleanupHost(host)
			return errors.New("agy RPC host cleanup failed")
		}
	}
	return nil
}

func (p *Provider) validatedVersion() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.version
}

func (p *Provider) transportPolicy() string {
	p.mu.Lock()
	getenv := p.getenv
	p.mu.Unlock()
	if getenv == nil {
		getenv = os.Getenv
	}
	return getenv("AVENOR_AGY_TRANSPORT")
}

func (p *Provider) selectTransport(ctx context.Context, opts runtime.StartOptions, resumeID, version string) (transportSelection, string, error) {
	p.mu.Lock()
	getenv := p.getenv
	factory := p.rpcHostFactory
	ptyFactory := p.ptyRPCHostFactory
	p.mu.Unlock()
	if factory == nil && ptyFactory != nil {
		factory = func(ctx context.Context, opts runtime.StartOptions, resumeID, version string) (rpcSessionHost, error) {
			return ptyFactory(ctx, opts, resumeID, version)
		}
	}
	return selectTransport(ctx, getenv, opts, resumeID, version, factory)
}

func (p *Provider) adoptSessionID(provisionalID string, info sessionInfo, s *sessionState) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.sessions[info.ConversationID]; existing != nil && existing != s {
		return errors.New("agy conversation is already active")
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
