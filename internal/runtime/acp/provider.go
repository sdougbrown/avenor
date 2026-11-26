package acp

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/warmqueue"
)

// errProviderClosed is returned by a launch the provider abandoned because
// Close ran first.
var errProviderClosed = errors.New("provider is closed")

// launchSettleTimeout bounds how long Close waits for an in-flight launch to
// return before it finishes on its own. A launch can be stuck in a start that
// ignores both the caller's context and the provider lifetime.
const launchSettleTimeout = 2 * time.Second

// sessionCloseTimeout bounds best-effort remote session cleanup when
// configureSession fails after session/new or session/resume succeeded.
const sessionCloseTimeout = 2 * time.Second

// ProviderConfig describes one ACP-speaking backend's differences from the
// shared provider. Every backend sets BackendID, Bin, and Opts; the rest are
// optional, and a field left at its zero value keeps the shared default, so this
// table is the whole of what a backend varies:
//
//	                     agy                   opencode-acp          gemini-acp  cursor-acp
//	Args                 built per run         acp --pure --log-…    --acp …     acp
//	SetCmdDir            true                  false                 false       false
//	EmitSessionStart     true                  false                 false       false
//	SubprocessDiscovery  false                 true                  false       false
//	AppendCWDArg         false                 true                  false       false
//	Authenticate         unset                 unset                 unset       cursor_login
//	ValidateStart        AVENOR_AGY_TRANSPORT  unset                 unset       unset
//	BuildClientArgs      --add-dir, --agent    unset                 unset       unset
//	BuildSessionOpenOpts channel-tools         unset                 unset       unset
//	ConfigureSession     model                 model, mode           no-op       no-op
//	BuildClientEnv       unset                 channel-tools, mode   unset       unset
//	TolerateInconsistentFinish unset          unset                 unset       unset
//
// Leaving a field unset is not neutral, it selects the shared default, and the
// consequences differ per field. EmitSessionStart publishes session.start, which
// internal/control/event_stream.go turns into TurnState "starting" and — outside
// a stable handler — run status running, so a backend that omits it publishes
// neither. SetCmdDir false leaves the child in the provider's own working
// directory rather than the session's. BuildSessionOpenOptions nil opens every
// session with no MCP servers, which drops broker messaging without an error.
// Group these variants behind one backend-options struct when a fifth backend
// lands; until then the table keeps the combinations in one place instead of
// spreading them across the backends.
type ProviderConfig struct {
	BackendID               string
	Bin                     string
	Args                    []string
	SetCmdDir               bool
	EmitSessionStart        bool
	SubprocessDiscovery     bool
	AppendCWDArg            bool
	Authenticate            string               // optional: if non-empty, call Client.Authenticate with this methodId after Initialize
	Opts                    runtime.StartOptions // authoritative launch options; retained so Resume launches with the same wrapper and settings
	ValidateStart           func(opts runtime.StartOptions) error
	BuildClientArgs         func(opts runtime.StartOptions, absDir string) ([]string, error)
	BuildSessionOpenOptions func(opts runtime.StartOptions, runID, brokerToken, brokerURL string) (SessionOpenOptions, error)
	ConfigureSession        func(ctx context.Context, session *Session, opts runtime.StartOptions) error
	BuildClientEnv          func(opts runtime.StartOptions, runID, brokerToken, brokerURL string) (map[string]string, func() error)
	// TolerateInconsistentFinish is the fx carve-out, not a pattern to build on:
	// fx's session/prompt can reject with InconsistentFinishReason after the
	// model already streamed its reply (its own finish-reason bookkeeping
	// disagrees with itself). When set and the session has streamed message
	// content, that error ends the turn as a warning instead of failing the run.
	TolerateInconsistentFinish bool
}

type brokerIdentity struct {
	runID  string
	token  string
	url    string
	broker *broker.Broker
	ok     bool
}

type clientLaunchSpec struct {
	absDir  string
	agent   string
	wrapper []string
}

type Provider struct {
	opts runtime.StartOptions

	mu             sync.Mutex
	backendID      string
	bin            string
	args           []string
	client         *Client
	clientLaunch   clientLaunchSpec
	hub            *eventHub
	sessions       map[string]*Session
	activePrompts  map[string]chan struct{}
	injectReserved map[string]int
	pendingOptions map[string]map[string][]any

	// startClient starts the backend process and completes its handshake.
	// Overridable in tests; nil uses the provider's own launch.
	startClient func(context.Context, ClientConfig) (*Client, error)

	// closing latches when Close runs. A launch still in flight when Close is
	// called tears down what it started instead of installing it afterwards.
	// launching counts those in-flight launches and launchIdle is closed when
	// the last of them settles.
	closing    bool
	launching  int
	launchIdle chan struct{}

	// lifetimeCtx ends when Close runs, so a launch Close abandoned cannot
	// outlive the provider. The first admitted launch creates it under the same
	// lock Close latches closing under, so Close can always cancel it.
	lifetimeCtx    context.Context
	lifetimeCancel context.CancelFunc

	setCmdDir                  bool
	emitSessionStart           bool
	subprocessDiscovery        bool
	appendCWDArg               bool
	authenticateID             string
	validateStart              func(opts runtime.StartOptions) error
	buildClientArgs            func(opts runtime.StartOptions, absDir string) ([]string, error)
	buildSessionOpenOptions    func(opts runtime.StartOptions, runID, brokerToken, brokerURL string) (SessionOpenOptions, error)
	configureSession           func(ctx context.Context, session *Session, opts runtime.StartOptions) error
	buildClientEnv             func(opts runtime.StartOptions, runID, brokerToken, brokerURL string) (map[string]string, func() error)
	clientEnvCleanup           func() error
	tolerateInconsistentFinish bool

	// streamedChunks counts the agent.message_chunk events the pump broadcast
	// per session. The fx carve-out in Prompt reads it to tell a turn the model
	// already answered from one that failed before any content.
	streamedChunks map[string]int

	// Broker fields for channel-wrapped prompt injection.
	broker       *broker.Broker
	runIDs       map[string]string // sessionID → runID
	pollContexts map[string]context.Context
	pollCancels  map[string]context.CancelFunc
	pollGen      map[string]uint64
	nextPollGen  uint64
	// Queue holds queued channel messages and the warm-admission guard;
	// LockWarmMessageAdmission is promoted from it.
	*warmqueue.Queue
}

func NewProvider(cfg ProviderConfig) runtime.Provider {
	return &Provider{
		opts:                       cfg.Opts,
		backendID:                  cfg.BackendID,
		bin:                        cfg.Bin,
		args:                       cfg.Args,
		sessions:                   map[string]*Session{},
		activePrompts:              map[string]chan struct{}{},
		injectReserved:             map[string]int{},
		runIDs:                     map[string]string{},
		pollContexts:               map[string]context.Context{},
		pollCancels:                map[string]context.CancelFunc{},
		pollGen:                    map[string]uint64{},
		Queue:                      warmqueue.NewQueue(),
		setCmdDir:                  cfg.SetCmdDir,
		emitSessionStart:           cfg.EmitSessionStart,
		subprocessDiscovery:        cfg.SubprocessDiscovery,
		appendCWDArg:               cfg.AppendCWDArg,
		authenticateID:             cfg.Authenticate,
		validateStart:              cfg.ValidateStart,
		buildClientArgs:            cfg.BuildClientArgs,
		buildSessionOpenOptions:    cfg.BuildSessionOpenOptions,
		configureSession:           cfg.ConfigureSession,
		buildClientEnv:             cfg.BuildClientEnv,
		tolerateInconsistentFinish: cfg.TolerateInconsistentFinish,
		streamedChunks:             map[string]int{},
	}
}

var _ runtime.Provider = (*Provider)(nil)

func (p *Provider) ensureMapsLocked() {
	if p.sessions == nil {
		p.sessions = map[string]*Session{}
	}
	if p.activePrompts == nil {
		p.activePrompts = map[string]chan struct{}{}
	}
	if p.injectReserved == nil {
		p.injectReserved = map[string]int{}
	}
	if p.streamedChunks == nil {
		p.streamedChunks = map[string]int{}
	}
	if p.runIDs == nil {
		p.runIDs = map[string]string{}
	}
	if p.pollContexts == nil {
		p.pollContexts = map[string]context.Context{}
	}
	if p.pollCancels == nil {
		p.pollCancels = map[string]context.CancelFunc{}
	}
	if p.pollGen == nil {
		p.pollGen = map[string]uint64{}
	}
}

func (p *Provider) effectiveDirLocked(fallback string) string {
	if p.clientLaunch.absDir != "" {
		return p.clientLaunch.absDir
	}
	return fallback
}

func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := runtime.MergeStartOptions(p.opts, opts)

	p.mu.Lock()
	closing := p.closing
	p.mu.Unlock()
	if closing {
		return runtime.Session{}, fmt.Errorf("%s: %w", p.backendID, errProviderClosed)
	}
	if p.validateStart != nil {
		if err := p.validateStart(merged); err != nil {
			return runtime.Session{}, err
		}
	}

	id := p.launchRun(merged)
	openOpts, err := p.sessionOpenOptions(merged, id)
	if err != nil {
		return runtime.Session{}, err
	}

	client, release, err := p.ensureClient(ctx, merged, func() (map[string]string, func() error) {
		return p.launchEnv(merged, id)
	})
	if err != nil {
		return runtime.Session{}, err
	}
	// The launch stays counted as in flight until this call is done with the
	// client it handed back, so Close cannot finish under it.
	defer release()

	session, err := client.NewSessionWithOptions(ctx, openOpts)
	if err != nil {
		return runtime.Session{}, p.abandonedError(client, err)
	}
	if p.configureSession != nil {
		if err := p.configureSession(ctx, session, merged); err != nil {
			p.closeUninstalledSession(session)
			return runtime.Session{}, p.abandonedError(client, err)
		}
	}

	if id.ok {
		session.RunID = id.runID
		session.BrokerToken = id.token
	}

	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		p.closeUninstalledSession(session)
		return runtime.Session{}, fmt.Errorf("%s: %w", p.backendID, errProviderClosed)
	}
	p.ensureMapsLocked()
	p.sessions[session.SessionID] = session

	var (
		pollCtx context.Context
		gen     uint64
	)
	if id.ok {
		if prevCancel := p.pollCancels[session.SessionID]; prevCancel != nil {
			prevCancel()
		}
		var cancel context.CancelFunc
		pollCtx, cancel = context.WithCancel(ctx)
		p.nextPollGen++
		gen = p.nextPollGen
		p.runIDs[session.SessionID] = id.runID
		p.pollContexts[session.SessionID] = pollCtx
		p.pollCancels[session.SessionID] = cancel
		p.pollGen[session.SessionID] = gen
	}
	dir := p.effectiveDirLocked(merged.Dir)
	hub := p.hub
	p.mu.Unlock()

	if id.ok {
		p.startPoller(pollCtx, session.SessionID, id, gen)
	}
	if p.emitSessionStart {
		p.publishToHub(hub, events.Event{
			Event:     "session.start",
			SessionID: session.SessionID,
			Fields: map[string]any{
				"backend":    p.backendID,
				"model":      merged.Model,
				"session_id": session.SessionID,
			},
		})
	}

	return runtime.Session{
		SessionID: session.SessionID,
		Backend:   p.backendID,
		Dir:       dir,
		PID:       client.PID(),
	}, nil
}

func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	return p.resume(ctx, sessionID, runtime.StartOptions{}, runtime.MergeStartOptions(p.opts, runtime.StartOptions{}), false)
}

// ResumeWithOptions dispatches a resume with the merged per-call launch
// options, so a client started by this call carries the caller's identity and
// configuration.
func (p *Provider) ResumeWithOptions(ctx context.Context, sessionID string, opts runtime.StartOptions) (runtime.Session, error) {
	return p.resume(ctx, sessionID, opts, runtime.MergeStartOptions(p.opts, opts), true)
}

func (p *Provider) resume(ctx context.Context, sessionID string, callOpts, opts runtime.StartOptions, optionAware bool) (runtime.Session, error) {
	if sessionID == "" {
		return runtime.Session{}, errors.New("session id is required")
	}
	// Thinking is per-call on resume: only an option-aware call may reach the
	// session setter, and only with the value it asked for, so a constructor
	// default never leaks into a plain resume.
	if optionAware {
		opts.Thinking = callOpts.Thinking
	} else {
		opts.Thinking = ""
	}

	p.mu.Lock()
	closing := p.closing
	existingSession := p.sessions[sessionID]
	existingBroker := p.broker
	p.mu.Unlock()
	if closing {
		return runtime.Session{}, fmt.Errorf("%s: %w", p.backendID, errProviderClosed)
	}
	if p.validateStart != nil {
		if err := p.validateStart(opts); err != nil {
			return runtime.Session{}, err
		}
	}

	var (
		id       brokerIdentity
		openOpts SessionOpenOptions
		buildEnv func() (map[string]string, func() error)
		wasWarm  = existingSession != nil
	)

	if wasWarm {
		if callOpts.RuntimeID != "" && callOpts.RuntimeID != existingSession.RunID {
			return runtime.Session{}, fmt.Errorf("%s: session %q already bound to run %q, cannot resume with %q", p.backendID, sessionID, existingSession.RunID, callOpts.RuntimeID)
		}
		if existingSession.RunID != "" && opts.RuntimeID != "" && opts.RuntimeID != existingSession.RunID {
			return runtime.Session{}, fmt.Errorf("%s: session %q already bound to run %q, cannot resume with %q", p.backendID, sessionID, existingSession.RunID, opts.RuntimeID)
		}
		if callOpts.Broker != nil && callOpts.Broker != existingBroker {
			return runtime.Session{}, fmt.Errorf("%s: session %q already bound to a different broker", p.backendID, sessionID)
		}
		if existingBroker != nil && opts.Broker != nil && opts.Broker != existingBroker {
			return runtime.Session{}, fmt.Errorf("%s: session %q already bound to a different broker", p.backendID, sessionID)
		}
		if existingSession.RunID != "" && existingSession.BrokerToken != "" && existingBroker != nil && existingBroker.URL() != "" {
			id = brokerIdentity{
				runID:  existingSession.RunID,
				token:  existingSession.BrokerToken,
				url:    existingBroker.URL(),
				broker: existingBroker,
				ok:     true,
			}
		}
		var err error
		openOpts, err = p.sessionOpenOptions(opts, id)
		if err != nil {
			return runtime.Session{}, err
		}
	} else {
		id = p.launchRun(opts)
		var err error
		openOpts, err = p.sessionOpenOptions(opts, id)
		if err != nil {
			return runtime.Session{}, err
		}
		buildEnv = func() (map[string]string, func() error) {
			return p.launchEnv(opts, id)
		}
	}

	client, release, err := p.ensureClient(ctx, opts, buildEnv)
	if err != nil {
		return runtime.Session{}, err
	}
	defer release()

	session, err := client.ResumeSessionWithOptions(ctx, sessionID, openOpts)
	if err != nil {
		return runtime.Session{}, p.abandonedError(client, err)
	}
	if p.configureSession != nil {
		if err := p.configureSession(ctx, session, opts); err != nil {
			if !wasWarm {
				p.closeUninstalledSession(session)
			}
			return runtime.Session{}, p.abandonedError(client, err)
		}
	}

	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		if !wasWarm {
			p.closeUninstalledSession(session)
		}
		return runtime.Session{}, fmt.Errorf("%s: %w", p.backendID, errProviderClosed)
	}
	p.ensureMapsLocked()
	if prev := p.sessions[sessionID]; prev != nil {
		if prev.RunID != "" {
			if id.ok && id.runID != prev.RunID {
				p.mu.Unlock()
				return runtime.Session{}, fmt.Errorf("%s: session %q already bound to run %q, cannot resume with %q", p.backendID, sessionID, prev.RunID, id.runID)
			}
			session.RunID = prev.RunID
			session.BrokerToken = prev.BrokerToken
			if !id.ok && p.broker != nil && prev.BrokerToken != "" && p.broker.URL() != "" {
				id = brokerIdentity{
					runID:  prev.RunID,
					token:  prev.BrokerToken,
					url:    p.broker.URL(),
					broker: p.broker,
					ok:     true,
				}
			}
		} else if id.ok {
			session.RunID = id.runID
			session.BrokerToken = id.token
		}
	} else if id.ok {
		session.RunID = id.runID
		session.BrokerToken = id.token
	}
	p.sessions[session.SessionID] = session

	var (
		startNewPoller bool
		pollCtx        context.Context
		gen            uint64
	)
	if id.ok {
		p.runIDs[session.SessionID] = id.runID
		liveCtx := p.pollContexts[session.SessionID]
		liveGen := p.pollGen[session.SessionID]
		if liveGen == 0 || liveCtx == nil || liveCtx.Err() != nil {
			if prevCancel := p.pollCancels[session.SessionID]; prevCancel != nil {
				prevCancel()
			}
			var cancel context.CancelFunc
			pollCtx, cancel = context.WithCancel(ctx)
			p.nextPollGen++
			gen = p.nextPollGen
			p.pollContexts[session.SessionID] = pollCtx
			p.pollCancels[session.SessionID] = cancel
			p.pollGen[session.SessionID] = gen
			startNewPoller = true
		}
	}
	dir := p.effectiveDirLocked(opts.Dir)
	p.mu.Unlock()

	if startNewPoller {
		p.startPoller(pollCtx, session.SessionID, id, gen)
	}

	return runtime.Session{
		SessionID: session.SessionID,
		Backend:   p.backendID,
		Dir:       dir,
		PID:       client.PID(),
	}, nil
}

func (p *Provider) startPoller(pollCtx context.Context, sessionID string, id brokerIdentity, gen uint64) {
	if !id.ok || id.broker == nil || id.runID == "" {
		return
	}
	go func() {
		defer func() {
			p.mu.Lock()
			if p.pollGen != nil && p.pollGen[sessionID] == gen {
				delete(p.pollContexts, sessionID)
				delete(p.pollCancels, sessionID)
				delete(p.pollGen, sessionID)
			}
			p.mu.Unlock()
		}()
		id.broker.PollAgentMessages(pollCtx, id.runID, func(wrapped string) {
			if p.Queue != nil {
				p.Queue.Enqueue(sessionID, wrapped)
			}
		})
	}()
}

func (p *Provider) closeUninstalledSession(session *Session) {
	if session == nil || session.Client == nil || session.Client.ended() {
		return
	}
	p.mu.Lock()
	closing := p.closing
	lifetime := p.lifetimeCtx
	p.mu.Unlock()
	if closing {
		return
	}
	baseCtx := context.Background()
	if lifetime != nil {
		baseCtx = lifetime
	}
	closeCtx, cancel := context.WithTimeout(baseCtx, sessionCloseTimeout)
	defer cancel()
	_ = session.Close(closeCtx)
}

func (p *Provider) sessionOpenOptions(opts runtime.StartOptions, id brokerIdentity) (SessionOpenOptions, error) {
	if p.buildSessionOpenOptions == nil {
		return SessionOpenOptions{}, nil
	}
	runID, token, url := "", "", ""
	if id.ok {
		runID, token, url = id.runID, id.token, id.url
	}
	return p.buildSessionOpenOptions(opts, runID, token, url)
}

// abandonedError reports the provider's own closure when Close ended the work in
// progress, keeping the underlying cause attached: Close cancelled the context
// a launch runs under, or closed the client the work was using. A failure that
// merely overlapped shutdown keeps its own error.
func (p *Provider) abandonedError(client *Client, err error) error {
	if err == nil {
		return nil
	}
	p.mu.Lock()
	closing := p.closing
	p.mu.Unlock()
	if !closing {
		return err
	}
	if !errors.Is(err, context.Canceled) && (client == nil || !client.ended()) {
		return err
	}
	return fmt.Errorf("%s: %w: %w", p.backendID, errProviderClosed, err)
}

// retainBroker records the caller's broker when the provider holds none, so a
// later launch without a per-call broker still finds one. It returns the broker
// the provider holds afterwards.
func (p *Provider) retainBroker(opts runtime.StartOptions) *broker.Broker {
	p.mu.Lock()
	defer p.mu.Unlock()
	if opts.Broker != nil && p.broker == nil {
		p.broker = opts.Broker
	}
	return p.broker
}

// launchRun creates the broker run that backend process config authenticates
// with, before the client starts, so it can send from the first turn. It uses
// the supervisor-assigned RuntimeID when available (parent-child routing) and a
// generated UUID otherwise.
func (p *Provider) launchRun(opts runtime.StartOptions) brokerIdentity {
	brokerRef := p.retainBroker(opts)
	if brokerRef == nil {
		return brokerIdentity{}
	}
	url := brokerRef.URL()
	if url == "" {
		return brokerIdentity{}
	}
	runID := opts.RuntimeID
	if runID == "" {
		runID = uuid.New().String()
	}
	brokerToken, _, err := brokerRef.EnsureRun(runID, opts.Label)
	if err != nil || brokerToken == "" {
		// A record already holds this run ID under a credential this supervisor did
		// not mint, so the launch carries no broker identity at all rather than one
		// someone else registered.
		return brokerIdentity{}
	}
	return brokerIdentity{
		runID:  runID,
		token:  brokerToken,
		url:    url,
		broker: brokerRef,
		ok:     true,
	}
}

// launchEnv builds the backend's client environment and its cleanup for one
// launch.
func (p *Provider) launchEnv(opts runtime.StartOptions, id brokerIdentity) (map[string]string, func() error) {
	p.mu.Lock()
	buildClientEnv := p.buildClientEnv
	p.mu.Unlock()
	if buildClientEnv == nil {
		return nil, nil
	}
	// Always call buildClientEnv so backends can inject config overrides (e.g.
	// mode overrides for subagent-mode agents) even without a broker. The
	// backend is responsible for returning nil when there's nothing to override,
	// so no temp file is created unnecessarily. It is handed the broker's http
	// URL, not the bound address: the one spelling of that base is the broker's
	// own (Broker.URL), so a backend never re-renders it.
	runID, token, brokerURL := "", "", ""
	if id.ok {
		runID, token, brokerURL = id.runID, id.token, id.url
	}
	return buildClientEnv(opts, runID, token, brokerURL)
}

func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return fmt.Errorf("%s: %w", p.backendID, errProviderClosed)
	}
	session := p.sessions[sessionID]
	if session == nil {
		p.mu.Unlock()
		return fmt.Errorf("session %q not found", sessionID)
	}
	if p.activePrompts == nil {
		p.activePrompts = map[string]chan struct{}{}
	}
	if p.injectReserved == nil {
		p.injectReserved = map[string]int{}
	}
	if p.activePrompts[sessionID] != nil || p.injectReserved[sessionID] > 0 {
		p.mu.Unlock()
		return fmt.Errorf("%s: prompt already in flight for session %q", p.backendID, sessionID)
	}
	myDone := make(chan struct{})
	p.activePrompts[sessionID] = myDone
	hub := p.hub
	if hub != nil {
		hub.beginAsyncOp()
	}
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		if p.activePrompts[sessionID] == myDone {
			delete(p.activePrompts, sessionID)
		}
		p.mu.Unlock()
		close(myDone)
		if hub != nil {
			hub.endAsyncOp()
		}
	}()

	event, err := session.Prompt(ctx, prompt)
	if err != nil {
		if p.tolerateInconsistentFinish && isInconsistentFinishReason(err) && p.hasStreamedChunks(sessionID) {
			p.publishToHub(hub, events.Event{
				Event:     "avenor.error",
				SessionID: sessionID,
				Fields: map[string]any{
					"message": "backend ended the turn with an inconsistent finish reason after streaming its reply; treating the turn as complete",
					"source":  "warning",
				},
			})
			event = events.Event{
				Event:     "session.end",
				SessionID: sessionID,
				Fields:    map[string]any{"stop_reason": "end_turn"},
			}
		} else {
			return err
		}
	}
	p.publishToHub(hub, event)
	return nil
}

// isInconsistentFinishReason matches fx's InconsistentFinishReason JSON-RPC
// error. It is the fx carve-out's recognizer, not a pattern to build on: the
// shared ACP client renders the error as "json-rpc session/prompt failed:
// <message> (<code>)", and fx's message names the condition.
func isInconsistentFinishReason(err error) bool {
	return err != nil && strings.Contains(err.Error(), "InconsistentFinishReason")
}

// hasStreamedChunks reports whether the pump ever broadcast an agent.message_chunk
// for the session, so a carve-out that swallows a prompt error can tell a turn
// the model already answered from one that produced nothing.
func (p *Provider) hasStreamedChunks(sessionID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.streamedChunks[sessionID] > 0
}

func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	session, err := p.session(sessionID)
	if err != nil {
		return err
	}
	return session.Cancel()
}

func (p *Provider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	hub := p.hub
	p.mu.Unlock()
	if hub == nil {
		return nil, errors.New("provider has not been started")
	}
	return hub.subscribe(ctx, sessionID)
}

func (p *Provider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	p.mu.Lock()
	client := p.client
	p.mu.Unlock()
	if client == nil {
		return errors.New("provider has not been started")
	}
	if requestID == "" {
		return errors.New("permission request id is required")
	}

	p.mu.Lock()
	cacheSessionID, options := pendingPermissionOptions(p.pendingOptions, sessionID, requestID)
	p.mu.Unlock()

	optionID, err := selectPermissionResponseOption(options, response)
	if err != nil {
		return fmt.Errorf("resolve permission request %q: %w", requestID, err)
	}
	if err := client.answerPermission(requestID, permissionResponseResult(response, optionID)); err != nil {
		return err
	}

	p.mu.Lock()
	if sessionOptions := p.pendingOptions[cacheSessionID]; sessionOptions != nil {
		delete(sessionOptions, requestID)
		if len(sessionOptions) == 0 {
			delete(p.pendingOptions, cacheSessionID)
		}
	}
	p.mu.Unlock()
	return nil
}

func (p *Provider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{
		Backend:             p.backendID,
		Permissions:         true,
		Resume:              true,
		ExternalServerURL:   false,
		SubprocessDiscovery: p.subprocessDiscovery,
		ModelSelection:      true,
	}, nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	p.closing = true
	if p.lifetimeCancel != nil {
		p.lifetimeCancel()
	}
	client := p.client
	hub := p.hub
	idle := p.launchIdle
	cancels := make([]context.CancelFunc, 0, len(p.pollCancels))
	for _, cancel := range p.pollCancels {
		cancels = append(cancels, cancel)
	}
	p.client = nil
	cleanup := p.clientEnvCleanup
	p.clientEnvCleanup = nil
	p.pendingOptions = nil
	p.sessions = map[string]*Session{}
	p.activePrompts = map[string]chan struct{}{}
	p.injectReserved = map[string]int{}
	p.streamedChunks = map[string]int{}
	p.runIDs = nil
	p.pollContexts = map[string]context.Context{}
	p.pollCancels = map[string]context.CancelFunc{}
	p.pollGen = map[string]uint64{}
	p.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	if hub != nil {
		hub.requestStop()
	}
	// A launch still in flight sees the closing flag when it tries to install,
	// tears down what it started, and returns an error. Wait for that within a
	// bound, since a launch can ignore both its caller's context and the
	// provider lifetime and never return.
	if idle != nil {
		select {
		case <-idle:
		case <-time.After(launchSettleTimeout):
			// Closing is latched, so no launch is admitted any more and the count
			// never has to wake Close again: forgetting the idle channel is enough
			// to keep a later Close from waiting out the bound once more.
			p.mu.Lock()
			p.launchIdle = nil
			p.mu.Unlock()
		}
	}
	if p.Queue != nil {
		p.Queue.Discard()
	}
	if cleanup != nil {
		_ = cleanup()
	}
	var err error
	if client != nil {
		err = client.Close()
	}
	if hub != nil {
		<-hub.pumpDone
	}
	return err
}

// beginLaunch admits one launch attempt, rejecting it once Close has run. It
// also creates the provider lifetime if this is the first attempt, inside the
// same critical section, so a launch admitted here always has a lifetime Close
// cancels instead of building one Close has already passed.
func (p *Provider) beginLaunch() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return fmt.Errorf("%s: %w", p.backendID, errProviderClosed)
	}
	if p.lifetimeCtx == nil {
		p.lifetimeCtx, p.lifetimeCancel = context.WithCancel(context.Background())
	}
	if p.launching == 0 {
		p.launchIdle = make(chan struct{})
	}
	p.launching++
	return nil
}

// settleLaunch records that a counted launch returned, waking Close once the
// last one has.
func (p *Provider) settleLaunch() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.launching > 0 {
		p.launching--
	}
	if p.launching > 0 || p.launchIdle == nil {
		return
	}
	close(p.launchIdle)
	p.launchIdle = nil
}

// launchContext derives the context for one launch attempt admitted by
// beginLaunch. It ends when the caller's context ends or when Close cancels the
// provider lifetime, so a launch nobody is waiting for cannot leave its child
// behind. keepAlive detaches the attempt from the lifetime once it has handed
// its client over, which lets Close close that client normally; abandon also
// ends the context, killing a process that is still starting. The lifetime is
// created at admission, so a launch cannot outrun Close by building its own.
func (p *Provider) launchContext(ctx context.Context) (launchCtx context.Context, keepAlive, abandon func()) {
	p.mu.Lock()
	lifetime := p.lifetimeCtx
	p.mu.Unlock()

	launchCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifetime, cancel)
	return launchCtx, func() { stop() }, func() { stop(); cancel() }
}

func resolveAbsDir(dir string) (string, error) {
	if dir == "" {
		dir = "."
	}
	return filepath.Abs(dir)
}

// checkLaunchCompat reports whether an installed client can serve this launch.
// The strict form compares every launch field; the warm form compares only the
// fields this caller set, because a warm launch may reuse a client that was
// started under defaults the caller never named.
func (p *Provider) checkLaunchCompat(opts runtime.StartOptions, candidate, installed clientLaunchSpec, strict bool) error {
	if p.buildClientArgs == nil && !p.setCmdDir {
		return nil
	}
	if (strict || opts.Dir != "") && installed.absDir != "" && candidate.absDir != installed.absDir {
		return fmt.Errorf("%s: client already running in dir %q, cannot reuse for %q", p.backendID, installed.absDir, candidate.absDir)
	}
	if (strict || opts.Agent != "") && candidate.agent != installed.agent {
		return fmt.Errorf("%s: client already running with agent %q, cannot reuse for %q", p.backendID, installed.agent, candidate.agent)
	}
	if (strict || len(opts.LaunchWrapper) > 0) && !slices.Equal(candidate.wrapper, installed.wrapper) {
		return fmt.Errorf("%s: client already running with launch wrapper %v, cannot reuse for %v", p.backendID, installed.wrapper, candidate.wrapper)
	}
	return nil
}

// checkWarmLaunchCompat compares an existing client against a launch that may
// adopt it as-is, so a field the launch did not set is not a mismatch.
func (p *Provider) checkWarmLaunchCompat(opts runtime.StartOptions, candidate, installed clientLaunchSpec) error {
	return p.checkLaunchCompat(opts, candidate, installed, false)
}

// checkConcurrentLaunchCompat compares the launch that lost an install race
// against the winner. Both launches are fully specified here, so every field
// must match.
func (p *Provider) checkConcurrentLaunchCompat(opts runtime.StartOptions, candidate, installed clientLaunchSpec) error {
	return p.checkLaunchCompat(opts, candidate, installed, true)
}

// ensureClient returns the provider's client, starting one when none is
// retained yet. buildEnv, when non-nil, builds the environment for that new
// client only, so a retained client never pays for a launch it will not use.
// The returned release ends the launch attempt and must be called exactly once,
// after the caller is done with the client: until then that launch counts as in
// flight, so Close waits for it and cancels it instead of finishing early. A
// client that was already installed when Close ran is not covered by that count,
// so a call on it can still see the client closed underneath. A launch that does
// not become the provider's client — because a client is already live, another
// launch won the install, Close ran first, or startup failed — cleans up its own
// generated config before returning. A retained client takes ownership of its
// config until Close.
func (p *Provider) ensureClient(ctx context.Context, opts runtime.StartOptions, buildEnv func() (map[string]string, func() error)) (*Client, func(), error) {
	absDir, err := resolveAbsDir(opts.Dir)
	if err != nil {
		return nil, nil, err
	}
	spec := clientLaunchSpec{
		absDir:  absDir,
		agent:   opts.Agent,
		wrapper: slices.Clone(opts.LaunchWrapper),
	}

	p.mu.Lock()
	client := p.client
	installedSpec := p.clientLaunch
	p.mu.Unlock()
	if client != nil {
		if err := p.checkWarmLaunchCompat(opts, spec, installedSpec); err != nil {
			return nil, nil, err
		}
		return client, func() {}, nil
	}
	if err := p.beginLaunch(); err != nil {
		return nil, nil, err
	}

	launchCtx, keepAlive, abandon := p.launchContext(ctx)
	handedOff := false
	defer func() {
		if handedOff {
			return
		}
		abandon()
		p.settleLaunch()
	}()

	if opts.ServerURL != "" {
		return nil, nil, fmt.Errorf("%s external server URL transport is not implemented by the stdio client", p.backendID)
	}

	args := p.args
	if p.buildClientArgs != nil {
		builtArgs, err := p.buildClientArgs(opts, absDir)
		if err != nil {
			return nil, nil, err
		}
		args = builtArgs
	}

	var (
		env     map[string]string
		cleanup func() error
	)
	if buildEnv != nil {
		env, cleanup = buildEnv()
	}

	start := p.startClient
	if start == nil {
		start = p.launch
	}
	launched, err := start(launchCtx, ClientConfig{
		Bin:                  p.bin,
		Args:                 args,
		Wrapper:              opts.LaunchWrapper,
		Dir:                  absDir,
		SetCmdDir:            p.setCmdDir,
		Env:                  env,
		AutoAnswerPermission: false,
		AppendCWDArg:         p.appendCWDArg,
		Provenance:           opts.Provenance,
	})
	if err != nil {
		runCleanup(cleanup)
		return nil, nil, p.abandonedError(nil, err)
	}

	p.mu.Lock()
	switch {
	case p.closing:
		p.mu.Unlock()
		_ = launched.Close()
		runCleanup(cleanup)
		return nil, nil, fmt.Errorf("%s: %w", p.backendID, errProviderClosed)
	case p.client != nil:
		retained := p.client
		winnerSpec := p.clientLaunch
		p.mu.Unlock()
		_ = launched.Close()
		runCleanup(cleanup)
		if err := p.checkConcurrentLaunchCompat(opts, spec, winnerSpec); err != nil {
			return nil, nil, err
		}
		return retained, func() {}, nil
	}
	p.client = launched
	p.clientLaunch = spec
	p.clientEnvCleanup = cleanup
	hub := newEventHub()
	p.hub = hub
	go p.pumpClientEvents(launched, hub)
	// The client is the provider's now: Close closes it normally instead of
	// ending its launch context. Detaching under the same lock Close latches
	// closing under keeps Close from killing a client it is about to adopt.
	keepAlive()
	p.mu.Unlock()

	// The attempt still counts as in flight, so Close waits for the caller to
	// finish with the client.
	handedOff = true
	return launched, p.settleLaunch, nil
}

// launch starts the backend process and completes its handshake.
func (p *Provider) launch(ctx context.Context, cfg ClientConfig) (*Client, error) {
	client, err := NewClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if _, err := client.Initialize(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	if p.authenticateID != "" {
		if err := client.Authenticate(ctx, p.authenticateID); err != nil {
			_ = client.Close()
			return nil, err
		}
	}
	return client, nil
}

func runCleanup(cleanup func() error) {
	if cleanup != nil {
		_ = cleanup()
	}
}

func (p *Provider) pumpClientEvents(client *Client, hub *eventHub) {
	abort := true
	defer func() {
		hub.finishPump(abort)
	}()

	var clientEvents <-chan events.Event
	if client != nil {
		clientEvents = client.Events()
	}

	drainClientEvents := func() {
		for clientEvents != nil {
			select {
			case ev, ok := <-clientEvents:
				if !ok {
					clientEvents = nil
					return
				}
				p.processAndBroadcast(hub, ev)
			default:
				return
			}
		}
	}

	drainOrderedEvents := func() {
		for {
			select {
			case req := <-hub.ordered:
				drainClientEvents()
				p.processAndBroadcast(hub, req.event)
				if req.ack != nil {
					close(req.ack)
				}
			default:
				return
			}
		}
	}

	for {
		if clientEvents == nil && hub.asyncOpsCount() == 0 {
			drainOrderedEvents()
			if hub.asyncOpsCount() == 0 {
				abort = false
				return
			}
		}

		select {
		case ev, ok := <-clientEvents:
			if !ok {
				clientEvents = nil
				continue
			}
			p.processAndBroadcast(hub, ev)
		case req := <-hub.ordered:
			drainClientEvents()
			p.processAndBroadcast(hub, req.event)
			if req.ack != nil {
				close(req.ack)
			}
		case <-hub.asyncWake:
			drainClientEvents()
			drainOrderedEvents()
		case <-hub.stop:
			drainClientEvents()
			drainOrderedEvents()
			return
		}
	}
}

func (p *Provider) processAndBroadcast(hub *eventHub, event events.Event) {
	if event.Event == "agent.message_chunk" {
		p.mu.Lock()
		p.ensureMapsLocked()
		p.streamedChunks[event.SessionID]++
		p.mu.Unlock()
	}
	p.cachePermissionOptionsWithHub(hub, event)
	if hub != nil {
		hub.broadcast(event)
	}
}

func (p *Provider) publish(event events.Event) {
	p.mu.Lock()
	hub := p.hub
	p.mu.Unlock()
	p.publishToHub(hub, event)
}

func (p *Provider) publishToHub(hub *eventHub, event events.Event) {
	if hub == nil {
		p.cachePermissionOptionsWithHub(nil, event)
		return
	}
	req := orderedEvent{
		event: event,
		ack:   make(chan struct{}),
	}
	select {
	case hub.ordered <- req:
	case <-hub.pumpDone:
		return
	}
	select {
	case <-req.ack:
	case <-hub.pumpDone:
	}
}

func (p *Provider) cachePermissionOptions(event events.Event) {
	p.mu.Lock()
	hub := p.hub
	p.mu.Unlock()
	p.cachePermissionOptionsWithHub(hub, event)
}

func (p *Provider) cachePermissionOptionsWithHub(hub *eventHub, event events.Event) {
	if event.Event == "session.end" {
		p.mu.Lock()
		if event.SessionID == "" {
			p.pendingOptions = nil
		} else {
			delete(p.pendingOptions, event.SessionID)
		}
		p.mu.Unlock()

		p.dispatchPendingMessages(hub, event.SessionID)
		return
	}
	if event.Event != "permission.request" {
		return
	}
	requestID, _ := event.Fields["request_id"].(string)
	if requestID == "" {
		return
	}
	options, _ := event.Fields["options"].([]any)
	if options == nil {
		return
	}
	clonedOptions := cloneSlice(options)
	p.mu.Lock()
	if p.pendingOptions == nil {
		p.pendingOptions = map[string]map[string][]any{}
	}
	sessionID := event.SessionID
	if p.pendingOptions[sessionID] == nil {
		p.pendingOptions[sessionID] = map[string][]any{}
	}
	p.pendingOptions[sessionID][requestID] = clonedOptions
	p.mu.Unlock()
}

func (p *Provider) session(sessionID string) (*Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	session := p.sessions[sessionID]
	if session == nil {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	return session, nil
}

func pendingPermissionOptions(pending map[string]map[string][]any, sessionID, requestID string) (string, []any) {
	if pending == nil {
		return "", nil
	}
	if sessionOptions := pending[sessionID]; sessionOptions != nil {
		if options := sessionOptions[requestID]; options != nil {
			return sessionID, options
		}
	}
	if sessionID != "" {
		if sessionOptions := pending[""]; sessionOptions != nil {
			if options := sessionOptions[requestID]; options != nil {
				return "", options
			}
		}
	}
	return "", nil
}

func permissionResponseResult(response runtime.PermissionResponse, optionID string) map[string]any {
	result := map[string]any{
		"outcome":  "selected",
		"optionId": optionID,
	}
	if response.Message != "" {
		result["message"] = response.Message
	}
	return map[string]any{"outcome": result}
}

// dispatchPendingMessages takes any queued channel-wrapped messages for a
// session at a turn boundary (after session.end) and dispatches injection on a
// separate goroutine so pumpClientEvents never blocks or deadlocks on publish.
func (p *Provider) dispatchPendingMessages(hub *eventHub, sessionID string) {
	if p.Queue == nil {
		return
	}
	msgs, done := p.Queue.Take(sessionID)
	if len(msgs) == 0 {
		return
	}
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		if done != nil {
			done()
		}
		return
	}
	p.ensureMapsLocked()
	p.injectReserved[sessionID]++
	p.mu.Unlock()
	if hub != nil {
		hub.beginAsyncOp()
	}
	go p.injectTakenMessages(hub, sessionID, msgs, done)
}

func (p *Provider) requeueMessages(sessionID string, msgs []string) {
	if p.Queue == nil || len(msgs) == 0 {
		return
	}
	p.mu.Lock()
	closing := p.closing
	p.mu.Unlock()
	if closing {
		return
	}
	for _, msg := range msgs {
		p.Queue.Enqueue(sessionID, msg)
	}
}

func (p *Provider) injectTakenMessages(hub *eventHub, sessionID string, msgs []string, done func()) {
	var myDone chan struct{}
	defer func() {
		p.mu.Lock()
		if myDone != nil && p.activePrompts[sessionID] == myDone {
			delete(p.activePrompts, sessionID)
		}
		if p.injectReserved[sessionID] > 1 {
			p.injectReserved[sessionID]--
		} else {
			delete(p.injectReserved, sessionID)
		}
		p.mu.Unlock()
		if myDone != nil {
			close(myDone)
		}
		if done != nil {
			done()
		}
		if hub != nil {
			hub.endAsyncOp()
		}
	}()

	var hubStop <-chan struct{}
	if hub != nil {
		hubStop = hub.stop
	}

	var (
		session *Session
		pollCtx context.Context
	)
	for {
		p.mu.Lock()
		if p.closing {
			p.mu.Unlock()
			return
		}
		session = p.sessions[sessionID]
		pollCtx = p.pollContexts[sessionID]
		if pollCtx == nil {
			pollCtx = context.Background()
		}
		if session == nil {
			p.mu.Unlock()
			p.requeueMessages(sessionID, msgs)
			return
		}
		inFlight := p.activePrompts[sessionID]
		if inFlight == nil {
			myDone = make(chan struct{})
			p.activePrompts[sessionID] = myDone
			p.mu.Unlock()
			break
		}
		p.mu.Unlock()

		select {
		case <-inFlight:
		case <-pollCtx.Done():
			p.requeueMessages(sessionID, msgs)
			return
		case <-hubStop:
			return
		}
	}

	for i, msg := range msgs {
		promptCtx, cancel := context.WithTimeout(pollCtx, 10*time.Second)
		_, err := session.Prompt(promptCtx, msg)
		cancel()
		if err != nil {
			if pollCtx.Err() == nil {
				p.requeueMessages(sessionID, msgs[i:])
			}
			return
		}
		p.publishToHub(hub, events.Event{
			Event:     "agent.message_chunk",
			SessionID: sessionID,
			Fields:    map[string]any{"source": "avenor-channel"},
		})
	}
}

func selectPermissionOption(options []any, approve bool) (string, error) {
	want := "reject"
	if approve {
		want = "allow"
	}
	for _, opt := range options {
		m, ok := opt.(map[string]any)
		if !ok {
			continue
		}
		optID, _ := m["optionId"].(string)
		kind := permission.NormalizeOptionKind(fmt.Sprint(m["kind"]))
		if kind == want {
			if optID == "" {
				return "", fmt.Errorf("permission option with kind %q missing optionId", want)
			}
			return optID, nil
		}
	}
	return "", fmt.Errorf("permission options missing kind %q", want)
}

func selectPermissionResponseOption(options []any, response runtime.PermissionResponse) (string, error) {
	if response.OptionID == "" {
		return selectPermissionOption(options, response.Allow)
	}
	want := "reject"
	if response.Allow {
		want = "allow"
	}
	for _, opt := range options {
		m, ok := opt.(map[string]any)
		if !ok {
			continue
		}
		optID, _ := m["optionId"].(string)
		if optID != response.OptionID {
			continue
		}
		kind := permission.NormalizeOptionKind(fmt.Sprint(m["kind"]))
		if kind != want {
			return "", fmt.Errorf("permission option %q has kind %q, want %q", response.OptionID, kind, want)
		}
		return optID, nil
	}
	return "", fmt.Errorf("permission options missing optionId %q", response.OptionID)
}

func cloneEvent(ev events.Event) events.Event {
	out := ev
	if ev.Fields != nil {
		out.Fields = cloneMap(ev.Fields)
	}
	return out
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneSlice(s []any) []any {
	if s == nil {
		return nil
	}
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = cloneValue(v)
	}
	return out
}

func cloneValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		return cloneMap(val)
	case []any:
		return cloneSlice(val)
	case []string:
		if val == nil {
			return nil
		}
		out := make([]string, len(val))
		copy(out, val)
		return out
	case map[string]string:
		if val == nil {
			return nil
		}
		out := make(map[string]string, len(val))
		for k, s := range val {
			out[k] = s
		}
		return out
	default:
		return v
	}
}
