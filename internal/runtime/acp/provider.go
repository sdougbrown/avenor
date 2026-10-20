package acp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// errProviderClosed is returned by a launch the provider abandoned because
// Close ran first.
var errProviderClosed = errors.New("provider is closed")

// launchSettleTimeout bounds how long Close waits for an in-flight launch to
// return before it finishes on its own. A launch can be stuck in a start that
// ignores the caller's context.
const launchSettleTimeout = 2 * time.Second

type ProviderConfig struct {
	BackendID           string
	Bin                 string
	Args                []string
	SubprocessDiscovery bool
	AppendCWDArg        bool
	Authenticate        string               // optional: if non-empty, call Client.Authenticate with this methodId after Initialize
	Opts                runtime.StartOptions // authoritative launch options; retained so Resume launches with the same wrapper and settings
	ConfigureSession    func(ctx context.Context, session *Session, opts runtime.StartOptions) error
	BuildClientEnv      func(opts runtime.StartOptions, runID, brokerToken, brokerAddr string) (map[string]string, func() error)
}

type Provider struct {
	opts runtime.StartOptions

	mu             sync.Mutex
	backendID      string
	bin            string
	args           []string
	client         *Client
	events         chan events.Event
	sessions       map[string]*Session
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

	subprocessDiscovery bool
	appendCWDArg        bool
	authenticateID      string
	configureSession    func(ctx context.Context, session *Session, opts runtime.StartOptions) error
	buildClientEnv      func(opts runtime.StartOptions, runID, brokerToken, brokerAddr string) (map[string]string, func() error)
	clientEnvCleanup    func() error

	// Broker fields for channel-wrapped prompt injection.
	broker          *broker.Broker
	runIDs          map[string]string // sessionID → runID
	pollContexts    map[string]context.Context
	pollCancels     map[string]context.CancelFunc
	pendingMu       sync.Mutex
	pendingMessages map[string][]string // sessionID → queued wrapped prompts
}

func NewProvider(cfg ProviderConfig) runtime.Provider {
	return &Provider{
		opts:                cfg.Opts,
		backendID:           cfg.BackendID,
		bin:                 cfg.Bin,
		args:                cfg.Args,
		sessions:            map[string]*Session{},
		runIDs:              map[string]string{},
		pollContexts:        map[string]context.Context{},
		pollCancels:         map[string]context.CancelFunc{},
		pendingMessages:     map[string][]string{},
		subprocessDiscovery: cfg.SubprocessDiscovery,
		appendCWDArg:        cfg.AppendCWDArg,
		authenticateID:      cfg.Authenticate,
		configureSession:    cfg.ConfigureSession,
		buildClientEnv:      cfg.BuildClientEnv,
	}
}

var _ runtime.Provider = (*Provider)(nil)

func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := runtime.MergeStartOptions(p.opts, opts)

	runID, brokerToken, brokerRef := p.launchRun(merged)
	if _, err := p.ensureClient(ctx, merged, func() (map[string]string, func() error) {
		return p.launchEnv(merged, runID, brokerToken, brokerRef)
	}); err != nil {
		return runtime.Session{}, err
	}

	session, err := p.client.NewSessionWithOptions(ctx, SessionOpenOptions{})
	if err != nil {
		return runtime.Session{}, err
	}
	if p.configureSession != nil {
		if err := p.configureSession(ctx, session, merged); err != nil {
			return runtime.Session{}, err
		}
	}

	session.RunID = runID
	session.BrokerToken = brokerToken
	p.mu.Lock()
	p.sessions[session.SessionID] = session
	if runID != "" {
		p.runIDs[session.SessionID] = runID
	}
	p.mu.Unlock()

	// Start the broker message polling goroutine if we have a broker and runID.
	if runID != "" {
		pollCtx, cancel := context.WithCancel(ctx)
		p.mu.Lock()
		p.pollContexts[session.SessionID] = pollCtx
		p.pollCancels[session.SessionID] = cancel
		p.mu.Unlock()
		go brokerRef.PollAgentMessages(pollCtx, runID, func(wrapped string) {
			p.pendingMu.Lock()
			p.pendingMessages[session.SessionID] = append(p.pendingMessages[session.SessionID], wrapped)
			p.pendingMu.Unlock()
		})
	}

	return runtime.Session{
		SessionID: session.SessionID,
		Backend:   p.backendID,
		Dir:       merged.Dir,
		PID:       p.client.PID(),
	}, nil
}

func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	return p.resume(ctx, sessionID, runtime.MergeStartOptions(p.opts, runtime.StartOptions{}), false)
}

// ResumeWithOptions dispatches a resume with the merged per-call launch
// options, so a client started by this call carries the caller's identity and
// configuration.
func (p *Provider) ResumeWithOptions(ctx context.Context, sessionID string, opts runtime.StartOptions) (runtime.Session, error) {
	return p.resume(ctx, sessionID, runtime.MergeStartOptions(p.opts, opts), true)
}

func (p *Provider) resume(ctx context.Context, sessionID string, opts runtime.StartOptions, optionAware bool) (runtime.Session, error) {
	if sessionID == "" {
		return runtime.Session{}, errors.New("session id is required")
	}
	// Only a new client needs a launch environment. A live client keeps the
	// identity and config it was started with, so resuming it builds nothing;
	// an option-aware resume still retains the caller's broker.
	var buildEnv func() (map[string]string, func() error)
	if optionAware {
		p.retainBroker(opts)
		buildEnv = func() (map[string]string, func() error) {
			runID, brokerToken, brokerRef := p.launchRun(opts)
			return p.launchEnv(opts, runID, brokerToken, brokerRef)
		}
	}
	if _, err := p.ensureClient(ctx, opts, buildEnv); err != nil {
		return runtime.Session{}, err
	}

	session, err := p.client.ResumeSession(ctx, sessionID)
	if err != nil {
		return runtime.Session{}, err
	}
	if p.configureSession != nil {
		if err := p.configureSession(ctx, session, opts); err != nil {
			return runtime.Session{}, err
		}
	}

	p.mu.Lock()
	p.sessions[session.SessionID] = session
	p.mu.Unlock()

	return runtime.Session{
		SessionID: session.SessionID,
		Backend:   p.backendID,
		Dir:       opts.Dir,
		PID:       p.client.PID(),
	}, nil
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
func (p *Provider) launchRun(opts runtime.StartOptions) (runID, brokerToken string, brokerRef *broker.Broker) {
	brokerRef = p.retainBroker(opts)
	if brokerRef == nil {
		return "", "", nil
	}
	runID = opts.RuntimeID
	if runID == "" {
		runID = uuid.New().String()
	}
	brokerToken, _ = brokerRef.EnsureRun(runID, opts.Label)
	return runID, brokerToken, brokerRef
}

// launchEnv builds the backend's client environment and its cleanup for one
// launch.
func (p *Provider) launchEnv(opts runtime.StartOptions, runID, brokerToken string, brokerRef *broker.Broker) (map[string]string, func() error) {
	p.mu.Lock()
	buildClientEnv := p.buildClientEnv
	p.mu.Unlock()
	if buildClientEnv == nil {
		return nil, nil
	}
	// Always call buildClientEnv so backends can inject config overrides (e.g.
	// mode overrides for subagent-mode agents) even without a broker. The
	// backend is responsible for returning nil when there's nothing to override,
	// so no temp file is created unnecessarily.
	brokerAddr := ""
	if brokerRef != nil {
		brokerAddr = brokerRef.Addr()
	}
	return buildClientEnv(opts, runID, brokerToken, brokerAddr)
}

func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	session, err := p.session(sessionID)
	if err != nil {
		return err
	}
	event, err := session.Prompt(ctx, prompt)
	if err != nil {
		return err
	}
	p.publish(event)
	return nil
}

func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	session, err := p.session(sessionID)
	if err != nil {
		return err
	}
	return session.Cancel()
}

func (p *Provider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	source := p.events
	p.mu.Unlock()
	if source == nil {
		return nil, errors.New("provider has not been started")
	}

	out := make(chan events.Event, 128)
	go func() {
		defer close(out)
		for {
			select {
			case event, ok := <-source:
				if !ok {
					return
				}
				if sessionID == "" || event.SessionID == "" || event.SessionID == sessionID {
					out <- event
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
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
	client := p.client
	idle := p.launchIdle
	cancels := make([]context.CancelFunc, 0, len(p.pollCancels))
	for _, cancel := range p.pollCancels {
		cancels = append(cancels, cancel)
	}
	p.client = nil
	cleanup := p.clientEnvCleanup
	p.clientEnvCleanup = nil
	p.pendingOptions = nil
	p.runIDs = nil
	p.pollContexts = map[string]context.Context{}
	p.pollCancels = map[string]context.CancelFunc{}
	p.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	// A launch still in flight sees the closing flag when it tries to install,
	// tears down what it started, and returns an error. Wait for that within a
	// bound, since a start can ignore the caller's context and never return.
	if idle != nil {
		select {
		case <-idle:
		case <-time.After(launchSettleTimeout):
		}
	}
	p.pendingMu.Lock()
	p.pendingMessages = map[string][]string{}
	p.pendingMu.Unlock()
	if cleanup != nil {
		_ = cleanup()
	}
	if client == nil {
		return nil
	}
	return client.Close()
}

// beginLaunch admits one launch attempt, rejecting it once Close has run.
func (p *Provider) beginLaunch() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return fmt.Errorf("%s: %w", p.backendID, errProviderClosed)
	}
	if p.launching == 0 {
		p.launchIdle = make(chan struct{})
	}
	p.launching++
	return nil
}

// settleLaunch records that an admitted launch returned, waking Close once the
// last one has.
func (p *Provider) settleLaunch() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.launching--
	if p.launching > 0 || p.launchIdle == nil {
		return
	}
	close(p.launchIdle)
	p.launchIdle = nil
}

// ensureClient starts the backend client when none is retained yet. buildEnv,
// when non-nil, builds the environment for that new client only, so a retained
// client never pays for a launch it will not use. A launch that does not become
// the provider's client — because a client is already live, another launch won
// the install, Close ran first, or startup failed — cleans up its own generated
// config before returning. A retained client takes ownership of its config
// until Close.
func (p *Provider) ensureClient(ctx context.Context, opts runtime.StartOptions, buildEnv func() (map[string]string, func() error)) (bool, error) {
	p.mu.Lock()
	retained := p.client != nil
	p.mu.Unlock()
	if retained {
		return false, nil
	}
	if err := p.beginLaunch(); err != nil {
		return false, err
	}
	defer p.settleLaunch()

	if opts.ServerURL != "" {
		return false, fmt.Errorf("%s external server URL transport is not implemented by the stdio client", p.backendID)
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
	client, err := start(ctx, ClientConfig{
		Bin:                  p.bin,
		Args:                 p.args,
		Wrapper:              opts.LaunchWrapper,
		Dir:                  opts.Dir,
		Env:                  env,
		AutoAnswerPermission: false,
		AppendCWDArg:         p.appendCWDArg,
		Provenance:           opts.Provenance,
	})
	if err != nil {
		runCleanup(cleanup)
		return false, err
	}

	p.mu.Lock()
	switch {
	case p.closing:
		p.mu.Unlock()
		_ = client.Close()
		runCleanup(cleanup)
		return false, fmt.Errorf("%s: %w", p.backendID, errProviderClosed)
	case p.client != nil:
		p.mu.Unlock()
		_ = client.Close()
		runCleanup(cleanup)
		return false, nil
	}
	p.client = client
	p.clientEnvCleanup = cleanup
	p.events = make(chan events.Event, 256)
	go p.pumpClientEvents(client, p.events)
	p.mu.Unlock()
	return true, nil
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

func (p *Provider) pumpClientEvents(client *Client, out chan<- events.Event) {
	for event := range client.Events() {
		p.cachePermissionOptions(event)
		out <- event
	}
}

func (p *Provider) publish(event events.Event) {
	p.cachePermissionOptions(event)
	p.mu.Lock()
	out := p.events
	p.mu.Unlock()
	if out == nil {
		return
	}
	out <- event
}

func (p *Provider) cachePermissionOptions(event events.Event) {
	if event.Event == "session.end" {
		// Inject any pending channel-wrapped messages before clearing.
		p.injectPendingMessages(event.SessionID)

		p.mu.Lock()
		if event.SessionID == "" {
			p.pendingOptions = nil
			p.mu.Unlock()
			return
		}
		delete(p.pendingOptions, event.SessionID)
		p.mu.Unlock()
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
	p.mu.Lock()
	if p.pendingOptions == nil {
		p.pendingOptions = map[string]map[string][]any{}
	}
	sessionID := event.SessionID
	if p.pendingOptions[sessionID] == nil {
		p.pendingOptions[sessionID] = map[string][]any{}
	}
	p.pendingOptions[sessionID][requestID] = options
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

// injectPendingMessages injects queued channel-wrapped messages for a session
// at a turn boundary (after session.end).
func (p *Provider) injectPendingMessages(sessionID string) {
	p.pendingMu.Lock()
	if p.pendingMessages == nil {
		p.pendingMu.Unlock()
		return
	}
	msgs := p.pendingMessages[sessionID]
	p.pendingMessages[sessionID] = nil
	p.pendingMu.Unlock()

	if len(msgs) == 0 {
		return
	}

	p.mu.Lock()
	session := p.sessions[sessionID]
	pollCtx := p.pollContexts[sessionID]
	p.mu.Unlock()
	if session == nil {
		return
	}
	if pollCtx == nil {
		pollCtx = context.Background()
	}

	for _, msg := range msgs {
		promptCtx, cancel := context.WithTimeout(pollCtx, 10*time.Second)
		_, err := session.Prompt(promptCtx, msg)
		cancel()
		if err != nil {
			continue
		}
		p.publish(events.Event{
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
