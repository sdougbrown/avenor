package acp

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type ProviderConfig struct {
	BackendID           string
	Bin                 string
	Args                []string
	SubprocessDiscovery bool
	AppendCWDArg        bool
	Authenticate        string // optional: if non-empty, call Client.Authenticate with this methodId after Initialize
	ConfigureSession    func(ctx context.Context, session *Session, opts runtime.StartOptions) error
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

	subprocessDiscovery bool
	appendCWDArg        bool
	authenticateID      string
	configureSession    func(ctx context.Context, session *Session, opts runtime.StartOptions) error
}

func NewProvider(cfg ProviderConfig) runtime.Provider {
	return &Provider{
		opts:                runtime.StartOptions{},
		backendID:           cfg.BackendID,
		bin:                 cfg.Bin,
		args:                cfg.Args,
		sessions:            map[string]*Session{},
		subprocessDiscovery: cfg.SubprocessDiscovery,
		appendCWDArg:        cfg.AppendCWDArg,
		authenticateID:      cfg.Authenticate,
		configureSession:    cfg.ConfigureSession,
	}
}

var _ runtime.Provider = (*Provider)(nil)

func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := runtime.MergeStartOptions(p.opts, opts)
	if err := p.ensureClient(ctx, merged); err != nil {
		return runtime.Session{}, err
	}

	session, err := p.client.NewSession(ctx)
	if err != nil {
		return runtime.Session{}, err
	}
	if p.configureSession != nil {
		if err := p.configureSession(ctx, session, merged); err != nil {
			return runtime.Session{}, err
		}
	}

	p.mu.Lock()
	p.sessions[session.SessionID] = session
	p.mu.Unlock()

	return runtime.Session{
		SessionID: session.SessionID,
		Backend:   p.backendID,
		Dir:       merged.Dir,
		PID:       p.client.PID(),
	}, nil
}

func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	if sessionID == "" {
		return runtime.Session{}, errors.New("session id is required")
	}
	if err := p.ensureClient(ctx, p.opts); err != nil {
		return runtime.Session{}, err
	}

	session, err := p.client.ResumeSession(ctx, sessionID)
	if err != nil {
		return runtime.Session{}, err
	}
	if p.configureSession != nil {
		if err := p.configureSession(ctx, session, p.opts); err != nil {
			return runtime.Session{}, err
		}
	}

	p.mu.Lock()
	p.sessions[session.SessionID] = session
	p.mu.Unlock()

	return runtime.Session{
		SessionID: session.SessionID,
		Backend:   p.backendID,
		Dir:       p.opts.Dir,
		PID:       p.client.PID(),
	}, nil
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
	client := p.client
	p.client = nil
	p.pendingOptions = nil
	p.mu.Unlock()
	if client == nil {
		return nil
	}
	return client.Close()
}

func (p *Provider) ensureClient(ctx context.Context, opts runtime.StartOptions) error {
	p.mu.Lock()
	if p.client != nil {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	if opts.ServerURL != "" {
		return fmt.Errorf("%s external server URL transport is not implemented by the stdio client", p.backendID)
	}

	client, err := NewClient(ctx, ClientConfig{
		Bin:                  p.bin,
		Args:                 p.args,
		Dir:                  opts.Dir,
		AutoAnswerPermission: false,
		AppendCWDArg:         p.appendCWDArg,
	})
	if err != nil {
		return err
	}
	if _, err := client.Initialize(ctx); err != nil {
		_ = client.Close()
		return err
	}
	if p.authenticateID != "" {
		if err := client.Authenticate(ctx, p.authenticateID); err != nil {
			_ = client.Close()
			return err
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		_ = client.Close()
		return nil
	}
	p.client = client
	p.events = make(chan events.Event, 256)
	go p.pumpClientEvents(client, p.events)
	return nil
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


