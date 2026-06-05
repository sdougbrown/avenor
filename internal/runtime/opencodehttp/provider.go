package opencodehttp

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

const backendID = "opencode-http"

// Provider implements runtime.Provider for opencode serve's HTTP API.
type Provider struct {
	opts runtime.StartOptions

	mu            sync.Mutex
	client        *Client
	started       bool
	streamCancel  context.CancelFunc
	sessions      map[string]sessionState
	subscribers   map[chan events.Event]string
	endedSessions map[string]bool // guards double-emit of session.end

	// Broker fields for channel-wrapped prompt injection.
	broker          *broker.Broker
	runIDs          map[string]string // sessionID → runID
	pollCancels     map[string]context.CancelFunc
	pendingMu       sync.Mutex
	pendingMessages map[string][]string // sessionID → queued wrapped prompts
}

type sessionState struct {
	sessionID string
	dir       string
	opts      runtime.StartOptions
}

// New creates a Provider with empty options.
func New() (runtime.Provider, error) {
	return NewWithOptions(runtime.StartOptions{})
}

// NewWithOptions creates a Provider with the given options.
func NewWithOptions(opts runtime.StartOptions) (runtime.Provider, error) {
	return &Provider{
		opts:            opts,
		sessions:        make(map[string]sessionState),
		subscribers:     make(map[chan events.Event]string),
		endedSessions:   make(map[string]bool),
		runIDs:          make(map[string]string),
		pollCancels:     make(map[string]context.CancelFunc),
		pendingMessages: make(map[string][]string),
	}, nil
}

var _ runtime.Provider = (*Provider)(nil)

func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := runtime.MergeStartOptions(p.opts, opts)
	if merged.ServerURL == "" {
		return runtime.Session{}, errors.New("server URL is required for opencode-http backend")
	}
	sessionDir, err := supportedDir(merged.Dir)
	if err != nil {
		return runtime.Session{}, err
	}
	c, err := p.ensureClient(ctx, merged)
	if err != nil {
		return runtime.Session{}, err
	}
	// Persist ServerURL so Resume works after Start-only URL provision.
	p.mu.Lock()
	p.opts.ServerURL = merged.ServerURL
	p.mu.Unlock()

	sessionID, err := c.CreateSession(ctx)
	if err != nil {
		return runtime.Session{}, fmt.Errorf("create session: %w", err)
	}

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
	merged.Dir = sessionDir
	p.sessions[sessionID] = sessionState{sessionID: sessionID, dir: sessionDir, opts: merged}
	p.runIDs[sessionID] = runID
	p.mu.Unlock()

	// Start the broker message polling goroutine.
	if runID != "" {
		pollCtx, cancel := context.WithCancel(ctx)
		p.mu.Lock()
		p.pollCancels[sessionID] = cancel
		p.mu.Unlock()
		go p.broker.PollAgentMessages(pollCtx, runID, func(wrapped string) {
			p.pendingMu.Lock()
			p.pendingMessages[sessionID] = append(p.pendingMessages[sessionID], wrapped)
			p.pendingMu.Unlock()
		})
	}

	return runtime.Session{
		SessionID: sessionID,
		Backend:   backendID,
		Dir:       sessionDir,
	}, nil
}

func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	if sessionID == "" {
		return runtime.Session{}, errors.New("session id is required")
	}
	p.mu.Lock()
	serverURL := p.opts.ServerURL
	p.mu.Unlock()
	if serverURL == "" {
		return runtime.Session{}, errors.New("server URL is required for opencode-http backend")
	}
	c, err := p.ensureClient(ctx, p.opts)
	if err != nil {
		return runtime.Session{}, err
	}
	info, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return runtime.Session{}, fmt.Errorf("resume session %q: %w", sessionID, err)
	}
	dir, _ := info["directory"].(string)

	p.mu.Lock()
	resumeOpts := p.opts
	resumeOpts.Dir = dir
	p.sessions[sessionID] = sessionState{sessionID: sessionID, dir: dir, opts: resumeOpts}
	p.mu.Unlock()

	return runtime.Session{
		SessionID: sessionID,
		Backend:   backendID,
		Dir:       dir,
	}, nil
}

func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	c, err := p.clientLocked()
	if err != nil {
		return err
	}
	p.beginSessionTurn(sessionID)
	opts := p.sessionOptions(sessionID)
	payload := map[string]any{
		"parts": []map[string]any{
			{"type": "text", "text": prompt},
		},
	}
	if opts.Agent != "" {
		payload["agent"] = opts.Agent
	}
	if opts.Model != "" {
		payload["model"] = mapModel(opts.Model)
	}
	result, err := c.SendMessage(ctx, sessionID, payload)
	if err != nil {
		return err
	}
	if evt, ok := messageResultEndEvent(sessionID, result); ok {
		p.publishSessionEnd(evt)
	}
	return nil
}

func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	c, err := p.clientLocked()
	if err != nil {
		return err
	}
	return c.Abort(ctx, sessionID)
}

func (p *Provider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil, errors.New("provider has not been started")
	}
	out := make(chan events.Event, 128)
	p.subscribers[out] = sessionID
	p.mu.Unlock()

	go func() {
		<-ctx.Done()
		p.removeSubscriber(out)
	}()
	return out, nil
}

func (p *Provider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	if requestID == "" {
		return errors.New("permission request id is required")
	}
	c, err := p.clientLocked()
	if err != nil {
		return fmt.Errorf("answer permission: %w", err)
	}
	reply := "once"
	if !response.Allow {
		reply = "reject"
	} else if response.OptionID == "allow_always" || response.OptionID == "always" {
		reply = "always"
	}
	return c.AnswerPermission(ctx, sessionID, requestID, map[string]any{
		"reply":    reply,
		"response": reply,
		"remember": reply == "always",
	})
}

func (p *Provider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{
		Backend:             backendID,
		Permissions:         true,
		Resume:              true,
		ExternalServerURL:   true,
		SubprocessDiscovery: false,
		ModelSelection:      true,
	}, nil
}

// Close shuts down the event stream and releases resources.
func (p *Provider) Close() error {
	p.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(p.pollCancels))
	for _, cancel := range p.pollCancels {
		cancels = append(cancels, cancel)
	}
	if p.streamCancel != nil {
		p.streamCancel()
		p.streamCancel = nil
	}
	p.pollCancels = make(map[string]context.CancelFunc)
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
	return nil
}

func (p *Provider) ensureClient(ctx context.Context, opts runtime.StartOptions) (*Client, error) {
	p.mu.Lock()
	if p.client != nil {
		c := p.client
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()

	// Store broker reference if provided.
	if opts.Broker != nil {
		p.mu.Lock()
		p.broker = opts.Broker
		p.mu.Unlock()
	}

	co, safeURL := clientOptionsFromURL(opts.ServerURL)
	client := NewClient(co)
	healthCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := client.Health(healthCtx); err != nil {
		return nil, fmt.Errorf("connect to opencode server at %s: %w", safeURL, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client, nil
	}
	p.client = client
	p.started = true
	streamCtx, streamCancel := context.WithCancel(context.Background())
	p.streamCancel = streamCancel

	// Start the SSE event stream in the background.
	go p.streamEvents(streamCtx, client)
	return client, nil
}

func (p *Provider) streamEvents(ctx context.Context, c *Client) {
	defer func() {
		p.mu.Lock()
		p.streamCancel = nil
		p.started = false
		p.closeSubscribersLocked()
		p.mu.Unlock()
	}()

	backoff := 100 * time.Millisecond
	for {
		body, err := c.StreamEvents(ctx)
		if err != nil {
			if !sleepOrDone(ctx, backoff) {
				return
			}
			if backoff < time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 100 * time.Millisecond

		rawEvents := make(chan events.Event, 64)
		go readSSEEvents(ctx, body, rawEvents)

		for {
			select {
			case evt, ok := <-rawEvents:
				if !ok {
					body.Close()
					goto reconnect
				}
				if evt.Event == "session.end" {
					// Inject pending channel-wrapped messages at turn boundary.
					p.injectPendingMessages(ctx, evt.SessionID)
					// POST /message is the authoritative terminal signal. Use
					// publishSessionEnd to deduplicate: if Prompt already
					// emitted session.end from the synchronous POST response,
					// publishSessionEnd skips this SSE event. If the POST
					// response carried no stop reason, this SSE event serves as
					// the fallback so subscribers don't hang.
					p.publishSessionEnd(evt)
				} else {
					p.publish(evt)
				}
			case <-ctx.Done():
				body.Close()
				return
			}
		}
	reconnect:
		if !sleepOrDone(ctx, backoff) {
			return
		}
	}
}

func (p *Provider) clientLocked() (*Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == nil {
		return nil, errors.New("provider has not been started")
	}
	return p.client, nil
}

func (p *Provider) sessionOptions(sessionID string) runtime.StartOptions {
	p.mu.Lock()
	defer p.mu.Unlock()
	if session, ok := p.sessions[sessionID]; ok {
		return session.opts
	}
	return p.opts
}

func (p *Provider) publish(event events.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for ch, sessionID := range p.subscribers {
		if sessionID == "" || event.SessionID == sessionID {
			select {
			case ch <- event:
			default:
				delete(p.subscribers, ch)
				close(ch)
			}
		}
	}
}

func (p *Provider) beginSessionTurn(sessionID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.endedSessions, sessionID)
}

// publishSessionEnd emits a session.end event, skipping if the session has
// already ended (guards against duplicate emits from the SSE idle transition
// and the synchronous POST /message response).
func (p *Provider) publishSessionEnd(evt events.Event) {
	p.mu.Lock()
	if p.endedSessions[evt.SessionID] {
		p.mu.Unlock()
		return
	}
	p.endedSessions[evt.SessionID] = true
	p.mu.Unlock()
	p.publish(evt)
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func messageResultEndEvent(sessionID string, result map[string]any) (events.Event, bool) {
	info, _ := result["info"].(map[string]any)
	if info == nil {
		return events.Event{}, false
	}
	stopReason := ""
	if errObj, _ := info["error"].(map[string]any); errObj != nil {
		errName, _ := errObj["name"].(string)
		stopReason = mapErrorToStopReason(errName)
	}
	if stopReason == "" {
		finish, _ := info["finish"].(string)
		stopReason = mapFinishToStopReason(finish)
	}
	if stopReason == "" {
		return events.Event{}, false
	}
	return events.Event{
		Event:     "session.end",
		SessionID: sessionID,
		Fields: map[string]any{
			"stop_reason": stopReason,
		},
	}, true
}

func mapFinishToStopReason(finish string) string {
	switch finish {
	case "stop":
		return "end_turn"
	case "":
		return ""
	default:
		return finish
	}
}

func (p *Provider) removeSubscriber(ch chan events.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.subscribers[ch]; ok {
		delete(p.subscribers, ch)
		close(ch)
	}
}

func (p *Provider) closeSubscribersLocked() {
	for ch := range p.subscribers {
		delete(p.subscribers, ch)
		close(ch)
	}
}

// mapModel converts a StartOptions.Model string (e.g. "deepseek/deepseek-v4-pro")
// to the object format expected by opencode serve.
func mapModel(model string) map[string]string {
	result := map[string]string{}
	if model == "" {
		return result
	}
	if idx := strings.IndexByte(model, '/'); idx >= 0 {
		result["providerID"] = model[:idx]
		result["modelID"] = model[idx+1:]
	} else {
		result["modelID"] = model
	}
	return result
}

func supportedDir(dir string) (string, error) {
	switch strings.TrimSpace(dir) {
	case "", ".":
		return "", nil
	default:
		return "", errors.New("opencode-http backend does not support --dir; start opencode serve in the target directory instead")
	}
}

// clientOptionsFromURL extracts credentials from the URL userinfo and returns
// ClientOptions with a credential-free base URL plus a safe URL for logging.
func clientOptionsFromURL(rawURL string) (ClientOptions, string) {
	u, err := url.Parse(rawURL)
	if err != nil {
		// Parse failure: return empty BaseURL and placeholder to avoid
		// leaking rawURL (which may contain credentials) into error
		// messages or network-level error strings.
		return ClientOptions{}, "(invalid url)"
	}
	co := ClientOptions{}
	if u.User != nil {
		co.Username = u.User.Username()
		co.Password, _ = u.User.Password()
		u.User = nil
	}
	co.BaseURL = u.String()
	return co, u.Redacted()
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
	p.beginSessionTurn(sessionID)
	for _, msg := range msgs {
		p.Prompt(ctx, sessionID, msg)
	}
}
