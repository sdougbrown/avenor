package pi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

const backendID = "pi"

type Provider struct {
	opts        runtime.StartOptions
	mu          sync.Mutex
	client      *client
	sessions    map[string]struct{}
	startClient func(ctx context.Context, opts runtime.StartOptions, resumeSessionID string) (*client, error)
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

	sessionID, err := childSessionID(ctx, c)
	if err != nil {
		return runtime.Session{}, err
	}
	if sessionID == "" {
		sessionID = "pi-" + newRequestID()
		c.stderr.Append("session ID not found in get_state response, generating new session ID")
	}

	if err := p.registerClient(sessionID, c); err != nil {
		return runtime.Session{}, err
	}

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

func (p *Provider) resume(ctx context.Context, sessionID string, opts runtime.StartOptions, optionAware bool) (runtime.Session, error) {
	if sessionID == "" {
		return runtime.Session{}, errors.New("session id is required")
	}
	merged := runtime.MergeStartOptions(p.opts, opts)
	// Thinking is a per-call request: the option-aware path applies only what
	// the caller asked for, never a constructor default, and plain resume keeps
	// its existing empty behavior.
	if optionAware {
		merged.Thinking = opts.Thinking
	} else {
		merged.Thinking = ""
	}
	if err := runtime.ValidateThinkingForBackend(backendID, merged.Thinking); err != nil {
		return runtime.Session{}, err
	}

	p.mu.Lock()
	_, exists := p.sessions[sessionID]
	c := p.client
	p.mu.Unlock()
	if !exists {
		// Without persisted sessions the prior run's transcript is gone, so
		// a fresh process could only run an empty session under this ID's
		// name; refuse instead of silently losing the prior context.
		if !SessionsPersistEnabled() {
			return runtime.Session{}, fmt.Errorf("session not found: %s (pi sessions run with --no-session; set %s=1 to persist sessions and enable cross-process resume)", sessionID, SessionsEnv)
		}
		return p.resumePersisted(ctx, sessionID, merged)
	}

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

// resumePersisted continues a prior run's persisted session in a fresh pi
// child: the persisted transcript supplies the prior context, so the resumed
// run is a real continuation, not a fresh session wearing the old ID. The
// session file must already exist — the caller read it from a sentinel —
// and the child's own get_state must report the same session ID, or the
// resume is refused rather than silently continuing elsewhere.
func (p *Provider) resumePersisted(ctx context.Context, sessionID string, opts runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	occupied := len(p.sessions) > 0
	p.mu.Unlock()
	if occupied {
		return runtime.Session{}, errors.New("pi rpc supports only one active session per provider")
	}
	dir, err := PersistSessionsDir()
	if err != nil {
		return runtime.Session{}, err
	}
	fileExists, err := SessionFileExists(dir, sessionID)
	if err != nil {
		return runtime.Session{}, err
	}
	if !fileExists {
		return runtime.Session{}, fmt.Errorf("session not found: %s (no persisted pi session file in %s; the prior run may have launched without %s=1, or pi never reported a real session id)", sessionID, dir, SessionsEnv)
	}
	if opts.Thinking != "" {
		if err := checkThinkingCapability(ctx); err != nil {
			return runtime.Session{}, fmt.Errorf("%w: %v", runtime.NewUnsupportedThinkingError(backendID), err)
		}
	}
	starter := p.startClient
	if starter == nil {
		starter = p.launchChild
	}
	c, err := starter(ctx, opts, sessionID)
	if err != nil {
		return runtime.Session{}, err
	}
	got, err := childSessionID(ctx, c)
	if err != nil {
		c.Close()
		return runtime.Session{}, err
	}
	if got != sessionID {
		c.Close()
		return runtime.Session{}, fmt.Errorf("resumed pi child reports session %q, want %s", got, sessionID)
	}

	if err := p.registerClient(sessionID, c); err != nil {
		return runtime.Session{}, err
	}
	return runtime.Session{SessionID: sessionID, Backend: backendID, Dir: opts.Dir, PID: c.Pid()}, nil
}

// registerClient installs c as the provider's single live session child. pi
// rpc supports one registered session per provider: registering while a
// session exists refuses and closes the fresh child, and a client left
// behind by a failed start is closed when the registration replaces it, so
// no live pi child is orphaned.
func (p *Provider) registerClient(sessionID string, c *client) error {
	p.mu.Lock()
	if len(p.sessions) > 0 {
		p.mu.Unlock()
		c.Close()
		return errors.New("pi rpc supports only one active session per provider")
	}
	replaced := p.client
	// The session ID is set before the registration publishes: a prompt
	// landing after the unlock must never see a client whose events would
	// fan out under an empty session ID.
	c.setSessionID(sessionID)
	p.sessions[sessionID] = struct{}{}
	p.client = c
	p.mu.Unlock()
	if replaced != nil && replaced != c {
		replaced.Close()
	}
	return nil
}

// childSessionID reads the session ID a fresh pi child reports at startup.
// Start and resumePersisted both read it here, so a change to the get_state
// response shape is made once.
func childSessionID(ctx context.Context, c *client) (string, error) {
	stateResult, err := c.sendCommand(ctx, map[string]any{"type": "get_state"})
	if err != nil {
		return "", fmt.Errorf("get_state: %w", err)
	}
	var stateMap map[string]any
	if err := json.Unmarshal(stateResult, &stateMap); err != nil {
		return "", fmt.Errorf("get_state result: %w", err)
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
	return sessionID, nil
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

	// Only this turn's output may describe this turn's failure: the rolling
	// buffer is process-lifetime, so the turn reads from its own mark instead
	// of discarding what an earlier turn printed.
	stderrMark := c.stderr.Mark()
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
	settle := func() {
		c.unsubscribe(sessionID, subCh)
		close(subCh)
		<-done
	}

	cmd := map[string]any{
		"type":    "prompt",
		"message": prompt,
		"id":      newRequestID(),
	}
	result, err := c.sendCommand(ctx, cmd)
	if err != nil {
		settle()
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := checkSuccessResponse(result, "prompt"); err != nil {
		settle()
		return err
	}

	select {
	case ev := <-turnCh:
		settle()
		return piTurnResult(sessionID, ev, c.stderrSince(stderrMark))
	case <-c.done:
		// The child exited before settling the turn. readLoop closes c.done
		// right after it fans the terminal event out, and the forwarder may not
		// have moved it to turnCh yet: close and drain the subscription first,
		// then read the queued turn before reporting the exit. Waiting on the
		// caller's deadline would misreport the failure as a timeout.
		settle()
		select {
		case ev := <-turnCh:
			return piTurnResult(sessionID, ev, c.stderrSince(stderrMark))
		default:
		}
		// The exit cause is whatever the child printed, and the last lines may
		// still be in the pipe: let the stderr reader drain before reading it.
		c.waitStderrDrain(piStderrDrainWait)
		return fmt.Errorf("turn failed for session %s: %s", sessionID, piExitCause(c.stderrSince(stderrMark)))
	case <-ctx.Done():
		settle()
		return ctx.Err()
	}
}

// piTurnResult maps a settled agent_end onto Prompt's result. The captured
// stderr carries failures that the event itself does not name, so it is the
// cause when error_message is absent.
func piTurnResult(sessionID string, ev events.Event, stderr string) error {
	reason, _ := ev.Fields["stop_reason"]
	switch reason {
	case "end_turn", "end_of_turn":
		return nil
	case "cancelled":
		return fmt.Errorf("turn cancelled for session %s", sessionID)
	case "error":
		msg, _ := ev.Fields["error_message"].(string)
		detail := piStderrTail(stderr)
		switch {
		case msg == "":
			msg = detail
		case detail != "":
			msg = fmt.Sprintf("%s (stderr: %s)", msg, detail)
		}
		if msg == "" {
			msg = "unknown error"
		}
		return fmt.Errorf("turn failed for session %s: %s", sessionID, msg)
	default:
		return nil
	}
}

func piExitCause(stderr string) string {
	if tail := piStderrTail(stderr); tail != "" {
		return tail
	}
	return "pi exited before the turn settled"
}

// piStderrDrainWait bounds how long the exit path waits for the child's last
// stderr lines: the pipe closes with the child, so this only covers a racing
// reader.
const piStderrDrainWait = 250 * time.Millisecond

// piStderrTail keeps only the last few captured stderr lines: the same buffer
// also accumulates client-side diagnostics.
func piStderrTail(stderr string) string {
	stderr = strings.TrimSpace(stderr)
	if stderr == "" {
		return ""
	}
	lines := strings.Split(stderr, "\n")
	if len(lines) > 3 {
		lines = lines[len(lines)-3:]
	}
	return strings.Join(lines, " | ")
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
		starter = p.launchChild
	}
	c, err := starter(ctx, opts, "")
	if err != nil {
		p.mu.Unlock()
		return nil, false, err
	}
	p.client = c
	p.mu.Unlock()
	return c, true, nil
}

// launchChild spawns the pi RPC child. A persisted-session environment
// points the child at the shared session directory; the ephemeral default
// launches --no-session. resumeSessionID binds a fresh child to a prior
// run's persisted session so its transcript continues.
func (p *Provider) launchChild(ctx context.Context, opts runtime.StartOptions, resumeSessionID string) (*client, error) {
	sessionDir := ""
	if SessionsPersistEnabled() {
		dir, err := PersistSessionsDir()
		if err != nil {
			return nil, fmt.Errorf("pi persisted sessions: %w", err)
		}
		sessionDir = dir
	}
	// The child is broker-configured only as a whole: URL, run id, and
	// token travel together or not at all, so it can never poll with a
	// half-configured identity. pi's extension authenticates each request
	// itself, so what it needs is the run's own credential.
	url, runID, token := runtime.ProvisionPeer(opts).Run()
	return StartClientWithAgentProfileThinkingAndDir(
		ctx, "", opts.Model, sessionDir, opts.Agent, opts.AgentProfile, opts.Thinking, opts.Dir,
		url, runID, token, opts.LaunchWrapper, opts.Provenance, resumeSessionID,
	)
}

func (p *Provider) setThinkingLevel(ctx context.Context, c *client, level string) error {
	result, err := c.sendCommand(ctx, map[string]any{
		"type":  "set_thinking_level",
		"level": level,
	})
	if err != nil {
		return fmt.Errorf("pi set_thinking_level %q: %w", level, err)
	}
	return checkSuccessResponse(result, fmt.Sprintf("set_thinking_level %q", level))
}

func checkSuccessResponse(result json.RawMessage, command string) error {
	var response struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return fmt.Errorf("decode pi %s response: %w", command, err)
	}
	if !response.Success {
		if response.Error == "" {
			response.Error = "unsuccessful response"
		}
		return fmt.Errorf("pi %s rejected: %s", command, response.Error)
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
