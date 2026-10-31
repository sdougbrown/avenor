// Package poo drives a poo binary over its public stdio protocol.
//
// One process serves one avenor session: the provider starts the binary the
// roster entry resolved, performs the handshake, opens a session, routes its
// events and owns its teardown. Permission answers and resume are Stage 3.
package poo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/poo/wire/protocol"
)

const backendID = "poo"

// ErrNotImplemented is the boundary Stage 2 deliberately leaves in place.
var ErrNotImplemented = errors.New("the poo backend does not implement this yet")

// Provider is the poo backend: one process, one session.
type Provider struct {
	opts runtime.StartOptions
	// protocolVersions is what the handshake offers. A test narrows it to
	// prove the backend refuses a version it does not speak.
	protocolVersions []int

	mu       sync.Mutex
	client   *client
	sessions map[string]*sessionState
}

// sessionState is one opened session and the run it currently owns.
type sessionState struct {
	id string
	// sessionDir and mode are what session/create reported for the session's
	// own records: where poo keeps them and whether it keeps any at all.
	sessionDir string
	mode       string

	mu        sync.Mutex
	activeRun string
}

func (s *sessionState) activeRunID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeRun
}

func (s *sessionState) setActiveRun(runID string) {
	s.mu.Lock()
	s.activeRun = runID
	s.mu.Unlock()
}

// NewWithOptions builds the poo provider for one session.
func NewWithOptions(opts runtime.StartOptions) *Provider {
	return &Provider{
		opts:             opts,
		protocolVersions: []int{protocol.Version},
		sessions:         map[string]*sessionState{},
	}
}

var _ runtime.Provider = (*Provider)(nil)

// RecoveredDenialIsCompletion reports that poo hands a denied call back to the
// model as a tool result and the run continues, so an end_turn after one is a
// completion rather than a tool_denied failure.
func (p *Provider) RecoveredDenialIsCompletion() bool { return true }

func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := runtime.MergeStartOptions(p.opts, opts)
	if err := runtime.ValidateThinkingForBackend(backendID, merged.Thinking); err != nil {
		return runtime.Session{}, err
	}
	p.mu.Lock()
	open := len(p.sessions)
	p.mu.Unlock()
	if open > 0 {
		return runtime.Session{}, errors.New("poo runs one session per process")
	}
	workingDir, err := absoluteDir(merged.Dir)
	if err != nil {
		return runtime.Session{}, err
	}
	c, err := p.ensureClient(ctx, merged)
	if err != nil {
		return runtime.Session{}, err
	}
	if _, err := c.initialize(ctx, p.versions()); err != nil {
		return runtime.Session{}, err
	}

	params := protocol.CreateParams{
		WorkingDir: workingDir,
		Profile:    merged.Agent,
	}
	if merged.Model != "" {
		params.Models = &protocol.ModelRoles{Chat: merged.Model}
	}
	info, err := c.createSession(ctx, params)
	if err != nil {
		return runtime.Session{}, describeCreateFailure(merged, err)
	}

	p.mu.Lock()
	p.sessions[info.SessionID] = &sessionState{id: info.SessionID, sessionDir: info.SessionDir, mode: info.Mode}
	p.mu.Unlock()

	return runtime.Session{
		SessionID: info.SessionID,
		Backend:   backendID,
		Dir:       merged.Dir,
		PID:       c.pid(),
	}, nil
}

// describeCreateFailure names the field poo refused, because the server's own
// message names the value and not what the roster entry should change.
func describeCreateFailure(opts runtime.StartOptions, err error) error {
	switch refusedCode(err) {
	case protocol.DataUnknownProfile:
		return fmt.Errorf("poo profile %q is not offered by this binary: %w", opts.Agent, err)
	case protocol.DataUnknownModel:
		return fmt.Errorf("poo chat model %q is not named in this binary's settings: %w", opts.Model, err)
	default:
		return err
	}
}

func (p *Provider) Resume(context.Context, string) (runtime.Session, error) {
	return runtime.Session{}, ErrNotImplemented
}

func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	st, err := p.session(sessionID)
	if err != nil {
		return err
	}
	c, err := p.getClient()
	if err != nil {
		return err
	}

	// Hold the session's events so the acceptance record is published before
	// anything the backend emitted ahead of its answer.
	release := c.pauseEvents(sessionID)
	runID, err := p.acceptInput(ctx, c, st, prompt)
	if err != nil {
		release(nil)
		return err
	}
	submitted := events.Event{
		Event:     "agent.prompt_submitted",
		SessionID: sessionID,
		Fields:    map[string]any{"prompt_length": len(prompt)},
	}
	release(&submitted)

	return waitForSettle(ctx, c, sessionID, runID)
}

// acceptInput starts a run, or steers the one already active.
func (p *Provider) acceptInput(ctx context.Context, c *client, st *sessionState, text string) (string, error) {
	if runID := st.activeRunID(); runID != "" {
		return p.steer(ctx, c, st, runID, text, startRetries)
	}
	return p.startRun(ctx, c, st, text, startRetries)
}

// startRetries bounds the start/steer handoff. A run that settles while the
// client is choosing between steering it and starting its own costs one retry;
// a session that keeps refusing both is reported instead of retried forever.
const startRetries = 1

func (p *Provider) startRun(ctx context.Context, c *client, st *sessionState, text string, retries int) (string, error) {
	raw, rpcErr, err := c.call(ctx, protocol.MethodRunPrompt, protocol.PromptParams{
		SessionID: st.id,
		Text:      text,
	})
	if err != nil {
		return "", err
	}
	if rpcErr != nil {
		if isRefusal(rpcErr, protocol.DataBusy) {
			// A run the client has not observed yet owns the session, or a run
			// woke it: steer that run instead of starting another.
			return p.steerActive(ctx, c, st, text, retries)
		}
		return "", fmt.Errorf("run/prompt: %s", describeRefusal(rpcErr))
	}
	var result protocol.PromptResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("decode run/prompt result: %w", err)
	}
	if result.RunID == "" {
		return "", errors.New("run/prompt returned no run id")
	}
	st.setActiveRun(result.RunID)
	return result.RunID, nil
}

// steerActive steers the session's run without knowing its id yet.
func (p *Provider) steerActive(ctx context.Context, c *client, st *sessionState, text string, retries int) (string, error) {
	runID, err := p.currentRunID(ctx, c, st.id)
	if err != nil {
		return "", err
	}
	if runID == "" {
		if retries <= 0 {
			return "", errors.New("poo reported the session busy but named no active run")
		}
		// The run that refused the prompt settled before the session could name
		// it, so there is nothing to steer: start the turn this prompt wanted.
		return p.startRun(ctx, c, st, text, retries-1)
	}
	st.setActiveRun(runID)
	return p.steer(ctx, c, st, runID, text, retries)
}

func (p *Provider) steer(ctx context.Context, c *client, st *sessionState, runID, text string, retries int) (string, error) {
	_, rpcErr, err := c.call(ctx, protocol.MethodRunMessage, protocol.MessageParams{
		SessionID: st.id,
		Text:      text,
		Mode:      "steer",
	})
	if err != nil {
		return "", err
	}
	if rpcErr != nil {
		if isRefusal(rpcErr, protocol.DataNotRunning) {
			// The run settled between the decision and the send: start the
			// turn this prompt was meant to be.
			st.setActiveRun("")
			if retries <= 0 {
				return "", fmt.Errorf("run/message: %s", describeRefusal(rpcErr))
			}
			return p.startRun(ctx, c, st, text, retries-1)
		}
		return "", fmt.Errorf("run/message: %s", describeRefusal(rpcErr))
	}
	return runID, nil
}

// currentRunID asks the session which run is active. An empty id means none is.
func (p *Provider) currentRunID(ctx context.Context, c *client, sessionID string) (string, error) {
	raw, rpcErr, err := c.call(ctx, protocol.MethodSessionGet, protocol.GetParams{SessionID: sessionID})
	if err != nil {
		return "", err
	}
	if rpcErr != nil {
		return "", fmt.Errorf("session/get: %s", describeRefusal(rpcErr))
	}
	var result protocol.GetResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("decode session/get result: %w", err)
	}
	return result.RunID, nil
}

// waitForSettle blocks until the run settles, re-asking when one wait runs out.
func waitForSettle(ctx context.Context, c *client, sessionID, runID string) error {
	for {
		raw, rpcErr, err := c.call(ctx, protocol.MethodRunInspect, protocol.InspectParams{
			SessionID: sessionID,
			Wait: &protocol.InspectWait{
				Until:     "settled",
				RunID:     runID,
				TimeoutMS: inspectWaitMS,
			},
		})
		if err != nil {
			return err
		}
		if rpcErr != nil {
			if isRefusal(rpcErr, protocol.DataSessionClosed) {
				return fmt.Errorf("poo closed session %s before run %s settled", sessionID, runID)
			}
			return fmt.Errorf("run/inspect: %s", describeRefusal(rpcErr))
		}
		var result protocol.InspectResult
		if err := json.Unmarshal(raw, &result); err != nil {
			return fmt.Errorf("decode run/inspect result: %w", err)
		}
		if result.TimedOut {
			continue
		}
		return settleError(sessionID, classifyFromEvent(result.Settled))
	}
}

func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	st, err := p.session(sessionID)
	if err != nil {
		return err
	}
	c, err := p.getClient()
	if err != nil {
		return err
	}
	runID := st.activeRunID()
	if runID == "" {
		runID, err = p.currentRunID(ctx, c, sessionID)
		if err != nil {
			return err
		}
	}
	if runID == "" {
		// Nothing is running, so there is nothing to cancel.
		return nil
	}
	_, rpcErr, err := c.call(ctx, protocol.MethodRunCancel, protocol.CancelParams{
		SessionID: sessionID,
		RunID:     runID,
	})
	if err != nil {
		return err
	}
	if rpcErr != nil {
		if isRefusal(rpcErr, protocol.DataRunNotFound, protocol.DataNotRunning, protocol.DataSessionClosed) {
			// The cancel is idempotent: a run that already settled is done.
			return nil
		}
		return fmt.Errorf("run/cancel: %s", describeRefusal(rpcErr))
	}
	return nil
}

func (p *Provider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	c, err := p.getClient()
	if err != nil {
		return nil, err
	}
	if _, err := p.session(sessionID); err != nil {
		return nil, err
	}
	out := make(chan events.Event, 256)
	c.subscribe(sessionID, out)
	go func() {
		<-ctx.Done()
		c.unsubscribe(sessionID, out)
		close(out)
	}()
	return out, nil
}

func (p *Provider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return ErrNotImplemented
}

// Capabilities describes what this backend can do now. Permission answers and
// resume land in Stage 3.
func (p *Provider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{
		Backend:             backendID,
		Permissions:         false,
		Resume:              false,
		ExternalServerURL:   false,
		SubprocessDiscovery: false,
		ModelSelection:      true,
	}, nil
}

// Close asks every open session to close, then closes stdin and reaps the
// process: SIGTERM at the exit grace, SIGKILL five seconds later.
func (p *Provider) Close() error {
	p.mu.Lock()
	c := p.client
	p.client = nil
	sessions := make([]*sessionState, 0, len(p.sessions))
	for _, st := range p.sessions {
		sessions = append(sessions, st)
	}
	p.sessions = map[string]*sessionState{}
	p.mu.Unlock()
	if c == nil {
		return nil
	}
	var firstErr error
	for _, st := range sessions {
		if err := c.closeSession(st.id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := c.shutdown(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (p *Provider) versions() []int {
	if len(p.protocolVersions) == 0 {
		return []int{protocol.Version}
	}
	return p.protocolVersions
}

func (p *Provider) ensureClient(ctx context.Context, opts runtime.StartOptions) (*client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client, nil
	}
	c, err := startClient(ctx, opts, p.observeEvent)
	if err != nil {
		return nil, err
	}
	p.client = c
	return c, nil
}

// observeEvent tracks the session's active run from the event stream, so a
// prompt that arrives while one is running steers it instead of starting a
// second.
func (p *Provider) observeEvent(ev sessionEvent) {
	p.mu.Lock()
	st := p.sessions[ev.SessionID]
	p.mu.Unlock()
	if st == nil {
		return
	}
	switch ev.Type {
	case "run.started":
		if runID, _ := ev.Fields["run_id"].(string); runID != "" {
			st.setActiveRun(runID)
		}
	case "run.settled":
		st.setActiveRun("")
	}
}

func (p *Provider) session(sessionID string) (*sessionState, error) {
	p.mu.Lock()
	st := p.sessions[sessionID]
	p.mu.Unlock()
	if st == nil {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	return st, nil
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

// absoluteDir resolves the directory the session's tools work in. poo requires
// it, and it must be absolute so the session does not depend on the child's
// working directory.
func absoluteDir(dir string) (string, error) {
	if dir == "" {
		dir = "."
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("resolve working directory %q: %w", dir, err)
	}
	return abs, nil
}
