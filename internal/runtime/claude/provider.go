// Package claude implements a channel-less runtime.Provider for Claude Code.
package claude

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
	"github.com/sdougbrown/avenor/internal/runtime/claudeutil"
)

const backendID = "claude"

const (
	paneScanInterval       = 500 * time.Millisecond
	transcriptScanInterval = 2 * time.Second
	promptSubmitRetryDelay = 750 * time.Millisecond
	defaultCancelGrace     = 3 * time.Second
)

type Provider struct {
	opts        runtime.StartOptions
	mu          sync.Mutex
	sessions    map[string]*claudecore.Session
	launcher    terminal.Launcher
	cancelGrace time.Duration
}

var _ runtime.Provider = (*Provider)(nil)

// defaultLauncher defaults to PTY; override with AVENOR_CLAUDE_TERMINAL=tmux.
// (claudecore.DefaultLauncher defaults to tmux for the channel backend.)
func defaultLauncher() terminal.Launcher {
	switch os.Getenv("AVENOR_CLAUDE_TERMINAL") {
	case "tmux":
		return terminal.TmuxLauncher{}
	default:
		return terminal.PTYLauncher{}
	}
}

func NewWithOptions(opts runtime.StartOptions) runtime.Provider {
	return &Provider{
		opts:        opts,
		sessions:    make(map[string]*claudecore.Session),
		launcher:    defaultLauncher(),
		cancelGrace: defaultCancelGrace,
	}
}

func New() runtime.Provider {
	return NewWithOptions(runtime.StartOptions{})
}

func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := runtime.MergeStartOptions(p.opts, opts)
	if err := runtime.ValidateThinkingForBackend(backendID, merged.Thinking); err != nil {
		return runtime.Session{}, err
	}
	if merged.Dir == "" || merged.Dir == "." {
		var err error
		merged.Dir, err = os.Getwd()
		if err != nil {
			return runtime.Session{}, fmt.Errorf("getcwd: %w", err)
		}
	}

	if _, err := exec.LookPath("claude"); err != nil {
		return runtime.Session{}, fmt.Errorf("claude binary not found in PATH: %w", err)
	}

	if _, ok := p.launcher.(terminal.TmuxLauncher); ok {
		if _, err := exec.LookPath("tmux"); err != nil {
			return runtime.Session{}, fmt.Errorf("tmux not found in PATH: %w", err)
		}
	}

	out, err := exec.Command("claude", "--version").Output()
	if err != nil {
		return runtime.Session{}, fmt.Errorf("claude --version failed: %w", err)
	}
	if !strings.Contains(strings.TrimSpace(string(out)), "Claude Code") {
		return runtime.Session{}, fmt.Errorf("unexpected claude version output: %s", strings.TrimSpace(string(out)))
	}
	if err := claudeutil.CheckEffortCapability(ctx, backendID, merged.Thinking); err != nil {
		return runtime.Session{}, err
	}

	sessionID := uuid.New().String()

	claudeArgs := claudeutil.BuildArgs(sessionID, "", merged)

	parts := make([]string, 0, len(claudeArgs)+2)
	parts = append(parts, "exec", "claude")
	for _, arg := range claudeArgs {
		parts = append(parts, claudecore.ShellQuote(arg))
	}
	shellCmd := strings.Join(parts, " ")

	var transcript *claudecore.TranscriptReader
	if home, err := os.UserHomeDir(); err == nil {
		transcript = claudecore.NewTranscriptReader(claudecore.TranscriptPath(home, merged.Dir, sessionID))
	}

	s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID:  sessionID,
		Dir:        merged.Dir,
		Transcript: transcript,
		EventsBuf:  64,
	})

	term, err := p.launcher.Start(s.Ctx, terminal.StartOptions{
		Name:    "avenor-" + sessionID[:8],
		Dir:     merged.Dir,
		Cols:    220,
		Rows:    50,
		Command: shellCmd,
	})
	if err != nil {
		s.CancelFn()
		return runtime.Session{}, fmt.Errorf("terminal launch: %w", err)
	}
	s.Term = term

	p.mu.Lock()
	p.sessions[sessionID] = s
	p.mu.Unlock()

	go p.runSession(s.Ctx, s)

	go func() {
		s.Emit(events.Event{
			Event:     "session.start",
			SessionID: sessionID,
			Fields: map[string]any{
				"backend":          backendID,
				"dir":              merged.Dir,
				"dangerously_load": false,
			},
		})
	}()

	return runtime.Session{
		SessionID: sessionID,
		Backend:   backendID,
		Dir:       merged.Dir,
		PID:       term.PID(),
	}, nil
}

func (p *Provider) runSession(ctx context.Context, s *claudecore.Session) {
	defer s.CancelFn()
	defer close(s.Done)
	// Deliberately do NOT close(s.Events): it has multiple concurrent senders
	// (the async Prompt goroutine, the session.start emitter, Cancel), and
	// closing a channel while other goroutines may still send to it panics with
	// "send on closed channel". Shutdown is signalled via s.CancelFn()/s.Ctx
	// instead; every send goes through s.Emit, which selects on s.Ctx.Done().
	// The Events() reader drains and closes its downstream channel on s.Ctx.Done.
	defer func() {
		p.mu.Lock()
		delete(p.sessions, s.SessionID)
		p.mu.Unlock()
	}()
	defer func() {
		_ = s.Term.Kill(context.Background())
	}()

	sessionGone := make(chan struct{})
	go func() {
		defer close(sessionGone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			if !s.Term.Alive(ctx) {
				return
			}
		}
	}()

	paneTick := time.NewTicker(paneScanInterval)
	defer paneTick.Stop()
	transcriptTick := time.NewTicker(transcriptScanInterval)
	defer transcriptTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sessionGone:
			if s.MarkFinished() {
				s.Emit(events.Event{
					Event:     "session.end",
					SessionID: s.SessionID,
					Fields: map[string]any{
						"status":      "done",
						"stop_reason": "end_turn",
					},
				})
			}
			return
		case <-paneTick.C:
			s.ScanTerminalTick()
		case <-transcriptTick.C:
			s.ScanTranscriptTick()
		}
	}
}

func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	p.mu.Lock()
	s, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return runtime.Session{}, fmt.Errorf("session not found: %s (cross-restart resume not yet supported)", sessionID)
	}
	if !s.Term.Alive(ctx) {
		return runtime.Session{}, fmt.Errorf("session %s terminal exited", sessionID)
	}
	return runtime.Session{
		SessionID: sessionID,
		Backend:   backendID,
		Dir:       s.Dir,
		PID:       s.Term.PID(),
	}, nil
}

func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	p.mu.Lock()
	s, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}
	s.Mu.Lock()
	finished := s.Finished
	s.Mu.Unlock()
	if finished {
		return fmt.Errorf("session already finished: %s", sessionID)
	}

	go func() {
		if !claudecore.WaitForPaneReady(s.Ctx, s.Term) {
			s.Emit(events.Event{
				Event:     "agent.prompt_failed",
				SessionID: s.SessionID,
				Fields: map[string]any{
					"error": "terminal not ready",
				},
			})
			return
		}
		if err := s.Term.PasteAndEnter(s.Ctx, prompt); err != nil {
			s.Emit(events.Event{
				Event:     "agent.prompt_failed",
				SessionID: s.SessionID,
				Fields: map[string]any{
					"error": err.Error(),
				},
			})
			return
		}
		s.Mu.Lock()
		s.Prompted = true
		s.Mu.Unlock()
		s.Emit(events.Event{
			Event:     "agent.prompt_submitted",
			SessionID: s.SessionID,
			Fields: map[string]any{
				"delivery":      s.Term.Kind(),
				"prompt_length": len(prompt),
			},
		})
		go retryPromptSubmitIfIdle(s.Ctx, s.Term, prompt)
	}()

	return nil
}

func retryPromptSubmitIfIdle(ctx context.Context, term terminal.Session, prompt string) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(promptSubmitRetryDelay):
	}

	out, err := term.Capture(ctx)
	if err != nil {
		return
	}
	text := string(out)
	if claudecore.ClassifyPane(text) != claudecore.PaneStateIdle {
		return
	}
	needle := prompt
	if len(needle) > 96 {
		needle = needle[:96]
	}
	if !strings.Contains(text, needle) {
		return
	}
	_ = term.SendKeys(ctx, terminal.KeyEnter)
}

func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	p.mu.Lock()
	s, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}

	_ = s.SendCancelKeys(ctx)

	cancelDeadline := time.NewTimer(p.cancelGrace)
	defer cancelDeadline.Stop()
	pollTick := time.NewTicker(150 * time.Millisecond)
	defer pollTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return p.forceCancel(s)
		case <-s.Ctx.Done():
			// runSession already exited (terminal died, sessionGone, etc.).
			// MarkFinished will lose the race; forceCancel returns nil.
			return p.forceCancel(s)
		case <-cancelDeadline.C:
			return p.forceCancel(s)
		case <-pollTick.C:
			state, err := claudecore.ScanTerminal(ctx, s.Term)
			if err != nil {
				continue
			}
			if state == claudecore.PaneStateIdle {
				if !s.MarkFinished() {
					return nil
				}
				s.Emit(events.Event{
					Event:     "session.end",
					SessionID: s.SessionID,
					Fields: map[string]any{
						"status":      "done",
						"stop_reason": "cancelled",
					},
				})
				// runSession's defer will call Term.Kill
				return nil
			}
		}
	}
}

func (p *Provider) forceCancel(s *claudecore.Session) error {
	if !s.MarkFinished() {
		return nil
	}
	s.Emit(events.Event{
		Event:     "session.end",
		SessionID: s.SessionID,
		Fields: map[string]any{
			"status":      "done",
			"stop_reason": "cancelled_forced",
		},
	})
	// runSession's defer will call Term.Kill
	return nil
}

func (p *Provider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	s, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("session not found: %s", sessionID)
	}

	out := make(chan events.Event, cap(s.Events))
	go func() {
		defer close(out)
		for {
			select {
			case e, ok := <-s.Events:
				if !ok {
					return
				}
				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
			case <-s.Ctx.Done():
				// Session ended. runSession never closes s.Events (closing a
				// channel with multiple senders is unsafe), so drain whatever is
				// buffered — notably the terminal session.end — before closing
				// out, so the consumer sees the stop reason. Then stop.
				for {
					select {
					case e := <-s.Events:
						select {
						case out <- e:
						case <-ctx.Done():
							return
						}
					default:
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (p *Provider) AnswerPermission(ctx context.Context, sessionID string, requestID string, resp runtime.PermissionResponse) error {
	p.mu.Lock()
	s, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}

	s.Mu.Lock()
	pendingPerm := s.PendingTerminalPerm
	s.Mu.Unlock()

	if pendingPerm == nil || pendingPerm.RequestID != requestID {
		return fmt.Errorf("no pending permission request %s", requestID)
	}

	s.Mu.Lock()
	s.PendingTerminalPerm = nil
	s.Mu.Unlock()

	key := "Esc"
	if resp.Allow {
		key = resp.OptionID
		if key == "" {
			key = "1"
		}
	}
	if !claudecore.ValidTmuxKey(key) {
		return fmt.Errorf("invalid option_id for terminal permission: %q", key)
	}
	keys := []terminal.Key{terminal.Key(key)}
	if key == "Esc" {
		keys = []terminal.Key{terminal.KeyEsc}
	}
	keys = append(keys, terminal.KeyEnter)
	return s.Term.SendKeys(ctx, keys...)
}

func (p *Provider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{
		Backend:             backendID,
		Permissions:         true,
		Resume:              true,
		ExternalServerURL:   false,
		SubprocessDiscovery: true,
		ModelSelection:      true,
	}, nil
}
