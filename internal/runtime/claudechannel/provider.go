// Package claudechannel implements a runtime.Provider for Claude Code via channels + tmux.
package claudechannel

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
	"github.com/sdougbrown/avenor/internal/runtime/claudeutil"
)

const backendID = "claude-channel"

const (
	startupPromptCheckInterval = 500 * time.Millisecond
	startupPromptTimeout       = 30 * time.Second
	promptSubmitRetryDelay     = 750 * time.Millisecond
	paneScanInterval           = 500 * time.Millisecond
	transcriptScanInterval     = 2 * time.Second
)

// Provider implements runtime.Provider for an interactive Claude Code session
// controlled via claude/channel push events with tmux lifecycle management.
type Provider struct {
	opts runtime.StartOptions

	mu        sync.Mutex
	sessions  map[string]*session
	broker    *broker.Broker
	globalTok string
	launcher  terminal.Launcher
}

type session struct {
	*claudecore.Session

	// claudechannel-only fields:
	mcpDir              string
	brokerURL           string
	sidecarTok          string
	mcpConfig           string
	mcpServer           string
	mcpProject          string
	channelReadyEmitted bool
}

// Ensure Provider implements runtime.Provider.
var _ runtime.Provider = (*Provider)(nil)

func NewWithOptions(opts runtime.StartOptions) runtime.Provider {
	return &Provider{
		opts:      opts,
		sessions:  make(map[string]*session),
		globalTok: broker.MakeToken(),
		launcher:  claudecore.DefaultLauncher(),
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
	if merged.Dir == "" {
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

	// Check Claude Code version.
	out, err := exec.Command("claude", "--version").Output()
	if err != nil {
		return runtime.Session{}, fmt.Errorf("claude --version failed: %w", err)
	}
	vStr := strings.TrimSpace(string(out))
	if !strings.Contains(vStr, "Claude Code") {
		return runtime.Session{}, fmt.Errorf("unexpected claude version output: %s", vStr)
	}
	if err := claudeutil.CheckEffortCapability(ctx, backendID, merged.Thinking); err != nil {
		return runtime.Session{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if merged.Broker != nil {
		p.broker = merged.Broker
	} else if p.broker == nil {
		p.broker = broker.New(p.globalTok)
		if err := p.broker.Start(); err != nil {
			return runtime.Session{}, fmt.Errorf("broker.Start: %w", err)
		}
	}

	// Generate IDs.
	sessionID := uuid.New().String()
	runID := uuid.New().String()
	sidecarTok, err := p.broker.CreateRun(runID)
	if err != nil {
		return runtime.Session{}, fmt.Errorf("broker create run: %w", err)
	}
	brokerURL := fmt.Sprintf("http://%s", p.broker.Addr())
	runSlug := shortRunID(runID)
	serverName := "avenor-channel-" + runSlug

	// Create ephemeral bootstrap state for this run.
	mcpDir, err := os.MkdirTemp("", "avenor-claude-channel-*")
	if err != nil {
		return runtime.Session{}, fmt.Errorf("mktemp: %w", err)
	}
	mcpConfigPath := filepath.Join(mcpDir, "mcp.json")
	avenorBin, err := os.Executable()
	if err != nil {
		_ = os.RemoveAll(mcpDir)
		return runtime.Session{}, fmt.Errorf("exe path: %w", err)
	}
	mcpConfig := map[string]any{
		"mcpServers": map[string]any{
			serverName: map[string]any{
				"command": avenorBin,
				"args": []string{
					"claude-channel",
					"--run-id", runID,
					"--token", sidecarTok,
					"--broker-url", brokerURL,
				},
			},
		},
	}
	configJSON, err := json.MarshalIndent(mcpConfig, "", "  ")
	if err != nil {
		_ = os.RemoveAll(mcpDir)
		return runtime.Session{}, fmt.Errorf("marshal mcp config: %w", err)
	}
	if err := os.WriteFile(mcpConfigPath, configJSON, 0600); err != nil {
		_ = os.RemoveAll(mcpDir)
		return runtime.Session{}, fmt.Errorf("write mcp config: %w", err)
	}
	projectMCPPath := filepath.Join(merged.Dir, ".mcp.json")
	if err := upsertProjectMCPServer(projectMCPPath, serverName, mcpConfig["mcpServers"].(map[string]any)[serverName]); err != nil {
		_ = os.RemoveAll(mcpDir)
		return runtime.Session{}, fmt.Errorf("upsert project mcp config: %w", err)
	}

	// Build claude args.
	claudeArgs := claudeutil.BuildArgs(sessionID, serverName, merged)

	// Build the shell command for the tmux session. Using `exec` replaces the
	// shell with claude so that #{pane_pid} reports claude's actual PID and the
	// tmux session exits when claude exits.
	parts := make([]string, 0, len(claudeArgs)+2)
	parts = append(parts, "exec", "claude")
	for _, arg := range claudeArgs {
		parts = append(parts, claudecore.ShellQuote(arg))
	}
	shellCmd := strings.Join(parts, " ")

	// Launch claude in a detached tmux session. tmux provides a real virtual
	// terminal, which is what prevents claude from falling back to --print mode.
	tmuxName := "avenor-" + runSlug

	var transcript *claudecore.TranscriptReader
	if home, err := os.UserHomeDir(); err == nil {
		transcript = claudecore.NewTranscriptReader(claudecore.TranscriptPath(home, merged.Dir, sessionID))
	}

	core := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID:  sessionID,
		RunID:      runID,
		Dir:        merged.Dir,
		TmuxName:   tmuxName,
		Transcript: transcript,
		EventsBuf:  64,
	})

	s := &session{
		Session:    core,
		mcpDir:     mcpDir,
		brokerURL:  brokerURL,
		sidecarTok: sidecarTok,
		mcpConfig:  mcpConfigPath,
		mcpServer:  serverName,
		mcpProject: projectMCPPath,
	}

	term, err := p.launcher.Start(s.Ctx, terminal.StartOptions{
		Name:    tmuxName,
		Dir:     merged.Dir,
		Cols:    220,
		Rows:    50,
		Command: shellCmd,
	})
	if err != nil {
		_ = removeProjectMCPServer(projectMCPPath, serverName)
		_ = os.RemoveAll(mcpDir)
		return runtime.Session{}, fmt.Errorf("terminal launch: %w", err)
	}
	s.Term = term

	p.sessions[sessionID] = s

	go autoConfirmDevelopmentChannelPrompt(s.Ctx, s)

	go p.runSession(s.Ctx, s)

	go func() {
		s.Emit(events.Event{
			Event:     "session.start",
			SessionID: sessionID,
			Fields: map[string]any{
				"backend":          backendID,
				"dir":              merged.Dir,
				"broker_url":       brokerURL,
				"dangerously_load": true,
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

// runSession supervises the Claude tmux session and forwards broker events into
// the event stream. It cleans up the tmux session and tmpdir on exit.
func (p *Provider) runSession(ctx context.Context, s *session) {
	defer s.CancelFn()
	defer close(s.Done)
	// Deliberately do NOT close(s.Events): it has multiple concurrent senders
	// (the async Prompt goroutine, the session.start emitter, broker-poll
	// events), and closing a channel while other goroutines may still send to it
	// panics with "send on closed channel". Shutdown is signalled via
	// s.CancelFn()/s.Ctx instead. Sends from other goroutines go through s.Emit,
	// which selects on s.Ctx.Done(). Raw sends in pollBrokerEvents and the
	// sessionGone branch below stay raw because they run in this goroutine and
	// cannot race teardown. The Events() reader drains and closes its
	// downstream channel on s.Ctx.Done().
	defer func() {
		p.mu.Lock()
		delete(p.sessions, s.SessionID)
		p.mu.Unlock()
	}()
	defer func() {
		p.broker.DeleteRun(s.RunID)
	}()
	defer func() {
		_ = s.Term.Kill(ctx)
		_ = removeProjectMCPServer(s.mcpProject, s.mcpServer)
		_ = os.RemoveAll(s.mcpDir)
	}()

	// Watch for the tmux session to disappear (claude exited).
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

	// Poll broker for sidecar events.
	pollTick := time.NewTicker(500 * time.Millisecond)
	defer pollTick.Stop()
	paneTick := time.NewTicker(paneScanInterval)
	defer paneTick.Stop()
	transcriptTick := time.NewTicker(transcriptScanInterval)
	defer transcriptTick.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = s.Term.Kill(ctx)
			return
		case <-sessionGone:
			if s.MarkFinished() {
				// Claude exited without calling avenor_finish. This runs in the
				// runSession goroutine, so it cannot race teardown. Using s.Emit
				// for consistency with the claude backend and for the s.Ctx.Done()
				// escape hatch if the buffer is full.
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
		case <-pollTick.C:
			p.pollBrokerEvents(s)
		case <-paneTick.C:
			s.ScanTerminalTick()
		case <-transcriptTick.C:
			s.ScanTranscriptTick()
		}
	}
}

func (p *Provider) pollBrokerEvents(s *session) {
	st := p.broker.GetRun(s.RunID)
	if st == nil {
		return
	}
	st.Lock()
	registeredAt := st.RegisteredAt
	reports := make([]broker.Report, len(st.Reports))
	copy(reports, st.Reports)
	st.Reports = st.Reports[:0]

	finishes := make([]broker.Finish, len(st.Finishes))
	copy(finishes, st.Finishes)
	st.Finishes = st.Finishes[:0]

	replies := make([]broker.Reply, len(st.Replies))
	copy(replies, st.Replies)
	st.Replies = st.Replies[:0]

	st.Unlock()

	// All sends below run in the runSession goroutine (via its pollTick branch),
	// the same goroutine whose deferred cleanup used to close s.Events. They
	// therefore cannot race teardown and remain raw sends.
	if markChannelReadyEmitted(s, registeredAt) {
		s.Events <- events.Event{
			Event:     "agent.channel_ready",
			SessionID: s.SessionID,
			Fields: map[string]any{
				"run_id":      s.RunID,
				"server_name": s.mcpServer,
				"source":      "channel",
			},
		}
	}

	for _, rep := range reports {
		s.Events <- events.Event{
			Event:     "agent.report",
			SessionID: s.SessionID,
			Fields: map[string]any{
				"state":   rep.State,
				"payload": json.RawMessage(rep.Payload),
				"source":  "channel",
			},
		}
		s.Events <- brokerEvent(s.SessionID, rep.State, rep.Payload)
	}
	for _, fin := range finishes {
		s.Events <- events.Event{
			Event:     "agent.finish",
			SessionID: s.SessionID,
			Fields: map[string]any{
				"status":        fin.Status,
				"summary":       fin.Summary,
				"files_changed": fin.FilesChanged,
				"payload":       json.RawMessage(fin.Payload),
				"source":        "channel",
			},
		}
		if !s.MarkFinished() {
			continue
		}
		s.Events <- events.Event{
			Event:     "session.end",
			SessionID: s.SessionID,
			Fields: map[string]any{
				"status":        fin.Status,
				"summary":       fin.Summary,
				"files_changed": fin.FilesChanged,
				"stop_reason":   mapFinishStatus(fin.Status),
			},
		}
	}
	for _, rep := range replies {
		s.Events <- events.Event{
			Event:     "agent.reply",
			SessionID: s.SessionID,
			Fields: map[string]any{
				"to":      rep.To,
				"payload": json.RawMessage(rep.Payload),
				"source":  "channel",
			},
		}
	}
}

func markChannelReadyEmitted(s *session, registeredAt time.Time) bool {
	// Zero RegisteredAt means the Claude sidecar has not registered with the
	// broker yet, so the channel is not ready from the orchestrator's point of
	// view even though the run state already exists.
	if registeredAt.IsZero() {
		return false
	}
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.channelReadyEmitted {
		return false
	}
	s.channelReadyEmitted = true
	return true
}

// Resume reattaches to an in-process session whose terminal is still alive.
// Only the in-process case is supported in v1: if avenor restarts, broker run
// state and the sidecar's MCP token are gone even if the tmux session is not,
// and cross-restart recovery is out of scope. Resume is rejected for launchers
// that don't survive the parent process (e.g. PTY).
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

	// Inject the prompt into Claude's terminal via tmux PTY. This is the
	// primary delivery mechanism: the channel method only works after Claude
	// has started its first turn, and the initial prompt must arrive as
	// terminal input to kick off the session.
	//
	// Wait for Claude's development-channel prompt to clear. Use a goroutine so
	// Prompt() doesn't block; the channel push serves as a backup if Claude is
	// already listening.
	go func() {
		if !claudecore.WaitForPaneReady(s.Ctx, s.Term) {
			return
		}

		if err := pastePromptAndSubmit(s.Ctx, s.Term, prompt); err != nil {
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

	// Also push via channel for idempotence and later turns.
	msg := broker.ControlMessage{
		ID:    uuid.New().String(),
		Type:  "continue",
		RunID: s.RunID,
		Payload: claudecore.MustJSON(map[string]any{
			"message": prompt,
		}),
	}
	if err := p.broker.PushControl(s.RunID, msg); err != nil {
		return err
	}
	s.Emit(events.Event{
		Event:     "agent.prompt_queued",
		SessionID: s.SessionID,
		Fields: map[string]any{
			"control_id":    msg.ID,
			"message_type":  msg.Type,
			"delivery":      "channel",
			"prompt_length": len(prompt),
		},
	})
	return nil
}

func pastePromptAndSubmit(ctx context.Context, term terminal.Session, prompt string) error {
	return term.PasteAndEnter(ctx, prompt)
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

func autoConfirmDevelopmentChannelPrompt(ctx context.Context, s *session) {
	deadline := time.NewTimer(startupPromptTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(startupPromptCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.Done:
			return
		case <-deadline.C:
			return
		case <-ticker.C:
		}

		out, err := s.Term.Capture(ctx)
		if err != nil {
			continue
		}
		if strings.Contains(out, "New MCP server found in this project") {
			_ = s.Term.SendKeys(ctx, terminal.Key("1"), terminal.KeyEnter)
			continue
		}
		if strings.Contains(out, "Loading development channels") {
			_ = s.Term.SendKeys(ctx, terminal.KeyEnter)
			continue
		}
	}
}

func shortRunID(runID string) string {
	if len(runID) >= 8 {
		return runID[:8]
	}
	if runID == "" {
		return "00000000"
	}
	return runID + strings.Repeat("0", 8-len(runID))
}

func brokerEvent(sessionID, state string, payload json.RawMessage) events.Event {
	var ev string
	var fields map[string]any
	switch state {
	case "thinking":
		ev = "agent.status"
		fields = map[string]any{"phase": "thinking"}
	case "working":
		ev = "agent.status"
		fields = map[string]any{"phase": "working"}
	case "checkpoint":
		ev = "agent.message_chunk"
		fields = map[string]any{"payload": json.RawMessage(payload)}
	case "blocked":
		ev = "agent.status"
		fields = map[string]any{"phase": "waiting"}
	case "permission_requested":
		ev = "permission.request"
		fields = map[string]any{"payload": json.RawMessage(payload)}
	default:
		ev = "agent.status"
		fields = map[string]any{"phase": state}
	}
	if payload != nil {
		var m map[string]any
		if err := json.Unmarshal(payload, &m); err == nil {
			for k, v := range m {
				fields[k] = v
			}
		}
	}
	return events.Event{Event: ev, SessionID: sessionID, Fields: fields}
}

func mapFinishStatus(status string) string {
	switch status {
	case "done":
		return "end_turn"
	case "failed":
		return "error"
	case "blocked":
		return "cancelled"
	default:
		return "end_turn"
	}
}

func (p *Provider) Cancel(ctx context.Context, sessionID string) error {
	p.mu.Lock()
	s, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}

	// Push graceful cancel via channel.
	msg := broker.ControlMessage{
		ID:      uuid.New().String(),
		Type:    "cancel",
		RunID:   s.RunID,
		Payload: claudecore.MustJSON(map[string]any{"reason": "user cancel"}),
	}
	_ = p.broker.PushControl(s.RunID, msg)

	// Escalate to hard kill after 2 seconds.
	go func() {
		time.Sleep(2 * time.Second)
		select {
		case <-s.Ctx.Done():
			return
		default:
		}
		_ = s.Term.Kill(s.Ctx)
	}()
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

	// Check if this is a terminal permission request.
	s.Mu.Lock()
	pendingPerm := s.PendingTerminalPerm
	s.Mu.Unlock()

	if pendingPerm != nil && pendingPerm.RequestID == requestID {
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

	// Not a terminal permission; route through broker (sidecar path).
	if s.Finished {
		return fmt.Errorf("session already finished: %s", sessionID)
	}

	state := "deny"
	if resp.Allow {
		state = "allow"
	}
	msg := broker.ControlMessage{
		ID:    uuid.New().String(),
		Type:  "permission_decision",
		RunID: s.RunID,
		Payload: claudecore.MustJSON(map[string]any{
			"request_id": requestID,
			"behavior":   state,
			"option_id":  resp.OptionID,
			"message":    resp.Message,
		}),
	}
	return p.broker.PushControl(s.RunID, msg)
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
