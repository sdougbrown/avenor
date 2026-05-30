// Package claudechannel implements a runtime.Provider for Claude Code via channels + tmux.
package claudechannel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/claudechannel/broker"
)

const backendID = "claude-channel"

// Provider implements runtime.Provider for an interactive Claude Code session
// controlled via claude/channel push events with tmux lifecycle management.
type Provider struct {
	opts runtime.StartOptions

	mu        sync.Mutex
	sessions  map[string]*session
	broker    *broker.Broker
	globalTok string
}

type session struct {
	sessionID  string
	runID      string
	dir        string
	tmuxName   string // tmux session name; used for lifecycle ops
	mcpDir     string // tmpdir holding MCP config; removed on session exit
	brokerURL  string
	sidecarTok string
	mcpConfig  string

	events   chan events.Event
	done     chan struct{}
	cancelFn context.CancelFunc

	startedAt time.Time
	finished  bool
	mu        sync.Mutex
}

// Ensure Provider implements runtime.Provider.
var _ runtime.Provider = (*Provider)(nil)

func NewWithOptions(opts runtime.StartOptions) runtime.Provider {
	return &Provider{
		opts:      opts,
		sessions:  make(map[string]*session),
		globalTok: broker.MakeToken(),
	}
}

func New() runtime.Provider {
	return NewWithOptions(runtime.StartOptions{})
}

func mergeStartOptions(base, override runtime.StartOptions) runtime.StartOptions {
	merged := base
	if override.Agent != "" {
		merged.Agent = override.Agent
	}
	if override.Label != "" {
		merged.Label = override.Label
	}
	if override.Dir != "" {
		merged.Dir = override.Dir
	}
	if override.ServerURL != "" {
		merged.ServerURL = override.ServerURL
	}
	if override.Model != "" {
		merged.Model = override.Model
	}
	return merged
}

func (p *Provider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	merged := mergeStartOptions(p.opts, opts)
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

	if _, err := exec.LookPath("tmux"); err != nil {
		return runtime.Session{}, fmt.Errorf("tmux not found in PATH: %w", err)
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

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.broker == nil {
		p.broker = broker.New(p.globalTok)
		if err := p.broker.Start(); err != nil {
			return runtime.Session{}, fmt.Errorf("broker start: %w", err)
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

	// Create temporary MCP config for this run.
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
			"avenor": map[string]any{
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

	// Build claude args.
	claudeArgs := []string{
		"--strict-mcp-config",
		"--mcp-config", mcpConfigPath,
		"--dangerously-load-development-channels", "server:avenor",
		"--session-id", sessionID,
	}
	if merged.Agent != "" {
		claudeArgs = append(claudeArgs, "--agent", merged.Agent)
	}
	if merged.Label != "" {
		claudeArgs = append(claudeArgs, "--name", merged.Label)
	}
	if merged.Model != "" {
		claudeArgs = append(claudeArgs, "--model", merged.Model)
	}
	claudeArgs = append(claudeArgs, "--permission-mode", "default")

	// Build the shell command for the tmux session. Using `exec` replaces the
	// shell with claude so that #{pane_pid} reports claude's actual PID and the
	// tmux session exits when claude exits.
	parts := make([]string, 0, len(claudeArgs)+2)
	parts = append(parts, "exec", "claude")
	for _, arg := range claudeArgs {
		parts = append(parts, shellQuote(arg))
	}
	shellCmd := strings.Join(parts, " ")

	// Launch claude in a detached tmux session. tmux provides a real virtual
	// terminal, which is what prevents claude from falling back to --print mode.
	tmuxName := "avenor-" + runID[:8]
	if tmuxOut, err := exec.Command(
		"tmux", "new-session",
		"-d",
		"-s", tmuxName,
		"-c", merged.Dir,
		"-x", "220",
		"-y", "50",
		shellCmd,
	).CombinedOutput(); err != nil {
		_ = os.RemoveAll(mcpDir)
		return runtime.Session{}, fmt.Errorf("tmux new-session: %w: %s", err, strings.TrimSpace(string(tmuxOut)))
	}

	// Wait for tmux pane to become ready by polling list-panes.
	pid := 0
	deadline := time.After(1 * time.Second)
	pollLoop:
	for {
		if pidOut, err := exec.Command("tmux", "list-panes", "-t", tmuxName, "-F", "#{pane_pid}").Output(); err == nil {
			if p := strings.TrimSpace(string(pidOut)); p != "" {
				pid, _ = strconv.Atoi(p)
			}
			break
		}
		select {
		case <-deadline:
			break pollLoop
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Auto-confirm the "Loading development channels" security prompt that
	// Claude Code shows when --dangerously-load-development-channels is used.
	// Without this, the detached tmux session blocks forever waiting for Enter.
	// Claude takes ~15-20s to start and render the prompt; we fire a delayed
	// send-keys in a goroutine so Start() returns without blocking.
	go func() {
		time.Sleep(20 * time.Second)
		_ = exec.Command("tmux", "send-keys", "-t", tmuxName, "Enter").Run()
	}()

	sessCtx, cancel := context.WithCancel(context.Background())
	s := &session{
		sessionID:  sessionID,
		runID:      runID,
		dir:        merged.Dir,
		tmuxName:   tmuxName,
		mcpDir:     mcpDir,
		brokerURL:  brokerURL,
		sidecarTok: sidecarTok,
		mcpConfig:  mcpConfigPath,
		events:     make(chan events.Event, 64),
		done:       make(chan struct{}),
		cancelFn:   cancel,
		startedAt:  time.Now(),
	}

	p.sessions[sessionID] = s

	go p.runSession(sessCtx, s)

	go func() {
		s.events <- events.Event{
			Event:     "session.start",
			SessionID: sessionID,
			Fields: map[string]any{
				"backend":          backendID,
				"dir":              merged.Dir,
				"broker_url":       brokerURL,
				"dangerously_load": true,
			},
		}
	}()

	return runtime.Session{
		SessionID: sessionID,
		Backend:   backendID,
		Dir:       merged.Dir,
		PID:       pid,
	}, nil
}

// runSession supervises the Claude tmux session and forwards broker events into
// the event stream. It cleans up the tmux session and tmpdir on exit.
func (p *Provider) runSession(ctx context.Context, s *session) {
	defer close(s.done)
	defer close(s.events)
	defer func() {
		_ = exec.Command("tmux", "kill-session", "-t", s.tmuxName).Run()
		_ = os.RemoveAll(s.mcpDir)
	}()

	// Watch for the tmux session to disappear (claude exited).
	sessionGone := make(chan struct{})
	go func() {
		defer close(sessionGone)
		for {
			time.Sleep(500 * time.Millisecond)
			if err := exec.Command("tmux", "has-session", "-t", s.tmuxName).Run(); err != nil {
				return
			}
		}
	}()

	// Poll broker for sidecar events.
	pollTick := time.NewTicker(500 * time.Millisecond)
	defer pollTick.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = exec.Command("tmux", "kill-session", "-t", s.tmuxName).Run()
			return
		case <-sessionGone:
			s.mu.Lock()
			alreadyFinished := s.finished
			s.mu.Unlock()
			if !alreadyFinished {
				// Claude exited without calling avenor_finish.
				s.events <- events.Event{
					Event:     "session.end",
					SessionID: s.sessionID,
					Fields: map[string]any{
						"status":      "done",
						"stop_reason": "end_turn",
					},
				}
			}
			return
		case <-pollTick.C:
			p.pollBrokerEvents(s)
		}
	}
}

func (p *Provider) pollBrokerEvents(s *session) {
	st := p.broker.GetRun(s.runID)
	if st == nil {
		return
	}
	st.Lock()
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

	for _, rep := range reports {
		s.events <- brokerEvent(s.sessionID, rep.State, rep.Payload)
	}
	for _, fin := range finishes {
		s.events <- events.Event{
			Event:     "session.end",
			SessionID: s.sessionID,
			Fields: map[string]any{
				"status":        fin.Status,
				"summary":       fin.Summary,
				"files_changed": fin.FilesChanged,
				"stop_reason":   mapFinishStatus(fin.Status),
			},
		}
		p.mu.Lock()
		s.finished = true
		p.mu.Unlock()
	}
	for _, rep := range replies {
		s.events <- events.Event{
			Event:     "agent.reply",
			SessionID: s.sessionID,
			Fields:    map[string]any{"to": rep.To, "payload": json.RawMessage(rep.Payload)},
		}
	}
}

func (p *Provider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	return runtime.Session{}, fmt.Errorf("resume not supported for %s", backendID)
}

func (p *Provider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	p.mu.Lock()
	s, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}
	if s.finished {
		return fmt.Errorf("session already finished: %s", sessionID)
	}

	// Inject the prompt into Claude's terminal via tmux PTY. This is the
	// primary delivery mechanism: the channel method only works after Claude
	// has started its first turn, and the initial prompt must arrive as
	// terminal input to kick off the session.
	//
	// Wait for Claude to finish loading (~25s from session start). Use a
	// goroutine so Prompt() doesn't block; the channel push serves as a
	// backup if Claude is already listening.
	go func() {
		wait := time.Until(s.startedAt.Add(30 * time.Second))
		if wait < 0 {
			wait = 0
		}

		time.Sleep(wait)
		// Type the prompt and press Enter. Use a short tool usage hint.
		fullPrompt := prompt + " (use avenor_report and avenor_finish)"
		_ = exec.Command("tmux", "send-keys", "-t", s.tmuxName, fullPrompt, "Enter").Run()

		// After sending the prompt, watch for Claude's permission dialog and
		// auto-answer it. This handles MCP tool call permissions that appear
		// as terminal dialogs rather than channel-based permission requests.
		p.pollAndAnswerPermissions(s)
	}()

	// Also push via channel for idempotence and later turns.
	msg := broker.ControlMessage{
		ID:    uuid.New().String(),
		Type:  "continue",
		RunID: s.runID,
		Payload: mustJSON(map[string]any{
			"message": prompt,
		}),
	}
	return p.broker.PushControl(s.runID, msg)
}

// pollAndAnswerPermissions watches the tmux pane for Claude's permission dialog
// ("Do you want to proceed?") and auto-answers "Yes" (option 1). It polls every
// 2 seconds for up to 60 seconds or until the session finishes. Multiple dialogs
// may appear during a single turn (e.g. one per tool call), so the loop keeps
// watching until the session ends.
func (p *Provider) pollAndAnswerPermissions(s *session) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	deadline := time.After(60 * time.Second)

	for {
		select {
		case <-s.done:
			return
		case <-deadline:
			return
		case <-ticker.C:
		}

		s.mu.Lock()
		if s.finished {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		out, err := exec.Command("tmux", "capture-pane", "-t", s.tmuxName, "-p").Output()
		if err != nil {
			// tmux session gone — Claude exited
			return
		}
		if bytes.Contains(out, []byte("Do you want to proceed?")) {
			_ = exec.Command("tmux", "send-keys", "-t", s.tmuxName, "1", "Enter").Run()
			// Don't return — there may be more dialogs.
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
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
		RunID:   s.runID,
		Payload: mustJSON(map[string]any{"reason": "user cancel"}),
	}
	_ = p.broker.PushControl(s.runID, msg)

	// Escalate to hard kill after 2 seconds.
	go func() {
		time.Sleep(2 * time.Second)
		_ = exec.Command("tmux", "kill-session", "-t", s.tmuxName).Run()
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

	out := make(chan events.Event, cap(s.events))
	go func() {
		defer close(out)
		for {
			select {
			case e, ok := <-s.events:
				if !ok {
					return
				}
				out <- e
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
	if s.finished {
		return fmt.Errorf("session already finished: %s", sessionID)
	}

	state := "deny"
	if resp.Allow {
		state = "allow"
	}
	msg := broker.ControlMessage{
		ID:    uuid.New().String(),
		Type:  "permission_decision",
		RunID: s.runID,
		Payload: mustJSON(map[string]any{
			"request_id": requestID,
			"behavior":   state,
			"option_id":  resp.OptionID,
			"message":    resp.Message,
		}),
	}
	return p.broker.PushControl(s.runID, msg)
}

func (p *Provider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{
		Backend:             backendID,
		Permissions:         false, // TODO: flip after Stage 7 spike
		Resume:              false,
		ExternalServerURL:   false,
		SubprocessDiscovery: true,
		ModelSelection:      true,
	}, nil
}

// shellQuote single-quotes a string for safe interpolation into a shell command.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
