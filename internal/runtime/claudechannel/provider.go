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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

const backendID = "claude-channel"

const (
	startupPromptCheckInterval = 500 * time.Millisecond
	startupPromptTimeout       = 30 * time.Second
	promptInjectCheckInterval  = 500 * time.Millisecond
	promptInjectTimeout        = 30 * time.Second
	promptSubmitRetryDelay     = 750 * time.Millisecond
	paneScanInterval           = 500 * time.Millisecond
	tmuxPermissionTimeout      = 60 * time.Second
)

type paneState string

const (
	paneStateUnknown    paneState = "unknown"
	paneStateActive     paneState = "active"
	paneStatePermission paneState = "permission"
	paneStateIdle       paneState = "idle"
)

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
	mcpDir     string // tmpdir holding ephemeral bootstrap state; removed on session exit
	brokerURL  string
	sidecarTok string
	mcpConfig  string
	mcpServer  string
	mcpProject string

	events   chan events.Event
	done     chan struct{}
	ctx      context.Context
	cancelFn context.CancelFunc

	startedAt           time.Time
	prompted            bool
	active              bool
	finished            bool
	channelReadyEmitted bool
	pendingTmuxPerm     *tmuxPermission
	mu                  sync.Mutex
}

type tmuxPermission struct {
	requestID string
	prompt    string
	options   []permissionOption
	tmuxName  string
	createdAt time.Time
}

type permissionOption struct {
	ID    string
	Label string
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
	claudeArgs := []string{
		"--dangerously-load-development-channels", "server:" + serverName,
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
	tmuxName := "avenor-" + runSlug
	if tmuxOut, err := exec.Command(
		"tmux", "new-session",
		"-d",
		"-s", tmuxName,
		"-c", merged.Dir,
		"-x", "220",
		"-y", "50",
		shellCmd,
	).CombinedOutput(); err != nil {
		_ = removeProjectMCPServer(projectMCPPath, serverName)
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
		mcpServer:  serverName,
		mcpProject: projectMCPPath,
		events:     make(chan events.Event, 64),
		done:       make(chan struct{}),
		ctx:        sessCtx,
		cancelFn:   cancel,
		startedAt:  time.Now(),
	}

	p.sessions[sessionID] = s

	go autoConfirmDevelopmentChannelPrompt(sessCtx, s)

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
	defer s.cancelFn()
	defer close(s.done)
	defer close(s.events)
	defer func() {
		p.mu.Lock()
		delete(p.sessions, s.sessionID)
		p.mu.Unlock()
	}()
	defer func() {
		p.broker.DeleteRun(s.runID)
	}()
	defer func() {
		_ = exec.Command("tmux", "kill-session", "-t", s.tmuxName).Run()
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
			if err := exec.Command("tmux", "has-session", "-t", s.tmuxName).Run(); err != nil {
				return
			}
		}
	}()

	// Poll broker for sidecar events.
	pollTick := time.NewTicker(500 * time.Millisecond)
	defer pollTick.Stop()
	paneTick := time.NewTicker(paneScanInterval)
	defer paneTick.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = exec.Command("tmux", "kill-session", "-t", s.tmuxName).Run()
			return
		case <-sessionGone:
			if markFinished(s) {
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
		case <-paneTick.C:
			p.scanPaneTick(s)
		}
	}
}

func (p *Provider) scanPaneTick(s *session) {
	state, err := scanPane(s.tmuxName)
	if err != nil {
		return
	}

	s.mu.Lock()
	prompted := s.prompted
	finished := s.finished
	currentPerm := s.pendingTmuxPerm
	s.mu.Unlock()

	if finished || !prompted {
		return
	}

	switch state {
	case paneStatePermission:
		markActive(s)

		// If we already brokered this permission, wait for resolution.
		if currentPerm != nil {
			return
		}

		// Parse the permission prompt and broker it.
		out, err := exec.Command("tmux", "capture-pane", "-t", s.tmuxName, "-p").Output()
		if err != nil {
			return
		}
		tmuxPerm := parseTmuxPermission(string(out))
		if tmuxPerm == nil {
			return
		}
		tmuxPerm.tmuxName = s.tmuxName
		tmuxPerm.createdAt = time.Now()
		tmuxPerm.requestID = uuid.New().String()

		s.mu.Lock()
		s.pendingTmuxPerm = tmuxPerm
		s.mu.Unlock()

		// Emit the brokered permission request.
		opts := make([]map[string]any, len(tmuxPerm.options))
		for i, o := range tmuxPerm.options {
			opts[i] = map[string]any{"id": o.ID, "label": o.Label}
		}
		s.events <- events.Event{
			Event:     "permission.request",
			SessionID: s.sessionID,
			Fields: map[string]any{
				"request_id":  tmuxPerm.requestID,
				"source":      "tmux-pane",
				"description": tmuxPerm.prompt,
				"options":     opts,
			},
		}

	case paneStateActive:
		markActive(s)
		emitNonBlocking(s, events.Event{Event: "agent.status", SessionID: s.sessionID, Fields: map[string]any{"phase": "working", "source": "tmux-pane"}})
	case paneStateIdle:
		// Idle means Claude is at the prompt, waiting for next input.
		// Don't end the session — the channel supports multi-turn.
		// Clear any stale permission.
		if currentPerm != nil {
			s.mu.Lock()
			s.pendingTmuxPerm = nil
			s.mu.Unlock()
		}
		emitNonBlocking(s, events.Event{Event: "agent.status", SessionID: s.sessionID, Fields: map[string]any{"phase": "waiting", "source": "tmux-pane"}})
	}
}

func (p *Provider) pollBrokerEvents(s *session) {
	st := p.broker.GetRun(s.runID)
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

	if markChannelReadyEmitted(s, registeredAt) {
		s.events <- events.Event{
			Event:     "agent.channel_ready",
			SessionID: s.sessionID,
			Fields: map[string]any{
				"run_id":      s.runID,
				"server_name": s.mcpServer,
				"source":      "channel",
			},
		}
	}

	for _, rep := range reports {
		s.events <- events.Event{
			Event:     "agent.report",
			SessionID: s.sessionID,
			Fields: map[string]any{
				"state":   rep.State,
				"payload": json.RawMessage(rep.Payload),
				"source":  "channel",
			},
		}
		s.events <- brokerEvent(s.sessionID, rep.State, rep.Payload)
	}
	for _, fin := range finishes {
		s.events <- events.Event{
			Event:     "agent.finish",
			SessionID: s.sessionID,
			Fields: map[string]any{
				"status":        fin.Status,
				"summary":       fin.Summary,
				"files_changed": fin.FilesChanged,
				"payload":       json.RawMessage(fin.Payload),
				"source":        "channel",
			},
		}
		if !markFinished(s) {
			continue
		}
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
	}
	for _, rep := range replies {
		s.events <- events.Event{
			Event:     "agent.reply",
			SessionID: s.sessionID,
			Fields: map[string]any{
				"to":      rep.To,
				"payload": json.RawMessage(rep.Payload),
				"source":  "channel",
			},
		}
	}
}

func markFinished(s *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return false
	}
	s.finished = true
	return true
}

func markChannelReadyEmitted(s *session, registeredAt time.Time) bool {
	// Zero RegisteredAt means the Claude sidecar has not registered with the
	// broker yet, so the channel is not ready from the orchestrator's point of
	// view even though the run state already exists.
	if registeredAt.IsZero() {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channelReadyEmitted {
		return false
	}
	s.channelReadyEmitted = true
	return true
}

func markActive(s *session) {
	s.mu.Lock()
	s.active = true
	s.mu.Unlock()
}

func emitNonBlocking(s *session, ev events.Event) {
	select {
	case s.events <- ev:
	default:
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
	s.mu.Lock()
	finished := s.finished
	s.mu.Unlock()
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
		if !waitForPaneReady(s.ctx, s.tmuxName) {
			return
		}

		if err := pastePromptAndSubmit(s.tmuxName, prompt); err != nil {
			return
		}
		s.mu.Lock()
		s.prompted = true
		s.mu.Unlock()
		emitNonBlocking(s, events.Event{
			Event:     "agent.prompt_submitted",
			SessionID: s.sessionID,
			Fields: map[string]any{
				"delivery":      "tmux",
				"prompt_length": len(prompt),
			},
		})
		go retryPromptSubmitIfIdle(s.ctx, s.tmuxName, prompt)
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
	if err := p.broker.PushControl(s.runID, msg); err != nil {
		return err
	}
	emitNonBlocking(s, events.Event{
		Event:     "agent.prompt_queued",
		SessionID: s.sessionID,
		Fields: map[string]any{
			"control_id":    msg.ID,
			"message_type":  msg.Type,
			"delivery":      "channel",
			"prompt_length": len(prompt),
		},
	})
	return nil
}

func pastePromptAndSubmit(tmuxName, prompt string) error {
	bufferName := "avenor-prompt-" + tmuxName
	cmd := exec.Command("tmux", "load-buffer", "-b", bufferName, "-")
	cmd.Stdin = strings.NewReader(prompt)
	if err := cmd.Run(); err != nil {
		return err
	}
	if err := exec.Command("tmux", "paste-buffer", "-t", tmuxName, "-b", bufferName).Run(); err != nil {
		return err
	}
	return exec.Command("tmux", "send-keys", "-t", tmuxName, "Enter").Run()
}

func retryPromptSubmitIfIdle(ctx context.Context, tmuxName, prompt string) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(promptSubmitRetryDelay):
	}

	out, err := exec.Command("tmux", "capture-pane", "-t", tmuxName, "-p").Output()
	if err != nil {
		return
	}
	text := string(out)
	if classifyPane(text) != paneStateIdle {
		return
	}
	needle := prompt
	if len(needle) > 96 {
		needle = needle[:96]
	}
	if !strings.Contains(text, needle) {
		return
	}
	_ = exec.Command("tmux", "send-keys", "-t", tmuxName, "Enter").Run()
}

func scanPane(tmuxName string) (paneState, error) {
	out, err := exec.Command("tmux", "capture-pane", "-t", tmuxName, "-p").Output()
	if err != nil {
		return paneStateUnknown, err
	}
	return classifyPane(string(out)), nil
}

func classifyPane(text string) paneState {
	if strings.Contains(text, "Do you want to proceed?") || strings.Contains(text, "Do you want to make this edit") || strings.Contains(text, "Yes, allow all edits") {
		return paneStatePermission
	}
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "❯") {
			return paneStateIdle
		}
		if looksLikeClaudeActivityLine(trimmed) {
			return paneStateActive
		}
	}
	return paneStateUnknown
}

func looksLikeClaudeActivityLine(line string) bool {
	if !(strings.HasPrefix(line, "✻") || strings.HasPrefix(line, "✢") || strings.HasPrefix(line, "●") || strings.HasPrefix(line, "○") || strings.HasPrefix(line, "◯")) {
		return false
	}
	fields := strings.Fields(line)
	for _, field := range fields {
		word := strings.Trim(field, "…,.·:;()[]{}")
		if len(word) > 4 && strings.HasSuffix(word, "ing") {
			return true
		}
	}
	return strings.Contains(line, "tool use") || strings.Contains(line, "tokens")
}

var tmuxOptionRE = regexp.MustCompile(`^\s*(?:❯\s*)?(\d+)\.\s+(.*)`)

// parseTmuxPermission extracts a tmuxPermission from pane text, or nil if the
// text doesn't represent a Claude permission dialog.
func parseTmuxPermission(text string) *tmuxPermission {
	lines := strings.Split(text, "\n")
	promptLines := make([]string, 0)
	options := make([]permissionOption, 0)

	inOptions := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if tmuxOptionRE.MatchString(trimmed) {
			matches := tmuxOptionRE.FindStringSubmatch(trimmed)
			if len(matches) == 3 {
				options = append(options, permissionOption{ID: matches[1], Label: strings.TrimSpace(matches[2])})
				inOptions = true
				continue
			}
		}
		if inOptions {
			// After options, skip footer lines (Esc to cancel, etc.)
			if strings.Contains(trimmed, "Esc to cancel") || strings.HasPrefix(trimmed, "Esc ") {
				continue
			}
			break
		}
		if !strings.HasPrefix(trimmed, "❯") && !strings.HasPrefix(trimmed, "●") && !strings.HasPrefix(trimmed, "✻") && !strings.HasPrefix(trimmed, "✢") {
			promptLines = append(promptLines, trimmed)
		}
	}

	if len(options) == 0 {
		return nil
	}

	prompt := strings.Join(promptLines, " ")
	if prompt == "" {
		// Try to use the permission markers as prompt fallback.
		if strings.Contains(text, "Do you want to proceed?") {
			prompt = "Claude is requesting permission to proceed"
		} else if strings.Contains(text, "Do you want to make this edit") {
			prompt = "Claude is requesting permission to edit"
		} else {
			prompt = "Claude permission request"
		}
	}

	return &tmuxPermission{
		prompt:  prompt,
		options: options,
	}
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
		case <-s.done:
			return
		case <-deadline.C:
			return
		case <-ticker.C:
		}

		out, err := exec.Command("tmux", "capture-pane", "-t", s.tmuxName, "-p").Output()
		if err != nil {
			continue
		}
		if bytes.Contains(out, []byte("New MCP server found in this project")) {
			_ = exec.Command("tmux", "send-keys", "-t", s.tmuxName, "1", "Enter").Run()
			continue
		}
		if bytes.Contains(out, []byte("Loading development channels")) {
			_ = exec.Command("tmux", "send-keys", "-t", s.tmuxName, "Enter").Run()
			continue
		}
	}
}

func waitForPaneReady(ctx context.Context, tmuxName string) bool {
	deadline := time.NewTimer(promptInjectTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(promptInjectCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return true
		case <-ticker.C:
		}

		out, err := exec.Command("tmux", "capture-pane", "-t", tmuxName, "-p").Output()
		if err != nil {
			return false
		}
		if !bytes.Contains(out, []byte("Loading development channels")) {
			return true
		}
	}
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
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

	// Check if this is a tmux-pane permission request.
	s.mu.Lock()
	tmuxPerm := s.pendingTmuxPerm
	s.mu.Unlock()

	if tmuxPerm != nil && tmuxPerm.requestID == requestID {
		s.mu.Lock()
		s.pendingTmuxPerm = nil
		s.mu.Unlock()

		key := "Esc"
		if resp.Allow {
			key = resp.OptionID
			if key == "" {
				key = "1"
			}
		}
		if !validTmuxKey(key) {
			return fmt.Errorf("invalid option_id for tmux permission: %q", key)
		}
		return exec.Command("tmux", "send-keys", "-t", tmuxPerm.tmuxName, key, "Enter").Run()
	}

	// Not a tmux permission; route through broker (sidecar path).
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
		Permissions:         true,
		Resume:              false,
		ExternalServerURL:   false,
		SubprocessDiscovery: true,
		ModelSelection:      true,
	}, nil
}

// validTmuxKey returns true if key is a safe single-character string for tmux send-keys.
func validTmuxKey(key string) bool {
	if len(key) == 0 || len(key) > 3 {
		return false
	}
	for _, r := range key {
		if r >= '0' && r <= '9' {
			continue
		}
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= 'A' && r <= 'Z' {
			continue
		}
		return false
	}
	return true
}

// shellQuote single-quotes a string for safe interpolation into a shell command.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
