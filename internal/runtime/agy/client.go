package agy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

const (
	startupTimeout  = 10 * time.Second
	firstPromptIdle = 5 * time.Minute
	shutdownGrace   = 2 * time.Second
	maxJSONLLine    = 50 * 1024 * 1024
	stderrCap       = 400
	printTimeout    = "24h"
	eventsChBuf     = 4096
)

var (
	ErrStartupTimeout     = errors.New("agy: startup timeout — no init event within 10s")
	ErrConversationMismatch = errors.New("agy: conversation_id mismatch")
	ErrLineTooLong        = errors.New("agy: JSONL line exceeds 50 MiB")
	ErrProcessDied        = errors.New("agy: process exited before result")
	ErrClientClosed       = errors.New("agy: client is closed")
)

// sessionInfo holds the metadata extracted from the init event.
type sessionInfo struct {
	SessionID      string
	Model          string
	AgyVersion     string
	ConversationID string
}

// client manages the lifecycle of an agy subprocess.
type client struct {
	mu        sync.Mutex
	proc      *exec.Cmd
	stdin     io.WriteCloser
	stdout    io.ReadCloser
	stderr    *rollingBuffer
	events    chan events.Event
	done      chan struct{}
	closeOnce sync.Once

	mode       string // "pending", "prompting", "resumed", "closed"
	sessionID  string
	initCache  map[string]any
	idleTimer  *time.Timer
	idleOnce   sync.Once

	testMode       bool     // if true, skip subprocess spawning
	lastResult     string   // last result/response from agent
	lastStopReason string   // last stop_reason from session.end

	version     string
	versionOnce sync.Once
	versionErr  error
}

func newClient(proc *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser, stderr io.ReadCloser) *client {
	c := &client{
		proc:      proc,
		stdin:     stdin,
		stdout:    stdout,
		stderr:    newRollingBuffer(stderrCap),
		events:    make(chan events.Event, eventsChBuf),
		done:      make(chan struct{}),
		initCache: make(map[string]any),
	}
	if stderr != nil {
		go c.drainStderr(stderr)
	}
	if stdout != nil {
		go c.readLoop()
	}
	return c
}

func newRollingBuffer(cap int) *rollingBuffer {
	return &rollingBuffer{cap: cap}
}

type rollingBuffer struct {
	mu    sync.Mutex
	lines []string
	cap   int
}

func (b *rollingBuffer) Append(line string) {
	b.mu.Lock()
	b.lines = append(b.lines, line)
	if len(b.lines) > b.cap {
		b.lines = b.lines[len(b.lines)-b.cap:]
	}
	b.mu.Unlock()
}

func (b *rollingBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var sb strings.Builder
	for _, line := range b.lines {
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// ensureVersion detects the agy binary version via sync.Once.
func (c *client) ensureVersion(ctx context.Context) {
	c.versionOnce.Do(func() {
		if c.testMode {
			return
		}
		cmd := exec.CommandContext(ctx, "agy", "--version")
		out, err := cmd.Output()
		if err != nil {
			c.versionErr = fmt.Errorf("agy --version: %w", err)
			return
		}
		c.version = strings.TrimSpace(string(out))
	})
}

// StartWithPipes sets up the client from pre-existing pipes (for testing)
// and waits for the init event. Does NOT launch a subprocess.
// If goroutines are already running (e.g., from fakeClient), it reuses them.
func (c *client) StartWithPipes(ctx context.Context, stdin io.WriteCloser, stdout io.ReadCloser, stderr io.ReadCloser) (sessionInfo, error) {
	c.mu.Lock()
	needsStart := c.events == nil || c.done == nil
	c.stdin = stdin
	c.stdout = stdout
	c.stderr = newRollingBuffer(stderrCap)
	if needsStart {
		c.events = make(chan events.Event, eventsChBuf)
		c.done = make(chan struct{})
		c.initCache = make(map[string]any)
		c.mode = "pending"
	} else {
		c.mode = "pending"
		if c.initCache == nil {
			c.initCache = make(map[string]any)
		}
	}
	c.mu.Unlock()

	if needsStart {
		go c.drainStderr(stderr)
		go c.readLoop()
	}

	info, err := c.waitForInit(ctx)
	if err != nil {
		c.doClose()
		return sessionInfo{}, err
	}

	c.idleOnce.Do(func() {
		c.idleTimer = time.AfterFunc(firstPromptIdle, func() {
			c.mu.Lock()
			if c.mode == "pending" {
				c.mu.Unlock()
				if c.proc != nil && c.proc.Process != nil {
					_ = c.proc.Process.Signal(os.Interrupt)
				}
				go c.doClose()
			} else {
				c.mu.Unlock()
			}
		})
	})

	return info, nil
}

// Start launches an agy subprocess for a new session (no prompt).
// It waits for the init event, caches session metadata, and returns a sessionInfo.
// After Start the client is in "pending" mode with an idle timer.
// Call FirstPrompt to submit the initial prompt, or Close to clean up.
func (c *client) Start(ctx context.Context, model, agent string) (sessionInfo, error) {
	c.ensureVersion(ctx)
	if c.versionErr != nil {
		return sessionInfo{}, c.versionErr
	}

	args := []string{"--output-format", "stream-json", "--print", "--print-timeout", printTimeout}
	if model != "" {
		args = append(args, "--model", model)
	}
	if agent != "" {
		args = append(args, "--agent", agent)
	}

	proc := exec.CommandContext(ctx, "agy", args...)

	stdin, err := proc.StdinPipe()
	if err != nil {
		return sessionInfo{}, fmt.Errorf("agy stdin pipe: %w", err)
	}
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return sessionInfo{}, fmt.Errorf("agy stdout pipe: %w", err)
	}
	stderr, err := proc.StderrPipe()
	if err != nil {
		return sessionInfo{}, fmt.Errorf("agy stderr pipe: %w", err)
	}

	if err := proc.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return sessionInfo{}, fmt.Errorf("start agy: %w", err)
	}

	c.mu.Lock()
	c.proc = proc
	c.stdin = stdin
	c.stdout = stdout
	c.stderr = newRollingBuffer(stderrCap)
	c.events = make(chan events.Event, eventsChBuf)
	c.done = make(chan struct{})
	c.initCache = make(map[string]any)
	c.mode = "pending"
	c.mu.Unlock()

	go c.drainStderr(stderr)
	go c.readLoop()

	info, err := c.waitForInit(ctx)
	if err != nil {
		c.doClose()
		return sessionInfo{}, err
	}

	// Start idle timer: if no FirstPrompt arrives within firstPromptIdle, clean up.
	c.idleOnce.Do(func() {
		c.idleTimer = time.AfterFunc(firstPromptIdle, func() {
			c.mu.Lock()
			if c.mode == "pending" {
				c.mu.Unlock()
				_ = c.proc.Process.Signal(os.Interrupt)
				go c.doClose()
			} else {
				c.mu.Unlock()
			}
		})
	})

	return info, nil
}

// waitForInit blocks until a session.start event arrives from the events channel,
// or the context is cancelled or the startup timeout fires.
func (c *client) waitForInit(ctx context.Context) (sessionInfo, error) {
	timeout := time.NewTimer(startupTimeout)
	defer timeout.Stop()

	for {
		select {
		case evt := <-c.events:
			if evt.Event == "session.start" && evt.Fields != nil {
				info := sessionInfo{
					SessionID: evt.SessionID,
				}
				if m, ok := evt.Fields["model"].(string); ok {
					info.Model = m
				}
				if v, ok := evt.Fields["agy_version"].(string); ok {
					info.AgyVersion = v
				}
				if cid, ok := evt.Fields["conversation_id"].(string); ok {
					info.ConversationID = cid
					c.mu.Lock()
					c.sessionID = cid
					c.initCache["conversation_id"] = cid
					c.initCache["model"] = info.Model
					c.initCache["agy_version"] = info.AgyVersion
					c.mu.Unlock()
				}
				return info, nil
			}
		case <-timeout.C:
			return sessionInfo{}, ErrStartupTimeout
		case <-ctx.Done():
			return sessionInfo{}, ctx.Err()
		}
	}
}

// FirstPrompt writes the prompt to the pending process's stdin, closes stdin,
// and reads until a session.end result arrives.
// Must be called after Start (client must be in "pending" mode).
func (c *client) FirstPrompt(ctx context.Context, prompt string) error {
	c.mu.Lock()
	if c.mode != "pending" {
		c.mu.Unlock()
		return fmt.Errorf("agy: cannot FirstPrompt in mode %q", c.mode)
	}
	if c.idleTimer != nil {
		c.idleTimer.Stop()
		c.idleTimer = nil
	}
	c.mode = "prompting"
	stdin := c.stdin
	c.mu.Unlock()

	if !c.testMode {
		_, writeErr := stdin.Write([]byte(prompt + "\n"))
		if writeErr != nil {
			return fmt.Errorf("write prompt to stdin: %w", writeErr)
		}
		_ = stdin.Close()
	}

	err := c.readUntilResult(ctx)

	// Cleanup after result arrives or context is cancelled.
	c.doClose()
	return err
}

// ResumePrompt starts a new agy subprocess with --conversation and --print,
// validates the init conversation_id matches, writes the prompt, and reads until result.
func (c *client) ResumePrompt(ctx context.Context, conversationID, prompt, model, agent string) error {
	c.mu.Lock()
	if c.proc != nil {
		// There's already a process running — clean it up first.
		c.mu.Unlock()
		_ = c.doClose()
		c.mu.Lock()
	}

	args := []string{"--conversation", conversationID, "--output-format", "stream-json", "--print-timeout", printTimeout, "--print", prompt}
	if model != "" {
		args = append(args, "--model", model)
	}
	if agent != "" {
		args = append(args, "--agent", agent)
	}

	proc := exec.CommandContext(ctx, "agy", args...)
	stdin, err := proc.StdinPipe()
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("agy stdin pipe: %w", err)
	}
	stdout, err := proc.StdoutPipe()
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("agy stdout pipe: %w", err)
	}
	stderr, err := proc.StderrPipe()
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("agy stderr pipe: %w", err)
	}

	if err := proc.Start(); err != nil {
		c.mu.Unlock()
		_ = stdin.Close()
		_ = stdout.Close()
		return fmt.Errorf("start agy resume: %w", err)
	}

	cl := newClient(proc, stdin, stdout, stderr)
	cl.mode = "resumed"
	c.mu.Unlock()

	// Validate init conversation_id matches expected
	initErr := cl.validateInit(ctx, conversationID)
	if initErr != nil {
		cl.doClose()
		return initErr
	}

	// Write prompt and close stdin
	_, err = cl.stdin.Write([]byte(prompt + "\n"))
	if err != nil {
		cl.doClose()
		return fmt.Errorf("write prompt to stdin: %w", err)
	}
	_ = cl.stdin.Close()

	err = cl.readUntilResult(ctx)

	// Take ownership of the sub-client's state on success.
	c.mu.Lock()
	if err == nil {
		c.proc = cl.proc
		c.stdin = cl.stdin
		c.stdout = cl.stdout
		c.stderr = cl.stderr
		c.events = cl.events
		c.done = cl.done
		c.mode = cl.mode
		c.initCache = cl.initCache
		c.sessionID = cl.sessionID
		// Don't close cl's events/done — we own them now.
		cl.events = nil
		cl.done = nil
	} else {
		c.mode = "resumed"
	}
	c.mu.Unlock()

	return err
}

// ResumePromptWithPipes is the test-friendly version of ResumePrompt.
// It takes pre-existing pipes instead of spawning a subprocess.
func (c *client) ResumePromptWithPipes(ctx context.Context, conversationID, prompt string, stdin io.WriteCloser, stdout io.ReadCloser, stderr io.ReadCloser) error {
	cl := newClient(nil, stdin, stdout, stderr)
	cl.mode = "resumed"
	cl.testMode = true

	initErr := cl.validateInit(ctx, conversationID)
	if initErr != nil {
		cl.doClose()
		return initErr
	}

	readErr := cl.readUntilResult(ctx)

	c.mu.Lock()
	if readErr == nil {
		c.proc = cl.proc
		c.stdin = cl.stdin
		c.stdout = cl.stdout
		c.stderr = cl.stderr
		c.events = cl.events
		c.done = cl.done
		c.mode = cl.mode
		c.initCache = cl.initCache
		c.sessionID = cl.sessionID
		cl.events = nil
		cl.done = nil
	} else {
		c.mode = "resumed"
	}
	c.mu.Unlock()

	return readErr
}

// ValidateAndReadResult validates init conversation_id then reads until result,
// reusing the existing client's goroutines and channels.
// Used when the client already has active reader goroutines (e.g. test mode).
func (c *client) ValidateAndReadResult(ctx context.Context, conversationID string) error {
	c.mu.Lock()
	c.mode = "resumed"
	c.mu.Unlock()

	err := c.validateInit(ctx, conversationID)
	if err != nil {
		return err
	}

	return c.readUntilResult(ctx)
}

// validateAndReadResult is the provider-facing method that also writes the prompt to stdin.
func (c *client) validateAndReadResult(ctx context.Context, conversationID, prompt string) error {
	c.mu.Lock()
	c.mode = "resumed"
	stdin := c.stdin
	c.mu.Unlock()

	err := c.validateInit(ctx, conversationID)
	if err != nil {
		return err
	}

	if !c.testMode {
		_, writeErr := stdin.Write([]byte(prompt + "\n"))
		if writeErr != nil {
			return fmt.Errorf("write prompt to stdin: %w", writeErr)
		}
		_ = stdin.Close()
	}

	return c.readUntilResult(ctx)
}

// validateInit waits for the init event and verifies conversation_id matches.
func (c *client) validateInit(ctx context.Context, expectedID string) error {
	timeout := time.NewTimer(startupTimeout)
	defer timeout.Stop()

	for {
		select {
		case evt := <-c.events:
			if evt.Event == "session.start" && evt.Fields != nil {
				if cid, ok := evt.Fields["conversation_id"].(string); ok {
					c.mu.Lock()
					c.sessionID = cid
					c.initCache["conversation_id"] = cid
					if m, ok := evt.Fields["model"].(string); ok {
						c.initCache["model"] = m
					}
					if v, ok := evt.Fields["agy_version"].(string); ok {
						c.initCache["agy_version"] = v
					}
					c.mu.Unlock()

					if cid != expectedID {
						return fmt.Errorf("conversation_id mismatch: expected %s, got %s", expectedID, cid)
					}
					return nil
				}
			}
		case <-timeout.C:
			return ErrStartupTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// readUntilResult blocks until a session.end event arrives, context is cancelled,
// or the process exits. Does NOT perform cleanup — caller must do that.
func (c *client) readUntilResult(ctx context.Context) error {
	for {
		select {
		case evt := <-c.events:
			if evt.Event == "session.end" {
				if evt.Fields != nil {
					if sr, ok := evt.Fields["stop_reason"].(string); ok {
						c.mu.Lock()
						c.lastStopReason = sr
						c.mu.Unlock()
					}
				}
				return nil
			}
		case <-c.done:
			c.mu.Lock()
			reason := c.lastStopReason
			c.mu.Unlock()
			if reason != "" && reason != "error" {
				return nil
			}
			return ErrProcessDied
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Cancel sends os.Interrupt to the process, waits shutdownGrace, then kills.
func (c *client) Cancel(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.proc == nil || c.proc.Process == nil {
		return ErrClientClosed
	}

	_ = c.proc.Process.Signal(os.Interrupt)

	grace := make(chan error, 1)
	go func() {
		grace <- c.proc.Wait()
	}()

	select {
	case <-grace:
		return nil
	case <-time.After(shutdownGrace):
		_ = c.proc.Process.Kill()
		<-grace
		return nil
	}
}

// Close performs idempotent cleanup: stops idle timer, closes pipes,
// sends interrupt, waits for process exit and goroutines, closes events channel.
func (c *client) Close() error {
	return c.doClose()
}

func (c *client) doClose() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.mode = "closed"

		if c.idleTimer != nil {
			c.idleTimer.Stop()
			c.idleTimer = nil
		}

		stdin := c.stdin
		stdout := c.stdout
		proc := c.proc
		done := c.done
		events := c.events
		c.stdin = nil
		c.stdout = nil
		c.proc = nil
		c.mu.Unlock()

		if stdin != nil {
			_ = stdin.Close()
		}
		if stdout != nil {
			_ = stdout.Close()
		}

		if proc != nil && proc.Process != nil {
			_ = proc.Process.Signal(os.Interrupt)
			waitCh := make(chan struct{})
			go func() {
				_, _ = proc.Process.Wait()
				close(waitCh)
			}()
			select {
			case <-waitCh:
			case <-time.After(shutdownGrace):
				_ = proc.Process.Kill()
				<-waitCh
			}
		}

		if done != nil {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
			}
		}
		if events != nil {
			close(events)
		}
	})
	return nil
}

// Events returns the channel of parsed events.
func (c *client) Events() <-chan events.Event {
	return c.events
}

// Stderr returns the bounded stderr content.
func (c *client) Stderr() string {
	return c.stderr.String()
}

// SessionID returns the conversation_id from init.
func (c *client) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

// Version returns the cached agy version string.
func (c *client) Version() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.version
}

// readLoop reads JSONL from stdout, parses events, and sends to the events channel.
func (c *client) readLoop() {
	defer close(c.done)
	if c.stdout == nil {
		return
	}
	scanner := bufio.NewScanner(c.stdout)
	scanner.Buffer(make([]byte, 1024*1024), maxJSONLLine)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var payload map[string]any
		if err := json.Unmarshal(line, &payload); err != nil {
			c.stderr.Append(fmt.Sprintf("dropped malformed JSONL line: %v", err))
			continue
		}

		evtType, _ := payload["type"].(string)
		if evtType == "" {
			continue
		}

		evts := c.translateEvent(payload)
		for _, evt := range evts {
			if evt.Event == "session.end" && evt.Fields != nil {
				if sr, ok := evt.Fields["stop_reason"].(string); ok {
					c.mu.Lock()
					c.lastStopReason = sr
					c.mu.Unlock()
				}
				if resp, ok := evt.Fields["final_output"].(string); ok {
					c.mu.Lock()
					c.lastResult = resp
					c.mu.Unlock()
				}
			}
			select {
			case c.events <- evt:
			default:
				c.stderr.Append(fmt.Sprintf("dropped event %q: global event buffer full", evt.Event))
			}
		}
	}

	if err := scanner.Err(); err != nil {
		c.stderr.Append(fmt.Sprintf("read loop error: %v", err))
	}
}

// translateEvent wraps the agy package's translateEvent with session state.
func (c *client) translateEvent(payload map[string]any) []events.Event {
	c.mu.Lock()
	isFirstInit := c.initCache == nil || c.initCache["conversation_id"] == nil
	sid := c.sessionID
	cache := c.initCache
	c.mu.Unlock()

	evts, _ := translateEvent(payload, sid, isFirstInit, cache)
	return evts
}

// drainStderr reads stderr into the rolling buffer.
func (c *client) drainStderr(r io.ReadCloser) {
	if r == nil {
		return
	}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		c.stderr.Append(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		c.stderr.Append(fmt.Sprintf("stderr drain error: %v", err))
	}
	_ = r.Close()
}