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
	startupTimeout = 10 * time.Second
	shutdownGrace  = 2 * time.Second
	maxJSONLLine   = 50 * 1024 * 1024
	stderrCap      = 400
	printTimeout   = "24h"
	eventsChBuf    = 4096
)

var (
	ErrStartupTimeout       = errors.New("agy: startup timeout — no init event within 10s")
	ErrConversationMismatch = errors.New("agy: conversation_id mismatch")
	ErrLineTooLong          = errors.New("agy: JSONL line exceeds 50 MiB")
	ErrProcessDied          = errors.New("agy: process exited before result")
	ErrClientClosed         = errors.New("agy: client is closed")
)

// sessionInfo holds the metadata extracted from the init event.
type sessionInfo struct {
	Model          string
	AgyVersion     string
	ConversationID string
}

// client manages the lifecycle of a single agy subprocess.
//
// Lifecycle:
//  1. Create via newClient.
//  2. Launch the first turn with LaunchInitial or a later turn with
//     LaunchResumed; both consume and validate init.
//  3. readLoop pushes subsequent translated events to eventsCh.
//  4. The provider relays Events() until session.end.
//  5. Close reaps the process and closes channels.
//
// Ownership rules:
//   - readLoop is the sole writer to eventsCh and the sole closer of doneCh.
//   - Close is idempotent via closeOnce and may be called from multiple
//     goroutines.
//   - Process-level I/O (proc, stdin, stdout) is stored in procState and is
//     mutated only at construction time or during LaunchInitial/LaunchResumed.
//   - All other mutable fields are protected by mu.
type client struct {
	mu sync.Mutex

	// procState holds the subprocess and its pipes.
	procState processState

	// mode tracks the lifecycle phase.
	mode string // "", "init", "running", "closed"

	sessionID  string
	initCache  map[string]any
	eventsCh   chan events.Event
	doneCh     chan struct{}  // closed by readLoop on stdout EOF
	readLoopWg sync.WaitGroup // tracks readLoop
	stderrWg   sync.WaitGroup // tracks drainStderr
	closeOnce  sync.Once
	killOnce   sync.Once // idempotent process kill on Cancel

	readErr error

	// version caches agy --version output (set once via ensureVersion).
	version     string
	versionErr  error
	versionOnce sync.Once

	stderr *rollingBuffer

	// testMode skips subprocess spawning and stdin writes.
	testMode bool
}

// processState captures the subprocess and its I/O at launch time.
type processState struct {
	proc   *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
}

// newClient creates a client attached to an already-started subprocess.
// The caller must start the process; newClient only wires I/O and launches
// readLoop / drainStderr goroutines.
func newClient(proc *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser, stderr io.ReadCloser) *client {
	c := &client{
		procState: processState{
			proc:   proc,
			stdin:  stdin,
			stdout: stdout,
		},
		mode:      "",
		initCache: make(map[string]any),
		eventsCh:  make(chan events.Event, eventsChBuf),
		doneCh:    make(chan struct{}),
		stderr:    newRollingBuffer(stderrCap),
	}
	if stderr != nil {
		c.stderrWg.Add(1)
		go c.drainStderr(stderr)
	}
	if stdout != nil {
		c.readLoopWg.Add(1)
		go c.readLoop()
	} else {
		close(c.doneCh)
	}
	return c
}

// ---------------------------------------------------------------------------
// Version detection
// ---------------------------------------------------------------------------

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
		c.mu.Lock()
		c.version = strings.TrimSpace(string(out))
		c.mu.Unlock()
	})
}

// ---------------------------------------------------------------------------
// Command construction
// ---------------------------------------------------------------------------

func initialArgs(prompt, model, agent, dir string) []string {
	args := []string{"--output-format", "stream-json", "--print-timeout", printTimeout}
	if model != "" {
		args = append(args, "--model", model)
	}
	if agent != "" {
		args = append(args, "--agent", agent)
	}
	if dir != "" {
		args = append(args, "--add-dir", dir)
	}
	return append(args, "--print", prompt)
}

func resumedArgs(conversationID, prompt, model, agent, dir string) []string {
	args := []string{
		"--conversation", conversationID,
		"--output-format", "stream-json",
		"--print-timeout", printTimeout,
	}
	if model != "" {
		args = append(args, "--model", model)
	}
	if agent != "" {
		args = append(args, "--agent", agent)
	}
	if dir != "" {
		args = append(args, "--add-dir", dir)
	}
	return append(args, "--print", prompt)
}

// ---------------------------------------------------------------------------
// Initial turn
// ---------------------------------------------------------------------------

// LaunchInitial starts agy with the required first prompt argument, waits for
// init, and leaves subsequent turn events queued for the provider to relay.
func (c *client) LaunchInitial(ctx context.Context, prompt, model, agent, dir string) (sessionInfo, error) {
	if c.versionErr != nil {
		return sessionInfo{}, c.versionErr
	}

	args := initialArgs(prompt, model, agent, dir)

	// The startup context bounds init discovery only. Provider Cancel/Close
	// owns the process after launch.
	proc := exec.CommandContext(context.Background(), "agy", args...)
	proc.Dir = dir
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return sessionInfo{}, fmt.Errorf("agy stdout pipe: %w", err)
	}
	stderr, err := proc.StderrPipe()
	if err != nil {
		return sessionInfo{}, fmt.Errorf("agy stderr pipe: %w", err)
	}
	if err := proc.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return sessionInfo{}, fmt.Errorf("start agy: %w", err)
	}

	c.reset(proc, nil, stdout)
	c.stderrWg.Add(1)
	go c.drainStderr(stderr)
	c.readLoopWg.Add(1)
	go c.readLoop()

	info, err := c.waitForInit(ctx)
	if err != nil {
		c.Close()
		return sessionInfo{}, c.withStderr(err)
	}
	c.mu.Lock()
	c.mode = "running"
	c.mu.Unlock()
	return info, nil
}

// reset reinitializes the client for a new subprocess.
func (c *client) reset(proc *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser) {
	c.mu.Lock()
	c.procState = processState{proc: proc, stdin: stdin, stdout: stdout}
	c.mode = ""
	c.sessionID = ""
	c.initCache = make(map[string]any)
	c.eventsCh = make(chan events.Event, eventsChBuf)
	c.doneCh = make(chan struct{})
	c.readLoopWg = sync.WaitGroup{}
	c.stderrWg = sync.WaitGroup{}
	c.closeOnce = sync.Once{}
	c.killOnce = sync.Once{}
	c.readErr = nil
	c.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Init validation
// ---------------------------------------------------------------------------

// waitForInit consumes events from eventsCh until a session.start event
// arrives, or the context is cancelled, or startupTimeout fires, or the
// channel is closed.
func (c *client) waitForInit(ctx context.Context) (sessionInfo, error) {
	timeout := time.NewTimer(startupTimeout)
	defer timeout.Stop()

	for {
		select {
		case evt, ok := <-c.eventsCh:
			if !ok {
				return sessionInfo{}, ErrProcessDied
			}
			if err := errorBeforeInit(evt); err != nil {
				return sessionInfo{}, err
			}
			if evt.Event == "session.start" && evt.Fields != nil {
				info := sessionInfo{}
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
		case <-c.doneCh:
			// readLoop exited (EOF or error). Drain remaining events.
			return c.drainUntilInit()
		}
	}
}

// errorBeforeInit preserves an upstream result error that arrives before init.
func errorBeforeInit(evt events.Event) error {
	if evt.Event != "session.end" {
		return nil
	}
	if message, _ := evt.Fields["error"].(string); message != "" {
		return fmt.Errorf("agy: startup failed: %s", message)
	}
	return errors.New("agy: process ended before init")
}

// drainUntilInit reads queued events after readLoop exits.
func (c *client) drainUntilInit() (sessionInfo, error) {
	for {
		select {
		case evt, ok := <-c.eventsCh:
			if !ok {
				return sessionInfo{}, ErrProcessDied
			}
			if err := errorBeforeInit(evt); err != nil {
				return sessionInfo{}, err
			}
			if evt.Event == "session.start" && evt.Fields != nil {
				info := sessionInfo{}
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
		default:
			return sessionInfo{}, c.processError(ErrProcessDied)
		}
	}
}

// validateInit reads events from eventsCh until a session.start event
// arrives, then verifies its conversation_id matches the expected value.
func (c *client) validateInit(ctx context.Context, expectedID string) error {
	timeout := time.NewTimer(startupTimeout)
	defer timeout.Stop()

	for {
		select {
		case evt, ok := <-c.eventsCh:
			if !ok {
				return ErrProcessDied
			}
			if err := errorBeforeInit(evt); err != nil {
				return err
			}
			if evt.Event == "session.start" && evt.Fields != nil {
				cid, _ := evt.Fields["conversation_id"].(string)
				model, _ := evt.Fields["model"].(string)
				v, _ := evt.Fields["agy_version"].(string)

				c.mu.Lock()
				c.sessionID = cid
				c.initCache["conversation_id"] = cid
				c.initCache["model"] = model
				c.initCache["agy_version"] = v
				c.mu.Unlock()

				if cid != expectedID {
					return fmt.Errorf("%w: expected %s, got %s", ErrConversationMismatch, expectedID, cid)
				}
				return nil
			}
		case <-timeout.C:
			return ErrStartupTimeout
		case <-ctx.Done():
			return ctx.Err()
		case <-c.doneCh:
			return c.drainValidateUntilInit(expectedID)
		}
	}
}

// drainValidateUntilInit drains remaining events from eventsCh after doneCh
// is closed, looking for a session.start to validate.
func (c *client) drainValidateUntilInit(expectedID string) error {
	for {
		select {
		case evt, ok := <-c.eventsCh:
			if !ok {
				return ErrProcessDied
			}
			if err := errorBeforeInit(evt); err != nil {
				return err
			}
			if evt.Event == "session.start" && evt.Fields != nil {
				cid, _ := evt.Fields["conversation_id"].(string)
				model, _ := evt.Fields["model"].(string)
				v, _ := evt.Fields["agy_version"].(string)

				c.mu.Lock()
				c.sessionID = cid
				c.initCache["conversation_id"] = cid
				c.initCache["model"] = model
				c.initCache["agy_version"] = v
				c.mu.Unlock()

				if cid != expectedID {
					return fmt.Errorf("%w: expected %s, got %s", ErrConversationMismatch, expectedID, cid)
				}
				return nil
			}
		default:
			return c.processError(ErrProcessDied)
		}
	}
}

// ---------------------------------------------------------------------------
// Prompt execution
// ---------------------------------------------------------------------------

// LaunchResumed starts a new agy subprocess with the given conversation ID
// and prompt (supplied as --print argument), validates the init event, then
// returns. The caller should then read from Events() until session.end.
//
// Any prior process is cleaned up first.
func (c *client) LaunchResumed(ctx context.Context, conversationID, prompt, model, agent, dir string) error {
	// Clean up any prior process.
	c.Close()

	args := resumedArgs(conversationID, prompt, model, agent, dir)

	proc := exec.CommandContext(context.Background(), "agy", args...)
	proc.Dir = dir

	stdin, err := proc.StdinPipe()
	if err != nil {
		return fmt.Errorf("agy stdin pipe: %w", err)
	}
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return fmt.Errorf("agy stdout pipe: %w", err)
	}
	stderr, err := proc.StderrPipe()
	if err != nil {
		return fmt.Errorf("agy stderr pipe: %w", err)
	}
	if err := proc.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return fmt.Errorf("start agy resume: %w", err)
	}

	c.reset(proc, stdin, stdout)
	_ = stdin.Close() // stdin not needed; prompt is on command line.

	c.stderrWg.Add(1)
	go c.drainStderr(stderr)

	c.readLoopWg.Add(1)
	go c.readLoop()

	if err := c.validateInit(ctx, conversationID); err != nil {
		c.Close()
		return c.withStderr(err)
	}

	c.mu.Lock()
	c.mode = "running"
	c.mu.Unlock()

	return nil
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

// Cancel sends os.Interrupt to the process, waits shutdownGrace, then kills.
// Idempotent per client via killOnce.
func (c *client) Cancel(ctx context.Context) error {
	c.mu.Lock()
	proc := c.procState.proc
	c.mu.Unlock()

	if proc == nil || proc.Process == nil {
		return ErrClientClosed
	}

	c.killOnce.Do(func() {
		_ = proc.Process.Signal(os.Interrupt)

		waitDone := make(chan struct{})
		go func() {
			_, _ = proc.Process.Wait()
			close(waitDone)
		}()

		select {
		case <-waitDone:
		case <-time.After(shutdownGrace):
			_ = proc.Process.Kill()
			<-waitDone
		}
	})

	return nil
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

// Close performs idempotent cleanup: kills the process, joins reader
// goroutines, and closes the events channel.
func (c *client) Close() error {
	c.closeOnce.Do(c.doClose)
	return nil
}

func (c *client) doClose() {
	c.mu.Lock()
	c.mode = "closed"
	proc := c.procState.proc
	stdin := c.procState.stdin
	stdout := c.procState.stdout
	c.mu.Unlock()

	// Close pipes first to unblock readers.
	if stdin != nil {
		_ = stdin.Close()
	}
	if stdout != nil {
		_ = stdout.Close()
	}

	// Kill the process via killOnce.
	c.killOnce.Do(func() {
		if proc != nil && proc.Process != nil {
			_ = proc.Process.Signal(os.Interrupt)
			waitDone := make(chan struct{})
			go func() {
				_, _ = proc.Process.Wait()
				close(waitDone)
			}()
			select {
			case <-waitDone:
			case <-time.After(shutdownGrace):
				_ = proc.Process.Kill()
				<-waitDone
			}
		}
	})

	// Wait for readLoop to exit.
	c.readLoopWg.Wait()

	// Drain stderr with a short bound.
	waitStderr := make(chan struct{})
	go func() {
		c.stderrWg.Wait()
		close(waitStderr)
	}()
	select {
	case <-waitStderr:
	case <-time.After(shutdownGrace):
	}

	// Close events channel (safe after readLoop has exited).
	close(c.eventsCh)
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

// Events returns a read-only channel of parsed events. The channel is closed
// after Close is called and readLoop has exited.
func (c *client) Events() <-chan events.Event {
	return c.eventsCh
}

// Stderr returns the bounded stderr content.
func (c *client) Stderr() string {
	return c.stderr.String()
}

func (c *client) withStderr(err error) error {
	stderr := strings.TrimSpace(c.Stderr())
	if stderr == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, stderr)
}

func (c *client) processError(fallback error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readErr != nil {
		return c.readErr
	}
	return fallback
}

// SessionID returns the conversation_id from the init event.
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

// Mode returns the client's operational mode for debugging.
func (c *client) Mode() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mode
}

// ---------------------------------------------------------------------------
// JSONL parsing (read loop)
// ---------------------------------------------------------------------------

// readLoop reads newline-delimited JSON from stdout, translates events, and
// sends them to eventsCh. It closes doneCh when stdout reaches EOF or an
// unrecoverable scanner error, then signals readLoopWg.Done().
func (c *client) readLoop() {
	defer c.readLoopWg.Done()
	defer close(c.doneCh)

	c.mu.Lock()
	stdout := c.procState.stdout
	c.mu.Unlock()

	if stdout == nil {
		return
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1024*1024), maxJSONLLine)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			c.stderr.Append(fmt.Sprintf("dropped malformed JSONL line: %v", err))
			continue
		}

		evtType, _ := raw["event"].(string)
		if evtType == "" {
			continue
		}

		evts := c.translateEvent(raw)
		for _, evt := range evts {
			if evt.Event == "session.end" {
				// Terminal delivery is part of the Provider contract and must not
				// be silently dropped.
				c.eventsCh <- evt
				continue
			}
			select {
			case c.eventsCh <- evt:
			default:
				c.stderr.Append(fmt.Sprintf("dropped event %q: event buffer full", evt.Event))
			}
		}
	}

	if err := scanner.Err(); err != nil {
		c.mu.Lock()
		closed := c.mode == "closed"
		if !closed {
			if strings.Contains(err.Error(), "token too long") {
				c.readErr = ErrLineTooLong
			} else {
				c.readErr = fmt.Errorf("agy: read JSONL: %w", err)
			}
		}
		c.mu.Unlock()
		if !closed {
			c.stderr.Append(fmt.Sprintf("read loop error: %v", err))
		}
	}
}

// translateEvent wraps the package-level translateEvent with session state.
func (c *client) translateEvent(raw map[string]any) []events.Event {
	c.mu.Lock()
	sid := c.sessionID
	isFirstInit := c.initCache["conversation_id"] == nil
	cache := c.initCache
	c.mu.Unlock()

	evts, updated := translateEvent(raw, sid, isFirstInit, cache)

	c.mu.Lock()
	for k, v := range updated {
		c.initCache[k] = v
	}
	c.mu.Unlock()

	return evts
}

// ---------------------------------------------------------------------------
// Stderr drain
// ---------------------------------------------------------------------------

func (c *client) drainStderr(r io.ReadCloser) {
	defer c.stderrWg.Done()
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

// ---------------------------------------------------------------------------
// Rolling buffer
// ---------------------------------------------------------------------------

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
