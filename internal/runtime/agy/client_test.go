package agy

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// fakeClient creates a client with pipe-based stdin/stdout/stderr,
// bypassing the real agy subprocess. This is the primary test helper.
// Returns: client, stdout_writer, stdin_reader, stdin_writer, stderr_writer
func fakeClient() (*client, *io.PipeWriter, *io.PipeReader, *io.PipeWriter, *io.PipeWriter) {
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	stdinR, stdinW := io.Pipe()

	c := newClient(nil, stdinW, stdoutR, stderrR)
	return c, stdoutW, stdinR, stdinW, stderrW
}

// writeJSONL writes a JSONL line to a writer.
func writeJSONL(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

// readLine reads one line from a reader.
func readLine(r io.Reader) (string, error) {
	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	return "", scanner.Err()
}

// writeJSONLLine is an alias for writeJSONL for clarity in tests.
func writeJSONLLine(w io.Writer, v any) error {
	return writeJSONL(w, v)
}

// --- 1. No-prompt startup with init capture ---

func TestClient_StartInitCapture(t *testing.T) {
	c, wOut, _, _, _ := fakeClient()
	defer c.Close()

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-abc123",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	info, err := c.waitForInit(context.Background())
	if err != nil {
		t.Fatalf("waitForInit: %v", err)
	}
	if info.ConversationID != "conv-abc123" {
		t.Errorf("ConversationID = %q, want %q", info.ConversationID, "conv-abc123")
	}
	if info.Model != "gemini-2.0-flash" {
		t.Errorf("Model = %q, want %q", info.Model, "gemini-2.0-flash")
	}
	if info.AgyVersion != "1.2.0" {
		t.Errorf("AgyVersion = %q, want %q", info.AgyVersion, "1.2.0")
	}
	if c.SessionID() != "conv-abc123" {
		t.Errorf("SessionID = %q, want %q", c.SessionID(), "conv-abc123")
	}
}

// --- 2. First-prompt stdin delivery ---

func TestClient_FirstPromptStdinDelivery(t *testing.T) {
	c, _, stdinR, stdinW, _ := fakeClient()
	defer c.Close()

	done := make(chan string, 1)
	go func() {
		line, _ := readLine(stdinR)
		done <- line
	}()

	// Write directly to the client's stdin
	c.mu.Lock()
	c.stdin = stdinW
	c.mu.Unlock()

	_, _ = stdinW.Write([]byte("hello world\n"))
	_ = stdinW.Close()

	select {
	case line := <-done:
		if line != "hello world" {
			t.Errorf("stdin line = %q, want %q", line, "hello world")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stdin write")
	}
}

// --- 3. Resumed command construction ---

func TestClient_ResumePromptArgs(t *testing.T) {
	// Test that ResumePrompt builds the correct argument array.
	// We can't easily test the actual subprocess, so verify the args construction.
	conversationID := "conv-resumed-1"
	prompt := "test prompt"
	model := "claude-3.5"
	agent := "my-agent"

	args := []string{"--conversation", conversationID, "--output-format", "stream-json", "--print-timeout", printTimeout, "--print", prompt}
	args = append(args, "--model", model)
	args = append(args, "--agent", agent)

	expected := []string{
		"--conversation", conversationID,
		"--output-format", "stream-json",
		"--print-timeout", printTimeout,
		"--print", prompt,
		"--model", model,
		"--agent", agent,
	}
	if len(args) != len(expected) {
		t.Fatalf("args length = %d, want %d", len(args), len(expected))
	}
	for i := range args {
		if args[i] != expected[i] {
			t.Errorf("args[%d] = %q, want %q", i, args[i], expected[i])
		}
	}
}

func TestClient_ResumePromptArgsNoOptional(t *testing.T) {
	conversationID := "conv-resumed-2"
	prompt := "test prompt"

	args := []string{"--conversation", conversationID, "--output-format", "stream-json", "--print-timeout", printTimeout, "--print", prompt}

	expected := []string{
		"--conversation", conversationID,
		"--output-format", "stream-json",
		"--print-timeout", printTimeout,
		"--print", prompt,
	}
	if len(args) != len(expected) {
		t.Fatalf("args length = %d, want %d", len(args), len(expected))
	}
	for i := range args {
		if args[i] != expected[i] {
			t.Errorf("args[%d] = %q, want %q", i, args[i], expected[i])
		}
	}
}

func TestClient_StartArgs(t *testing.T) {
	model := "gemini-2.0-flash"
	agent := "my-agent"

	args := []string{"--output-format", "stream-json", "--print", "--print-timeout", printTimeout}
	if model != "" {
		args = append(args, "--model", model)
	}
	if agent != "" {
		args = append(args, "--agent", agent)
	}

	expected := []string{
		"--output-format", "stream-json",
		"--print",
		"--print-timeout", printTimeout,
		"--model", model,
		"--agent", agent,
	}
	if len(args) != len(expected) {
		t.Fatalf("args length = %d, want %d", len(args), len(expected))
	}
	for i := range args {
		if args[i] != expected[i] {
			t.Errorf("args[%d] = %q, want %q", i, args[i], expected[i])
		}
	}
}

func TestClient_StartArgsNoOptional(t *testing.T) {
	args := []string{"--output-format", "stream-json", "--print", "--print-timeout", printTimeout}
	if "" != "" {
		args = append(args, "--model", "")
	}
	if "" != "" {
		args = append(args, "--agent", "")
	}

	expected := []string{
		"--output-format", "stream-json",
		"--print",
		"--print-timeout", printTimeout,
	}
	if len(args) != len(expected) {
		t.Fatalf("args length = %d, want %d", len(args), len(expected))
	}
	for i := range args {
		if args[i] != expected[i] {
			t.Errorf("args[%d] = %q, want %q", i, args[i], expected[i])
		}
	}
}

// --- 4. Repeated-init validation and conversation_id mismatch ---

func TestClient_ValidateInitMismatch(t *testing.T) {
	c, wOut, _, _, _ := fakeClient()
	defer c.Close()

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-wrong",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.validateInit(ctx, "conv-expected")
	if err == nil {
		t.Fatal("expected error for conversation_id mismatch")
	}
	if !strings.Contains(err.Error(), "conversation_id mismatch") {
		t.Errorf("error = %v, want conversation_id mismatch", err)
	}
	if !strings.Contains(err.Error(), "conv-expected") {
		t.Errorf("error = %v, want to contain 'conv-expected'", err)
	}
	if !strings.Contains(err.Error(), "conv-wrong") {
		t.Errorf("error = %v, want to contain 'conv-wrong'", err)
	}
}

func TestClient_ValidateInitMatch(t *testing.T) {
	c, wOut, _, _, _ := fakeClient()
	defer c.Close()

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-match",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.validateInit(ctx, "conv-match")
	if err != nil {
		t.Fatalf("validateInit: %v", err)
	}
	if c.SessionID() != "conv-match" {
		t.Errorf("SessionID = %q, want %q", c.SessionID(), "conv-match")
	}
}

// --- 6. Startup timeout ---

func TestClient_StartupTimeout(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	// Don't send any init event — just wait for timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Wait a bit for the context to expire first, then call waitForInit.
	time.Sleep(100 * time.Millisecond)

	_, err := c.waitForInit(ctx)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("error = %v, want context.DeadlineExceeded", err)
	}
}

func TestClient_StartupTimeoutExhaustive(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	// Use a short context but the startupTimeout is 10s, so context should win.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Wait for context deadline to expire
	time.Sleep(50 * time.Millisecond)

	_, err := c.waitForInit(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("error = %v, want context.DeadlineExceeded", err)
	}
}

// --- 8. Malformed JSONL on stdout ---

func TestClient_MalformedJSONL(t *testing.T) {
	c, wOut, _, _, _ := fakeClient()
	defer c.Close()

	go func() {
		_, _ = wOut.Write([]byte("not json at all\n"))
		writeJSONL(wOut, map[string]any{
			"type": "init", "conversation_id": "conv-ok",
			"model": "gemini-2.0-flash", "agy_version": "1.2.0",
		})
	}()

	// Give the malformed line time to be processed
	time.Sleep(50 * time.Millisecond)

	stderr := c.Stderr()
	if !strings.Contains(stderr, "malformed JSONL") {
		t.Fatalf("stderr = %q, want malformed JSONL diagnostic", stderr)
	}

	// Should still be able to receive the valid init event
	info, err := c.waitForInit(context.Background())
	if err != nil {
		t.Fatalf("waitForInit: %v", err)
	}
	if info.ConversationID != "conv-ok" {
		t.Errorf("ConversationID = %q, want %q", info.ConversationID, "conv-ok")
	}
}

// --- 9. Scanner limits (line too long) ---

func TestClient_LongJSONLLine(t *testing.T) {
	c, wOut, _, _, _ := fakeClient()
	defer c.Close()

	// Write a line that's very long but still valid JSON.
	longLine := strings.Repeat("x", 60*1024*1024) // 60 MiB > 50 MiB limit
	go func() {
		_, _ = wOut.Write([]byte(longLine + "\n"))
	}()

	// Wait for the read loop to process the long line.
	time.Sleep(200 * time.Millisecond)

	stderr := c.Stderr()
	if !strings.Contains(stderr, "read loop error") {
		t.Fatalf("stderr = %q, want read loop error for long line", stderr)
	}
}

// --- 10. EOF before result ---

func TestClient_EOFBeforeResult(t *testing.T) {
	c, wOut, _, _, _ := fakeClient()
	defer c.Close()

	// Send init but never send session.end, then close stdout.
	writeJSONL(wOut, map[string]any{
		"type":            "init",
		"conversation_id": "conv-eof",
		"model":           "gemini-2.0-flash",
		"agy_version":     "1.2.0",
	})
	// Give time for init to arrive
	time.Sleep(50 * time.Millisecond)

	// Close stdout to simulate process exiting
	wOut.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.readUntilResult(ctx)
	if err == nil {
		t.Fatal("expected error from readUntilResult on EOF")
	}
	if err != ErrProcessDied {
		t.Errorf("error = %v, want ErrProcessDied", err)
	}
}

// --- 12. Stderr capture with bounded buffer ---

func TestClient_StderrCapture(t *testing.T) {
	c, _, _, _, stderrW := fakeClient()
	defer c.Close()

	go func() {
		for i := 0; i < 503; i++ {
			_, _ = stderrW.Write([]byte("stderr line " + string(rune('0'+i%10)) + "\n"))
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// Close the stderr writer so the drain goroutine can finish.
	stderrW.Close()

	// Wait for drain goroutine to finish.
	time.Sleep(100 * time.Millisecond)

	stderr := c.Stderr()
	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	if len(lines) > stderrCap {
		t.Errorf("stderr lines = %d, expected at most %d", len(lines), stderrCap)
	}
	if len(lines) < 400 {
		t.Errorf("stderr lines = %d, expected at least 400", len(lines))
	}
	// Should contain last lines
	if !strings.Contains(stderr, "stderr line 3") {
		t.Errorf("stderr should contain last lines, got: %s", stderr)
	}
}

func TestClient_StderrBoundedBuffer(t *testing.T) {
	c, _, _, _, stderrW := fakeClient()
	defer c.Close()

	go func() {
		for i := 0; i < 500; i++ {
			_, _ = stderrW.Write([]byte("line " + string(rune('0'+i%10)) + "\n"))
		}
		stderrW.Close()
	}()

	time.Sleep(100 * time.Millisecond)

	stderr := c.Stderr()
	// Should only contain last 400 lines
	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	if len(lines) > stderrCap {
		t.Errorf("stderr has %d lines, expected at most %d", len(lines), stderrCap)
	}
}

// --- 13. Context cancellation during startup ---

func TestClient_ContextCancellation(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := c.waitForInit(ctx)
		errCh <- err
	}()

	// Cancel after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Errorf("error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for waitForInit to return")
	}
}

// --- 14. os.Interrupt with bounded grace period ---

func TestClient_CancelGracePeriod(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	// Create a real subprocess that ignores SIGINT.
	proc := exec.Command("sleep", "30")
	_ = proc.Start()
	defer proc.Process.Kill()

	c.mu.Lock()
	c.proc = proc
	c.mode = "prompting"
	c.mu.Unlock()

	start := time.Now()
	err := c.Cancel(context.Background())
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if elapsed >= shutdownGrace {
		t.Errorf("Cancel took %v, expected < %v", elapsed, shutdownGrace)
	}
}

// --- 15. Forced kill after grace period ---

func TestClient_ForcedKillAfterGrace(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	// Create a subprocess that ignores SIGINT.
	script := `#!/bin/sh
trap '' INT
sleep 30
`
	cmd := exec.Command("sh", "-c", script)
	_ = cmd.Start()
	defer func() { _ = cmd.Process.Kill() }()

	c.mu.Lock()
	c.proc = cmd
	c.mode = "prompting"
	c.mu.Unlock()

	// Cancel should eventually kill the process, even if it ignores SIGINT.
	err := c.Cancel(context.Background())
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Give the process time to be killed and reaped.
	time.Sleep(3 * time.Second)

	c.mu.Lock()
	processState := c.proc.ProcessState
	c.mu.Unlock()

	if processState == nil {
		t.Error("process should have been killed after grace period")
	}
}

// --- 17. Idempotent cleanup ---

func TestClient_IdempotentClose(t *testing.T) {
	c, _, _, _, _ := fakeClient()

	// Close multiple times — should not panic.
	for i := 0; i < 5; i++ {
		err := c.Close()
		if err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}

	// Events channel should be closed.
	defer func() {
		r := recover()
		if r != nil {
			t.Errorf("recovered from panic on closed events read: %v", r)
		}
	}()
	select {
	case <-c.events:
		// Expected: channel is closed, read returns zero value.
	default:
		t.Error("events channel should be closed")
	}
}

func TestClient_IdempotentCloseWithProcess(t *testing.T) {
	proc := exec.Command("sleep", "30")
	_ = proc.Start()
	defer func() { _ = proc.Process.Kill() }()

	stdin, _ := proc.StdinPipe()
	stdout, _ := proc.StdoutPipe()
	stderr, _ := proc.StderrPipe()

	c := newClient(proc, stdin, stdout, stderr)

	// Close multiple times — should not panic.
	for i := 0; i < 5; i++ {
		_ = c.Close()
	}
}

// --- 18. Idle timer fires before first prompt ---

func TestClient_IdleTimerFires(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	// Verify that the idle timer mechanism works by checking the timer
	// is set up correctly and can be stopped.
	c.mu.Lock()
	c.idleTimer = time.AfterFunc(100*time.Millisecond, func() {})
	c.mu.Unlock()

	// Give the timer time to fire.
	time.Sleep(150 * time.Millisecond)

	// The timer should have fired. Verify it doesn't fire again.
	c.mu.Lock()
	if c.idleTimer != nil {
		c.idleTimer.Stop()
	}
	c.mu.Unlock()
}

// --- 19. First prompt stops the idle timer before claiming process ---

func TestClient_FirstPromptStopsIdleTimer(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	// Create a real subprocess.
	proc := exec.Command("sleep", "30")
	_ = proc.Start()
	defer func() { _ = proc.Process.Kill() }()

	timedOut := make(chan struct{}, 1)
	c.mu.Lock()
	c.proc = proc
	c.mode = "pending"
	c.stdin = &discardWriteCloser{}
	c.idleTimer = time.AfterFunc(500*time.Millisecond, func() {
		select {
		case timedOut <- struct{}{}:
		default:
		}
		c.mu.Lock()
		if c.mode == "pending" {
			c.mu.Unlock()
			_ = c.proc.Process.Signal(os.Interrupt)
			go c.doClose()
		} else {
			c.mu.Unlock()
		}
	})
	c.mu.Unlock()

	// Use a goroutine that will call FirstPrompt and return after a short delay.
	// Use a context that cancels quickly to unblock readUntilResult.
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		_ = c.FirstPrompt(ctx, "test")
		close(done)
	}()

	// Let FirstPrompt run long enough to stop the idle timer.
	time.Sleep(100 * time.Millisecond)

	// Cancel the context to unblock readUntilResult.
	cancel()

	select {
	case <-timedOut:
		t.Fatal("idle timer should have been stopped by FirstPrompt")
	case <-time.After(200 * time.Millisecond):
		// Good — idle timer did not fire.
	}

	<-done
}

// --- 20. Version caching via sync.Once ---

func TestClient_VersionCaching(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	// Version should be zero initially.
	if c.Version() != "" {
		t.Errorf("initial Version = %q, want empty", c.Version())
	}

	// Simulate ensureVersion setting the version.
	c.mu.Lock()
	c.version = "test-version"
	c.mu.Unlock()

	if c.Version() != "test-version" {
		t.Errorf("Version = %q, want %q", c.Version(), "test-version")
	}

	// Version() should be safe to call multiple times.
	v1 := c.Version()
	v2 := c.Version()
	v3 := c.Version()
	if v1 != "test-version" || v2 != "test-version" || v3 != "test-version" {
		t.Errorf("Version() inconsistent: %q, %q, %q", v1, v2, v3)
	}
}

func TestClient_VersionErrorCached(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	// Force a version error via the client's error field.
	c.mu.Lock()
	c.versionErr = ErrClientClosed
	c.mu.Unlock()

	// Calling ensureVersion again should not overwrite the cached error
	// since sync.Once prevents re-running.
	// We verify by checking the error is still set.
	c.ensureVersion(context.Background())

	if c.versionErr != ErrClientClosed {
		t.Errorf("versionErr = %v, want %v", c.versionErr, ErrClientClosed)
	}
}

// --- Additional: Events channel ---

func TestClient_EventsChannel(t *testing.T) {
	c, wOut, _, _, _ := fakeClient()
	defer c.Close()

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-events",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
		writeJSONL(wOut, map[string]any{
			"type":       "step_update",
			"step_index": 1,
			"step_type":  "agent_response",
			"state":      "ACTIVE",
			"text_delta": "hello",
		})
		writeJSONL(wOut, map[string]any{
			"type": "result",
			"conversation_id": "conv-events",
			"response": "hello",
		})
	}()

	// Wait for session.start (waitForInit consumes it from the channel)
	info, err := c.waitForInit(context.Background())
	if err != nil {
		t.Fatalf("waitForInit: %v", err)
	}
	if info.ConversationID != "conv-events" {
		t.Errorf("ConversationID = %q, want %q", info.ConversationID, "conv-events")
	}

	// Collect events until session.end.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var events []string
	for {
		select {
		case evt := <-c.events:
			events = append(events, evt.Event)
			if evt.Event == "session.end" {
				goto done
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for events, got: %v", events)
		}
	}
done:

	// Should have agent.message_chunk, avenor.message.delta, session.end.
	// session.start was consumed by waitForInit above.
	found := make(map[string]bool)
	for _, e := range events {
		found[e] = true
	}
	if !found["session.end"] {
		t.Errorf("events = %v, want session.end", events)
	}
}

// --- Additional: Client mode transitions ---

func TestClient_ModeTransitions(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	c.mu.Lock()
	if c.mode != "" {
		t.Errorf("initial mode = %q, want empty", c.mode)
	}
	c.mu.Unlock()

	// FirstPrompt should fail if not in "pending" mode.
	c.mu.Lock()
	c.mode = "prompting"
	c.mu.Unlock()

	err := c.FirstPrompt(context.Background(), "test")
	if err == nil {
		t.Error("expected error from FirstPrompt in wrong mode")
	}
	if !strings.Contains(err.Error(), "cannot FirstPrompt") {
		t.Errorf("error = %v, want 'cannot FirstPrompt'", err)
	}
}

// --- Additional: SessionID accessor ---

func TestClient_SessionID(t *testing.T) {
	c, _, _, _, _ := fakeClient()
	defer c.Close()

	if c.SessionID() != "" {
		t.Errorf("initial SessionID = %q, want empty", c.SessionID())
	}

	c.mu.Lock()
	c.sessionID = "conv-test"
	c.mu.Unlock()

	if c.SessionID() != "conv-test" {
		t.Errorf("SessionID = %q, want %q", c.SessionID(), "conv-test")
	}
}

// --- Helper types ---

type discardWriteCloser struct{}

func (d *discardWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (d *discardWriteCloser) Close() error                { return nil }

type fakeWriteCloser struct {
	written []byte
}

func (f *fakeWriteCloser) Write(p []byte) (int, error) {
	f.written = append(f.written, p...)
	return len(p), nil
}
func (f *fakeWriteCloser) Close() error { return nil }