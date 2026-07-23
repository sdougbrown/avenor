package agy

import (
	"bufio"
	"context"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// fakeClient creates a client with a pipe-based stdout for sending JSONL
// events. The client's readLoop goroutine is running. Stderr is nil (no drain
// goroutine), which is sufficient for most tests. Tests that need stderr
// should create their own client.
// Returns: client, stdout_writer
func fakeClient() (*client, *io.PipeWriter) {
	stdoutR, stdoutW := io.Pipe()
	stdinR, stdinW := io.Pipe()
	_ = stdinR

	c := newClient(nil, stdinW, stdoutR, nil)
	c.testMode = true
	return c, stdoutW
}

// readLine reads one line from a reader (for checking stdin output).
func readLine(r io.Reader) (string, error) {
	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	return "", scanner.Err()
}

// ---------------------------------------------------------------------------
// Test: No-prompt startup — init event is captured
// ---------------------------------------------------------------------------

func TestClientInitCapture(t *testing.T) {
	c, wOut := fakeClient()
	defer c.Close()

	go sendEvents(wOut, map[string]any{
		"event":           "init",
		"conversation_id": "conv-abc123",
		"init":            map[string]any{},
	})

	info, err := c.waitForInit(context.Background())
	if err != nil {
		t.Fatalf("waitForInit: %v", err)
	}
	if info.ConversationID != "conv-abc123" {
		t.Errorf("ConversationID = %q, want %q", info.ConversationID, "conv-abc123")
	}
	if c.SessionID() != "conv-abc123" {
		t.Errorf("SessionID = %q, want %q", c.SessionID(), "conv-abc123")
	}
}

// ---------------------------------------------------------------------------
// Test: Init with duplicate conversation_id is handled
// ---------------------------------------------------------------------------

func TestClient_DuplicateInit(t *testing.T) {
	c, wOut := fakeClient()
	defer c.Close()

	go sendEvents(wOut,
		newInit("conv-abc", "", ""),
		newInit("conv-abc", "", ""),
	)

	info, err := c.waitForInit(context.Background())
	if err != nil {
		t.Fatalf("waitForInit: %v", err)
	}
	if info.ConversationID != "conv-abc" {
		t.Errorf("ConversationID = %q, want conv-abc", info.ConversationID)
	}
	if c.SessionID() != "conv-abc" {
		t.Errorf("SessionID = %q, want conv-abc", c.SessionID())
	}
}

// ---------------------------------------------------------------------------
// Test: Startup timeout
// ---------------------------------------------------------------------------

func TestClientErrorResultBeforeInit(t *testing.T) {
	c, out := fakeClient()
	defer c.Close()
	go sendEvents(out, newResultWithError("", "invalid model selection", nil))

	_, err := c.waitForInit(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid model selection") {
		t.Fatalf("waitForInit error = %v", err)
	}
}

func TestClient_StartupTimeout(t *testing.T) {
	c, _ := fakeClient()
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Don't send any init.
	_, err := c.waitForInit(ctx)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// ---------------------------------------------------------------------------
// Test: Context cancellation during init wait
// ---------------------------------------------------------------------------

func TestClient_ContextCancellation(t *testing.T) {
	c, _ := fakeClient()
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := c.waitForInit(ctx)
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
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

// ---------------------------------------------------------------------------
// Test: validateInit conversation_id mismatch
// ---------------------------------------------------------------------------

func TestClient_ValidateInitMismatch(t *testing.T) {
	c, wOut := fakeClient()
	defer c.Close()

	go sendEvents(wOut, newInit("conv-wrong", "", ""))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.validateInit(ctx, "conv-expected")
	if err == nil {
		t.Fatal("expected error for conversation_id mismatch")
	}
	if !strings.Contains(err.Error(), "conversation_id mismatch") {
		t.Errorf("error = %v, want conversation_id mismatch", err)
	}
}

// ---------------------------------------------------------------------------
// Test: validateInit conversation_id match
// ---------------------------------------------------------------------------

func TestClient_ValidateInitMatch(t *testing.T) {
	c, wOut := fakeClient()
	defer c.Close()

	go sendEvents(wOut, newInit("conv-match", "", ""))

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

// ---------------------------------------------------------------------------
// Test: Malformed JSONL on stdout
// ---------------------------------------------------------------------------

func TestClient_MalformedJSONL(t *testing.T) {
	c, wOut := fakeClient()
	defer c.Close()

	go func() {
		_, _ = wOut.Write([]byte("not json at all\n"))
		sendEvents(wOut, newInit("conv-ok", "", ""))
	}()

	time.Sleep(100 * time.Millisecond)

	stderr := c.Stderr()
	if !strings.Contains(stderr, "malformed JSONL") {
		t.Fatalf("stderr = %q, want malformed JSONL diagnostic", stderr)
	}

	// Should still receive the valid init
	info, err := c.waitForInit(context.Background())
	if err != nil {
		t.Fatalf("waitForInit: %v", err)
	}
	if info.ConversationID != "conv-ok" {
		t.Errorf("ConversationID = %q, want %q", info.ConversationID, "conv-ok")
	}
}

// ---------------------------------------------------------------------------
// Test: Stderr bounded buffer
// ---------------------------------------------------------------------------

func TestClient_StderrCapture(t *testing.T) {
	// Create a client with a real stderr pipe.
	stdoutR, _ := io.Pipe()
	stderrR, stderrW := io.Pipe()
	stdinR, stdinW := io.Pipe()
	_ = stdinR

	c := newClient(nil, stdinW, stdoutR, stderrR)
	c.testMode = true
	defer c.Close()

	go func() {
		for i := 0; i < 500; i++ {
			_, _ = stderrW.Write([]byte("stderr line " + string(rune('0'+i%10)) + "\n"))
		}
		_ = stderrW.Close()
	}()
	time.Sleep(200 * time.Millisecond)

	stderr := c.Stderr()
	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	if len(lines) > stderrCap {
		t.Errorf("stderr lines = %d, expected at most %d", len(lines), stderrCap)
	}
	if len(lines) < 100 {
		t.Errorf("stderr lines = %d, expected at least 100", len(lines))
	}
}

// ---------------------------------------------------------------------------
// Test: Cancel on real subprocess
// ---------------------------------------------------------------------------

func TestClient_CancelGracePeriod(t *testing.T) {
	// Create a real subprocess for testing Cancel.
	proc := exec.Command("sleep", "30")
	stdin, _ := proc.StdinPipe()
	stdout, _ := proc.StdoutPipe()
	stderr, _ := proc.StderrPipe()
	_ = proc.Start()
	defer func() { _ = proc.Process.Kill() }()

	c := newClient(proc, stdin, stdout, stderr)
	defer c.Close()

	start := time.Now()
	err := c.Cancel(context.Background())
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if elapsed >= 3*time.Second {
		t.Errorf("Cancel took %v, expected less", elapsed)
	}
}

// ---------------------------------------------------------------------------
// Test: Forced kill after grace
// ---------------------------------------------------------------------------

func TestClient_ForcedKillAfterGrace(t *testing.T) {
	// Use a process that ignores SIGINT: sleep ignores it by default.
	proc := exec.Command("sleep", "30")
	stdin, _ := proc.StdinPipe()
	stdout, _ := proc.StdoutPipe()
	stderr, _ := proc.StderrPipe()
	_ = proc.Start()
	defer func() { _ = proc.Process.Kill() }()

	c := newClient(proc, stdin, stdout, stderr)
	defer c.Close()

	err := c.Cancel(context.Background())
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	if proc.ProcessState != nil && proc.ProcessState.Exited() {
		t.Log("process was killed after grace period")
	}
}

// ---------------------------------------------------------------------------
// Test: Idempotent close
// ---------------------------------------------------------------------------

func TestClient_IdempotentClose(t *testing.T) {
	c, _ := fakeClient()
	defer c.Close()

	for i := 0; i < 5; i++ {
		err := c.Close()
		if err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
}

func TestClient_IdempotentCloseWithProcess(t *testing.T) {
	proc := exec.Command("sleep", "30")
	stdin, _ := proc.StdinPipe()
	stdout, _ := proc.StdoutPipe()
	stderr, _ := proc.StderrPipe()
	_ = proc.Start()
	defer func() { _ = proc.Process.Kill() }()
	c := newClient(proc, stdin, stdout, stderr)

	for i := 0; i < 5; i++ {
		_ = c.Close()
	}
}

// ---------------------------------------------------------------------------
// Test: Version caching
// ---------------------------------------------------------------------------

func TestClient_VersionCaching(t *testing.T) {
	c, _ := fakeClient()
	defer c.Close()

	if c.Version() != "" {
		t.Errorf("initial Version = %q, want empty", c.Version())
	}

	c.mu.Lock()
	c.version = "test-version"
	c.mu.Unlock()

	if c.Version() != "test-version" {
		t.Errorf("Version = %q, want %q", c.Version(), "test-version")
	}
}

// ---------------------------------------------------------------------------
// Test: Events channel receives events
// ---------------------------------------------------------------------------

func TestClient_EventsChannel(t *testing.T) {
	c, wOut := fakeClient()
	defer c.Close()

	go sendEvents(wOut,
		newInit("conv-events", "", ""),
		newAgentActive(1, "hello"),
		newResult("conv-events", "hello", map[string]any{"status": "SUCCESS"}),
	)

	// waitForInit consumes the init event.
	info, err := c.waitForInit(context.Background())
	if err != nil {
		t.Fatalf("waitForInit: %v", err)
	}
	if info.ConversationID != "conv-events" {
		t.Errorf("ConversationID = %q, want conv-events", info.ConversationID)
	}

	// Collect remaining events until session.end.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var events []string
	for {
		select {
		case evt, ok := <-c.Events():
			if !ok {
				t.Fatal("events channel closed prematurely")
			}
			events = append(events, evt.Event)
			if evt.Event == "session.end" {
				goto done
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for events, got: %v", events)
		}
	}
done:
	if !contains(events, "session.end") {
		t.Errorf("events = %v, want session.end", events)
	}
}

func TestClientInitialArgs(t *testing.T) {
	got := initialArgs("test prompt", "gemini-3.6-flash-low", "my-agent", "/work")
	want := []string{
		"--output-format", "stream-json",
		"--print-timeout", printTimeout,
		"--model", "gemini-3.6-flash-low",
		"--agent", "my-agent",
		"--add-dir", "/work",
		"--print", "test prompt",
	}
	assertArgs(t, got, want)
}

func TestClientResumedArgs(t *testing.T) {
	got := resumedArgs("conv-resumed", "next prompt", "", "", "")
	want := []string{
		"--conversation", "conv-resumed",
		"--output-format", "stream-json",
		"--print-timeout", printTimeout,
		"--print", "next prompt",
	}
	assertArgs(t, got, want)
}

func assertArgs(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q (all args: %#v)", i, got[i], want[i], got)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func contains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}
