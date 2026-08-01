package pi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestCapabilities(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Backend != "pi" {
		t.Errorf("Backend = %q, want pi", caps.Backend)
	}
	if !caps.Permissions {
		t.Error("Permissions should be true")
	}
	if !caps.Resume {
		t.Error("Resume should be true")
	}
	if caps.ExternalServerURL {
		t.Error("ExternalServerURL should be false")
	}
	if caps.SubprocessDiscovery {
		t.Error("SubprocessDiscovery should be false")
	}
	if !caps.ModelSelection {
		t.Error("ModelSelection should be true")
	}
}

func TestResumeExisting(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})
	p.sessions["pi_existing"] = struct{}{}

	sess, err := p.Resume(context.Background(), "pi_existing")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if sess.SessionID != "pi_existing" {
		t.Errorf("SessionID = %q, want pi_existing", sess.SessionID)
	}
	if sess.Backend != "pi" {
		t.Errorf("Backend = %q", sess.Backend)
	}
	if sess.Dir != "/work" {
		t.Errorf("Dir = %q, want /work", sess.Dir)
	}
}

func TestResumeRejectsSecondDistinctSession(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})
	p.sessions["pi_existing"] = struct{}{}
	c, _, _ := fakeClient()
	defer c.Close()
	p.client = c

	_, err := p.Resume(context.Background(), "pi_other")
	if err == nil {
		t.Fatal("expected error for second pi session")
	}
	if !strings.Contains(err.Error(), "only one active session") {
		t.Fatalf("error = %v, want single-session validation", err)
	}
}

func TestResumeEmptyID(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	_, err := p.Resume(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty session id")
	}
}

func TestAnswerPermissionNotStarted(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	err := p.AnswerPermission(context.Background(), "ses", "req", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error for not-started provider")
	}
}

func TestAnswerPermissionEmptyRequestID(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, _, _ := fakeClient()
	defer c.Close()
	p.client = c
	err := p.AnswerPermission(context.Background(), "ses", "", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error for empty request id")
	}
	if !strings.Contains(err.Error(), "permission request id is required") {
		t.Fatalf("error = %v, want permission request id validation", err)
	}
}

func TestAnswerPermissionWritesExtensionUIResponse(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, _, rIn := fakeClient()
	defer c.Close()
	p.client = c
	c.mu.Lock()
	c.approvals["req-1"] = pendingApproval{
		id:        "req-1",
		method:    "select",
		rawID:     "ui-1",
		sessionID: "ses",
	}
	c.mu.Unlock()

	done := make(chan map[string]any, 1)
	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		done <- cmd
	}()

	err := p.AnswerPermission(context.Background(), "ses", "req-1", runtime.PermissionResponse{
		Allow:    true,
		OptionID: "Allow",
	})
	if err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}

	select {
	case cmd := <-done:
		if cmd["type"] != "extension_ui_response" {
			t.Errorf("type = %v, want extension_ui_response", cmd["type"])
		}
		if cmd["id"] != "ui-1" {
			t.Errorf("id = %v, want ui-1", cmd["id"])
		}
		if cmd["value"] != "Allow" {
			t.Errorf("value = %v, want Allow", cmd["value"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for extension UI response")
	}
}

func TestEventsNotStarted(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	_, err := p.Events(context.Background(), "ses")
	if err == nil {
		t.Fatal("expected error for not-started provider")
	}
}

func TestCancelNoSession(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	err := p.Cancel(context.Background(), "unknown")
	if err == nil {
		t.Fatal("expected error for unknown session")
	}
}

func TestCloseNotStarted(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	if err := p.Close(); err != nil {
		t.Fatalf("Close on nil client: %v", err)
	}
}

func TestAnswerPermissionDenied(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, _, rIn := fakeClient()
	defer c.Close()
	p.client = c
	c.mu.Lock()
	c.approvals["req-deny"] = pendingApproval{
		id:        "req-deny",
		method:    "select",
		rawID:     "ui-deny",
		sessionID: "ses",
	}
	c.mu.Unlock()

	done := make(chan map[string]any, 1)
	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		done <- cmd
	}()

	err := p.AnswerPermission(context.Background(), "ses", "req-deny", runtime.PermissionResponse{
		Allow: false,
	})
	if err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}

	select {
	case cmd := <-done:
		if cmd["type"] != "extension_ui_response" {
			t.Errorf("type = %v, want extension_ui_response", cmd["type"])
		}
		if cmd["cancelled"] != true {
			t.Errorf("cancelled = %v, want true", cmd["cancelled"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for denied extension UI response")
	}
}

func TestFreshStartUsesThinkingFlagWithoutSetter(t *testing.T) {
	originalHelp := piHelpOutput
	piHelpOutput = func(context.Context) ([]byte, error) { return []byte("--thinking <level>"), nil }
	t.Cleanup(func() { piHelpOutput = originalHelp })

	for _, level := range []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"} {
		t.Run(level, func(t *testing.T) {
			p := NewWithOptions(runtime.StartOptions{})
			var launched runtime.StartOptions
			c, wOut, rIn := fakeClient()
			p.startClient = func(_ context.Context, opts runtime.StartOptions) (*client, error) {
				launched = opts
				return c, nil
			}
			defer p.Close()
			first := make(chan string, 1)
			go func() {
				command, _ := readCommand(rIn)
				first <- command["type"].(string)
				writeLine(wOut, map[string]any{"type": "response", "id": command["id"], "success": true, "sessionId": "pi-fresh"})
			}()
			if _, err := p.Start(context.Background(), runtime.StartOptions{Thinking: level}); err != nil {
				t.Fatalf("Start: %v", err)
			}
			if launched.Thinking != level {
				t.Fatalf("launch thinking = %q", launched.Thinking)
			}
			if command := <-first; command != "get_state" {
				t.Fatalf("first command = %q, want get_state (no setter on fresh client)", command)
			}
		})
	}
}

func TestProviderRejectsInvalidThinkingBeforeClient(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	launched := false
	p.startClient = func(context.Context, runtime.StartOptions) (*client, error) {
		launched = true
		return nil, errors.New("unexpected launch")
	}
	_, err := p.Start(context.Background(), runtime.StartOptions{Thinking: "HIGH"})
	if err == nil || !strings.Contains(err.Error(), "invalid thinking value") {
		t.Fatalf("error = %v", err)
	}
	if launched {
		t.Fatal("client launched after invalid thinking")
	}
}

func TestReusedStartSetsThinkingBeforeGetState(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, wOut, rIn := fakeClient()
	defer c.Close()
	p.client = c
	commands := make(chan []string, 1)
	go func() {
		var got []string
		for i := 0; i < 2; i++ {
			command, _ := readCommand(rIn)
			got = append(got, command["type"].(string))
			response := map[string]any{"type": "response", "id": command["id"], "success": true}
			if command["type"] == "get_state" {
				response["sessionId"] = "pi-reused"
			}
			writeLine(wOut, response)
		}
		commands <- got
	}()
	if _, err := p.Start(context.Background(), runtime.StartOptions{Thinking: "high"}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	got := <-commands
	if strings.Join(got, ",") != "set_thinking_level,get_state" {
		t.Fatalf("commands = %v", got)
	}
}

func TestReusedThinkingSetterFailureStopsBeforeState(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, wOut, rIn := fakeClient()
	defer c.Close()
	p.client = c
	go func() {
		command, _ := readCommand(rIn)
		writeLine(wOut, map[string]any{"type": "response", "id": command["id"], "success": false, "error": "level rejected"})
	}()
	_, err := p.Start(context.Background(), runtime.StartOptions{Thinking: "max"})
	if err == nil || !strings.Contains(err.Error(), `pi set_thinking_level "max" rejected: level rejected`) || strings.Contains(err.Error(), "does not support parameter") {
		t.Fatalf("error = %v", err)
	}
	p.mu.Lock()
	registered := len(p.sessions)
	p.mu.Unlock()
	if registered != 0 {
		t.Fatalf("registered sessions = %d", registered)
	}
}

func TestPlainResumeExistingSessionSendsNoThinkingSetter(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Thinking: "high"})
	c, _, _ := fakeClient()
	defer c.Close()
	p.client = c
	p.sessions["pi-existing"] = struct{}{}
	done := make(chan error, 1)
	go func() {
		_, err := p.Resume(context.Background(), "pi-existing")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Resume: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("plain Resume blocked sending a thinking setter")
	}
}

func TestReusedResumeThinkingSetterFailure(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, wOut, rIn := fakeClient()
	defer c.Close()
	p.client = c
	p.sessions["pi-existing"] = struct{}{}
	go func() {
		command, _ := readCommand(rIn)
		writeLine(wOut, map[string]any{"type": "response", "id": command["id"], "success": false, "error": "level rejected"})
	}()
	_, err := p.ResumeWithOptions(context.Background(), "pi-existing", runtime.StartOptions{Thinking: "max"})
	if err == nil || !strings.Contains(err.Error(), `pi set_thinking_level "max" rejected: level rejected`) || strings.Contains(err.Error(), "does not support parameter") {
		t.Fatalf("error = %v", err)
	}
}

type failingWriteCloser struct{}

func (failingWriteCloser) Write([]byte) (int, error) { return 0, errors.New("transport unavailable") }
func (failingWriteCloser) Close() error              { return nil }

func TestThinkingSetterTransportAndDecodeErrorsAreNotCapabilityErrors(t *testing.T) {
	t.Run("transport", func(t *testing.T) {
		c := &client{stdin: failingWriteCloser{}, pending: map[string]chan json.RawMessage{}, done: make(chan struct{})}
		err := NewWithOptions(runtime.StartOptions{}).setThinkingLevel(context.Background(), c, "high")
		if err == nil || !strings.Contains(err.Error(), `pi set_thinking_level "high":`) || strings.Contains(err.Error(), "does not support parameter") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("decode", func(t *testing.T) {
		c, wOut, rIn := fakeClient()
		defer c.Close()
		go func() {
			command, _ := readCommand(rIn)
			writeLine(wOut, map[string]any{"type": "response", "id": command["id"], "success": "invalid"})
		}()
		err := NewWithOptions(runtime.StartOptions{}).setThinkingLevel(context.Background(), c, "high")
		if err == nil || !strings.Contains(err.Error(), `decode pi set_thinking_level "high" response:`) || strings.Contains(err.Error(), "does not support parameter") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestFreshResumeUsesStartupThinkingWithoutSetter(t *testing.T) {
	originalHelp := piHelpOutput
	piHelpOutput = func(context.Context) ([]byte, error) { return []byte("--thinking <level>"), nil }
	t.Cleanup(func() { piHelpOutput = originalHelp })
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})
	c, _, _ := fakeClient()
	var launched runtime.StartOptions
	p.startClient = func(_ context.Context, opts runtime.StartOptions) (*client, error) {
		launched = opts
		return c, nil
	}
	defer p.Close()
	if _, err := p.ResumeWithOptions(context.Background(), "pi-resume", runtime.StartOptions{Thinking: "xhigh"}); err != nil {
		t.Fatalf("ResumeWithOptions: %v", err)
	}
	if launched.Thinking != "xhigh" {
		t.Fatalf("launch thinking = %q", launched.Thinking)
	}
}

func TestThinkingHelpMismatchRejectsBeforeLaunch(t *testing.T) {
	originalHelp := piHelpOutput
	piHelpOutput = func(context.Context) ([]byte, error) { return []byte("usage: pi"), nil }
	t.Cleanup(func() { piHelpOutput = originalHelp })
	p := NewWithOptions(runtime.StartOptions{})
	launched := false
	p.startClient = func(context.Context, runtime.StartOptions) (*client, error) {
		launched = true
		return nil, errors.New("unexpected launch")
	}
	_, err := p.Start(context.Background(), runtime.StartOptions{Thinking: "low"})
	if err == nil || !strings.Contains(err.Error(), "thinking") || !strings.Contains(err.Error(), "pi") {
		t.Fatalf("error = %v", err)
	}
	if launched {
		t.Fatal("client launched after failed help capability check")
	}
}

func TestProviderImplementsInterface(t *testing.T) {
	var _ runtime.Provider = (*Provider)(nil)
}

func TestPiProviderHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_PI_PROVIDER_HELPER") != "1" {
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var command map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &command); err != nil {
			os.Exit(2)
		}
		if err := encoder.Encode(map[string]any{
			"type":      "response",
			"id":        command["id"],
			"sessionId": "pi-test-session",
		}); err != nil {
			os.Exit(3)
		}
	}
	os.Exit(0)
}

func withFakePiCommand(t *testing.T) func() *exec.Cmd {
	t.Helper()
	original := piExecCommandContext
	t.Cleanup(func() { piExecCommandContext = original })

	var command *exec.Cmd
	piExecCommandContext = func(_ context.Context, _ string, _ ...string) *exec.Cmd {
		command = exec.Command(os.Args[0], "-test.run=^TestPiProviderHelperProcess$")
		command.Env = append(os.Environ(), "GO_WANT_PI_PROVIDER_HELPER=1")
		return command
	}
	return func() *exec.Cmd { return command }
}

func TestProviderStartUsesRequestedWorkingDirectory(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider runtime.StartOptions
		start    runtime.StartOptions
	}{
		{
			name:     "start option",
			provider: runtime.StartOptions{Dir: t.TempDir()},
			start:    runtime.StartOptions{Dir: t.TempDir()},
		},
		{
			name:     "provider fallback",
			provider: runtime.StartOptions{Dir: t.TempDir()},
			start:    runtime.StartOptions{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			captured := withFakePiCommand(t)
			p := NewWithOptions(tc.provider)
			defer p.Close()

			sess, err := p.Start(context.Background(), tc.start)
			if err != nil {
				t.Fatalf("Start: %v", err)
			}

			wantDir := tc.start.Dir
			if wantDir == "" {
				wantDir = tc.provider.Dir
			}
			if sess.Dir != wantDir {
				t.Errorf("session dir = %q, want %q", sess.Dir, wantDir)
			}
			if captured() == nil {
				t.Fatal("Pi command was not created")
			}
			if captured().Dir != wantDir {
				t.Errorf("Pi command cwd = %q, want %q", captured().Dir, wantDir)
			}
		})
	}
}

func TestProviderResumeUsesProviderWorkingDirectory(t *testing.T) {
	dir := t.TempDir()
	captured := withFakePiCommand(t)
	p := NewWithOptions(runtime.StartOptions{Dir: dir})
	defer p.Close()

	sess, err := p.Resume(context.Background(), "pi-resumed")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if sess.Dir != dir {
		t.Errorf("session dir = %q, want %q", sess.Dir, dir)
	}
	if captured() == nil {
		t.Fatal("Pi command was not created")
	}
	if captured().Dir != dir {
		t.Errorf("Pi command cwd = %q, want %q", captured().Dir, dir)
	}
}

func TestProviderStartParsesDataSessionID(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})
	c, wOut, rIn := fakeClient()
	defer c.Close()
	p.client = c

	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		id, _ := cmd["id"].(string)
		writeLine(wOut, map[string]any{
			"type": "response",
			"id":   id,
			"data": map[string]any{
				"sessionId": "pi-from-data",
			},
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{Dir: "/work"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if sess.SessionID != "pi-from-data" {
		t.Errorf("SessionID = %q, want pi-from-data", sess.SessionID)
	}
}

func TestAnswerPermissionWrongSession(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, _, _ := fakeClient()
	defer c.Close()
	p.client = c
	c.mu.Lock()
	c.approvals["req-1"] = pendingApproval{
		id:        "req-1",
		method:    "select",
		rawID:     "ui-1",
		sessionID: "other-ses",
	}
	c.mu.Unlock()

	err := p.AnswerPermission(context.Background(), "ses", "req-1", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error for wrong session")
	}
	if !strings.Contains(err.Error(), "belongs to session") {
		t.Fatalf("error = %v, want session mismatch", err)
	}
}

func TestAnswerPermissionNotApproved(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, _, _ := fakeClient()
	defer c.Close()
	p.client = c

	err := p.AnswerPermission(context.Background(), "ses", "req-missing", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error for unknown approval")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error = %v, want not found", err)
	}
}
