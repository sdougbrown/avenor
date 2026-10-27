package pi

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
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
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
	if !strings.Contains(err.Error(), "session not found") {
		t.Fatalf("error = %v, want session-not-found refusal", err)
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

func TestProviderStartBuildsBrokerURLForChild(t *testing.T) {
	b := broker.New("url-test-token")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer b.Stop()
	token, _, _ := b.EnsureRun("rt_1", "")

	captured := withFakePiCommand(t)
	p := NewWithOptions(runtime.StartOptions{Broker: b, RuntimeID: "rt_1", BrokerToken: token})
	defer p.Close()

	if _, err := p.Start(context.Background(), runtime.StartOptions{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cmd := captured()
	if cmd == nil {
		t.Fatal("Pi command was not created")
	}
	env := make(map[string]string)
	for _, entry := range cmd.Env {
		if k, v, ok := strings.Cut(entry, "="); ok {
			env[k] = v
		}
	}
	if want := "http://" + b.Addr(); env["AVENOR_BROKER_URL"] != want {
		t.Errorf("AVENOR_BROKER_URL = %q, want %q", env["AVENOR_BROKER_URL"], want)
	}
	if env["AVENOR_RUN_ID"] != "rt_1" {
		t.Errorf("AVENOR_RUN_ID = %q, want rt_1", env["AVENOR_RUN_ID"])
	}
	if env["AVENOR_BROKER_TOKEN"] != token {
		t.Errorf("AVENOR_BROKER_TOKEN = %q, want %q", env["AVENOR_BROKER_TOKEN"], token)
	}
}

func TestProviderStartOmitsBrokerURLWithoutToken(t *testing.T) {
	b := broker.New("url-test-token")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer b.Stop()

	captured := withFakePiCommand(t)
	// Broker and runtime ID without a token must not half-configure the child.
	p := NewWithOptions(runtime.StartOptions{Broker: b, RuntimeID: "rt_1"})
	defer p.Close()

	if _, err := p.Start(context.Background(), runtime.StartOptions{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cmd := captured()
	if cmd == nil {
		t.Fatal("Pi command was not created")
	}
	if cmd.Env == nil {
		t.Fatal("pi command env was not assigned; child would inherit the parent env")
	}
	for _, entry := range cmd.Env {
		for _, key := range []string{"AVENOR_BROKER_URL", "AVENOR_RUN_ID", "AVENOR_BROKER_TOKEN"} {
			if strings.HasPrefix(entry, key+"=") {
				t.Errorf("%s set without a broker token: %q", key, entry)
			}
		}
	}
}

func TestProviderStartOmitsBrokerConfigWithoutListener(t *testing.T) {
	// A broker that never started has no address, so the child gets no broker
	// configuration at all.
	b := broker.New("url-test-token")
	captured := withFakePiCommand(t)
	p := NewWithOptions(runtime.StartOptions{Broker: b, RuntimeID: "rt_1", BrokerToken: "tok"})
	defer p.Close()

	if _, err := p.Start(context.Background(), runtime.StartOptions{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cmd := captured()
	if cmd == nil {
		t.Fatal("Pi command was not created")
	}
	for _, entry := range cmd.Env {
		for _, key := range []string{"AVENOR_BROKER_URL", "AVENOR_RUN_ID", "AVENOR_BROKER_TOKEN"} {
			if strings.HasPrefix(entry, key+"=") {
				t.Errorf("%s set for a broker with no listener: %q", key, entry)
			}
		}
	}
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

func TestProviderColdResumeRefusesBeforeStartingPi(t *testing.T) {
	dir := t.TempDir()
	captured := withFakePiCommand(t)
	p := NewWithOptions(runtime.StartOptions{Dir: dir})
	defer p.Close()

	_, err := p.Resume(context.Background(), "pi-resumed")
	if err == nil || !strings.Contains(err.Error(), "cross-process resume") {
		t.Fatalf("error = %v, want cross-process resume refusal", err)
	}
	if captured() != nil {
		t.Fatal("cold resume must not create a pi command")
	}

	// The thinking variant must refuse at the same point, before any
	// capability probing or client launch.
	if _, err := p.ResumeWithOptions(context.Background(), "pi-resumed", runtime.StartOptions{Thinking: "xhigh"}); err == nil || !strings.Contains(err.Error(), "cross-process resume") {
		t.Fatalf("error = %v, want cross-process resume refusal for thinking resume", err)
	}
	if captured() != nil {
		t.Fatal("cold resume with thinking must not create a pi command")
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

func TestPromptTurnErrorSurfacesStderrDetail(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, wOut, rIn := fakeClient()
	defer c.Close()
	c.setSessionID("ses-pi-err")
	p.client = c
	p.sessions = map[string]struct{}{"ses-pi-err": {}}

	errCh := make(chan error, 1)
	go func() { errCh <- p.Prompt(context.Background(), "ses-pi-err", "hi") }()

	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		c.stderr.Append("model 429: rate limited")
		writeLine(wOut, map[string]any{"type": "response", "id": cmd["id"], "success": true})
		writeLine(wOut, map[string]any{"type": "agent_end", "stopReason": "error"})
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for prompt result")
	}
	if err == nil {
		t.Fatal("expected a turn error")
	}
	if !strings.Contains(err.Error(), "model 429: rate limited") {
		t.Fatalf("error = %v; want the stderr detail in the message", err)
	}
	if strings.Contains(err.Error(), "unknown error") {
		t.Fatalf("error = %v; want the captured stderr as the cause, not unknown error", err)
	}
}

// A pi child that exits before the caller's deadline reports what it printed.
func promptOnClient(c *client, sessionID string) *Provider {
	p := NewWithOptions(runtime.StartOptions{})
	p.client = c
	p.sessions = map[string]struct{}{sessionID: {}}
	return p
}

func TestPromptHandlesClientExitBeforeDeadline(t *testing.T) {
	tests := []struct {
		name   string
		settle bool
		want   string // empty: the settled turn is the result
	}{
		{name: "settled turn", settle: true},
		{name: "unsettled turn reports the child's cause", want: "authentication failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, wOut, rIn := fakeClient()
			defer c.Close()
			c.setSessionID("ses-pi-exit")
			p := promptOnClient(c, "ses-pi-exit")

			errCh := make(chan error, 1)
			go func() { errCh <- p.Prompt(context.Background(), "ses-pi-exit", "hi") }()

			go func() {
				cmd, err := readCommand(rIn)
				if err != nil {
					return
				}
				c.stderr.Append("pi: authentication failed")
				writeLine(wOut, map[string]any{"type": "response", "id": cmd["id"], "success": true})
				if tt.settle {
					writeLine(wOut, map[string]any{"type": "agent_end", "stopReason": "stop"})
				}
				_ = wOut.Close()
			}()

			select {
			case err := <-errCh:
				switch {
				case tt.want == "" && err != nil:
					t.Fatalf("Prompt = %v, want the settled turn", err)
				case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
					t.Fatalf("Prompt = %v, want the child's cause %q", err, tt.want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Prompt did not return after the pi child exited")
			}
		})
	}
}

// The rolling stderr buffer is process-lifetime: a stale limit line from an
// earlier turn must not describe a later, unrelated failure.
func TestPromptStderrIsPerTurn(t *testing.T) {
	c, wOut, rIn := fakeClient()
	defer c.Close()
	c.setSessionID("ses-pi-turns")
	p := promptOnClient(c, "ses-pi-turns")

	go func() {
		first, err := readCommand(rIn)
		if err != nil {
			return
		}
		c.stderr.Append("model 429: rate limited")
		writeLine(wOut, map[string]any{"type": "response", "id": first["id"], "success": true})
		writeLine(wOut, map[string]any{"type": "agent_end", "stopReason": "error", "errorMessage": "429: rate limited"})

		second, err := readCommand(rIn)
		if err != nil {
			return
		}
		writeLine(wOut, map[string]any{"type": "response", "id": second["id"], "success": true})
		writeLine(wOut, map[string]any{"type": "agent_end", "stopReason": "error", "errorMessage": "400: invalid model name"})
	}()

	if err := p.Prompt(context.Background(), "ses-pi-turns", "first"); err == nil || !strings.Contains(err.Error(), "429") {
		t.Fatalf("first Prompt = %v, want the rate limit", err)
	}
	if err := p.Prompt(context.Background(), "ses-pi-turns", "second"); err == nil || strings.Contains(err.Error(), "429") {
		t.Fatalf("second Prompt = %v, want only the current turn's cause", err)
	}
}

// The child's last stderr lines reach the exit cause through the real pipe.
func TestPromptExitCauseWaitsForStderrDrain(t *testing.T) {
	clientIn, clientInW := io.Pipe()
	clientOut, clientOutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	c := newClient(nil, clientInW, clientOut, stderrR)
	defer c.Close()
	c.setSessionID("ses_pi_pipe")
	p := promptOnClient(c, "ses_pi_pipe")

	errCh := make(chan error, 1)
	go func() { errCh <- p.Prompt(context.Background(), "ses_pi_pipe", "hi") }()

	go func() {
		cmd, err := readCommand(clientIn)
		if err != nil {
			return
		}
		writeLine(clientOutW, map[string]any{"type": "response", "id": cmd["id"], "success": true})
		// Stdout ends first, so a reader that does not wait finds no cause yet.
		_ = clientOutW.Close()
		time.Sleep(20 * time.Millisecond)
		_, _ = fmt.Fprintln(stderrW, `402: {"message":"Insufficient Balance","code":"invalid_request_error"}`)
		_ = stderrW.Close()
	}()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "Insufficient Balance") {
			t.Fatalf("error = %v; want the cause the child wrote to the stderr pipe", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Prompt did not return after the child exited")
	}
}

func TestPromptTurnErrorOmitsEmptyStderr(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, wOut, rIn := fakeClient()
	defer c.Close()
	c.setSessionID("ses-pi-clean")
	p.client = c
	p.sessions = map[string]struct{}{"ses-pi-clean": {}}

	errCh := make(chan error, 1)
	go func() { errCh <- p.Prompt(context.Background(), "ses-pi-clean", "hi") }()

	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		writeLine(wOut, map[string]any{"type": "response", "id": cmd["id"], "success": true})
		writeLine(wOut, map[string]any{"type": "agent_end", "stopReason": "error"})
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for prompt result")
	}
	if err == nil {
		t.Fatal("expected a turn error")
	}
	if strings.Contains(err.Error(), "(stderr") {
		t.Fatalf("error = %v; want no stderr suffix when nothing was captured", err)
	}
}

// pi emits agent_end with willRetry=true when its auto-retry layer will
// re-attempt the turn after backoff. The provider must keep waiting for the
// settled agent_end instead of treating the retry-pending turn as terminal,
// and the settled error must carry the real provider message.
func TestPromptWaitsThroughPiAutoRetryBeforeTerminalError(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, wOut, rIn := fakeClient()
	defer c.Close()
	c.setSessionID("ses-retry")
	p.client = c
	p.sessions = map[string]struct{}{"ses-retry": {}}

	errCh := make(chan error, 1)
	go func() { errCh <- p.Prompt(context.Background(), "ses-retry", "hi") }()

	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		writeLine(wOut, map[string]any{"type": "response", "id": cmd["id"], "success": true})
		// First agent_end: failed turn, pi will retry.
		writeLine(wOut, map[string]any{
			"type": "agent_end",
			"messages": []any{
				map[string]any{"role": "assistant", "stopReason": "error",
					"errorMessage": "500: litellm.InternalServerError - Connection error"},
			},
			"willRetry": true,
		})
		// pi's own retry markers, then the settled second attempt.
		writeLine(wOut, map[string]any{"type": "auto_retry_start", "attempt": 1, "maxAttempts": 3, "delayMs": 2000})
		writeLine(wOut, map[string]any{
			"type": "agent_end",
			"messages": []any{
				map[string]any{"role": "assistant", "stopReason": "error",
					"errorMessage": "500: litellm.InternalServerError - Connection error"},
			},
			"willRetry": false,
		})
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for prompt result; provider must wait past a retry-pending agent_end")
	}
	if err == nil {
		t.Fatal("expected a turn error from the settled agent_end")
	}
	if !strings.Contains(err.Error(), "litellm.InternalServerError") {
		t.Fatalf("error = %v; want the real provider error text", err)
	}
}

// A retry that succeeds settles the turn with no error.
func TestPromptRecoversWhenPiAutoRetrySucceeds(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, wOut, rIn := fakeClient()
	defer c.Close()
	c.setSessionID("ses-recover")
	p.client = c
	p.sessions = map[string]struct{}{"ses-recover": {}}

	errCh := make(chan error, 1)
	go func() { errCh <- p.Prompt(context.Background(), "ses-recover", "hi") }()

	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		writeLine(wOut, map[string]any{"type": "response", "id": cmd["id"], "success": true})
		writeLine(wOut, map[string]any{
			"type": "agent_end",
			"messages": []any{
				map[string]any{"role": "assistant", "stopReason": "error",
					"errorMessage": "503: service unavailable"},
			},
			"willRetry": true,
		})
		writeLine(wOut, map[string]any{"type": "auto_retry_start", "attempt": 1, "maxAttempts": 3, "delayMs": 2000})
		writeLine(wOut, map[string]any{"type": "auto_retry_end", "success": true, "attempt": 1})
		writeLine(wOut, map[string]any{
			"type": "agent_end",
			"messages": []any{
				map[string]any{"role": "assistant", "stopReason": "stop",
					"content": []any{map[string]any{"type": "text", "text": "OK"}}},
			},
			"willRetry": false,
		})
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for prompt result")
	}
	if err != nil {
		t.Fatalf("Prompt: %v; want recovery after the successful retry", err)
	}
}

// The pi backend reports prompt submission through the event stream like the
// claude backend: the event carries the prompt length, never the text.
func TestPromptEmitsPromptSubmittedEvent(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, wOut, rIn := fakeClient()
	defer c.Close()
	c.setSessionID("ses-pi-prompt")
	p.client = c
	p.sessions = map[string]struct{}{"ses-pi-prompt": {}}

	eventsCh := subscribeForSession(c, "ses-pi-prompt")

	errCh := make(chan error, 1)
	go func() { errCh <- p.Prompt(context.Background(), "ses-pi-prompt", "hello pi") }()

	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		writeLine(wOut, map[string]any{"type": "response", "id": cmd["id"], "success": true})
		writeLine(wOut, map[string]any{"type": "agent_end", "stopReason": "stop"})
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for prompt result")
	}
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	// session.end and agent.prompt_submitted both fan out to the subscriber;
	// drain until the submission event arrives.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-eventsCh:
			if ev.Event != "agent.prompt_submitted" {
				continue
			}
			if ev.SessionID != "ses-pi-prompt" {
				t.Errorf("session_id = %q, want ses-pi-prompt", ev.SessionID)
			}
			if got, _ := ev.Fields["prompt_length"].(int); got != len("hello pi") {
				t.Errorf("prompt_length = %v, want %d", ev.Fields["prompt_length"], len("hello pi"))
			}
			for key, value := range ev.Fields {
				if text, ok := value.(string); ok && strings.Contains(text, "hello pi") {
					t.Errorf("event carries the prompt text in field %q", key)
				}
			}
			return
		case <-deadline:
			t.Fatal("no agent.prompt_submitted event")
		}
	}
}

// A pi child that refuses the prompt (success:false) never starts a turn, so
// the backend must not report submission: no agent.prompt_submitted event
// fires and Prompt returns the child's refusal.
func TestPromptRejectedEmitsNoPromptSubmittedEvent(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	c, wOut, rIn := fakeClient()
	defer c.Close()
	c.setSessionID("ses-pi-reject")
	p.client = c
	p.sessions = map[string]struct{}{"ses-pi-reject": {}}

	eventsCh := subscribeForSession(c, "ses-pi-reject")

	errCh := make(chan error, 1)
	go func() { errCh <- p.Prompt(context.Background(), "ses-pi-reject", "hello pi") }()

	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		writeLine(wOut, map[string]any{"type": "response", "id": cmd["id"], "success": false, "error": "prompt rejected by child"})
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for prompt result")
	}
	if err == nil {
		t.Fatal("expected an error when the child rejects the prompt")
	}
	if !strings.Contains(err.Error(), "prompt rejected by child") {
		t.Fatalf("error = %v; want the child's refusal in the message", err)
	}

	// The turn never started, so no submission event may have fired. The
	// refusal path fans nothing out, so a bounded drain is safe: if the
	// (buggy) submission event were buffered it would already be here.
	deadline := time.After(500 * time.Millisecond)
	for {
		select {
		case ev := <-eventsCh:
			if ev.Event == "agent.prompt_submitted" {
				t.Fatal("agent.prompt_submitted emitted for a rejected prompt")
			}
		case <-deadline:
			return
		}
	}
}
