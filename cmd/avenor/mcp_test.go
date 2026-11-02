package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/mcpserver"
	"github.com/sdougbrown/avenor/internal/messaging"
)

func TestRunMCPInvalidTransport(t *testing.T) {
	_, err := mcpserver.NewServer(mcpserver.Options{Transport: "invalid"})
	if err == nil {
		t.Fatal("expected error for invalid transport")
	}
	if !strings.Contains(err.Error(), "unsupported transport") {
		t.Fatalf("expected error to mention unsupported transport, got: %v", err)
	}
}

func TestRunMCPNoAutostartWithoutSupervisorSocket(t *testing.T) {
	_, err := mcpserver.NewServer(mcpserver.Options{
		Transport:   "stdio",
		NoAutostart: true,
	})
	if err == nil {
		t.Fatal("expected error for no-autostart without supervisor socket")
	}
	if !strings.Contains(err.Error(), "no-autostart requires") {
		t.Fatalf("expected error to mention no-autostart requires, got: %v", err)
	}
}

func TestRunMCPValidFlags(t *testing.T) {
	s, err := mcpserver.NewServer(mcpserver.Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: &stubControlClient{},
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v, want nil", err)
	}
	if s == nil {
		t.Fatal("NewServer() returned nil")
	}
	s.Close()
}

func TestRunMCPAllowedHostValidation(t *testing.T) {
	for _, entry := range []string{"", "*.ts.net", "box:8443", "box.example/ts.net", "box example.ts.net", "<host>.<tailnet>.ts.net"} {
		var list allowedHostList
		if err := list.Set(entry); err == nil {
			t.Fatalf("Set(%q) succeeded, want error", entry)
		}
	}
}

func TestRunMCPAllowedHostValid(t *testing.T) {
	var list allowedHostList
	if err := list.Set("box.example.ts.net"); err != nil {
		t.Fatalf("Set() error = %v, want nil", err)
	}
	if err := list.Set("other.example.ts.net"); err != nil {
		t.Fatalf("Set() error = %v, want nil", err)
	}
	// A substituted (normal) hostname is still accepted after the placeholder
	// guard rejects < and >.
	if err := list.Set("box.tailnet.ts.net"); err != nil {
		t.Fatalf("Set() error = %v, want nil", err)
	}
	if got := list.String(); got != "box.example.ts.net,other.example.ts.net,box.tailnet.ts.net" {
		t.Fatalf("String() = %q, want %q", got, "box.example.ts.net,other.example.ts.net,box.tailnet.ts.net")
	}
}

func TestRunMCPAllowedHostStdioRejected(t *testing.T) {
	if got := runDeclared([]string{"mcp", "--transport", "stdio", "--allowed-host", "box.example.ts.net"}, os.Stdout, os.Stderr); got != 2 {
		t.Fatalf("runDeclared() = %d, want 2", got)
	}
}

func TestRunMCPAllowedHostHTTPAccepted(t *testing.T) {
	if err := mcpFlagError("http", []string{"box.example.ts.net"}); err != nil {
		t.Fatalf("mcpFlagError() = %v, want nil", err)
	}
}

func TestRunMCPAllowedHostInvalidTransport(t *testing.T) {
	if err := mcpFlagError("invalid", nil); err == nil {
		t.Fatal("expected error for invalid transport")
	}
}

func TestMaxWaitSemantics(t *testing.T) {
	// The parse-level default is 25s on both transports; an explicit 0
	// disables the clamp. mcpMaxWaitError only rejects negative budgets.
	if err := mcpMaxWaitError(0); err != nil {
		t.Fatalf("mcpMaxWaitError(0) = %v, want nil", err)
	}
	if err := mcpMaxWaitError(25 * time.Second); err != nil {
		t.Fatalf("mcpMaxWaitError(25s) = %v, want nil", err)
	}
	if err := mcpMaxWaitError(-1 * time.Second); err == nil {
		t.Fatal("mcpMaxWaitError(-1s) = nil, want an error")
	}
}

func TestRunMCPMaxWaitNegativeRejected(t *testing.T) {
	// --transport http skips the stdio --max-wait rejection so the negative
	// value branch is the one exercised. MCP_AUTH_TOKEN is forced empty so a
	// regressed guard exits at the token error, not at the guard — the stderr
	// assertion is what distinguishes the two paths.
	t.Setenv("MCP_AUTH_TOKEN", "")

	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w
	got := runDeclared([]string{"mcp", "--transport", "http", "--max-wait", "-5s"}, os.Stdout, os.Stderr)
	w.Close()
	os.Stderr = oldStderr

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if got != 2 {
		t.Fatalf("runDeclared() = %d, want 2", got)
	}
	if !strings.Contains(buf.String(), "--max-wait must not be negative") {
		t.Fatalf("stderr = %q, want it to contain %q", buf.String(), "--max-wait must not be negative")
	}
}

func TestRunMCPMaxWaitStdioAccepted(t *testing.T) {
	// The wait budget applies on both transports now: a nonzero --max-wait on
	// stdio passes validation (the stdio server exits 0 on closed stdin).
	if got := runDeclared([]string{"mcp", "--transport", "stdio", "--max-wait", "5s"}, os.Stdout, os.Stderr); got == 2 {
		t.Fatalf("runDeclared() = %d, want anything but the usage code 2", got)
	}
}

func TestRunMCPAllowedHostInvalidRejected(t *testing.T) {
	// At least one invalid entry (including a tab) must be rejected through
	// the declared mcp dispatch, not just a direct Set call.
	for _, entry := range []string{"box\thost", "box:8443"} {
		if got := runDeclared([]string{"mcp", "--transport", "http", "--allowed-host", entry}, os.Stdout, os.Stderr); got != 2 {
			t.Fatalf("runDeclared() = %d, want 2 (invalid --allowed-host %q)", got, entry)
		}
	}
}

type stubControlClient struct{}

func (s *stubControlClient) Status(runtimeID string) (map[string]any, error)     { return nil, nil }
func (s *stubControlClient) List() ([]map[string]any, error)                     { return nil, nil }
func (s *stubControlClient) Spawn(params map[string]any) (map[string]any, error) { return nil, nil }

func (s *stubControlClient) SpawnMCP(params map[string]any) (map[string]any, error) { return nil, nil }
func (s *stubControlClient) Shutdown(mode string) error                             { return nil }
func (s *stubControlClient) Cancel(runtimeID string) error                          { return nil }
func (s *stubControlClient) Close() error                                           { return nil }
func (s *stubControlClient) Closed() bool                                           { return false }
func (s *stubControlClient) AnswerPermission(runtimeID, requestID, optionID string) error {
	return nil
}
func (s *stubControlClient) AnswerPermissionWithMessage(runtimeID, requestID, optionID, message string) error {
	return nil
}
func (s *stubControlClient) WorkflowStatus(string) (map[string]any, error) { return nil, nil }
func (s *stubControlClient) WorkflowWait(string, time.Duration) (map[string]any, error) {
	return nil, nil
}
func (s *stubControlClient) WorkflowInspect(string) (map[string]any, error) { return nil, nil }
func (s *stubControlClient) WorkflowEvents(string, int64, int) (map[string]any, error) {
	return nil, nil
}
func (s *stubControlClient) WorkflowComplete(string, map[string]any) (map[string]any, error) {
	return nil, nil
}
func (s *stubControlClient) WorkflowGate(string, map[string]any) (map[string]any, error) {
	return nil, nil
}
func (s *stubControlClient) WorkflowCancel(string, string, string) (map[string]any, error) {
	return nil, nil
}
func (s *stubControlClient) WorkflowControllerStatus(string) (map[string]any, error) {
	return nil, nil
}
func (s *stubControlClient) WorkflowControllerList() (map[string]any, error) {
	return nil, nil
}
func (s *stubControlClient) WorkflowReady(string, int) (map[string]any, error) {
	return nil, nil
}
func (s *stubControlClient) MessageSendContext(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	return messaging.SendResult{}, nil
}
func (s *stubControlClient) MessageWaitContext(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	return messaging.WaitResult{}, nil
}
func (s *stubControlClient) MessageReplyContext(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	return messaging.SendResult{}, nil
}
func (s *stubControlClient) MessageWithdrawContext(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	return messaging.WithdrawResult{}, nil
}
func (s *stubControlClient) MessageReceiveContext(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return messaging.ReceiveResult{}, nil
}
func (s *stubControlClient) MessagePeersContext(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	return messaging.PeersResult{}, nil
}
func (s *stubControlClient) MessagePreflightContext(ctx context.Context) (messaging.PreflightResult, error) {
	return messaging.PreflightResult{}, nil
}
func (s *stubControlClient) ResumeContext(_ context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	return messaging.ResumeResult{}, nil
}
func (s *stubControlClient) SessionRenameContext(_ context.Context, sessionID, name string) (messaging.RenameResult, error) {
	return messaging.RenameResult{}, nil
}
func (s *stubControlClient) SessionLastContext(_ context.Context, runtimeID string) (messaging.PeerLast, error) {
	return messaging.PeerLast{}, nil
}

func (s *stubControlClient) SessionLastTurnContext(_ context.Context, runtimeID, _ string) (messaging.PeerLast, error) {
	return s.SessionLastContext(context.Background(), runtimeID)
}

func writeTokenFile(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	// WriteFile's perm is masked by the process umask; force the intended
	// mode so the rejection tests exercise it regardless of umask.
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMCPAuthTokenFilePrecedence(t *testing.T) {
	path := writeTokenFile(t, "file-token\n", 0o600)

	// Explicit --auth-token beats the file.
	token, err := resolveAuthToken(true, true, "flag-token", path, "env-token")
	if err != nil {
		t.Fatalf("flag over file: %v", err)
	}
	if token != "flag-token" {
		t.Fatalf("token = %q, want flag-token", token)
	}

	// File beats the environment.
	token, err = resolveAuthToken(false, true, "flag-token", path, "env-token")
	if err != nil {
		t.Fatalf("file over env: %v", err)
	}
	if token != "file-token" {
		t.Fatalf("token = %q, want file-token", token)
	}

	// Without flag or file, the environment is used.
	token, err = resolveAuthToken(false, false, "", "", "env-token")
	if err != nil {
		t.Fatalf("env fallback: %v", err)
	}
	if token != "env-token" {
		t.Fatalf("token = %q, want env-token", token)
	}
}

func TestMCPAuthTokenEmptyFlagFallsThroughToEnv(t *testing.T) {
	// An explicitly passed empty --auth-token must not be treated as
	// authoritative; it should fall through to the env value.
	token, err := resolveAuthToken(true, false, "", "", "env-token")
	if err != nil {
		t.Fatalf("resolveAuthToken: %v", err)
	}
	if token != "env-token" {
		t.Fatalf("token = %q, want env-token", token)
	}
}

func TestMCPAuthTokenFileRejected(t *testing.T) {
	empty := writeTokenFile(t, "   \n", 0o600)
	if _, err := readAuthTokenFile(empty); err == nil {
		t.Fatal("expected empty token file to be rejected")
	}

	loose := writeTokenFile(t, "secret\n", 0o644)
	if _, err := readAuthTokenFile(loose); err == nil {
		t.Fatal("expected mode 0644 token file to be rejected")
	}

	missing := filepath.Join(t.TempDir(), "absent")
	if _, err := readAuthTokenFile(missing); err == nil {
		t.Fatal("expected missing token file to be rejected")
	}
}

func TestMCPAuthTokenFileAccepted(t *testing.T) {
	path := writeTokenFile(t, "secret-token\n", 0o600)
	token, err := readAuthTokenFile(path)
	if err != nil {
		t.Fatalf("readAuthTokenFile: %v", err)
	}
	if token != "secret-token" {
		t.Fatalf("token = %q, want secret-token", token)
	}
}

func TestRunMCPAuthTokenAndFileConflictRejected(t *testing.T) {
	path := writeTokenFile(t, "file-token\n", 0o600)

	// Capture stderr: the guard message reaches Run's stderr writer, so the
	// os.Stderr swap below is what the injected stream picks up.
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	defer func() { os.Stderr = oldStderr }()

	got := runDeclared([]string{"mcp", "--transport", "http", "--auth-token", "flag-token", "--auth-token-file", path}, os.Stdout, os.Stderr)
	w.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatal(err)
	}

	if got != 2 {
		t.Fatalf("runDeclared() = %d, want 2", got)
	}
	if !strings.Contains(buf.String(), "cannot both be set") {
		t.Fatalf("stderr = %q, want the flag-conflict guard message", buf.String())
	}
}
