package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/mcpserver"
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
	if got := runMCP([]string{"--transport", "stdio", "--allowed-host", "box.example.ts.net"}); got != 1 {
		t.Fatalf("runMCP() = %d, want 1", got)
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

func TestEffectiveMaxWait(t *testing.T) {
	if got := effectiveMaxWait("stdio", false, 0); got != 0 {
		t.Fatalf("stdio default = %v, want 0", got)
	}
	if got := effectiveMaxWait("http", false, 0); got != 25*time.Second {
		t.Fatalf("http default = %v, want 25s", got)
	}
	if got := effectiveMaxWait("http", true, 0); got != 0 {
		t.Fatalf("explicit zero = %v, want 0", got)
	}
	if got := effectiveMaxWait("stdio", true, 5*time.Minute); got != 5*time.Minute {
		t.Fatalf("explicit value = %v, want 5m", got)
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
	got := runMCP([]string{"--transport", "http", "--max-wait", "-5s"})
	w.Close()
	os.Stderr = oldStderr

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if got != 1 {
		t.Fatalf("runMCP() = %d, want 1", got)
	}
	if !strings.Contains(buf.String(), "--max-wait must not be negative") {
		t.Fatalf("stderr = %q, want it to contain %q", buf.String(), "--max-wait must not be negative")
	}
}

func TestRunMCPMaxWaitStdioRejected(t *testing.T) {
	// stdio never clamps, so an explicit nonzero --max-wait is rejected before
	// the server would block.
	if got := runMCP([]string{"--transport", "stdio", "--max-wait", "5s"}); got != 1 {
		t.Fatalf("runMCP() = %d, want 1 (stdio --max-wait 5s)", got)
	}
}

func TestMCPMaxWaitTransport(t *testing.T) {
	// runMCP blocks on Run/RunHTTP, so the accepted cases are validated at the
	// helper level; the rejected case is covered by TestRunMCPMaxWaitStdioRejected.
	for _, tc := range []struct {
		name      string
		transport string
		explicit  bool
		value     time.Duration
		wantErr   bool
	}{
		{"stdio default", "stdio", false, 0, false},
		{"stdio explicit zero", "stdio", true, 0, false},
		{"stdio explicit nonzero", "stdio", true, 5 * time.Second, true},
		{"http explicit zero", "http", true, 0, false},
		{"http explicit nonzero", "http", true, 5 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := mcpMaxWaitError(tc.transport, tc.explicit, tc.value)
			if (err != nil) != tc.wantErr {
				t.Fatalf("mcpMaxWaitError(%q, %v, %v) = %v, wantErr %v", tc.transport, tc.explicit, tc.value, err, tc.wantErr)
			}
		})
	}
}

func TestRunMCPAllowedHostInvalidRejected(t *testing.T) {
	// At least one invalid entry (including a tab) must be rejected through
	// runMCP, not just a direct Set call.
	for _, entry := range []string{"box\thost", "box:8443"} {
		if got := runMCP([]string{"--transport", "http", "--allowed-host", entry}); got != 1 {
			t.Fatalf("runMCP() = %d, want 1 (invalid --allowed-host %q)", got, entry)
		}
	}
}

type stubControlClient struct{}

func (s *stubControlClient) Status(runtimeID string) (map[string]any, error)     { return nil, nil }
func (s *stubControlClient) List() ([]map[string]any, error)                     { return nil, nil }
func (s *stubControlClient) Spawn(params map[string]any) (map[string]any, error) { return nil, nil }
func (s *stubControlClient) Shutdown(mode string) error                          { return nil }
func (s *stubControlClient) Close() error                                        { return nil }
func (s *stubControlClient) Closed() bool                                        { return false }
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
func (s *stubControlClient) WorkflowControllerStatus(string) (map[string]any, error) {
	return nil, nil
}
func (s *stubControlClient) WorkflowControllerList() (map[string]any, error) {
	return nil, nil
}
