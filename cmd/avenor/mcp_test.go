package main

import (
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
	for _, entry := range []string{"", "*.ts.net", "box:8443", "box.example/ts.net", "box example.ts.net"} {
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
	if got := list.String(); got != "box.example.ts.net,other.example.ts.net" {
		t.Fatalf("String() = %q, want %q", got, "box.example.ts.net,other.example.ts.net")
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
	if got := runMCP([]string{"--max-wait", "-5s"}); got != 1 {
		t.Fatalf("runMCP() = %d, want 1", got)
	}
}

type stubControlClient struct{}

func (s *stubControlClient) Status(runtimeID string) (map[string]any, error)     { return nil, nil }
func (s *stubControlClient) List() ([]map[string]any, error)                     { return nil, nil }
func (s *stubControlClient) Spawn(params map[string]any) (map[string]any, error) { return nil, nil }
func (s *stubControlClient) Shutdown(mode string) error                          { return nil }
func (s *stubControlClient) Close() error                                        { return nil }
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
