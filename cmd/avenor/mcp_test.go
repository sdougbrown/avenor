package main

import (
	"strings"
	"testing"

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
