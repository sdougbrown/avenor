package pi

import (
	"context"
	"strings"
	"testing"

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
	p.sessions["pi_existing"] = "pi_existing"

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

func TestProviderImplementsInterface(t *testing.T) {
	var _ runtime.Provider = (*Provider)(nil)
}
