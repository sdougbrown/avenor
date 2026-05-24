package pi

import (
	"context"
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

func TestProviderImplementsInterface(t *testing.T) {
	var _ runtime.Provider = (*Provider)(nil)
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
