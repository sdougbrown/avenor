package claudechannel

import (
	"context"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestNewWithOptions(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{
		Agent: "jockey",
		Model: "claude-sonnet-4",
	})
	if p == nil {
		t.Fatal("NewWithOptions returned nil provider")
	}
}

func TestCapabilities(t *testing.T) {
	p := New()
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Backend != backendID {
		t.Errorf("Backend = %q, want %q", caps.Backend, backendID)
	}
	if caps.Permissions {
		t.Error("Permissions should be false until verified")
	}
	if caps.Resume {
		t.Error("Resume should be false")
	}
	if !caps.SubprocessDiscovery {
		t.Error("SubprocessDiscovery should be true")
	}
}

func TestResumeFails(t *testing.T) {
	p := New()
	_, err := p.Resume(context.Background(), "ses_1")
	if err == nil {
		t.Fatal("expected error for resume")
	}
}

func TestAnswerPermissionNotStarted(t *testing.T) {
	p := New()
	err := p.AnswerPermission(context.Background(), "ses", "req", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
}

func TestBrokerEventMapping(t *testing.T) {
	cases := []struct {
		state    string
		wantEv   string
		wantPhase string
	}{
		{"thinking", "agent.status", "thinking"},
		{"working", "agent.status", "working"},
		{"checkpoint", "agent.message_chunk", ""},
		{"blocked", "agent.status", "waiting"},
		{"started", "agent.status", "started"},
		{		"unknown", "agent.status", "unknown"},
	}

	for range cases {
		// dummy to satisfy "notused"
	}

	for _, c := range cases {
		payload := []byte(`{"summary":"ok"}`)
		ev := brokerEvent("ses_1", c.state, payload)
		if ev.Event != c.wantEv {
			t.Errorf("brokerEvent(%q) event = %q, want %q", c.state, ev.Event, c.wantEv)
		}
		if c.wantPhase != "" && ev.Fields["phase"] != c.wantPhase {
			t.Errorf("brokerEvent(%q) phase = %v, want %v", c.state, ev.Fields["phase"], c.wantPhase)
		}
		if ev.SessionID != "ses_1" {
			t.Errorf("brokerEvent(%q) sessionID = %q, want ses_1", c.state, ev.SessionID)
		}
	}
}

func TestMapFinishStatus(t *testing.T) {
	cases := []struct {
		input, want string
	}{
		{"done", "end_turn"},
		{"failed", "error"},
		{"blocked", "cancelled"},
		{"unknown", "end_turn"},
	}
	for _, c := range cases {
		got := mapFinishStatus(c.input)
		if got != c.want {
			t.Errorf("mapFinishStatus(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestProviderInterface(t *testing.T) {
	var _ runtime.Provider = (*Provider)(nil)
}

func TestEventsNotFound(t *testing.T) {
	p := New()
	_, err := p.Events(context.Background(), "no-such-session")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
}

func TestPromptNotFound(t *testing.T) {
	p := New()
	err := p.Prompt(context.Background(), "no-such-session", "hello")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
}

func TestCancelNotFound(t *testing.T) {
	p := New()
	err := p.Cancel(context.Background(), "no-such-session")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
}
