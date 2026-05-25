package opencodeacp

import (
	"context"
	"errors"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

type fakeSessionConfigurer struct {
	modelCalls []modelCall
	modeCalls  []modeCall
}

type modelCall struct {
	sessionID string
	modelID   string
}

type modeCall struct {
	sessionID string
	modeID    string
}

func (f *fakeSessionConfigurer) SetSessionModel(_ context.Context, sessionID, modelID string) error {
	f.modelCalls = append(f.modelCalls, modelCall{sessionID, modelID})
	return nil
}

func (f *fakeSessionConfigurer) SetSessionMode(_ context.Context, sessionID, modeID string) error {
	f.modeCalls = append(f.modeCalls, modeCall{sessionID, modeID})
	return nil
}

func TestConfigureSessionAgentAndModel(t *testing.T) {
	fake := &fakeSessionConfigurer{}

	err := configureSession(context.Background(), fake, "ses_123", runtime.StartOptions{
		Agent: "jockey",
		Model: "deepseek/deepseek-v4-pro",
	})
	if err != nil {
		t.Fatalf("configureSession: %v", err)
	}

	if len(fake.modelCalls) != 1 {
		t.Fatalf("expected 1 SetSessionModel call, got %d", len(fake.modelCalls))
	}
	if fake.modelCalls[0].sessionID != "ses_123" {
		t.Errorf("model sessionID = %q, want ses_123", fake.modelCalls[0].sessionID)
	}
	if fake.modelCalls[0].modelID != "deepseek/deepseek-v4-pro" {
		t.Errorf("modelID = %q, want deepseek/deepseek-v4-pro", fake.modelCalls[0].modelID)
	}

	if len(fake.modeCalls) != 1 {
		t.Fatalf("expected 1 SetSessionMode call, got %d", len(fake.modeCalls))
	}
	if fake.modeCalls[0].sessionID != "ses_123" {
		t.Errorf("mode sessionID = %q, want ses_123", fake.modeCalls[0].sessionID)
	}
	if fake.modeCalls[0].modeID != "jockey" {
		t.Errorf("modeID = %q, want jockey", fake.modeCalls[0].modeID)
	}
}

func TestConfigureSessionOnlyModel(t *testing.T) {
	fake := &fakeSessionConfigurer{}

	err := configureSession(context.Background(), fake, "ses_456", runtime.StartOptions{
		Model: "gpt-4",
	})
	if err != nil {
		t.Fatalf("configureSession: %v", err)
	}

	if len(fake.modelCalls) != 1 {
		t.Fatalf("expected 1 model call, got %d", len(fake.modelCalls))
	}
	if fake.modelCalls[0].modelID != "gpt-4" {
		t.Errorf("modelID = %q, want gpt-4", fake.modelCalls[0].modelID)
	}
	if len(fake.modeCalls) != 0 {
		t.Fatalf("expected 0 mode calls, got %d", len(fake.modeCalls))
	}
}

func TestConfigureSessionOnlyAgent(t *testing.T) {
	fake := &fakeSessionConfigurer{}

	err := configureSession(context.Background(), fake, "ses_789", runtime.StartOptions{
		Agent: "mule",
	})
	if err != nil {
		t.Fatalf("configureSession: %v", err)
	}

	if len(fake.modeCalls) != 1 {
		t.Fatalf("expected 1 mode call, got %d", len(fake.modeCalls))
	}
	if fake.modeCalls[0].modeID != "mule" {
		t.Errorf("modeID = %q, want mule", fake.modeCalls[0].modeID)
	}
	if len(fake.modelCalls) != 0 {
		t.Fatalf("expected 0 model calls, got %d", len(fake.modelCalls))
	}
}

func TestConfigureSessionEmpty(t *testing.T) {
	fake := &fakeSessionConfigurer{}

	err := configureSession(context.Background(), fake, "ses_empty", runtime.StartOptions{})
	if err != nil {
		t.Fatalf("configureSession: %v", err)
	}

	if len(fake.modelCalls) != 0 || len(fake.modeCalls) != 0 {
		t.Fatalf("expected no calls, got model=%d mode=%d", len(fake.modelCalls), len(fake.modeCalls))
	}
}

func TestConfigureSessionModelError(t *testing.T) {
	fake := &errorConfigurer{modelErr: errors.New("model failed")}

	err := configureSession(context.Background(), fake, "ses_err", runtime.StartOptions{
		Model: "bad-model",
		Agent: "jockey",
	})
	if err == nil || err.Error() != "model failed" {
		t.Fatalf("expected 'model failed' error, got %v", err)
	}
	if fake.modeCalls != 0 {
		t.Fatalf("SetSessionMode was called %d times after SetSessionModel error — should have returned early", fake.modeCalls)
	}
}

func TestConfigureSessionModeError(t *testing.T) {
	fake := &errorConfigurer{modeErr: errors.New("mode failed")}

	err := configureSession(context.Background(), fake, "ses_err", runtime.StartOptions{
		Agent: "bad-agent",
	})
	if err == nil || err.Error() != "mode failed" {
		t.Fatalf("expected 'mode failed' error, got %v", err)
	}
}

type errorConfigurer struct {
	modelErr  error
	modeErr   error
	modeCalls int
}

func (e *errorConfigurer) SetSessionModel(_ context.Context, _, _ string) error {
	return e.modelErr
}

func (e *errorConfigurer) SetSessionMode(_ context.Context, _, _ string) error {
	e.modeCalls++
	return e.modeErr
}
