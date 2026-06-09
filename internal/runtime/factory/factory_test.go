package factory

import (
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestNewProviderAcp(t *testing.T) {
	p, err := NewProvider(runtime.StartOptions{}, "opencode-acp")
	if err != nil {
		t.Fatalf("NewProvider(acp) error = %v", err)
	}
	if p == nil {
		t.Fatal("NewProvider(acp) provider is nil")
	}
}

func TestNewProviderHTTP(t *testing.T) {
	p, err := NewProvider(runtime.StartOptions{}, "opencode-http")
	if err != nil {
		t.Fatalf("NewProvider(http) error = %v", err)
	}
	if p == nil {
		t.Fatal("NewProvider(http) provider is nil")
	}
}

func TestNewProviderCodex(t *testing.T) {
	p, err := NewProvider(runtime.StartOptions{}, "codex-app-server")
	if err != nil {
		t.Fatalf("NewProvider(codex) error = %v", err)
	}
	if p == nil {
		t.Fatal("NewProvider(codex) provider is nil")
	}
}

func TestNewProviderGeminiACP(t *testing.T) {
	p, err := NewProvider(runtime.StartOptions{}, "gemini-acp")
	if err != nil {
		t.Fatalf("NewProvider(gemini-acp) error = %v", err)
	}
	if p == nil {
		t.Fatal("NewProvider(gemini-acp) provider is nil")
	}
}

func TestNewProviderCursorACP(t *testing.T) {
	p, err := NewProvider(runtime.StartOptions{}, "cursor-acp")
	if err != nil {
		t.Fatalf("NewProvider(cursor-acp) error = %v", err)
	}
	if p == nil {
		t.Fatal("NewProvider(cursor-acp) provider is nil")
	}
}

func TestNewProviderClaude(t *testing.T) {
	p, err := NewProvider(runtime.StartOptions{}, "claude")
	if err != nil {
		t.Fatalf("NewProvider(claude) error = %v", err)
	}
	if p == nil {
		t.Fatal("NewProvider(claude) provider is nil")
	}
}

func TestNewProviderUnknown(t *testing.T) {
	p, err := NewProvider(runtime.StartOptions{}, "unknown")
	if p != nil {
		t.Fatal("NewProvider(unknown) expected nil provider")
	}
	if err == nil {
		t.Fatal("NewProvider(unknown) expected error")
	}
	if err.Error() != `unknown backend "unknown"` {
		t.Fatalf("NewProvider(unknown) error = %q, want %q", err.Error(), `unknown backend "unknown"`)
	}
}
