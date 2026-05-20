package geminiacp

import (
	"context"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestNewReturnsProvider(t *testing.T) {
	p := New()
	if p == nil {
		t.Fatal("New() returned nil")
	}
}

func TestCapabilities(t *testing.T) {
	p := New()
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	if caps.Backend != "gemini-acp" {
		t.Fatalf("Capabilities().Backend = %q, want %q", caps.Backend, "gemini-acp")
	}
}

func TestNewImplementsProvider(t *testing.T) {
	var _ runtime.Provider = New()
}
