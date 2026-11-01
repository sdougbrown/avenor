package factory

import (
	"strings"
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

func TestNewProviderAgy(t *testing.T) {
	p, err := NewProvider(runtime.StartOptions{}, "agy")
	if err != nil {
		t.Fatalf("NewProvider(agy) error = %v", err)
	}
	if p == nil {
		t.Fatal("NewProvider(agy) provider is nil")
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

// TestNewProviderRejectsWrapperForNoProcessBackends pins the admission
// contract: backends that never exec a child process (opencode-http connects
// to a running server, pony runs in-process) must reject a launch_wrapper at
// provider construction instead of silently recording a wrapper that is
// never applied.
func TestNewProviderRejectsWrapperForNoProcessBackends(t *testing.T) {
	for _, backend := range []string{"opencode-http", "pony"} {
		_, err := NewProvider(runtime.StartOptions{LaunchWrapper: []string{"/usr/bin/nice", "-n", "5"}}, backend)
		if err == nil {
			t.Fatalf("NewProvider(%s) with launch_wrapper: want error, got nil", backend)
		}
		if !strings.Contains(err.Error(), "launch_wrapper: not supported for backend "+backend) {
			t.Fatalf("NewProvider(%s) error = %v, want launch_wrapper not-supported error", backend, err)
		}
		// The same backend without a wrapper must still construct.
		if _, err := NewProvider(runtime.StartOptions{}, backend); err != nil {
			t.Fatalf("NewProvider(%s) without wrapper: %v", backend, err)
		}
	}
}

func TestNewProviderPoo(t *testing.T) {
	p, err := NewProvider(runtime.StartOptions{}, "poo")
	if err != nil {
		t.Fatalf("NewProvider(poo) error = %v", err)
	}
	if p == nil {
		t.Fatal("NewProvider(poo) provider is nil")
	}
}

// poo starts a child process, so the launch wrapper applies to it exactly like
// any other process backend: construction accepts a wrapper instead of failing
// with the no-process rejection.
func TestNewProviderPooAcceptsLaunchWrapper(t *testing.T) {
	if _, err := NewProvider(runtime.StartOptions{LaunchWrapper: []string{"/usr/bin/nice", "-n", "5"}}, "poo"); err != nil {
		t.Fatalf("NewProvider(poo) with launch_wrapper: %v", err)
	}
}
