package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestRunClaudeMCPWaitValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"negative", []string{"--backend", "claude", "--claude-mcp-wait", "-1s"}, "negative"},
		{"over cap", []string{"--backend", "claude", "--claude-mcp-wait", "61s"}, "limit"},
		{"unsupported backend", []string{"--backend", "agy", "--claude-mcp-wait", "5s"}, "claude-mcp-wait"},
		{"workflow", []string{"--loop-file", "loop.json", "--claude-mcp-wait", "5s"}, "--loop-file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldNewProvider := newProvider
			called := false
			newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) {
				called = true
				return newScriptedProvider(), nil
			}
			t.Cleanup(func() { newProvider = oldNewProvider })

			var stderr strings.Builder
			args := append([]string{"--dir", t.TempDir(), "--prompt", "hi"}, tc.args...)
			if got := run(args, func(string) string { return "" }, &stderr); got != 1 {
				t.Fatalf("run() = %d, want 1; stderr=%s", got, stderr.String())
			}
			if called || !strings.Contains(stderr.String(), tc.wantErr) {
				t.Fatalf("called=%v stderr=%q, want %q", called, stderr.String(), tc.wantErr)
			}
		})
	}
}

func TestRunClaudeMCPWaitReachesStartOptions(t *testing.T) {
	var got runtime.StartOptions
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	newProvider = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		got = opts
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	var stderr strings.Builder
	args := []string{"--dir", t.TempDir(), "--backend", "claude", "--claude-mcp-wait", "7s", "--prompt", "noop"}
	if code := run(args, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	if got.ClaudeMCPWait != 7*time.Second {
		t.Fatalf("StartOptions.ClaudeMCPWait = %v, want 7s", got.ClaudeMCPWait)
	}
}
