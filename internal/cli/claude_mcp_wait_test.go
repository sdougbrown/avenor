package cli

import (
	"context"
	"os"
	"path/filepath"
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
		{"not shorter than progress timeout", []string{"--backend", "claude", "--claude-mcp-wait", "10s", "--progress-timeout", "10s"}, "--progress-timeout"},
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

func TestStartSessionValidatesClaudeMCPWait(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backend string
		wait    time.Duration
		wantErr string
	}{
		{"negative", "claude", -time.Second, "negative"},
		{"over cap", "claude", runtime.MaxClaudeMCPWait + time.Nanosecond, "limit"},
		{"unsupported backend", "pi", time.Second, "claude-mcp-wait"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &plainThinkingProvider{}
			_, err := StartSession(context.Background(), p, tc.backend, runtime.StartOptions{ClaudeMCPWait: tc.wait}, "")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("%s error = %v, want it to mention %q", tc.name, err, tc.wantErr)
			}
			if p.startCalls != 0 || p.resumeCalls != 0 {
				t.Fatalf("%s launched a session", tc.name)
			}
		})
	}
}

func TestStartSessionRejectsClaudeMCPWaitOnResume(t *testing.T) {
	for _, backend := range []string{"claude", "claude-channel"} {
		p := &plainThinkingProvider{}
		_, err := StartSession(context.Background(), p, backend, runtime.StartOptions{ClaudeMCPWait: 7 * time.Second}, "session")
		if err == nil || !strings.Contains(err.Error(), "claude-mcp-wait") || !strings.Contains(err.Error(), "only when starting a session") {
			t.Fatalf("%s error = %v, want a start-only rejection", backend, err)
		}
		if p.resumeCalls != 0 || p.startCalls != 0 {
			t.Fatalf("%s started a replacement process", backend)
		}
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

func TestRunClaudeMCPWaitWithRosterEntry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		backend  string
		wantCode int
	}{
		{"claude entry carries the wait", "claude", 0},
		{"non-claude entry rejects it", "codex-app-server", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got runtime.StartOptions
			called := false
			oldNewProvider := newProvider
			provider := newScriptedProvider()
			newProvider = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
				called = true
				got = opts
				return provider, nil
			}
			t.Cleanup(func() { newProvider = oldNewProvider })

			dir := t.TempDir()
			rosterPath := filepath.Join(dir, "roster.json")
			contents := `{"horse":{"backend":"` + tc.backend + `","model":"m"}}`
			if err := os.WriteFile(rosterPath, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			args := []string{"--dir", dir, "--roster-file", rosterPath, "--roster-entry", "horse", "--claude-mcp-wait", "7s", "--prompt", "noop"}
			var stderr strings.Builder
			if code := run(args, func(string) string { return "" }, &stderr); code != tc.wantCode {
				t.Fatalf("run() = %d, want %d; stderr=%s", code, tc.wantCode, stderr.String())
			}
			if tc.wantCode == 0 {
				if got.ClaudeMCPWait != 7*time.Second {
					t.Fatalf("StartOptions.ClaudeMCPWait = %v, want 7s", got.ClaudeMCPWait)
				}
			} else if called || !strings.Contains(stderr.String(), "claude-mcp-wait") {
				t.Fatalf("called=%v stderr=%q, want claude-mcp-wait rejection before launch", called, stderr.String())
			}
		})
	}
}
