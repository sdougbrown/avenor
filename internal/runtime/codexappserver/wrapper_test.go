package codexappserver

import (
	"context"
	"os/exec"
	"slices"
	"testing"
)

// TestStartClientWithoutWrapperPreservesArgv proves the no-wrapper backend
// launch is unchanged at the codex app-server launch site.
func TestStartClientWithoutWrapperPreservesArgv(t *testing.T) {
	captured := captureStartClient(t, nil)
	want := []string{"codex", "app-server"}
	if !slices.Equal(captured, want) {
		t.Fatalf("codex argv = %#v, want %#v", captured, want)
	}
}

// TestStartClientWrapperPrefixesArgv proves wrapper prefixing at the codex
// launch site: the wrapper argv is placed in front of `codex app-server`.
func TestStartClientWrapperPrefixesArgv(t *testing.T) {
	captured := captureStartClient(t, []string{"nice", "-n", "5"})
	want := []string{"nice", "-n", "5", "codex", "app-server"}
	if !slices.Equal(captured, want) {
		t.Fatalf("codex argv = %#v, want %#v", captured, want)
	}
}

// captureStartClient runs StartClient against a stub process and returns the
// argv the launch site built. The stub never speaks the protocol, so the
// initialize handshake fails and StartClient reports the failed startup, which
// this test ignores; the argv is captured before any of that.
func captureStartClient(t *testing.T, launchWrapper []string) []string {
	t.Helper()
	original := appServerCommandContext
	t.Cleanup(func() { appServerCommandContext = original })

	var captured []string
	appServerCommandContext = func(_ context.Context, name string, argv ...string) *exec.Cmd {
		captured = append([]string{name}, argv...)
		return exec.Command("cat")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c, err := StartClient(ctx, launchWrapper)
	if err == nil {
		_ = c.Close()
	}
	if captured == nil {
		t.Fatal("codex launch command was not created")
	}
	return captured
}
