package agy

import (
	"context"
	"os/exec"
	"slices"
	"testing"
)

// TestLaunchInitialWithoutWrapperPreservesArgv proves the no-wrapper backend
// launch is unchanged at the agy initial-launch site.
func TestLaunchInitialWithoutWrapperPreservesArgv(t *testing.T) {
	captured := captureAgyLaunch(t, "hi", nil)
	want := []string{"agy", "--output-format", "stream-json", "--print-timeout", printTimeout, "--print", "hi"}
	if !slices.Equal(captured, want) {
		t.Fatalf("agy argv = %#v, want %#v", captured, want)
	}
}

// TestLaunchInitialWrapperPrefixesArgv proves wrapper prefixing at a non-ACP
// backend launch site: the wrapper argv is placed in front of agy's command.
func TestLaunchInitialWrapperPrefixesArgv(t *testing.T) {
	captured := captureAgyLaunch(t, "hi", []string{"nice", "-n", "5"})
	want := []string{"nice", "-n", "5", "agy", "--output-format", "stream-json", "--print-timeout", printTimeout, "--print", "hi"}
	if !slices.Equal(captured, want) {
		t.Fatalf("agy argv = %#v, want %#v", captured, want)
	}
}

// captureAgyLaunch runs LaunchInitial against a stub process and returns the
// argv the launch site built. The stub exits immediately, so init never
// arrives; the caller-supplied cancel bounds the wait and LaunchInitial
// reports the cancelled startup, which this test ignores.
func captureAgyLaunch(t *testing.T, prompt string, launchWrapper []string) []string {
	t.Helper()
	original := agyLaunchCommandContext
	t.Cleanup(func() { agyLaunchCommandContext = original })

	var captured []string
	agyLaunchCommandContext = func(_ context.Context, name string, argv ...string) *exec.Cmd {
		captured = append([]string{name}, argv...)
		return exec.Command("cat")
	}

	c := newClient(nil, nil, nil, nil)
	c.launchWrapper = launchWrapper
	_, _ = c.LaunchInitial(context.Background(), prompt, "", "", "")
	if captured == nil {
		t.Fatal("agy launch command was not created")
	}
	return captured
}
