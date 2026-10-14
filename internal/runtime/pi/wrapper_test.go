package pi

import (
	"context"
	"os/exec"
	"slices"
	"testing"
)

// TestStartClientWithoutWrapperPreservesArgv proves the no-wrapper backend
// launch is unchanged at the pi backend launch site.
func TestStartClientWithoutWrapperPreservesArgv(t *testing.T) {
	captured := capturePiLaunch(t, nil)
	want := []string{"pi", "--mode", "rpc", "--no-session"}
	if !slices.Equal(captured, want) {
		t.Fatalf("pi argv = %#v, want %#v", captured, want)
	}
}

// TestStartClientWrapperPrefixesArgv proves wrapper prefixing at the pi
// backend launch site: the wrapper argv is placed in front of pi's command.
func TestStartClientWrapperPrefixesArgv(t *testing.T) {
	captured := capturePiLaunch(t, []string{"nice", "-n", "5"})
	want := []string{"nice", "-n", "5", "pi", "--mode", "rpc", "--no-session"}
	if !slices.Equal(captured, want) {
		t.Fatalf("pi argv = %#v, want %#v", captured, want)
	}
}

func capturePiLaunch(t *testing.T, launchWrapper []string) []string {
	t.Helper()
	original := piExecCommandContext
	t.Cleanup(func() { piExecCommandContext = original })

	var captured []string
	piExecCommandContext = func(_ context.Context, name string, args ...string) *exec.Cmd {
		captured = append([]string{name}, args...)
		return exec.Command("cat")
	}

	client, err := StartClientWithAgentProfileThinkingAndDir(
		context.Background(), "", "", "", "", "", "", "", "", "", "", launchWrapper,
	)
	if err != nil {
		t.Fatalf("StartClientWithAgentProfileThinkingAndDir: %v", err)
	}
	defer client.Close()
	if captured == nil {
		t.Fatal("pi command was not created")
	}
	return captured
}
