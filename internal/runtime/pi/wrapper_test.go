package pi

import (
	"context"
	"os/exec"
	"slices"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
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

// TestProviderStartWrapperReachesLaunchSite proves the Provider.Start handoff:
// the merged StartOptions.LaunchWrapper must reach the launch site (the
// provider's default starter passes opts.LaunchWrapper into the launch
// command), not just a client built in a unit test.
func TestProviderStartWrapperReachesLaunchSite(t *testing.T) {
	original := piExecCommandContext
	t.Cleanup(func() { piExecCommandContext = original })

	var captured []string
	piExecCommandContext = func(_ context.Context, name string, args ...string) *exec.Cmd {
		captured = append([]string{name}, args...)
		return exec.Command("cat")
	}

	p := NewWithOptions(runtime.StartOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The cancelled context makes Start fail right after the launch command is
	// built; the argv capture is the assertion, the error is expected.
	_, _ = p.Start(ctx, runtime.StartOptions{LaunchWrapper: []string{"nice", "-n", "5"}})
	p.Close()

	want := []string{"nice", "-n", "5", "pi", "--mode", "rpc", "--no-session"}
	if !slices.Equal(captured, want) {
		t.Fatalf("pi argv = %#v, want %#v", captured, want)
	}
}
