package codexappserver

import (
	"context"
	"os/exec"
	"slices"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
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

// TestProviderStartWrapperReachesLaunchSite proves the Provider.Start handoff:
// the merged StartOptions.LaunchWrapper must reach the launch site, not just a
// client built in a unit test.
func TestProviderStartWrapperReachesLaunchSite(t *testing.T) {
	captured := captureProviderLaunch(t, func(p *Provider, ctx context.Context) {
		_, _ = p.Start(ctx, runtime.StartOptions{LaunchWrapper: []string{"nice", "-n", "5"}})
	})
	want := []string{"nice", "-n", "5", "codex", "app-server"}
	if !slices.Equal(captured, want) {
		t.Fatalf("codex argv = %#v, want %#v", captured, want)
	}
}

// TestProviderResumeWrapperReachesLaunchSite proves the resume handoff: resume
// launches through the provider's own StartOptions (p.opts.LaunchWrapper), the
// authoritative options retained at construction.
func TestProviderResumeWrapperReachesLaunchSite(t *testing.T) {
	captured := captureProviderLaunch(t, func(p *Provider, ctx context.Context) {
		_, _ = p.Resume(ctx, "ses_resume_wrapper")
	}, runtime.StartOptions{LaunchWrapper: []string{"nice", "-n", "5"}})
	want := []string{"nice", "-n", "5", "codex", "app-server"}
	if !slices.Equal(captured, want) {
		t.Fatalf("codex argv = %#v, want %#v", captured, want)
	}
}

// captureProviderLaunch stubs the process-creation seam, runs the provider
// call, and returns the argv the launch site built. The cancelled context and
// stub process make the client handshake fail, so the provider call reports an
// error, which the caller ignores; the argv is captured before any of that.
func captureProviderLaunch(t *testing.T, run func(*Provider, context.Context), opts ...runtime.StartOptions) []string {
	t.Helper()
	original := appServerCommandContext
	t.Cleanup(func() { appServerCommandContext = original })

	var captured []string
	appServerCommandContext = func(_ context.Context, name string, argv ...string) *exec.Cmd {
		captured = append([]string{name}, argv...)
		return exec.Command("cat")
	}

	base := runtime.StartOptions{}
	if len(opts) > 0 {
		base = opts[0]
	}
	p := NewWithOptions(base)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	run(p, ctx)
	p.Close()
	if captured == nil {
		t.Fatal("codex launch command was not created")
	}
	return captured
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
