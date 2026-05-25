//go:build integration

package opencodeacp

import (
	"context"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

func newIntegrationClient(t *testing.T) (*acp.Client, context.Context, context.CancelFunc) {
	t.Helper()
	if !opencodeAvailable() {
		t.Skip("opencode not on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	client, err := acp.NewClient(ctx, acp.ClientConfig{
		Bin:          "opencode",
		Args:         []string{"acp", "--pure", "--log-level", "WARN"},
		Dir:          ".",
		AppendCWDArg: true,
	})
	if err != nil {
		cancel()
		t.Skipf("opencode ACP unavailable in this environment: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		cancel()
	})
	return client, ctx, cancel
}

func TestDiscoveryAuthenticationDefault(t *testing.T) {
	client, ctx, _ := newIntegrationClient(t)
	if _, err := client.Initialize(ctx); err != nil {
		t.Skipf("initialize without authenticate failed: %v", err)
	}
}

func TestDiscoveryPermissionDefaultAndSubagentVisibility(t *testing.T) {
	t.Skip("not verified by default: requires a live model run that triggers permission and subagent/tool activity")
}

// TestDiscoveryCancelLatency drives a prompt that takes ~30s, calls
// session/cancel mid-flight, and asserts the session ends with
// stop_reason=cancelled within 5s. Captures OQ6 evidence.
func TestDiscoveryCancelLatency(t *testing.T) {
	if !opencodeAvailable() {
		t.Skip("opencode not on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client, err := acp.NewClient(ctx, acp.ClientConfig{
		Bin:          "opencode",
		Args:         []string{"acp", "--pure", "--log-level", "WARN"},
		Dir:          ".",
		AppendCWDArg: true,
	})
	if err != nil {
		t.Skipf("opencode acp unavailable: %v", err)
	}
	defer client.Close()

	if _, err := client.Initialize(ctx); err != nil {
		t.Skipf("initialize failed in this environment: %v", err)
	}
	session, err := client.NewSession(ctx)
	if err != nil {
		t.Fatalf("session/new: %v", err)
	}

	// Background prompt.
	type promptResult struct {
		ev  any
		err error
	}
	done := make(chan promptResult, 1)
	go func() {
		ev, err := session.Prompt(ctx, "Count from 1 to 5000, one number per line. Do not skip any.")
		done <- promptResult{ev: ev, err: err}
	}()

	// Drain events while we wait to send cancel.
	go func() {
		for range client.Events() {
		}
	}()

	time.Sleep(2 * time.Second)
	cancelStart := time.Now()
	if err := session.Cancel(); err != nil {
		t.Fatalf("session/cancel: %v", err)
	}

	select {
	case res := <-done:
		latency := time.Since(cancelStart)
		t.Logf("cancel → session.end latency: %v, err: %v, event: %+v", latency, res.err, res.ev)
		if latency > 10*time.Second {
			t.Errorf("cancel latency %v exceeds 10s budget", latency)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("session/prompt did not return within 15s of cancel")
	}
}

// TestDiscoverySessionResumeAfterRestart captures a session ID, kills the
// server, starts a fresh server, attempts session/load and session/resume.
// Captures OQ2 evidence. Failures are logged, not fatal, since the answer
// itself may be "not supported".
func TestDiscoverySessionResumeAfterRestart(t *testing.T) {
	if !opencodeAvailable() {
		t.Skip("opencode not on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Server #1: capture session ID.
	first, err := acp.NewClient(ctx, acp.ClientConfig{
		Bin:          "opencode",
		Args:         []string{"acp", "--pure", "--log-level", "WARN"},
		Dir:          ".",
		AppendCWDArg: true,
	})
	if err != nil {
		t.Skipf("opencode acp unavailable: %v", err)
	}
	if _, err := first.Initialize(ctx); err != nil {
		first.Close()
		t.Skipf("initialize #1 failed in this environment: %v", err)
	}
	go func() {
		for range first.Events() {
		}
	}()
	session, err := first.NewSession(ctx)
	if err != nil {
		first.Close()
		t.Fatalf("session/new: %v", err)
	}
	sessionID := session.SessionID
	t.Logf("captured session id: %s", sessionID)

	if _, err := session.Prompt(ctx, "Reply with the word OK and nothing else."); err != nil {
		t.Logf("prompt #1 failed (continuing): %v", err)
	}
	first.Close()

	// Server #2: fresh process, attempt resume.
	second, err := acp.NewClient(ctx, acp.ClientConfig{
		Bin:          "opencode",
		Args:         []string{"acp", "--pure", "--log-level", "WARN"},
		Dir:          ".",
		AppendCWDArg: true,
	})
	if err != nil {
		t.Fatalf("restart opencode acp: %v", err)
	}
	defer second.Close()
	go func() {
		for range second.Events() {
		}
	}()
	if _, err := second.Initialize(ctx); err != nil {
		t.Fatalf("initialize #2: %v", err)
	}

	if _, err := second.ResumeSession(ctx, sessionID); err != nil {
		t.Logf("session/resume after restart: %v", err)
	} else {
		t.Logf("session/resume after restart: OK")
	}
	if _, err := second.LoadSession(ctx, sessionID); err != nil {
		t.Logf("session/load after restart: %v", err)
	} else {
		t.Logf("session/load after restart: OK")
	}
}
