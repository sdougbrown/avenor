package poo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	avruntime "github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/poo/wire/protocol"
)

// fallbackFixture records each process's inherited environment before the shim
// optionally removes the manifest paths, as an older binary would ignore them.
func fallbackFixture(t *testing.T, ignoreVars string) (avruntime.StartOptions, string) {
	t.Helper()
	t.Setenv("POO_FEEDS_PATH", "")
	t.Setenv("POO_CLI_REGISTRY_PATH", "")
	stub := filepath.Join(t.TempDir(), "avenor")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexec sleep 60\n"), 0700); err != nil {
		t.Fatal(err)
	}
	oldExecutable := avenorExecutable
	avenorExecutable = func() (string, error) { return stub, nil }
	t.Cleanup(func() { avenorExecutable = oldExecutable })
	b := broker.New(broker.MakeToken())
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	token, ok, err := b.EnsureRun("rt_fallback", "")
	if err != nil || !ok {
		t.Fatalf("EnsureRun: %v %v", ok, err)
	}
	f := newPooFixture(t, newFakeEndpoint(t))
	opts := f.startOptions(t)
	dir := t.TempDir()
	shim := filepath.Join(dir, "poo-shim")
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
	clear := ""
	if ignoreVars != "" {
		clear = "unset " + ignoreVars + "\n"
	}
	script := fmt.Sprintf("#!/bin/sh\necho x >> %s\nn=$(wc -l < %s)\nenv > %s\"$n\"\n%sexec %s \"$@\"\n", quote(filepath.Join(dir, "count")), quote(filepath.Join(dir, "count")), quote(filepath.Join(dir, "env.")), clear, quote(opts.Launch.Command))
	if err := os.WriteFile(shim, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	opts.Launch.Command = shim
	opts.MessagingOn, opts.Broker, opts.RuntimeID, opts.BrokerToken = true, b, "rt_fallback", token
	return opts, dir
}

func fallbackLaunches(t *testing.T, dir string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "count"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(data), "\n")
}

func assertFallbackEnv(t *testing.T, dir string, launch int, on bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("env.%d", launch)))
	if err != nil {
		t.Fatal(err)
	}
	env := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok {
			env[key] = value
		}
	}
	for _, key := range []string{"AVENOR_BROKER_URL", "AVENOR_RUN_ID", "AVENOR_BROKER_TOKEN", "POO_FEEDS_PATH", "POO_CLI_REGISTRY_PATH"} {
		if (env[key] != "") != on {
			t.Errorf("launch %d: %s present = %v, want %v", launch, key, env[key] != "", on)
		}
	}
}

func assertFallbackSessionOff(t *testing.T, p *Provider, session avruntime.Session) {
	t.Helper()
	if session.MessagingOn == nil || *session.MessagingOn {
		t.Fatalf("outcome = %v, want off", session.MessagingOn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, rpcErr, err := p.client.call(ctx, protocol.MethodSessionGet, protocol.GetParams{SessionID: session.SessionID})
	if err != nil || rpcErr != nil {
		t.Fatalf("session/get: %v %v", err, rpcErr)
	}
	var result protocol.GetResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	for _, job := range result.Jobs {
		if job.JobID == "feed:avenor-inbox" {
			t.Fatalf("fallback session still has inbox job: %+v", result.Jobs)
		}
	}
}

func TestMessagingIgnoredManifestsFallback(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	oldInterval, oldLimit := messagingPollInterval, messagingPollLimit
	messagingPollInterval, messagingPollLimit = 10*time.Millisecond, 150*time.Millisecond
	t.Cleanup(func() { messagingPollInterval, messagingPollLimit = oldInterval, oldLimit })
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit_%v", explicit), func(t *testing.T) {
			opts, dir := fallbackFixture(t, "POO_FEEDS_PATH POO_CLI_REGISTRY_PATH")
			if explicit {
				on := true
				opts.Messaging = &on
			}
			p := NewWithOptions(opts)
			t.Cleanup(func() { _ = p.Close() })
			session, err := p.Start(context.Background(), avruntime.StartOptions{})
			if explicit {
				if err == nil || !strings.Contains(err.Error(), "v0.2.0") {
					t.Fatalf("explicit request error = %v, want minimum version", err)
				}
				if got := fallbackLaunches(t, dir); got != 1 {
					t.Fatalf("launches = %d, want 1", got)
				}
				assertFallbackEnv(t, dir, 1, true)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := fallbackLaunches(t, dir); got != 2 {
				t.Fatalf("launches = %d, want 2", got)
			}
			assertFallbackEnv(t, dir, 1, true)
			assertFallbackEnv(t, dir, 2, false)
			assertFallbackSessionOff(t, p, session)

			// The provider's own restart must use the effective, not requested, choice.
			p.discardProcess(p.client)
			resumed, err := p.Resume(context.Background(), session.SessionID)
			if err != nil {
				t.Fatalf("resume after process exit: %v", err)
			}
			if got := fallbackLaunches(t, dir); got != 3 {
				t.Fatalf("launches after resume = %d, want 3", got)
			}
			assertFallbackEnv(t, dir, 3, false)
			assertFallbackSessionOff(t, p, resumed)
		})
	}
}

func TestMessagingFeedWithoutCallCLIFallback(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	opts, dir := fallbackFixture(t, "POO_CLI_REGISTRY_PATH")

	// First verify what this shim actually adopts using the same real binary.
	c, err := startClient(context.Background(), opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.shutdown() })
	if _, err := c.initialize(context.Background(), []int{protocol.Version}); err != nil {
		t.Fatal(err)
	}
	info, err := c.createSession(context.Background(), protocol.CreateParams{WorkingDir: opts.Dir, Profile: opts.Agent, Models: &protocol.ModelRoles{Chat: opts.Model}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range info.Tools {
		if tool.Name == "call_cli" {
			t.Fatal("shim unexpectedly adopted call_cli")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		raw, rpcErr, err := c.call(ctx, protocol.MethodSessionGet, protocol.GetParams{SessionID: info.SessionID})
		if err != nil || rpcErr != nil {
			t.Fatalf("session/get: %v %v", err, rpcErr)
		}
		var result protocol.GetResult
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, job := range result.Jobs {
			found = found || job.JobID == "feed:avenor-inbox"
		}
		if found {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("shim did not adopt the inbox feed")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := c.closeSession(info.SessionID); err != nil {
		t.Fatal(err)
	}
	if err := c.shutdown(); err != nil {
		t.Fatal(err)
	}

	p := NewWithOptions(opts)
	t.Cleanup(func() { _ = p.Close() })
	session, err := p.Start(context.Background(), avruntime.StartOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := fallbackLaunches(t, dir); got != 3 {
		t.Fatalf("launches = %d, want probe plus two attempts", got)
	}
	assertFallbackEnv(t, dir, 2, true)
	assertFallbackEnv(t, dir, 3, false)
	assertFallbackSessionOff(t, p, session)
}

func TestMessagingReadOnlyFallback(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	opts, dir := fallbackFixture(t, "")
	opts.Agent = "search"
	p := NewWithOptions(opts)
	t.Cleanup(func() { _ = p.Close() })
	session, err := p.Start(context.Background(), avruntime.StartOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := fallbackLaunches(t, dir); got != 2 {
		t.Fatalf("launches = %d, want 2", got)
	}
	assertFallbackEnv(t, dir, 1, true)
	assertFallbackEnv(t, dir, 2, false)
	assertFallbackSessionOff(t, p, session)
}

func TestMessagingManifestWriteFailure(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocked, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", blocked)
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit_%v", explicit), func(t *testing.T) {
			opts, dir := fallbackFixture(t, "")
			if explicit {
				on := true
				opts.Messaging = &on
			}
			p := NewWithOptions(opts)
			t.Cleanup(func() { _ = p.Close() })
			session, err := p.Start(context.Background(), avruntime.StartOptions{})
			if explicit {
				if err == nil || !strings.Contains(err.Error(), "v0.2.0") {
					t.Fatalf("explicit request error = %v, want minimum version", err)
				}
				if got := fallbackLaunches(t, dir); got != 0 {
					t.Fatalf("launches = %d, want 0", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := fallbackLaunches(t, dir); got != 1 {
				t.Fatalf("launches = %d, want 1", got)
			}
			assertFallbackEnv(t, dir, 1, false)
			assertFallbackSessionOff(t, p, session)
		})
	}
}
