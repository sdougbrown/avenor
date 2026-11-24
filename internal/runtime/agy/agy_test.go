package agy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

// closeableProvider is the provider surface these tests drive: everything the
// runtime.Provider contract promises, plus ResumeWithOptions and Close.
type closeableProvider interface {
	runtime.Provider
	ResumeWithOptions(context.Context, string, runtime.StartOptions) (runtime.Session, error)
	Close() error
}

func newProviderWithDeps(t *testing.T, opts runtime.StartOptions, d deps) closeableProvider {
	t.Helper()
	p, ok := newWithDeps(opts, d).(closeableProvider)
	if !ok {
		t.Fatal("agy provider does not implement ResumeWithOptions/Close")
	}
	return p
}

// testDeps reports no retired transport and a resolvable avenor executable, so
// a test that cares about neither never touches process globals.
func testDeps() deps {
	return deps{
		getenv:     func(string) string { return "" },
		executable: func() (string, error) { return "/opt/avenor", nil },
	}
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// installOnPath prepends dir to PATH so a launch of bin "agy" resolves there.
func installOnPath(t *testing.T, dir string) {
	t.Helper()
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func waitForLog(t *testing.T, path, substr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(raw), substr) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %q in %s; log:\n%s", substr, path, raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readLog(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

func waitForLines(t *testing.T, path string, want []string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil && len(raw) > 0 {
			got := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
			if slices.Equal(got, want) {
				return
			}
		}
		if time.Now().After(deadline) {
			raw, _ := os.ReadFile(path)
			t.Fatalf("%s = %q, want %v", path, string(raw), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// argvRecorderBody renders a script that appends its argv and working
// directory to logPath, one entry per line.
func argvRecorderBody(logPath string) string {
	quoted := shellQuote(logPath)
	return "#!/bin/sh\nprintf '%s\\n' \"$0\" \"$@\" >> " + quoted + "\npwd >> " + quoted + "\n"
}

// wrapperRecorderBody renders a wrapper that records its own argv, drops the
// opaque tag the launch passed, and execs the backend behind it.
func wrapperRecorderBody(logPath string) string {
	return argvRecorderBody(logPath) + "shift\nexec \"$@\"\n"
}

// fakeAGYBody is a stand-in for `agy acp --stdio`: it speaks the stdio
// JSON-RPC NDJSON subset the backend uses, logs every frame it receives to
// __LOG__, and streams the events a turn produces. A prompt containing
// "permission" raises a permission request (answering it completes the turn);
// one containing "slow" delays its chunks so a test can overlap it, and one
// containing "hold" stays open until session/cancel ends it.
const fakeAGYBody = `log=__LOG__
pending=""
trap 'exit 0' TERM
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$log"
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"ses_fake_1"}}\n' "$id"
      ;;
    *'"method":"session/resume"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      ;;
    *'"method":"session/set_config_option"'*)
      case "$line" in
        *bad-model*)
          printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32602,"message":"Unknown model ID"}}\n' "$id"
          ;;
        *)
          printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
          ;;
      esac
      ;;
    *'"method":"session/prompt"'*)
      case "$line" in *slow*) sleep 1 ;; esac
      printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_fake_1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"chunk"}}}}\n'
      printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_fake_1","update":{"sessionUpdate":"tool_call","toolCallId":"call_1","title":"run"}}}\n'
      printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_fake_1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call_1","status":"running"}}}\n'
      case "$line" in
        *hold*)
          pending="$id"
          ;;
        *permission*)
          pending="$id"
          printf '{"jsonrpc":"2.0","id":9001,"method":"session/request_permission","params":{"sessionId":"ses_fake_1","toolCallId":"call_1","options":[{"optionId":"allow-once","kind":"allow_once","name":"Allow"},{"optionId":"reject-once","kind":"reject_once","name":"Reject"}]}}\n'
          ;;
        *)
          printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn","usage":{"inputTokens":11,"outputTokens":7,"totalTokens":18,"cachedReadTokens":2,"cachedWriteTokens":3,"thoughtTokens":5}}}\n' "$id"
          ;;
      esac
      ;;
    *'"method":"session/cancel"'*)
      if [ -n "$pending" ]; then
        printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"cancelled"}}\n' "$pending"
        pending=""
      fi
      ;;
    *'"method":"session/close"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      ;;
    *)
      if [ "$id" = "9001" ] && [ -n "$pending" ]; then
        printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_fake_1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call_1","status":"completed"}}}\n'
        printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn","usage":{"inputTokens":4,"outputTokens":2,"totalTokens":6,"cachedWriteTokens":1,"thoughtTokens":2}}}\n' "$pending"
        pending=""
      fi
      ;;
  esac
done
`

// installFakeAGY installs the fake `agy` on PATH. When obsPath is non-empty the
// child first records its environment there, so an identity test observes what
// the process itself saw.
func installFakeAGY(t *testing.T, logPath, obsPath string) string {
	t.Helper()
	body := "#!/bin/sh\n"
	if obsPath != "" {
		body += identitytest.Body(obsPath)
	}
	body += strings.ReplaceAll(fakeAGYBody, "__LOG__", shellQuote(logPath))
	dir := t.TempDir()
	path := filepath.Join(dir, "agy")
	writeFile(t, path, body)
	installOnPath(t, dir)
	return path
}

func eventKinds(evs []events.Event) []string {
	kinds := make([]string, len(evs))
	for i, ev := range evs {
		kinds[i] = ev.Event
	}
	return kinds
}

// collectUntil drains ch in order until done returns true and returns
// everything it saw, failing the test on timeout or an early close.
func collectUntil(t *testing.T, ch <-chan events.Event, timeout time.Duration, done func(events.Event) bool) []events.Event {
	t.Helper()
	var collected []events.Event
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("event channel closed early; saw %v", eventKinds(collected))
			}
			collected = append(collected, ev)
			if done(ev) {
				return collected
			}
		case <-deadline:
			t.Fatalf("timed out waiting for an event; saw %v", eventKinds(collected))
		}
	}
}

func assertPIDGone(t *testing.T, pid int) {
	t.Helper()
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("child process %d outlived Close: %v", pid, err)
	}
}

func TestCapabilities(t *testing.T) {
	tests := []struct {
		name      string
		transport string
	}{
		{name: "transport unset"},
		{name: "transport set", transport: "headless"},
	}
	for _, tt := range tests {
		for _, construct := range []struct {
			name string
			new  func() runtime.Provider
		}{
			{name: "New", new: New},
			{name: "NewWithOptions", new: func() runtime.Provider { return NewWithOptions(runtime.StartOptions{}) }},
		} {
			t.Run(tt.name+"/"+construct.name, func(t *testing.T) {
				t.Setenv(transportEnvVar, tt.transport)
				caps, err := construct.new().Capabilities(context.Background())
				if err != nil {
					t.Fatalf("Capabilities: %v", err)
				}
				if caps.Backend != "agy" {
					t.Errorf("Backend = %q, want agy", caps.Backend)
				}
				if !caps.Permissions {
					t.Error("Permissions = false, want true")
				}
				if !caps.Resume {
					t.Error("Resume = false, want true")
				}
				if !caps.ModelSelection {
					t.Error("ModelSelection = false, want true")
				}
				if caps.ExternalServerURL {
					t.Error("ExternalServerURL = true, want false")
				}
				if caps.SubprocessDiscovery {
					t.Error("SubprocessDiscovery = true, want false")
				}
			})
		}
	}
}

func TestTransportMigrationError(t *testing.T) {
	wantError := func(raw string) string {
		return fmt.Sprintf("AVENOR_AGY_TRANSPORT is no longer supported (got %q); the agy backend now uses ACP directly — unset AVENOR_AGY_TRANSPORT", raw)
	}

	for _, raw := range []string{"headless", "rpc", "auto", " ", "bogus"} {
		t.Run(fmt.Sprintf("value %q", raw), func(t *testing.T) {
			logPath := filepath.Join(t.TempDir(), "acp.log")
			installFakeAGY(t, logPath, "")
			p := newProviderWithDeps(t, runtime.StartOptions{Dir: t.TempDir()}, deps{
				getenv:     func(string) string { return raw },
				executable: os.Executable,
			})

			ctx := context.Background()
			if _, err := p.Start(ctx, runtime.StartOptions{}); err == nil || err.Error() != wantError(raw) {
				t.Fatalf("Start err = %v, want %q", err, wantError(raw))
			}
			if _, err := p.Resume(ctx, "ses_1"); err == nil || err.Error() != wantError(raw) {
				t.Fatalf("Resume err = %v, want %q", err, wantError(raw))
			}
			if _, err := p.ResumeWithOptions(ctx, "ses_1", runtime.StartOptions{}); err == nil || err.Error() != wantError(raw) {
				t.Fatalf("ResumeWithOptions err = %v, want %q", err, wantError(raw))
			}
			if _, err := os.Stat(logPath); !os.IsNotExist(err) {
				t.Fatalf("a child process was spawned before the migration error: %v", err)
			}
		})
	}

	t.Run("unset transport succeeds", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "acp.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{Dir: t.TempDir()}, testDeps())
		defer func() { _ = p.Close() }()

		ses, err := p.Start(context.Background(), runtime.StartOptions{})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if ses.SessionID != "ses_fake_1" {
			t.Fatalf("SessionID = %q, want ses_fake_1", ses.SessionID)
		}
	})

	t.Run("empty session id precedes transport check", func(t *testing.T) {
		p := newProviderWithDeps(t, runtime.StartOptions{}, deps{
			getenv:     func(string) string { return "rpc" },
			executable: os.Executable,
		})
		if _, err := p.Resume(context.Background(), ""); err == nil || err.Error() != "session id is required" {
			t.Fatalf("Resume empty sessionID = %v, want 'session id is required'", err)
		}
		if _, err := p.ResumeWithOptions(context.Background(), "", runtime.StartOptions{}); err == nil || err.Error() != "session id is required" {
			t.Fatalf("ResumeWithOptions empty sessionID = %v, want 'session id is required'", err)
		}
	})
}

func TestLaunchArgvAndCwd(t *testing.T) {
	const forbidden = "--dangerously-skip-permissions"

	t.Run("start bare", func(t *testing.T) {
		dir := t.TempDir()
		logPath := filepath.Join(t.TempDir(), "argv.log")
		bin := filepath.Join(t.TempDir(), "agy")
		writeFile(t, bin, argvRecorderBody(logPath))
		installOnPath(t, filepath.Dir(bin))

		p := newProviderWithDeps(t, runtime.StartOptions{Dir: dir}, testDeps())
		_, _ = p.Start(context.Background(), runtime.StartOptions{})
		_ = p.Close()

		waitForLines(t, logPath, []string{bin, "acp", "--stdio", "--add-dir", dir, dir})
		if strings.Contains(readLog(t, logPath), forbidden) {
			t.Fatalf("launch passed %s", forbidden)
		}
	})

	t.Run("start with agent", func(t *testing.T) {
		dir := t.TempDir()
		logPath := filepath.Join(t.TempDir(), "argv.log")
		bin := filepath.Join(t.TempDir(), "agy")
		writeFile(t, bin, argvRecorderBody(logPath))
		installOnPath(t, filepath.Dir(bin))

		p := newProviderWithDeps(t, runtime.StartOptions{Dir: dir}, testDeps())
		_, _ = p.Start(context.Background(), runtime.StartOptions{Agent: "reviewer"})
		_ = p.Close()

		waitForLines(t, logPath, []string{bin, "acp", "--stdio", "--add-dir", dir, "--agent", "reviewer", dir})
	})

	t.Run("start with wrapper prefixes argv and keeps cwd", func(t *testing.T) {
		dir := t.TempDir()
		agyLog := filepath.Join(t.TempDir(), "agy.log")
		wrapperLog := filepath.Join(t.TempDir(), "wrapper.log")
		bin := filepath.Join(t.TempDir(), "agy")
		writeFile(t, bin, argvRecorderBody(agyLog))
		wrapper := filepath.Join(t.TempDir(), "wrapper.sh")
		writeFile(t, wrapper, wrapperRecorderBody(wrapperLog))
		installOnPath(t, filepath.Dir(bin))

		p := newProviderWithDeps(t, runtime.StartOptions{
			Dir:           dir,
			LaunchWrapper: []string{wrapper, "sentinel"},
		}, testDeps())
		_, _ = p.Start(context.Background(), runtime.StartOptions{})
		_ = p.Close()

		waitForLines(t, wrapperLog, []string{wrapper, "sentinel", "agy", "acp", "--stdio", "--add-dir", dir, dir})
		waitForLines(t, agyLog, []string{bin, "acp", "--stdio", "--add-dir", dir, dir})
	})

	t.Run("cold resume with wrapper prefixes argv", func(t *testing.T) {
		dir := t.TempDir()
		agyLog := filepath.Join(t.TempDir(), "agy.log")
		wrapperLog := filepath.Join(t.TempDir(), "wrapper.log")
		bin := filepath.Join(t.TempDir(), "agy")
		writeFile(t, bin, argvRecorderBody(agyLog))
		wrapper := filepath.Join(t.TempDir(), "wrapper.sh")
		writeFile(t, wrapper, wrapperRecorderBody(wrapperLog))
		installOnPath(t, filepath.Dir(bin))

		p := newProviderWithDeps(t, runtime.StartOptions{
			Dir:           dir,
			LaunchWrapper: []string{wrapper, "sentinel"},
		}, testDeps())
		_, _ = p.Resume(context.Background(), "ses_cold")
		_ = p.Close()

		waitForLines(t, wrapperLog, []string{wrapper, "sentinel", "agy", "acp", "--stdio", "--add-dir", dir, dir})
		waitForLines(t, agyLog, []string{bin, "acp", "--stdio", "--add-dir", dir, dir})
	})
}

func TestIdentityChildEnv(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	logPath := filepath.Join(t.TempDir(), "acp.log")
	installFakeAGY(t, logPath, obsPath)

	provenance := identitytest.Provenance("rt_child")
	p := newProviderWithDeps(t, runtime.StartOptions{Dir: t.TempDir()}, testDeps())
	defer func() { _ = p.Close() }()

	if _, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: provenance,
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	observed := identitytest.Read(t, obsPath)
	identitytest.Assert(t, observed, provenance)
	// This backend provisions no peer credential, so the shared scrub is the
	// only thing keeping an inherited one out of the child.
	identitytest.AssertPeerEnv(t, observed, nil)
}

func TestExecutableFailureWithBroker(t *testing.T) {
	newBroker := func(t *testing.T) *broker.Broker {
		t.Helper()
		b := broker.New("")
		if err := b.Start(); err != nil {
			t.Fatalf("start broker: %v", err)
		}
		t.Cleanup(func() { _ = b.Stop() })
		return b
	}

	tests := []struct {
		name string
		call func(p closeableProvider, b *broker.Broker) error
	}{
		{
			name: "Start",
			call: func(p closeableProvider, b *broker.Broker) error {
				_, err := p.Start(context.Background(), runtime.StartOptions{Broker: b, RuntimeID: "run_exe"})
				return err
			},
		},
		{
			name: "ResumeWithOptions",
			call: func(p closeableProvider, b *broker.Broker) error {
				_, err := p.ResumeWithOptions(context.Background(), "ses_cold", runtime.StartOptions{Broker: b, RuntimeID: "run_exe"})
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBroker(t)
			logPath := filepath.Join(t.TempDir(), "acp.log")
			installFakeAGY(t, logPath, "")
			exeErr := errors.New("no avenor executable")
			p := newProviderWithDeps(t, runtime.StartOptions{Dir: t.TempDir()}, deps{
				getenv:     func(string) string { return "" },
				executable: func() (string, error) { return "", exeErr },
			})
			defer func() { _ = p.Close() }()

			err := tt.call(p, b)
			if err == nil {
				t.Fatal("expected executable resolution to fail the launch")
			}
			if !errors.Is(err, exeErr) {
				t.Fatalf("err = %v, want it to wrap %v", err, exeErr)
			}
			if !strings.Contains(err.Error(), "agy: resolve avenor executable for channel-tools") {
				t.Fatalf("err = %v, want the channel-tools resolution context", err)
			}
			// Resolution happens before the client launches, so no child ever
			// runs: no session/new, session/resume, or poller can exist.
			if _, statErr := os.Stat(logPath); !os.IsNotExist(statErr) {
				t.Fatalf("child process started before executable resolution: %v", statErr)
			}
		})
	}
}

func TestSessionStartAndMCPServers(t *testing.T) {
	t.Run("broker supplies channel-tools", func(t *testing.T) {
		b := broker.New("")
		if err := b.Start(); err != nil {
			t.Fatalf("start broker: %v", err)
		}
		defer func() { _ = b.Stop() }()

		logPath := filepath.Join(t.TempDir(), "acp.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
		defer func() { _ = p.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		ses, err := p.Start(ctx, runtime.StartOptions{
			Dir:       t.TempDir(),
			Broker:    b,
			RuntimeID: "run_mcp",
			Model:     "gemini-4-argon-high",
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}

		evCh, err := p.Events(ctx, ses.SessionID)
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		got := collectUntil(t, evCh, 5*time.Second, func(ev events.Event) bool { return ev.Event == "session.start" })
		if len(got) != 1 {
			t.Fatalf("events before prompt = %v, want only session.start", eventKinds(got))
		}
		start := got[0]
		if start.Fields["backend"] != "agy" {
			t.Errorf("session.start backend = %v, want agy", start.Fields["backend"])
		}
		if start.Fields["model"] != "gemini-4-argon-high" {
			t.Errorf("session.start model = %v, want gemini-4-argon-high", start.Fields["model"])
		}
		if start.Fields["session_id"] != ses.SessionID {
			t.Errorf("session.start session_id = %v, want %v", start.Fields["session_id"], ses.SessionID)
		}

		waitForLog(t, logPath, `"method":"session/new"`)
		log := readLog(t, logPath)
		for _, want := range []string{
			`"mcpServers":[{"name":"avenor-channel-tools","command":"/opt/avenor","args":["channel-tools","--run-id","run_mcp","--broker-url","` + b.URL() + `"],"env":[{"name":"AVENOR_BROKER_TOKEN","value":"`,
			`"}]}]`,
		} {
			if !strings.Contains(log, want) {
				t.Fatalf("session/new missing %q; log:\n%s", want, log)
			}
		}
	})

	t.Run("no broker sends an empty server list", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "acp.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
		defer func() { _ = p.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if _, err := p.Start(ctx, runtime.StartOptions{Dir: t.TempDir()}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		waitForLog(t, logPath, `"method":"session/new"`)
		if log := readLog(t, logPath); !strings.Contains(log, `"mcpServers":[]`) {
			t.Fatalf("session/new without a broker did not send mcpServers []; log:\n%s", log)
		}
	})
}

func TestConfigureSessionModel(t *testing.T) {
	t.Run("model applied", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "acp.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
		defer func() { _ = p.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if _, err := p.Start(ctx, runtime.StartOptions{Dir: t.TempDir(), Model: "gemini-4-argon-high"}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		waitForLog(t, logPath, `"method":"session/set_config_option"`)
		log := readLog(t, logPath)
		if !strings.Contains(log, `"configId":"model"`) || !strings.Contains(log, `"value":"gemini-4-argon-high"`) {
			t.Fatalf("set_config_option did not select the model; log:\n%s", log)
		}
	})

	t.Run("no model skips configuration", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "acp.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
		defer func() { _ = p.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if _, err := p.Start(ctx, runtime.StartOptions{Dir: t.TempDir()}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		waitForLog(t, logPath, `"method":"session/new"`)
		if log := readLog(t, logPath); strings.Contains(log, "session/set_config_option") {
			t.Fatalf("session config was set without a model; log:\n%s", log)
		}
	})

	t.Run("unknown model fails Start and closes the session", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "acp.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
		defer func() { _ = p.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, err := p.Start(ctx, runtime.StartOptions{Dir: t.TempDir(), Model: "bad-model"})
		if err == nil || !strings.Contains(err.Error(), "-32602") {
			t.Fatalf("Start err = %v, want the -32602 unknown-model error", err)
		}
		waitForLog(t, logPath, `"method":"session/close"`)
		if err := p.Prompt(ctx, "ses_fake_1", "hi"); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("Prompt after failed configure = %v, want the session to be unregistered", err)
		}
	})

	t.Run("unknown model fails a cold resume and closes the session", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "acp.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
		defer func() { _ = p.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, err := p.ResumeWithOptions(ctx, "ses_fake_1", runtime.StartOptions{Dir: t.TempDir(), Model: "bad-model"})
		if err == nil || !strings.Contains(err.Error(), "-32602") {
			t.Fatalf("ResumeWithOptions err = %v, want the -32602 unknown-model error", err)
		}
		waitForLog(t, logPath, `"method":"session/close"`)
		if err := p.Prompt(ctx, "ses_fake_1", "hi"); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("Prompt after failed configure = %v, want the session to be unregistered", err)
		}
	})
}

func TestPromptStreamsEventsAndUsage(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "acp.log")
	installFakeAGY(t, logPath, "")
	p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ses, err := p.Start(ctx, runtime.StartOptions{Dir: t.TempDir(), Model: "gemini-4-argon-high"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	evCh, err := p.Events(ctx, ses.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	promptErr := make(chan error, 1)
	go func() { promptErr <- p.Prompt(ctx, ses.SessionID, "say hi") }()

	got := collectUntil(t, evCh, 10*time.Second, func(ev events.Event) bool { return ev.Event == "session.end" })
	if err := <-promptErr; err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	want := []string{"session.start", "agent.message_chunk", "tool.call", "tool.call_update", "session.end"}
	if kinds := eventKinds(got); !slices.Equal(kinds, want) {
		t.Fatalf("events = %v, want %v", kinds, want)
	}
	end := got[len(got)-1]
	if end.SessionID != ses.SessionID {
		t.Errorf("session.end SessionID = %q, want %q", end.SessionID, ses.SessionID)
	}
	if end.Fields["stop_reason"] != "end_turn" {
		t.Errorf("session.end stop_reason = %v, want end_turn", end.Fields["stop_reason"])
	}
	usage, ok := end.Fields["usage"].(map[string]any)
	if !ok {
		t.Fatalf("session.end usage = %T, want a map", end.Fields["usage"])
	}
	for key, wantValue := range map[string]float64{
		"input_tokens":        11,
		"output_tokens":       7,
		"total_tokens":        18,
		"cached_read_tokens":  2,
		"cached_write_tokens": 3,
		"thought_tokens":      5,
	} {
		if usage[key] != wantValue {
			t.Errorf("usage[%q] = %v, want %v", key, usage[key], wantValue)
		}
	}

	// A sequential follow-up turn on the same session streams again.
	if err := p.Prompt(ctx, ses.SessionID, "again"); err != nil {
		t.Fatalf("follow-up Prompt: %v", err)
	}
	follow := collectUntil(t, evCh, 10*time.Second, func(ev events.Event) bool { return ev.Event == "session.end" })
	if kinds := eventKinds(follow); !slices.Contains(kinds, "agent.message_chunk") {
		t.Fatalf("follow-up turn events = %v, want a message chunk", kinds)
	}
}

func TestPromptPermissionAndAnswer(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "acp.log")
	installFakeAGY(t, logPath, "")
	p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ses, err := p.Start(ctx, runtime.StartOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	evCh, err := p.Events(ctx, ses.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	promptErr := make(chan error, 1)
	go func() { promptErr <- p.Prompt(ctx, ses.SessionID, "please permission") }()

	got := collectUntil(t, evCh, 10*time.Second, func(ev events.Event) bool { return ev.Event == "permission.request" })
	permission := got[len(got)-1]
	requestID, _ := permission.Fields["request_id"].(string)
	if requestID == "" {
		t.Fatalf("permission.request missing request_id: %+v", permission)
	}
	if err := p.AnswerPermission(ctx, ses.SessionID, requestID, runtime.PermissionResponse{Allow: true}); err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}

	after := collectUntil(t, evCh, 10*time.Second, func(ev events.Event) bool { return ev.Event == "session.end" })
	if err := <-promptErr; err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if kinds := eventKinds(after); !slices.Contains(kinds, "tool.call_update") {
		t.Fatalf("post-permission events = %v, want a tool call update", kinds)
	}
	waitForLog(t, logPath, `"optionId":"allow-once"`)
}

func TestOverlappingPromptRejected(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "acp.log")
	installFakeAGY(t, logPath, "")
	p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ses, err := p.Start(ctx, runtime.StartOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	first := make(chan error, 1)
	go func() { first <- p.Prompt(ctx, ses.SessionID, "hold first") }()
	waitForLog(t, logPath, `"text":"hold first"`)

	if err := p.Prompt(ctx, ses.SessionID, "second"); err == nil || !strings.Contains(err.Error(), "already in flight") {
		t.Fatalf("overlapping Prompt = %v, want an already-in-flight rejection", err)
	}
	if err := p.Cancel(ctx, ses.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := <-first; err != nil {
		t.Fatalf("first Prompt: %v", err)
	}
}

func TestCancelIsIdempotentAndTurnStaysUsable(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "acp.log")
	installFakeAGY(t, logPath, "")
	p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ses, err := p.Start(ctx, runtime.StartOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	evCh, err := p.Events(ctx, ses.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	promptErr := make(chan error, 1)
	go func() { promptErr <- p.Prompt(ctx, ses.SessionID, "hold cancel") }()
	waitForLog(t, logPath, `"text":"hold cancel"`)

	if err := p.Cancel(ctx, ses.SessionID); err != nil {
		t.Fatalf("first Cancel: %v", err)
	}
	if err := p.Cancel(ctx, ses.SessionID); err != nil {
		t.Fatalf("second Cancel: %v", err)
	}
	got := collectUntil(t, evCh, 10*time.Second, func(ev events.Event) bool { return ev.Event == "session.end" })
	if err := <-promptErr; err != nil {
		t.Fatalf("cancelled Prompt: %v", err)
	}
	end := got[len(got)-1]
	if end.Fields["stop_reason"] != "cancelled" {
		t.Fatalf("cancelled turn stop_reason = %v, want cancelled", end.Fields["stop_reason"])
	}
	waitForLog(t, logPath, `"method":"session/cancel"`)

	// The cancelled turn is terminal; the next turn on the same session works.
	if err := p.Prompt(ctx, ses.SessionID, "after cancel"); err != nil {
		t.Fatalf("Prompt after cancel: %v", err)
	}
}

func TestColdResume(t *testing.T) {
	newBroker := func(t *testing.T) *broker.Broker {
		t.Helper()
		b := broker.New("")
		if err := b.Start(); err != nil {
			t.Fatalf("start broker: %v", err)
		}
		t.Cleanup(func() { _ = b.Stop() })
		return b
	}

	// startSession launches one provider and returns its session ID after
	// closing the process, so the resume below is a genuine cross-process one.
	startSession := func(t *testing.T, d deps) string {
		t.Helper()
		logPath := filepath.Join(t.TempDir(), "start.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{}, d)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		ses, err := p.Start(ctx, runtime.StartOptions{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return ses.SessionID
	}

	t.Run("ResumeWithOptions carries broker servers and model", func(t *testing.T) {
		b := newBroker(t)
		sessionID := startSession(t, testDeps())

		logPath := filepath.Join(t.TempDir(), "resume.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
		defer func() { _ = p.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		resumed, err := p.ResumeWithOptions(ctx, sessionID, runtime.StartOptions{
			Dir:       t.TempDir(),
			Broker:    b,
			RuntimeID: "run_resume",
			Model:     "gemini-4-argon-high",
		})
		if err != nil {
			t.Fatalf("ResumeWithOptions: %v", err)
		}
		if resumed.SessionID != sessionID {
			t.Fatalf("resumed SessionID = %q, want %q", resumed.SessionID, sessionID)
		}
		waitForLog(t, logPath, `"method":"session/resume"`)
		log := readLog(t, logPath)
		for _, want := range []string{
			`"mcpServers":[{"name":"avenor-channel-tools"`,
			`"channel-tools","--run-id","run_resume"`,
			`"--broker-url","` + b.URL() + `"`,
			`"configId":"model"`,
			`"value":"gemini-4-argon-high"`,
		} {
			if !strings.Contains(log, want) {
				t.Fatalf("session/resume missing %q; log:\n%s", want, log)
			}
		}

		evCh, err := p.Events(ctx, resumed.SessionID)
		if err != nil {
			t.Fatalf("Events after cold resume: %v", err)
		}
		if err := p.Prompt(ctx, resumed.SessionID, "after cold resume"); err != nil {
			t.Fatalf("Prompt after cold resume: %v", err)
		}
		got := collectUntil(t, evCh, 10*time.Second, func(ev events.Event) bool { return ev.Event == "session.end" })
		if kinds := eventKinds(got); !slices.Contains(kinds, "agent.message_chunk") || !slices.Contains(kinds, "session.end") {
			t.Fatalf("post-resume events = %v, want message_chunk and session.end", kinds)
		}
	})

	t.Run("plain Resume with constructor broker carries mcpServers and model", func(t *testing.T) {
		b := newBroker(t)
		sessionID := startSession(t, testDeps())

		logPath := filepath.Join(t.TempDir(), "resume-ctor.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{
			Dir:       t.TempDir(),
			Broker:    b,
			RuntimeID: "run_ctor_resume",
			Model:     "gemini-4-argon-high",
		}, testDeps())
		defer func() { _ = p.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		resumed, err := p.Resume(ctx, sessionID)
		if err != nil {
			t.Fatalf("plain Resume with constructor broker: %v", err)
		}
		if resumed.SessionID != sessionID {
			t.Fatalf("resumed SessionID = %q, want %q", resumed.SessionID, sessionID)
		}
		waitForLog(t, logPath, `"method":"session/resume"`)
		log := readLog(t, logPath)
		for _, want := range []string{
			`"mcpServers":[{"name":"avenor-channel-tools"`,
			`"channel-tools","--run-id","run_ctor_resume"`,
			`"--broker-url","` + b.URL() + `"`,
			`"configId":"model"`,
			`"value":"gemini-4-argon-high"`,
		} {
			if !strings.Contains(log, want) {
				t.Fatalf("plain Resume with constructor broker missing %q; log:\n%s", want, log)
			}
		}
	})

	t.Run("plain Resume sends an empty server list", func(t *testing.T) {
		sessionID := startSession(t, testDeps())

		logPath := filepath.Join(t.TempDir(), "resume.log")
		installFakeAGY(t, logPath, "")
		p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())
		defer func() { _ = p.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		resumed, err := p.Resume(ctx, sessionID)
		if err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if resumed.SessionID != sessionID {
			t.Fatalf("resumed SessionID = %q, want %q", resumed.SessionID, sessionID)
		}
		waitForLog(t, logPath, `"method":"session/resume"`)
		if log := readLog(t, logPath); !strings.Contains(log, `"mcpServers":[]`) {
			t.Fatalf("plain resume did not send mcpServers []; log:\n%s", log)
		}
	})
}

func TestCloseTerminatesChild(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "acp.log")
	installFakeAGY(t, logPath, "")
	p := newProviderWithDeps(t, runtime.StartOptions{}, testDeps())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ses, err := p.Start(ctx, runtime.StartOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if ses.PID <= 0 {
		t.Fatalf("Start PID = %d, want the child's pid", ses.PID)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertPIDGone(t, ses.PID)
}
