package claude

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

var identityObservationKeys = []string{
	runtime.EnvRuntimeID,
	runtime.EnvExecutionID,
	runtime.EnvKind,
	runtime.EnvPhase,
	runtime.EnvIteration,
	runtime.EnvLabel,
	runtime.EnvAttempt,
	"AVENOR_PARENT_RUN_ID",
	runtime.EnvRuntimeID + "_SUFFIX",
	"AVENOR_TREE_BUDGET",
	"AVENOR_RUN_ID",
	"AVENOR_BROKER_TOKEN",
}

const unsetValue = "__UNSET__"

func observerBody(obsPath string) string {
	var b strings.Builder
	b.WriteString("observe() {\n")
	b.WriteString("  printf '%s=%s\\n' \"$1\" \"$(printf '%s' \"$2\" | od -An -tx1 | tr -d ' \\n')\"\n")
	b.WriteString("}\n")
	b.WriteString("{\n")
	for _, key := range identityObservationKeys {
		fmt.Fprintf(&b, "observe %s \"${%s-%s}\"\n", key, key, unsetValue)
	}
	fmt.Fprintf(&b, "} > %s\n", testShellQuote(obsPath))
	return b.String()
}

func testShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func readIdentityObservation(t *testing.T, path string) map[string]string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil &&
			strings.Contains(string(raw), "AVENOR_RUNTIME_ID=") &&
			strings.Contains(string(raw), identityObservationKeys[len(identityObservationKeys)-1]+"=") {
			observed := map[string]string{}
			for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
				key, encoded, ok := strings.Cut(line, "=")
				if !ok {
					t.Fatalf("malformed observation line %q", line)
				}
				value, err := hex.DecodeString(encoded)
				if err != nil {
					t.Fatalf("decode %s observation: %v", key, err)
				}
				observed[key] = string(value)
			}
			return observed
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for identity observation at %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func poisonParentIdentity(t *testing.T) {
	t.Helper()
	t.Setenv("AVENOR_CLAUDE_TERMINAL", "")
	t.Setenv(runtime.EnvRuntimeID, "parent-runtime")
	t.Setenv(runtime.EnvExecutionID, "parent-execution")
	t.Setenv(runtime.EnvKind, "parent-kind")
	t.Setenv(runtime.EnvPhase, "parent-phase")
	t.Setenv(runtime.EnvIteration, "99")
	t.Setenv(runtime.EnvLabel, "parent-label")
	t.Setenv(runtime.EnvAttempt, "42")
	t.Setenv("AVENOR_PARENT_RUN_ID", "parent-superseded")
	t.Setenv(runtime.EnvRuntimeID+"_SUFFIX", "near-miss")
	t.Setenv("AVENOR_TREE_BUDGET", "7")
	t.Setenv("AVENOR_RUN_ID", "broker-run")
	t.Setenv("AVENOR_BROKER_TOKEN", "broker-token")
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "parent-claude-marker")
}

// fakeClaudeIdentityChild installs a real `claude` child that answers the
// version preflight and records the allowlisted environment when launched.
func fakeClaudeIdentityChild(t *testing.T, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then\n  echo 'Claude Code 1.0.0 (fake)'\n  exit 0\nfi\n" +
		observerBody(obsPath) +
		"sleep 30\n"
	if err := os.WriteFile(filepath.Join(binDir, "claude"), []byte(body), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func isolateTmuxServer(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux not installed")
	}
	t.Setenv("TMUX_TMPDIR", t.TempDir())
	prior, had := os.LookupEnv("TMUX")
	if err := os.Unsetenv("TMUX"); err != nil {
		t.Fatalf("unset TMUX: %v", err)
	}
	t.Cleanup(func() {
		_ = exec.Command("tmux", "kill-server").Run()
		if had {
			_ = os.Setenv("TMUX", prior)
		}
	})
}

func testProvenance() *runtime.LaunchProvenance {
	return &runtime.LaunchProvenance{
		RuntimeID:   "rt_child",
		ExecutionID: "rt_child",
		Kind:        "team",
		Phase:       "member 'one' $(id) \nsecond",
		Label:       "label with 'quotes' $(echo pwned) `id`;done",
		Attempt:     2,
	}
}

func assertIdentityObservation(t *testing.T, observed map[string]string, want *runtime.LaunchProvenance) {
	t.Helper()
	for _, entry := range want.IdentityEnv() {
		key, value, _ := strings.Cut(entry, "=")
		if observed[key] != value {
			t.Errorf("%s = %q, want %q", key, observed[key], value)
		}
	}
	if observed["AVENOR_PARENT_RUN_ID"] != unsetValue {
		t.Errorf("AVENOR_PARENT_RUN_ID = %q, want it scrubbed", observed["AVENOR_PARENT_RUN_ID"])
	}
	if observed[runtime.EnvRuntimeID+"_SUFFIX"] != "near-miss" {
		t.Errorf("near-miss variable = %q, want it preserved", observed[runtime.EnvRuntimeID+"_SUFFIX"])
	}
	if observed["AVENOR_TREE_BUDGET"] != "7" {
		t.Errorf("AVENOR_TREE_BUDGET = %q, want 7", observed["AVENOR_TREE_BUDGET"])
	}
	if observed["AVENOR_RUN_ID"] != "broker-run" || observed["AVENOR_BROKER_TOKEN"] != "broker-token" {
		t.Errorf("broker pair = %q/%q, want it preserved", observed["AVENOR_RUN_ID"], observed["AVENOR_BROKER_TOKEN"])
	}
}

func stopClaudeSession(t *testing.T, p *Provider, sessionID string) {
	t.Helper()
	p.mu.Lock()
	s := p.sessions[sessionID]
	p.mu.Unlock()
	if s == nil {
		return
	}
	s.CancelFn()
	_ = s.Term.Kill(context.Background())
}

func TestIdentityChildEnv(t *testing.T) {
	t.Run("pty", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		fakeClaudeIdentityChild(t, obsPath)

		provenance := testProvenance()
		p := New().(*Provider)
		session, err := p.Start(context.Background(), runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: provenance,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer stopClaudeSession(t, p, session.SessionID)

		assertIdentityObservation(t, readIdentityObservation(t, obsPath), provenance)
	})

	t.Run("tmux", func(t *testing.T) {
		isolateTmuxServer(t)
		poisonParentIdentity(t)
		t.Setenv("AVENOR_CLAUDE_TERMINAL", "tmux")
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		fakeClaudeIdentityChild(t, obsPath)

		provenance := testProvenance()
		p := New().(*Provider)
		session, err := p.Start(context.Background(), runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: provenance,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer stopClaudeSession(t, p, session.SessionID)

		assertIdentityObservation(t, readIdentityObservation(t, obsPath), provenance)
	})
}
