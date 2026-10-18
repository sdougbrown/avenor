//go:build linux || darwin || freebsd

package terminal

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
	"CLAUDE_CODE_CHILD_SESSION",
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
	for _, key := range []string{"AVENOR_PARENT_RUN_ID", "CLAUDE_CODE_CHILD_SESSION"} {
		if observed[key] != unsetValue {
			t.Errorf("%s = %q, want it scrubbed", key, observed[key])
		}
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

// TestIdentityEnvPrefixQuotesHostileValues runs the rendered tmux prefix under
// a real shell: the exported values must arrive byte-for-byte and the label
// must not execute as command substitution.
func TestIdentityEnvPrefixQuotesHostileValues(t *testing.T) {
	provenance := testProvenance()
	keys := identityObservationKeys
	var script strings.Builder
	script.WriteString(identityEnvPrefix(provenance))
	for _, key := range keys {
		fmt.Fprintf(&script, "printf '%%s=%%s\\n' %s \"$(printf '%%s' \"${%s-%s}\" | od -An -tx1 | tr -d ' \\n')\"\n",
			key, key, unsetValue)
	}

	cmd := exec.Command("sh", "-c", script.String())
	cmd.Env = append(cmd.Environ(),
		"AVENOR_PARENT_RUN_ID=parent-superseded",
		runtime.EnvPhase+"=parent-phase",
		runtime.EnvAttempt+"=42",
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("prefixed command failed: %v; output: %s", err, out)
	}

	observed := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		key, encoded, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("malformed observation line %q", line)
		}
		value, err := hex.DecodeString(encoded)
		if err != nil {
			t.Fatalf("decode %s: %v", key, err)
		}
		observed[key] = string(value)
	}
	for _, entry := range provenance.IdentityEnv() {
		key, value, _ := strings.Cut(entry, "=")
		if observed[key] != value {
			t.Errorf("%s = %q, want %q", key, observed[key], value)
		}
	}
	if observed["AVENOR_PARENT_RUN_ID"] != unsetValue {
		t.Errorf("AVENOR_PARENT_RUN_ID = %q, want it unset", observed["AVENOR_PARENT_RUN_ID"])
	}
	if strings.Contains(string(out), "pwned") {
		t.Errorf("label was executed by the shell: %s", out)
	}
}

func TestPTYLauncherAddsLaunchIdentity(t *testing.T) {
	poisonParentIdentity(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	command := observerBody(obsPath) + "printf 'READY\\n'; sleep 30"

	provenance := testProvenance()
	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{
		Name:       "identity-pty",
		Command:    command,
		Provenance: provenance,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer session.Kill(context.Background())

	assertIdentityObservation(t, readIdentityObservation(t, obsPath), provenance)
}

// isolateTmuxServer points tmux at a private socket directory and detaches
// from any server this process inherited from, so the pane is the only child
// this test owns.
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

func TestTmuxLauncherAddsLaunchIdentityInPane(t *testing.T) {
	isolateTmuxServer(t)
	poisonParentIdentity(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	command := observerBody(obsPath) + "sleep 30"

	provenance := testProvenance()
	session, err := (TmuxLauncher{}).Start(context.Background(), StartOptions{
		Name:       "identity-tmux",
		Command:    command,
		Provenance: provenance,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer session.Kill(context.Background())

	assertIdentityObservation(t, readIdentityObservation(t, obsPath), provenance)
}

// TestTmuxPaneInheritsPoisonedServerEnv pins the reason the prefix exists: a
// pane inherits the tmux server's environment, which avenor does not own, so
// inherited identity must be removed inside the command.
func TestTmuxPaneInheritsPoisonedServerEnv(t *testing.T) {
	isolateTmuxServer(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	command := observerBody(obsPath) + "sleep 30"

	// Start the private server with poisoned identity in its environment.
	server := exec.Command("tmux", "new-session", "-d", "-s", "identity-poison", "sh", "-c", "sleep 30")
	server.Env = append(server.Environ(),
		runtime.EnvRuntimeID+"=server-runtime",
		runtime.EnvPhase+"=server-phase",
		"AVENOR_PARENT_RUN_ID=server-superseded",
		runtime.EnvRuntimeID+"_SUFFIX=near-miss",
		"AVENOR_TREE_BUDGET=7",
		"AVENOR_RUN_ID=broker-run",
		"AVENOR_BROKER_TOKEN=broker-token",
	)
	if out, err := server.CombinedOutput(); err != nil {
		t.Fatalf("start poisoned tmux server: %v: %s", err, out)
	}

	provenance := testProvenance()
	session, err := (TmuxLauncher{}).Start(context.Background(), StartOptions{
		Name:       "identity-pane",
		Command:    command,
		Provenance: provenance,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer session.Kill(context.Background())

	assertIdentityObservation(t, readIdentityObservation(t, obsPath), provenance)
}
