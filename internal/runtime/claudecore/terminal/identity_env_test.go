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

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

// terminalObservationExtras is the parent Claude Code session marker the
// observer also records: a hosted terminal must not inherit it.
var terminalObservationExtras = []string{"CLAUDE_CODE_CHILD_SESSION"}

func poisonParentIdentity(t *testing.T) {
	t.Helper()
	identitytest.PoisonParent(t)
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "parent-claude-marker")
}

// TestIdentityEnvPrefixQuotesHostileValues runs the rendered tmux prefix under
// a real shell: the exported values must arrive byte-for-byte and the label
// must stay literal text instead of executing as command substitution.
func TestIdentityEnvPrefixQuotesHostileValues(t *testing.T) {
	provenance := identitytest.Provenance("rt_child")
	keys := identitytest.Keys()
	var script strings.Builder
	script.WriteString(identityEnvPrefix(provenance))
	for _, key := range keys {
		fmt.Fprintf(&script, "printf '%%s=%%s\\n' %s \"$(printf '%%s' \"${%s-%s}\" | od -An -tx1 | tr -d ' \\n')\"\n",
			key, key, identitytest.UnsetValue)
	}

	cmd := exec.Command("sh", "-c", script.String())
	cmd.Env = append(cmd.Environ(),
		"AVENOR_PARENT_RUN_ID=parent-superseded",
		runtime.EnvPhase+"=parent-phase",
		runtime.EnvAttempt+"=42",
		"AVENOR_TREE_BUDGET=7",
		runtime.EnvRuntimeID+"_SUFFIX=near-miss",
		"AVENOR_RUN_ID=broker-run",
		"AVENOR_BROKER_TOKEN=broker-token",
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
	identitytest.Assert(t, observed, provenance)
	if label := observed[runtime.EnvLabel]; !strings.Contains(label, "$(echo pwned)") {
		t.Errorf("label = %q, want the command substitution preserved as text", label)
	}
}

func TestPTYLauncherAddsLaunchIdentity(t *testing.T) {
	t.Run("identity", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		command := identitytest.Body(obsPath, terminalObservationExtras...) + "printf 'READY\\n'; sleep 30"

		provenance := identitytest.Provenance("rt_child")
		session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{
			Name:       "identity-pty",
			Command:    command,
			Provenance: provenance,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer session.Kill(context.Background())

		observed := identitytest.Read(t, obsPath)
		identitytest.Assert(t, observed, provenance, terminalObservationExtras...)
		identitytest.AssertBrokerPreserved(t, observed)
	})

	t.Run("nil identity clears inherited values", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		command := identitytest.Body(obsPath, terminalObservationExtras...) + "printf 'READY\\n'; sleep 30"

		session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{
			Name:    "identity-pty-nil",
			Command: command,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer session.Kill(context.Background())

		observed := identitytest.Read(t, obsPath)
		identitytest.AssertCleared(t, observed, terminalObservationExtras...)
		identitytest.AssertBrokerPreserved(t, observed)
	})
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
	t.Run("identity", func(t *testing.T) {
		isolateTmuxServer(t)
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		command := identitytest.Body(obsPath, terminalObservationExtras...) + "sleep 30"

		provenance := identitytest.Provenance("rt_child")
		session, err := (TmuxLauncher{}).Start(context.Background(), StartOptions{
			Name:       "identity-tmux",
			Command:    command,
			Provenance: provenance,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer session.Kill(context.Background())

		observed := identitytest.Read(t, obsPath)
		identitytest.Assert(t, observed, provenance, terminalObservationExtras...)
		identitytest.AssertBrokerPreserved(t, observed)
	})

	t.Run("nil identity clears inherited values", func(t *testing.T) {
		isolateTmuxServer(t)
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		command := identitytest.Body(obsPath, terminalObservationExtras...) + "sleep 30"

		session, err := (TmuxLauncher{}).Start(context.Background(), StartOptions{
			Name:    "identity-tmux-nil",
			Command: command,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer session.Kill(context.Background())

		observed := identitytest.Read(t, obsPath)
		identitytest.AssertCleared(t, observed, terminalObservationExtras...)
		identitytest.AssertBrokerPreserved(t, observed)
	})
}

// TestTmuxPaneInheritsPoisonedServerEnv pins the reason the prefix exists: a
// pane inherits the tmux server's environment, which avenor does not own, so
// inherited identity must be removed inside the command.
func TestTmuxPaneInheritsPoisonedServerEnv(t *testing.T) {
	isolateTmuxServer(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	command := identitytest.Body(obsPath, terminalObservationExtras...) + "sleep 30"

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

	provenance := identitytest.Provenance("rt_child")
	session, err := (TmuxLauncher{}).Start(context.Background(), StartOptions{
		Name:       "identity-pane",
		Command:    command,
		Provenance: provenance,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer session.Kill(context.Background())

	observed := identitytest.Read(t, obsPath)
	identitytest.Assert(t, observed, provenance, terminalObservationExtras...)
	identitytest.AssertBrokerPreserved(t, observed)
}
