package terminal

import (
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/token"
)

// The tmux path cannot give a pane an environment: a pane inherits the tmux
// server's. So a provisioned peer credential reaches the child only through the
// command prefix — every inherited name is unset first, then the launch's own
// values are exported.
func TestTmuxPrefixExportsProvisionedPeerCredentials(t *testing.T) {
	args := tmuxNewSessionArgs(StartOptions{
		Name:       "avenor-test",
		Dir:        "/tmp/x",
		Cols:       220,
		Rows:       50,
		Command:    "exec claude",
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_7", Label: "child-a"},
		Env: []string{
			"AVENOR_BROKER_URL=http://127.0.0.1:9",
			"AVENOR_RUN_ID=rt_7",
			"AVENOR_BROKER_TOKEN=tok",
			"not-an-entry",
		},
	})
	shellCmd := args[len(args)-1]

	for _, name := range runtime.PeerEnvNames() {
		if !strings.Contains(shellCmd, " "+name) && !strings.Contains(shellCmd, " "+name+"=") {
			t.Errorf("tmux command = %q, want %s unset before it is exported", shellCmd, name)
		}
	}
	for _, want := range []string{
		"export AVENOR_BROKER_URL='http://127.0.0.1:9';",
		"export AVENOR_RUN_ID='rt_7';",
		"export AVENOR_BROKER_TOKEN='tok';",
		// The label travels once, as the launch identity.
		"export AVENOR_LABEL='child-a';",
	} {
		if !strings.Contains(shellCmd, want) {
			t.Errorf("tmux command = %q, want it to contain %q", shellCmd, want)
		}
	}
	if strings.Contains(shellCmd, "not-an-entry") {
		t.Errorf("tmux command = %q, want the malformed entry dropped", shellCmd)
	}
	if !strings.HasSuffix(shellCmd, "exec claude") {
		t.Errorf("tmux command = %q, want the command last", shellCmd)
	}
}

// A launch that provisions nothing still strips the names: the tmux server may
// hold a peer's credentials from a session started days ago, and a child that
// kept them would join the broker as that run.
func TestTmuxPrefixStripsInheritedPeerCredentials(t *testing.T) {
	args := tmuxNewSessionArgs(StartOptions{
		Name:    "avenor-test",
		Dir:     "/tmp/x",
		Cols:    220,
		Rows:    50,
		Command: "exec claude",
	})
	shellCmd := args[len(args)-1]
	for _, name := range runtime.PeerEnvNames() {
		if !strings.Contains(shellCmd, "unset ") || !strings.Contains(shellCmd, " "+name) {
			t.Errorf("tmux command = %q, want %s unset", shellCmd, name)
		}
		if strings.Contains(shellCmd, "export "+name+"=") {
			t.Errorf("tmux command = %q, want %s not exported", shellCmd, name)
		}
	}
}

// A caller-supplied Env is appended after every scrub layer, so without its own
// filter it would reintroduce exactly what those layers keep out — here a
// messaging token and the parent-link credential — or overwrite the launch's
// identity. Provisioned peer credentials are the entries it may still carry.
func TestTmuxPrefixRefusesCallerSuppliedScrubbedNames(t *testing.T) {
	args := tmuxNewSessionArgs(StartOptions{
		Name:       "avenor-test",
		Dir:        "/tmp/x",
		Cols:       220,
		Rows:       50,
		Command:    "exec claude",
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_7", Label: "child-a"},
		Env: []string{
			"AVENOR_BROKER_URL=http://127.0.0.1:9",
			"CLAUDE_CODE_MESSAGING_TOKEN=caller-secret",
			token.EnvBrokerParentToken + "=caller-parent-secret",
			"AVENOR_LABEL=spoofed-label",
			"AVENOR_SESSION_NAME=spoofed-session",
		},
	})
	shellCmd := args[len(args)-1]

	for _, refused := range []string{
		"export CLAUDE_CODE_MESSAGING_TOKEN=",
		"export " + token.EnvBrokerParentToken + "=",
		"export AVENOR_LABEL='spoofed-label';",
		"export AVENOR_SESSION_NAME=",
	} {
		if strings.Contains(shellCmd, refused) {
			t.Errorf("tmux command = %q, want it to refuse %q", shellCmd, refused)
		}
	}
	if !strings.Contains(shellCmd, "export AVENOR_LABEL='child-a';") {
		t.Errorf("tmux command = %q, want the launch's own label", shellCmd)
	}
	if !strings.Contains(shellCmd, "export AVENOR_BROKER_URL='http://127.0.0.1:9';") {
		t.Errorf("tmux command = %q, want the provisioned peer credential exported", shellCmd)
	}
}

func TestPeerEnvStripsInheritedAndAppendsProvisioned(t *testing.T) {
	got := PeerEnv([]string{
		"PATH=/bin",
		"AVENOR_RUN_ID=parent-run",
		"AVENOR_BROKER_TOKEN",
		"AVENOR_BROKER_URL=http://stale.invalid",
		"AVENOR_RUN_ID_SUFFIX=near-miss",
	}, []string{
		"AVENOR_RUN_ID=rt_7",
		"no-value-entry",
	})
	want := []string{
		"PATH=/bin",
		"AVENOR_RUN_ID_SUFFIX=near-miss",
		"AVENOR_RUN_ID=rt_7",
	}
	if len(got) != len(want) {
		t.Fatalf("PeerEnv = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("PeerEnv = %q, want %q", got, want)
		}
	}
}

// The provisioned set is the whole peer vocabulary plus whatever the caller
// needs, and nothing named in the scrub lists survives it.
func TestPeerEnvRefusesScrubbedCallerEntries(t *testing.T) {
	got := PeerEnv([]string{"PATH=/bin"}, []string{
		"AVENOR_RUN_ID=rt_7",
		"AVENOR_BROKER_TOKEN=tok",
		"CLAUDE_CODE_CHILD_SESSION=1",
		"CLAUDE_CODE_MESSAGING_TOKEN=caller-secret",
		token.EnvBrokerParentToken + "=caller-parent-secret",
		runtime.EnvLabel + "=spoofed-label",
		"AVENOR_SESSION_NAME=spoofed-session",
		"AVENOR_TREE_BUDGET=7",
	})
	want := []string{
		"PATH=/bin",
		"AVENOR_RUN_ID=rt_7",
		"AVENOR_BROKER_TOKEN=tok",
		"AVENOR_TREE_BUDGET=7",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("PeerEnv = %q, want %q", got, want)
	}
}
