package terminal

import (
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/shellquote"
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
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_7"},
		Env: []string{
			"AVENOR_BROKER_URL=http://127.0.0.1:9",
			"AVENOR_RUN_ID=rt_7",
			"AVENOR_BROKER_TOKEN=tok",
			"AVENOR_SESSION_NAME=child 'a'",
			"not-an-entry",
		},
	})
	shellCmd := args[len(args)-1]

	for _, name := range avenorPeerEnv {
		if !strings.Contains(shellCmd, " "+name) && !strings.Contains(shellCmd, " "+name+"=") {
			t.Errorf("tmux command = %q, want %s unset before it is exported", shellCmd, name)
		}
	}
	for _, want := range []string{
		"export AVENOR_BROKER_URL='http://127.0.0.1:9';",
		"export AVENOR_RUN_ID='rt_7';",
		"export AVENOR_BROKER_TOKEN='tok';",
		"export AVENOR_SESSION_NAME=" + shellquote.Quote("child 'a'") + ";",
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
	for _, name := range avenorPeerEnv {
		if !strings.Contains(shellCmd, "unset ") || !strings.Contains(shellCmd, " "+name) {
			t.Errorf("tmux command = %q, want %s unset", shellCmd, name)
		}
		if strings.Contains(shellCmd, "export "+name+"=") {
			t.Errorf("tmux command = %q, want %s not exported", shellCmd, name)
		}
	}
}

func TestPeerEnvStripsInheritedAndAppendsProvisioned(t *testing.T) {
	got := PeerEnv([]string{
		"PATH=/bin",
		"AVENOR_RUN_ID=parent-run",
		"AVENOR_BROKER_TOKEN",
		"AVENOR_BROKER_URL=http://stale.invalid",
		"AVENOR_SESSION_NAME=parent-session",
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
