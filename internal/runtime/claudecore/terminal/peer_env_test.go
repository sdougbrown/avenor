package terminal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/token"
)

// The tmux path cannot give a pane an environment: a pane inherits the tmux
// server's. So a provisioned peer credential reaches the child through the
// command prefix — every inherited name is unset first, then the launch's own
// values are sourced from this launch's env file. The values stay out of the
// argv itself, which any local process can read.
func TestTmuxCommandSourcesProvisionedPeerCredentialsFromAFile(t *testing.T) {
	opts := StartOptions{
		Name:       "avenor-test",
		Dir:        "/tmp/x",
		Cols:       220,
		Rows:       50,
		Command:    "exec claude",
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_7", Label: "child-a"},
		Env: []string{
			"AVENOR_BROKER_URL=http://127.0.0.1:9",
			"AVENOR_RUN_ID=rt_7",
			"AVENOR_BROKER_TOKEN=tok-that-must-not-be-argv",
			"not-an-entry",
		},
	}
	envFile, err := writeLaunchEnvFile(opts.Env)
	if err != nil {
		t.Fatalf("writeLaunchEnvFile: %v", err)
	}
	defer func() { _ = os.RemoveAll(filepath.Dir(envFile)) }()

	args := tmuxNewSessionArgs(opts, envFile)
	shellCmd := args[len(args)-1]

	for _, arg := range args {
		if strings.Contains(arg, "tok-that-must-not-be-argv") {
			t.Errorf("tmux argv element = %q, want no provisioned value in it", arg)
		}
	}
	for _, name := range runtime.PeerEnvNames() {
		if !strings.Contains(shellCmd, " "+name) {
			t.Errorf("tmux command = %q, want %s unset before it is exported", shellCmd, name)
		}
	}
	if !strings.Contains(shellCmd, ". '"+envFile+"'; ") {
		t.Errorf("tmux command = %q, want it to source %q", shellCmd, envFile)
	}
	// The label travels once, as the launch identity, which is not a secret.
	if !strings.Contains(shellCmd, "export AVENOR_LABEL='child-a';") {
		t.Errorf("tmux command = %q, want the launch's own label", shellCmd)
	}
	if strings.Contains(shellCmd, "not-an-entry") {
		t.Errorf("tmux command = %q, want the malformed entry dropped", shellCmd)
	}
	if !strings.HasSuffix(shellCmd, "exec claude") {
		t.Errorf("tmux command = %q, want the command last", shellCmd)
	}

	info, err := os.Stat(envFile)
	if err != nil {
		t.Fatalf("stat env file: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("env file mode = %o, want 600", got)
	}
	dirInfo, err := os.Stat(filepath.Dir(envFile))
	if err != nil {
		t.Fatalf("stat env dir: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Errorf("env dir mode = %o, want 700", got)
	}

	source, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatalf("read env file: %v", err)
	}
	for _, want := range []string{
		"export AVENOR_BROKER_URL='http://127.0.0.1:9'; ",
		"export AVENOR_RUN_ID='rt_7'; ",
		"export AVENOR_BROKER_TOKEN='tok-that-must-not-be-argv'; ",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("env file = %q, want it to contain %q", source, want)
		}
	}
}

// A launch that provisions nothing writes no file and still strips the names:
// the tmux server may hold a peer's credentials from a session started days
// ago, and a child that kept them would join the broker as that run.
func TestTmuxPrefixStripsInheritedPeerCredentials(t *testing.T) {
	args := tmuxNewSessionArgs(StartOptions{
		Name:    "avenor-test",
		Dir:     "/tmp/x",
		Cols:    220,
		Rows:    50,
		Command: "exec claude",
	}, "")
	shellCmd := args[len(args)-1]
	for _, name := range runtime.PeerEnvNames() {
		if !strings.Contains(shellCmd, "unset ") || !strings.Contains(shellCmd, " "+name) {
			t.Errorf("tmux command = %q, want %s unset", shellCmd, name)
		}
		if strings.Contains(shellCmd, "export "+name+"=") {
			t.Errorf("tmux command = %q, want %s not exported", shellCmd, name)
		}
	}
	if strings.Contains(shellCmd, ". '") {
		t.Errorf("tmux command = %q, want no env file sourced", shellCmd)
	}
}

// A caller-supplied Env is appended after every scrub layer, so without its own
// filter it would reintroduce exactly what those layers keep out — here a
// messaging token and the parent-link credential — or overwrite the launch's
// identity. Provisioned peer credentials are the entries it may still carry.
func TestTmuxPrefixRefusesCallerSuppliedScrubbedNames(t *testing.T) {
	opts := StartOptions{
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
	}
	// The filter is the file's writer, so a refused entry is one the rendered
	// launch never carries at all.
	source := launchEnvSource(opts.Env)
	shellCmd := tmuxNewSessionArgs(opts, "")[len(tmuxNewSessionArgs(opts, ""))-1]

	for _, refused := range []string{
		"CLAUDE_CODE_MESSAGING_TOKEN=",
		token.EnvBrokerParentToken,
		"spoofed-label",
		"AVENOR_SESSION_NAME",
	} {
		if strings.Contains(source, refused) {
			t.Errorf("launch env = %q, want it to refuse %q", source, refused)
		}
	}
	if !strings.Contains(source, "export AVENOR_BROKER_URL='http://127.0.0.1:9'; ") {
		t.Errorf("launch env = %q, want the provisioned peer credential exported", source)
	}
	if !strings.Contains(shellCmd, "export AVENOR_LABEL='child-a';") {
		t.Errorf("tmux command = %q, want the launch's own label", shellCmd)
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
