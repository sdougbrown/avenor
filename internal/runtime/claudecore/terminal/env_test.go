package terminal

import (
	"slices"
	"strings"
	"testing"
)

func TestScrubParentClaudeEnvDropsSessionIdentity(t *testing.T) {
	in := []string{
		"PATH=/usr/bin",
		"CLAUDE_CODE_CHILD_SESSION=1",
		"CLAUDE_CODE_SESSION_ID=abc-123",
		"CLAUDE_PID=4242",
		"HOME=/home/x",
	}
	got := ScrubParentClaudeEnv(in)
	want := []string{"PATH=/usr/bin", "HOME=/home/x"}
	if !slices.Equal(got, want) {
		t.Fatalf("ScrubParentClaudeEnv = %q, want %q", got, want)
	}
}

func TestScrubParentClaudeEnvKeepsUnrelatedAndNearMisses(t *testing.T) {
	// Only exact names are dropped. A longer name that merely starts with a
	// scrubbed one is a different variable and must survive.
	in := []string{
		"CLAUDE_CODE_CHILD_SESSION_EXTRA=keep",
		"CLAUDE_PIDGIN=keep",
		"CLAUDECODE=1",
		"CLAUDE_CODE_ENTRYPOINT=cli",
		"NOT_AN_ASSIGNMENT",
	}
	got := ScrubParentClaudeEnv(in)
	if !slices.Equal(got, in) {
		t.Fatalf("ScrubParentClaudeEnv = %q, want it unchanged", got)
	}
}

func TestScrubParentClaudeEnvEmptyInput(t *testing.T) {
	got := ScrubParentClaudeEnv([]string{})
	if !slices.Equal(got, []string{}) {
		t.Fatalf("ScrubParentClaudeEnv([]string{}) = %q, want empty", got)
	}
}

func TestScrubParentClaudeEnvDropsValuelessNames(t *testing.T) {
	got := ScrubParentClaudeEnv([]string{"CLAUDE_CODE_CHILD_SESSION", "PATH=/bin"})
	if !slices.Equal(got, []string{"PATH=/bin"}) {
		t.Fatalf("ScrubParentClaudeEnv = %q, want the bare name dropped too", got)
	}
}

func TestUnsetParentClaudeEnvPrefixCoversEveryScrubbedName(t *testing.T) {
	prefix := UnsetParentClaudeEnvPrefix()
	// `unset`, not `env -u`: the wrapped commands begin with the `exec` builtin,
	// which env cannot run.
	if !strings.HasPrefix(prefix, "unset ") {
		t.Fatalf("prefix = %q, want it to start with \"unset \"", prefix)
	}
	if !strings.HasSuffix(prefix, "; ") {
		t.Fatalf("prefix = %q, want it terminated with \"; \" so it composes with a command", prefix)
	}
	for _, name := range parentClaudeSessionEnv {
		if !strings.Contains(prefix, " "+name) {
			t.Errorf("prefix %q does not unset %s", prefix, name)
		}
	}
}
