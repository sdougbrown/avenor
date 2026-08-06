package terminal

import "strings"

// parentClaudeSessionEnv names the variables that identify the Claude Code
// session avenor itself was launched from. A hosted terminal must not inherit
// them. The process it starts is an independent session with its own
// --session-id, not a continuation of whoever started avenor.
//
// CLAUDE_CODE_CHILD_SESSION is the one that breaks a run outright. Claude Code
// reads it as a nested-session marker and disables transcript persistence. It
// says so in a startup banner:
//
//	⚠ Transcript saving is off — inherited CLAUDE_CODE_CHILD_SESSION marker
//
// The transcript is the only end-of-turn signal the claude backend has. So the
// turn completes on screen while avenor waits for an end_turn record that is
// never written. The run hangs for its whole timeout with no error. This fires
// whenever avenor is launched from inside Claude Code, which is how the avenor
// MCP plugin runs it.
//
// The other two carry the parent's identity rather than changing behaviour. They
// are dropped so a hosted session cannot be mistaken for its launcher.
var parentClaudeSessionEnv = []string{
	"CLAUDE_CODE_CHILD_SESSION",
	"CLAUDE_CODE_SESSION_ID",
	"CLAUDE_PID",
}

// ScrubParentClaudeEnv returns env without the parent's Claude Code session
// identity. Entries are "NAME=VALUE" as in os.Environ; a bare name with no "="
// is matched as a whole. Matching is exact, so a longer variable that merely
// starts with a scrubbed name survives.
func ScrubParentClaudeEnv(env []string) []string {
	out := make([]string, 0, len(env))
	for _, entry := range env {
		name := entry
		if eq := strings.IndexByte(entry, '='); eq >= 0 {
			name = entry[:eq]
		}
		if !isParentClaudeSessionVar(name) {
			out = append(out, entry)
		}
	}
	return out
}

func isParentClaudeSessionVar(name string) bool {
	for _, scrubbed := range parentClaudeSessionEnv {
		if name == scrubbed {
			return true
		}
	}
	return false
}

// UnsetParentClaudeEnvPrefix returns a shell prefix that strips the same
// variables from a command, terminated so it composes directly with one.
//
// tmux is why this exists alongside ScrubParentClaudeEnv. A tmux pane inherits
// the tmux *server's* environment, which avenor does not own. That server can
// have been started long before, from inside a Claude Code session. It then
// hands the marker to every pane it creates for the rest of its life. Setting
// the child's environment does not help there, and `tmux new-session -e` only
// assigns a variable, it cannot remove one. Stripping inside the command works
// whichever environment the server holds.
//
// This uses the `unset` builtin rather than `env -u` because the wrapped commands
// start with `exec`, which is also a builtin. `env -u X exec claude` sends env
// looking for a binary named "exec". `unset` also adds no process, so the pane
// PID stays claude's own.
func UnsetParentClaudeEnvPrefix() string {
	var b strings.Builder
	b.WriteString("unset")
	for _, name := range parentClaudeSessionEnv {
		b.WriteString(" ")
		b.WriteString(name)
	}
	b.WriteString("; ")
	return b.String()
}
