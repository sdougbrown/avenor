package claudecore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

const testRoster = "advisor, centaur, claude, codex:codex-rescue, Explore, general-purpose, horse, mule, Plan, statusline-setup"

// agentNotFoundStderr is verbatim what Claude Code writes to stderr when --agent
// names an agent it cannot resolve, then exits with status 1. The colour codes
// are what FORCE_COLOR=1 produces, and the PTY launcher sets FORCE_COLOR=1.
const agentNotFoundStderr = "\x1b[0m\x1b[31m\x1b[31m--agent 'general' not found. Available agents: " +
	testRoster + "\x1b[39m\x1b[0m\n"

// agentNotFoundPane is the same banner as a pane scrape, where tmux and vt10x
// have already resolved the escapes away.
const agentNotFoundPane = "--agent 'general' not found. Available agents: " + testRoster + "\n"

func TestDetectAgentNotFound(t *testing.T) {
	tests := []struct {
		name          string
		text          string
		wantAgent     string
		wantAvailable string
	}{
		{
			name:          "pane scrape of the banner",
			text:          agentNotFoundPane,
			wantAgent:     "general",
			wantAvailable: testRoster,
		},
		{
			name:          "colourised stderr log",
			text:          StripANSI(agentNotFoundStderr),
			wantAgent:     "general",
			wantAvailable: testRoster,
		},
		{
			// The roster is read to end-of-text, not end-of-line, so a roster
			// that wrapped in a narrow pane still comes back whole.
			name:          "roster wrapped onto the next line",
			text:          "--agent 'genral' not found. Available agents: advisor, centaur, claude,\nhorse, mule, Plan\n",
			wantAgent:     "genral",
			wantAvailable: "advisor, centaur, claude, horse, mule, Plan",
		},
		{
			name:          "banner below the command that produced it",
			text:          "$ claude --agent general -p 'say hi'\n" + agentNotFoundPane,
			wantAgent:     "general",
			wantAvailable: testRoster,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fail, ok := DetectAgentNotFound(tc.text)
			if !ok {
				t.Fatalf("refused launch not detected; the run would report done with no output:\n%s", tc.text)
			}
			if fail.Agent != tc.wantAgent {
				t.Errorf("Agent = %q, want %q", fail.Agent, tc.wantAgent)
			}
			if fail.Available != tc.wantAvailable {
				t.Errorf("Available = %q, want %q", fail.Available, tc.wantAvailable)
			}
		})
	}
}

func TestDetectAgentNotFoundIgnoresHealthyOutput(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{
			name: "empty capture",
			text: "",
		},
		{
			name: "known-good ready pane",
			text: readyPane,
		},
		{
			// "not found" is ordinary agent output. Only the full phrase, with
			// the "Available agents:" tail, means a refused launch.
			name: "not found in unrelated turn output",
			text: testRule + "\n❯ read the config\n● Bash(cat config.toml)\n  cat: config.toml: not found\n" + testRule + "\n",
		},
		{
			name: "agent hitting a missing command",
			text: testRule + "\n❯ run the linter\n  zsh: command not found: golangci-lint\n" + testRule + "\n",
		},
		{
			name: "roster listed without a rejection",
			text: testRule + "\n❯ which agents exist?\n  Available agents: horse, mule, centaur\n" + testRule + "\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if fail, ok := DetectAgentNotFound(tc.text); ok {
				t.Fatalf("healthy output reported as a refused launch (agent %q):\n%s", fail.Agent, tc.text)
			}
		})
	}
}

func TestAgentNotFoundMessageCarriesTheDiagnostics(t *testing.T) {
	fail, ok := DetectAgentNotFound(agentNotFoundPane)
	if !ok {
		t.Fatal("banner not detected")
	}
	msg := fail.Message()
	for _, want := range []string{"general", "horse", "statusline-setup"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q is missing %q; whoever typo'd the name needs it", msg, want)
		}
	}
}

func TestStripANSILeavesTextIntact(t *testing.T) {
	got := StripANSI(agentNotFoundStderr)
	if strings.ContainsRune(got, '\x1b') {
		t.Errorf("escape bytes survived into %q", got)
	}
	if !strings.HasPrefix(got, "--agent 'general' not found.") {
		t.Errorf("stripped text lost its head: %q", got)
	}
	if !strings.Contains(got, testRoster) {
		t.Errorf("stripped text lost the roster: %q", got)
	}
}

func TestLaunchCommandRedirectsStderr(t *testing.T) {
	// Every word goes through ShellQuote, the redirect target included, so a path
	// with a space in it survives the shell.
	got := LaunchCommand([]string{"--agent", "my agent"}, "/tmp/log with space.log")
	want := `exec claude "--agent" "my agent" 2>"/tmp/log with space.log"`
	if got != want {
		t.Errorf("LaunchCommand = %q, want %q", got, want)
	}
	if got := LaunchCommand([]string{"--agent", "horse"}, ""); strings.Contains(got, "2>") {
		t.Errorf("no log requested but command redirects: %q", got)
	}
}

func TestEmitTerminalGoneFailsOnAgentNotFoundInStderrLog(t *testing.T) {
	// The stderr log is the only source that survives a tmux pane: tmux destroys
	// the session with the process, and the banner never reaches the pane grid.
	s := newTestSession(t, "")
	writeStderrLog(t, s, agentNotFoundStderr)

	s.EmitTerminalGone(s.Ctx)

	if ev := drainEvent(t, s); ev.Event != "agent.launch_failed" {
		t.Fatalf("first event = %q, want agent.launch_failed", ev.Event)
	}
	assertAgentNotFoundEnd(t, drainEvent(t, s))
	// A one-shot caller exits on session.end, which can beat runSession's
	// teardown defer, so the log has to be gone by the time it is emitted.
	if _, err := os.Stat(s.StderrLog); !os.IsNotExist(err) {
		t.Errorf("stderr log %s survived the terminal event (stat err = %v)", s.StderrLog, err)
	}
}

func TestEmitTerminalGoneFailsOnAgentNotFoundInPane(t *testing.T) {
	// A PTY keeps its vt10x screen after the child exits, so the pane is a
	// second source there even with no stderr log.
	s := newTestSession(t, agentNotFoundPane)

	s.EmitTerminalGone(s.Ctx)

	if ev := drainEvent(t, s); ev.Event != "agent.launch_failed" {
		t.Fatalf("first event = %q, want agent.launch_failed", ev.Event)
	}
	assertAgentNotFoundEnd(t, drainEvent(t, s))
}

func TestEmitTerminalGoneFailsWhenNoPromptWasEverSubmitted(t *testing.T) {
	// Any other launch that dies before avenor pastes a prompt: no turn ran, so
	// "done" would be a lie even though avenor cannot name the reason.
	s := newTestSession(t, "")
	writeStderrLog(t, s, "\x1b[31mnode: bad option: --effort\x1b[0m\n")

	s.EmitTerminalGone(s.Ctx)

	diag := drainEvent(t, s)
	if diag.Event != "agent.launch_failed" {
		t.Fatalf("first event = %q, want agent.launch_failed", diag.Event)
	}
	end := drainEvent(t, s)
	if got := end.Fields["stop_reason"]; got != StopReasonLaunchFailed {
		t.Errorf("stop_reason = %v, want %s", got, StopReasonLaunchFailed)
	}
	if got := end.Fields["status"]; got != "failed" {
		t.Errorf("status = %v, want failed", got)
	}
	msg, _ := end.Fields["error"].(string)
	if !strings.Contains(msg, "bad option: --effort") {
		t.Errorf("error = %q, want claude's stderr in it", msg)
	}
	if strings.ContainsRune(msg, '\x1b') {
		t.Errorf("error carries terminal escapes: %q", msg)
	}
}

func TestEmitTerminalGoneReportsDoneAfterARealTurn(t *testing.T) {
	s := newTestSession(t, readyPane)
	s.Mu.Lock()
	s.Prompted = true
	s.Mu.Unlock()

	s.EmitTerminalGone(s.Ctx)

	end := drainEvent(t, s)
	if end.Event != "session.end" {
		t.Fatalf("event = %q, want session.end", end.Event)
	}
	if got := end.Fields["stop_reason"]; got != "end_turn" {
		t.Errorf("stop_reason = %v, want end_turn", got)
	}
	if got := end.Fields["status"]; got != "done" {
		t.Errorf("status = %v, want done", got)
	}
}

func TestEmitTerminalGoneStaysQuietWhenAlreadyFinished(t *testing.T) {
	s := newTestSession(t, agentNotFoundPane)
	if !s.MarkFinished() {
		t.Fatal("fresh session already finished")
	}

	s.EmitTerminalGone(s.Ctx)

	select {
	case ev := <-s.Events:
		t.Fatalf("emitted %q after the session was already finished", ev.Event)
	default:
	}
}

func newTestSession(t *testing.T, pane string) *Session {
	t.Helper()
	s := NewSession(context.Background(), SessionOptions{
		SessionID: "ses-launch",
		EventsBuf: 8,
	})
	t.Cleanup(s.CancelFn)
	s.Term = terminal.NewFakeSession("test-term", 1, pane)
	return s
}

func writeStderrLog(t *testing.T, s *Session, content string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stderr.log")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write stderr log: %v", err)
	}
	s.StderrLog = path
}

func drainEvent(t *testing.T, s *Session) events.Event {
	t.Helper()
	select {
	case ev := <-s.Events:
		return ev
	default:
		t.Fatal("no event emitted")
		return events.Event{}
	}
}

func TestTruncateRunesPreservesValidUTF8(t *testing.T) {
	// "hello 🌍 world" — the emoji is a 4-byte rune (U+1F30D). Bytes 5-8.
	s := "hello 🌍 world"

	// Byte limit that falls inside the emoji's trailing bytes (byte 7).
	got := truncateRunes(s, 7)
	if !utf8.ValidString(got) {
		t.Errorf("truncated string is not valid UTF-8: %q", got)
	}
	// The cut should retreat to before the emoji start.
	want := "hello "
	if got != want {
		t.Errorf("truncateRunes(%q, 7) = %q, want %q", s, got, want)
	}

	// Byte limit that lands exactly on the emoji start byte (byte 6).
	// max=6 fits "hello " exactly; the emoji starts at byte 6 and needs
	// 4 more bytes, so it cannot be included.
	got = truncateRunes(s, 6)
	if !utf8.ValidString(got) {
		t.Errorf("truncated string is not valid UTF-8: %q", got)
	}
	want = "hello "
	if got != want {
		t.Errorf("truncateRunes(%q, 6) = %q, want %q", s, got, want)
	}

	// Limit larger than the string length returns the original.
	got = truncateRunes(s, 20)
	if got != s {
		t.Errorf("truncateRunes(%q, 20) = %q, want %q", s, got, s)
	}

	// Multi-rune CJK text: each character is 3 bytes in UTF-8.
	cjk := "你好世界"
	got = truncateRunes(cjk, 5)
	if !utf8.ValidString(got) {
		t.Errorf("truncated CJK string is not valid UTF-8: %q", got)
	}
	// Byte 5 falls inside the second character's 3-byte sequence; should keep
	// only the first character (bytes 0-2), so result is "你".
	want = "你"
	if got != want {
		t.Errorf("truncateRunes(%q, 5) = %q, want %q", cjk, got, want)
	}
}

func TestReadStderrLogEmptyAndMissingPath(t *testing.T) {
	t.Run("empty path returns empty string", func(t *testing.T) {
		s := newTestSession(t, "")
		if got := s.readStderrLog(); got != "" {
			t.Errorf("readStderrLog with empty path = %q, want empty", got)
		}
	})

	t.Run("empty file returns empty string", func(t *testing.T) {
		s := newTestSession(t, "")
		s.StderrLog = filepath.Join(t.TempDir(), "stderr.log")
		if err := os.WriteFile(s.StderrLog, nil, 0o600); err != nil {
			t.Fatalf("create empty stderr log: %v", err)
		}
		if got := s.readStderrLog(); got != "" {
			t.Errorf("readStderrLog with empty file = %q, want empty", got)
		}
	})

	t.Run("missing file returns empty string", func(t *testing.T) {
		s := newTestSession(t, "")
		s.StderrLog = filepath.Join(t.TempDir(), "missing.log")
		if got := s.readStderrLog(); got != "" {
			t.Errorf("readStderrLog with missing file = %q, want empty", got)
		}
	})
}

func assertAgentNotFoundEnd(t *testing.T, end events.Event) {
	t.Helper()
	if end.Event != "session.end" {
		t.Fatalf("event = %q, want session.end", end.Event)
	}
	if got := end.Fields["status"]; got != "failed" {
		t.Errorf("status = %v, want failed", got)
	}
	if got := end.Fields["stop_reason"]; got != StopReasonAgentNotFound {
		t.Errorf("stop_reason = %v, want %s", got, StopReasonAgentNotFound)
	}
	if got := end.Fields["agent"]; got != "general" {
		t.Errorf("agent = %v, want general", got)
	}
	if got := end.Fields["available_agents"]; got != testRoster {
		t.Errorf("available_agents = %v, want the roster", got)
	}
	msg, _ := end.Fields["error"].(string)
	if !strings.Contains(msg, "general") || !strings.Contains(msg, "statusline-setup") {
		t.Errorf("error = %q, want the rejected name and the available list", msg)
	}
}
