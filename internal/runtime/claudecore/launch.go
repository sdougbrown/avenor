package claudecore

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/shellquote"
)

// Stop reasons for a Claude session that ended without ever running a turn.
// All report status=failed: the old behaviour reported status=done with an
// empty output, which is byte-for-byte what a real end_turn that said nothing
// looks like, so callers could not tell a typo from a completed run.
const (
	// StopReasonAgentNotFound means Claude Code rejected the --agent name.
	StopReasonAgentNotFound = "agent_not_found"
	// StopReasonLaunchFailed means claude exited before avenor submitted a
	// prompt, for a reason avenor could not name.
	StopReasonLaunchFailed = "launch_failed"
	// StopReasonTerminalNotReady means claude never showed its input box, so
	// avenor never submitted the first prompt.
	StopReasonTerminalNotReady = "terminal_not_ready"
)

// agentNotFoundBanner is the invariant middle of the one-line error Claude Code
// writes to stderr when --agent names an agent it cannot resolve, before exiting
// with status 1:
//
//	--agent 'general' not found. Available agents: advisor, centaur, horse, ...
//
// Both ends of that line carry per-run values — the rejected name on the left,
// the installed roster on the right — so the match is anchored between them, the
// same way channelPolicyBlocked in the claudechannel provider anchors on its
// banner's invariant middle rather than its per-run "(server:...)" tail.
// Keeping "Available agents:" in the needle is what prevents a false positive:
// "not found" alone is ordinary agent output (a missing file, an empty grep) and
// shows up in healthy panes.
const agentNotFoundBanner = "not found. Available agents:"

// agentNotFoundAgent extracts the rejected name from the banner's left side.
// The parse is diagnostic only — detection never depends on it succeeding.
var agentNotFoundAgent = regexp.MustCompile(`--agent '([^']*)' ` + regexp.QuoteMeta(agentNotFoundBanner))

// maxDiagnosticText bounds the roster and the stderr tail copied into events.
// Everything after the banner is roster, and reading to the end of the text is
// what recovers a roster that wrapped onto following lines — but it is still
// scraped output, so cap it rather than forwarding a whole screen or log.
const maxDiagnosticText = 512

// AgentNotFound is the diagnosis for a launch Claude Code refused because it did
// not recognise the requested agent.
type AgentNotFound struct {
	// Agent is the rejected --agent value. Empty when the banner did not parse.
	Agent string
	// Available is the roster Claude Code listed, whitespace-collapsed. Empty
	// when the capture did not include it.
	Available string
}

// DetectAgentNotFound reports whether text holds the unrecognised-agent banner,
// along with whatever diagnostics it could recover from it.
func DetectAgentNotFound(text string) (AgentNotFound, bool) {
	idx := strings.Index(text, agentNotFoundBanner)
	if idx < 0 {
		return AgentNotFound{}, false
	}
	fail := AgentNotFound{
		Available: truncateRunes(collapseSpace(text[idx+len(agentNotFoundBanner):]), maxDiagnosticText),
	}
	if m := agentNotFoundAgent.FindStringSubmatch(text); m != nil {
		fail.Agent = m[1]
	}
	return fail, true
}

// Message renders the diagnosis for the event stream. The roster is the single
// most useful thing for whoever typo'd the agent name, so it goes in the message
// and not only in a structured field.
func (a AgentNotFound) Message() string {
	msg := "claude rejected the --agent value: agent not found"
	if a.Agent != "" {
		msg = fmt.Sprintf("claude rejected --agent %q: agent not found", a.Agent)
	}
	if a.Available != "" {
		msg += "; available agents: " + a.Available
	}
	return msg
}

// LaunchCommand renders the shell command that starts claude inside a terminal.
//
// `exec` replaces the shell with claude so that tmux's #{pane_pid} reports
// claude's own PID and the terminal exits when claude does.
//
// stderr goes to stderrLog because a fatal launch banner is otherwise
// unreachable. Claude Code writes it to stderr and exits within ~300ms; tmux
// then destroys the session together with its pane, and `capture-pane` shows a
// blank grid for the whole life of that pane anyway (confirmed at 25ms sampling,
// and with remain-on-exit holding the dead pane open). A file survives both. The
// redirect costs nothing on a healthy run: Claude Code draws its TUI on stdout
// and leaves stderr empty.
func LaunchCommand(args []string, stderrLog string, launchWrapper []string) string {
	parts := make([]string, 0, len(args)+len(launchWrapper)+3)
	parts = append(parts, "exec")
	// The wrapper argv goes in front of claude so the shell execs into the
	// wrapper, which owns the claude process tree. Each element is quoted so it
	// stays exactly one shell word, matching the argv semantics of the
	// non-terminal backends; unquoted, the shell would parse wrapper text like
	// `;` or `$()` as command syntax.
	for _, element := range launchWrapper {
		parts = append(parts, shellquote.Quote(element))
	}
	parts = append(parts, "claude")
	for _, arg := range args {
		parts = append(parts, shellquote.Quote(arg))
	}
	if stderrLog != "" {
		parts = append(parts, "2>"+shellquote.Quote(stderrLog))
	}
	return strings.Join(parts, " ")
}

// CreateStderrLog creates the file LaunchCommand redirects claude's stderr into.
// The caller owns it and should remove it on teardown via Session.
func CreateStderrLog() (string, error) {
	f, err := os.CreateTemp("", "avenor-claude-stderr-*.log")
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

// RemoveStderrLog deletes the stderr log. Safe to call on a session that has
// none.
func (s *Session) RemoveStderrLog() {
	if s.StderrLog == "" {
		return
	}
	_ = os.Remove(s.StderrLog)
}

// maxStderrLogRead bounds how much of the log is read back. A refused launch
// prints one line and exits, so the head is where the diagnosis lives.
const maxStderrLogRead = 64 << 10

func (s *Session) readStderrLog() string {
	if s.StderrLog == "" {
		return ""
	}
	f, err := os.Open(s.StderrLog)
	if err != nil {
		return ""
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxStderrLogRead))
	if err != nil && len(raw) == 0 {
		return ""
	}
	return StripANSI(string(raw))
}

// ansiEscape matches the CSI and OSC sequences Claude Code colours its stderr
// with. They bracket the banner text rather than splitting it, so detection
// would survive without stripping — but the roster is read to end-of-text, and
// leaving escape bytes in it would put terminal control codes into the event
// stream and anything that renders it.
var ansiEscape = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)`)

// StripANSI removes terminal escape sequences from scraped output.
func StripANSI(s string) string {
	return ansiEscape.ReplaceAllString(s, "")
}

func collapseSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func truncateRunes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	// Cut on a rune boundary: the roster is scraped text and nothing guarantees
	// the byte limit lands between runes.
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// EmitTerminalGone emits the terminal session.end for a Claude process that
// exited on its own. Both providers call it from their sessionGone branch, which
// cannot tell a finished turn from a refused launch by itself — the alive poll
// sees the same dead terminal either way — so classify the exit first.
//
// MarkFinished is the existing guard against double-emitting: a run that already
// reported avenor_finish, a cancel, or a transcript end_turn keeps that verdict.
func (s *Session) EmitTerminalGone(ctx context.Context) {
	fail, agentNotFound := s.detectAgentNotFound(ctx)
	stderrTail := truncateRunes(collapseSpace(s.readStderrLog()), maxDiagnosticText)
	prompted := s.everPrompted()
	// The log has given up everything it has, so drop it here rather than relying
	// only on runSession's teardown defer: session.end is what lets a one-shot
	// caller exit, and a process that exits on that event can beat the defer,
	// leaving the temp file behind. The defer stays as the backstop for the paths
	// that never reach this function.
	s.RemoveStderrLog()

	if !s.MarkFinished() {
		return
	}
	switch {
	case agentNotFound:
		s.emitLaunchFailure(StopReasonAgentNotFound, fail.Message(), map[string]any{
			"agent":            fail.Agent,
			"available_agents": fail.Available,
		})
	case !prompted:
		// claude died before avenor pasted a prompt, so no turn ever ran and
		// "done" would be a lie. BuildArgs never passes a prompt on the command
		// line, and the channel push only reaches Claude after the first
		// terminal-delivered turn, so Prompted is the whole story here.
		msg := "claude exited before avenor submitted a prompt; the run never started"
		if stderrTail != "" {
			msg += "; stderr: " + stderrTail
		}
		// Under a launch wrapper claude's stderr is the wrapper's terminal, so
		// a crash leaves the log empty and only the pane shows it.
		s.emitLaunchFailure(StopReasonLaunchFailed, msg, map[string]any{
			"stderr": stderrTail,
			"pane":   s.paneText(ctx),
		})
	default:
		s.Emit(events.Event{
			Event:     "session.end",
			SessionID: s.SessionID,
			Fields: map[string]any{
				"status":      "done",
				"stop_reason": "end_turn",
			},
		})
	}
}

// FailUnreadyLaunch ends a session whose first prompt never found Claude's
// input box, and reports what the pane showed instead. Claude is still running
// there, parked on a screen avenor does not answer, such as a settings-approval
// or MCP-server dialog, so without a terminal status a caller waiting on
// session.end waits forever. Cancelling the session makes runSession kill the
// terminal.
//
// It leaves a session that already ran a turn alone: a later prompt that finds
// the pane busy is not a failed launch. A cancelled ctx means the session is
// already ending, so it reports nothing.
func (s *Session) FailUnreadyLaunch(ctx context.Context) {
	if s.everPrompted() {
		return
	}
	if ctx.Err() == nil && s.MarkFinished() {
		s.emitLaunchFailure(StopReasonTerminalNotReady,
			"claude never showed its input box, so avenor never submitted a prompt; the run never started",
			map[string]any{"pane": s.paneText(ctx)})
	}
	if s.CancelFn != nil {
		s.CancelFn()
	}
}

// paneText is the bottom of the pane as one line of text for a diagnostic,
// where a dialog's options or a crash's last lines are. Rules and blank lines
// are dropped, since a full-width rule alone would fill the budget. It goes in
// its own field, never in the error: failure text is read for provider limits,
// and a screen can say anything.
func (s *Session) paneText(ctx context.Context) string {
	if s.Term == nil {
		return ""
	}
	out, err := s.Term.Capture(ctx)
	if err != nil {
		return ""
	}
	var lines []string
	for _, line := range strings.Split(normalizePane(out), "\n") {
		var words []string
		for _, w := range strings.Fields(line) {
			if strings.Trim(w, "─") != "" {
				words = append(words, w)
			}
		}
		if len(words) > 0 {
			lines = append(lines, strings.Join(words, " "))
		}
	}
	text := strings.Join(lines, " | ")
	if len(text) <= maxDiagnosticText {
		return text
	}
	cut := len(text) - maxDiagnosticText
	for cut < len(text) && !utf8.RuneStart(text[cut]) {
		cut++
	}
	return text[cut:]
}

// detectAgentNotFound looks for the banner in the stderr log first — the only
// source that survives a tmux pane — then in whatever the terminal can still
// capture. A PTY keeps its vt10x screen after the child exits, so the pane is a
// useful second source there.
func (s *Session) detectAgentNotFound(ctx context.Context) (AgentNotFound, bool) {
	if fail, ok := DetectAgentNotFound(s.readStderrLog()); ok {
		return fail, true
	}
	if s.Term == nil {
		return AgentNotFound{}, false
	}
	out, err := s.Term.Capture(ctx)
	if err != nil {
		return AgentNotFound{}, false
	}
	return DetectAgentNotFound(out)
}

func (s *Session) everPrompted() bool {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return s.Prompted
}

// emitLaunchFailure emits the diagnostic event plus the terminal session.end for
// a run that never started. Callers must have won the MarkFinished race.
//
// The session.end is not optional. Cancelling the context instead leaves both
// providers' runSession ctx.Done branch killing the terminal and emitting
// nothing, so anything gating on a terminal status — avenor_status,
// avenor_result — waits forever. This mirrors the channel-blocked path in the
// claudechannel provider.
func (s *Session) emitLaunchFailure(stopReason, message string, extra map[string]any) {
	source := ""
	if s.Term != nil {
		source = s.Term.Kind()
	}
	diag := map[string]any{
		"error":       message,
		"stop_reason": stopReason,
		"source":      source,
	}
	end := map[string]any{
		"status":      "failed",
		"stop_reason": stopReason,
		"error":       message,
	}
	for k, v := range extra {
		diag[k] = v
		end[k] = v
	}
	s.Emit(events.Event{Event: "agent.launch_failed", SessionID: s.SessionID, Fields: diag})
	s.Emit(events.Event{Event: "session.end", SessionID: s.SessionID, Fields: end})
}
