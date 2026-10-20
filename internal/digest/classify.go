package digest

import (
	"regexp"
	"strconv"
	"strings"
)

const (
	TagMilestone = "MILESTONE"
	TagFinding   = "FINDING"
	TagActivity  = "ACTIVITY"
)

// confidenceRe matches "confidence: N%" or "confidence: N" (at a word boundary)
// where N is a decimal integer, case-insensitively. Capturing group 1 is the
// numeric string. The trailing `(?:%|\b)` requires either a literal "%" or a
// word boundary so "confidence: 60ms" does not match (the "m" is a word char
// adjacent to the digits, not a boundary), while "confidence: 60%" and
// "confidence: 60 " and bare "60" at end-of-string all still match.
var confidenceRe = regexp.MustCompile(`(?i)confidence:\s*(\d+)(?:%|\b)`)

// Classify returns a tag (MILESTONE, FINDING, or ACTIVITY) for a parsed event.
// The event map is what DigestLine has already unmarshaled.
//
// Rules (in priority order):
//
//  1. MILESTONE: session.end, session.renamed, permission.request, and tool.call /
//     tool.call_update where kind=="commit" or kind=="task" with
//     status=="completed", or where the title/command contains "git commit".
//
//  2. FINDING: agent.message_chunk, agent.thought_chunk, user.message_chunk where
//     the content text contains a deliberate finding signal (confidence ≥60,
//     explicit [finding] prefix, or multi-word phrase: "reviewer flagged",
//     "correction needed", "failed test").
//
//  3. ACTIVITY: everything else.
func Classify(event map[string]any) string {
	name := stringField(event, "event")
	switch name {
	case "session.end":
		return TagMilestone
	case "session.renamed":
		return TagMilestone
	case "permission.request":
		return TagMilestone
	case "avenor.loop.start":
		return TagMilestone
	case "avenor.phase.end":
		return TagMilestone
	case "avenor.loop.end":
		return TagMilestone
	case "avenor.phase.start":
		return TagActivity
	case "avenor.retry", "avenor.error":
		return TagMilestone
	case "permission.response":
		return TagActivity
	case "agent.status":
		switch stringField(event, "phase") {
		case "done", "waiting":
			return TagMilestone
		default:
			return TagActivity
		}
	case "tool.call", "tool.call_update":
		return classifyTool(event)
	case "agent.message_chunk", "agent.thought_chunk", "user.message_chunk":
		return classifyChunk(event)
	default:
		return TagActivity
	}
}

// classifyTool returns MILESTONE when the tool event signals a commit action,
// and ACTIVITY otherwise.
func classifyTool(event map[string]any) string {
	kind := strings.ToLower(stringField(event, "kind"))
	status := strings.ToLower(stringField(event, "status"))
	title := stringField(event, "title")
	command := stringField(event, "command")

	// kind == "commit" is always a milestone.
	if kind == "commit" {
		return TagMilestone
	}

	// kind == "task" that has completed is a milestone.
	if kind == "task" && status == "completed" {
		return TagMilestone
	}

	// Title or command containing "git commit" is a milestone (case-insensitive).
	if containsGitCommit(title) || containsGitCommit(command) {
		return TagMilestone
	}

	return TagActivity
}

// containsGitCommit reports whether s contains the substring "git commit"
// case-insensitively.
func containsGitCommit(s string) bool {
	return strings.Contains(strings.ToLower(s), "git commit")
}

// classifyChunk returns FINDING when the chunk's content text matches a
// deliberate finding signal, and ACTIVITY otherwise. We lean conservative:
// single generic words like "error" or "found" are excluded to avoid
// false-positives on stack-trace noise.
func classifyChunk(event map[string]any) string {
	text := strings.ToLower(contentText(event["content"]))
	if text == "" {
		return TagActivity
	}

	// Explicit [finding] marker (any case) — highest priority within chunk rules.
	if strings.Contains(text, "[finding]") {
		return TagFinding
	}

	// Multi-word phrases that signal a deliberate finding.
	if strings.Contains(text, "reviewer flagged") ||
		strings.Contains(text, "correction needed") ||
		strings.Contains(text, "failed test") {
		return TagFinding
	}

	// confidence: N% or confidence: N where ANY occurrence has N >= 60.
	// We loop over all matches so "confidence: 40% ... confidence: 80%" correctly
	// returns FINDING (the first match is not necessarily the highest value).
	for _, m := range confidenceRe.FindAllStringSubmatch(text, -1) {
		if n, err := strconv.Atoi(m[1]); err == nil && n >= 60 {
			return TagFinding
		}
	}

	return TagActivity
}
