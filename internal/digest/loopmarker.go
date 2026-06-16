package digest

import (
	"regexp"
	"strings"
)

var loopMarkerRe = regexp.MustCompile(`(?i)^\s*\[loop:\s*(\w+)(?:\s*\|\s*([^\]]*))?\]\s*$`)

// workflowAngleRe matches <|workflow: directive|> or <|workflow: directive | label|>
// on its own line, case-insensitively. Group 1 is the directive word; group 2 is
// the optional label.
var workflowAngleRe = regexp.MustCompile(`(?i)^\s*<\|workflow:\s*(\w+)\s*(?:\|\s*(.*))?\|>\s*$`)

func loopDirectiveSeverity(directive string) int {
	// Keep in sync with phaseconfig.LoopDirectiveSeverity. digest intentionally
	// avoids importing phaseconfig so marker parsing stays dependency-light.
	switch directive {
	case "abort":
		return 3
	case "exit":
		return 2
	case "continue":
		return 1
	default:
		return 0
	}
}

func ExtractLoopMarker(text string) (directive, label string, ok bool) {
	var bestDir string
	var bestLabel string
	bestSev := 0
	inFence := false
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		// Try legacy [loop: ...] first. Both syntaxes are full-line markers, so
		// a matching legacy line cannot also contain a valid angle-token marker.
		m := loopMarkerRe.FindStringSubmatch(line)
		if m != nil {
			dir := strings.ToLower(m[1])
			sev := loopDirectiveSeverity(dir)
			if sev > 0 {
				if sev > bestSev {
					bestDir = dir
					bestLabel = strings.TrimSpace(m[2])
					bestSev = sev
				}
			}
			continue
		}
		// Try canonical angle-token <|workflow: ...|>.
		m = workflowAngleRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		dir := strings.ToLower(m[1])
		sev := loopDirectiveSeverity(dir)
		if sev == 0 {
			continue
		}
		if sev > bestSev {
			bestDir = dir
			bestLabel = strings.TrimSpace(m[2])
			bestSev = sev
		}
	}
	if bestSev == 0 {
		return "", "", false
	}
	return bestDir, bestLabel, true
}
