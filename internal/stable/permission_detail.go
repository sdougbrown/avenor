package stable

import "strings"

// permissionDetailLimit bounds a derived permission summary. The status payload
// is a presentation surface, and a backend tool title can be arbitrarily long.
const permissionDetailLimit = 400

// derivePermissionDetail lifts a flat summary out of a permission request that
// only carries the backend's tool call.
//
// Adapters that pass the backend payload through publish it nested under
// "toolCall" and leave the flat fields empty, so a consumer reading the status
// sees a request with no subject: the orchestrator is asked to allow or deny
// something it cannot name. It also fills "command" when the request nests one,
// which is what the command-path analysis in the event fanout reads.
//
// Requests that already carry a description (the claudecore and pony adapters
// both publish one) are left untouched. Returns true when it changed the map.
func derivePermissionDetail(permission map[string]any) bool {
	if permission == nil {
		return false
	}
	toolCall, _ := permission["toolCall"].(map[string]any)
	rawInput, _ := toolCall["rawInput"].(map[string]any)

	changed := false

	if _, ok := nonEmptyString(permission["command"]); !ok {
		if command, ok := nonEmptyString(rawInput["command"]); ok {
			permission["command"] = command
			changed = true
		}
	}

	if _, ok := nonEmptyString(permission["description"]); ok {
		return changed
	}

	// Prefer what the backend itself called the call, then the command, then the
	// tool-call kind, so a request always carries something to decide on.
	for _, candidate := range []any{toolCall["title"], rawInput["command"], permission["command"], toolCall["kind"]} {
		value, ok := nonEmptyString(candidate)
		if !ok {
			continue
		}
		permission["description"] = truncateRunes(collapsePermissionWhitespace(value), permissionDetailLimit)
		changed = true
		break
	}
	return changed
}

// nonEmptyString reports the trimmed value of a string field and whether it had
// content. Anything that is not a string is absent, never an error.
func nonEmptyString(value any) (string, bool) {
	text, ok := value.(string)
	if !ok {
		return "", false
	}
	text = strings.TrimSpace(text)
	return text, text != ""
}

// collapsePermissionWhitespace keeps a multi-line command readable on one line.
func collapsePermissionWhitespace(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
