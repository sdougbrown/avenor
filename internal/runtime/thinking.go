package runtime

import "fmt"

var thinkingValueList = []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"}

var validThinkingValues = map[string]struct{}{
	"off":     {},
	"minimal": {},
	"low":     {},
	"medium":  {},
	"high":    {},
	"xhigh":   {},
	"max":     {},
}

var backendThinkingValues = map[string]map[string]struct{}{
	"codex-app-server": validThinkingValues,
	"pi":               validThinkingValues,
	"claude": {
		"low": {}, "medium": {}, "high": {}, "xhigh": {}, "max": {},
	},
	"claude-channel": {
		"low": {}, "medium": {}, "high": {}, "xhigh": {}, "max": {},
	},
	"opencode-acp":  {},
	"opencode-http": {},
	"gemini-acp":    {},
	"cursor-acp":    {},
	"agy":           {},
	"pony":          {},
}

// ValidateThinking accepts the canonical thinking controls and an empty value.
func ValidateThinking(value string) error {
	if value == "" {
		return nil
	}
	if _, ok := validThinkingValues[value]; !ok {
		return fmt.Errorf("invalid thinking value %q (allowed: %s)", value, joinThinkingValues(thinkingValueList))
	}
	return nil
}

// ValidateThinkingForBackend applies the conservative backend policy after
// validating the canonical value.
func ValidateThinkingForBackend(backend, value string) error {
	if err := ValidateThinking(value); err != nil {
		return err
	}
	if value == "" {
		return nil
	}
	values, ok := backendThinkingValues[backend]
	if !ok {
		return NewUnsupportedThinkingError(backend)
	}
	if _, ok := values[value]; !ok {
		if len(values) == 0 {
			return NewUnsupportedThinkingError(backend)
		}
		return NewUnsupportedThinkingValueError(backend, value)
	}
	return nil
}

// NewUnsupportedThinkingError returns the shared unsupported-parameter error.
func NewUnsupportedThinkingError(backend string) error {
	return fmt.Errorf("backend %q does not support parameter %q", backend, "thinking")
}

// NewUnsupportedThinkingValueError reports a canonical value that the backend
// supports in general but cannot apply natively.
func NewUnsupportedThinkingValueError(backend, value string) error {
	values := backendThinkingValues[backend]
	allowed := make([]string, 0, len(values))
	for _, candidate := range thinkingValueList {
		if _, ok := values[candidate]; ok {
			allowed = append(allowed, candidate)
		}
	}
	return fmt.Errorf("backend %q does not support thinking value %q (allowed: %s)", backend, value, joinThinkingValues(allowed))
}

// NewStartOnlyThinkingError reports that a backend supports the value only on
// a new session, not an explicit resume.
func NewStartOnlyThinkingError(backend, value string) error {
	return fmt.Errorf("backend %q supports parameter %q only when starting a session; cannot apply value %q on an explicit resume", backend, "thinking", value)
}

func joinThinkingValues(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	result := values[0]
	for _, value := range values[1:] {
		result += ", " + value
	}
	return result
}
