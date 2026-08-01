package runtime

import "fmt"

const thinkingValues = "off, minimal, low, medium, high, xhigh, max"

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
		return fmt.Errorf("invalid thinking value %q (allowed: %s)", value, thinkingValues)
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
		return NewUnsupportedThinkingError(backend)
	}
	return nil
}

// NewUnsupportedThinkingError returns the shared unsupported-parameter error.
func NewUnsupportedThinkingError(backend string) error {
	return fmt.Errorf("backend %q does not support parameter %q", backend, "thinking")
}
