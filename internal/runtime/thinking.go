package runtime

import (
	"fmt"
	"strings"

	"github.com/sdougbrown/avenor/internal/thinkingpolicy"
)

// ValidateThinking accepts the canonical thinking controls and an empty value.
func ValidateThinking(value string) error {
	return thinkingpolicy.ValidateCanonical(value)
}

// ValidateLaunchWrapper rejects wrapper argvs containing empty elements or
// NUL bytes. The wrapper is otherwise not interpreted or validated further:
// whatever argv is supplied is what ends up in front of the backend command.
// Allowlist authorization is not part of this check; it lives with the
// surfaces that own a policy (see the supervisor's MCP launch wrapper policy).
func ValidateLaunchWrapper(wrapper []string) error {
	for _, element := range wrapper {
		if element == "" {
			return fmt.Errorf("launch wrapper elements must be non-empty")
		}
		if strings.ContainsRune(element, '\x00') {
			return fmt.Errorf("launch wrapper elements must not contain NUL")
		}
	}
	return nil
}

// ValidateThinkingForBackend applies the conservative backend policy after
// validating the canonical value. This is the start-session policy; explicit
// resume support is represented separately (see thinkingpolicy.Policies).
func ValidateThinkingForBackend(backend, value string) error {
	if err := ValidateThinking(value); err != nil {
		return err
	}
	if value == "" {
		return nil
	}
	switch thinkingpolicy.Evaluate(backend, value, false) {
	case thinkingpolicy.OK, thinkingpolicy.StartOnly:
		return nil
	case thinkingpolicy.UnsupportedValue:
		return NewUnsupportedThinkingValueError(backend, value)
	default:
		return NewUnsupportedThinkingError(backend)
	}
}

// ValidateThinkingForBackendResume applies the conservative backend policy for
// an explicit resume, distinguishing start-only support from capability gaps.
func ValidateThinkingForBackendResume(backend, value string) error {
	if err := ValidateThinking(value); err != nil {
		return err
	}
	if value == "" {
		return nil
	}
	switch thinkingpolicy.Evaluate(backend, value, true) {
	case thinkingpolicy.OK:
		return nil
	case thinkingpolicy.UnsupportedValue:
		return NewUnsupportedThinkingValueError(backend, value)
	case thinkingpolicy.StartOnly:
		return NewStartOnlyThinkingError(backend, value)
	default:
		return NewUnsupportedThinkingError(backend)
	}
}

// NewUnsupportedThinkingError returns the shared unsupported-parameter error.
func NewUnsupportedThinkingError(backend string) error {
	return fmt.Errorf("backend %q does not support parameter %q", backend, "thinking")
}

// NewUnsupportedThinkingValueError reports a canonical value that the backend
// supports in general but cannot apply natively.
func NewUnsupportedThinkingValueError(backend, value string) error {
	allowed := thinkingpolicy.StartValues(backend)
	return fmt.Errorf("backend %q does not support thinking value %q (allowed: %s)", backend, value, joinThinkingValues(allowed))
}

// NewBackendOmittedThinkingError annotates a thinking rejection when the
// caller supplied no backend, so the runtime default decided. Naming only that
// default's verdict reads as a choice the caller made, so the message names the
// omission.
func NewBackendOmittedThinkingError(backend string, cause error) error {
	return fmt.Errorf("thinking requires an explicit backend: %w (backend was omitted, so it defaulted to %q)", cause, backend)
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
