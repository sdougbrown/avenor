package thinkingpolicy

import "fmt"

// CanonicalValues is the single Go source for the canonical thinking tuple.
// It is verified against the Umpire schema's canonical condition and the
// TypeScript THINKING_LEVELS tuple by introspection tests so the three never
// drift apart.
func CanonicalValues() []string {
	return []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"}
}

// IsCanonical reports whether value is empty or one of the canonical values,
// using the generated Umpire evaluation.
func IsCanonical(value string) bool {
	if value == "" {
		return true
	}
	avail := Check(
		ThinkingPolicyFields{Thinking: &value},
		ThinkingPolicyConditions{},
		ThinkingPolicyFields{},
	)
	return avail.Thinking.Valid != nil && *avail.Thinking.Valid
}

// ValidateCanonical returns a descriptive error for a non-canonical value.
func ValidateCanonical(value string) error {
	if IsCanonical(value) {
		return nil
	}
	return fmt.Errorf("invalid thinking value %q (allowed: %s)", value, joinValues(CanonicalValues()))
}

// Outcome distinguishes the static thinking policy result for a given
// (backend, value, resume) combination so callers can surface a specific error.
type Outcome int

const (
	// OK means the combination is accepted.
	OK Outcome = iota
	// UnsupportedCapability means the backend does not support thinking at all.
	UnsupportedCapability
	// UnsupportedValue means the backend supports thinking but not this value.
	UnsupportedValue
	// StartOnly means the value is only supported when starting a session, not
	// on an explicit resume.
	StartOnly
)

// supportedBackends is the set of backends that have thinking support, derived
// from the Umpire schema's eitherOf branches at init time.
var supportedBackends = func() map[string]bool {
	data, err := readSchemaRules()
	if err != nil {
		// Fallback: derive from the generated branch constants. The schema
		// includes branches for these backends in their condIn expressions.
		return map[string]bool{
			"codex-app-server": true,
			"pi":               true,
			"claude":           true,
			"claude-channel":   true,
		}
	}
	return extractBackendsFromRules(data)
}()

// Evaluate applies the backend policy for a (backend, value, resume) combination
// using the generated Umpire Check function with backend and resume conditions.
// An empty value is always accepted. The outcome classification
// (UnsupportedCapability / UnsupportedValue / StartOnly) is derived from the
// generated Fair flag and the active branch.
func Evaluate(backend, value string, resume bool) Outcome {
	if value == "" {
		return OK
	}
	v := value
	avail := Check(
		ThinkingPolicyFields{Thinking: &v},
		ThinkingPolicyConditions{Backend: backend, Resume: resume},
		ThinkingPolicyFields{},
	)
	if avail.Thinking.Fair {
		return OK
	}
	// Fair is false: classify the failure mode.
	if !supportedBackends[backend] {
		return UnsupportedCapability
	}
	// The backend is known but the value was rejected. Check if it would be
	// accepted on a fresh start (resume=false) to distinguish StartOnly.
	if resume {
		startAvail := Check(
			ThinkingPolicyFields{Thinking: &v},
			ThinkingPolicyConditions{Backend: backend, Resume: false},
			ThinkingPolicyFields{},
		)
		if startAvail.Thinking.Fair {
			return StartOnly
		}
	}
	return UnsupportedValue
}

// StartValues returns the thinking values supported by backend when starting,
// or nil if the backend is unknown or has no thinking support.
func StartValues(backend string) []string {
	if !supportedBackends[backend] {
		return nil
	}
	var result []string
	for _, v := range CanonicalValues() {
		avail := Check(
			ThinkingPolicyFields{Thinking: &v},
			ThinkingPolicyConditions{Backend: backend, Resume: false},
			ThinkingPolicyFields{},
		)
		if avail.Thinking.Fair {
			result = append(result, v)
		}
	}
	return result
}

// ResumeValues returns the thinking values supported by backend on an explicit
// resume, or nil if the backend is unknown or has no thinking support.
func ResumeValues(backend string) []string {
	if !supportedBackends[backend] {
		return nil
	}
	var result []string
	for _, v := range CanonicalValues() {
		avail := Check(
			ThinkingPolicyFields{Thinking: &v},
			ThinkingPolicyConditions{Backend: backend, Resume: true},
			ThinkingPolicyFields{},
		)
		if avail.Thinking.Fair {
			result = append(result, v)
		}
	}
	return result
}

func joinValues(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	result := values[0]
	for _, value := range values[1:] {
		result += ", " + value
	}
	return result
}