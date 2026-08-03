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

// Policy holds the canonical thinking values a backend supports on a fresh
// start and on an explicit resume.
type Policy struct {
	Start  []string
	Resume []string
}

// Policies is the single source for the static backend thinking policy. Start
// and explicit-resume support are represented separately. It is mirrored by
// the TypeScript packages and exercised by the shared conformance fixture.
var Policies = map[string]Policy{
	"codex-app-server": {Start: CanonicalValues(), Resume: CanonicalValues()},
	"pi":               {Start: CanonicalValues(), Resume: CanonicalValues()},
	"claude":           {Start: []string{"low", "medium", "high", "xhigh", "max"}}, // start-only on resume
	"claude-channel":   {Start: []string{"low", "medium", "high", "xhigh", "max"}}, // start-only on resume
	"opencode-acp":     {},
	"opencode-http":    {},
	"gemini-acp":       {},
	"cursor-acp":       {},
	"agy":              {},
	"pony":             {},
}

// StartValues returns the thinking values supported by backend when starting,
// or nil if the backend is unknown.
func StartValues(backend string) []string {
	p, ok := Policies[backend]
	if !ok {
		return nil
	}
	return p.Start
}

// ResumeValues returns the thinking values supported by backend on an explicit
// resume, or nil if the backend is unknown.
func ResumeValues(backend string) []string {
	p, ok := Policies[backend]
	if !ok {
		return nil
	}
	return p.Resume
}

// Evaluate applies the static policy for a (backend, value, resume) combination.
// An empty value is always accepted. UnsupportedCapability takes precedence
// over StartOnly, and UnsupportedValue over both, matching the existing
// conservative runtime behavior.
func Evaluate(backend, value string, resume bool) Outcome {
	if value == "" {
		return OK
	}
	p, known := Policies[backend]
	if !known {
		return UnsupportedCapability
	}
	set := p.Start
	if resume {
		set = p.Resume
	}
	if len(set) == 0 {
		if resume && len(p.Start) > 0 {
			return StartOnly
		}
		return UnsupportedCapability
	}
	if !contains(set, value) {
		return UnsupportedValue
	}
	return OK
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
