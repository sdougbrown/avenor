package runtime

import (
	"strings"
	"testing"
)

func TestValidateThinking(t *testing.T) {
	for _, value := range []string{"", "off", "minimal", "low", "medium", "high", "xhigh", "max"} {
		t.Run(value, func(t *testing.T) {
			if err := ValidateThinking(value); err != nil {
				t.Fatalf("ValidateThinking(%q): %v", value, err)
			}
		})
	}
	for _, value := range []string{"HIGH", "auto", " low"} {
		t.Run("invalid_"+value, func(t *testing.T) {
			err := ValidateThinking(value)
			if err == nil || !strings.Contains(err.Error(), value) || !strings.Contains(err.Error(), "off, minimal, low, medium, high, xhigh, max") {
				t.Fatalf("ValidateThinking(%q) = %v", value, err)
			}
		})
	}
}

func TestThinkingBackendPolicy(t *testing.T) {
	all := []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"}
	policy := map[string]map[string]bool{
		"codex-app-server": {"off": true, "minimal": true, "low": true, "medium": true, "high": true, "xhigh": true, "max": true},
		"pi":               {"off": true, "minimal": true, "low": true, "medium": true, "high": true, "xhigh": true, "max": true},
		"claude":           {"low": true, "medium": true, "high": true, "xhigh": true, "max": true},
		"opencode-acp":     {},
		"opencode-http":    {},
		"gemini-acp":       {},
		"cursor-acp":       {},
		"agy":              {},
		"pony":             {},
	}
	for backend, accepted := range policy {
		if err := ValidateThinkingForBackend(backend, ""); err != nil {
			t.Errorf("%s empty: %v", backend, err)
		}
		for _, value := range all {
			err := ValidateThinkingForBackend(backend, value)
			if accepted[value] && err != nil {
				t.Errorf("%s/%s rejected: %v", backend, value, err)
			}
			if !accepted[value] {
				if err == nil || !strings.Contains(err.Error(), backend) || !strings.Contains(err.Error(), "thinking") {
					t.Errorf("%s/%s error = %v", backend, value, err)
				}
			}
		}
	}
}

func TestThinkingErrorsDistinguishUnsupportedValueAndCapability(t *testing.T) {
	valueErr := ValidateThinkingForBackend("claude", "off")
	if valueErr == nil || !strings.Contains(valueErr.Error(), "thinking value \"off\"") || !strings.Contains(valueErr.Error(), "allowed: low, medium, high, xhigh, max") {
		t.Fatalf("value error = %v", valueErr)
	}

	capabilityErr := ValidateThinkingForBackend("agy", "low")
	if capabilityErr == nil || capabilityErr.Error() != `backend "agy" does not support parameter "thinking"` {
		t.Fatalf("Agy capability error = %v", capabilityErr)
	}
}

func TestValidateThinkingForBackendResume(t *testing.T) {
	// Codex and Pi accept canonical values on explicit resume.
	for _, backend := range []string{"codex-app-server", "pi"} {
		if err := ValidateThinkingForBackendResume(backend, "off"); err != nil {
			t.Errorf("%s resume off: %v", backend, err)
		}
	}
	// Claude is start-only on explicit resume.
	err := ValidateThinkingForBackendResume("claude", "low")
	if err == nil || !strings.Contains(err.Error(), "only when starting a session") {
		t.Errorf("resume low error = %v", err)
	}
	// Backends without support report an unsupported capability.
	err = ValidateThinkingForBackendResume("agy", "low")
	if err == nil || !strings.Contains(err.Error(), `does not support parameter "thinking"`) {
		t.Errorf("agy resume low error = %v", err)
	}
	// Non-canonical value is rejected before policy.
	if err := ValidateThinkingForBackendResume("pi", "HIGH"); err == nil {
		t.Error("pi resume HIGH accepted")
	}
}

func TestJoinThinkingValues(t *testing.T) {
	if got := joinThinkingValues(nil); got != "none" {
		t.Fatalf("empty values = %q", got)
	}
	if got := joinThinkingValues([]string{"low", "high"}); got != "low, high" {
		t.Fatalf("values = %q", got)
	}
}

func TestMergeStartOptionsThinking(t *testing.T) {
	base := StartOptions{Thinking: "low"}
	if got := MergeStartOptions(base, StartOptions{}).Thinking; got != "low" {
		t.Fatalf("inherited thinking = %q", got)
	}
	if got := MergeStartOptions(base, StartOptions{Thinking: "high"}).Thinking; got != "high" {
		t.Fatalf("overridden thinking = %q", got)
	}
	if got := MergeStartOptions(StartOptions{}, StartOptions{}).Thinking; got != "" {
		t.Fatalf("empty thinking = %q", got)
	}
}

// ValidateThinkingForChosenBackend decides what a rejection says. A backend the
// request supplied reports its own verdict; the runtime default names the
// omission; a canonical value error keeps its own message either way.
func TestValidateThinkingForChosenBackend(t *testing.T) {
	supplied := ValidateThinkingForChosenBackend("opencode-acp", "high", true)
	if supplied == nil || supplied.Error() != `backend "opencode-acp" does not support parameter "thinking"` {
		t.Fatalf("supplied backend = %v, want its own verdict", supplied)
	}
	defaulted := ValidateThinkingForChosenBackend("opencode-acp", "high", false)
	if defaulted == nil || !strings.Contains(defaulted.Error(), "requires an explicit backend") || !strings.Contains(defaulted.Error(), "backend was omitted") {
		t.Fatalf("defaulted backend = %v, want the omission note", defaulted)
	}
	value := ValidateThinkingForChosenBackend("opencode-acp", "HIGH", false)
	if value == nil || !strings.Contains(value.Error(), `invalid thinking value "HIGH"`) || strings.Contains(value.Error(), "backend was omitted") {
		t.Fatalf("invalid value = %v, want the canonical error without the omission note", value)
	}
	if err := ValidateThinkingForChosenBackend("pi", "high", false); err != nil {
		t.Fatalf("pi with high = %v, want acceptance", err)
	}
}
