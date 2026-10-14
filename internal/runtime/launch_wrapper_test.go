package runtime

import (
	"slices"
	"testing"
)

func TestValidateLaunchWrapperAcceptsNonEmptyElements(t *testing.T) {
	if err := ValidateLaunchWrapper([]string{"firejail", "--private"}); err != nil {
		t.Fatalf("ValidateLaunchWrapper() = %v, want nil", err)
	}
	if err := ValidateLaunchWrapper(nil); err != nil {
		t.Fatalf("ValidateLaunchWrapper(nil) = %v, want nil", err)
	}
}

func TestValidateLaunchWrapperRejectsEmptyElement(t *testing.T) {
	for _, wrapper := range [][]string{{""}, {"nice", ""}} {
		if err := ValidateLaunchWrapper(wrapper); err == nil {
			t.Fatalf("ValidateLaunchWrapper(%q) = nil, want error", wrapper)
		}
	}
}

func TestMergeStartOptionsOverridesLaunchWrapper(t *testing.T) {
	base := StartOptions{LaunchWrapper: []string{"base"}}
	merged := MergeStartOptions(base, StartOptions{LaunchWrapper: []string{"nice", "-n", "5"}})
	if !slices.Equal(merged.LaunchWrapper, []string{"nice", "-n", "5"}) {
		t.Fatalf("merged wrapper = %#v, want [nice -n 5]", merged.LaunchWrapper)
	}
	// An absent override keeps the base wrapper.
	merged = MergeStartOptions(base, StartOptions{})
	if !slices.Equal(merged.LaunchWrapper, []string{"base"}) {
		t.Fatalf("merged wrapper = %#v, want [base]", merged.LaunchWrapper)
	}
}
