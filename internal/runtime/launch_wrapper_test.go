package runtime

import (
	"slices"
	"testing"

	"github.com/sdougbrown/avenor/internal/backendlaunch"
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
		err := ValidateLaunchWrapper(wrapper)
		if err == nil {
			t.Fatalf("ValidateLaunchWrapper(%q) = nil, want error", wrapper)
		}
		if err.Error() != "launch wrapper elements must be non-empty" {
			t.Fatalf("ValidateLaunchWrapper(%q) error = %q, want %q", wrapper, err, "launch wrapper elements must be non-empty")
		}
	}
}

func TestValidateLaunchWrapperRejectsNULByte(t *testing.T) {
	if err := ValidateLaunchWrapper([]string{"nice", "a\x00b"}); err == nil {
		t.Fatal("ValidateLaunchWrapper() = nil, want error")
	} else if err.Error() != "launch wrapper elements must not contain NUL" {
		t.Fatalf("ValidateLaunchWrapper() = %q, want NUL error", err)
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

func TestMergeStartOptionsOverridesLaunch(t *testing.T) {
	base := StartOptions{Launch: backendlaunch.Spec{Command: "/opt/poo", Args: []string{"serve"}}}
	override := backendlaunch.Spec{Command: "/opt/umpire-poo", Args: []string{"serve"}, Settings: "/etc/poo.yaml"}
	merged := MergeStartOptions(base, StartOptions{Launch: override})
	if !merged.Launch.Equal(override) {
		t.Fatalf("merged launch = %#v, want %#v", merged.Launch, override)
	}
	// An absent override keeps the base launch.
	merged = MergeStartOptions(base, StartOptions{})
	if !merged.Launch.Equal(base.Launch) {
		t.Fatalf("merged launch = %#v, want %#v", merged.Launch, base.Launch)
	}
}
