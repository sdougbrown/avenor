package poo

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/backendlaunch"
	"github.com/sdougbrown/avenor/internal/rosterconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// stubOnPath writes an executable named poo into a fresh directory and puts
// that directory alone on PATH.
func stubOnPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "poo")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	return path
}

func TestResolveUsesNamedCommandAsGiven(t *testing.T) {
	spec, err := backendlaunch.Resolve(backendID, "reviewer", backendlaunch.Input{
		Command:  "bin/umpire-poo",
		Settings: "/etc/umpire-bot/poo-settings.yaml",
	})
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	want, absErr := filepath.Abs("bin/umpire-poo")
	if absErr != nil {
		t.Fatal(absErr)
	}
	if spec.Command != want {
		t.Fatalf("Command = %q, want absolute %q", spec.Command, want)
	}
	if !slices.Equal(spec.Args, []string{"serve"}) {
		t.Fatalf("Args = %#v, want the serve default", spec.Args)
	}
	if spec.Settings != "/etc/umpire-bot/poo-settings.yaml" {
		t.Fatalf("Settings = %q, want the roster path", spec.Settings)
	}
}

func TestResolveHonorsExplicitArgsAndEmptyArgs(t *testing.T) {
	spec, err := backendlaunch.Resolve(backendID, "reviewer", backendlaunch.Input{
		Command: "/opt/bin/umpire-poo",
		Args:    []string{"serve", "--verbose"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(spec.Args, []string{"serve", "--verbose"}) {
		t.Fatalf("Args = %#v, want the explicit list", spec.Args)
	}

	empty, err := backendlaunch.Resolve(backendID, "reviewer", backendlaunch.Input{
		Command: "/opt/bin/umpire-poo",
		Args:    []string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Args) != 0 {
		t.Fatalf("Args = %#v, want an explicit empty list honored", empty.Args)
	}
}

func TestResolveDefaultsToPooOnPath(t *testing.T) {
	stub := stubOnPath(t)
	spec, err := backendlaunch.Resolve(backendID, "reviewer", backendlaunch.Input{})
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	abs, absErr := filepath.Abs(stub)
	if absErr != nil {
		t.Fatal(absErr)
	}
	if spec.Command != abs {
		t.Fatalf("Command = %q, want the PATH stub at %q", spec.Command, abs)
	}
	if !slices.Equal(spec.Args, []string{"serve"}) {
		t.Fatalf("Args = %#v, want the serve default", spec.Args)
	}
}

func TestResolveMissingBinaryNamesTheRosterEntry(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	_, err := backendlaunch.Resolve(backendID, "umpire-reviewer", backendlaunch.Input{})
	if err == nil {
		t.Fatal("Resolve() with no poo on PATH returned no error")
	}
	if !strings.Contains(err.Error(), `roster entry "umpire-reviewer"`) || !strings.Contains(err.Error(), `"poo" not found on PATH`) {
		t.Fatalf("Resolve() error = %q, want the entry and missing executable named", err)
	}
}

func TestResolveMissingBinaryNamesTheBackendWithoutAnEntry(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	_, err := backendlaunch.Resolve(backendID, "", backendlaunch.Input{})
	if err == nil {
		t.Fatal("Resolve() with no poo on PATH returned no error")
	}
	if !strings.Contains(err.Error(), `backend "poo"`) || !strings.Contains(err.Error(), `"poo" not found on PATH`) {
		t.Fatalf("Resolve() error = %q, want the backend and missing executable named", err)
	}
}

func TestArgvAppendsSettingsOnlyWhenSet(t *testing.T) {
	spec := backendlaunch.Spec{Command: "/opt/bin/umpire-poo", Args: []string{"serve"}}
	if got, want := Argv(spec), []string{"/opt/bin/umpire-poo", "serve"}; !slices.Equal(got, want) {
		t.Fatalf("Argv() = %#v, want %#v", got, want)
	}
	spec.Settings = "/etc/umpire-bot/poo-settings.yaml"
	if got, want := Argv(spec), []string{"/opt/bin/umpire-poo", "serve", "--settings", "/etc/umpire-bot/poo-settings.yaml"}; !slices.Equal(got, want) {
		t.Fatalf("Argv() = %#v, want %#v", got, want)
	}
}

// A poo roster entry may carry the three launch fields only because the poo
// package declared them; roster loading and direct launch resolution must agree
// on the resulting spec.
func TestRosterPooEntryResolvesItsLaunchFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roster.json")
	contents := `{"umpire-reviewer":{
		"backend":"poo",
		"command":"/opt/umpire-bot/bin/umpire-poo",
		"args":["serve"],
		"settings":"/etc/umpire-bot/poo-settings.yaml",
		"agent":"search"
	}}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	roster, err := rosterconfig.Load(path)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	spec, err := roster.ResolveLaunch("umpire-reviewer")
	if err != nil {
		t.Fatalf("ResolveLaunch() = %v", err)
	}
	if spec.Command != "/opt/umpire-bot/bin/umpire-poo" || !slices.Equal(spec.Args, []string{"serve"}) || spec.Settings != "/etc/umpire-bot/poo-settings.yaml" {
		t.Fatalf("ResolveLaunch() = %+v, want the roster entry's launch fields", spec)
	}
}

// A poo roster entry loads, but the shared thinking policy refuses it with the
// same unsupported-parameter error every backend without thinking support gets.
func TestRosterPooEntryWithThinkingIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roster.json")
	if err := os.WriteFile(path, []byte(`{"reviewer":{"backend":"poo","agent":"search","thinking":"high"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	roster, err := rosterconfig.Load(path)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	entry, err := roster.Lookup("reviewer")
	if err != nil {
		t.Fatal(err)
	}
	err = runtime.ValidateThinkingForBackend(entry.Backend, entry.Thinking)
	if err == nil {
		t.Fatal("ValidateThinkingForBackend(poo, high) = nil, want the unsupported-parameter error")
	}
	if err.Error() != runtime.NewUnsupportedThinkingError(entry.Backend).Error() {
		t.Fatalf("ValidateThinkingForBackend() = %q, want the shared unsupported-parameter error", err)
	}
}

func TestProviderStartReportsTheStageOneBoundary(t *testing.T) {
	provider := NewWithOptions(runtime.StartOptions{})
	_, err := provider.Start(context.Background(), runtime.StartOptions{})
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("Start() error = %v, want ErrNotImplemented", err)
	}
}
