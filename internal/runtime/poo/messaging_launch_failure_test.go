package poo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	avruntime "github.com/sdougbrown/avenor/internal/runtime"
)

func TestManifestRefusal(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		{"feeds.paths: invalid", true},
		{"cli_registry.paths: invalid", true},
		{"other setting: invalid", false},
	} {
		if got := manifestRefusal(tc.text); got != tc.want {
			t.Errorf("manifestRefusal(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}
}

func TestMessagingLaunchFailures(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	for _, scenario := range []struct{ name, action string }{
		{"exit-before-initialize", "echo 'feeds.paths refused' >&2; exit 1"},
		{"bad-feeds-path", "export POO_FEEDS_PATH=/nonexistent-avenor-test-feeds"},
		{"bad-registry-path", "export POO_CLI_REGISTRY_PATH=/nonexistent-avenor-test-registry"},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit_%v", scenario.name, explicit), func(t *testing.T) {
				opts, dir := fallbackFixture(t, "")
				original := buildPoo(t)
				shim := opts.Launch.Command
				quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
				script := fmt.Sprintf("#!/bin/sh\necho x >> %s\nn=$(wc -l < %s)\nenv > %s\"$n\"\nif [ -n \"$POO_FEEDS_PATH\" ]; then %s; fi\nexec %s \"$@\"\n", quote(filepath.Join(dir, "count")), quote(filepath.Join(dir, "count")), quote(filepath.Join(dir, "env.")), scenario.action, quote(original))
				if err := os.WriteFile(shim, []byte(script), 0700); err != nil {
					t.Fatal(err)
				}
				if explicit {
					on := true
					opts.Messaging = &on
				}
				p := NewWithOptions(opts)
				t.Cleanup(func() { _ = p.Close() })
				session, err := p.Start(context.Background(), avruntime.StartOptions{})
				if explicit {
					if err == nil || !strings.Contains(err.Error(), "v0.2.0") {
						t.Fatalf("explicit error = %v", err)
					}
					if got := fallbackLaunches(t, dir); got != 1 {
						t.Fatalf("launches = %d, want 1", got)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := fallbackLaunches(t, dir); got != 2 {
					t.Fatalf("launches = %d, want 2", got)
				}
				assertFallbackEnv(t, dir, 2, false)
				assertFallbackSessionOff(t, p, session)
			})
		}
	}
}

func TestEarlyManifestFailureReportsOnlyFieldLine(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	for _, tc := range []struct{ name, line, want string }{
		{"field line", "feeds.paths: invalid", "feeds.paths: invalid"},
		{"unicode at limit", "feeds.paths: " + strings.Repeat("x", 386) + "éafter", "feeds.paths: " + strings.Repeat("x", 386) + "é"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, _ := fallbackFixture(t, "")
			on := true
			opts.Messaging = &on
			script := "#!/bin/sh\necho trace-before >&2\necho '" + tc.line + "' >&2\necho trace-after >&2\nexit 1\n"
			if err := os.WriteFile(opts.Launch.Command, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			p := NewWithOptions(opts)
			t.Cleanup(func() { _ = p.Close() })
			_, err := p.Start(context.Background(), avruntime.StartOptions{})
			var unavailable *avruntime.MessagingUnavailableError
			if !errors.As(err, &unavailable) || err.Error() != "poo messaging requires poo v0.2.0 or later: "+tc.want {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestEarlyCrashIsNotMessagingFailure(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			opts, dir := fallbackFixture(t, "")
			if explicit {
				on := true
				opts.Messaging = &on
			}
			script := fmt.Sprintf("#!/bin/sh\necho x >> '%s'\necho 'crashed' >&2\nexit 1\n", filepath.Join(dir, "count"))
			if err := os.WriteFile(opts.Launch.Command, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			p := NewWithOptions(opts)
			t.Cleanup(func() { _ = p.Close() })
			_, err := p.Start(context.Background(), avruntime.StartOptions{})
			var unavailable *avruntime.MessagingUnavailableError
			if err == nil || errors.As(err, &unavailable) {
				t.Fatalf("ordinary launch error wanted, got %v", err)
			}
			if got := fallbackLaunches(t, dir); got != 1 {
				t.Fatalf("launches = %d, want 1", got)
			}
		})
	}
}

func TestMessagingOffDoesNotFallback(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	opts, dir := fallbackFixture(t, "")
	opts.MessagingOn = false
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
	script := fmt.Sprintf("#!/bin/sh\necho x >> %s\nexit 1\n", quote(filepath.Join(dir, "count")))
	if err := os.WriteFile(opts.Launch.Command, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	p := NewWithOptions(opts)
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Start(context.Background(), avruntime.StartOptions{}); err == nil {
		t.Fatal("expected launch failure")
	}
	if got := fallbackLaunches(t, dir); got != 1 {
		t.Fatalf("launches = %d, want 1", got)
	}
}

func TestMessagingLoadManifestFailure(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	for _, variable := range []string{"POO_FEEDS_PATH", "POO_CLI_REGISTRY_PATH"} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit_%v", variable, explicit), func(t *testing.T) {
				opts, dir := fallbackFixture(t, "")
				off := opts
				off.MessagingOn = false
				p := NewWithOptions(off)
				session, err := p.Start(context.Background(), avruntime.StartOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
				quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
				script := fmt.Sprintf("#!/bin/sh\necho x >> %s\nn=$(wc -l < %s)\nenv > %s\"$n\"\nif [ -n \"$POO_FEEDS_PATH\" ]; then export %s=/nonexistent-avenor-load-test; fi\nexec %s \"$@\"\n", quote(filepath.Join(dir, "count")), quote(filepath.Join(dir, "count")), quote(filepath.Join(dir, "env.")), variable, quote(buildPoo(t)))
				if err := os.WriteFile(opts.Launch.Command, []byte(script), 0700); err != nil {
					t.Fatal(err)
				}
				if explicit {
					on := true
					opts.Messaging = &on
				}
				resumer := NewWithOptions(opts)
				t.Cleanup(func() { _ = resumer.Close() })
				resumed, err := resumer.Resume(context.Background(), session.SessionID)
				if explicit {
					if err == nil || !strings.Contains(err.Error(), "v0.2.0") {
						t.Fatalf("error = %v", err)
					}
					if got := fallbackLaunches(t, dir); got != 2 {
						t.Fatalf("launches = %d, want 2", got)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := fallbackLaunches(t, dir); got != 3 {
					t.Fatalf("launches = %d, want 3", got)
				}
				assertFallbackEnv(t, dir, 3, false)
				assertFallbackSessionOff(t, resumer, resumed)
			})
		}
	}
}
