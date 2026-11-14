package claudecore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// trustConfigFixture is a config file in its own temp directory, with a home
// directory and run directory that are not the config.
type trustConfigFixture struct {
	configPath string
	home       string
	work       string
}

func newTrustConfigFixture(t *testing.T, body string) trustConfigFixture {
	t.Helper()
	dir := t.TempDir()
	f := trustConfigFixture{
		configPath: filepath.Join(dir, "config", ".claude.json"),
		home:       filepath.Join(dir, "home"),
		work:       filepath.Join(dir, "work"),
	}
	for _, d := range []string{filepath.Dir(f.configPath), f.home, f.work} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	if body != "" {
		if err := os.WriteFile(f.configPath, []byte(body), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
	}
	return f
}

func projectKey(t *testing.T, dir string) string {
	t.Helper()
	key, err := trustKey(dir)
	if err != nil {
		t.Fatalf("trustKey(%s): %v", dir, err)
	}
	return key
}

func readConfig(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	return raw
}

func configLockPath(configPath string) string { return configPath + ".lock" }

// fakeTrustClock makes trustClock report base+offset, so a test can put a lock
// past its rename window without sleeping.
func fakeTrustClock(t *testing.T, offset *time.Duration) {
	t.Helper()
	base := time.Now()
	saved := trustClock
	t.Cleanup(func() { trustClock = saved })
	trustClock = func() time.Time { return base.Add(*offset) }
}

func requireNoConfigLock(t *testing.T, configPath string) {
	t.Helper()
	if _, err := os.Stat(configLockPath(configPath)); !os.IsNotExist(err) {
		t.Fatalf("config lock %s is still there: %v", configLockPath(configPath), err)
	}
}

func requireNoTempLeftovers(t *testing.T, configPath string) {
	t.Helper()
	pattern := filepath.Join(filepath.Dir(configPath), "."+filepath.Base(configPath)+".tmp-*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	if len(matches) != 0 {
		t.Fatalf("temp files left behind: %v", matches)
	}
}

func decodeConfig(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("config is not a JSON object: %v", err)
	}
	return doc
}

func decodeProjects(t *testing.T, doc map[string]json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var projects map[string]json.RawMessage
	if err := json.Unmarshal(doc["projects"], &projects); err != nil {
		t.Fatalf("projects: %v", err)
	}
	return projects
}

func decodeEntry(t *testing.T, projects map[string]json.RawMessage, key string) map[string]json.RawMessage {
	t.Helper()
	encoded, ok := projects[key]
	if !ok {
		t.Fatalf("no projects entry for %q (keys: %v)", key, projectKeys(projects))
	}
	var entry map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &entry); err != nil {
		t.Fatalf("projects[%q]: %v", key, err)
	}
	return entry
}

func projectKeys(projects map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(projects))
	for key := range projects {
		keys = append(keys, key)
	}
	return keys
}

func entryTrusted(t *testing.T, entry map[string]json.RawMessage) bool {
	t.Helper()
	var trusted bool
	if err := json.Unmarshal(entry[trustedProjectsField], &trusted); err != nil {
		t.Fatalf("%s = %s, not a bool", trustedProjectsField, entry[trustedProjectsField])
	}
	return trusted
}

// compactJSON removes formatting so a value can be compared with its original
// spelling.
func compactJSON(t *testing.T, raw []byte) string {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		t.Fatalf("compact %s: %v", raw, err)
	}
	return buf.String()
}

func TestPreAcceptTrustAddsTheProjectEntry(t *testing.T) {
	f := newTrustConfigFixture(t, `{"numStartups": 3, "projects": {}}`)
	key := projectKey(t, f.work)

	if err := preAcceptTrust(context.Background(), f.configPath, f.home, f.work); err != nil {
		t.Fatalf("preAcceptTrust: %v", err)
	}
	doc := decodeConfig(t, readConfig(t, f.configPath))
	if got := string(doc["numStartups"]); got != "3" {
		t.Errorf("numStartups = %s, want 3", got)
	}
	if entry := decodeEntry(t, decodeProjects(t, doc), key); !entryTrusted(t, entry) {
		t.Errorf("projects[%q] was not marked trusted: %s", key, entry[trustedProjectsField])
	}
}

func TestPreAcceptTrustCreatesTheProjectsObject(t *testing.T) {
	f := newTrustConfigFixture(t, `{"numStartups": 3}`)
	key := projectKey(t, f.work)

	if err := preAcceptTrust(context.Background(), f.configPath, f.home, f.work); err != nil {
		t.Fatalf("preAcceptTrust: %v", err)
	}
	entry := decodeEntry(t, decodeProjects(t, decodeConfig(t, readConfig(t, f.configPath))), key)
	if !entryTrusted(t, entry) {
		t.Errorf("projects[%q] was not marked trusted", key)
	}
}

func TestPreAcceptTrustFlipsAnExistingFalseFlag(t *testing.T) {
	f := newTrustConfigFixture(t, ``)
	key := projectKey(t, f.work)
	body := `{"projects": {` + mustQuote(t, key) + `: {"hasTrustDialogAccepted": false, "allowedTools": ["Read"]}}}`
	if err := os.WriteFile(f.configPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := preAcceptTrust(context.Background(), f.configPath, f.home, f.work); err != nil {
		t.Fatalf("preAcceptTrust: %v", err)
	}
	entry := decodeEntry(t, decodeProjects(t, decodeConfig(t, readConfig(t, f.configPath))), key)
	if !entryTrusted(t, entry) {
		t.Error("hasTrustDialogAccepted was not flipped to true")
	}
	if got := compactJSON(t, entry["allowedTools"]); got != `["Read"]` {
		t.Errorf("allowedTools = %s, want [\"Read\"]", got)
	}
}

func TestPreAcceptTrustLeavesATrustedEntryUntouched(t *testing.T) {
	f := newTrustConfigFixture(t, ``)
	key := projectKey(t, f.work)
	body := `{"projects": {` + mustQuote(t, key) + `: {"hasTrustDialogAccepted": true}}}`
	if err := os.WriteFile(f.configPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	before, err := os.Stat(f.configPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	beforeBytes := readConfig(t, f.configPath)

	if err := preAcceptTrust(context.Background(), f.configPath, f.home, f.work); err != nil {
		t.Fatalf("preAcceptTrust: %v", err)
	}
	after, err := os.Stat(f.configPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Error("already trusted config was replaced")
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Errorf("mtime changed from %v to %v", before.ModTime(), after.ModTime())
	}
	if got := readConfig(t, f.configPath); string(got) != string(beforeBytes) {
		t.Error("already trusted config bytes changed")
	}
}

func TestPreAcceptTrustPreservesUnknownFieldsAndNumbers(t *testing.T) {
	f := newTrustConfigFixture(t, ``)
	key := projectKey(t, f.work)
	body := `{
  "big": 12345678901234567890,
  "nested": {"value": 1.234567890123456789},
  "projects": {
    ` + mustQuote(t, key) + `: {
      "lastCost": 0.12345678901234567,
      "mcpServers": {"server": {"count": 98765432109876543210}},
      "hasTrustDialogAccepted": false
    },
    "/other/project": {"hasTrustDialogAccepted": true}
  }
}`
	if err := os.WriteFile(f.configPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := preAcceptTrust(context.Background(), f.configPath, f.home, f.work); err != nil {
		t.Fatalf("preAcceptTrust: %v", err)
	}
	doc := decodeConfig(t, readConfig(t, f.configPath))
	for field, want := range map[string]string{
		"big":    "12345678901234567890",
		"nested": `{"value":1.234567890123456789}`,
	} {
		if got := compactJSON(t, doc[field]); got != want {
			t.Errorf("%s = %s, want %s", field, got, want)
		}
	}
	projects := decodeProjects(t, doc)
	entry := decodeEntry(t, projects, key)
	for field, want := range map[string]string{
		"lastCost":   "0.12345678901234567",
		"mcpServers": `{"server":{"count":98765432109876543210}}`,
	} {
		if got := compactJSON(t, entry[field]); got != want {
			t.Errorf("projects[%q].%s = %s, want %s", key, field, got, want)
		}
	}
	if other := decodeEntry(t, projects, "/other/project"); !entryTrusted(t, other) {
		t.Error("an unrelated project entry was changed")
	}
}

func TestPreAcceptTrustDoesNotEscapeUntouchedStrings(t *testing.T) {
	f := newTrustConfigFixture(t, ``)
	body := `{"mcpServers":{"s":{"url":"http://host/p?a=1&b=<2>"}}}`
	if err := os.WriteFile(f.configPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := preAcceptTrust(context.Background(), f.configPath, f.home, f.work); err != nil {
		t.Fatalf("preAcceptTrust: %v", err)
	}
	got := string(readConfig(t, f.configPath))
	if !strings.Contains(got, `"url": "http://host/p?a=1&b=<2>"`) {
		t.Errorf("untouched string was rewritten:\n%s", got)
	}
}

func TestPreAcceptTrustPreservesFileMode(t *testing.T) {
	for _, mode := range []os.FileMode{0o600, 0o640, 0o644} {
		t.Run(mode.String(), func(t *testing.T) {
			f := newTrustConfigFixture(t, `{"projects": {}}`)
			if err := os.Chmod(f.configPath, mode); err != nil {
				t.Fatalf("chmod: %v", err)
			}
			if err := preAcceptTrust(context.Background(), f.configPath, f.home, f.work); err != nil {
				t.Fatalf("preAcceptTrust: %v", err)
			}
			info, err := os.Stat(f.configPath)
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			if got := info.Mode().Perm(); got != mode {
				t.Errorf("mode = %v, want %v", got, mode)
			}
		})
	}
}

func TestPreAcceptTrustWritesThroughASymlinkedConfig(t *testing.T) {
	f := newTrustConfigFixture(t, ``)
	realPath := filepath.Join(t.TempDir(), "real.json")
	if err := os.WriteFile(realPath, []byte(`{"projects": {}}`), 0o600); err != nil {
		t.Fatalf("write real config: %v", err)
	}
	linkPath := f.configPath
	if err := os.Symlink(realPath, linkPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	key := projectKey(t, f.work)

	if err := preAcceptTrust(context.Background(), linkPath, f.home, f.work); err != nil {
		t.Fatalf("preAcceptTrust: %v", err)
	}
	link, err := os.Lstat(linkPath)
	if err != nil {
		t.Fatalf("lstat link: %v", err)
	}
	if link.Mode()&os.ModeSymlink == 0 {
		t.Error("the symlink was replaced by a regular file")
	}
	if target, err := os.Readlink(linkPath); err != nil || target != realPath {
		t.Errorf("link target = %q, %v; want %q", target, err, realPath)
	}
	entry := decodeEntry(t, decodeProjects(t, decodeConfig(t, readConfig(t, realPath))), key)
	if !entryTrusted(t, entry) {
		t.Error("the symlink target was not updated")
	}
}

func TestPreAcceptTrustIgnoresAMissingConfig(t *testing.T) {
	f := newTrustConfigFixture(t, ``)
	if err := preAcceptTrust(context.Background(), f.configPath, f.home, f.work); err != nil {
		t.Fatalf("preAcceptTrust: %v", err)
	}
	if _, err := os.Stat(f.configPath); !os.IsNotExist(err) {
		t.Errorf("config was created: %v", err)
	}
}

func TestPreAcceptTrustRejectsMalformedConfig(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": `)
	before := readConfig(t, f.configPath)

	if err := preAcceptTrust(context.Background(), f.configPath, f.home, f.work); err == nil {
		t.Fatal("preAcceptTrust accepted malformed JSON")
	}
	if got := readConfig(t, f.configPath); string(got) != string(before) {
		t.Error("malformed config bytes changed")
	}
}

func TestPreAcceptTrustRejectsUnexpectedShapes(t *testing.T) {
	for name, body := range map[string]func(key string) string{
		"top level array": func(string) string { return `[]` },
		"top level null":  func(string) string { return `null` },
		"projects array": func(string) string {
			return `{"projects": []}`
		},
		"projects null": func(string) string {
			return `{"projects": null}`
		},
		"project entry string": func(key string) string {
			return `{"projects": {` + mustQuote(t, key) + `: "trusted"}}`
		},
		"project entry null": func(key string) string {
			return `{"projects": {` + mustQuote(t, key) + `: null}}`
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newTrustConfigFixture(t, ``)
			key := projectKey(t, f.work)
			if err := os.WriteFile(f.configPath, []byte(body(key)), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			before := readConfig(t, f.configPath)

			if err := preAcceptTrust(context.Background(), f.configPath, f.home, f.work); err == nil {
				t.Fatal("preAcceptTrust accepted an unexpected shape")
			}
			if got := readConfig(t, f.configPath); string(got) != string(before) {
				t.Error("config bytes changed on a rejected shape")
			}
		})
	}
}

func TestPreAcceptTrustRefusesRootAndHome(t *testing.T) {
	for name, dir := range map[string]func(f trustConfigFixture) string{
		"filesystem root": func(trustConfigFixture) string { return string(filepath.Separator) },
		"home directory":  func(f trustConfigFixture) string { return f.home },
		"home's parent":   func(f trustConfigFixture) string { return filepath.Dir(f.home) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newTrustConfigFixture(t, `{"projects": {}}`)
			before := readConfig(t, f.configPath)

			if err := preAcceptTrust(context.Background(), f.configPath, f.home, dir(f)); err == nil {
				t.Fatal("preAcceptTrust accepted a refused directory")
			}
			if got := readConfig(t, f.configPath); string(got) != string(before) {
				t.Error("config bytes changed for a refused directory")
			}
		})
	}
}

func TestPreAcceptTrustRefusesSymlinkedHomeOrRunDir(t *testing.T) {
	for name, tc := range map[string]struct {
		home func(t *testing.T, f trustConfigFixture) string
		dir  func(t *testing.T, f trustConfigFixture) string
	}{
		"home given through a symlink": {
			home: func(t *testing.T, f trustConfigFixture) string {
				link := filepath.Join(filepath.Dir(f.home), "home-link")
				if err := os.Symlink(f.home, link); err != nil {
					t.Fatalf("symlink home: %v", err)
				}
				return link
			},
			dir: func(t *testing.T, f trustConfigFixture) string { return f.home },
		},
		"home resolved, run directory through a symlink": {
			home: func(t *testing.T, f trustConfigFixture) string { return f.home },
			dir: func(t *testing.T, f trustConfigFixture) string {
				link := filepath.Join(filepath.Dir(f.home), "work-link")
				if err := os.Symlink(f.home, link); err != nil {
					t.Fatalf("symlink home: %v", err)
				}
				return link
			},
		},
		"run directory is a symlink to home": {
			home: func(t *testing.T, f trustConfigFixture) string { return f.home },
			dir: func(t *testing.T, f trustConfigFixture) string {
				link := filepath.Join(filepath.Dir(f.home), "to-home")
				if err := os.Symlink(f.home, link); err != nil {
					t.Fatalf("symlink home: %v", err)
				}
				return link
			},
		},
		"run directory is a symlink above home": {
			home: func(t *testing.T, f trustConfigFixture) string { return f.home },
			dir: func(t *testing.T, f trustConfigFixture) string {
				above := filepath.Dir(f.home)
				link := filepath.Join(t.TempDir(), "above-home")
				if err := os.Symlink(above, link); err != nil {
					t.Fatalf("symlink above home: %v", err)
				}
				return link
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newTrustConfigFixture(t, `{"projects": {}}`)
			before := readConfig(t, f.configPath)

			if err := preAcceptTrust(context.Background(), f.configPath, tc.home(t, f), tc.dir(t, f)); err == nil {
				t.Fatal("preAcceptTrust accepted a directory that resolves to or above home")
			}
			if got := readConfig(t, f.configPath); string(got) != string(before) {
				t.Error("config bytes changed for a refused directory")
			}
		})
	}
}

func TestPreAcceptTrustRefusesAWorldWritableStickyDirectory(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	shared := filepath.Join(t.TempDir(), "shared")
	if err := os.MkdirAll(shared, 0o700); err != nil {
		t.Fatalf("mkdir shared: %v", err)
	}
	if err := os.Chmod(shared, 0o777|os.ModeSticky); err != nil {
		t.Fatalf("chmod shared: %v", err)
	}

	err := preAcceptTrust(context.Background(), f.configPath, f.home, shared)
	if !errors.Is(err, errRefuseWorldWritableSticky) {
		t.Fatalf("preAcceptTrust error = %v, want the world-writable sticky refusal", err)
	}
	if got := string(readConfig(t, f.configPath)); got != `{"projects": {}}` {
		t.Errorf("config changed for a shared directory: %s", got)
	}
}

// TestPreAcceptTrustTreatsRefusalsAsSuccess pins the quiet refusal: PreAcceptTrust
// reports nil for the directories it will not trust, so the provider does not log
// a failure that is the intended outcome.
func TestPreAcceptTrustTreatsRefusalsAsSuccess(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	shared := filepath.Join(t.TempDir(), "shared")
	if err := os.MkdirAll(shared, 0o700); err != nil {
		t.Fatalf("mkdir shared: %v", err)
	}
	if err := os.Chmod(shared, 0o777|os.ModeSticky); err != nil {
		t.Fatalf("chmod shared: %v", err)
	}
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Dir(f.configPath))
	t.Setenv("HOME", f.home)

	for name, dir := range map[string]string{
		"filesystem root":       string(filepath.Separator),
		"home directory":        f.home,
		"home's parent":         filepath.Dir(f.home),
		"world-writable sticky": shared,
	} {
		t.Run(name, func(t *testing.T) {
			if err := PreAcceptTrust(context.Background(), dir); err != nil {
				t.Errorf("PreAcceptTrust(%s) = %v, want nil for an expected refusal", dir, err)
			}
			if got := string(readConfig(t, f.configPath)); got != `{"projects": {}}` {
				t.Errorf("config changed for a refused directory: %s", got)
			}
		})
	}
}

func TestPreAcceptTrustDefaultsToHomeConfig(t *testing.T) {
	for _, tc := range []struct {
		name  string
		unset bool
	}{
		{"unset", true},
		{"empty", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			work := t.TempDir()
			configPath := filepath.Join(home, ".claude.json")
			if err := os.WriteFile(configPath, []byte(`{"projects": {}}`), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			t.Setenv("HOME", home)
			t.Setenv("CLAUDE_CONFIG_DIR", "")
			t.Setenv("CLAUDE_CODE_CUSTOM_OAUTH_URL", "")
			if tc.unset {
				if err := os.Unsetenv("CLAUDE_CONFIG_DIR"); err != nil {
					t.Fatalf("unset CLAUDE_CONFIG_DIR: %v", err)
				}
			}
			key := projectKey(t, work)

			if err := PreAcceptTrust(context.Background(), work); err != nil {
				t.Fatalf("PreAcceptTrust: %v", err)
			}
			entry := decodeEntry(t, decodeProjects(t, decodeConfig(t, readConfig(t, configPath))), key)
			if !entryTrusted(t, entry) {
				t.Errorf("$HOME/.claude.json was not updated (keys: %v)", projectKeys(decodeProjects(t, decodeConfig(t, readConfig(t, configPath)))))
			}
		})
	}
}

func TestPreAcceptTrustRefusesARelativeClaudeConfigDir(t *testing.T) {
	home := t.TempDir()
	work := t.TempDir()
	configPath := filepath.Join(home, ".claude.json")
	if err := os.WriteFile(configPath, []byte(`{"projects": {}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join("relative", "config"))

	err := PreAcceptTrust(context.Background(), work)
	if err == nil || !strings.Contains(err.Error(), "CLAUDE_CONFIG_DIR") {
		t.Fatalf("PreAcceptTrust error = %v, want a relative CLAUDE_CONFIG_DIR refusal", err)
	}
	if got := string(readConfig(t, configPath)); got != `{"projects": {}}` {
		t.Errorf("config changed with a relative CLAUDE_CONFIG_DIR: %s", got)
	}
}

// TestPreAcceptTrustFallsBackToThePasswdHome covers a run started with HOME
// unset, as a service environment may be: the home refusals still apply,
// through the passwd entry rather than by skipping the checks.
func TestPreAcceptTrustFallsBackToThePasswdHome(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	t.Setenv("HOME", "")
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Dir(f.configPath))
	t.Setenv("CLAUDE_CODE_CUSTOM_OAUTH_URL", "")
	saved := passwdHome
	t.Cleanup(func() { passwdHome = saved })
	passwdHome = func() (string, error) { return f.home, nil }
	before := readConfig(t, f.configPath)

	if err := PreAcceptTrust(context.Background(), f.home); err != nil {
		t.Fatalf("PreAcceptTrust(home) = %v, want nil for a refusal", err)
	}
	if got := string(readConfig(t, f.configPath)); got != string(before) {
		t.Errorf("config changed for the passwd home: %s", got)
	}
}

// TestPreAcceptTrustNeedsAHomeDirectory checks that no home directory at all
// is a failure, not a skip: without it the home refusals cannot be applied.
func TestPreAcceptTrustNeedsAHomeDirectory(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	t.Setenv("HOME", "")
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Dir(f.configPath))
	saved := passwdHome
	t.Cleanup(func() { passwdHome = saved })
	passwdHome = func() (string, error) { return "", errors.New("no passwd entry") }
	before := readConfig(t, f.configPath)

	err := PreAcceptTrust(context.Background(), f.work)
	if err == nil {
		t.Fatal("PreAcceptTrust edited the config with no home directory to check")
	}
	if isTrustRefusal(err) {
		t.Errorf("a missing home directory read as a refusal: %v", err)
	}
	if got := string(readConfig(t, f.configPath)); got != string(before) {
		t.Errorf("config changed with no home directory: %s", got)
	}
}

// TestPreAcceptTrustSkipsACustomOAuthConfig checks the run's Claude Code would
// read .claude-custom-oauth.json, so avenor writes nothing rather than trust
// in a file that run never reads.
func TestPreAcceptTrustSkipsACustomOAuthConfig(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	t.Setenv("HOME", f.home)
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Dir(f.configPath))
	t.Setenv("CLAUDE_CODE_CUSTOM_OAUTH_URL", "https://claude.example.invalid")
	before := readConfig(t, f.configPath)

	if err := PreAcceptTrust(context.Background(), f.work); err != nil {
		t.Fatalf("PreAcceptTrust: %v", err)
	}
	if got := string(readConfig(t, f.configPath)); got != string(before) {
		t.Errorf("config changed with CLAUDE_CODE_CUSTOM_OAUTH_URL set: %s", got)
	}
}

// TestPreAcceptTrustSkipsALegacyConfigFile checks a non-empty legacy
// .config.json is left alone: Claude Code reads that file in preference to
// .claude.json, so trust written to .claude.json would never match.
func TestPreAcceptTrustSkipsALegacyConfigFile(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	legacyPath := filepath.Join(filepath.Dir(f.configPath), legacyConfigName)
	if err := os.WriteFile(legacyPath, []byte(`{"projects": {}}`), 0o600); err != nil {
		t.Fatalf("write legacy config: %v", err)
	}
	t.Setenv("HOME", f.home)
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Dir(f.configPath))
	t.Setenv("CLAUDE_CODE_CUSTOM_OAUTH_URL", "")
	before := readConfig(t, f.configPath)

	if err := PreAcceptTrust(context.Background(), f.work); err != nil {
		t.Fatalf("PreAcceptTrust: %v", err)
	}
	for name, path := range map[string]string{"current": f.configPath, "legacy": legacyPath} {
		if got := string(readConfig(t, path)); got != string(before) {
			t.Errorf("the %s config changed: %s", name, got)
		}
	}
}

// TestPreAcceptTrustEditsPastAnEmptyLegacyConfig pins the size test on the
// skip: Claude Code ignores an empty legacy .config.json and reads
// .claude.json, so avenor must edit it.
func TestPreAcceptTrustEditsPastAnEmptyLegacyConfig(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	legacyPath := filepath.Join(filepath.Dir(f.configPath), legacyConfigName)
	if err := os.WriteFile(legacyPath, nil, 0o600); err != nil {
		t.Fatalf("write empty legacy config: %v", err)
	}
	t.Setenv("HOME", f.home)
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Dir(f.configPath))
	t.Setenv("CLAUDE_CODE_CUSTOM_OAUTH_URL", "")
	key := projectKey(t, f.work)

	if err := PreAcceptTrust(context.Background(), f.work); err != nil {
		t.Fatalf("PreAcceptTrust: %v", err)
	}
	entry := decodeEntry(t, decodeProjects(t, decodeConfig(t, readConfig(t, f.configPath))), key)
	if !entryTrusted(t, entry) {
		t.Error("an empty legacy .config.json suppressed the edit")
	}
}

// TestPreAcceptTrustDetectsAReplacedFileWithTheSameSizeAndMtime pins file
// identity in change detection: Claude Code replaces its config by temp file
// and rename, so a same-size rewrite inside one mtime tick differs only by
// inode. Without the identity check the edit would rename its stale copy over
// the newer write.
func TestPreAcceptTrustDetectsAReplacedFileWithTheSameSizeAndMtime(t *testing.T) {
	f := newTrustConfigFixture(t, ``)
	key := projectKey(t, f.work)
	original := `{"marker":"aaaa","projects": {` + mustQuote(t, key) + `: {"hasTrustDialogAccepted": false}}}`
	if err := os.WriteFile(f.configPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	info, err := os.Stat(f.configPath)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	replacement := `{"marker":"bbbb","projects": {` + mustQuote(t, key) + `: {"hasTrustDialogAccepted": false}}}`
	if len(replacement) != len(original) {
		t.Fatalf("fixture rewrite is %d bytes, want %d so only the inode differs", len(replacement), len(original))
	}

	calls := 0
	afterRead := func() {
		calls++
		if calls != 1 {
			return
		}
		temp, err := os.CreateTemp(filepath.Dir(f.configPath), ".concurrent-*")
		if err != nil {
			t.Fatalf("create concurrent temp: %v", err)
		}
		tempName := temp.Name()
		if _, err := temp.WriteString(replacement); err != nil {
			t.Fatalf("write concurrent config: %v", err)
		}
		if err := temp.Close(); err != nil {
			t.Fatalf("close concurrent config: %v", err)
		}
		if err := os.Rename(tempName, f.configPath); err != nil {
			t.Fatalf("rename concurrent config: %v", err)
		}
		// Restore the original mtime so only the inode gives the write away.
		if err := os.Chtimes(f.configPath, info.ModTime(), info.ModTime()); err != nil {
			t.Fatalf("restore mtime: %v", err)
		}
	}

	if err := editTrustConfig(context.Background(), f.configPath, key, afterRead); err != nil {
		t.Fatalf("editTrustConfig: %v", err)
	}
	if calls < 2 {
		t.Fatalf("the config was read %d times, want a re-read", calls)
	}
	doc := decodeConfig(t, readConfig(t, f.configPath))
	if got := compactJSON(t, doc["marker"]); got != `"bbbb"` {
		t.Errorf("marker = %s, want bbbb (the same-size rewrite was clobbered)", got)
	}
	if !entryTrusted(t, decodeEntry(t, decodeProjects(t, doc), key)) {
		t.Error("the retried edit did not mark the project trusted")
	}
}

func TestPreAcceptTrustRefusesToEditUnderAHeldLock(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	key := projectKey(t, f.work)
	before := readConfig(t, f.configPath)
	lockPath := configLockPath(f.configPath)
	if err := os.Mkdir(lockPath, 0o755); err != nil {
		t.Fatalf("hold lock: %v", err)
	}

	if err := editTrustConfig(context.Background(), f.configPath, key, nil); err == nil {
		t.Fatal("editTrustConfig edited the config while another holder had the lock")
	}
	if got := readConfig(t, f.configPath); string(got) != string(before) {
		t.Error("config bytes changed while the lock was held")
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("the lock avenor did not create was removed: %v", err)
	}

	if err := os.Remove(lockPath); err != nil {
		t.Fatalf("release lock: %v", err)
	}
	if err := editTrustConfig(context.Background(), f.configPath, key, nil); err != nil {
		t.Fatalf("editTrustConfig after the lock was released: %v", err)
	}
	if !entryTrusted(t, decodeEntry(t, decodeProjects(t, decodeConfig(t, readConfig(t, f.configPath))), key)) {
		t.Error("the project was not marked trusted after the lock was released")
	}
	requireNoConfigLock(t, f.configPath)
}

// TestPreAcceptTrustEndsTheLockWaitOnCancel checks the wait for a held lock
// answers the caller's ctx: a cancelled Start must not sit out the whole lock
// timeout before launching.
func TestPreAcceptTrustEndsTheLockWaitOnCancel(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	key := projectKey(t, f.work)
	before := readConfig(t, f.configPath)
	lockPath := configLockPath(f.configPath)
	if err := os.Mkdir(lockPath, 0o755); err != nil {
		t.Fatalf("hold lock: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timer := time.AfterFunc(50*time.Millisecond, cancel)
	defer timer.Stop()

	start := time.Now()
	err := editTrustConfig(ctx, f.configPath, key, nil)
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("editTrustConfig = %v, want the cancellation", err)
	}
	if elapsed > trustLockTotalWait/2 {
		t.Errorf("the wait ran %s after cancellation, want it to end well before the %s timeout", elapsed, trustLockTotalWait)
	}
	if got := readConfig(t, f.configPath); string(got) != string(before) {
		t.Error("config bytes changed on a cancelled edit")
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("the lock avenor did not create was removed: %v", err)
	}
}

// TestTrustLockReleaseKeepsALockThatChanged pins the ownership check in
// release: Claude Code's proper-lockfile takes a lock over after 10s, so a
// stalled avenor must not delete the directory Claude Code now holds.
func TestTrustLockReleaseKeepsALockThatChanged(t *testing.T) {
	t.Run("identity changed", func(t *testing.T) {
		f := newTrustConfigFixture(t, `{"projects": {}}`)
		lock, err := acquireTrustLock(context.Background(), f.configPath)
		if err != nil {
			t.Fatalf("acquireTrustLock: %v", err)
		}
		// Another writer replaced the lock directory with its own.
		if err := os.Remove(lock.path); err != nil {
			t.Fatalf("remove lock: %v", err)
		}
		if err := os.WriteFile(lock.path, []byte("taken over"), 0o600); err != nil {
			t.Fatalf("replace lock: %v", err)
		}

		lock.release()

		if _, err := os.Stat(lock.path); err != nil {
			t.Fatalf("release removed a lock that is no longer avenor's: %v", err)
		}
	})
	t.Run("mtime changed", func(t *testing.T) {
		f := newTrustConfigFixture(t, `{"projects": {}}`)
		lock, err := acquireTrustLock(context.Background(), f.configPath)
		if err != nil {
			t.Fatalf("acquireTrustLock: %v", err)
		}
		touched := time.Now().Add(time.Hour)
		if err := os.Chtimes(lock.path, touched, touched); err != nil {
			t.Fatalf("touch lock: %v", err)
		}

		lock.release()

		if _, err := os.Stat(lock.path); err != nil {
			t.Fatalf("release removed a lock whose mtime changed: %v", err)
		}
	})
}

// TestPreAcceptTrustAbortsPastTheRenameWindow checks an edit that held the
// lock longer than the stale window gives up instead of renaming: Claude Code
// may own the lock by then, so the edit would land under a lock it no longer
// holds.
func TestPreAcceptTrustAbortsPastTheRenameWindow(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	key := projectKey(t, f.work)
	before := readConfig(t, f.configPath)
	offset := time.Duration(0)
	fakeTrustClock(t, &offset)
	afterRead := func() { offset = trustLockRenameWindow + time.Second }

	err := editTrustConfig(context.Background(), f.configPath, key, afterRead)
	if err == nil {
		t.Fatal("editTrustConfig renamed the config after the rename window")
	}
	if got := readConfig(t, f.configPath); string(got) != string(before) {
		t.Error("config bytes changed past the rename window")
	}
	requireNoConfigLock(t, f.configPath)
	requireNoTempLeftovers(t, f.configPath)
}

// TestPreAcceptTrustLeavesAForeignOwnedConfigAlone checks a config owned by
// another user is never replaced: the temp file belongs to avenor, so a root
// or sudo run would take the user's config over.
func TestPreAcceptTrustLeavesAForeignOwnedConfigAlone(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	key := projectKey(t, f.work)
	before := readConfig(t, f.configPath)
	saved := trustFileOwner
	t.Cleanup(func() { trustFileOwner = saved })
	foreign := uint32(os.Geteuid()) + 1
	trustFileOwner = func(fs.FileInfo) (uint32, bool) { return foreign, true }

	err := editTrustConfig(context.Background(), f.configPath, key, nil)
	if err == nil {
		t.Fatal("editTrustConfig replaced a config owned by another user")
	}
	if got := readConfig(t, f.configPath); string(got) != string(before) {
		t.Error("config bytes changed for a config owned by another user")
	}
	if info, err := os.Stat(f.configPath); err != nil {
		t.Fatalf("stat config: %v", err)
	} else if owner, _ := trustFileOwner(info); owner != foreign {
		t.Errorf("config owner = %d, want the original %d", owner, foreign)
	}
	requireNoConfigLock(t, f.configPath)
	requireNoTempLeftovers(t, f.configPath)
}

func TestPreAcceptTrustLeavesNoLockBehind(t *testing.T) {
	t.Run("after success", func(t *testing.T) {
		f := newTrustConfigFixture(t, `{"projects": {}}`)
		if err := editTrustConfig(context.Background(), f.configPath, projectKey(t, f.work), nil); err != nil {
			t.Fatalf("editTrustConfig: %v", err)
		}
		requireNoConfigLock(t, f.configPath)
	})
	t.Run("after a parse error", func(t *testing.T) {
		f := newTrustConfigFixture(t, `{"projects": `)
		if err := editTrustConfig(context.Background(), f.configPath, projectKey(t, f.work), nil); err == nil {
			t.Fatal("editTrustConfig accepted malformed JSON")
		}
		requireNoConfigLock(t, f.configPath)
	})
	t.Run("after retries are exhausted", func(t *testing.T) {
		f := newTrustConfigFixture(t, `{"projects": {}}`)
		key := projectKey(t, f.work)
		body := readConfig(t, f.configPath)
		calls := 0
		afterRead := func() {
			calls++
			if err := os.WriteFile(f.configPath, append(append([]byte{}, body...), bytes.Repeat([]byte(" "), calls)...), 0o600); err != nil {
				t.Fatalf("concurrent write: %v", err)
			}
		}
		if err := editTrustConfig(context.Background(), f.configPath, key, afterRead); err == nil {
			t.Fatal("editTrustConfig kept retrying a config that always changed")
		}
		requireNoConfigLock(t, f.configPath)
		requireNoTempLeftovers(t, f.configPath)
	})
}

// TestPreAcceptTrustLosesNoUpdateBetweenGoroutines runs two edits of the same
// config at once. Nothing in this process serialises them: the config lock
// directory does, so both edits land instead of one replacing the other's
// write.
func TestPreAcceptTrustLosesNoUpdateBetweenGoroutines(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	other := filepath.Join(t.TempDir(), "other")
	if err := os.MkdirAll(other, 0o700); err != nil {
		t.Fatalf("mkdir other: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, dir := range []string{f.work, other} {
		wg.Add(1)
		go func(dir string) {
			defer wg.Done()
			<-start
			errs <- preAcceptTrust(context.Background(), f.configPath, f.home, dir)
		}(dir)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("preAcceptTrust: %v", err)
		}
	}

	projects := decodeProjects(t, decodeConfig(t, readConfig(t, f.configPath)))
	for _, dir := range []string{f.work, other} {
		key := projectKey(t, dir)
		if !entryTrusted(t, decodeEntry(t, projects, key)) {
			t.Errorf("projects[%q] was lost (keys: %v)", key, projectKeys(projects))
		}
	}
}

func TestPreAcceptTrustRetriesAfterAConcurrentWrite(t *testing.T) {
	f := newTrustConfigFixture(t, ``)
	key := projectKey(t, f.work)
	body := `{"projects": {` + mustQuote(t, key) + `: {"hasTrustDialogAccepted": false}}}`
	if err := os.WriteFile(f.configPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	calls := 0
	afterRead := func() {
		calls++
		if calls != 1 {
			return
		}
		// A second writer adds a field between the read and the rename.
		concurrent := `{"concurrent": true, "projects": {` + mustQuote(t, key) + `: {"hasTrustDialogAccepted": false}}}`
		if err := os.WriteFile(f.configPath, []byte(concurrent), 0o600); err != nil {
			t.Errorf("concurrent write: %v", err)
		}
	}

	if err := editTrustConfig(context.Background(), f.configPath, key, afterRead); err != nil {
		t.Fatalf("editTrustConfig: %v", err)
	}
	if calls < 2 {
		t.Fatalf("the config was read %d times, want a re-read", calls)
	}
	doc := decodeConfig(t, readConfig(t, f.configPath))
	if got := string(doc["concurrent"]); got != "true" {
		t.Errorf("concurrent field = %s, want true (the newer write was clobbered)", got)
	}
	if !entryTrusted(t, decodeEntry(t, decodeProjects(t, doc), key)) {
		t.Error("the retried edit did not mark the project trusted")
	}
	requireNoConfigLock(t, f.configPath)
	requireNoTempLeftovers(t, f.configPath)
}

func TestPreAcceptTrustGivesUpOnPersistentWrites(t *testing.T) {
	f := newTrustConfigFixture(t, ``)
	key := projectKey(t, f.work)
	body := `{"projects": {` + mustQuote(t, key) + `: {"hasTrustDialogAccepted": false}}}`
	if err := os.WriteFile(f.configPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	calls := 0
	afterRead := func() {
		calls++
		// Every attempt sees a different size, so none can rename.
		concurrent := body + strings.Repeat(" ", calls)
		if err := os.WriteFile(f.configPath, []byte(concurrent), 0o600); err != nil {
			t.Errorf("concurrent write: %v", err)
		}
	}

	err := editTrustConfig(context.Background(), f.configPath, key, afterRead)
	if err == nil {
		t.Fatal("editTrustConfig kept retrying a config that always changed")
	}
	if calls != trustConfigAttempts {
		t.Errorf("read %d times, want %d", calls, trustConfigAttempts)
	}
	if entry := decodeEntry(t, decodeProjects(t, decodeConfig(t, readConfig(t, f.configPath))), key); entryTrusted(t, entry) {
		t.Error("a losing retry wrote its edit anyway")
	}
	requireNoConfigLock(t, f.configPath)
	requireNoTempLeftovers(t, f.configPath)
}

func TestPreAcceptTrustKeysTheRunDirectoryAlone(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	// A .git entry in an ancestor must not move the trust record to it.
	repo := filepath.Join(t.TempDir(), "repo")
	runDir := filepath.Join(repo, "package")
	if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o700); err != nil {
		t.Fatalf("mkdir .git: %v", err)
	}
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}
	key := projectKey(t, runDir)

	if err := preAcceptTrust(context.Background(), f.configPath, f.home, runDir); err != nil {
		t.Fatalf("preAcceptTrust: %v", err)
	}
	projects := decodeProjects(t, decodeConfig(t, readConfig(t, f.configPath)))
	if len(projects) != 1 {
		t.Fatalf("projects has %d entries, want only the run directory: %v", len(projects), projectKeys(projects))
	}
	if !entryTrusted(t, decodeEntry(t, projects, key)) {
		t.Errorf("projects[%q] was not marked trusted", key)
	}
}

func TestPreAcceptTrustResolvesASymlinkedRunDirectory(t *testing.T) {
	f := newTrustConfigFixture(t, `{"projects": {}}`)
	realDir := filepath.Join(t.TempDir(), "real")
	if err := os.MkdirAll(realDir, 0o700); err != nil {
		t.Fatalf("mkdir real dir: %v", err)
	}
	linkDir := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	key := projectKey(t, realDir)

	if err := preAcceptTrust(context.Background(), f.configPath, f.home, linkDir); err != nil {
		t.Fatalf("preAcceptTrust: %v", err)
	}
	projects := decodeProjects(t, decodeConfig(t, readConfig(t, f.configPath)))
	if !entryTrusted(t, decodeEntry(t, projects, key)) {
		t.Errorf("trust was not filed under the resolved directory %q (keys: %v)", key, projectKeys(projects))
	}
}

func TestPreAcceptTrustUsesClaudeConfigDir(t *testing.T) {
	dir := t.TempDir()
	home := filepath.Join(dir, "home")
	configDir := filepath.Join(dir, "claude-config")
	work := filepath.Join(dir, "work")
	for _, d := range []string{home, configDir, work} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	homeConfig := filepath.Join(home, ".claude.json")
	if err := os.WriteFile(homeConfig, []byte(`{"projects": {}}`), 0o600); err != nil {
		t.Fatalf("write home config: %v", err)
	}
	configPath := filepath.Join(configDir, ".claude.json")
	if err := os.WriteFile(configPath, []byte(`{"projects": {}}`), 0o600); err != nil {
		t.Fatalf("write config dir config: %v", err)
	}
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", configDir)
	t.Setenv("CLAUDE_CODE_CUSTOM_OAUTH_URL", "")
	key := projectKey(t, work)

	if err := PreAcceptTrust(context.Background(), work); err != nil {
		t.Fatalf("PreAcceptTrust: %v", err)
	}
	if !entryTrusted(t, decodeEntry(t, decodeProjects(t, decodeConfig(t, readConfig(t, configPath))), key)) {
		t.Error("CLAUDE_CONFIG_DIR/.claude.json was not updated")
	}
	if got := string(readConfig(t, homeConfig)); got != `{"projects": {}}` {
		t.Errorf("~/.claude.json changed: %s", got)
	}
}

func mustQuote(t *testing.T, value string) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %q: %v", value, err)
	}
	return string(raw)
}
