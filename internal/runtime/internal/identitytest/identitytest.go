// Package identitytest holds the child-observation fixture the runtime backend
// tests share. A helper child serializes an allowlisted environment to a file,
// so a test asserts what the process itself saw rather than what avenor
// intended to pass.
package identitytest

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// unsetValue is what the observer reports for a variable the child did not
// inherit, so a test can tell "unset" from "empty".
const unsetValue = "__UNSET__"

// keys returns every environment name the observer serializes: launch identity
// plus the operational variables that prove the scrub is exact.
func keys() []string {
	return append(runtime.IdentityEnvNames(),
		runtime.EnvRuntimeID+"_SUFFIX", // near miss of an identity name
		"AVENOR_TREE_BUDGET",
		"AVENOR_RUN_ID",
		"AVENOR_BROKER_TOKEN",
		"AVENOR_BROKER_URL",
	)
}

// shellQuote quotes s for a POSIX shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// WriteTransparentWrapper writes a launch wrapper that runs its argv and
// environment unchanged, and returns its path.
func WriteTransparentWrapper(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wrapper.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexec \"$@\"\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}
	return path
}

// Body renders a shell block that writes this launch's observation atomically
// as KEY=hex(value) lines. Values are hex encoded so quotes, newlines and
// command-substitution text stay unambiguous, and the rename means a reader
// never sees a half-written file.
func Body(obsPath string) string {
	var b strings.Builder
	b.WriteString("observe() {\n")
	b.WriteString("  printf '%s=%s\\n' \"$1\" \"$(printf '%s' \"$2\" | od -An -tx1 | tr -d ' \\n')\"\n")
	b.WriteString("}\n")
	fmt.Fprintf(&b, "obs=%s\n", shellQuote(obsPath))
	b.WriteString("{\n")
	for _, key := range keys() {
		fmt.Fprintf(&b, "observe %s \"${%s-%s}\"\n", key, key, unsetValue)
	}
	b.WriteString("} > \"$obs.tmp\" && mv \"$obs.tmp\" \"$obs\"\n")
	return b.String()
}

// Read waits for the observation file and decodes it.
func Read(t *testing.T, path string) map[string]string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil && len(raw) > 0 {
			return decode(t, raw)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for an identity observation in %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func decode(t *testing.T, raw []byte) map[string]string {
	t.Helper()
	observed := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		key, encoded, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("malformed observation line %q", line)
		}
		value, err := hex.DecodeString(encoded)
		if err != nil {
			t.Fatalf("decode %s observation: %v", key, err)
		}
		observed[key] = string(value)
	}
	return observed
}

// PoisonParent exports inherited identity plus operational variables, so a
// child that keeps them instead of scrubbing fails the test.
func PoisonParent(t *testing.T) {
	t.Helper()
	for key, value := range map[string]string{
		runtime.EnvRuntimeID:             "parent-runtime",
		runtime.EnvExecutionID:           "parent-execution",
		runtime.EnvKind:                  "parent-kind",
		runtime.EnvPhase:                 "parent-phase",
		runtime.EnvIteration:             "99",
		runtime.EnvLabel:                 "parent-label",
		runtime.EnvAttempt:               "42",
		"AVENOR_PARENT_RUN_ID":           "parent-superseded",
		runtime.EnvRuntimeID + "_SUFFIX": "near-miss",
		"AVENOR_TREE_BUDGET":             "7",
		"AVENOR_RUN_ID":                  "broker-run",
		"AVENOR_BROKER_TOKEN":            "broker-token",
		"AVENOR_BROKER_URL":              "http://stale.invalid",
	} {
		t.Setenv(key, value)
	}
}

// Provenance is a launch identity whose values a shell must not reinterpret.
func Provenance(runtimeID string) *runtime.LaunchProvenance {
	return &runtime.LaunchProvenance{
		RuntimeID:   runtimeID,
		ExecutionID: runtimeID,
		Kind:        "team",
		Phase:       "member 'one' $(id) \nsecond",
		Label:       "label with 'quotes' $(echo pwned) `id`;done",
		Attempt:     2,
	}
}

// Assert requires the identity the launch supplied and an exact scrub: identity
// names the launch did not set are unset, the superseded name is gone, and near
// misses plus unrelated avenor variables survive.
func Assert(t *testing.T, observed map[string]string, want *runtime.LaunchProvenance) {
	t.Helper()
	supplied := map[string]string{}
	for _, entry := range want.IdentityEnv() {
		key, value, _ := strings.Cut(entry, "=")
		supplied[key] = value
	}
	for _, key := range runtime.IdentityEnvNames() {
		if value, ok := supplied[key]; ok {
			if observed[key] != value {
				t.Errorf("%s = %q, want %q", key, observed[key], value)
			}
			continue
		}
		if observed[key] != unsetValue {
			t.Errorf("%s = %q, want it unset", key, observed[key])
		}
	}
	if observed[runtime.EnvRuntimeID+"_SUFFIX"] != "near-miss" {
		t.Errorf("near-miss variable = %q, want it preserved", observed[runtime.EnvRuntimeID+"_SUFFIX"])
	}
	if observed["AVENOR_TREE_BUDGET"] != "7" {
		t.Errorf("AVENOR_TREE_BUDGET = %q, want 7", observed["AVENOR_TREE_BUDGET"])
	}
}

// AssertBrokerPreserved requires the broker credential pair to survive an
// identity scrub.
func AssertBrokerPreserved(t *testing.T, observed map[string]string) {
	t.Helper()
	if observed["AVENOR_RUN_ID"] != "broker-run" || observed["AVENOR_BROKER_TOKEN"] != "broker-token" {
		t.Errorf("broker pair = %q/%q, want it preserved", observed["AVENOR_RUN_ID"], observed["AVENOR_BROKER_TOKEN"])
	}
}
