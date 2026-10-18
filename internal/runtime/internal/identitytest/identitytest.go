// Package identitytest holds the child-observation fixture the runtime backend
// tests share. A helper child serializes an allowlisted environment to a file,
// so a test asserts what the process itself saw rather than what avenor
// intended to pass.
package identitytest

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// UnsetValue is what the observer reports for a variable the child did not
// inherit, so a test can tell "unset" from "empty".
const UnsetValue = "__UNSET__"

// Sentinel lines bracket every observation group. The completion sentinel is
// written last, so a reader waits for a finished group instead of for
// particular keys to appear.
const (
	groupSentinel = "IDENTITY_OBSERVATION_BEGIN"
	doneSentinel  = "IDENTITY_OBSERVATION_END"
)

// OperationalKeys are the non-identity variables the observer records to show
// the scrub is exact: a near miss of an identity name, an unrelated avenor
// variable and the broker credential pair.
func OperationalKeys() []string {
	return []string{
		runtime.EnvRuntimeID + "_SUFFIX",
		"AVENOR_TREE_BUDGET",
		"AVENOR_RUN_ID",
		"AVENOR_BROKER_TOKEN",
	}
}

// Keys returns every environment name the observer serializes.
func Keys(extra ...string) []string {
	keys := runtime.IdentityEnvNames()
	keys = append(keys, OperationalKeys()...)
	return append(keys, extra...)
}

// ShellQuote quotes s for a POSIX shell.
func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// Body renders a shell block that appends one observation group to obsPath as
// KEY=hex(value) lines. Values are hex encoded so quotes, newlines and
// command-substitution text stay unambiguous. Appending keeps every launch of
// a session readable.
func Body(obsPath string, extraKeys ...string) string {
	var b strings.Builder
	b.WriteString("observe() {\n")
	b.WriteString("  printf '%s=%s\\n' \"$1\" \"$(printf '%s' \"$2\" | od -An -tx1 | tr -d ' \\n')\"\n")
	b.WriteString("}\n")
	b.WriteString("{\n")
	fmt.Fprintf(&b, "printf '%s\\n'\n", groupSentinel)
	for _, key := range Keys(extraKeys...) {
		fmt.Fprintf(&b, "observe %s \"${%s-%s}\"\n", key, key, UnsetValue)
	}
	fmt.Fprintf(&b, "printf '%s\\n'\n", doneSentinel)
	fmt.Fprintf(&b, "} >> %s\n", ShellQuote(obsPath))
	return b.String()
}

// Read waits for the last finished observation group and decodes it.
func Read(t *testing.T, path string) map[string]string {
	t.Helper()
	groups := ReadGroups(t, path, 1)
	return groups[len(groups)-1]
}

// ReadGroups waits for want finished observation groups, so a session that
// launches more than one process can read each launch.
func ReadGroups(t *testing.T, path string, want int) []map[string]string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			if groups := decode(t, raw); len(groups) >= want {
				return groups
			}
		}
		if time.Now().After(deadline) {
			raw, _ := os.ReadFile(path)
			t.Fatalf("timed out waiting for %d identity observation groups; have:\n%s", want, raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// decode returns every group the completion sentinel closed. A trailing group
// that is still being written is ignored.
func decode(t *testing.T, raw []byte) []map[string]string {
	t.Helper()
	text := string(raw)
	end := strings.LastIndexByte(text, '\n')
	if end < 0 {
		return nil
	}
	var (
		groups  []map[string]string
		current map[string]string
	)
	for _, line := range strings.Split(text[:end], "\n") {
		switch line {
		case groupSentinel:
			current = map[string]string{}
		case doneSentinel:
			if current == nil {
				t.Fatalf("identity observation sentinel without a group")
			}
			groups = append(groups, current)
			current = nil
		case "":
		default:
			if current == nil {
				t.Fatalf("identity observation %q outside a group", line)
			}
			key, encoded, ok := strings.Cut(line, "=")
			if !ok {
				t.Fatalf("malformed observation line %q", line)
			}
			value, err := hex.DecodeString(encoded)
			if err != nil {
				t.Fatalf("decode %s observation: %v", key, err)
			}
			current[key] = string(value)
		}
	}
	return groups
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
// misses plus unrelated avenor variables survive. AlsoScrubbed names are
// backend markers that must not reach the child.
func Assert(t *testing.T, observed map[string]string, want *runtime.LaunchProvenance, alsoScrubbed ...string) {
	t.Helper()
	if want == nil {
		AssertCleared(t, observed, alsoScrubbed...)
		return
	}
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
		if observed[key] != UnsetValue {
			t.Errorf("%s = %q, want it unset", key, observed[key])
		}
	}
	for _, key := range alsoScrubbed {
		if observed[key] != UnsetValue {
			t.Errorf("%s = %q, want it scrubbed", key, observed[key])
		}
	}
	if observed[runtime.EnvRuntimeID+"_SUFFIX"] != "near-miss" {
		t.Errorf("near-miss variable = %q, want it preserved", observed[runtime.EnvRuntimeID+"_SUFFIX"])
	}
	if observed["AVENOR_TREE_BUDGET"] != "7" {
		t.Errorf("AVENOR_TREE_BUDGET = %q, want 7", observed["AVENOR_TREE_BUDGET"])
	}
}

// AssertCleared requires that no identity name reached the child.
func AssertCleared(t *testing.T, observed map[string]string, alsoScrubbed ...string) {
	t.Helper()
	for _, key := range runtime.IdentityEnvNames() {
		if observed[key] != UnsetValue {
			t.Errorf("%s = %q, want it scrubbed", key, observed[key])
		}
	}
	for _, key := range alsoScrubbed {
		if observed[key] != UnsetValue {
			t.Errorf("%s = %q, want it scrubbed", key, observed[key])
		}
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
