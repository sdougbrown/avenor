package factory

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

var identityObservationKeys = []string{
	runtime.EnvRuntimeID,
	runtime.EnvExecutionID,
	runtime.EnvKind,
	runtime.EnvPhase,
	runtime.EnvIteration,
	runtime.EnvLabel,
	runtime.EnvAttempt,
	"AVENOR_PARENT_RUN_ID",
	runtime.EnvRuntimeID + "_SUFFIX",
	"AVENOR_TREE_BUDGET",
	"AVENOR_RUN_ID",
	"AVENOR_BROKER_TOKEN",
}

const unsetValue = "__UNSET__"

func observerBody(obsPath string) string {
	var b strings.Builder
	b.WriteString("observe() {\n")
	b.WriteString("  printf '%s=%s\\n' \"$1\" \"$(printf '%s' \"$2\" | od -An -tx1 | tr -d ' \\n')\"\n")
	b.WriteString("}\n")
	b.WriteString("{\n")
	for _, key := range identityObservationKeys {
		fmt.Fprintf(&b, "observe %s \"${%s-%s}\"\n", key, key, unsetValue)
	}
	fmt.Fprintf(&b, "} > %s\n", testShellQuote(obsPath))
	return b.String()
}

func testShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func writeAcpIdentityChild(t *testing.T, binName, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" + observerBody(obsPath) + `
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  [ -n "$id" ] || continue
  printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"sessionId":"ses_identity"}}\n' "$id"
done
`
	if err := os.WriteFile(filepath.Join(binDir, binName), []byte(body), 0o755); err != nil {
		t.Fatalf("write %s child: %v", binName, err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func readIdentityObservation(t *testing.T, path string) map[string]string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil &&
			strings.Contains(string(raw), "AVENOR_RUNTIME_ID=") &&
			strings.Contains(string(raw), identityObservationKeys[len(identityObservationKeys)-1]+"=") {
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
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for identity observation at %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func poisonParentIdentity(t *testing.T) {
	t.Helper()
	t.Setenv(runtime.EnvRuntimeID, "parent-runtime")
	t.Setenv(runtime.EnvExecutionID, "parent-execution")
	t.Setenv(runtime.EnvKind, "parent-kind")
	t.Setenv(runtime.EnvPhase, "parent-phase")
	t.Setenv(runtime.EnvIteration, "99")
	t.Setenv(runtime.EnvLabel, "parent-label")
	t.Setenv(runtime.EnvAttempt, "42")
	t.Setenv("AVENOR_PARENT_RUN_ID", "parent-superseded")
	t.Setenv(runtime.EnvRuntimeID+"_SUFFIX", "near-miss")
	t.Setenv("AVENOR_TREE_BUDGET", "7")
	t.Setenv("AVENOR_RUN_ID", "broker-run")
	t.Setenv("AVENOR_BROKER_TOKEN", "broker-token")
}

// TestIdentityChildEnv drives every ACP-family constructor through the factory
// and asserts what the real child process saw.
func TestIdentityChildEnv(t *testing.T) {
	cases := []struct {
		backend string
		bin     string
	}{
		{backend: "opencode-acp", bin: "opencode"},
		{backend: "gemini-acp", bin: "gemini"},
		{backend: "cursor-acp", bin: "agent"},
	}
	for _, tc := range cases {
		t.Run(tc.backend, func(t *testing.T) {
			poisonParentIdentity(t)
			obsPath := filepath.Join(t.TempDir(), "observed.env")
			writeAcpIdentityChild(t, tc.bin, obsPath)

			provenance := &runtime.LaunchProvenance{
				RuntimeID:   "rt_child",
				ExecutionID: "rt_child",
				Kind:        "team",
				Phase:       "reviewer 'one' $(id) \nsecond",
				Label:       "label with 'quotes' $(echo pwned) `id`;done",
				Attempt:     2,
			}
			p, err := NewProvider(runtime.StartOptions{}, tc.backend)
			if err != nil {
				t.Fatalf("NewProvider: %v", err)
			}
			if _, err := p.Start(context.Background(), runtime.StartOptions{
				Dir:        t.TempDir(),
				Provenance: provenance,
			}); err != nil {
				t.Fatalf("Start: %v", err)
			}
			t.Cleanup(func() {
				if closer, ok := p.(interface{ Close() error }); ok {
					_ = closer.Close()
				}
			})

			observed := readIdentityObservation(t, obsPath)
			for _, entry := range provenance.IdentityEnv() {
				key, value, _ := strings.Cut(entry, "=")
				if observed[key] != value {
					t.Errorf("%s = %q, want %q", key, observed[key], value)
				}
			}
			if observed["AVENOR_PARENT_RUN_ID"] != unsetValue {
				t.Errorf("AVENOR_PARENT_RUN_ID = %q, want it scrubbed", observed["AVENOR_PARENT_RUN_ID"])
			}
			if observed[runtime.EnvRuntimeID+"_SUFFIX"] != "near-miss" {
				t.Errorf("near-miss variable = %q, want it preserved", observed[runtime.EnvRuntimeID+"_SUFFIX"])
			}
			if observed["AVENOR_TREE_BUDGET"] != "7" {
				t.Errorf("AVENOR_TREE_BUDGET = %q, want 7", observed["AVENOR_TREE_BUDGET"])
			}
			if observed["AVENOR_RUN_ID"] != "broker-run" || observed["AVENOR_BROKER_TOKEN"] != "broker-token" {
				t.Errorf("broker pair = %q/%q, want it preserved", observed["AVENOR_RUN_ID"], observed["AVENOR_BROKER_TOKEN"])
			}
		})
	}
}
