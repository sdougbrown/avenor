package codexappserver

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

// writeCodexIdentityChild installs a real `codex` child on PATH. It speaks the
// minimal app-server handshake and records the allowlisted environment first.
func writeCodexIdentityChild(t *testing.T, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" + observerBody(obsPath) + `
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  [ -n "$id" ] || continue
  case "$line" in
    *'"method":"thread/start"'*|*'"method":"thread/resume"'*)
      printf '{"id":"%s","result":{"thread":{"id":"th_child"}}}\n' "$id"
      ;;
    *)
      printf '{"id":"%s","result":{}}\n' "$id"
      ;;
  esac
done
`
	if err := os.WriteFile(filepath.Join(binDir, "codex"), []byte(body), 0o755); err != nil {
		t.Fatalf("write codex child: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func writeTransparentWrapper(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wrapper.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexec \"$@\"\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}
	return path
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

func assertIdentityObservation(t *testing.T, observed map[string]string, want *runtime.LaunchProvenance) {
	t.Helper()
	for _, entry := range want.IdentityEnv() {
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
}

func freshProvenance() *runtime.LaunchProvenance {
	return &runtime.LaunchProvenance{
		RuntimeID:   "rt_child",
		ExecutionID: "rt_child",
		Kind:        "team",
		Phase:       "reviewer 'one' $(id) \nsecond",
		Label:       "label with 'quotes' $(echo pwned) `id`;done",
		Attempt:     2,
	}
}

func TestIdentityChildEnv(t *testing.T) {
	t.Run("start client through a transparent wrapper", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeCodexIdentityChild(t, obsPath)
		wrapper := writeTransparentWrapper(t)

		provenance := freshProvenance()
		c, err := StartClient(context.Background(), runtime.StartOptions{
			LaunchWrapper: []string{wrapper},
			Provenance:    provenance,
		})
		if err != nil {
			t.Fatalf("StartClient: %v", err)
		}
		defer c.Close()

		assertIdentityObservation(t, readIdentityObservation(t, obsPath), provenance)
	})

	t.Run("nil identity clears inherited values", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeCodexIdentityChild(t, obsPath)

		c, err := StartClient(context.Background(), runtime.StartOptions{})
		if err != nil {
			t.Fatalf("StartClient: %v", err)
		}
		defer c.Close()

		observed := readIdentityObservation(t, obsPath)
		for _, key := range []string{
			runtime.EnvRuntimeID, runtime.EnvExecutionID, runtime.EnvKind,
			runtime.EnvPhase, runtime.EnvIteration, runtime.EnvLabel,
			runtime.EnvAttempt, "AVENOR_PARENT_RUN_ID",
		} {
			if observed[key] != unsetValue {
				t.Errorf("%s = %q, want it scrubbed", key, observed[key])
			}
		}
	})
}

func TestProviderStartUsesProvenanceAtLaunch(t *testing.T) {
	poisonParentIdentity(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	writeCodexIdentityChild(t, obsPath)

	provenance := freshProvenance()
	p := NewWithOptions(runtime.StartOptions{})
	defer p.Close()
	if _, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: provenance,
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	assertIdentityObservation(t, readIdentityObservation(t, obsPath), provenance)
}

// TestProviderResumeWithOptionsCarriesFreshIdentity pins the fresh-client
// resume path: the new app-server is launched with the caller's merged
// identity, not the constructor default.
func TestProviderResumeWithOptionsCarriesFreshIdentity(t *testing.T) {
	poisonParentIdentity(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	writeCodexIdentityChild(t, obsPath)

	p := NewWithOptions(runtime.StartOptions{
		Provenance: &runtime.LaunchProvenance{
			RuntimeID:   "rt_old",
			ExecutionID: "rt_old",
			Kind:        "loop",
			Phase:       "old-phase",
			Iteration:   intPtr(4),
			Label:       "old-label",
			Attempt:     1,
		},
	})
	defer p.Close()

	fresh := freshProvenance()
	if _, err := p.ResumeWithOptions(context.Background(), "th_unknown", runtime.StartOptions{
		Provenance: fresh,
	}); err != nil {
		t.Fatalf("ResumeWithOptions: %v", err)
	}

	observed := readIdentityObservation(t, obsPath)
	assertIdentityObservation(t, observed, fresh)
	if observed[runtime.EnvIteration] != unsetValue {
		t.Errorf("AVENOR_ITERATION = %q, want the previous iteration cleared", observed[runtime.EnvIteration])
	}
}

// TestProviderResumeWithOptionsKeepsLiveProcessIdentity pins the live-process
// half: an existing app-server keeps the identity it was launched with.
func TestProviderResumeWithOptionsKeepsLiveProcessIdentity(t *testing.T) {
	poisonParentIdentity(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	writeCodexIdentityChild(t, obsPath)

	first := freshProvenance()
	p := NewWithOptions(runtime.StartOptions{})
	defer p.Close()
	session, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: first,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	firstObserved := readIdentityObservation(t, obsPath)

	if _, err := p.ResumeWithOptions(context.Background(), session.SessionID, runtime.StartOptions{
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_new", ExecutionID: "rt_new", Kind: "run", Attempt: 5},
	}); err != nil {
		t.Fatalf("ResumeWithOptions: %v", err)
	}

	observed := readIdentityObservation(t, obsPath)
	if observed[runtime.EnvRuntimeID] != firstObserved[runtime.EnvRuntimeID] || observed[runtime.EnvKind] != "team" {
		t.Fatalf("live process identity changed: %v", observed)
	}
	if p.client == nil {
		t.Fatal("resume dropped the live client")
	}
}

func intPtr(v int) *int { return &v }
