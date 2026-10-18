package pi

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
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// identityObservationKeys is the allowlist the helper child serializes. It
// covers the exported identity names, the superseded name that must be
// scrubbed, the preserved broker pair, a near-miss and an unrelated
// operational variable.
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
	"AVENOR_BROKER_URL",
	"AVENOR_BROKER_TOKEN",
	"PI_AGENT",
}

const unsetValue = "__UNSET__"

// observerBody renders a shell block that writes every allowlisted variable to
// obsPath as KEY=hex(value), so quotes, newlines and command substitution stay
// unambiguous.
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

// writeIdentityPiChild installs a real `pi` child on PATH. The child records
// the allowlisted environment before answering get_state, so the observation
// is what the process itself saw.
func writeIdentityPiChild(t *testing.T, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	script := "#!/bin/sh\n" + observerBody(obsPath) + `
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  [ -n "$id" ] || continue
  printf '{"type":"response","id":"%s","sessionId":"pi_child"}\n' "$id"
done
`
	if err := os.WriteFile(filepath.Join(binDir, "pi"), []byte(script), 0o755); err != nil {
		t.Fatalf("write pi child: %v", err)
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
	t.Setenv("AVENOR_RUN_ID", "stale-run")
	t.Setenv("AVENOR_BROKER_URL", "http://stale.invalid")
	t.Setenv("AVENOR_BROKER_TOKEN", "stale-token")
	t.Setenv("PI_AGENT", "stale-agent")
}

func assertProvenanceObservation(t *testing.T, observed map[string]string, want *runtime.LaunchProvenance) {
	t.Helper()
	for _, entry := range want.IdentityEnv() {
		key, value, _ := strings.Cut(entry, "=")
		if observed[key] != value {
			t.Errorf("%s = %q, want %q", key, observed[key], value)
		}
	}
	for _, key := range []string{"AVENOR_PARENT_RUN_ID"} {
		if observed[key] != unsetValue {
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

func TestIdentityChildEnv(t *testing.T) {
	provenance := &runtime.LaunchProvenance{
		RuntimeID:   "rt_child",
		ExecutionID: "rt_child",
		Kind:        "team",
		Phase:       "reviewer 'one' $(id) \nsecond",
		Label:       "label with 'quotes' $(echo pwned) `id`;done",
		Attempt:     2,
	}

	t.Run("fresh start", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityPiChild(t, obsPath)

		p := NewWithOptions(runtime.StartOptions{})
		t.Cleanup(func() { _ = p.Close() })
		session, err := p.Start(context.Background(), runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: provenance,
			Agent:      "jockey",
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if session.SessionID != "pi_child" {
			t.Fatalf("session = %q, want pi_child", session.SessionID)
		}

		observed := readIdentityObservation(t, obsPath)
		assertProvenanceObservation(t, observed, provenance)
		if observed["PI_AGENT"] != "jockey" {
			t.Errorf("PI_AGENT = %q, want jockey", observed["PI_AGENT"])
		}
	})

	t.Run("transparent wrapper and broker credentials", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityPiChild(t, obsPath)
		wrapper := writeTransparentWrapper(t)

		b := broker.New(broker.MakeToken())
		if err := b.Start(); err != nil {
			t.Fatalf("broker.Start: %v", err)
		}
		t.Cleanup(func() { _ = b.Stop() })
		brokerToken, _ := b.EnsureRun("rt_broker")
		brokerURL := "http://" + b.Addr()

		p := NewWithOptions(runtime.StartOptions{})
		t.Cleanup(func() { _ = p.Close() })
		_, err := p.Start(context.Background(), runtime.StartOptions{
			Dir:           t.TempDir(),
			LaunchWrapper: []string{wrapper},
			RuntimeID:     "rt_broker",
			BrokerToken:   brokerToken,
			Broker:        b,
			Provenance:    provenance,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}

		observed := readIdentityObservation(t, obsPath)
		assertProvenanceObservation(t, observed, provenance)
		if observed["AVENOR_RUN_ID"] != "rt_broker" {
			t.Errorf("AVENOR_RUN_ID = %q, want rt_broker", observed["AVENOR_RUN_ID"])
		}
		if observed["AVENOR_BROKER_URL"] != brokerURL {
			t.Errorf("AVENOR_BROKER_URL = %q, want %q", observed["AVENOR_BROKER_URL"], brokerURL)
		}
		if observed["AVENOR_BROKER_TOKEN"] != brokerToken {
			t.Errorf("AVENOR_BROKER_TOKEN = %q, want the fresh token", observed["AVENOR_BROKER_TOKEN"])
		}
	})

	t.Run("nil identity clears inherited values", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityPiChild(t, obsPath)

		p := NewWithOptions(runtime.StartOptions{})
		t.Cleanup(func() { _ = p.Close() })
		if _, err := p.Start(context.Background(), runtime.StartOptions{Dir: t.TempDir()}); err != nil {
			t.Fatalf("Start: %v", err)
		}

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
		if observed["PI_AGENT"] != unsetValue {
			t.Errorf("PI_AGENT = %q, want stale agent removed", observed["PI_AGENT"])
		}
	})
}

func TestResumeWithOptionsIgnoresConstructorThinking(t *testing.T) {
	c, clientOut, clientIn := fakeClient()
	defer c.Close()

	// Answer every command so a wrongly applied thinking RPC cannot block the
	// resume; the transcript is the assertion.
	commands := make(chan map[string]any, 4)
	go func() {
		for {
			cmd, err := readCommand(clientIn)
			if err != nil {
				return
			}
			commands <- cmd
			writeLine(clientOut, map[string]any{
				"type":    "response",
				"id":      cmd["id"],
				"success": true,
			})
		}
	}()

	p := NewWithOptions(runtime.StartOptions{Thinking: "high"})
	p.client = c
	p.sessions["pi_live"] = struct{}{}
	c.setSessionID("pi_live")

	if _, err := p.ResumeWithOptions(context.Background(), "pi_live", runtime.StartOptions{
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_new", Attempt: 2},
	}); err != nil {
		t.Fatalf("ResumeWithOptions: %v", err)
	}

	// An identity-only resume must not send a thinking RPC even though the
	// constructor asked for one.
	select {
	case cmd := <-commands:
		t.Fatalf("identity-only resume wrote %v, want no RPC", cmd)
	case <-time.After(300 * time.Millisecond):
	}
}
