package agy

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
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
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

// observerBody appends one BEGIN-delimited group per launch so tests can read
// every process a session started.
func observerBody(obsPath string) string {
	var b strings.Builder
	b.WriteString("observe() {\n")
	b.WriteString("  printf '%s=%s\\n' \"$1\" \"$(printf '%s' \"$2\" | od -An -tx1 | tr -d ' \\n')\"\n")
	b.WriteString("}\n")
	b.WriteString("{\n")
	b.WriteString("printf 'BEGIN\\n'\n")
	for _, key := range identityObservationKeys {
		fmt.Fprintf(&b, "observe %s \"${%s-%s}\"\n", key, key, unsetValue)
	}
	fmt.Fprintf(&b, "} >> %s\n", testShellQuote(obsPath))
	return b.String()
}

func testShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// writeIdentityAgy installs a real `agy` child on PATH that records its
// environment and then emits a minimal init/result pair for the conversation
// it was asked to use.
func writeIdentityAgy(t *testing.T, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" + observerBody(obsPath) + `
conv=""
prev=""
for arg in "$@"; do
  if [ "$prev" = "--conversation" ]; then conv="$arg"; fi
  prev="$arg"
done
[ -n "$conv" ] || conv="conv_child"
printf '{"event":"init","conversation_id":"%s"}\n' "$conv"
printf '{"event":"result","conversation_id":"%s","result":{"response":"ok","status":"SUCCESS"}}\n' "$conv"
`
	if err := os.WriteFile(filepath.Join(binDir, "agy"), []byte(body), 0o755); err != nil {
		t.Fatalf("write agy child: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func readIdentityObservations(t *testing.T, path string, want int) []map[string]string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			groups := parseIdentityObservations(t, raw)
			if len(groups) >= want {
				return groups
			}
		}
		if time.Now().After(deadline) {
			raw, _ := os.ReadFile(path)
			t.Fatalf("timed out waiting for %d identity observations; have:\n%s", want, raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func parseIdentityObservations(t *testing.T, raw []byte) []map[string]string {
	t.Helper()
	var groups []map[string]string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "BEGIN" {
			groups = append(groups, map[string]string{})
			continue
		}
		if len(groups) == 0 {
			continue
		}
		key, encoded, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("malformed observation line %q", line)
		}
		value, err := hex.DecodeString(encoded)
		if err != nil {
			t.Fatalf("decode %s observation: %v", key, err)
		}
		groups[len(groups)-1][key] = string(value)
	}
	return groups
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

func identityProvenance(runtimeID string) *runtime.LaunchProvenance {
	return &runtime.LaunchProvenance{
		RuntimeID:   runtimeID,
		ExecutionID: runtimeID,
		Kind:        "loop",
		Phase:       "body 'one' $(id) \nsecond",
		Label:       "label with 'quotes' $(echo pwned) `id`;done",
		Attempt:     2,
	}
}

func assertIdentityGroup(t *testing.T, observed map[string]string, want *runtime.LaunchProvenance) {
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

func TestIdentityChildEnv(t *testing.T) {
	t.Run("initial print", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityAgy(t, obsPath)

		provenance := identityProvenance("rt_child")
		p := testProvider(runtime.StartOptions{Dir: t.TempDir()})
		session, err := p.Start(context.Background(), runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: provenance,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := p.Prompt(context.Background(), session.SessionID, "hi"); err != nil {
			t.Fatalf("Prompt: %v", err)
		}

		groups := readIdentityObservations(t, obsPath, 1)
		if len(groups) != 1 {
			t.Fatalf("launches = %d, want 1", len(groups))
		}
		assertIdentityGroup(t, groups[0], provenance)
	})

	t.Run("resumed print continuation keeps the launch identity", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityAgy(t, obsPath)

		want := identityProvenance("rt_resumed")
		caller := identityProvenance("rt_resumed")
		p := testProvider(runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: identityProvenance("rt_constructor"),
		})
		session, err := p.ResumeWithOptions(context.Background(), "conv_resume", runtime.StartOptions{
			Provenance: caller,
		})
		if err != nil {
			t.Fatalf("ResumeWithOptions: %v", err)
		}
		// A later caller mutation must not reach the snapshotted session.
		caller.RuntimeID = "rt_mutated"
		caller.Attempt = 9

		for i := 0; i < 2; i++ {
			if err := p.Prompt(context.Background(), session.SessionID, "hi"); err != nil {
				t.Fatalf("Prompt %d: %v", i, err)
			}
		}

		groups := readIdentityObservations(t, obsPath, 2)
		if len(groups) != 2 {
			t.Fatalf("launches = %d, want 2", len(groups))
		}
		for i, group := range groups {
			if group[runtime.EnvRuntimeID] != want.RuntimeID || group[runtime.EnvAttempt] != "2" {
				t.Fatalf("launch %d identity = %s/%s, want %s/2", i, group[runtime.EnvRuntimeID], group[runtime.EnvAttempt], want.RuntimeID)
			}
		}
	})

	t.Run("rpc pty host", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityAgy(t, obsPath)

		provenance := identityProvenance("rt_rpc")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := startPTYRPCHost(ctx, terminal.PTYLauncher{}, runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: provenance,
		}, "", "1.1.5", rpcDiscoveryOptions{})
		if err == nil {
			t.Fatal("startPTYRPCHost succeeded against a child with no RPC endpoint")
		}

		groups := readIdentityObservations(t, obsPath, 1)
		assertIdentityGroup(t, groups[0], provenance)
	})
}
