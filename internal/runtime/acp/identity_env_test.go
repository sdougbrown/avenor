package acp

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
	"OPENCODE_CONFIG",
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
	if observed["AVENOR_RUN_ID"] != "broker-run" || observed["AVENOR_BROKER_TOKEN"] != "broker-token" {
		t.Errorf("broker pair = %q/%q, want it preserved", observed["AVENOR_RUN_ID"], observed["AVENOR_BROKER_TOKEN"])
	}
}

func identityProvenance() *runtime.LaunchProvenance {
	return &runtime.LaunchProvenance{
		RuntimeID:   "rt_child",
		ExecutionID: "rt_child",
		Kind:        "loop",
		Phase:       "body $(id) 'quoted'",
		Label:       "label 'quoted' $(echo pwned) `id`;done",
		Attempt:     3,
	}
}

func TestIdentityChildEnv(t *testing.T) {
	poisonParentIdentity(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	configPath := filepath.Join(t.TempDir(), "candidate.json")
	if err := os.WriteFile(configPath, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	script := filepath.Join(t.TempDir(), "acp-child.sh")
	body := "#!/bin/sh\n" + observerBody(obsPath) + "cat > /dev/null\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write child: %v", err)
	}
	wrapper := filepath.Join(t.TempDir(), "wrapper.sh")
	if err := os.WriteFile(wrapper, []byte("#!/bin/sh\nexec \"$@\"\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}

	provenance := identityProvenance()
	client, err := NewClient(context.Background(), ClientConfig{
		Bin:     script,
		Args:    []string{"acp"},
		Wrapper: []string{wrapper},
		// A backend hook that reintroduces identity must not win: scrubbing
		// runs on the combined environment before this launch is added.
		Env: map[string]string{
			"OPENCODE_CONFIG":      configPath,
			runtime.EnvPhase:       "hook-phase",
			"AVENOR_PARENT_RUN_ID": "hook-parent",
			runtime.EnvRuntimeID:   "hook-runtime",
			runtime.EnvExecutionID: "hook-execution",
			runtime.EnvIteration:   "77",
			"AVENOR_NON_IDENTITY":  "kept",
		},
		Provenance: provenance,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	observed := readIdentityObservation(t, obsPath)
	assertIdentityObservation(t, observed, provenance)
	if observed["OPENCODE_CONFIG"] != configPath {
		t.Errorf("OPENCODE_CONFIG = %q, want the hook config preserved", observed["OPENCODE_CONFIG"])
	}
}

func TestProviderStartUsesProvenanceAtLaunch(t *testing.T) {
	poisonParentIdentity(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	stub := writeACPStub(t, acpStub{Mode: "serve", Observe: obsPath})
	p := newStubProvider(stub, nil)

	provenance := identityProvenance()
	if _, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: provenance,
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	observed := readIdentityObservation(t, obsPath)
	assertIdentityObservation(t, observed, provenance)
}

// TestProviderResumeWithOptionsCarriesFreshIdentity pins the option-aware
// resume dispatch: a new client gets the merged launch options and config,
// and the previous identity is replaced whole.
func TestProviderResumeWithOptionsCarriesFreshIdentity(t *testing.T) {
	poisonParentIdentity(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	stub := writeACPStub(t, acpStub{Mode: "serve", Observe: obsPath})
	var builtFor []runtime.StartOptions
	configPath := filepath.Join(t.TempDir(), "candidate.json")
	if err := os.WriteFile(configPath, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	p := newStubProvider(stub, func(opts runtime.StartOptions, _, _, _ string) (map[string]string, func() error) {
		builtFor = append(builtFor, opts)
		return candidateEnv(configPath), func() error { return os.Remove(configPath) }
	})
	p.opts = runtime.StartOptions{
		Provenance: &runtime.LaunchProvenance{
			RuntimeID:   "rt_old",
			ExecutionID: "rt_old",
			Kind:        "team",
			Phase:       "old-phase",
			Iteration:   intPtr(4),
			Label:       "old-label",
			Attempt:     1,
		},
	}
	defer p.Close()

	fresh := identityProvenance()
	if _, err := p.ResumeWithOptions(context.Background(), "ses_resume", runtime.StartOptions{
		Agent:      "reviewer",
		Provenance: fresh,
	}); err != nil {
		t.Fatalf("ResumeWithOptions: %v", err)
	}

	observed := readIdentityObservation(t, obsPath)
	assertIdentityObservation(t, observed, fresh)
	if observed[runtime.EnvIteration] != unsetValue {
		t.Errorf("AVENOR_ITERATION = %q, want the previous iteration cleared", observed[runtime.EnvIteration])
	}
	if len(builtFor) == 0 {
		t.Fatal("resume did not build client env from merged options")
	}
	last := builtFor[len(builtFor)-1]
	if last.Agent != "reviewer" || last.Provenance == nil || last.Provenance.RuntimeID != "rt_child" {
		t.Fatalf("resume built env for %+v, want the merged per-call options", last)
	}

	_ = p.Close()
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("resume launch config leaked after Close: %v", err)
	}
}

// TestProviderResumeWithOptionsKeepsLiveClientIdentity pins the other half: a
// live client keeps the identity it was launched with and no replacement
// process starts.
func TestProviderResumeWithOptionsKeepsLiveClientIdentity(t *testing.T) {
	poisonParentIdentity(t)
	readyDir := t.TempDir()
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	stub := writeACPStub(t, acpStub{Mode: "serve", Observe: obsPath, Ready: filepath.Join(readyDir, "pid."), ReadyPerProcess: true})
	p := newStubProvider(stub, nil)

	first := identityProvenance()
	if _, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: first,
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()
	if _, err := os.Stat(obsPath); err != nil {
		t.Fatalf("first observation missing: %v", err)
	}
	firstObserved := readIdentityObservation(t, obsPath)

	if _, err := p.ResumeWithOptions(context.Background(), "ses_live", runtime.StartOptions{
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_new", ExecutionID: "rt_new", Kind: "run", Attempt: 5},
	}); err != nil {
		t.Fatalf("ResumeWithOptions: %v", err)
	}

	entries, err := os.ReadDir(readyDir)
	if err != nil {
		t.Fatalf("read ready dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("resume started %d clients, want the single live client", len(entries))
	}
	if got := readIdentityObservation(t, obsPath); got[runtime.EnvRuntimeID] != firstObserved[runtime.EnvRuntimeID] ||
		got[runtime.EnvKind] != "loop" {
		t.Fatalf("live client identity changed: %v", got)
	}
}

func intPtr(v int) *int { return &v }
