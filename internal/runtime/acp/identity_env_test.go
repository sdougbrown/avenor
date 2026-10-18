package acp

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

// acpObservationExtras is the backend hook variable the observer also records:
// the scrub must not touch it.
var acpObservationExtras = []string{"OPENCODE_CONFIG"}

func identityProvenance() *runtime.LaunchProvenance {
	provenance := identitytest.Provenance("rt_child")
	provenance.Kind = "loop"
	provenance.Phase = "body $(id) 'quoted'"
	provenance.Attempt = 3
	return provenance
}

func TestIdentityChildEnv(t *testing.T) {
	t.Run("hook env cannot reintroduce identity", func(t *testing.T) {
		identitytest.PoisonParent(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		configPath := filepath.Join(t.TempDir(), "candidate.json")
		if err := os.WriteFile(configPath, []byte(`{}`), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}

		script := filepath.Join(t.TempDir(), "acp-child.sh")
		body := "#!/bin/sh\n" + identitytest.Body(obsPath, acpObservationExtras...) + "cat > /dev/null\n"
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

		observed := identitytest.Read(t, obsPath)
		identitytest.Assert(t, observed, provenance)
		identitytest.AssertBrokerPreserved(t, observed)
		if observed["OPENCODE_CONFIG"] != configPath {
			t.Errorf("OPENCODE_CONFIG = %q, want the hook config preserved", observed["OPENCODE_CONFIG"])
		}
	})

	t.Run("nil identity clears inherited values", func(t *testing.T) {
		identitytest.PoisonParent(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")

		script := filepath.Join(t.TempDir(), "acp-child.sh")
		body := "#!/bin/sh\n" + identitytest.Body(obsPath, acpObservationExtras...) + "cat > /dev/null\n"
		if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
			t.Fatalf("write child: %v", err)
		}

		client, err := NewClient(context.Background(), ClientConfig{
			Bin:  script,
			Args: []string{"acp"},
		})
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		defer client.Close()

		observed := identitytest.Read(t, obsPath)
		identitytest.AssertCleared(t, observed)
		identitytest.AssertBrokerPreserved(t, observed)
	})
}

func TestProviderStartUsesProvenanceAtLaunch(t *testing.T) {
	identitytest.PoisonParent(t)
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

	observed := identitytest.Read(t, obsPath)
	identitytest.Assert(t, observed, provenance)
	identitytest.AssertBrokerPreserved(t, observed)
}

// TestProviderResumeClearsInheritedIdentity pins plain resume, which keeps the
// constructor defaults: with no constructor identity, inherited values still
// do not reach the new client.
func TestProviderResumeClearsInheritedIdentity(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	stub := writeACPStub(t, acpStub{Mode: "serve", Observe: obsPath})
	p := newStubProvider(stub, nil)
	defer p.Close()

	if _, err := p.Resume(context.Background(), "ses_plain"); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	observed := identitytest.Read(t, obsPath)
	identitytest.AssertCleared(t, observed)
	identitytest.AssertBrokerPreserved(t, observed)
}

// TestProviderResumeWithOptionsCarriesFreshIdentity pins the option-aware
// resume dispatch: a new client gets the merged launch options and config,
// and the previous identity is replaced whole.
func TestProviderResumeWithOptionsCarriesFreshIdentity(t *testing.T) {
	identitytest.PoisonParent(t)
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

	identitytest.Assert(t, identitytest.Read(t, obsPath), fresh)
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
// live client keeps the identity it was launched with, no replacement process
// starts, and no launch environment is built for the reuse.
func TestProviderResumeWithOptionsKeepsLiveClientIdentity(t *testing.T) {
	identitytest.PoisonParent(t)
	readyDir := t.TempDir()
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	stub := writeACPStub(t, acpStub{Mode: "serve", Observe: obsPath, Ready: filepath.Join(readyDir, "pid."), ReadyPerProcess: true})
	var builtFor int
	p := newStubProvider(stub, func(runtime.StartOptions, string, string, string) (map[string]string, func() error) {
		builtFor++
		return nil, nil
	})

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
	firstObserved := identitytest.Read(t, obsPath)
	builtForStart := builtFor

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
	if builtFor != builtForStart {
		t.Fatalf("live resume built %d launch environments, want none", builtFor-builtForStart)
	}
	if got := identitytest.Read(t, obsPath); got[runtime.EnvRuntimeID] != firstObserved[runtime.EnvRuntimeID] ||
		got[runtime.EnvKind] != "loop" {
		t.Fatalf("live client identity changed: %v", got)
	}
}

func intPtr(v int) *int { return &v }
