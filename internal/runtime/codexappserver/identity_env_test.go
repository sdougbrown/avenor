package codexappserver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

func writeTransparentWrapper(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wrapper.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexec \"$@\"\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}
	return path
}

// writeCodexIdentityChild installs a real `codex` child on PATH. It speaks the
// minimal app-server handshake and records the allowlisted environment first.
func writeCodexIdentityChild(t *testing.T, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" + identitytest.Body(obsPath) + `
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

func TestIdentityChildEnv(t *testing.T) {
	t.Run("start client through a transparent wrapper", func(t *testing.T) {
		identitytest.PoisonParent(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeCodexIdentityChild(t, obsPath)
		wrapper := writeTransparentWrapper(t)

		provenance := identitytest.Provenance("rt_child")
		c, err := StartClient(context.Background(), runtime.StartOptions{
			LaunchWrapper: []string{wrapper},
			Provenance:    provenance,
		})
		if err != nil {
			t.Fatalf("StartClient: %v", err)
		}
		defer c.Close()

		observed := identitytest.Read(t, obsPath)
		identitytest.Assert(t, observed, provenance)
		identitytest.AssertBrokerPreserved(t, observed)
	})

	t.Run("nil identity clears inherited values", func(t *testing.T) {
		identitytest.PoisonParent(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeCodexIdentityChild(t, obsPath)

		c, err := StartClient(context.Background(), runtime.StartOptions{})
		if err != nil {
			t.Fatalf("StartClient: %v", err)
		}
		defer c.Close()

		observed := identitytest.Read(t, obsPath)
		identitytest.AssertCleared(t, observed)
		identitytest.AssertBrokerPreserved(t, observed)
	})
}

func TestProviderStartUsesProvenanceAtLaunch(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	writeCodexIdentityChild(t, obsPath)

	provenance := identitytest.Provenance("rt_child")
	p := NewWithOptions(runtime.StartOptions{})
	defer p.Close()
	if _, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: provenance,
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	identitytest.Assert(t, identitytest.Read(t, obsPath), provenance)
}

// TestProviderResumeWithOptionsCarriesFreshIdentity pins the fresh-client
// resume path: the new app-server is launched with the caller's merged
// identity, not the constructor default. The constructor identity carries an
// iteration and a label the fresh launch must not keep.
func TestProviderResumeWithOptionsCarriesFreshIdentity(t *testing.T) {
	identitytest.PoisonParent(t)
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

	fresh := identitytest.Provenance("rt_child")
	if _, err := p.ResumeWithOptions(context.Background(), "th_unknown", runtime.StartOptions{
		Provenance: fresh,
	}); err != nil {
		t.Fatalf("ResumeWithOptions: %v", err)
	}

	identitytest.Assert(t, identitytest.Read(t, obsPath), fresh)
}

// TestProviderResumeWithOptionsKeepsLiveProcessIdentity pins the live-process
// half: an existing app-server keeps the identity it was launched with.
func TestProviderResumeWithOptionsKeepsLiveProcessIdentity(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	writeCodexIdentityChild(t, obsPath)

	first := identitytest.Provenance("rt_child")
	p := NewWithOptions(runtime.StartOptions{})
	defer p.Close()
	session, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: first,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	firstObserved := identitytest.Read(t, obsPath)

	if _, err := p.ResumeWithOptions(context.Background(), session.SessionID, runtime.StartOptions{
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_new", ExecutionID: "rt_new", Kind: "run", Attempt: 5},
	}); err != nil {
		t.Fatalf("ResumeWithOptions: %v", err)
	}

	observed := identitytest.Read(t, obsPath)
	if observed[runtime.EnvRuntimeID] != firstObserved[runtime.EnvRuntimeID] || observed[runtime.EnvKind] != "team" {
		t.Fatalf("live process identity changed: %v", observed)
	}
	if p.client == nil {
		t.Fatal("resume dropped the live client")
	}
}

func intPtr(v int) *int { return &v }
