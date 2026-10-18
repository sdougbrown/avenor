package agy

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

// writeIdentityAgy installs a real `agy` child on PATH that records its
// environment and then emits a minimal init/result pair for the conversation
// it was asked to use.
func writeIdentityAgy(t *testing.T, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" + identitytest.Body(obsPath) + `
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

func identityProvenance(runtimeID string) *runtime.LaunchProvenance {
	provenance := identitytest.Provenance(runtimeID)
	provenance.Kind = "loop"
	provenance.Phase = "body 'one' $(id) \nsecond"
	return provenance
}

func TestIdentityChildEnv(t *testing.T) {
	t.Run("initial print", func(t *testing.T) {
		identitytest.PoisonParent(t)
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

		groups := identitytest.ReadGroups(t, obsPath, 1)
		if len(groups) != 1 {
			t.Fatalf("launches = %d, want 1", len(groups))
		}
		identitytest.Assert(t, groups[0], provenance)
		identitytest.AssertBrokerPreserved(t, groups[0])
	})

	t.Run("nil identity clears inherited values", func(t *testing.T) {
		identitytest.PoisonParent(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityAgy(t, obsPath)

		p := testProvider(runtime.StartOptions{Dir: t.TempDir()})
		session, err := p.Start(context.Background(), runtime.StartOptions{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := p.Prompt(context.Background(), session.SessionID, "hi"); err != nil {
			t.Fatalf("Prompt: %v", err)
		}

		observed := identitytest.Read(t, obsPath)
		identitytest.AssertCleared(t, observed)
		identitytest.AssertBrokerPreserved(t, observed)
	})

	t.Run("resumed print continuation keeps the launch identity", func(t *testing.T) {
		identitytest.PoisonParent(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityAgy(t, obsPath)

		want := identityProvenance("rt_resumed")
		want.Iteration = identitytest.IntPtr(3)
		caller := identityProvenance("rt_resumed")
		caller.Iteration = identitytest.IntPtr(3)
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
		// A later caller mutation must not reach the snapshotted session,
		// including through the iteration pointer.
		caller.RuntimeID = "rt_mutated"
		caller.Attempt = 9
		*caller.Iteration = 9

		for i := 0; i < 2; i++ {
			if err := p.Prompt(context.Background(), session.SessionID, "hi"); err != nil {
				t.Fatalf("Prompt %d: %v", i, err)
			}
		}

		groups := identitytest.ReadGroups(t, obsPath, 2)
		if len(groups) != 2 {
			t.Fatalf("launches = %d, want 2", len(groups))
		}
		for _, group := range groups {
			identitytest.Assert(t, group, want)
			identitytest.AssertBrokerPreserved(t, group)
		}
	})

	t.Run("nil identity on plain resume clears inherited values", func(t *testing.T) {
		identitytest.PoisonParent(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityAgy(t, obsPath)

		p := testProvider(runtime.StartOptions{Dir: t.TempDir()})
		session, err := p.Resume(context.Background(), "conv_plain")
		if err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if err := p.Prompt(context.Background(), session.SessionID, "hi"); err != nil {
			t.Fatalf("Prompt: %v", err)
		}

		observed := identitytest.Read(t, obsPath)
		identitytest.AssertCleared(t, observed)
		identitytest.AssertBrokerPreserved(t, observed)
	})

	t.Run("rpc pty host", func(t *testing.T) {
		identitytest.PoisonParent(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityAgy(t, obsPath)

		provenance := identityProvenance("rt_rpc")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		host, err := startPTYRPCHost(ctx, terminal.PTYLauncher{}, runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: provenance,
		}, "", "1.1.5", rpcDiscoveryOptions{})

		// The child records its environment before RPC discovery, so the
		// identity assertion holds whatever discovery does.
		observed := identitytest.Read(t, obsPath)
		identitytest.Assert(t, observed, provenance)
		identitytest.AssertBrokerPreserved(t, observed)

		if err == nil {
			_ = host.Close(ctx)
			t.Fatal("startPTYRPCHost succeeded against a child with no RPC endpoint")
		}
	})
}
