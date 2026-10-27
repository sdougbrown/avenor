package acp

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

// TestIdentityChildEnv asserts in one launch what the ACP child saw: this
// launch's identity, none of the inherited identity, and no inherited peer
// credential either — this backend provisions none, so the shared scrub is the
// only thing that keeps the launcher's run out of the child. An env hook that
// reintroduces identity must not win, because the scrub runs on the combined
// environment.
func TestIdentityChildEnv(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")

	script := filepath.Join(t.TempDir(), "acp-child.sh")
	body := "#!/bin/sh\n" + identitytest.Body(obsPath) + "cat > /dev/null\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write child: %v", err)
	}

	provenance := identitytest.Provenance("rt_child")
	client, err := NewClient(context.Background(), ClientConfig{
		Bin:     script,
		Args:    []string{"acp"},
		Wrapper: []string{identitytest.WriteTransparentWrapper(t)},
		Env: map[string]string{
			runtime.EnvPhase:       "hook-phase",
			"AVENOR_PARENT_RUN_ID": "hook-parent",
			runtime.EnvIteration:   "77",
		},
		Provenance: provenance,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	observed := identitytest.Read(t, obsPath)
	identitytest.Assert(t, observed, provenance)
	// An inherited peer credential names the launcher's run, which another live
	// session already holds, so the shared scrub removes it: this launch joins
	// as nothing at all.
	identitytest.AssertPeerEnv(t, observed, nil)
}
