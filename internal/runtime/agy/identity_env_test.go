package agy

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
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

// TestIdentityChildEnv asserts in one print launch what the agy child saw: this
// launch's identity, none of the inherited identity, and no inherited peer
// credential — this backend provisions none, so the shared scrub carries the
// guarantee.
func TestIdentityChildEnv(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	writeIdentityAgy(t, obsPath)

	provenance := identitytest.Provenance("rt_child")
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

	observed := identitytest.Read(t, obsPath)
	identitytest.Assert(t, observed, provenance)
	// An inherited peer credential names the launcher's run, which another live
	// session already holds, so the shared scrub removes it: this launch joins
	// as nothing at all.
	identitytest.AssertPeerEnv(t, observed, nil)
}
