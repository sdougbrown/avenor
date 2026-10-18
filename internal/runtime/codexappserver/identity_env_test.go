package codexappserver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

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

// TestIdentityChildEnv asserts in one launch what the app-server child saw:
// this launch's identity through the opaque wrapper prefix, none of the
// inherited identity, and the untouched broker pair.
func TestIdentityChildEnv(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	writeCodexIdentityChild(t, obsPath)

	provenance := identitytest.Provenance("rt_child")
	c, err := StartClient(context.Background(), runtime.StartOptions{
		LaunchWrapper: []string{identitytest.WriteTransparentWrapper(t)},
		Provenance:    provenance,
	})
	if err != nil {
		t.Fatalf("StartClient: %v", err)
	}
	defer c.Close()

	observed := identitytest.Read(t, obsPath)
	identitytest.Assert(t, observed, provenance)
	identitytest.AssertBrokerPreserved(t, observed)
}
