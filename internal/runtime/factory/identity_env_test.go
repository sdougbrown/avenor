package factory

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

func writeAcpIdentityChild(t *testing.T, binName, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" + identitytest.Body(obsPath) + `
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  [ -n "$id" ] || continue
  printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"sessionId":"ses_identity"}}\n' "$id"
done
`
	if err := os.WriteFile(filepath.Join(binDir, binName), []byte(body), 0o755); err != nil {
		t.Fatalf("write %s child: %v", binName, err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// TestIdentityChildEnv drives every ACP-family constructor through the factory
// and asserts what the real child process saw.
func TestIdentityChildEnv(t *testing.T) {
	cases := []struct {
		backend string
		bin     string
	}{
		{backend: "opencode-acp", bin: "opencode"},
		{backend: "gemini-acp", bin: "gemini"},
		{backend: "cursor-acp", bin: "agent"},
	}
	for _, tc := range cases {
		t.Run(tc.backend, func(t *testing.T) {
			identitytest.PoisonParent(t)
			obsPath := filepath.Join(t.TempDir(), "observed.env")
			writeAcpIdentityChild(t, tc.bin, obsPath)

			provenance := identitytest.Provenance("rt_child")
			p, err := NewProvider(runtime.StartOptions{}, tc.backend)
			if err != nil {
				t.Fatalf("NewProvider: %v", err)
			}
			if _, err := p.Start(context.Background(), runtime.StartOptions{
				Dir:        t.TempDir(),
				Provenance: provenance,
			}); err != nil {
				t.Fatalf("Start: %v", err)
			}
			t.Cleanup(func() {
				if closer, ok := p.(interface{ Close() error }); ok {
					_ = closer.Close()
				}
			})

			observed := identitytest.Read(t, obsPath)
			identitytest.Assert(t, observed, provenance)
			identitytest.AssertBrokerPreserved(t, observed)
		})
	}
}
