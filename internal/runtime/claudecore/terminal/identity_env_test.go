//go:build linux || darwin || freebsd

package terminal

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

// TestPTYLauncherAddsLaunchIdentity asserts in one run what the hosted PTY
// child saw: this launch's identity, none of the inherited identity, and the
// untouched broker pair.
func TestPTYLauncherAddsLaunchIdentity(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	command := identitytest.Body(obsPath) + "sleep 30"

	provenance := identitytest.Provenance("rt_child")
	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{
		Name:       "identity-pty",
		Command:    command,
		Provenance: provenance,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer session.Kill(context.Background())

	observed := identitytest.Read(t, obsPath)
	identitytest.Assert(t, observed, provenance)
	identitytest.AssertBrokerPreserved(t, observed)
}
