//go:build linux || darwin || freebsd

package terminal

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
	"github.com/sdougbrown/avenor/internal/token"
)

// TestPTYLauncherAddsLaunchIdentity asserts in one run what the hosted PTY
// child saw: this launch's identity and none of the inherited identity.
//
// The inherited avenor peer credentials go too. A session that loaded the
// plugin exports them into everything it launches, so an inherited pair would
// make this child join the broker as the launcher's run — a record another live
// session is already polling.
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
	identitytest.AssertPeerEnv(t, observed, nil)
}

// TestPTYLauncherExportsProvisionedPeerCredentials is the other half of the
// same contract: what the launcher provisions is what the child sees, even
// over an inherited pair of the same names.
func TestPTYLauncherExportsProvisionedPeerCredentials(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	command := identitytest.Body(obsPath) + "sleep 30"

	provisioned := map[string]string{
		"AVENOR_BROKER_URL":   "http://127.0.0.1:1",
		"AVENOR_RUN_ID":       "rt_child",
		"AVENOR_BROKER_TOKEN": "child-token",
	}
	env := make([]string, 0, len(provisioned))
	for key, value := range provisioned {
		env = append(env, key+"="+value)
	}
	// A caller entry is appended after the scrub layers, so it must not be able
	// to reintroduce what they remove or overwrite this launch's identity. The
	// parent-poisoned values are the ones the child must not see either way.
	env = append(env,
		"CLAUDE_CODE_MESSAGING_TOKEN=caller-secret",
		token.EnvBrokerParentToken+"=caller-parent-secret",
		runtime.EnvLabel+"=spoofed-label",
		"AVENOR_SESSION_NAME=spoofed-session",
	)

	provenance := identitytest.Provenance("rt_child")
	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{
		Name:       "identity-pty-provisioned",
		Command:    command,
		Provenance: provenance,
		Env:        env,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer session.Kill(context.Background())

	observed := identitytest.Read(t, obsPath)
	identitytest.Assert(t, observed, provenance)
	identitytest.AssertPeerEnv(t, observed, provisioned)
}
