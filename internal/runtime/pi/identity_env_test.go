package pi

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

// writeIdentityPiChild installs a real `pi` child on PATH. The child records
// the allowlisted environment before answering, so the observation is what the
// process itself saw.
func writeIdentityPiChild(t *testing.T, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	script := "#!/bin/sh\n" + identitytest.Body(obsPath) + `
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  [ -n "$id" ] || continue
  printf '{"type":"response","id":"%s","sessionId":"pi_child"}\n' "$id"
done
`
	if err := os.WriteFile(filepath.Join(binDir, "pi"), []byte(script), 0o755); err != nil {
		t.Fatalf("write pi child: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// TestIdentityChildEnv asserts in one launch what the pi child saw: this
// launch's identity, none of the inherited identity, and the broker pair the
// launch supplied rather than the parent's stale pair.
func TestIdentityChildEnv(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	writeIdentityPiChild(t, obsPath)

	b := broker.New(broker.MakeToken())
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	token, ok, err := b.EnsureRun("rt_child", "")
	if err != nil || !ok {
		t.Fatalf("EnsureRun = (%q, %v, %v), want a minted run", token, ok, err)
	}

	provenance := identitytest.Provenance("rt_child")
	p := NewWithOptions(runtime.StartOptions{})
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:         t.TempDir(),
		RuntimeID:   "rt_child",
		Broker:      b,
		BrokerToken: token,
		Provenance:  provenance,
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	observed := identitytest.Read(t, obsPath)
	identitytest.Assert(t, observed, provenance)
	for key, want := range map[string]string{
		"AVENOR_RUN_ID":       "rt_child",
		"AVENOR_BROKER_TOKEN": token,
		"AVENOR_BROKER_URL":   "http://" + b.Addr(),
	} {
		if observed[key] != want {
			t.Errorf("%s = %q, want %q", key, observed[key], want)
		}
	}
}

// TestResumeWithOptionsIgnoresConstructorThinking pins the per-call thinking
// rule: constructor thinking never reaches a resume, so an identity-only resume
// sends no thinking RPC.
func TestResumeWithOptionsIgnoresConstructorThinking(t *testing.T) {
	c, clientOut, clientIn := fakeClient()
	defer c.Close()

	// Answer every command so a wrongly applied thinking RPC cannot block the
	// resume; the transcript is the assertion.
	commands := make(chan map[string]any, 4)
	go func() {
		for {
			cmd, err := readCommand(clientIn)
			if err != nil {
				return
			}
			commands <- cmd
			writeLine(clientOut, map[string]any{
				"type":    "response",
				"id":      cmd["id"],
				"success": true,
			})
		}
	}()

	p := NewWithOptions(runtime.StartOptions{Thinking: "high"})
	p.client = c
	p.sessions["pi_live"] = struct{}{}
	c.setSessionID("pi_live")

	if _, err := p.ResumeWithOptions(context.Background(), "pi_live", runtime.StartOptions{
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_new", Attempt: 2},
	}); err != nil {
		t.Fatalf("ResumeWithOptions: %v", err)
	}

	select {
	case cmd := <-commands:
		t.Fatalf("identity-only resume wrote %v, want no RPC", cmd)
	case <-time.After(300 * time.Millisecond):
	}
}
