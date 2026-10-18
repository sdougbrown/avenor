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

// piObservationExtras are the pi-specific variables the observer also records:
// the agent name.
var piObservationExtras = []string{"PI_AGENT"}

// writeIdentityPiChild installs a real `pi` child on PATH. The child records
// the allowlisted environment before answering get_state, so the observation
// is what the process itself saw.
func writeIdentityPiChild(t *testing.T, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	script := "#!/bin/sh\n" + identitytest.Body(obsPath, piObservationExtras...) + `
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

func writeTransparentWrapper(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wrapper.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexec \"$@\"\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}
	return path
}

func TestIdentityChildEnv(t *testing.T) {
	provenance := identitytest.Provenance("rt_child")

	t.Run("fresh start", func(t *testing.T) {
		identitytest.PoisonParent(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityPiChild(t, obsPath)

		p := NewWithOptions(runtime.StartOptions{})
		t.Cleanup(func() { _ = p.Close() })
		session, err := p.Start(context.Background(), runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: provenance,
			Agent:      "jockey",
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if session.SessionID != "pi_child" {
			t.Fatalf("session = %q, want pi_child", session.SessionID)
		}

		observed := identitytest.Read(t, obsPath)
		identitytest.Assert(t, observed, provenance)
		if observed["PI_AGENT"] != "jockey" {
			t.Errorf("PI_AGENT = %q, want jockey", observed["PI_AGENT"])
		}
	})

	t.Run("transparent wrapper and broker credentials", func(t *testing.T) {
		identitytest.PoisonParent(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityPiChild(t, obsPath)
		wrapper := writeTransparentWrapper(t)

		b := broker.New(broker.MakeToken())
		if err := b.Start(); err != nil {
			t.Fatalf("broker.Start: %v", err)
		}
		t.Cleanup(func() { _ = b.Stop() })
		brokerToken, _ := b.EnsureRun("rt_broker")
		brokerURL := "http://" + b.Addr()

		p := NewWithOptions(runtime.StartOptions{})
		t.Cleanup(func() { _ = p.Close() })
		_, err := p.Start(context.Background(), runtime.StartOptions{
			Dir:           t.TempDir(),
			LaunchWrapper: []string{wrapper},
			RuntimeID:     "rt_broker",
			BrokerToken:   brokerToken,
			Broker:        b,
			Provenance:    provenance,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}

		observed := identitytest.Read(t, obsPath)
		identitytest.Assert(t, observed, provenance)
		if observed["AVENOR_RUN_ID"] != "rt_broker" {
			t.Errorf("AVENOR_RUN_ID = %q, want rt_broker", observed["AVENOR_RUN_ID"])
		}
		identitytest.AssertBrokerURL(t, observed, brokerURL)
		if observed["AVENOR_BROKER_TOKEN"] != brokerToken {
			t.Errorf("AVENOR_BROKER_TOKEN = %q, want the fresh token", observed["AVENOR_BROKER_TOKEN"])
		}
	})

	t.Run("nil identity clears inherited values", func(t *testing.T) {
		identitytest.PoisonParent(t)
		t.Setenv("PI_AGENT", "stale-agent")
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		writeIdentityPiChild(t, obsPath)

		p := NewWithOptions(runtime.StartOptions{})
		t.Cleanup(func() { _ = p.Close() })
		if _, err := p.Start(context.Background(), runtime.StartOptions{Dir: t.TempDir()}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		observed := identitytest.Read(t, obsPath)
		identitytest.AssertCleared(t, observed)
		if observed["PI_AGENT"] != identitytest.UnsetValue {
			t.Errorf("PI_AGENT = %q, want stale agent removed", observed["PI_AGENT"])
		}
	})
}

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

	// An identity-only resume must not send a thinking RPC even though the
	// constructor asked for one.
	select {
	case cmd := <-commands:
		t.Fatalf("identity-only resume wrote %v, want no RPC", cmd)
	case <-time.After(300 * time.Millisecond):
	}
}
