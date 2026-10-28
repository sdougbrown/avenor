package stable

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// A retired peer is not a target: resolving it by id and resolving it by name
// answer the same way, so a rename cannot report success on a run nobody can
// reach and a resume cannot be admitted by one spelling and refused by the other.
func TestResolveRunReferenceRefusesATombstoneEitherWay(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 0)
	credentials := map[string]any{"run_id": "pi-peer", "token": "secret", "peer": true}
	for _, path := range []string{"/register", "/retire"} {
		body, err := json.Marshal(credentials)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.Post(sup.broker.URL()+path, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s = %d", path, resp.StatusCode)
		}
	}

	for _, ref := range []string{"pi-peer", "Peer By Name"} {
		if ref == "Peer By Name" {
			// Name resolution only differs from id resolution when a name exists.
			sup.broker.SetHarnessName("pi-peer", ref)
		}
		got, err := sup.resolveRunReference(ref)
		var refusal *router.ToolError
		if !errors.As(err, &refusal) || refusal.Code != "unknown_target" {
			t.Fatalf("resolve %q = %q, %v; want unknown_target", ref, got, err)
		}
	}
}
