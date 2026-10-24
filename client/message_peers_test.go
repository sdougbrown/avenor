package client_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/control/controltest"
	"github.com/sdougbrown/avenor/internal/messaging"
)

// partialPeersTransport serves a peer listing that could not be completed: the
// peers it did read, with the reason the listing was incomplete.
type partialPeersTransport struct {
	messaging.Transport
}

func (t partialPeersTransport) Peers(context.Context, messaging.PeersRequest) (messaging.PeersResult, error) {
	return messaging.PeersResult{
		Peers:      []messaging.Peer{{RunID: "rt_2", Session: []byte(`{"run_id":"rt_2"}`)}},
		Generation: "g",
	}, messaging.ErrIncompleteListing
}

// TestMessagePeersMapsTheIncompleteListing pins that the control result's
// peers_error becomes the incomplete-listing class on the client side, with
// the peers that were read kept beside it, so a caller can answer for a peer
// it can see and refuse for one it cannot.
func TestMessagePeersMapsTheIncompleteListing(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "ctrl.sock")
	srv, _ := controltest.NewServer(control.NewState("run_test", "", 0))
	controltest.SetMessageTransport(srv, partialPeersTransport{})
	if err := srv.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer srv.Stop()

	c, err := client.Dial(socketPath)
	if err != nil {
		t.Fatalf("dial control socket: %v", err)
	}
	defer c.Close()

	result, err := c.MessagePeersContext(context.Background(), messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: "supervisor"},
		Generation: "g",
	})
	if !errors.Is(err, messaging.ErrIncompleteListing) {
		t.Fatalf("peers error = %v, want the incomplete listing reported", err)
	}
	if len(result.Peers) != 1 || result.Peers[0].RunID != "rt_2" {
		t.Fatalf("peers = %+v, want the peer the listing did read", result.Peers)
	}
}
