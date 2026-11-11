package mcpserver

import (
	"context"
	"errors"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// brokerPeer finds key in the broker's peer listing, the one `peers list`
// reads: an exact run id, else an exact label that names a single peer. A run a
// plugin session registered, or a run seen through a parent, lives only there
// and has no runtime on the supervisor. A listing that cannot be read, or a
// label two peers share, answers no peer, so the caller keeps its own failure.
func (s *Server) brokerPeer(ctx context.Context, cl ControlClient, supervisorPath, key string) (messaging.PeerRecord, bool) {
	provider, err := newMCPMessageProvider(ctx, s, cl, supervisorPath)
	if err != nil {
		return messaging.PeerRecord{}, false
	}
	peers, err := provider.listedPeers(ctx)
	if err != nil {
		return messaging.PeerRecord{}, false
	}
	for _, peer := range peers {
		if peer.RunID == key {
			return peer, true
		}
	}
	if candidates, _ := messaging.LabelCandidates(peers, key); len(candidates) == 1 {
		for _, peer := range peers {
			if peer.RunID == candidates[0] {
				return peer, true
			}
		}
	}
	return messaging.PeerRecord{}, false
}

// brokerPeerStatus is the status a broker peer can answer: its identity and the
// status `peers list` shows for it, translated the way every peer row is. It has
// no runtime, phase or event log to report.
func brokerPeerStatus(peer messaging.PeerRecord) map[string]any {
	status := map[string]any{"run_id": peer.RunID}
	for key, value := range map[string]string{
		"label":   peer.Label,
		"status":  string(messaging.PeerRunFacts(peer).Status),
		"backend": peer.Backend,
		"model":   peer.Model,
		"dir":     peer.Dir,
	} {
		if value != "" {
			status[key] = value
		}
	}
	return status
}

var (
	errBrokerPeerNoWait   = errors.New("this run is a broker peer with no lifecycle to wait on; status reads its broker status without wait_for")
	errBrokerPeerNoEvents = errors.New("this run is a broker peer with no event log; peers action last reads its last agent message")
)
