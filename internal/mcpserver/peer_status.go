package mcpserver

import (
	"context"
	"errors"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// brokerPeer finds key in the broker's peer listing, the one `peers list`
// reads: an exact run id, else an exact label. A run a plugin session
// registered, or a run seen through a parent, lives only there and has no
// runtime on the supervisor. The resolution says what the listing named: a
// unique match carries the peer, an ambiguous one carries the candidate run
// ids. A listing that cannot be read answers no match, so the caller keeps its
// own failure.
func (s *Server) brokerPeer(ctx context.Context, cl ControlClient, supervisorPath, key string) (messaging.PeerRecord, messaging.Resolution) {
	none := messaging.Resolution{Match: messaging.MatchNone}
	provider, err := newMCPMessageProvider(ctx, s, cl, supervisorPath)
	if err != nil {
		return messaging.PeerRecord{}, none
	}
	peers, err := provider.listedPeers(ctx)
	if err != nil {
		return messaging.PeerRecord{}, none
	}
	res := matchListedPeer(peers, key)
	switch res.Match {
	case messaging.MatchAmbiguous:
		return messaging.PeerRecord{}, res
	case messaging.MatchUnique:
		for _, peer := range peers {
			if peer.RunID == res.RunID {
				return peer, res
			}
		}
	}
	return messaging.PeerRecord{}, none
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
