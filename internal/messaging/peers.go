package messaging

import (
	"encoding/json"

	"github.com/sdougbrown/avenor/internal/runstate"
)

// DecodePeerRecords is the one peer decoder every surface uses to turn a
// broker's peer listing into the contract's peer records. It owns the decode
// loop, the empty-session guard and the run-id fallback, so a field added to
// the peer contract reaches every provider's list, resolve and run-state paths
// instead of stopping at a per-package copy.
//
// The peer's own id is the identity the listing publishes. A session record's
// run_id is the record's content, so it cannot rename the peer.
func DecodePeerRecords(peers []Peer) ([]PeerRecord, error) {
	records := make([]PeerRecord, 0, len(peers))
	for _, peer := range peers {
		var record PeerRecord
		if len(peer.Session) > 0 {
			if err := json.Unmarshal(peer.Session, &record); err != nil {
				return nil, err
			}
		}
		if peer.RunID != "" {
			record.RunID = peer.RunID
		}
		records = append(records, record)
	}
	return records, nil
}

// PeerListing decodes a peer listing a transport read. A transport that could
// not read every peer returns the peers it did read together with an error:
// those peers are decoded and kept, and the error travels with them, so a
// caller answers for a peer it can see and refuses to answer for a peer it
// cannot. A listing that could not be read at all returns no peers and the
// error alone.
func PeerListing(peers []Peer, err error) ([]PeerRecord, error) {
	records, decodeErr := DecodePeerRecords(peers)
	if decodeErr != nil {
		return nil, decodeErr
	}
	if err != nil && len(peers) == 0 {
		return nil, err
	}
	return records, err
}

// LabelCandidates collects the run ids in records whose broker label equals
// ref, in the records' order (the broker listing order), together with the
// matched label. Both resolution planes switch on the candidate count with
// this one collection, so duplicate-label handling cannot land twice.
func LabelCandidates(records []PeerRecord, ref string) (candidates []string, label string) {
	for _, record := range records {
		if record.Label != ref {
			continue
		}
		candidates = append(candidates, record.RunID)
		label = record.Label
	}
	return candidates, label
}

// PeerRunFacts is the one peer-row-to-facts translation, so every surface
// classifies a broker peer row the same way. A peer row carries no phase stream
// and no sentinel, so the phase is PhaseNone, liveness is the inverse of turn
// completion, and the run is never resumable: the supervisor that spawned it
// owns a resume.
func PeerRunFacts(record PeerRecord) RunFacts {
	translation := runstate.Translate(runstate.RuntimeStatus(record.Status), runstate.PhaseNone)
	return RunFacts{
		RunID:     record.RunID,
		Status:    translation.Status,
		Phase:     runstate.PhaseNone,
		Live:      !translation.TurnComplete,
		Resumable: false,
	}
}
