package broker

import (
	"context"
	"net/http"
	"time"
)

const peerLease = 60 * time.Second

// renewPeerLeaseLocked extends a registered peer's lease from now. The caller
// holds Mu. It is the one place the lease length is applied, so a heartbeat and
// a poll cannot renew by different rules.
func (st *RunState) renewPeerLeaseLocked(now time.Time) {
	if st.peer {
		st.peerDeadline = now.Add(peerLease)
	}
}

// peerUnavailableLocked reads the lease under Mu. Peer tombstones retain the
// registration credential so a delayed registration cannot revive a dead inbox.
func (st *RunState) peerUnavailableLocked(now time.Time) string {
	if !st.peer {
		return ""
	}
	if st.peerRetired {
		return "peer-retired"
	}
	if !now.Before(st.peerDeadline) {
		return "peer-expired"
	}
	return ""
}

func (b *Broker) handleRetirePeer(w http.ResponseWriter, r *http.Request) {
	st, _, ok := b.runStateFor(authenticatedCaller(r))
	if !ok {
		writeCodedError(w, http.StatusNotFound, "run not found", "target-unavailable")
		return
	}
	st.peerActivity.Lock()
	defer st.peerActivity.Unlock()
	st.Mu.Lock()
	if !st.peer {
		st.Mu.Unlock()
		writeCodedError(w, http.StatusForbidden, "only self-registered peers may retire", "not-permitted")
		return
	}
	st.peerRetired = true
	st.ControlQueue = nil
	st.signalLocked()
	st.Mu.Unlock()
	b.recordRemoved(st.RunID, false)
	writeJSONBody(w, http.StatusOK, map[string]any{"retired": true})
}

// runPeerLeaseLoop retires absent consumers independently of peer listings, so
// pending ask waiters are released even while nobody is discovering peers.
func (b *Broker) runPeerLeaseLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			b.expirePeers(now)
		}
	}
}

// expirePeers tombstones every peer whose lease lapsed. Each peer is handled
// under its own activity lock, taken without blocking: a peer that holds the
// lock is serving a request, so it is alive and a later tick retires it.
// Blocking here would let one wedged request delay tombstoning for every peer,
// keeping the rest routable long after their leases lapsed.
func (b *Broker) expirePeers(now time.Time) {
	b.mu.RLock()
	states := make([]*RunState, 0, len(b.runs))
	for _, st := range b.runs {
		if st.peer {
			states = append(states, st)
		}
	}
	b.mu.RUnlock()
	for _, st := range states {
		if !st.peerActivity.TryLock() {
			continue
		}
		st.Mu.Lock()
		expired := !st.peerRetired && !now.Before(st.peerDeadline)
		if expired {
			st.peerRetired = true
			st.ControlQueue = nil
			st.signalLocked()
		}
		st.Mu.Unlock()
		if expired {
			b.recordRemoved(st.RunID, false)
		}
		st.peerActivity.Unlock()
	}
}
