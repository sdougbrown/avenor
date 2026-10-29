package broker

import (
	"context"
	"net/http"
	"time"
)

const peerLease = 60 * time.Second

// peerTombstoneTTL is how long a retired peer's record is kept after its identity
// stops answering. The lease alone fences a delayed registration within a minute;
// the rest of the window is what keeps the run's last turn readable after the
// session is gone. A long-lived broker would otherwise hold one record — its
// queue, its permission maps, its final output — per session, forever.
const peerTombstoneTTL = 30 * time.Minute

// retirePeerLocked tombstones a peer under Mu: it stops answering, and gives up
// what only its own consumer could have used. The caller records the removal.
func (st *RunState) retirePeerLocked(now time.Time) {
	st.peerRetired = true
	st.peerRetiredAt = now
	st.ControlQueue = nil
	st.signalLocked()
}

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
	st.retirePeerLocked(time.Now())
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
			b.reapPeers(now)
		}
	}
}

// expirePeers tombstones every peer whose lease lapsed. Each peer is handled
// under its own activity lock, taken without blocking: a peer that holds the
// lock is serving a request, so it is alive and a later tick retires it.
// Blocking here would let one wedged request delay tombstoning for every peer,
// keeping the rest routable long after their leases lapsed.
//
// The wait a skipped peer defers is its own release of pending asks, not its
// reachability: peerUnavailableLocked already refuses its sends on the deadline,
// so a request that never finishes leaves that peer's tombstone late, never its
// inbox open.
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
			st.retirePeerLocked(now)
		}
		st.Mu.Unlock()
		if expired {
			b.recordRemoved(st.RunID, false)
		}
		st.peerActivity.Unlock()
	}
}

// reapPeers forgets a peer whose tombstone has stood past the retention window. A
// retired identity refuses every request, so nothing is left to fence: what goes
// is the record — its last turn stops answering, and its run id becomes
// registrable again. That is the trade the window buys, and it is why the window
// is far longer than any registration can plausibly be delayed.
//
// The activity lock is taken without blocking, as expirePeers takes it: a record
// a request still holds is one tick away from being reaped, never mid-write.
func (b *Broker) reapPeers(now time.Time) {
	b.mu.RLock()
	stale := make([]*RunState, 0, 1)
	for _, st := range b.runs {
		if !st.peer {
			continue
		}
		st.Mu.Lock()
		due := st.peerRetired && !now.Before(st.peerRetiredAt.Add(peerTombstoneTTL))
		st.Mu.Unlock()
		if due {
			stale = append(stale, st)
		}
	}
	b.mu.RUnlock()
	for _, st := range stale {
		if !st.peerActivity.TryLock() {
			continue
		}
		b.DeleteRun(st.RunID)
		st.peerActivity.Unlock()
	}
}
