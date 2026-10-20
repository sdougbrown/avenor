package askstore

import (
	"context"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// Prune terminates expired edges and reclaims terminal fences whose acceptance
// deadline has passed. Fence retention is keyed to the ID's encoded deadline,
// never to an edge's creation time. Reclaiming before terminating keeps the
// record written for an edge removed by this tick until the next one.
func (s *Store) Prune(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reclaimLocked(now)
	for _, e := range s.active {
		if now.Before(e.deadline) {
			continue
		}
		if e.completed {
			// A replied edge whose waiter never consumed it still fences its ID
			// so the ID cannot be admitted again.
			s.consumeLocked(e)
			continue
		}
		s.completeLocked(e, messaging.OutcomeTimedOut)
	}
}

// RunPruneLoop calls Prune on each tick until ctx is done.
func (s *Store) RunPruneLoop(ctx context.Context) {
	ticker := time.NewTicker(s.pruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Prune(s.now())
		}
	}
}
