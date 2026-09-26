package stable

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/workflow"
)

// workflow_heartbeat.go holds the executors' live lease heartbeat: one
// goroutine per workflow attempt that renews the attempt's workflow-node
// lease every TTL/3 via the manager's Heartbeat command for as long as the
// attempt's runtime is live. Without it, a runtime that outlives its lease's
// TTL would be swept as lease_expired while still running, letting a second
// attempt run concurrently.

// leaseHeartbeat is one attempt's heartbeat goroutine. Stop closes the stop
// channel; the goroutine exits on stop, supervisor shutdown, or a heartbeat
// error meaning the lease is no longer held, and removes itself from the
// supervisor's registry.
type leaseHeartbeat struct {
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// Stop signals the heartbeat goroutine to exit without waiting for it.
func (hb *leaseHeartbeat) Stop() {
	if hb == nil {
		return
	}
	hb.stopOnce.Do(func() { close(hb.stop) })
}

// StopAndWait signals the heartbeat goroutine to exit and blocks until it
// has, so no heartbeat can be applied after the caller's terminal fact.
func (hb *leaseHeartbeat) StopAndWait() {
	if hb == nil {
		return
	}
	hb.Stop()
	<-hb.done
}

// startLeaseHeartbeat launches the renewal goroutine for one dispatched
// attempt and registers it for shutdown cleanup. The cadence is the lease
// TTL divided by three (falling back to DefaultLeaseTTL when the context
// carries no TTL), so at least two renewals land inside every TTL window.
// The goroutine holds no supervisor mutex while calling the workflow
// manager.
func (s *Supervisor) startLeaseHeartbeat(ec workflow.ExecutorContext) *leaseHeartbeat {
	hb := &leaseHeartbeat{stop: make(chan struct{}), done: make(chan struct{})}
	ttl := ec.LeaseTTL
	if ttl <= 0 {
		ttl = workflow.DefaultLeaseTTL
	}
	interval := ttl / 3
	if interval <= 0 {
		interval = time.Second
	}
	s.heartbeatMu.Lock()
	s.heartbeats[hb] = struct{}{}
	s.heartbeatMu.Unlock()
	go func() {
		defer close(hb.done)
		defer func() {
			s.heartbeatMu.Lock()
			delete(s.heartbeats, hb)
			s.heartbeatMu.Unlock()
		}()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hb.stop:
				return
			case <-s.shutdownCh:
				return
			case <-ticker.C:
			}
			mgr := s.workflowManager()
			if mgr == nil {
				// The workflow barrier never produced a manager; nothing can
				// be renewed. Retry on the next tick in case the barrier
				// completes later.
				continue
			}
			err := mgr.Heartbeat(ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.LeaseID, ec.OwnerToken)
			if err == nil {
				continue
			}
			if errors.Is(err, workflow.ErrLeaseNotHeld) {
				// The lease was swept or replaced: renewal can never succeed
				// again, so stop instead of retrying.
				fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: lease no longer held, stopping heartbeat: %v\n",
					ec.WorkflowID, ec.NodeID, ec.AttemptID, err)
				return
			}
			// Transient error (revision contention, IO): retry on the next
			// tick.
		}
	}()
	return hb
}

// stopLeaseHeartbeats stops every registered heartbeat goroutine. It runs on
// supervisor shutdown so no heartbeat outlives the process's runtimes.
func (s *Supervisor) stopLeaseHeartbeats() {
	s.heartbeatMu.Lock()
	defer s.heartbeatMu.Unlock()
	for hb := range s.heartbeats {
		hb.Stop()
	}
}
