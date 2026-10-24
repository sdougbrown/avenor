// Package parentlink connects one broker to its parent broker. A child broker
// with a configured parent resolves a target locally first and only then
// through its parent; the parent resolves the descendants that registered with
// it. The link owns no ask edge of its own: a cross-branch ask is admitted
// exactly once, in the ancestor's ask store, and the relay that carries it into
// the next queue publishes into that queue without admitting it again.
package parentlink

import (
	"fmt"
	"sync"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// ErrRunClaimed reports a registration that claims a run another child broker
// already owns.
var ErrRunClaimed = fmt.Errorf("%w: run already claimed by another child broker", messaging.ErrInvalidRequest)

// ErrParentUnavailable reports a parent broker that could not be reached or
// refused the hop. It classifies as ErrTargetUnavailable: the target cannot be
// reached from this branch right now, and nothing is replayed.
var ErrParentUnavailable = fmt.Errorf("%w: parent broker unreachable", messaging.ErrTargetUnavailable)

// Registry is a parent broker's record of the child brokers that registered
// with it: each child's endpoint, the generation its ask store pinned, and the
// runs it owns. It owns no queue, performs no I/O and stores no ask edge.
type Registry struct {
	mu       sync.RWMutex
	children map[string]*child
	owners   map[string]string
}

// child is one registered child broker.
type child struct {
	id         string
	endpoint   string
	generation string
	// runs maps the run ID this broker addresses a claimed run by to the run ID
	// the owning branch knows it by, so a namespaced claim still relays to the
	// canonical run.
	runs   map[string]string
	labels map[string]string
}

// ParentRunKey reports the run ID a parent broker addresses a descendant's run
// by. A branch supervisor run is reserved in every branch, so the parent's own
// supervisor and each descendant's supervisor would otherwise be the same run;
// it is namespaced by the branch identity that claims it. Every other run ID is
// minted by the branch that owns it, so it stays canonical.
func ParentRunKey(childID, runID string) string {
	if runID != messaging.SupervisorRunID {
		return runID
	}
	return runID + "@" + childID
}

// Hop is one child broker as a relay next hop: the endpoint a relay is posted
// to, the run ID that branch knows the target by, and the generation that
// endpoint pinned when the child registered.
type Hop struct {
	ChildID    string
	Endpoint   string
	Generation string
	Label      string
	// RunID is the target run's canonical run ID in its own branch, which is
	// what the relay posts to. It differs from the run ID this broker addresses
	// the run by only for a namespaced branch-supervisor claim.
	RunID string
}

// NewRegistry builds an empty descendant registry.
func NewRegistry() *Registry {
	return &Registry{
		children: make(map[string]*child),
		owners:   make(map[string]string),
	}
}

// Register replaces one child's claim with the runs it now owns and reports the
// runs it adopted and the ones it stopped claiming. A run another child already
// owns is refused and nothing is applied.
func (r *Registry) Register(reg messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
	claim := messaging.DescendantClaim{}
	keys := make([]string, len(reg.Runs))
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, run := range reg.Runs {
		key := ParentRunKey(reg.ChildID, run.RunID)
		if owner, ok := r.owners[key]; ok && owner != reg.ChildID {
			return messaging.DescendantClaim{}, fmt.Errorf("%w: %s", ErrRunClaimed, run.RunID)
		}
		keys[i] = key
	}
	current, ok := r.children[reg.ChildID]
	if !ok {
		current = &child{id: reg.ChildID, runs: map[string]string{}, labels: map[string]string{}}
		r.children[reg.ChildID] = current
	}
	current.endpoint = reg.Endpoint
	current.generation = reg.Generation
	adopted := make(map[string]struct{}, len(reg.Runs))
	for i, run := range reg.Runs {
		key := keys[i]
		claim.Runs = append(claim.Runs, messaging.ClaimedRun{
			RunID: run.RunID,
			Key:   key,
			Label: run.Label,
			Token: run.Token,
		})
		adopted[key] = struct{}{}
		if _, held := r.owners[key]; !held {
			claim.Adopted = append(claim.Adopted, key)
		}
		r.owners[key] = reg.ChildID
		current.runs[key] = run.RunID
		current.labels[key] = run.Label
	}
	for runID := range r.owners {
		if r.owners[runID] != reg.ChildID {
			continue
		}
		if _, still := adopted[runID]; still {
			continue
		}
		delete(r.owners, runID)
		delete(current.runs, runID)
		delete(current.labels, runID)
		claim.Retired = append(claim.Retired, runID)
	}
	return claim, nil
}

// Deregister retires one child and reports the runs it had claimed.
func (r *Registry) Deregister(childID string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.children[childID]; !ok {
		return nil, fmt.Errorf("%w: child %q is not registered", messaging.ErrTargetUnavailable, childID)
	}
	delete(r.children, childID)
	var runs []string
	for runID, owner := range r.owners {
		if owner != childID {
			continue
		}
		delete(r.owners, runID)
		runs = append(runs, runID)
	}
	return runs, nil
}

// Forget drops one run's claim, which is what a relay that found its child gone
// leaves behind.
func (r *Registry) Forget(runID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.owners[runID]
	if !ok {
		return
	}
	delete(r.owners, runID)
	if current, ok := r.children[owner]; ok {
		delete(current.runs, runID)
		delete(current.labels, runID)
	}
}

// Owns reports whether runID belongs to a registered child broker.
func (r *Registry) Owns(runID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.owners[runID]
	return ok
}

// Owner reports the child that owns runID.
func (r *Registry) Owner(runID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	owner, ok := r.owners[runID]
	return owner, ok
}

// Hop reports the next hop a relay for runID takes.
func (r *Registry) Hop(runID string) (Hop, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	owner, ok := r.owners[runID]
	if !ok {
		return Hop{}, false
	}
	current := r.children[owner]
	if current == nil || current.endpoint == "" {
		return Hop{}, false
	}
	return Hop{
		ChildID:    current.id,
		Endpoint:   current.endpoint,
		Generation: current.generation,
		Label:      current.labels[runID],
		RunID:      current.runs[runID],
	}, true
}

// Export narrows a peer listing to what the authenticated caller may see. This
// broker's own runs are always exported; a run a registered child owns is
// exported to a caller in that child's own branch and to this broker's own
// callers (its owned descendants), but never to a caller in a sibling branch.
func (r *Registry) Export(peers []messaging.Peer, caller string) []messaging.Peer {
	callerBranch, callerIsDescendant := r.Owner(caller)
	r.mu.RLock()
	defer r.mu.RUnlock()
	exported := make([]messaging.Peer, 0, len(peers))
	for _, peer := range peers {
		owner, registered := r.owners[peer.RunID]
		switch {
		case !registered:
			exported = append(exported, peer)
		case !callerIsDescendant, owner == callerBranch:
			exported = append(exported, peer)
		}
	}
	return exported
}
