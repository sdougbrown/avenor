// Package parentlink connects one broker to its parent broker. A child broker
// with a configured parent resolves a target locally first and only then
// through its parent; the parent resolves the descendants that registered with
// it. The link owns no ask edge of its own: a cross-branch ask is admitted
// exactly once, in the ancestor's ask store, and the relay that carries it into
// the next queue publishes into that queue without admitting it again.
package parentlink

import (
	"fmt"
	"net/url"
	"sync"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// ErrRunClaimed reports a registration that claims a run another child broker
// already owns.
var ErrRunClaimed = fmt.Errorf("%w: run already claimed by another child broker", messaging.ErrInvalidRequest)

// ErrParentUnavailable reports a peer broker that could not be reached or
// refused the hop. It classifies as ErrTargetUnavailable: the target cannot be
// reached from this branch right now, and nothing is replayed.
var ErrParentUnavailable = fmt.Errorf("%w: the peer broker is unreachable", messaging.ErrTargetUnavailable)

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
	labels     map[string]string
}

// Hop is one child broker as a relay next hop: the endpoint a relay is posted
// to and the generation that endpoint pinned when the child registered.
type Hop struct {
	ChildID    string
	Endpoint   string
	Generation string
	Label      string
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
	if err := validEndpoint(reg.Endpoint); err != nil {
		return claim, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, run := range reg.Runs {
		if owner, ok := r.owners[run.RunID]; ok && owner != reg.ChildID {
			return messaging.DescendantClaim{}, fmt.Errorf("%w: %s", ErrRunClaimed, run.RunID)
		}
	}
	current, ok := r.children[reg.ChildID]
	if !ok {
		current = &child{id: reg.ChildID, labels: map[string]string{}}
		r.children[reg.ChildID] = current
	}
	current.endpoint = reg.Endpoint
	current.generation = reg.Generation
	adopted := make(map[string]struct{}, len(reg.Runs))
	for _, run := range reg.Runs {
		adopted[run.RunID] = struct{}{}
		if _, held := r.owners[run.RunID]; !held {
			claim.Adopted = append(claim.Adopted, run.RunID)
		}
		r.owners[run.RunID] = reg.ChildID
		current.labels[run.RunID] = run.Label
	}
	for runID := range r.owners {
		if r.owners[runID] != reg.ChildID {
			continue
		}
		if _, still := adopted[runID]; still {
			continue
		}
		delete(r.owners, runID)
		delete(current.labels, runID)
		claim.Retired = append(claim.Retired, runID)
	}
	return claim, nil
}

// validEndpoint refuses an endpoint the relay could not post to. The relay
// posts the target run's credential to the endpoint a child registered, so an
// endpoint that is not an absolute http address is refused rather than
// carried. An empty endpoint names no route at all and is left to the hop.
func validEndpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("%w: the branch's endpoint is not a URL: %v", messaging.ErrInvalidRequest, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%w: the branch's endpoint must be an http address", messaging.ErrInvalidRequest)
	}
	if parsed.Host == "" {
		return fmt.Errorf("%w: the branch's endpoint names no host", messaging.ErrInvalidRequest)
	}
	return nil
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
	}, true
}

// Export narrows a peer listing to what the authenticated caller may see. This
// broker's own runs are always exported; a run a registered child owns is
// exported to a caller in that child's own branch and to this broker's own
// callers (its owned descendants), but never to a caller in a sibling branch.
func (r *Registry) Export(peers []messaging.Peer, caller string) []messaging.Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// The caller's branch and the peers' owners are read under the same lock, so
	// a registration that lands mid-listing cannot split what the caller is from
	// what it is shown.
	callerBranch, callerIsDescendant := r.owners[caller]
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
