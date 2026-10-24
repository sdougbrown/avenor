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
	"strings"
	"sync"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// ErrRunClaimed reports a registration that claims a run another child broker
// already owns.
var ErrRunClaimed = fmt.Errorf("%w: run already claimed by another child broker", messaging.ErrInvalidRequest)

// ErrRunLocallyOwned reports a registration that claims a run this broker
// itself owns. A locally owned run is never a descendant's to relay into.
var ErrRunLocallyOwned = fmt.Errorf("%w: run already owned by this broker", messaging.ErrInvalidRequest)

// ErrInvalidRunID reports a registration that claims a run ID the run-key
// namespacing reserves: a run ID carrying "@" reads as a branch-namespaced
// key, so a child claiming one could forge another branch's supervisor
// identity. Run IDs are minted without the separator.
var ErrInvalidRunID = fmt.Errorf("%w: run id contains the reserved separator \"@\"", messaging.ErrInvalidRequest)

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
	// runs maps the run ID this broker addresses a claimed run by to the run ID
	// the owning branch knows it by, so a namespaced claim still relays to the
	// canonical run.
	runs map[string]string
	// tokens holds, per run, the credential the child handed the parent in its
	// registration. The relay posts it to the child, so the credential the
	// child's own runs already use is the one that travels.
	tokens map[string]string
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
// to, the run ID that branch knows the target by, the generation that endpoint
// pinned when the child registered, and the credential the child handed its
// parent for the target run.
type Hop struct {
	ChildID    string
	Endpoint   string
	Generation string
	// Token is the credential the child handed its parent for this run, which
	// the relay posts back to the child.
	Token string
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
	if err := validEndpoint(reg.Endpoint); err != nil {
		return claim, err
	}
	for _, run := range reg.Runs {
		if strings.Contains(run.RunID, "@") {
			return messaging.DescendantClaim{}, fmt.Errorf("%w: %q", ErrInvalidRunID, run.RunID)
		}
	}
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
		current = &child{id: reg.ChildID, runs: map[string]string{}, tokens: map[string]string{}}
		r.children[reg.ChildID] = current
	}
	current.endpoint = reg.Endpoint
	current.generation = reg.Generation
	adopted := make(map[string]struct{}, len(reg.Runs))
	for i, run := range reg.Runs {
		key := keys[i]
		claim.Runs = append(claim.Runs, messaging.ClaimedRun{RunClaim: run, Key: key})
		adopted[key] = struct{}{}
		if _, held := r.owners[key]; !held {
			claim.Adopted = append(claim.Adopted, key)
		}
		r.owners[key] = reg.ChildID
		current.runs[key] = run.RunID
		current.tokens[key] = run.Token
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
		delete(current.tokens, runID)
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
	// The relay posts its hop to the endpoint's root, so a path under the host
	// would be one the relay never sends to.
	if parsed.EscapedPath() != "" {
		return fmt.Errorf("%w: the branch's endpoint must be its broker's root address", messaging.ErrInvalidRequest)
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

// ForgetIfStale drops one run's claim, which is what a relay that found its
// child gone leaves behind, but only while the claim still resolves to hop: the
// run's endpoint, generation, canonical run ID and the credential the relay
// authenticated with. A replacement branch that registered while the refusal was
// in flight has refreshed the claim, and so has a run that restarted inside a live
// branch, which keeps the endpoint and generation but rotates its credential;
// retiring either claim would take a healthy run out of routing. It reports
// whether it dropped the claim.
func (r *Registry) ForgetIfStale(runID string, hop Hop) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.owners[runID]
	if !ok || owner != hop.ChildID {
		return false
	}
	current := r.children[owner]
	if current == nil ||
		current.endpoint != hop.Endpoint ||
		current.generation != hop.Generation ||
		current.runs[runID] != hop.RunID ||
		current.tokens[runID] != hop.Token {
		return false
	}
	delete(r.owners, runID)
	delete(current.runs, runID)
	delete(current.tokens, runID)
	return true
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
		Token:      current.tokens[runID],
		RunID:      current.runs[runID],
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
