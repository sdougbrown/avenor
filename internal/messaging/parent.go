package messaging

import "context"

// This file is the broker-hierarchy seam. A child broker with a configured
// parent resolves a target locally first and only then through its parent; the
// parent resolves the descendants that registered with it. It adds no second
// ask edge: the ancestor's ask store owns the one edge a cross-branch ask has,
// and the relay that carries it into the next queue publishes into that queue
// without admitting the ask again.

// ParentLink is the seam a broker serves its parent-link routes from: the
// descendant registry the parent's registration routes write, and the reply
// routes a relayed ask binds before the broker exposes it to a consumer.
type ParentLink interface {
	// Owns reports whether runID belongs to a registered child broker.
	Owns(runID string) bool
	// Key reports the run ID this broker addresses childID's runID by, which is
	// how a claim and every later hop name a descendant's run.
	Key(childID, runID string) string
	// Register adopts one child broker's claim on the runs it owns and reports
	// what the claim changed. A run another child already owns is refused.
	Register(reg DescendantRegistration) (DescendantClaim, error)
	// Deregister retires one child and reports the run keys it had claimed with
	// the credential each claim was registered with, but only while the
	// registration still matches the closing branch's view; a registration a
	// same-identity replacement replaced is refused with
	// ErrGenerationMismatch and nothing is retired.
	Deregister(childID string, reg DescendantRegistration) (map[string]string, error)
	// RecordRelay records the reply route of a relayed ask. The broker calls it
	// before it exposes the ask to a consumer, so a reply always follows the
	// hop the ask arrived on.
	RecordRelay(route RelayRoute) error
}

// RunClaim is one run a branch owns: the display name that branch knows it by
// and the credential that branch authenticates it with. A branch hands its
// parent the credentials its own runs already use, so a forwarded ask arrives as
// the run that asked and a reply can complete the edge at the hop that admitted
// it.
type RunClaim struct {
	RunID string `json:"run_id"`
	Label string `json:"label,omitempty"`
	Token string `json:"token,omitempty"`
}

// DescendantRegistration is one child broker's claim on the runs it owns: where
// its broker answers, the generation its ask store pinned, and the runs. It is
// also the view a closing branch holds of the registration its parent last
// accepted: a deregistration carries it for the parent to compare against the
// registration it holds, so a deregistration that arrives after a replacement
// re-registered is refused instead of retiring the replacement's claims.
type DescendantRegistration struct {
	ChildID    string     `json:"child_id"`
	Endpoint   string     `json:"endpoint"`
	Generation string     `json:"generation"`
	Runs       []RunClaim `json:"runs"`
}

// ClaimedRun is one run a descendant claims, as the parent broker addresses it.
// RunID is the canonical run ID the owning branch knows the run by and Key is
// the run ID this broker routes to: every claim is namespaced by the branch that
// makes it, because sibling branches mint the same run IDs. Label and Token are
// what the owning branch hands the parent, so the parent authenticates the run
// and relays into it with the run's own credential.
type ClaimedRun struct {
	RunClaim
	// Key is the run ID this broker addresses the claimed run by.
	Key string
}

// DescendantClaim reports what one registration changed, so the broker can
// adopt every run record the child now claims and retire the ones it stopped
// claiming. Runs, Adopted and Retired all name runs by the run ID this broker
// addresses them by.
type DescendantClaim struct {
	Runs    []ClaimedRun
	Adopted []string
	Retired []string
}

// RelayRoute is one relayed ask's reply route, recorded before the ask is
// exposed to a consumer: the run that asked, and the generation the ancestor's
// ask store admitted the edge under. A reply that finds that generation stale
// is never replayed onto a replacement ancestor.
type RelayRoute struct {
	MessageID      string
	AskerRunID     string
	EdgeGeneration string
}

// callerRunKey carries the authenticated run a broker request came from.
type callerRunKey struct{}

// WithCallerRun records the authenticated run a broker request came from, so a
// peer export can be narrowed to what that caller may see.
func WithCallerRun(ctx context.Context, runID string) context.Context {
	if runID == "" {
		return ctx
	}
	return context.WithValue(ctx, callerRunKey{}, runID)
}

// CallerRun reports the authenticated run a broker request came from.
func CallerRun(ctx context.Context) (string, bool) {
	runID, ok := ctx.Value(callerRunKey{}).(string)
	return runID, ok && runID != ""
}
