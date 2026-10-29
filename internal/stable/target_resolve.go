package stable

import (
	"fmt"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// resolveRunReference resolves a run reference through the one ladder the
// resume and rename authorities share: an exact broker runtime id, then the one
// display name. A public run id never resolves here; each surface resolves it
// by ID before calling. Unmatched references are unknown_target and a name
// several runs share is ambiguous_name, both rendered from the same facts.
func (s *Supervisor) resolveRunReference(ref string) (string, error) {
	if s.broker == nil {
		return "", unknownTargetError(ref)
	}
	// A tombstone is a run this broker knows and can no longer route to, so the
	// ladder refuses it the way it refuses an id it has never seen: resolving it
	// would let a rename report success on a peer nobody can reach.
	if s.broker.Reachable(ref) {
		return ref, nil
	}
	matches := s.broker.MatchLabel(ref)
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return "", unknownTargetError(ref)
	}
	facts := map[string]string{
		"target":     messaging.Quote(ref),
		"count":      fmt.Sprint(len(matches)),
		"candidates": messaging.QuoteList(matches),
	}
	return "", &router.ToolError{
		Code:    "ambiguous_name",
		Message: messaging.Repair("ambiguous_name", facts),
		Facts:   facts,
	}
}

func unknownTargetError(ref string) error {
	return &router.ToolError{
		Code:    "unknown_target",
		Message: messaging.Repair("unknown_target", map[string]string{"target": messaging.Quote(ref)}),
	}
}
