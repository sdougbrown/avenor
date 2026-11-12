package stable

import (
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/tooladmission"
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
	return "", router.AmbiguousName(ref, matches)
}

func unknownTargetError(ref string) error {
	return &router.ToolError{
		Code:    "unknown_target",
		Message: messaging.Repair("unknown_target", map[string]string{"target": tooladmission.Quote(ref)}),
	}
}
