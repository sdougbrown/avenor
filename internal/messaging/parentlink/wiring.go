package parentlink

import "github.com/sdougbrown/avenor/internal/messaging"

// WiringTarget is the broker surface Install wires a parent link onto. The
// interface keeps this package from importing the broker package while every
// host that installs a link runs the same install sequence.
type WiringTarget interface {
	LocalMessenger() messaging.Messenger
	SetParentLinkToken(token string)
	SetMessenger(messaging.Messenger)
	SetParentLink(messaging.ParentLink)
	SetRunObserver(fn func())
	// DeleteRun removes one run record the target adopted for a descendant, so
	// the relay can retire both halves of the seam when it stops routing a run.
	DeleteRun(runID string)
}

// Install builds the Messenger for opts and installs it on target: the
// parent-route credential first, then the local messenger delegate, the
// parent-link seam and the run observer. The credential goes first so the
// parent routes never serve without it, and the messenger seam before the
// parent-link seam so a registration accepted in between still routes through
// the link. Every host that installs a link — test branch or supervisor — runs
// this sequence; the caller owns Start and Close.
func Install(opts Options, target WiringTarget) (*Messenger, error) {
	if opts.RetireRuns == nil {
		opts.RetireRuns = func(runIDs []string) {
			for _, runID := range runIDs {
				target.DeleteRun(runID)
			}
		}
	}
	link, err := New(opts, target.LocalMessenger())
	if err != nil {
		return nil, err
	}
	target.SetParentLinkToken(opts.Token)
	target.SetMessenger(link)
	target.SetParentLink(link)
	target.SetRunObserver(link.RunChanged)
	return link, nil
}

// RunClaims reports the runs a branch publishes to its parent: every session
// its broker serves minus the runs a child branch owns, each with the
// credential the run already uses. The broker's session record lives in the
// broker package, which this leaf does not import, so identify reports a
// session's run ID and display label; owned reports whether a run belongs to a
// child branch's claim, and token reports a run's credential.
func RunClaims[T any](sessions []T, identify func(session T) (runID, label string), owned func(runID string) bool, token func(runID string) (string, bool)) []messaging.RunClaim {
	claims := make([]messaging.RunClaim, 0, len(sessions))
	for _, session := range sessions {
		runID, label := identify(session)
		if owned(runID) {
			continue
		}
		credential, _ := token(runID)
		claims = append(claims, messaging.RunClaim{RunID: runID, Label: label, Token: credential})
	}
	return claims
}
