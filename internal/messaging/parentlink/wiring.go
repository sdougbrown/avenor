package parentlink

import "github.com/sdougbrown/avenor/internal/messaging"

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
