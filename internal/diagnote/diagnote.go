// Package diagnote gates repeated one-line diagnostics: a caller decides under
// the gate's lock whether a line is still due, then writes it itself, so a
// blocked writer cannot stall concurrent callers.
package diagnote

import "sync"

// Cap bounds the number of distinct keys a Gate remembers before it switches
// to one-shot suppression.
const Cap = 16

// SuppressedNote is the shared wording of the one-time line a caller emits
// after the cap is reached; each caller prefixes it with its own component
// label when writing the line.
const SuppressedNote = "further unrecognized values suppressed"

// Gate deduplicates diagnostic keys and caps how many distinct keys it
// remembers. The zero value is ready to use; a nil *Gate admits nothing.
type Gate struct {
	mu sync.Mutex
	// seen records the keys already admitted; suppressed marks that the cap
	// was reached and the single suppression admission was spent. Both are
	// guarded by mu.
	seen       map[string]struct{}
	suppressed bool
}

// Admit reports whether a diagnostic line is still owed for key: log is true
// when key was never admitted and the cap is not reached; suppressed is true
// exactly once, the first time Admit is called after the cap is reached. The
// decision is recorded under the lock; the caller writes its own message after
// Admit returns.
func (g *Gate) Admit(key string) (log, suppressed bool) {
	if g == nil {
		return false, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.seen[key]; ok {
		return false, false
	}
	if len(g.seen) < Cap {
		if g.seen == nil {
			g.seen = make(map[string]struct{})
		}
		g.seen[key] = struct{}{}
		return true, false
	}
	if g.suppressed {
		return false, false
	}
	g.suppressed = true
	return false, true
}
