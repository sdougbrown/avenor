package stable

import "fmt"

// WithTurn serializes a turn-bound operation with warm turn admission: the
// operation runs under the runtime's turnMu, so it cannot interleave with
// admission for the same runtime, and other runtimes' turns proceed
// independently. Lock order: see childRuntime.turnMu.
func (s *Supervisor) WithTurn(runtimeID, sentinel string, operation func() error) error {
	s.controlMu.Lock()
	child := s.runtimes[runtimeID]
	s.controlMu.Unlock()
	if child == nil {
		return fmt.Errorf("turn no longer available for runtime %q", runtimeID)
	}
	child.turnMu.Lock()
	defer child.turnMu.Unlock()
	child.mu.Lock()
	matches := sentinel != "" && child.sentinelFile == sentinel
	child.mu.Unlock()
	if !matches {
		return fmt.Errorf("turn no longer current for runtime %q", runtimeID)
	}
	return operation()
}
