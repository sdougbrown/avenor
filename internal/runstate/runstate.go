// Package runstate normalizes supervisor lifecycle status for consumers.
package runstate

// Translation is the consumer-facing lifecycle state derived from a raw
// supervisor status and phase.
type Translation struct {
	Status       string
	Phase        string
	TurnComplete bool
}

// Translate normalizes a raw supervisor status and phase. An idle runtime is
// complete only when its phase is terminal. A running runtime stays running
// even when terminal phase metadata arrives before active-attempt cleanup
// finishes.
func Translate(status, phase string) Translation {
	translatedStatus := status
	translatedPhase := phase

	switch status {
	case "running":
		translatedStatus = "running"
		if isTerminalPhase(phase) {
			translatedPhase = ""
		}
	case "idle":
		if isTerminalPhase(phase) {
			translatedStatus = phase
		} else {
			translatedStatus = "running"
		}
	case "ended":
		translatedStatus = "done"
	case "done", "failed", "timeout", "killed", "waiting":
		// Already in consumer-facing form.
	case "blocked":
		translatedStatus = "failed"
	default:
		translatedStatus = "running"
	}

	return Translation{
		Status:       translatedStatus,
		Phase:        translatedPhase,
		TurnComplete: IsTerminalStatus(translatedStatus),
	}
}

// IsTurnComplete reports whether a raw supervisor status and phase identify a
// completed turn. It uses the same normalization as Translate so callers do
// not need to reproduce the running/terminal-phase race handling.
func IsTurnComplete(status, phase string) bool {
	return Translate(status, phase).TurnComplete
}

// IsTerminalStatus reports whether a consumer-facing status is terminal.
func IsTerminalStatus(status string) bool {
	switch status {
	case "done", "failed", "timeout", "killed":
		return true
	default:
		return false
	}
}

func isTerminalPhase(phase string) bool {
	return IsTerminalStatus(phase)
}
