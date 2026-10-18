// Package runstate normalizes supervisor lifecycle status for consumers.
package runstate

import "strings"

// RuntimeStatus is the typed vocabulary of consumer-facing runtime statuses.
type RuntimeStatus string

const (
	// Raw supervisor statuses as consumers may observe them.
	StatusIdle    RuntimeStatus = "idle"
	StatusRunning RuntimeStatus = "running"
	StatusEnded   RuntimeStatus = "ended"

	// Consumer-facing terminal and waiting statuses.
	StatusDone    RuntimeStatus = "done"
	StatusFailed  RuntimeStatus = "failed"
	StatusTimeout RuntimeStatus = "timeout"
	StatusKilled  RuntimeStatus = "killed"
	StatusWaiting RuntimeStatus = "waiting"
	StatusBlocked RuntimeStatus = "blocked"

	// StatusUnknown is the consumer-facing status for a raw status Translate
	// does not recognize. It is not terminal.
	StatusUnknown RuntimeStatus = "unknown"
)

// Phase is the typed vocabulary of consumer-facing runtime phases.
type Phase string

const (
	PhaseNone     Phase = ""
	PhaseThinking Phase = "thinking"
	PhaseWorking  Phase = "working"
	PhaseWaiting  Phase = "waiting"
	PhaseDone     Phase = "done"

	// PhaseUnknown is the consumer-facing phase for a raw phase that is not
	// in the recognized set.
	PhaseUnknown Phase = "unknown"
)

// ParsePhase maps a raw phase string onto the Phase vocabulary. It returns the
// matching constant and true for the empty phase and the four named phases,
// and PhaseUnknown with false for anything else.
func ParsePhase(raw string) (Phase, bool) {
	switch Phase(raw) {
	case PhaseNone, PhaseThinking, PhaseWorking, PhaseWaiting, PhaseDone:
		return Phase(raw), true
	default:
		return PhaseUnknown, false
	}
}

// Translation is the consumer-facing lifecycle state derived from a raw
// supervisor status and phase. StatusRecognized reports whether Translate
// recognized the raw status; it is false only for the StatusUnknown fallback.
type Translation struct {
	Status           RuntimeStatus
	Phase            Phase
	TurnComplete     bool
	StatusRecognized bool
}

// Translate normalizes a raw supervisor status and phase. An idle runtime is
// complete only when its phase is terminal. A running runtime stays running
// even when terminal phase metadata arrives before active-attempt cleanup
// finishes. An unrecognized status maps to StatusUnknown with
// StatusRecognized false.
func Translate(status RuntimeStatus, phase Phase) Translation {
	translatedStatus := status
	translatedPhase := phase
	recognized := true

	switch status {
	case StatusRunning:
		translatedStatus = StatusRunning
		if IsTerminalPhase(phase) {
			translatedPhase = PhaseNone
		}
	case StatusIdle:
		if IsTerminalPhase(phase) {
			translatedStatus = RuntimeStatus(phase)
		} else {
			translatedStatus = StatusRunning
		}
	case StatusEnded:
		translatedStatus = StatusDone
	case StatusDone, StatusFailed, StatusTimeout, StatusKilled, StatusWaiting:
		// Already in consumer-facing form.
	case StatusBlocked:
		translatedStatus = StatusFailed
	default:
		translatedStatus = StatusUnknown
		recognized = false
	}

	return Translation{
		Status:           translatedStatus,
		Phase:            translatedPhase,
		TurnComplete:     IsTerminalStatus(translatedStatus),
		StatusRecognized: recognized,
	}
}

// IsTurnComplete reports whether a raw supervisor status and phase identify a
// completed turn. It uses the same normalization as Translate so callers do
// not need to reproduce the running/terminal-phase race handling.
func IsTurnComplete(status RuntimeStatus, phase Phase) bool {
	return Translate(status, phase).TurnComplete
}

// IsTerminalStatus reports whether a consumer-facing status is terminal.
func IsTerminalStatus(status RuntimeStatus) bool {
	switch status {
	case StatusDone, StatusFailed, StatusTimeout, StatusKilled:
		return true
	default:
		return false
	}
}

// IsTerminalPhase reports whether a phase value is one of the terminal
// consumer-facing statuses.
func IsTerminalPhase(phase Phase) bool {
	return IsTerminalStatus(RuntimeStatus(phase))
}

// SentinelStatus maps a completion sentinel's status word to the
// consumer-facing status it represents. Unrecognized status words pass
// through lowercased.
func SentinelStatus(raw string) RuntimeStatus {
	switch raw {
	case "DONE":
		return StatusDone
	case "FAILED", "BLOCKED":
		return StatusFailed
	case "TIMEOUT":
		return StatusTimeout
	case "KILLED":
		return StatusKilled
	default:
		return RuntimeStatus(strings.ToLower(raw))
	}
}
