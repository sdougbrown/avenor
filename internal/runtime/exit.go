package runtime

const SessionIDConflictStopReason = "session_id_conflict"

// IsRetryableFailure reports whether a failed attempt may be retried. Session
// identity conflicts are fatal: retrying could resume a provisional ID or let
// an attempt finish without adopting the provider's authoritative session ID.
func IsRetryableFailure(exitCode int, stopReason string) bool {
	return exitCode == 1 && stopReason != SessionIDConflictStopReason
}

// ExitCodeForStopReason returns Avenor's locked process exit code for a
// terminal ACP stop reason.
func ExitCodeForStopReason(stopReason string) int {
	switch stopReason {
	case "end_turn":
		return 0
	case "refusal":
		return 2
	case "max_tokens":
		return 3
	case "max_turn_requests":
		return 4
	case "blocked":
		return 5
	case "cancelled":
		return 130
	case "progress_timeout":
		return 124
	case "timeout":
		return 124
	default:
		return 1
	}
}

// StopReasonForExitCode is the inverse of ExitCodeForStopReason. Returns an
// empty string for exit code 1 (generic failure) since there is no canonical
// stop reason for that case.
func StopReasonForExitCode(exitCode int) string {
	switch exitCode {
	case 0:
		return "end_turn"
	case 2:
		return "refusal"
	case 3:
		return "max_tokens"
	case 4:
		return "max_turn_requests"
	case 5:
		return "blocked"
	case 124:
		return "timeout"
	case 130:
		return "cancelled"
	default:
		return ""
	}
}
