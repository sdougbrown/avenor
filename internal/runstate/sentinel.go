package runstate

import (
	"fmt"
	"os"
	"strings"
)

// Sentinel is a run's completion sentinel: the first line is the status word and
// the remaining lines carry KEY=value metadata. The status word is kept verbatim
// so a caller can report what the run actually wrote.
type Sentinel struct {
	Status     string // DONE | FAILED | BLOCKED | TIMEOUT | KILLED (verbatim)
	SessionID  string // SESSION= value
	StopReason string // STOP_REASON= value
}

// ReadSentinel reads the sentinel file at path.
func ReadSentinel(path string) (*Sentinel, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sentinel: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 || lines[0] == "" {
		return nil, fmt.Errorf("empty sentinel file")
	}
	sentinel := &Sentinel{Status: lines[0]}
	for _, line := range lines[1:] {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch key {
		case "SESSION":
			sentinel.SessionID = value
		case "STOP_REASON":
			sentinel.StopReason = value
		}
	}
	return sentinel, nil
}

// ReadSentinelSession returns the provider session a resumable sentinel names.
// DONE, FAILED and BLOCKED settle the provider session itself, so the backend
// transcript persists and a resume can reload and continue it. TIMEOUT and
// KILLED are supervisor-imposed terminations, where an automatic continuation
// would surprise.
func ReadSentinelSession(path string) (string, error) {
	sentinel, err := ReadSentinel(path)
	if err != nil {
		return "", err
	}
	if !isResumableSentinel(sentinel.Status) {
		return "", fmt.Errorf("run is not resumable (status: %s)", strings.ToLower(sentinel.Status))
	}
	if sentinel.SessionID == "" {
		return "", fmt.Errorf("no session in sentinel")
	}
	return sentinel.SessionID, nil
}

// isResumableSentinel reports whether a sentinel status word names a status a
// resume may continue. It reads the SentinelStatus mapping rather than
// re-spelling the status words, so a status added to that vocabulary is
// classified once.
func isResumableSentinel(raw string) bool {
	switch SentinelStatus(strings.ToUpper(raw)) {
	case StatusDone, StatusFailed:
		return true
	}
	return false
}
