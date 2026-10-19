package cli

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// derivePermBase derives the permission handler base path from a sentinel path.
// Rule: if the path ends in ".done", strip it and append ".perm".
// Otherwise append ".perm" directly.
// This mirrors the derive_permission_base function in dispatch-avenor.sh.
func DerivePermBase(sentinelPath string) string {
	if strings.HasSuffix(sentinelPath, ".done") {
		return strings.TrimSuffix(sentinelPath, ".done") + ".perm"
	}
	return sentinelPath + ".perm"
}

// sentinelContent builds the sentinel file content from an exit code plus
// session metadata extracted from the event log. This matches the shim's
// output format exactly.
//
// Exit code semantics:
//
//	0   → DONE\nSESSION=...\nSTOP_REASON=...\n[RETRY_AFTER=...\n][RUN=...\n][RESULT=...\n]
//	5   → BLOCKED\nSESSION=...\nSTOP_REASON=blocked\n[REASON=...\n][RETRY_AFTER=...\n][RUN=...\n][RESULT=...\n]
//	124 → TIMEOUT\nSESSION=...\nSTOP_REASON=...\n[RETRY_AFTER=...\n][RUN=...\n][RESULT=...\n]
//	130 → KILLED\nSESSION=...\nSTOP_REASON=...\nEXIT_CODE=130\n[RETRY_AFTER=...\n][RUN=...\n][RESULT=...\n]
//	N   → FAILED\nSESSION=...\nSTOP_REASON=...\nEXIT_CODE=N\n[RETRY_AFTER=...\n][RUN=...\n][RESULT=...\n]
//
// The RUN= line is omitted when runID is empty.
// The REASON= line is only emitted for exit code 5 and only when reason is non-empty.
// The RETRY_AFTER= line is emitted only when the provider supplied a hint.
// The RESULT= line is omitted unless resultPath names a result file that was written.
func sentinelContent(exitCode int, sessionID, stopReason, runID string, retryAfter int64, reason, resultPath string) string {
	// The format is line-oriented, so no provider- or agent-supplied value may
	// carry its own newline.
	sessionID = sentinelLineValue(sessionID)
	stopReason = sentinelLineValue(stopReason)
	trailer := ""
	if runID != "" {
		trailer = "RUN=" + sentinelLineValue(runID) + "\n"
	}
	if resultPath != "" {
		trailer += "RESULT=" + sentinelLineValue(resultPath) + "\n"
	}
	retry := ""
	if retryAfter > 0 {
		retry = fmt.Sprintf("RETRY_AFTER=%d\n", retryAfter)
	}
	switch exitCode {
	case 0:
		if stopReason == "" {
			stopReason = "end_turn"
		}
		return fmt.Sprintf("DONE\nSESSION=%s\nSTOP_REASON=%s\n%s%s", sessionID, stopReason, retry, trailer)
	case 5:
		blockedStop := "blocked"
		reasonLine := ""
		if reason != "" {
			reasonLine = "REASON=" + sentinelLineValue(reason) + "\n"
		}
		return fmt.Sprintf("BLOCKED\nSESSION=%s\nSTOP_REASON=%s\n%s%s%s", sessionID, blockedStop, reasonLine, retry, trailer)
	case 124:
		if stopReason == "" {
			stopReason = "timeout"
		}
		return fmt.Sprintf("TIMEOUT\nSESSION=%s\nSTOP_REASON=%s\n%s%s", sessionID, stopReason, retry, trailer)
	case 130:
		if stopReason == "" {
			stopReason = "cancelled"
		}
		return fmt.Sprintf("KILLED\nSESSION=%s\nSTOP_REASON=%s\nEXIT_CODE=130\n%s%s", sessionID, stopReason, retry, trailer)
	default:
		if stopReason == "" {
			stopReason = fmt.Sprintf("exit_%d", exitCode)
		}
		return fmt.Sprintf("FAILED\nSESSION=%s\nSTOP_REASON=%s\nEXIT_CODE=%d\n%s%s", sessionID, stopReason, exitCode, retry, trailer)
	}
}

// sentinelLineValue keeps a value on its own single line of the sentinel file.
func sentinelLineValue(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return value
}

// sessionEndFields scans an NDJSON event log and returns the session_id and
// stop_reason from the LAST session.end event. If no such event exists, both
// strings are empty.
func sessionEndFields(eventLogPath string) (sessionID, stopReason string) {
	f, err := os.Open(eventLogPath)
	if err != nil {
		return "", ""
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Large events (tool output) can exceed the default 64 KiB limit.
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 4*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var row struct {
			Event     string `json:"event"`
			SessionID string `json:"session_id"`
			// sessionID is the alternate casing seen in some event payloads.
			SessionIDCamel string `json:"sessionID"`
			StopReason     string `json:"stop_reason"`
		}
		if err := json.Unmarshal(line, &row); err != nil {
			continue
		}
		if row.Event != "session.end" {
			continue
		}
		sid := row.SessionID
		if sid == "" {
			sid = row.SessionIDCamel
		}
		sessionID = sid
		stopReason = row.StopReason
	}
	return sessionID, stopReason
}

// WriteSentinelResult writes the completion sentinel file at path through the
// shared atomic write, with an optional reason line (used for exit code 5
// "blocked" sentinels), an optional provider retry-after hint in seconds, and
// an optional result file to expose as RESULT=. Errors are logged to stderr but
// never affect the caller's exit code.
func WriteSentinelResult(path string, exitCode int, sessionID, stopReason, runID string, retryAfter int64, reason, resultPath string, stderr io.Writer) {
	content := sentinelContent(exitCode, sessionID, stopReason, runID, retryAfter, reason, resultPath)
	if err := writeFileAtomic(path, ".sentinel-*.tmp", content); err != nil {
		fmt.Fprintf(stderr, "avenor: sentinel: %v\n", err)
	}
}

// WriteSentinel writes the completion sentinel file at path using an
// atomic tmp+rename strategy. Errors are logged to stderr but never affect
// the caller's exit code.
func WriteSentinel(path string, exitCode int, sessionID, stopReason, runID string, stderr io.Writer) {
	WriteSentinelResult(path, exitCode, sessionID, stopReason, runID, 0, "", "", stderr)
}

// cleanupSentinelFiles removes pre-existing sentinel and permission request
// files before a run, matching the shim's pre-run cleanup.
// Only called when --sentinel-file is set.
// Non-ENOENT errors are logged to stderr; ENOENT is silently ignored.
func cleanupSentinelFiles(sentinelPath, permBase string, stderr io.Writer) {
	removeOrLog(sentinelPath, stderr)
	if permBase != "" {
		removeOrLog(permBase+".req", stderr)
		removeOrLog(permBase+".req.response", stderr)
	}
}

// removeOrLog calls os.Remove and logs any error that is not ErrNotExist.
func removeOrLog(path string, stderr io.Writer) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(stderr, "avenor: cleanup %s: %v\n", path, err)
	}
}
