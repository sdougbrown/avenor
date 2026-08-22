package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
)

// replayEvents replays events from an NDJSON log beyond the snapshot's applied
// revision. It returns the advanced snapshot, the number of events replayed,
// a non-zero byte offset if a malformed trailing event was truncated to the
// last good line, and any error. An event that fails to reduce is a replay
// error and aborts without truncation.
func replayEvents(snap Snapshot, path string) (Snapshot, int, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return snap, 0, 0, nil
		}
		return snap, 0, 0, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return snap, 0, 0, nil
	}

	workflowID := string(snap.Instance.WorkflowID)
	lines := splitLines(data)
	next := snap
	count := 0
	var lastGoodEnd int64

	for i, line := range lines {
		end := line.end
		lineData := data[line.start:line.end]
		var e Event
		if err := json.Unmarshal(lineData, &e); err == nil {
			var rerr error
			next, rerr = Reduce(next, e)
			if rerr != nil {
				return next, count, 0, rerr
			}
			count++
			lastGoodEnd = end
			continue
		}
		// Unmarshal failed. A trailing malformed final line is an incomplete
		// write; truncate it away. Anything earlier is real corruption.
		if i == len(lines)-1 {
			log.Printf("workflow %s: truncating incomplete final event at byte %d", workflowID, lastGoodEnd)
			if err := os.Truncate(path, lastGoodEnd); err != nil {
				return next, count, 0, err
			}
			return next, count, lastGoodEnd, nil
		}
		return next, count, 0, fmt.Errorf("workflow %s event log corrupted at line %d", workflowID, i+1)
	}

	return next, count, 0, nil
}

type lineSpan struct {
	start int64
	end   int64
}

// splitLines splits data on '\n', recording each line's byte end offset (just
// after the newline, or len(data) for a final line with no trailing newline).
func splitLines(data []byte) []lineSpan {
	var lines []lineSpan
	start := int64(0)
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, lineSpan{start: start, end: int64(i) + 1})
			start = int64(i) + 1
		}
	}
	if start < int64(len(data)) {
		lines = append(lines, lineSpan{start: start, end: int64(len(data))})
	}
	return lines
}
