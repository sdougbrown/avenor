package mcpserver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sdougbrown/avenor/internal/events"
)

func readEvents(path string, types []string, limit int, afterSeq *int64) ([]map[string]any, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			if afterSeq != nil {
				return []map[string]any{}, *afterSeq, nil
			}
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("read event log: %w", err)
	}
	defer f.Close()

	var matched []map[string]any
	var fileMaxSeq int64
	scanner := bufio.NewScanner(f)
	const maxCapacity = 1024 * 1024 // 1 MiB
	buf := make([]byte, 4096)
	scanner.Buffer(buf, maxCapacity)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}

		// fileMaxSeq is the safe resume point: the highest seq anywhere in the
		// file, regardless of the type filter.
		if seq, ok := parseSeq(event); ok && seq > fileMaxSeq {
			fileMaxSeq = seq
		}

		if len(types) > 0 {
			found := false
			for _, t := range types {
				if et, ok := event["type"].(string); ok && et == t {
					found = true
					break
				}
				if ev, ok := event["event"].(string); ok && ev == t {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		// A cursor (including 0) resumes the stream: keep only events with a seq
		// greater than the cursor. Events without a seq are dropped — they
		// cannot be positioned on the sequence.
		if afterSeq != nil {
			seq, ok := parseSeq(event)
			if !ok || seq <= *afterSeq {
				continue
			}
		}
		// avenor_events is an attachment/display surface. Durable NDJSON keeps
		// the original reply, while this presentation copy remains bounded.
		if finalOutput, _ := event["final_output"].(string); finalOutput != "" {
			preview := events.BoundedFinalOutput(finalOutput)
			event["final_output"] = preview
			if preview != finalOutput {
				event["final_output_truncated"] = true
			}
		}
		matched = append(matched, event)
	}

	if err := scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("scan event log: %w", err)
	}

	if afterSeq != nil {
		// Cursor mode pages oldest-first: take the first limit matches.
		if limit > 0 && len(matched) > limit {
			matched = matched[:limit]
		}
	} else if limit > 0 && len(matched) > limit {
		matched = matched[len(matched)-limit:]
	}

	// latestSeq is the highest seq among the returned events. With a cursor,
	// an empty page returns the cursor itself so the caller can stop; without
	// one, a page of seq-less events (or an empty page) falls back to
	// fileMaxSeq — the safe resume point, 0 for a file with no sequence
	// numbers.
	var latestSeq int64
	var returnedHasSeq bool
	if afterSeq != nil && len(matched) == 0 {
		latestSeq = *afterSeq
	}
	for _, e := range matched {
		if seq, ok := parseSeq(e); ok {
			returnedHasSeq = true
			if seq > latestSeq {
				latestSeq = seq
			}
		}
	}
	if !returnedHasSeq && afterSeq == nil {
		latestSeq = fileMaxSeq
	}

	if matched == nil {
		matched = []map[string]any{}
	}

	return matched, latestSeq, nil
}

// parseSeq extracts an event's sequence number. JSON numbers decode to
// float64; the value is integral by construction.
func parseSeq(event map[string]any) (int64, bool) {
	seq, ok := event["seq"].(float64)
	if !ok {
		return 0, false
	}
	return int64(seq), true
}

// readFinalOutput scans a durable log without Scanner's token ceiling. It is
// used only by the explicit result fallback, never by an inspector or event
// attachment path, so a large terminal reply is returned exactly.
func readFinalOutput(path string) (string, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("read event log: %w", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var output string
	var found bool
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(strings.TrimSpace(string(line))) > 0 {
			var event map[string]any
			if err := json.Unmarshal(line, &event); err == nil {
				if name, _ := event["event"].(string); name == "session.end" {
					if text, ok := event["final_output"].(string); ok {
						output = text
						found = true
					}
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return output, found, nil
			}
			return "", false, fmt.Errorf("read event log: %w", readErr)
		}
	}
}
