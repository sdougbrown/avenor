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

func readEvents(path string, types []string, limit int) ([]map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read event log: %w", err)
	}
	defer f.Close()

	var matched []map[string]any
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
		return nil, fmt.Errorf("scan event log: %w", err)
	}

	if limit > 0 && len(matched) > limit {
		matched = matched[len(matched)-limit:]
	}

	if matched == nil {
		matched = []map[string]any{}
	}

	return matched, nil
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
