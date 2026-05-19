package mcpserver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
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
