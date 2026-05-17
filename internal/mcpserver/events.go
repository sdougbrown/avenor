package mcpserver

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func readEvents(path string, types []string, limit int) ([]map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read event log: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var all []map[string]any
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}

		if len(types) > 0 {
			matched := false
			for _, t := range types {
				if et, ok := event["type"].(string); ok && et == t {
					matched = true
					break
				}
				if ev, ok := event["event"].(string); ok && ev == t {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		all = append(all, event)
	}

	if limit > 0 && len(all) > limit {
		all = all[len(all)-limit:]
	}

	return all, nil
}
