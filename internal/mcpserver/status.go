package mcpserver

import (
	"fmt"
	"os"
	"strings"
)

type sentinelData struct {
	Status     string
	SessionID  string
	StopReason string
}

func readSentinel(path string) (*sentinelData, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sentinel: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 || lines[0] == "" {
		return nil, fmt.Errorf("empty sentinel file")
	}
	sd := &sentinelData{Status: lines[0]}
	for _, line := range lines[1:] {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch k {
		case "SESSION":
			sd.SessionID = v
		case "STOP_REASON":
			sd.StopReason = v
		}
	}
	return sd, nil
}

func translateStatus(raw map[string]any, sentinelPath string) map[string]any {
	result := make(map[string]any)

	for _, k := range []string{"runtime_id", "label", "dir", "phase", "phase_label", "pending_permission", "backend", "agent", "model", "parent_id", "children", "event_path", "usage", "latest_seq", "final_output"} {
		if v, ok := raw[k]; ok {
			result[k] = v
		}
	}

	rawStatus, _ := raw["status"].(string)
	rawSession, _ := raw["session_id"].(string)

	switch rawStatus {
	case "idle", "running":
		result["status"] = "running"
		if rawSession != "" {
			result["session_id"] = rawSession
		}
	case "ended":
		if sentinelPath != "" {
			if sd, err := readSentinel(sentinelPath); err == nil {
				applySentinelStatus(result, sd)
				return result
			}
		}
		result["status"] = "done"
		if rawSession != "" {
			result["session_id"] = rawSession
		}
	case "done", "failed", "timeout", "killed", "waiting":
		result["status"] = rawStatus
		if rawSession != "" {
			result["session_id"] = rawSession
		}
		if stopReason, ok := raw["stop_reason"]; ok {
			result["stop_reason"] = stopReason
		}
	case "blocked":
		result["status"] = "failed"
		if rawSession != "" {
			result["session_id"] = rawSession
		}
	default:
		result["status"] = "running"
		if sentinelPath != "" {
			if sd, err := readSentinel(sentinelPath); err == nil {
				applySentinelStatus(result, sd)
			}
		}
		if rawSession != "" {
			result["session_id"] = rawSession
		}
	}

	return result
}

func readSentinelSession(path string) (string, error) {
	sd, err := readSentinel(path)
	if err != nil {
		return "", err // readSentinel already wraps with descriptive context
	}
	if sd.Status != "DONE" {
		return "", fmt.Errorf("run is not resumable (status: %s)", strings.ToLower(sd.Status))
	}
	if sd.SessionID == "" {
		return "", fmt.Errorf("no session in sentinel")
	}
	return sd.SessionID, nil
}

func applySentinelStatus(result map[string]any, sd *sentinelData) {
	switch sd.Status {
	case "DONE":
		result["status"] = "done"
	case "FAILED", "BLOCKED":
		result["status"] = "failed"
	case "TIMEOUT":
		result["status"] = "timeout"
	case "KILLED":
		result["status"] = "killed"
	default:
		result["status"] = strings.ToLower(sd.Status)
	}
	if sd.SessionID != "" {
		result["session_id"] = sd.SessionID
	}
	if sd.StopReason != "" {
		result["stop_reason"] = sd.StopReason
	}
}
