package mcpserver

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/sdougbrown/avenor/internal/runstate"
)

type sentinelData struct {
	Status     string
	SessionID  string
	StopReason string
}

// statusRun is the typed shape of one run status in avenor_status structured
// output. Pointer fields preserve the pass-through presence semantics of the
// underlying status maps: a key is emitted only when the status source
// supplied it, including explicit empty strings such as phase_label.
type statusRun struct {
	RunID                *string        `json:"run_id,omitempty"`
	Label                *string        `json:"label,omitempty"`
	Status               *string        `json:"status,omitempty"`
	RuntimeID            *string        `json:"runtime_id,omitempty"`
	SessionID            *string        `json:"session_id,omitempty"`
	StopReason           *string        `json:"stop_reason,omitempty"`
	Phase                *string        `json:"phase,omitempty"`
	PhaseLabel           *string        `json:"phase_label,omitempty"`
	PendingPermission    *bool          `json:"pending_permission,omitempty"`
	Dir                  *string        `json:"dir,omitempty"`
	Thinking             *string        `json:"thinking,omitempty"`
	Backend              *string        `json:"backend,omitempty"`
	Agent                *string        `json:"agent,omitempty"`
	AgentProfile         *string        `json:"agent_profile,omitempty"`
	Model                *string        `json:"model,omitempty"`
	RosterFile           *string        `json:"roster_file,omitempty"`
	RosterEntry          *string        `json:"roster_entry,omitempty"`
	EffectiveBackend     *string        `json:"effective_backend,omitempty"`
	EffectiveAgent       *string        `json:"effective_agent,omitempty"`
	EffectiveModel       *string        `json:"effective_model,omitempty"`
	ParentID             *string        `json:"parent_id,omitempty"`
	Children             []string       `json:"children,omitempty"`
	EventPath            *string        `json:"event_path,omitempty"`
	Usage                map[string]any `json:"usage,omitempty"`
	LatestSeq            *int64         `json:"latest_seq,omitempty"`
	FinalOutput          *string        `json:"final_output,omitempty"`
	FinalOutputTruncated *bool          `json:"final_output_truncated,omitempty"`
	StartedAt            *int64         `json:"started_at,omitempty"`
	TimedOut             *bool          `json:"timed_out,omitempty"`
}

// statusToolOutput is the structured output of the avenor_status tool. The
// single-run form carries the status fields themselves; the list form (no
// run_id) carries runs and count. The embedded value keeps the two forms in
// one type so the SDK can derive the tool's output schema.
type statusToolOutput struct {
	statusRun
	Runs  []statusRun `json:"runs,omitempty"`
	Count *int        `json:"count,omitempty"`
}

// statusRunFromMap converts a translated status map into the typed output
// shape. Keys outside the statusRun fields are dropped, mirroring the
// TypeScript reference implementation's field allowlist.
func statusRunFromMap(m map[string]any) statusRun {
	var run statusRun
	if m == nil {
		return run
	}
	b, err := json.Marshal(m)
	if err != nil {
		return run
	}
	_ = json.Unmarshal(b, &run)
	return run
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

	for _, k := range []string{"runtime_id", "label", "dir", "phase", "phase_label", "pending_permission", "backend", "agent", "agent_profile", "model", "roster_file", "roster_entry", "effective_backend", "effective_agent", "effective_model", "parent_id", "children", "event_path", "usage", "latest_seq", "final_output", "final_output_truncated", "started_at"} {
		if v, ok := raw[k]; ok {
			result[k] = v
		}
	}

	rawStatus, _ := raw["status"].(string)
	rawSession, _ := raw["session_id"].(string)
	rawPhase, _ := raw["phase"].(string)
	translated := runstate.Translate(rawStatus, rawPhase)
	result["status"] = translated.Status

	switch rawStatus {
	case "running":
		// Keep this status running while active. session.end publishes a terminal
		// phase before the attempt defer clears child.active; retry selection can
		// still be pending. Team members share child.active, so this also covers
		// concurrent work.
		if translated.Phase != rawPhase {
			// Keep phase empty until deferred cleanup sets child.active false.
			result["phase"] = translated.Phase
			result["phase_label"] = ""
		}
		if rawSession != "" {
			result["session_id"] = rawSession
		}

	case "idle":
		if translated.TurnComplete {
			// Use raw metadata because a failed sentinel write can leave an earlier turn's file.
			if rawSession != "" {
				result["session_id"] = rawSession
			}
			if stopReason, ok := raw["stop_reason"]; ok {
				result["stop_reason"] = stopReason
			}
			return result
		}
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
		if rawSession != "" {
			result["session_id"] = rawSession
		}
	case "done", "failed", "timeout", "killed", "waiting":
		if rawSession != "" {
			result["session_id"] = rawSession
		}
		if stopReason, ok := raw["stop_reason"]; ok {
			result["stop_reason"] = stopReason
		}
	case "blocked":
		if rawSession != "" {
			result["session_id"] = rawSession
		}
	default:
		// Sentinels are an MCP registry fallback, not part of shared supervisor
		// state translation. Preserve the live session ID as authoritative.
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

func applyRunInfoIdentity(status map[string]any, info *RunInfo) {
	if info == nil {
		return
	}
	setIfMissing := func(key, value string) {
		if value == "" {
			return
		}
		// Field presence from live supervisor status is authoritative, including
		// an empty string that deliberately clears a prior workflow identity.
		// Registry metadata is fallback only when the supervisor omitted the key.
		if _, present := status[key]; !present {
			status[key] = value
		}
	}
	effectiveAgent := info.EffectiveAgent
	if effectiveAgent == "" {
		effectiveAgent = info.Agent
	}
	effectiveModel := info.EffectiveModel
	if effectiveModel == "" {
		effectiveModel = info.Model
	}
	effectiveBackend := info.EffectiveBackend
	if effectiveBackend == "" {
		effectiveBackend = info.Backend
	}
	setIfMissing("roster_file", info.RosterFile)
	setIfMissing("roster_entry", info.RosterEntry)
	setIfMissing("agent_profile", info.AgentProfile)
	setIfMissing("agent", effectiveAgent)
	setIfMissing("model", effectiveModel)
	setIfMissing("backend", effectiveBackend)
	setIfMissing("effective_agent", effectiveAgent)
	setIfMissing("effective_model", effectiveModel)
	setIfMissing("effective_backend", effectiveBackend)
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
