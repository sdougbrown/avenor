package mcpserver

import (
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
	RunID      *string `json:"run_id,omitempty"`
	Label      *string `json:"label,omitempty"`
	Status     *string `json:"status,omitempty"`
	RuntimeID  *string `json:"runtime_id,omitempty"`
	SessionID  *string `json:"session_id,omitempty"`
	StopReason *string `json:"stop_reason,omitempty"`
	Phase      *string `json:"phase,omitempty"`
	PhaseLabel *string `json:"phase_label,omitempty"`
	// PendingPermission is genuinely polymorphic on the wire (bool | record),
	// so `any` is intentional.
	PendingPermission    any            `json:"pending_permission,omitempty"`
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
	Permission           map[string]any `json:"permission,omitempty"`
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
	// Runs is a pointer so an empty list still emits "runs": [] — a plain
	// slice with omitempty would drop the key when len == 0.
	Runs  *[]statusRun `json:"runs,omitempty"`
	Count *int         `json:"count,omitempty"`
}

// statusRunFromMap converts a translated status map into the typed output
// shape. Keys outside the statusRun fields are dropped (mirroring the
// TypeScript reference's own field allowlist, whose field set differs
// slightly — it carries pid, this carries thinking/started_at/timed_out).
func statusRunFromMap(m map[string]any) statusRun {
	var run statusRun
	if m == nil {
		return run
	}
	run.RunID = stringPtr(m, "run_id")
	run.Label = stringPtr(m, "label")
	run.Status = stringPtr(m, "status")
	run.RuntimeID = stringPtr(m, "runtime_id")
	run.SessionID = stringPtr(m, "session_id")
	run.StopReason = stringPtr(m, "stop_reason")
	run.Phase = stringPtr(m, "phase")
	run.PhaseLabel = stringPtr(m, "phase_label")
	if v, ok := m["pending_permission"]; ok {
		run.PendingPermission = v
	}
	run.Dir = stringPtr(m, "dir")
	run.Thinking = stringPtr(m, "thinking")
	run.Backend = stringPtr(m, "backend")
	run.Agent = stringPtr(m, "agent")
	run.AgentProfile = stringPtr(m, "agent_profile")
	run.Model = stringPtr(m, "model")
	run.RosterFile = stringPtr(m, "roster_file")
	run.RosterEntry = stringPtr(m, "roster_entry")
	run.EffectiveBackend = stringPtr(m, "effective_backend")
	run.EffectiveAgent = stringPtr(m, "effective_agent")
	run.EffectiveModel = stringPtr(m, "effective_model")
	run.ParentID = stringPtr(m, "parent_id")
	run.Children = childrenOf(m)
	run.EventPath = stringPtr(m, "event_path")
	run.Usage = usageOf(m)
	run.Permission = recordOf(m, "permission")
	run.LatestSeq = int64Ptr(m, "latest_seq")
	run.FinalOutput = stringPtr(m, "final_output")
	run.FinalOutputTruncated = boolPtr(m, "final_output_truncated")
	run.StartedAt = int64Ptr(m, "started_at")
	run.TimedOut = boolPtr(m, "timed_out")
	return run
}

func stringPtr(m map[string]any, key string) *string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return &s
		}
	}
	return nil
}

func boolPtr(m map[string]any, key string) *bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return &b
		}
	}
	return nil
}

func int64Ptr(m map[string]any, key string) *int64 {
	if v, ok := m[key]; ok {
		if f, ok := v.(float64); ok {
			n := int64(f)
			return &n
		}
	}
	return nil
}

func childrenOf(m map[string]any) []string {
	v, ok := m["children"]
	if !ok {
		return nil
	}
	switch c := v.(type) {
	case []string:
		return c
	case []any:
		result := make([]string, 0, len(c))
		for _, item := range c {
			s, ok := item.(string)
			if !ok {
				return nil
			}
			result = append(result, s)
		}
		return result
	}
	return nil
}

func usageOf(m map[string]any) map[string]any {
	return recordOf(m, "usage")
}

// recordOf passes through a free-form record field (usage, permission). The
// permission record carries provider-dependent request details, so a closed
// struct would drop keys the wire may carry.
func recordOf(m map[string]any, key string) map[string]any {
	if v, ok := m[key]; ok {
		if u, ok := v.(map[string]any); ok {
			return u
		}
	}
	return nil
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

	for _, k := range []string{"runtime_id", "label", "dir", "phase", "phase_label", "pending_permission", "permission", "backend", "agent", "agent_profile", "model", "roster_file", "roster_entry", "effective_backend", "effective_agent", "effective_model", "parent_id", "children", "event_path", "usage", "latest_seq", "final_output", "final_output_truncated", "started_at"} {
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
