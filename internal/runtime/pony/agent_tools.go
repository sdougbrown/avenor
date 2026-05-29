package pony

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/sdougbrown/avenor/internal/runtime/pony/tools"
)

// newOrchestrationTools creates the orchestration tools bound to the given executor.
func newOrchestrationTools(executor OrchestratorExecutor) []tools.Tool {
	return []tools.Tool{
		&spawnAgentTool{executor: executor},
		&spawnAgentsTool{executor: executor},
		&sendPromptTool{executor: executor},
		&getStatusTool{executor: executor},
		&waitForDoneTool{executor: executor},
		&sendToParentTool{executor: executor},
	}
}

// spawnAgentTool creates a new child agent session.
type spawnAgentTool struct {
	executor OrchestratorExecutor
}

func (t *spawnAgentTool) Name() string { return "spawn_agent" }
func (t *spawnAgentTool) Description() string {
	return "Create a new child agent session. Returns the session ID of the spawned agent. Pass parameters like backend, model, and prompt to configure the child."
}

// spawnAgentInput matches the structure of control-socket spawn params.
type spawnAgentInput struct {
	Backend string `json:"backend,omitempty"`
	Model   string `json:"model,omitempty"`
	Prompt  string `json:"prompt,omitempty"`
	Dir     string `json:"dir,omitempty"`
	Label   string `json:"label,omitempty"`
}

func (t *spawnAgentTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"backend": {"type": "string", "description": "Agent backend to use (opencode-acp, codex-app-server, pony, etc.)"},
			"model": {"type": "string", "description": "Model to use for the child agent"},
			"prompt": {"type": "string", "description": "Initial prompt for the child agent"},
			"dir": {"type": "string", "description": "Working directory for the child agent"},
			"label": {"type": "string", "description": "Human-readable label for this child agent"}
		},
		"required": ["prompt"]
	}`)
}

func (t *spawnAgentTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input spawnAgentInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("spawn_agent: invalid args: %w", err)
	}
	if input.Prompt == "" {
		return "", fmt.Errorf("spawn_agent: prompt is required")
	}

	if input.Dir != "" {
		baseAbs, err := filepath.Abs(workingDir)
		if err != nil {
			return "", fmt.Errorf("spawn_agent: resolve working directory: %w", err)
		}
		dirAbs, err := filepath.Abs(input.Dir)
		if err != nil {
			return "", fmt.Errorf("spawn_agent: resolve dir: %w", err)
		}
		rel, err := filepath.Rel(baseAbs, dirAbs)
		if err != nil || strings.HasPrefix(rel, "..") {
			return "", fmt.Errorf("spawn_agent: dir %q is outside the working directory %q", input.Dir, workingDir)
		}
	}

	params := map[string]any{
		"prompt": input.Prompt,
	}
	if input.Backend != "" {
		params["backend"] = input.Backend
	}
	if input.Model != "" {
		params["model"] = input.Model
	}
	if input.Dir != "" {
		params["dir"] = input.Dir
	} else {
		params["dir"] = workingDir
	}
	if input.Label != "" {
		params["label"] = input.Label
	}
	// Include parent's runtime ID for parent-child tracking.
	if runtimeID, ok := ctx.Value(RuntimeIDKey).(string); ok && runtimeID != "" {
		params["runtime_id"] = runtimeID
	}

	sessionID, err := t.executor.SpawnAgent(ctx, params)
	if err != nil {
		return "", fmt.Errorf("spawn_agent: %w", err)
	}
	return fmt.Sprintf("Agent spawned with session ID: %s", sessionID), nil
}

// spawnAgentsTool creates multiple child agent sessions in one call.
type spawnAgentsTool struct {
	executor OrchestratorExecutor
}

func (t *spawnAgentsTool) Name() string { return "spawn_agents" }
func (t *spawnAgentsTool) Description() string {
	return "Create multiple child agent sessions in a single call. All agents run concurrently. Returns session IDs with labels for each spawned agent."
}

type spawnAgentsInput struct {
	Agents []struct {
		Backend string `json:"backend,omitempty"`
		Model   string `json:"model,omitempty"`
		Prompt  string `json:"prompt"`
		Dir     string `json:"dir,omitempty"`
		Label   string `json:"label,omitempty"`
	} `json:"agents"`
}

func (t *spawnAgentsTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"agents": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"backend": {"type": "string", "description": "Agent backend to use"},
						"model": {"type": "string", "description": "Model to use for the child agent"},
						"prompt": {"type": "string", "description": "Initial prompt for the child agent"},
						"dir": {"type": "string", "description": "Working directory for the child agent"},
						"label": {"type": "string", "description": "Human-readable label"}
					},
					"required": ["prompt"]
				},
				"description": "List of agents to spawn"
			}
		},
		"required": ["agents"]
	}`)
}

func (t *spawnAgentsTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input spawnAgentsInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("spawn_agents: invalid args: %w", err)
	}
	if len(input.Agents) == 0 {
		return "", fmt.Errorf("spawn_agents: at least one agent is required")
	}
	baseAbs, err := filepath.Abs(workingDir)
	if err != nil {
		return "", fmt.Errorf("spawn_agents: resolve working directory: %w", err)
	}

	var parts []string
	for _, agent := range input.Agents {
		if agent.Prompt == "" {
			return "", fmt.Errorf("spawn_agents: prompt is required for each agent")
		}
		if agent.Dir != "" {
			dirAbs, err := filepath.Abs(agent.Dir)
			if err != nil {
				return "", fmt.Errorf("spawn_agents: resolve dir: %w", err)
			}
			rel, err := filepath.Rel(baseAbs, dirAbs)
			if err != nil || strings.HasPrefix(rel, "..") {
				return "", fmt.Errorf("spawn_agents: dir %q is outside the working directory %q", agent.Dir, workingDir)
			}
		}

		params := map[string]any{
			"prompt": agent.Prompt,
		}
		if agent.Backend != "" {
			params["backend"] = agent.Backend
		}
		if agent.Model != "" {
			params["model"] = agent.Model
		}
		if agent.Dir != "" {
			params["dir"] = agent.Dir
		} else {
			params["dir"] = workingDir
		}
		if agent.Label != "" {
			params["label"] = agent.Label
		}
		// Include parent's runtime ID for parent-child tracking.
		if runtimeID, ok := ctx.Value(RuntimeIDKey).(string); ok && runtimeID != "" {
			params["runtime_id"] = runtimeID
		}

		sessionID, err := t.executor.SpawnAgent(ctx, params)
		if err != nil {
			return "", fmt.Errorf("spawn_agents: %w", err)
		}
		desc := sessionID
		if agent.Label != "" {
			desc = sessionID + "=" + agent.Label
		}
		parts = append(parts, desc)
	}

	return "Spawned agents: [" + strings.Join(parts, ", ") + "]", nil
}

// sendPromptTool sends a follow-up prompt to an existing child agent.
type sendPromptTool struct {
	executor OrchestratorExecutor
}

func (t *sendPromptTool) Name() string { return "send_prompt" }
func (t *sendPromptTool) Description() string {
	return "Send a follow-up prompt to an existing child agent session. Use the session ID returned by spawn_agent."
}
func (t *sendPromptTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"session_id": {"type": "string", "description": "Session ID of the child agent"},
			"prompt": {"type": "string", "description": "Follow-up prompt text"},
			"request_id": {"type": "string", "description": "Optional child.question request ID for duplicate-suppression"}
		},
		"required": ["session_id", "prompt"]
	}`)
}

type sendPromptInput struct {
	SessionID string `json:"session_id"`
	Prompt    string `json:"prompt"`
	RequestID string `json:"request_id,omitempty"`
}

func (t *sendPromptTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input sendPromptInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("send_prompt: invalid args: %w", err)
	}
	if input.SessionID == "" || input.Prompt == "" {
		return "", fmt.Errorf("send_prompt: session_id and prompt are required")
	}
	if err := t.executor.SendPrompt(ctx, input.SessionID, input.Prompt, input.RequestID); err != nil {
		return "", fmt.Errorf("send_prompt: %w", err)
	}
	return "Prompt sent.", nil
}

// getStatusTool gets the current status of a child agent.
type getStatusTool struct {
	executor OrchestratorExecutor
}

func (t *getStatusTool) Name() string { return "get_status" }
func (t *getStatusTool) Description() string {
	return "Get the current status of a child agent session. Returns phase, state, and other status information."
}
func (t *getStatusTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"session_id": {"type": "string", "description": "Session ID of the child agent"}
		},
		"required": ["session_id"]
	}`)
}

type getStatusInput struct {
	SessionID string `json:"session_id"`
}

func (t *getStatusTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input getStatusInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("get_status: invalid args: %w", err)
	}
	if input.SessionID == "" {
		return "", fmt.Errorf("get_status: session_id is required")
	}
	status, err := t.executor.GetStatus(ctx, input.SessionID)
	if err != nil {
		return "", fmt.Errorf("get_status: %w", err)
	}
	statusJSON, err := json.Marshal(status)
	if err != nil {
		return "", fmt.Errorf("get_status: marshal result: %w", err)
	}
	return string(statusJSON), nil
}

// waitForDoneTool blocks until a child agent session completes.
type waitForDoneTool struct {
	executor OrchestratorExecutor
}

func (t *waitForDoneTool) Name() string { return "wait_for_done" }
func (t *waitForDoneTool) Description() string {
	return "Block until a child agent session completes. Use this after spawn_agent to wait for the result. This tool may take a long time."
}
func (t *waitForDoneTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"session_id": {"type": "string", "description": "Session ID of the child agent to wait for"}
		},
		"required": ["session_id"]
	}`)
}

type waitForDoneInput struct {
	SessionID string `json:"session_id"`
}

func (t *waitForDoneTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input waitForDoneInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("wait_for_done: invalid args: %w", err)
	}
	if input.SessionID == "" {
		return "", fmt.Errorf("wait_for_done: session_id is required")
	}
	result, err := t.executor.WaitForDone(ctx, input.SessionID)
	if err != nil {
		return "", fmt.Errorf("wait_for_done: %w", err)
	}
	return fmt.Sprintf("Agent %s completed (exit %d, %s). Output files: %v",
		input.SessionID, result.ExitCode, result.StopReason, result.OutputFiles), nil
}

// sendToParentTool allows a child agent to send a message back to its parent.
type sendToParentTool struct {
	executor OrchestratorExecutor
}

func (t *sendToParentTool) Name() string { return "send_to_parent" }
func (t *sendToParentTool) Description() string {
	return "Send a message from a child agent back to its parent agent. The parent will see this as a child.question event."
}
func (t *sendToParentTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"message": {"type": "string", "description": "The message to send to the parent agent"}
		},
		"required": ["message"]
	}`)
}

func (t *sendToParentTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("send_to_parent: invalid args: %w", err)
	}
	if input.Message == "" {
		return "", fmt.Errorf("send_to_parent: message is required")
	}
	// Read the runtime ID from context; it was injected by the provider
	// during Prompt. When empty (e.g., sessions without runtime context),
	// the control server handler will validate and return a clear error.
	runtimeID, _ := ctx.Value(RuntimeIDKey).(string)
	if runtimeID == "" {
		return "", fmt.Errorf("send_to_parent: runtime context is required")
	}
	if err := t.executor.SendToParent(ctx, runtimeID, input.Message); err != nil {
		return "", fmt.Errorf("send_to_parent: %w", err)
	}
	return "Message sent to parent agent.", nil
}
