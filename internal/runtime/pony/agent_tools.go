package pony

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/sdougbrown/avenor/internal/runtime/pony/tools"
)

// newOrchestrationTools creates the four orchestration tools bound to the given executor.
func newOrchestrationTools(executor OrchestratorExecutor) []tools.Tool {
	return []tools.Tool{
		&spawnAgentTool{executor: executor},
		&sendPromptTool{executor: executor},
		&getStatusTool{executor: executor},
		&waitForDoneTool{executor: executor},
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
		rel, err := filepath.Rel(workingDir, input.Dir)
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

	sessionID, err := t.executor.SpawnAgent(ctx, params)
	if err != nil {
		return "", fmt.Errorf("spawn_agent: %w", err)
	}
	return fmt.Sprintf("Agent spawned with session ID: %s", sessionID), nil
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
			"prompt": {"type": "string", "description": "Follow-up prompt text"}
		},
		"required": ["session_id", "prompt"]
	}`)
}

type sendPromptInput struct {
	SessionID string `json:"session_id"`
	Prompt    string `json:"prompt"`
}

func (t *sendPromptTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input sendPromptInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("send_prompt: invalid args: %w", err)
	}
	if input.SessionID == "" || input.Prompt == "" {
		return "", fmt.Errorf("send_prompt: session_id and prompt are required")
	}
	if err := t.executor.SendPrompt(ctx, input.SessionID, input.Prompt); err != nil {
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
	if err := t.executor.WaitForDone(ctx, input.SessionID); err != nil {
		return "", fmt.Errorf("wait_for_done: %w", err)
	}
	return "Child agent completed.", nil
}
