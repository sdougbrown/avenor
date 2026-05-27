package pony

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/runtime/pony/tools"
)

// PonyConfig is the top-level pony configuration loaded from the JSON config file.
type PonyConfig struct {
	BaseURL      string             `json:"base_url"`
	APIKeyEnv    string             `json:"api_key_env"`
	DefaultAgent string             `json:"default_agent"`
	Profiles     map[string]Profile `json:"profiles"`
}

// Profile defines a named agent profile with its own model, prompts, and tool gating.
type Profile struct {
	Model          string      `json:"model"`
	SystemPrompt   string      `json:"system_prompt,omitempty"`
	InitialPrompt  string      `json:"initial_prompt,omitempty"`
	InjectAgentsMD bool        `json:"inject_agents_md,omitempty"`
	Tools          ToolGate    `json:"tools"`
	MaxTokens      int         `json:"max_tokens,omitempty"`
	ToolApproval   ToolApproval `json:"tool_approval,omitempty"`
	ShellConfig    *ShellConfig `json:"shell_config,omitempty"`
	BaseURL        string      `json:"base_url,omitempty"`
	APIKeyEnv      string      `json:"api_key_env,omitempty"`
	Skills         []string    `json:"skills,omitempty"`       // skill names to load
}

// ToolGate controls which tool groups are enabled for a profile.
type ToolGate struct {
	Local         bool `json:"local"`
	Orchestration bool `json:"orchestration"`
}

// ToolApproval maps tool names to whether they require approval before execution.
type ToolApproval map[string]bool

// ShellConfig overrides shell tool defaults per-profile.
// This is a reference to the tools.ShellConfig type, aliased for convenience.
type ShellConfig = tools.ShellConfig

// LoadPonyConfig reads and parses a JSON pony config file.
func LoadPonyConfig(path string) (*PonyConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load pony config: %w", err)
	}
	var cfg PonyConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse pony config: %w", err)
	}
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("pony config: base_url is required")
	}
	if cfg.APIKeyEnv == "" {
		return nil, fmt.Errorf("pony config: api_key_env is required")
	}
	if len(cfg.Profiles) == 0 {
		return nil, fmt.Errorf("pony config: at least one profile is required")
	}
	return &cfg, nil
}

// ResolveProfile looks up a profile by name, falling back to default_agent.
func (c *PonyConfig) ResolveProfile(name string) (*Profile, error) {
	if name == "" {
		name = c.DefaultAgent
	}
	if name == "" {
		// Sort keys for deterministic fallback
		keys := make([]string, 0, len(c.Profiles))
		for k := range c.Profiles {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			p := c.Profiles[k]
			if p.MaxTokens <= 0 {
				p.MaxTokens = 4096
			}
			return &p, nil
		}
	}
	p, ok := c.Profiles[name]
	if !ok {
		return nil, fmt.Errorf("pony config: unknown profile %q", name)
	}
	if p.MaxTokens <= 0 {
		p.MaxTokens = 4096
	}
	return &p, nil
}

// NewControlSocketExecutor creates an OrchestratorExecutor backed by a
// control socket client. This is used by the CLI to wire orchestration tools.
func NewControlSocketExecutor(socketPath string) (OrchestratorExecutor, error) {
	c, err := client.Dial(socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial control socket: %w", err)
	}
	return &controlSocketExecutor{client: c}, nil
}

type controlSocketExecutor struct {
	client *client.Client
}

func (e *controlSocketExecutor) SpawnAgent(ctx context.Context, params map[string]any) (string, error) {
	result, err := e.client.Spawn(params)
	if err != nil {
		return "", err
	}
	if sid, ok := result["session_id"].(string); ok && sid != "" {
		return sid, nil
	}
	if rid, ok := result["runtime_id"].(string); ok && rid != "" {
		return rid, nil
	}
	return "", fmt.Errorf("no session_id or runtime_id in spawn result")
}

func (e *controlSocketExecutor) SendPrompt(ctx context.Context, sessionID, prompt string) error {
	return e.client.Prompt(sessionID, prompt)
}

func (e *controlSocketExecutor) GetStatus(ctx context.Context, sessionID string) (map[string]any, error) {
	return e.client.Status(sessionID)
}

func (e *controlSocketExecutor) WaitForDone(ctx context.Context, sessionID string) error {
	eventsCh := e.client.Events()
	for {
		select {
		case evt, ok := <-eventsCh:
			if !ok {
				return fmt.Errorf("event channel closed while waiting for session %s", sessionID)
			}
			if evt.Event == "session.end" && (evt.RuntimeID == sessionID || evt.SessionID == sessionID) {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
