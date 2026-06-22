package opencodeacp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

const backendID = "opencode-acp"

var ProbePrompt = acp.ProbePrompt

func New() runtime.Provider {
	return NewWithOptions(runtime.StartOptions{})
}

func NewWithOptions(opts runtime.StartOptions) runtime.Provider {
	return acp.NewProvider(acp.ProviderConfig{
		BackendID:           backendID,
		Bin:                 "opencode",
		Args:                []string{"acp", "--pure", "--log-level", "WARN"},
		SubprocessDiscovery: true,
		AppendCWDArg:        true,
		BuildClientEnv:      buildClientEnv,
		ConfigureSession: func(ctx context.Context, session *acp.Session, opts runtime.StartOptions) error {
			return configureSession(ctx, session.Client, session.SessionID, opts)
		},
	})
}

func buildClientEnv(opts runtime.StartOptions, runID, brokerToken, brokerAddr string) (map[string]string, func() error) {
	hasOverrides := false

	// Inject the avenor-channel-tools MCP server when broker info is available.
	// Without a broker (e.g. CLI-only runs), skip the MCP injection — the mode
	// override below is the only thing needed.
	mcpOverride := map[string]any{}
	if runID != "" && brokerToken != "" && brokerAddr != "" {
		avenorBin, err := os.Executable()
		if err != nil {
			return nil, nil
		}
		mergeConfig(mcpOverride, map[string]any{
			"mcp": map[string]any{
				"avenor-channel-tools": map[string]any{
					"type":    "local",
					"enabled": true,
					"command": []string{avenorBin, "channel-tools", "--run-id", runID, "--token", brokerToken, "--broker-url", fmt.Sprintf("http://%s", brokerAddr)},
				},
			},
			"tools": map[string]any{
				"avenor-channel-tools*": true,
			},
		})
		hasOverrides = true
	}

	// opencode's ACP filters subagent-mode agents out of the selectable
	// modes list, causing set_mode to reject them. Override the agent's
	// mode to "all" in the per-run config so it passes the filter. The deep
	// merge preserves the agent's prompt, model, permissions, and tool
	// config from the global config — only the mode field is flipped.
	modeOverride := map[string]any{}
	if opts.Agent != "" {
		if mode := readAgentMode(opts.Agent, os.Getenv); mode == "subagent" {
			mergeConfig(modeOverride, map[string]any{
				"agent": map[string]any{
					opts.Agent: map[string]any{
						"mode": "all",
					},
				},
			})
			hasOverrides = true
		}
	}

	// If there's nothing to override, don't write a config file — let the
	// child process use the global config as-is.
	if !hasOverrides {
		return nil, nil
	}

	// Merge existing OPENCODE_CONFIG / OPENCODE_CONFIG_CONTENT so we layer
	// on top of any caller-provided config rather than replacing it.
	config := map[string]any{}
	mergeConfigFile(config, os.Getenv("OPENCODE_CONFIG"))
	mergeConfigContent(config, os.Getenv("OPENCODE_CONFIG_CONTENT"))
	mergeConfig(config, mcpOverride)
	mergeConfig(config, modeOverride)

	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, nil
	}

	// Write per-run config to a temp file. OPENCODE_CONFIG_CONTENT does not
	// reliably register MCP servers in opencode acp; file-based config does.
	cfgPath := filepath.Join(os.TempDir(), "avenor-opencode-acp-"+runIDWithFallback(runID)+".json")
	if err := os.WriteFile(cfgPath, encoded, 0o600); err != nil {
		return nil, nil
	}
	return map[string]string{"OPENCODE_CONFIG": cfgPath}, func() error { return os.Remove(cfgPath) }
}

func runIDWithFallback(runID string) string {
	if runID != "" {
		return runID
	}
	return uuid.New().String()
}

func mergeConfigFile(dst map[string]any, path string) {
	if path == "" {
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	mergeConfigContent(dst, string(raw))
}

func mergeConfigContent(dst map[string]any, raw string) {
	if raw == "" {
		return
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return
	}
	mergeConfig(dst, parsed)
}

func mergeConfig(dst, src map[string]any) {
	for key, value := range src {
		srcMap, srcOK := value.(map[string]any)
		dstMap, dstOK := dst[key].(map[string]any)
		if srcOK && dstOK {
			mergeConfig(dstMap, srcMap)
			continue
		}
		dst[key] = value
	}
}

// readAgentMode resolves the declared mode for the given agent name from the
// opencode config. Returns "" if the agent is not found or has no mode set.
// Errors are silently treated as not-found — a missing or malformed config
// should not block the spawn (configureSession will surface the set_mode
// failure if the agent genuinely can't be selected).
func readAgentMode(agentName string, getenv func(string) string) string {
	for _, path := range opencodeConfigPaths(getenv) {
		mode := readAgentModeFromFile(path, agentName)
		if mode != "" {
			return mode
		}
	}
	return ""
}

func opencodeConfigPaths(getenv func(string) string) []string {
	if getenv == nil {
		getenv = os.Getenv
	}
	var paths []string
	// OPENCODE_CONFIG overlays the default config in opencode, so check both.
	// The explicit file may not exist (e.g. set to a local override path that
	// hasn't been created), so always include the default paths as a fallback.
	if path := getenv("OPENCODE_CONFIG"); path != "" {
		paths = append(paths, path)
	}
	if dir := getenv("OPENCODE_CONFIG_DIR"); dir != "" {
		paths = append(paths, filepath.Join(dir, "opencode.json"), filepath.Join(dir, "opencode.jsonc"))
	} else if xdg := getenv("XDG_CONFIG_HOME"); xdg != "" {
		dir := filepath.Join(xdg, "opencode")
		paths = append(paths, filepath.Join(dir, "opencode.json"), filepath.Join(dir, "opencode.jsonc"))
	} else {
		home, err := os.UserHomeDir()
		if err == nil {
			dir := filepath.Join(home, ".config", "opencode")
			paths = append(paths, filepath.Join(dir, "opencode.json"), filepath.Join(dir, "opencode.jsonc"))
		}
	}
	return paths
}

func readAgentModeFromFile(path, agentName string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var config struct {
		Agent map[string]struct {
			Mode string `json:"mode"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return ""
	}
	if agent, ok := config.Agent[agentName]; ok {
		return agent.Mode
	}
	return ""
}

// sessionConfigurer is the subset of *acp.Client needed for session configuration.
type sessionConfigurer interface {
	SetSessionMode(ctx context.Context, sessionID, modeID string) error
	SetSessionModel(ctx context.Context, sessionID, modelID string) error
}

func configureSession(ctx context.Context, cfg sessionConfigurer, sessionID string, opts runtime.StartOptions) error {
	if opts.Model != "" {
		if err := cfg.SetSessionModel(ctx, sessionID, opts.Model); err != nil {
			return err
		}
	}
	if opts.Agent != "" {
		if err := cfg.SetSessionMode(ctx, sessionID, opts.Agent); err != nil {
			return err
		}
	}
	return nil
}

func opencodeAvailable() bool {
	_, err := exec.LookPath("opencode")
	return err == nil
}

func RunProbe(ctx context.Context, dir string) ([]events.Event, error) {
	return RunProbeWithPrompt(ctx, dir, ProbePrompt)
}

func RunProbeWithPrompt(ctx context.Context, dir, prompt string) ([]events.Event, error) {
	client, err := acp.NewClient(ctx, acp.ClientConfig{
		Bin:                  "opencode",
		Args:                 []string{"acp", "--pure", "--log-level", "WARN"},
		Dir:                  dir,
		AutoAnswerPermission: true,
		AppendCWDArg:         true,
	})
	if err != nil {
		return nil, err
	}
	defer client.Close()

	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := client.Initialize(initCtx); err != nil {
		return nil, err
	}

	session, err := client.NewSession(initCtx)
	if err != nil {
		return nil, err
	}

	eventsCh := client.Events()
	promptDone := make(chan struct {
		event events.Event
		err   error
	}, 1)
	go func() {
		event, err := session.Prompt(ctx, prompt)
		promptDone <- struct {
			event events.Event
			err   error
		}{event: event, err: err}
	}()

	var transcript []events.Event
	for {
		select {
		case event, ok := <-eventsCh:
			if ok {
				transcript = append(transcript, event)
			}
		case result := <-promptDone:
			if result.err != nil {
				return transcript, result.err
			}
			transcript = append(transcript, result.event)
			return transcript, nil
		case <-ctx.Done():
			_ = session.Cancel()
			return transcript, ctx.Err()
		}
	}
}
