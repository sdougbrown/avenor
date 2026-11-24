// Package agy adapts the Antigravity CLI's ACP stdio server (`agy acp
// --stdio`) to the avenor runtime.Provider contract. Broker messaging rides on
// the standard ACP `mcpServers` session parameter, which `agy acp` spawns
// natively; model and thinking selection use the `model` and `thought_level`
// session config options.
package agy

import (
	"context"
	"fmt"
	"os"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

const backendID = "agy"

// transportEnvVar is the retired transport selector. `agy` now speaks ACP only,
// so a value here means the caller still expects a mode that no longer exists;
// failing loudly is better than silently running something else.
const transportEnvVar = "AVENOR_AGY_TRANSPORT"

// deps carries the process-level lookups the adapter performs. Tests inject
// them instead of mutating process globals, which keeps parallel tests
// race-clean.
type deps struct {
	getenv     func(string) string
	executable func() (string, error)
}

func defaultDeps() deps {
	return deps{getenv: os.Getenv, executable: os.Executable}
}

// New constructs an agy provider with no backend-scoped launch options.
func New() runtime.Provider {
	return NewWithOptions(runtime.StartOptions{})
}

// NewWithOptions constructs an agy provider whose launch defaults are opts.
func NewWithOptions(opts runtime.StartOptions) runtime.Provider {
	return newWithDeps(opts, defaultDeps())
}

func newWithDeps(opts runtime.StartOptions, d deps) runtime.Provider {
	if d.getenv == nil {
		d.getenv = os.Getenv
	}
	if d.executable == nil {
		d.executable = os.Executable
	}
	return acp.NewProvider(acp.ProviderConfig{
		BackendID:           backendID,
		Opts:                opts,
		Bin:                 "agy",
		SetCmdDir:           true,
		EmitSessionStart:    true,
		SubprocessDiscovery: false,
		AppendCWDArg:        false,
		ValidateStart: func(runtime.StartOptions) error {
			return validateTransportEnv(d.getenv)
		},
		BuildClientArgs: func(o runtime.StartOptions, absDir string) ([]string, error) {
			return buildClientArgs(o, absDir), nil
		},
		BuildSessionOpenOptions: func(_ runtime.StartOptions, runID, brokerToken, brokerURL string) (acp.SessionOpenOptions, error) {
			return buildSessionOpenOptions(d, runID, brokerToken, brokerURL)
		},
		ConfigureSession: configureSession,
	})
}

// validateTransportEnv rejects the retired AVENOR_AGY_TRANSPORT selector. Any
// non-empty raw value fails, whitespace included, because a caller who set it
// believes a mode is in effect that this backend no longer implements.
func validateTransportEnv(getenv func(string) string) error {
	raw := getenv(transportEnvVar)
	if raw == "" {
		return nil
	}
	return fmt.Errorf("AVENOR_AGY_TRANSPORT is no longer supported (got %q); the agy backend now uses ACP directly — unset AVENOR_AGY_TRANSPORT", raw)
}

// buildClientArgs renders the `agy acp` argv. The workspace is passed as
// --add-dir; the agent persona is a CLI flag because `agy acp` modes are
// permission modes, not agent personas. `--dangerously-skip-permissions` is
// never passed: permissions are answered over ACP.
func buildClientArgs(opts runtime.StartOptions, absDir string) []string {
	args := []string{"acp", "--stdio", "--add-dir", absDir}
	if opts.Agent != "" {
		args = append(args, "--agent", opts.Agent)
	}
	return args
}

// buildSessionOpenOptions adds the avenor channel-tools MCP server so the child
// can send and receive broker messages natively. Without broker credentials
// there is nothing to register, and the session opens with no MCP servers. The
// registration itself comes from runtime.ChannelToolsMCPFor, which the
// opencode-acp adapter renders into its own config shape; this function only
// maps it onto an ACP server entry.
func buildSessionOpenOptions(d deps, runID, brokerToken, brokerURL string) (acp.SessionOpenOptions, error) {
	if runID == "" || brokerToken == "" || brokerURL == "" {
		return acp.SessionOpenOptions{}, nil
	}
	bin, err := d.executable()
	if err != nil {
		return acp.SessionOpenOptions{}, fmt.Errorf("agy: resolve avenor executable for channel-tools: %w", err)
	}
	server, ok := runtime.ChannelToolsMCPFor(bin, runID, brokerToken, brokerURL)
	if !ok {
		return acp.SessionOpenOptions{}, nil
	}
	env := make([]acp.EnvVar, len(server.Env))
	for i, pair := range server.Env {
		env[i] = acp.EnvVar{Name: pair.Name, Value: pair.Value}
	}
	return acp.SessionOpenOptions{
		MCPServers: []acp.MCPServer{{
			Name:    server.Name,
			Command: server.Command,
			Args:    server.Args,
			Env:     env,
		}},
	}, nil
}

// configureSession applies the per-session model selection and the thinking
// level. The backend's ACP modes are permission modes, so opts.Agent is not
// applied here. The model is passed through unchanged: a suffixed model slug
// is the caller's choice, not an inference about effort. A model rejection
// stops the launch before the thought level is sent.
func configureSession(ctx context.Context, session *acp.Session, opts runtime.StartOptions) error {
	if opts.Model != "" {
		if err := session.Client.SetSessionModel(ctx, session.SessionID, opts.Model); err != nil {
			return err
		}
	}
	if opts.Thinking != "" {
		if err := session.Client.SetSessionConfigOption(ctx, session.SessionID, "thought_level", opts.Thinking); err != nil {
			return fmt.Errorf("%s thinking %q: %w", backendID, opts.Thinking, err)
		}
	}
	return nil
}
