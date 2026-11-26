package dshacp

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

const backendID = "dsh-acp"

func New() runtime.Provider {
	return NewWithOptions(runtime.StartOptions{})
}

// NewWithOptions builds the DeepSeek Harness backend. DSH ships an ACP server
// behind its `acp` profile template (`dsh --profile acp`), which advertises
// session new/list/resume/close, model and reasoning-effort config options, and
// session/request_permission, so the shared ACP provider drives it unchanged.
//
// DSH resolves its own credentials and provider; a launch with none succeeds
// and fails at the first prompt, which the run reports as a normal prompt
// failure rather than a spawn error. That ordering is documented in
// docs/backends.md.
func NewWithOptions(opts runtime.StartOptions) runtime.Provider {
	return acp.NewProvider(acp.ProviderConfig{
		BackendID:           backendID,
		Opts:                opts,
		Bin:                 "dsh",
		Args:                []string{"--profile", "acp"},
		SubprocessDiscovery: false,
		AppendCWDArg:        false,
		Authenticate:        "",
		ConfigureSession:    configureSession,
		ModelSelection:      true,
	})
}

// configureSession applies the per-session model. DSH reports the model as a
// session config option, so an explicit model wins over the profile's pinned
// default. The agent field is not applied: DSH's ACP modes are permission
// modes, not agent names.
func configureSession(ctx context.Context, session *acp.Session, opts runtime.StartOptions) error {
	if opts.Model == "" {
		return nil
	}
	return session.Client.SetSessionModel(ctx, session.SessionID, dshModelValue(opts.Model))
}

// dshModelValue converts a caller's model reference to the value DSH's ACP
// server accepts. That server encodes a model as a JSON [provider, model]
// pair, which is not something a caller should have to write, so a
// `provider/model` reference is converted and an already-encoded value passes
// through unchanged. A bare model id is forwarded as-is: DSH rejects it with a
// clear unknown-model error rather than guessing a provider.
func dshModelValue(model string) string {
	trimmed := strings.TrimSpace(model)
	if strings.HasPrefix(trimmed, "[") {
		return trimmed
	}
	provider, id, ok := strings.Cut(trimmed, "/")
	if !ok || provider == "" || id == "" {
		return trimmed
	}
	encoded, err := json.Marshal([]string{provider, id})
	if err != nil {
		return trimmed
	}
	return string(encoded)
}
