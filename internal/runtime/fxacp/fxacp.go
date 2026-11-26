package fxacp

import (
	"context"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

const backendID = "fx"

func New() runtime.Provider {
	return NewWithOptions(runtime.StartOptions{})
}

func NewWithOptions(opts runtime.StartOptions) runtime.Provider {
	return acp.NewProvider(acp.ProviderConfig{
		BackendID:           backendID,
		Opts:                opts,
		Bin:                 "fx",
		Args:                []string{"acp"},
		SubprocessDiscovery: false,
		ConfigureSession:    noopConfigureSession,
		// fx's ACP server can reject session/prompt with
		// InconsistentFinishReason after the model already streamed its reply;
		// tolerate it so basic turns complete (see ProviderConfig).
		TolerateInconsistentFinish: true,
	})
}

// noopConfigureSession is a no-op because fx sets its provider and model via
// the --provider and --model CLI flags at process spawn time, so per-session
// config is not supported.
func noopConfigureSession(ctx context.Context, session *acp.Session, opts runtime.StartOptions) error {
	return nil
}
