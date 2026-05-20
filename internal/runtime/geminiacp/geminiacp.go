package geminiacp

import (
	"context"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

const backendID = "gemini-acp"

func New() runtime.Provider {
	return NewWithOptions(runtime.StartOptions{})
}

func NewWithOptions(opts runtime.StartOptions) runtime.Provider {
	return acp.NewProvider(acp.ProviderConfig{
		BackendID:           backendID,
		Bin:                 "gemini",
		Args:                []string{"--acp", "--approval-mode", "default"},
		SubprocessDiscovery: false,
		ConfigureSession:    noopConfigureSession,
	})
}

// noopConfigureSession is a no-op because Gemini sets its model via the
// --model CLI flag at process spawn time, so per-session config is not
// currently supported.
func noopConfigureSession(ctx context.Context, session *acp.Session, opts runtime.StartOptions) error {
	return nil
}
