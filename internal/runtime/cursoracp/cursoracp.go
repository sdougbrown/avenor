package cursoracp

import (
	"context"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

const backendID = "cursor-acp"

func New() runtime.Provider {
	return NewWithOptions(runtime.StartOptions{})
}

func NewWithOptions(opts runtime.StartOptions) runtime.Provider {
	return acp.NewProvider(acp.ProviderConfig{
		BackendID:             backendID,
		Bin:                   "agent",
		Args:                  []string{"acp"},
		SubprocessDiscovery:   false,
		AppendCWDArg:          false,
		Authenticate:          "cursor_login",
		ConfigureSession:      noopConfigureSession,
	})
}

// noopConfigureSession is a no-op because Cursor sets its model/config via the
// `agent` binary config, not via ACP session methods.
func noopConfigureSession(ctx context.Context, session *acp.Session, opts runtime.StartOptions) error {
	return nil
}
