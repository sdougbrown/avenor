package fxacp

import (
	"context"
	"strings"

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
		SubprocessDiscovery: false,
		ConfigureSession:    noopConfigureSession,
		PromptFailedPolicy:  PromptFailedPolicy(),
		BuildClientArgs:     buildClientArgs,
		ModelInLaunch:       true,
		ModelSelection:      true,
	})
}

// buildClientArgs renders the launch argv per run. fx takes its provider and
// model at process spawn time — its ACP server implements no session-level
// config — so --model rides the launch and the model is part of the launch
// identity (ModelInLaunch), never reused across models by a warm client.
func buildClientArgs(opts runtime.StartOptions, _ string) ([]string, error) {
	args := []string{"acp"}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	return args, nil
}

// noopConfigureSession is a no-op because fx sets its provider and model via
// the --provider and --model CLI flags at process spawn time, so per-session
// config is not supported.
func noopConfigureSession(ctx context.Context, session *acp.Session, opts runtime.StartOptions) error {
	return nil
}

// PromptFailedPolicy is fx's turn-finish carve-out, not a pattern to build on:
// fx's session/prompt can reject with InconsistentFinishReason after the model
// already streamed its reply (its own finish-reason bookkeeping disagrees with
// itself). When the turn streamed reply content, the shared provider treats
// that error as a completed turn with a warning; without streamed content, and
// for every other error, the failure stands.
func PromptFailedPolicy() acp.PromptFailedPolicy {
	return func(session *acp.Session, err error) bool {
		return err != nil && strings.Contains(err.Error(), "InconsistentFinishReason") && session.StreamedContent()
	}
}
