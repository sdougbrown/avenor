// Package factory constructs runtime.Provider instances by backend name.
//
// Supported backends:
//   - opencode-acp: spawns opencode acp subprocess, owns lifecycle.
//   - opencode-http: talks to a running opencode serve instance over HTTP/SSE.
//   - codex-app-server: spawns Codex app-server subprocess via JSON-RPC over stdio.
//   - gemini-acp: spawns gemini --acp subprocess, owns lifecycle.
//   - cursor-acp: spawns agent acp subprocess, owns lifecycle.
//   - pi: spawns pi --mode rpc subprocess, owns lifecycle.
//   - pony: test/stub backend.
//   - claude: channel-less Claude Code backend; PTY by default, AVENOR_CLAUDE_TERMINAL=tmux for tmux.
//   - claude-channel: broker-channel Claude Code backend; requires tmux; uses --dangerously-load-development-channels.
package factory

import (
	"fmt"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/agy"
	"github.com/sdougbrown/avenor/internal/runtime/claude"
	"github.com/sdougbrown/avenor/internal/runtime/claudechannel"
	"github.com/sdougbrown/avenor/internal/runtime/codexappserver"
	"github.com/sdougbrown/avenor/internal/runtime/cursoracp"
	"github.com/sdougbrown/avenor/internal/runtime/geminiacp"
	"github.com/sdougbrown/avenor/internal/runtime/opencodeacp"
	"github.com/sdougbrown/avenor/internal/runtime/opencodehttp"
	"github.com/sdougbrown/avenor/internal/runtime/pi"
	"github.com/sdougbrown/avenor/internal/runtime/pony"
)

func NewProvider(startOpts runtime.StartOptions, backend string) (runtime.Provider, error) {
	switch backend {
	case "opencode-acp":
		return opencodeacp.NewWithOptions(startOpts), nil
	case "opencode-http":
		return opencodehttp.NewWithOptions(startOpts)
	case "codex-app-server":
		return codexappserver.NewWithOptions(startOpts), nil
	case "gemini-acp":
		return geminiacp.NewWithOptions(startOpts), nil
	case "agy":
		return agy.NewWithOptions(startOpts), nil
	case "cursor-acp":
		return cursoracp.NewWithOptions(startOpts), nil
	case "pi":
		return pi.NewWithOptions(startOpts), nil
	case "pony":
		return pony.NewWithOptions(startOpts), nil
	case "claude":
		return claude.NewWithOptions(startOpts), nil
	case "claude-channel":
		return claudechannel.NewWithOptions(startOpts), nil
	default:
		return nil, fmt.Errorf("unknown backend %q", backend)
	}
}
