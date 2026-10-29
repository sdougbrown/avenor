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
//   - claude: Claude Code backend; PTY by default, AVENOR_CLAUDE_TERMINAL=tmux for tmux.
package factory

import (
	"fmt"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/agy"
	"github.com/sdougbrown/avenor/internal/runtime/claude"
	"github.com/sdougbrown/avenor/internal/runtime/codexappserver"
	"github.com/sdougbrown/avenor/internal/runtime/cursoracp"
	"github.com/sdougbrown/avenor/internal/runtime/geminiacp"
	"github.com/sdougbrown/avenor/internal/runtime/opencodeacp"
	"github.com/sdougbrown/avenor/internal/runtime/opencodehttp"
	"github.com/sdougbrown/avenor/internal/runtime/pi"
	"github.com/sdougbrown/avenor/internal/runtime/pony"
)

// noProcessBackends connect to an already-running or in-process runtime and
// never exec a child process, so a launch wrapper has nothing to prefix.
var noProcessBackends = map[string]bool{
	"opencode-http": true,
	"pony":          true,
}

func NewProvider(startOpts runtime.StartOptions, backend string) (runtime.Provider, error) {
	if len(startOpts.LaunchWrapper) > 0 && noProcessBackends[backend] {
		return nil, fmt.Errorf("launch_wrapper: not supported for backend %s (spawns no process to wrap)", backend)
	}
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
	default:
		return nil, fmt.Errorf("unknown backend %q", backend)
	}
}

// ResolvesPersistedSessions reports whether the backend can continue a prior
// run's session in a fresh process under this spawn's persisted-session
// choice. pi qualifies when the choice or the environment opts in; every
// other backend's resume path handles its own eligibility, so the resume
// gate in the supervisor consults the factory rather than importing a
// concrete backend.
func ResolvesPersistedSessions(backend string, opt *bool) bool {
	if backend != "pi" {
		return true
	}
	return pi.ResolvePersistSessions(opt)
}
