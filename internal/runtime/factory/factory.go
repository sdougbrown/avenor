// Package factory constructs runtime.Provider instances by backend name.
//
// Supported backends:
//   - opencode-acp: spawns opencode acp subprocess, owns lifecycle.
//   - opencode-http: talks to a running opencode serve instance over HTTP/SSE.
//   - codex-app-server: spawns Codex app-server subprocess via JSON-RPC over stdio.
//   - gemini-acp: spawns gemini --acp subprocess, owns lifecycle.
//   - cursor-acp: spawns agent acp subprocess, owns lifecycle.
//   - pi: spawns pi --mode rpc subprocess, owns lifecycle.
//   - poo: spawns a poo binary subprocess (protocol client lands in Stage 2).
//   - pony: test/stub backend.
//   - claude: Claude Code backend; PTY by default, AVENOR_CLAUDE_TERMINAL=tmux for tmux.
package factory

import (
	"fmt"
	"slices"

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
	"github.com/sdougbrown/avenor/internal/runtime/poo"
)

// knownBackends is the single backend registry the CLI consults before a
// provider exists. It lists every backend NewProvider dispatches; adding one
// there without a name here, or the reverse, fails TestKnownBackendsMatchNewProvider.
var knownBackends = map[string]bool{
	"opencode-acp":     true,
	"opencode-http":    true,
	"codex-app-server": true,
	"gemini-acp":       true,
	"agy":              true,
	"cursor-acp":       true,
	"pi":               true,
	"poo":              true,
	"pony":             true,
	"claude":           true,
}

// IsKnown reports whether this build registers the backend. It answers the
// question a caller otherwise re-lists by hand before a provider exists.
func IsKnown(backend string) bool {
	return knownBackends[backend]
}

// KnownBackends returns every backend name this build registers, sorted, so a
// caller that must cover all of them iterates this instead of re-listing them.
func KnownBackends() []string {
	names := make([]string, 0, len(knownBackends))
	for name := range knownBackends {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

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
	case "poo":
		return poo.NewWithOptions(startOpts), nil
	case "pony":
		return pony.NewWithOptions(startOpts), nil
	case "claude":
		return claude.NewWithOptions(startOpts), nil
	default:
		return nil, fmt.Errorf("unknown backend %q", backend)
	}
}
