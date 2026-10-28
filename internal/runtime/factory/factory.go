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
	"maps"
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

// providers is the one place a backend is registered: IsKnown, KnownBackends
// and NewProvider all read it, so a name cannot be known without being
// constructible. A backend whose constructor cannot fail is wrapped by a
// closure; opencode-http already returns its own error.
var providers = map[string]func(runtime.StartOptions) (runtime.Provider, error){
	"opencode-acp": func(o runtime.StartOptions) (runtime.Provider, error) {
		return opencodeacp.NewWithOptions(o), nil
	},
	"opencode-http": opencodehttp.NewWithOptions,
	"codex-app-server": func(o runtime.StartOptions) (runtime.Provider, error) {
		return codexappserver.NewWithOptions(o), nil
	},
	"gemini-acp": func(o runtime.StartOptions) (runtime.Provider, error) {
		return geminiacp.NewWithOptions(o), nil
	},
	"agy": func(o runtime.StartOptions) (runtime.Provider, error) {
		return agy.NewWithOptions(o), nil
	},
	"cursor-acp": func(o runtime.StartOptions) (runtime.Provider, error) {
		return cursoracp.NewWithOptions(o), nil
	},
	"pi": func(o runtime.StartOptions) (runtime.Provider, error) {
		return pi.NewWithOptions(o), nil
	},
	"poo": func(o runtime.StartOptions) (runtime.Provider, error) {
		return poo.NewWithOptions(o), nil
	},
	"pony": func(o runtime.StartOptions) (runtime.Provider, error) {
		return pony.NewWithOptions(o), nil
	},
	"claude": func(o runtime.StartOptions) (runtime.Provider, error) {
		return claude.NewWithOptions(o), nil
	},
}

// IsKnown reports whether this build registers the backend. It answers the
// question a caller otherwise re-lists by hand before a provider exists.
func IsKnown(backend string) bool {
	return providers[backend] != nil
}

// KnownBackends returns every backend name this build registers, sorted, so a
// caller that must cover all of them iterates this instead of re-listing them.
func KnownBackends() []string {
	return slices.Sorted(maps.Keys(providers))
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
	construct := providers[backend]
	if construct == nil {
		return nil, fmt.Errorf("unknown backend %q", backend)
	}
	return construct(startOpts)
}
