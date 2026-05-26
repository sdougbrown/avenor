package factory

import (
	"fmt"

	"github.com/sdougbrown/avenor/internal/runtime"
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
	case "cursor-acp":
		return cursoracp.NewWithOptions(startOpts), nil
	case "pi":
		return pi.NewWithOptions(startOpts), nil
	case "pony":
		return pony.NewWithOptions(startOpts), nil
	default:
		return nil, fmt.Errorf("unknown backend %q", backend)
	}
}
