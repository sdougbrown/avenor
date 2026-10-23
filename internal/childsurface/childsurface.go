// Package childsurface holds the MCP plumbing the child-facing tool surfaces
// share: the tool result envelope, the router bound to one prepared child
// provider, and the mapping from a router outcome to that envelope. A surface
// supplies only its own tools, its render functions and its peers wiring.
package childsurface

import (
	"context"

	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// Provider is a surface's router provider plus the generation binding every
// child provider performs before the router is built. It is an interface so a
// surface that adds capabilities to the shared child provider — the
// claude-channel sidecar adds ControlReply — keeps them for the router.
type Provider interface {
	router.Provider
	Prepare(ctx context.Context) error
}

// Envelope wraps one tool outcome in the MCP tool result: content carries the
// rendered sentence as its single text part, structuredContent carries the
// shared result object, and isError marks a refusal.
func Envelope(object map[string]any, text string, isError bool) map[string]any {
	return map[string]any{
		"content":           []map[string]any{{"type": "text", "text": text}},
		"structuredContent": object,
		"isError":           isError,
	}
}

// Router prepares the provider for the generation it binds to and hands it to
// the shared router. Preparation is idempotent, so a surface calls this on
// every tool call and the provider keeps the generation it read first.
func Router(ctx context.Context, provider Provider) (*router.Router, error) {
	if err := provider.Prepare(ctx); err != nil {
		return nil, err
	}
	return router.NewRouter(provider)
}

// Route maps one router outcome into the MCP envelope: a router refusal becomes
// an isError result carrying the router's output and message, any other error
// goes back to the JSON-RPC layer, and render turns a successful result into
// the structured object and its rendered text.
func Route(call func() (router.Result, error), render func(router.Result) (map[string]any, string)) (any, error) {
	result, err := call()
	if err != nil {
		if object, text, ok := router.ToolFailure(err); ok {
			return Envelope(object, text, true), nil
		}
		return nil, err
	}
	object, text := render(result)
	return Envelope(object, text, false), nil
}
