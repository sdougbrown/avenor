package model

import "context"

// Adapter translates between the canonical Request/Chunk types and a
// specific model provider's wire format.
type Adapter interface {
	// Stream sends a request and returns a channel of Chunks.
	// The implementation MUST close the channel after the stream ends or errors.
	// The implementation MUST respect context cancellation.
	Stream(ctx context.Context, req Request) (<-chan Chunk, error)

	// Name returns a human-readable identifier for this adapter,
	// e.g. "openai", "anthropic", "gemini".
	Name() string
}
