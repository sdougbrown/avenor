//go:build !board

package board

import "io/fs"

// stubFS is an empty filesystem that always returns an error.
// Used when the board frontend is not built (no -tags board).
type stubFS struct{}

func (stubFS) Open(name string) (fs.File, error) {
	return nil, fs.ErrNotExist
}

// EmbeddedFS is empty when built without the board frontend.
// The board UI serves a placeholder page instead.
var EmbeddedFS fs.FS = stubFS{}