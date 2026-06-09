//go:build board

package board

import "embed"

// The Vite build output is copied here by the js-board-build mise task.
// This file is only compiled when -tags board is set. Without it, stub.go
// provides an empty FS and the board UI shows a "build frontend" message.
//
//go:embed all:dist
var EmbeddedFS embed.FS