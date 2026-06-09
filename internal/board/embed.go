package board

import "embed"

// The Vite build output is copied here by the js-board-build mise task.
//go:embed all:dist
var EmbeddedFS embed.FS
