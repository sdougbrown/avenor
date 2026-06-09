package board

// Config holds the configuration for the board HTTP server.
type Config struct {
	Addr           string   // e.g. "127.0.0.1:0" (default), "0.0.0.0:8080"
	ScanDirs       []string // directories to scan for *.sock files, default ["~/.avenor/sockets"]
	Socket         string   // single socket path; if set, ScanDirs is ignored
	AllowMutations bool     // enable POST /api/cancel and POST /api/prompt
	Token          string   // auth token; if empty and NoAuth is false, enforce loopback-only binding
	NoAuth         bool     // skip auth entirely (allows non-loopback without token)
	NoOpen         bool     // don't open browser after starting
}
