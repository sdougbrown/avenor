package runtime

import "github.com/sdougbrown/avenor/internal/thinkingpolicy"

// IsClaudeFamily reports whether the backend is one of the claude-family
// backends. They are the only backends the MCP wait applies to, and the only
// ones that hold start-only options (the thinking level and the MCP wait) in
// the process they spawn: their explicit resume takes no per-call options.
// Other bare-Resume backends (pony, opencode-http) reject start-only options
// at start, so their resumes never carry any.
func IsClaudeFamily(backend string) bool {
	return backend == "claude" || backend == "claude-channel"
}

// DropStartOnlyOptions removes options a resumed session cannot honor. The
// thinking drop follows the thinking policy's resume verdict, so a new
// start-only backend is honored without parallel edits; the MCP wait stays
// keyed on the claude family, which owns the wait's backend support.
func DropStartOnlyOptions(backend string, opts StartOptions) StartOptions {
	if thinkingpolicy.Evaluate(backend, opts.Thinking, true) == thinkingpolicy.StartOnly {
		opts.Thinking = ""
	}
	if IsClaudeFamily(backend) {
		opts.ClaudeMCPWait = 0
	}
	return opts
}
