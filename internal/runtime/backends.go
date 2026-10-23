package runtime

// IsClaudeFamily reports whether the backend is one of the claude-family
// backends. They are the only backends the MCP wait applies to, and the only
// ones that hold start-only options (the thinking level and the MCP wait) in
// the process they spawn: their explicit resume takes no per-call options.
// Other bare-Resume backends (pony, opencode-http) reject start-only options
// at start, so their resumes never carry any.
func IsClaudeFamily(backend string) bool {
	return backend == "claude" || backend == "claude-channel"
}
