package runtime

// ChannelToolsMCP is the avenor channel-tools MCP server a backend registers for
// a run that has broker credentials: the server name, the avenor binary to
// spawn, its argv, and the environment the server needs. Each backend renders it
// into its own wire shape — an ACP `mcpServers` entry for the acp adapter, an
// opencode `mcp` config entry for the opencode-acp adapter — so the
// registration itself is defined once.
//
// Env carries the run token and Args never does: a spawned server's argv is
// readable by any same-user process through /proc/<pid>/cmdline for the lifetime
// of the child, and the token authenticates to the broker as this run.
type ChannelToolsMCP struct {
	Name    string
	Command string
	Args    []string
	Env     []EnvPair
}

// EnvPair is one environment variable a spawned server receives. The slice form
// keeps the rendered environment ordered, which an ACP `env` array needs.
type EnvPair struct {
	Name  string
	Value string
}

// ChannelToolsMCPFor returns the channel-tools registration for one run, and
// false when any coordinate is empty: without broker credentials there is
// nothing to register, and the session opens with no MCP servers.
func ChannelToolsMCPFor(bin, runID, brokerToken, brokerURL string) (ChannelToolsMCP, bool) {
	if bin == "" || runID == "" || brokerToken == "" || brokerURL == "" {
		return ChannelToolsMCP{}, false
	}
	return ChannelToolsMCP{
		Name:    "avenor-channel-tools",
		Command: bin,
		Args:    []string{"channel-tools", "--run-id", runID, "--broker-url", brokerURL},
		Env:     []EnvPair{{Name: EnvBrokerToken, Value: brokerToken}},
	}, true
}
