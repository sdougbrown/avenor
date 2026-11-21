package mcpserver

import (
	"time"

	"github.com/sdougbrown/avenor/internal/supervisor"
)

// supervisorLifecycle is the MCP server's hold on the supervisor it autostarted
// or attached to. The lifecycle itself lives in internal/supervisor, which the
// conversation feed starts and stops its own supervisor through.
type supervisorLifecycle = supervisor.Lifecycle

// startSupervisorFunc is the launcher the server autostarts a supervisor with.
// Tests replace it to count, stub or observe the launch.
var startSupervisorFunc = func(socketPath string, idleTimeout time.Duration, mcpLaunchWrapperPolicyPath string) (*supervisorLifecycle, error) {
	return supervisor.Start(socketPath, idleTimeout, mcpLaunchWrapperPolicyPath, supervisor.Options{})
}
