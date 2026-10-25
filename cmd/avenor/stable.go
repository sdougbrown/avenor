package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/stable"
	"github.com/sdougbrown/avenor/internal/token"
)

// newSupervisorFn is the supervisor constructor used by runStable. Tests patch
// it to capture the Config without relying on Run's side effects.
var newSupervisorFn = stable.NewSupervisor

func runStable(args []string) int {
	fs := flag.NewFlagSet("stable", flag.ContinueOnError)
	controlSocket := fs.String("control-socket", "", "unix socket path for the control plane (required)")
	httpDebug := fs.String("http-debug", "", "http debug adapter bind address")
	maxRuntimes := fs.Int("max-runtimes", 16, "maximum concurrent child runtimes")
	maxTreeBudget := fs.Int("max-tree-budget", admission.DefaultTreeBudget, "maximum concurrent runtimes across the whole supervisor tree including nested supervisors (0 uses the default)")
	idempotencyCapacity := fs.Int("idempotency-capacity", 1024, "idempotent-spawn store capacity: completed entries plus in-flight reservations (must be > 0)")
	idleTimeout := fs.Duration("idle-timeout", 0, "exit after this duration with no live child runtimes (parked runtimes count as live); control connections do not delay it")
	shutdownTimeout := fs.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown timeout before killing children")
	parkedTimeout := fs.Duration("parked-timeout", 30*time.Minute, "how long a finished runtime stays parked awaiting a follow-up prompt before it is reaped (0 = park until shutdown)")
	leaseSweepInterval := fs.Duration("lease-sweep-interval", stable.DefaultLeaseSweepInterval, "how often the supervisor expires workflow leases whose heartbeat stopped (0 = disable; leases then expire only on restart recovery)")
	permClaimTimeout := fs.Duration("permission-claim-timeout", 0, "how long to wait for a connected socket client to answer a permission request before falling through to the file handler or 'none' resolver (0 = disabled: fall through only when all clients disconnect; use a non-zero value for unattended automation where client processes may hang)")
	mcpLaunchWrapperPolicyPath := fs.String("mcp-launch-wrapper-policy", "", "path to a JSON allowlist of launch_wrapper argv entries permitted for MCP-driven spawns (default: disabled)")
	workflowRoot := fs.String("workflow-root", "", "workflow store root (default: $XDG_STATE_HOME/avenor/workflows, else $HOME/.avenor/workflows)")
	workflowAdapterDir := fs.String("workflow-adapter-dir", "", "host-owned trusted adapter manifest directory (default: $XDG_CONFIG_HOME/avenor/workflow-adapters, else $HOME/.config/avenor/workflow-adapters)")
	brokerParentURL := fs.String("broker-parent-url", "", "http:// address of the parent broker to join. The credential comes from $AVENOR_PARENT_BROKER_TOKEN (or --broker-parent-token as an explicit override); a credential without a URL runs this broker as the tree's parent broker")
	brokerParentToken := fs.String("broker-parent-token", "", "parent-link credential overriding $AVENOR_PARENT_BROKER_TOKEN; prefer the env var, since argv is world-readable via /proc/<pid>/cmdline")
	brokerParentInsecure := fs.Bool("broker-parent-insecure", false, "allow a plaintext http:// parent broker URL to a non-loopback host (the bearer credential is exposed on the path)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *controlSocket == "" {
		fmt.Fprintln(os.Stderr, "avenor stable: --control-socket is required")
		return 1
	}
	if *idempotencyCapacity <= 0 {
		fmt.Fprintln(os.Stderr, "avenor stable: --idempotency-capacity must be greater than 0")
		return 1
	}
	credential := *brokerParentToken
	if credential == "" {
		credential = os.Getenv(token.EnvBrokerParentToken)
	}
	if *brokerParentURL != "" && credential == "" {
		fmt.Fprintf(os.Stderr, "avenor stable: %s or --broker-parent-token must be set when --broker-parent-url is used\n", token.EnvBrokerParentToken)
		return 1
	}

	// A nested supervisor inherits its parent's tree budget via the environment.
	// A root supervisor (no inherited budget) creates one in Avenor-owned
	// runtime state and propagates it to descendants so the whole tree shares
	// capacity.
	treeBudgetFile := os.Getenv(admission.EnvTreeBudget)

	tombstoneFile := *controlSocket + ".dead"
	if err := os.Remove(tombstoneFile); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "avenor stable: remove stale tombstone: %v\n", err)
	}

	var mcpPolicy *stable.MCPLaunchWrapperPolicy
	if *mcpLaunchWrapperPolicyPath != "" {
		policy, err := stable.LoadMCPLaunchWrapperPolicy(*mcpLaunchWrapperPolicyPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: mcp launch wrapper policy: %v\n", err)
			return 1
		}
		mcpPolicy = policy
	}

	sup := newSupervisorFn(stable.Config{
		ControlSocket:              *controlSocket,
		TombstoneFile:              tombstoneFile,
		HTTPDebug:                  *httpDebug,
		MaxRuntimes:                *maxRuntimes,
		MaxTreeBudget:              *maxTreeBudget,
		IdempotencyCapacity:        *idempotencyCapacity,
		TreeBudgetFile:             treeBudgetFile,
		IdleTimeout:                *idleTimeout,
		ShutdownTimeout:            *shutdownTimeout,
		ParkedRuntimeTimeout:       *parkedTimeout,
		WorkflowLeaseSweepInterval: *leaseSweepInterval,
		PermissionClaimTimeout:     *permClaimTimeout,
		WorkflowRoot:               *workflowRoot,
		WorkflowAdapterDir:         *workflowAdapterDir,
		MCPLaunchWrapperPolicy:     mcpPolicy,
		BrokerParentURL:            *brokerParentURL,
		BrokerParentToken:          credential,
		BrokerParentInsecure:       *brokerParentInsecure,
	})
	if treeBudgetFile == "" {
		// Root: propagate the tree budget path to descendant processes.
		if p := sup.TreeBudgetPath(); p != "" {
			os.Setenv(admission.EnvTreeBudget, p)
		}
	}
	return sup.Run()
}
