package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/stable"
	"github.com/sdougbrown/avenor/internal/token"
	"github.com/sdougbrown/cli-contract"
)

// newSupervisorFn is the supervisor constructor used by handleStable. Tests
// patch it to capture the Config without relying on Run's side effects.
var newSupervisorFn = stable.NewSupervisor

// handleStable runs the supervisor until it exits; the supervisor owns
// signal handling and returns the process exit code.
func handleStable(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	controlSocket := inv.Flags["control-socket"]
	if controlSocket == "" {
		return nil, usageErrorf("avenor stable: --control-socket is required")
	}
	idempotencyCapacity := inv.Ints["idempotency-capacity"]
	if idempotencyCapacity <= 0 {
		return nil, usageErrorf("avenor stable: --idempotency-capacity must be greater than 0")
	}

	credential := inv.Flags["broker-parent-token"]
	if credential == "" {
		credential = os.Getenv(token.EnvBrokerParentToken)
	}
	parentURL := inv.Flags["broker-parent-url"]
	if parentURL == "" {
		parentURL = strings.TrimSpace(os.Getenv(token.EnvBrokerParentURL))
	}
	advertiseURL := inv.Flags["broker-advertise-url"]
	if advertiseURL == "" {
		advertiseURL = os.Getenv(token.EnvBrokerAdvertiseURL)
	}
	if parentURL != "" && credential == "" {
		return nil, usageErrorf("avenor stable: %s or --broker-parent-token must be set when %s or --broker-parent-url supplies a parent broker URL",
			token.EnvBrokerParentToken, token.EnvBrokerParentURL)
	}

	// A nested supervisor inherits its parent's tree budget via the environment.
	// A root supervisor (no inherited budget) creates one in Avenor-owned
	// runtime state and propagates it to descendants so the whole tree shares
	// capacity.
	treeBudgetFile := os.Getenv(admission.EnvTreeBudget)

	tombstoneFile := controlSocket + ".dead"
	if err := os.Remove(tombstoneFile); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("avenor stable: remove stale tombstone: %w", err)
	}

	mcpPolicy, _, err := resolveMCPLaunchWrapperPolicy(inv.Flags["mcp-launch-wrapper-policy"])
	if err != nil {
		return nil, fmt.Errorf("avenor stable: mcp launch wrapper policy: %w", err)
	}

	sup := newSupervisorFn(stable.Config{
		ControlSocket:              controlSocket,
		TombstoneFile:              tombstoneFile,
		HTTPDebug:                  inv.Flags["http-debug"],
		MaxRuntimes:                inv.Ints["max-runtimes"],
		MaxTreeBudget:              inv.Ints["max-tree-budget"],
		IdempotencyCapacity:        idempotencyCapacity,
		TreeBudgetFile:             treeBudgetFile,
		IdleTimeout:                inv.Durations["idle-timeout"],
		ShutdownTimeout:            inv.Durations["shutdown-timeout"],
		ParkedRuntimeTimeout:       inv.Durations["parked-timeout"],
		WorkflowLeaseSweepInterval: inv.Durations["lease-sweep-interval"],
		PermissionClaimTimeout:     inv.Durations["permission-claim-timeout"],
		WorkflowRoot:               inv.Flags["workflow-root"],
		WorkflowAdapterDir:         inv.Flags["workflow-adapter-dir"],
		MCPLaunchWrapperPolicy:     mcpPolicy,
		BrokerParentURL:            parentURL,
		BrokerParentToken:          credential,
		BrokerAddr:                 inv.Flags["broker-addr"],
		BrokerAdvertiseURL:         advertiseURL,
	})
	if treeBudgetFile == "" {
		// Root: propagate the tree budget path to descendant processes.
		if p := sup.TreeBudgetPath(); p != "" {
			os.Setenv(admission.EnvTreeBudget, p)
		}
	}
	return exitCode(sup.Run()), nil
}

// orDefault substitutes the fallback when the flag value is the zero value:
// declared defaults document intent, handlers apply them.
func orDefault[T comparable](v, fallback T) T {
	var zero T
	if v == zero {
		return fallback
	}
	return v
}
