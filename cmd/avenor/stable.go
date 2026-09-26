package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/stable"
)

func runStable(args []string) int {
	fs := flag.NewFlagSet("stable", flag.ContinueOnError)
	controlSocket := fs.String("control-socket", "", "unix socket path for the control plane (required)")
	httpDebug := fs.String("http-debug", "", "http debug adapter bind address")
	maxRuntimes := fs.Int("max-runtimes", 16, "maximum concurrent child runtimes")
	maxTreeBudget := fs.Int("max-tree-budget", admission.DefaultTreeBudget, "maximum concurrent runtimes across the whole supervisor tree including nested supervisors (0 uses the default)")
	idleTimeout := fs.Duration("idle-timeout", 0, "exit after this duration with no child runtimes and no control connections")
	shutdownTimeout := fs.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown timeout before killing children")
	parkedTimeout := fs.Duration("parked-timeout", 30*time.Minute, "how long a finished runtime stays parked awaiting a follow-up prompt before it is reaped (0 = park until shutdown)")
	permClaimTimeout := fs.Duration("permission-claim-timeout", 0, "how long to wait for a connected socket client to answer a permission request before falling through to the file handler or 'none' resolver (0 = disabled: fall through only when all clients disconnect; use a non-zero value for unattended automation where client processes may hang)")
	workflowRoot := fs.String("workflow-root", "", "workflow store root (default: $XDG_STATE_HOME/avenor/workflows, else $HOME/.avenor/workflows)")
	workflowAdapterDir := fs.String("workflow-adapter-dir", "", "host-owned trusted adapter manifest directory (default: $XDG_CONFIG_HOME/avenor/workflow-adapters, else $HOME/.config/avenor/workflow-adapters)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *controlSocket == "" {
		fmt.Fprintln(os.Stderr, "avenor stable: --control-socket is required")
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

	sup := stable.NewSupervisor(stable.Config{
		ControlSocket:          *controlSocket,
		TombstoneFile:          tombstoneFile,
		HTTPDebug:              *httpDebug,
		MaxRuntimes:            *maxRuntimes,
		MaxTreeBudget:          *maxTreeBudget,
		TreeBudgetFile:         treeBudgetFile,
		IdleTimeout:            *idleTimeout,
		ShutdownTimeout:        *shutdownTimeout,
		ParkedRuntimeTimeout:   *parkedTimeout,
		PermissionClaimTimeout: *permClaimTimeout,
		WorkflowRoot:           *workflowRoot,
		WorkflowAdapterDir:     *workflowAdapterDir,
	})
	if treeBudgetFile == "" {
		// Root: propagate the tree budget path to descendant processes.
		if p := sup.TreeBudgetPath(); p != "" {
			os.Setenv(admission.EnvTreeBudget, p)
		}
	}
	return sup.Run()
}
