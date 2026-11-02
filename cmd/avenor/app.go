package main

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/stable"
	"github.com/sdougbrown/cli-contract"
)

// app declares the migrated Avenor command surface. Commands still on the
// legacy dispatch path (answer, await, watch, control, workflow, mcp, run)
// are absent until their migration lands; main.go routes them to the legacy
// switch in the meantime.
func app() *clicontract.App {
	return &clicontract.App{
		Name:                "avenor",
		Version:             Version,
		ErrorPrefix:         "avenor",
		HandlerOwnedSignals: true,
		VersionTemplate:     "avenor v{{.Version}}\n",
		Commands: []*clicontract.Command{
			{
				Name:  "probe",
				Short: "run a one-shot backend probe and write the transcript",
				Flags: []clicontract.Flag{
					{Name: "dir", Kind: clicontract.String, Usage: "working directory for the probe", Default: "."},
					{Name: "out", Kind: clicontract.String, Usage: "path to write the probe transcript", Required: true},
					{Name: "prompt", Kind: clicontract.String, Usage: "override the default probe prompt (Stage 1 discovery)"},
					{Name: "timeout", Kind: clicontract.Duration, Usage: "probe timeout", Default: 5 * time.Minute},
				},
				Handle: handleProbe,
			},
			{
				Name:  "stable",
				Short: "run the long-lived runtime supervisor",
				Flags: []clicontract.Flag{
					{Name: "control-socket", Kind: clicontract.String, Usage: "unix socket path for the control plane", Required: true},
					{Name: "http-debug", Kind: clicontract.String, Usage: "http debug adapter bind address"},
					{Name: "max-runtimes", Kind: clicontract.Int, Usage: "maximum concurrent child runtimes", Default: 16},
					{Name: "max-tree-budget", Kind: clicontract.Int, Usage: "maximum concurrent runtimes across the whole supervisor tree including nested supervisors (0 uses the default)", Default: admission.DefaultTreeBudget},
					{Name: "idempotency-capacity", Kind: clicontract.Int, Usage: "idempotent-spawn store capacity: completed entries plus in-flight reservations (0 uses the default)", Default: 1024},
					{Name: "idle-timeout", Kind: clicontract.Duration, Usage: "exit after this duration with no live child runtimes (parked runtimes count as live); control connections do not delay it"},
					{Name: "shutdown-timeout", Kind: clicontract.Duration, Usage: "graceful shutdown timeout before killing children", Default: 10 * time.Second},
					{Name: "parked-timeout", Kind: clicontract.Duration, Usage: "how long a finished runtime stays parked awaiting a follow-up prompt before it is reaped (0 = park until shutdown)", Default: 30 * time.Minute},
					{Name: "lease-sweep-interval", Kind: clicontract.Duration, Usage: "how often the supervisor expires workflow leases whose heartbeat stopped (0 = disable; leases then expire only on restart recovery)", Default: stable.DefaultLeaseSweepInterval},
					{Name: "permission-claim-timeout", Kind: clicontract.Duration, Usage: "how long to wait for a connected socket client to answer a permission request before falling through to the file handler or 'none' resolver (0 = disabled: fall through only when all clients disconnect; use a non-zero value for unattended automation where client processes may hang)"},
					{Name: "mcp-launch-wrapper-policy", Kind: clicontract.String, Usage: "path to a JSON allowlist of launch_wrapper argv entries permitted for MCP-driven spawns (default: $AVENOR_MCP_LAUNCH_WRAPPER_POLICY, else launch-wrapper-policy.json under the avenor config home (XDG_CONFIG_HOME/avenor or ~/.config/avenor); with none of them present, disabled)"},
					{Name: "workflow-root", Kind: clicontract.String, Usage: "workflow store root (default: $XDG_STATE_HOME/avenor/workflows, else $HOME/.avenor/workflows)"},
					{Name: "workflow-adapter-dir", Kind: clicontract.String, Usage: "host-owned trusted adapter manifest directory (default: $XDG_CONFIG_HOME/avenor/workflow-adapters, else $HOME/.config/avenor/workflow-adapters)"},
					{Name: "broker-parent-url", Kind: clicontract.String, Usage: "http:// address of the parent broker to join. An empty flag uses $AVENOR_PARENT_BROKER_URL. The credential comes from $AVENOR_PARENT_BROKER_TOKEN (or --broker-parent-token as an explicit override); a credential without a URL runs this broker as the tree's parent broker"},
					{Name: "broker-parent-token", Kind: clicontract.String, Usage: "parent-link credential overriding $AVENOR_PARENT_BROKER_TOKEN; prefer the env var, since argv is world-readable via /proc/<pid>/cmdline"},
					{Name: "broker-addr", Kind: clicontract.String, Usage: "address the broker binds, published beside the control socket as <socket>.broker.url. Empty binds an ephemeral loopback port; a durable hub names a concrete interface address and fixed port (e.g. 127.0.0.1:8787). A wildcard bind is refused because children on this host are told to reach the bound address, which a wildcard does not name"},
					{Name: "broker-advertise-url", Kind: clicontract.String, Usage: "http:// or https:// URL other hosts reach this broker on, used for the <socket>.broker.url record and registration with the parent broker. Empty advertises the bound address. Use it when a TLS terminator or NAT publishes the broker under another host, port or scheme; children on this host keep the bound address"},
				},
				Handle: handleStable,
			},
			{
				Name:  "board",
				Short: "serve the board UI for local avenor sessions",
				Flags: []clicontract.Flag{
					{Name: "addr", Kind: clicontract.String, Usage: "bind address (:0 = random port)", Default: "127.0.0.1:0"},
					{Name: "scan", Kind: clicontract.String, Usage: "directories to scan for *.sock files (comma-separated, default: ~/.avenor/sockets)"},
					{Name: "socket", Kind: clicontract.String, Usage: "single socket path; if set, --scan is ignored"},
					{Name: "token", Kind: clicontract.String, Usage: "auth token; required for non-loopback unless --no-auth is set"},
					{Name: "allow-mutations", Kind: clicontract.Bool, Usage: "enable POST /api/cancel and POST /api/prompt"},
					{Name: "no-auth", Kind: clicontract.Bool, Usage: "disable auth entirely (allows remote access without token)"},
					{Name: "no-open", Kind: clicontract.Bool, Usage: "do not open browser after starting"},
				},
				Handle: handleBoard,
			},
			{
				Name:  "channel-tools",
				Short: "run the channel tools loop for one broker run",
				Flags: []clicontract.Flag{
					{Name: "run-id", Kind: clicontract.String, Usage: "broker run id", Required: true},
					{Name: "token", Kind: clicontract.String, Usage: "broker auth token", Required: true},
					{Name: "broker-url", Kind: clicontract.String, Usage: "broker HTTP base URL", Required: true},
				},
				Handle: handleChannelTools,
			},
			{
				Name:  "parent-token",
				Short: "generate a parent-link credential",
				Raw:   handleParentToken,
			},
			{
				Name:  "verify",
				Short: "load and validate config files without starting a run",
				Flags: []clicontract.Flag{
					{Name: "dir", Kind: clicontract.String, Usage: "working directory (config file paths are resolved relative to this)", Default: "."},
					{Name: "loop-file", Kind: clicontract.String, Usage: "path to loop config to validate"},
					{Name: "team-file", Kind: clicontract.String, Usage: "path to team config to validate"},
					{Name: "roster-file", Kind: clicontract.String, Usage: "path to roster config to validate (standalone or as fallback for loop/team)"},
					{Name: "roster-entry", Kind: clicontract.String, Usage: "roster entry name to look up (requires --roster-file)"},
				},
				Handle: handleVerify,
			},
			{
				Name:  "schema",
				Short: "print the machine-readable command-surface document",
				Raw:   handleSchema,
			},
		},
	}
}

// schemaDocument is the shape the schema command writes: the declared
// surface as data, under Avenor's schema id.
type schemaDocument struct {
	Schema      string                               `json:"schema"`
	Version     string                               `json:"version"`
	Commands    map[string]clicontract.CommandSchema `json:"commands"`
	CommonFlags map[string]clicontract.FlagSchema    `json:"common_flags"`
}

func handleSchema(ctx context.Context, inv clicontract.Invocation, stdout io.Writer) error {
	a := app()
	doc := schemaDocument{
		Schema:      "avenor/cli-schema/v1",
		Version:     Version,
		Commands:    a.Schemas(),
		CommonFlags: a.CommonFlagSchemas(),
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(doc)
}
