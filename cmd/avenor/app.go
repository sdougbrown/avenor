package main

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/stable"
	"github.com/sdougbrown/cli-contract"
)

// app declares the Avenor command surface; every command dispatches
// through the library.
func app() *clicontract.App {
	runCmd := cli.RunCommand()
	runCmd.Handle = handleRun
	return &clicontract.App{
		Name:                "avenor",
		Version:             Version,
		ErrorPrefix:         "avenor",
		HandlerOwnedSignals: true,
		BareHelp:            true,
		RootLong:            "Avenor orchestrates agent runs.",

		VersionTemplate: "avenor v{{.Version}}\n",
		Commands: []*clicontract.Command{
			runCmd,
			feedCommand(),
			messageCommand(),
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
					{Name: "idempotency-capacity", Kind: clicontract.Int, Usage: "idempotent-spawn store capacity: completed entries plus in-flight reservations (must be > 0)", Default: 1024},
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
				InputRules: []*clicontract.Rule{
					clicontract.EnabledOn("roster-entry", clicontract.Or(clicontract.Not(clicontract.Truthy("roster-entry")), clicontract.Truthy("roster-file")), "--roster-entry requires --roster-file"),
				},
				Handle: handleVerify,
			},
			{
				Name:  "watch",
				Short: "digest an avenor log into human-readable events",
				Positionals: []clicontract.Positional{
					{Name: "log", Required: true},
				},
				Flags: []clicontract.Flag{
					{Name: "follow", Kind: clicontract.Bool, Usage: "poll and tail the log"},
					{Name: "format", Kind: clicontract.String, Usage: "output format: plain (EVENT lines) or json (pass-through NDJSON)", Default: "plain"},
					{Name: "classify", Kind: clicontract.Bool, Usage: "prefix each plain-format digest line with MILESTONE | FINDING | ACTIVITY; in json format, adds a top-level \"classify\" field"},
					{Name: "accumulate", Kind: clicontract.Bool, Usage: "buffer consecutive thought/message chunks with the same session ID and event type, flushing them as a single human-readable line (plain format only)"},
					{Name: "poll-interval", Kind: clicontract.Duration, Usage: "follow-mode sleep interval", Default: 250 * time.Millisecond},
					{Name: "since-cursor", Kind: clicontract.String, Usage: "cursor file path: seek to saved offset before reading; rewrite offset on exit"},
				},
				Handle: handleWatch,
			},
			{
				Name:  "await",
				Short: "wait for a run to reach attention or completion",
				Positionals: []clicontract.Positional{
					{Name: "target", Required: true},
				},
				Flags: []clicontract.Flag{
					{Name: "socket", Kind: clicontract.String, Usage: "explicit control socket path; omitting scans ~/.avenor/sockets"},
					{Name: "until", Kind: clicontract.String, Usage: "wait until: attention or done", Default: "attention"},
					{Name: "timeout", Kind: clicontract.Duration, Usage: "wall-clock timeout"},
					{Name: "print-output", Kind: clicontract.Bool, Usage: "print complete result output before exiting"},
					{Name: "format", Kind: clicontract.String, Usage: "output format: plain or json", Default: "plain"},
				},
				Handle: handleAwait,
			},
			{
				Name:  "answer",
				Short: "answer a pending permission request from its request file",
				Positionals: []clicontract.Positional{
					{Name: "perm-base", Required: true},
				},
				Flags: []clicontract.Flag{
					{Name: "option", Kind: clicontract.String, Usage: "option id to select", Required: true},
					{Name: "message", Kind: clicontract.String, Usage: "free-text message to include in the response"},
					{Name: "outcome", Kind: clicontract.String, Usage: "outcome: selected or cancelled", Default: "selected"},
					{Name: "force", Kind: clicontract.Bool, Usage: "overwrite an existing response file"},
				},
				Handle: handleAnswer,
			},
			{
				Name:  "mcp",
				Short: "serve the Model Context Protocol interface",
				Flags: []clicontract.Flag{
					{Name: "transport", Kind: clicontract.String, Usage: "transport for MCP server (\"stdio\" or \"http\")", Default: "stdio"},
					{Name: "control-socket", Kind: clicontract.String, Usage: "unix socket path for the control plane"},
					{Name: "supervisor-socket", Kind: clicontract.String, Usage: "unix socket path for the supervisor"},
					{Name: "no-autostart", Kind: clicontract.Bool, Usage: "disable automatic supervisor start"},
					{Name: "idle-timeout", Kind: clicontract.Duration, Usage: "idle timeout before server exits", Default: 30 * time.Minute},
					{Name: "addr", Kind: clicontract.String, Usage: "address to listen on for HTTP transport", Default: "127.0.0.1:3748"},
					{Name: "auth-token", Kind: clicontract.String, Usage: "bearer token required for HTTP transport (defaults to MCP_AUTH_TOKEN)"},
					{Name: "auth-token-file", Kind: clicontract.String, Usage: "path to a mode-0600 file containing the bearer token for HTTP transport"},
					{Name: "max-wait", Kind: clicontract.Duration, Usage: "approximate polling budget for blocking tools (0 disables)", Default: 25 * time.Second},
					{Name: "allowed-host", Kind: clicontract.Repeat, Usage: "exact tailnet hostname accepted on the HTTP transport (repeatable)"},
					{Name: "mcp-launch-wrapper-policy", Kind: clicontract.String, Usage: "path to a launch_wrapper policy file forwarded to a supervisor this command autostarts (default: $AVENOR_MCP_LAUNCH_WRAPPER_POLICY, else launch-wrapper-policy.json under the avenor config home (XDG_CONFIG_HOME/avenor or ~/.config/avenor)); ignored when attaching to an existing supervisor, though a named file that is missing or malformed still fails"},
				},
				InputRules: []*clicontract.Rule{
					clicontract.EnabledOn("auth-token-file", clicontract.Or(clicontract.Not(clicontract.Truthy("auth-token-file")), clicontract.Not(clicontract.Truthy("auth-token"))), "--auth-token and --auth-token-file cannot both be set"),
					clicontract.EnabledOn("no-autostart", clicontract.Or(clicontract.Not(clicontract.Truthy("no-autostart")), clicontract.Truthy("supervisor-socket")), "--no-autostart requires --supervisor-socket"),
				},
				Handle: handleMCP,
			},
			{
				Name:  "workflow",
				Short: "drive durable workflows over the control socket",
				PersistentFlags: []clicontract.Flag{
					{Name: "socket", Kind: clicontract.String, Usage: "control socket path (required)"},
				},
				Subcommands: []*clicontract.Command{
					{
						Name:  "create",
						Short: "register a workflow template from a JSON file",
						Flags: []clicontract.Flag{
							{Name: "request-file", Kind: clicontract.String, Usage: "path to template JSON file (required)", Required: true},
						},
						Handle: handleWorkflowCreate,
					},
					{
						Name:  "instantiate",
						Short: "start one workflow instance from a template",
						Flags: []clicontract.Flag{
							{Name: "template-id", Kind: clicontract.String, Usage: "template id (required)", Required: true},
							{Name: "template-version", Kind: clicontract.String, Usage: "template version (required)", Required: true},
							{Name: "request-file", Kind: clicontract.String, Usage: "path to instance JSON file (required; {\"metadata\":{...},\"params\":{...}} or {})", Required: true},
						},
						Handle: handleWorkflowInstantiate,
					},
					{
						Name:        "status",
						Short:       "print one workflow instance's status",
						Positionals: []clicontract.Positional{{Name: "workflow-id", Required: true}},
						Handle:      handleWorkflowStatus,
					},
					{
						Name:        "wait",
						Short:       "block until one workflow instance settles",
						Positionals: []clicontract.Positional{{Name: "workflow-id", Required: true}},
						Flags: []clicontract.Flag{
							{Name: "timeout", Kind: clicontract.Duration, Usage: "wall-clock timeout for the wait", Default: 30 * time.Second},
						},
						Handle: handleWorkflowWait,
					},
					{
						Name:        "inspect",
						Short:       "print one workflow instance's full record",
						Positionals: []clicontract.Positional{{Name: "workflow-id", Required: true}},
						Handle:      handleWorkflowInspect,
					},
					{
						Name:        "events",
						Short:       "list one workflow instance's events",
						Positionals: []clicontract.Positional{{Name: "workflow-id", Required: true}},
						Flags: []clicontract.Flag{
							{Name: "after-seq", Kind: clicontract.Int, Usage: "only events with sequence greater than this value"},
							{Name: "limit", Kind: clicontract.Int, Usage: "maximum number of events (0 for server default)"},
						},
						Handle: handleWorkflowEvents,
					},
					{
						Name:        "complete",
						Short:       "complete a machine/external handoff activation",
						Positionals: []clicontract.Positional{{Name: "workflow-id", Required: true}, {Name: "node-id", Required: true}},
						Flags: []clicontract.Flag{
							{Name: "activation-id", Kind: clicontract.String, Usage: "activation id (required)", Required: true},
							{Name: "attempt-id", Kind: clicontract.String, Usage: "attempt id (required)", Required: true},
							{Name: "lease-id", Kind: clicontract.String, Usage: "lease id (required)", Required: true},
							{Name: "request-file", Kind: clicontract.String, Usage: "path to complete request JSON file (required; must contain owner_token, outcome, outputs, artifacts)", Required: true},
						},
						Handle: handleWorkflowComplete,
					},
					{
						Name:        "gate",
						Short:       "record a gate decision on a parked activation",
						Positionals: []clicontract.Positional{{Name: "workflow-id", Required: true}, {Name: "node-id", Required: true}, {Name: "gate-id", Required: true}},
						Flags: []clicontract.Flag{
							{Name: "activation-id", Kind: clicontract.String, Usage: "activation id (required)", Required: true},
							{Name: "operation", Kind: clicontract.String, Usage: "gate operation (required; satisfy, reject, waive, or external_result)", Required: true},
							{Name: "request-file", Kind: clicontract.String, Usage: "path to gate request JSON file (required)", Required: true},
						},
						Handle: handleWorkflowGate,
					},
					{
						Name:        "skip",
						Short:       "waive every unsatisfied required gate on a parked activation",
						Positionals: []clicontract.Positional{{Name: "workflow-id", Required: true}, {Name: "node-id", Required: true}},
						Flags: []clicontract.Flag{
							{Name: "request-file", Kind: clicontract.String, Usage: "path to skip request JSON file (required; must contain actor, reason, evidence_ids)", Required: true},
						},
						Handle: handleWorkflowSkip,
					},
					{
						Name:        "cancel",
						Short:       "cancel a whole workflow instance",
						Positionals: []clicontract.Positional{{Name: "workflow-id", Required: true}},
						Flags: []clicontract.Flag{
							{Name: "reason", Kind: clicontract.String, Usage: "reason for the cancel (required; recorded on the workflow.canceled event)", Required: true},
							{Name: "actor", Kind: clicontract.String, Usage: "actor id recorded as the canceling operator", Default: "cli"},
						},
						Handle: handleWorkflowCancel,
					},
					{
						Name:        "unblock",
						Short:       "return a blocked activation to ready",
						Positionals: []clicontract.Positional{{Name: "workflow-id", Required: true}, {Name: "node-id", Required: true}},
						Flags: []clicontract.Flag{
							{Name: "request-file", Kind: clicontract.String, Usage: "path to unblock request JSON file (required; must contain actor, reason)", Required: true},
						},
						Handle: handleWorkflowUnblock,
					},
					{
						Name:        "ready",
						Short:       "list a controller's ready work items",
						Positionals: []clicontract.Positional{{Name: "controller-id", Required: true}},
						Flags: []clicontract.Flag{
							{Name: "limit", Kind: clicontract.Int, Usage: "maximum number of ready items (0 for server default)"},
						},
						Handle: handleWorkflowReady,
					},
					{
						Name:  "controller",
						Short: "manage durable controllers",
						Subcommands: []*clicontract.Command{
							{
								Name:  "create",
								Short: "register a durable controller from a JSON file",
								Flags: []clicontract.Flag{
									{Name: "request-file", Kind: clicontract.String, Usage: "path to controller JSON file (required)", Required: true},
								},
								Handle: handleWorkflowControllerCreate,
							},
							{
								Name:        "enable",
								Short:       "enable one controller",
								Positionals: []clicontract.Positional{{Name: "controller-id", Required: true}},
								Handle:      handleWorkflowControllerEnable,
							},
							{
								Name:        "disable",
								Short:       "disable one controller",
								Positionals: []clicontract.Positional{{Name: "controller-id", Required: true}},
								Flags: []clicontract.Flag{
									{Name: "reason", Kind: clicontract.String, Usage: "reason for disabling (required)", Required: true},
								},
								Handle: handleWorkflowControllerDisable,
							},
							{
								Name:        "status",
								Short:       "print one controller's status",
								Positionals: []clicontract.Positional{{Name: "controller-id", Required: true}},
								Handle:      handleWorkflowControllerStatus,
							},
							{
								Name:   "list",
								Short:  "print every controller on the socket",
								Handle: handleWorkflowControllerList,
							},
						},
					},
				},
			},
			{
				Name:  "control",
				Short: "drive a running supervisor over its control socket",
				PersistentFlags: []clicontract.Flag{
					{Name: "socket", Kind: clicontract.String, Usage: "control socket path (required)"},
				},
				Subcommands: []*clicontract.Command{
					{
						Name:  "status",
						Short: "print one runtime's snapshot, or every runtime's",
						Positionals: []clicontract.Positional{
							{Name: "runtime-id"},
						},
						Handle: handleControlStatus,
					},
					{
						Name:   "list",
						Short:  "print every runtime on the socket",
						Handle: handleControlList,
					},
					{
						Name:   "tail",
						Short:  "stream control events as NDJSON",
						Handle: handleControlTail,
					},
					{
						Name:  "spawn",
						Short: "spawn one runtime as the socket's owner",
						Flags: []clicontract.Flag{
							{Name: "prompt", Kind: clicontract.String, Usage: "inline prompt text"},
							{Name: "prompt-file", Kind: clicontract.String, Usage: "path to prompt file"},
							{Name: "dir", Kind: clicontract.String, Usage: "working directory for the runtime"},
							{Name: "agent", Kind: clicontract.String, Usage: "agent name"},
							{Name: "label", Kind: clicontract.String, Usage: "free-form label for log correlation"},
							{Name: "model", Kind: clicontract.String, Usage: "backend-specific model id"},
							{Name: "thinking", Kind: clicontract.String, Usage: "thinking level (off, minimal, low, medium, high, xhigh, max)"},
							{Name: "backend", Kind: clicontract.String, Usage: "runtime backend"},
							{Name: "roster-file", Kind: clicontract.String, Usage: "path to the roster map"},
							{Name: "roster-entry", Kind: clicontract.String, Usage: "roster entry key"},
							{Name: "server-url", Kind: clicontract.String, Usage: "long-lived ACP server endpoint"},
							{Name: "on-event", Kind: clicontract.String, Usage: "path to write NDJSON events"},
							{Name: "sentinel-file", Kind: clicontract.String, Usage: "path to write a completion sentinel"},
							{Name: "permission-handler", Kind: clicontract.String, Usage: "permission handler, supports file:<path>"},
							{Name: "auto-approve", Kind: clicontract.Bool, Usage: "automatically approve all permission requests"},
							{Name: "persist-sessions", Kind: clicontract.Bool, Usage: "persist the backend session (pi: a session directory instead of --no-session, enabling cross-process resume; omitted inherits AVENOR_PI_SESSIONS, true and false force the choice)"},
							{Name: "messaging", Kind: clicontract.Bool, Usage: "enable poo messaging (defaults on for delegation spawns, off for workflow nodes, loop or team phases, and permission-decider runs)"},
							{Name: "timeout", Kind: clicontract.Int, Usage: "overall session timeout in seconds"},
							{Name: "max-retries", Kind: clicontract.Int, Usage: "maximum retry attempts on transient failure"},
							{Name: "launch-wrapper", Kind: clicontract.Repeat, Usage: "opaque argv prefix in front of the backend command; repeatable, each use adds one element"},
						},
						Handle: handleControlSpawn,
					},
					{
						Name:  "cancel",
						Short: "cancel one runtime (or the whole session)",
						Positionals: []clicontract.Positional{
							{Name: "runtime-id"},
						},
						Handle: handleControlCancel,
					},
					{
						Name:  "prompt",
						Short: "queue one prompt for one runtime",
						Positionals: []clicontract.Positional{
							{Name: "text", Required: true},
							{Name: "runtime-id"},
						},
						Handle: handleControlPrompt,
					},
					{
						Name:  "interrupt-and-prompt",
						Short: "interrupt one runtime and queue a prompt",
						Positionals: []clicontract.Positional{
							{Name: "text", Required: true},
							{Name: "runtime-id"},
						},
						Handle: handleControlInterruptAndPrompt,
					},
					{
						Name:  "answer-permission",
						Short: "answer one pending permission request",
						Positionals: []clicontract.Positional{
							{Name: "request-id", Required: true},
							{Name: "option-id", Required: true},
							{Name: "runtime-id"},
						},
						Handle: handleControlAnswerPermission,
					},
					{
						Name:  "shutdown",
						Short: "stop the supervisor: graceful by default",
						Positionals: []clicontract.Positional{
							{Name: "mode"},
						},
						Handle: handleControlShutdown,
					},
				},
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
