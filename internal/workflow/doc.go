// Package workflow owns Avenor's durable workflow state machine.
//
// # Contract boundary
//
// The portable Umpire schema is intentionally limited to stable top-level
// template fields and their presence. Umpire v1 is an availability schema
// rather than a general nested JSON-schema language. Stage 1 strictly decodes
// the top-level envelope and node fields and validates the action discriminator;
// Stage 2 replaces its temporary envelope with typed node, gate, output, loop,
// and child-workflow values and strict action variants. The generated Check
// function is always called by the hand-written adapter; it is
// not the reducer and does not replace graph- or state-dependent validation.
// The strict JSON boundary rejects duplicate keys and bounds templates to 4 MiB,
// 64 levels of nesting, and 10,000 members per object or array.
//
// A template pins a schema version, template ID and version, entry nodes,
// declared nodes, and terminal outcomes. Nodes declare one of the action kinds
// run, loop, team, manual, external, or workflow. Ordinary dependency edges
// are acyclic. Cycles exist only through explicit bounded loops with an
// iteration limit, checkpoint, and exit outcomes. A workflow action pins its
// child template and maps only declared typed inputs, outputs, and outcomes.
//
// Durable commands are authority checked. A command has one idempotency key;
// each emitted event has its own event ID and workflow-scoped sequence. The
// snapshot revision is the last applied workflow sequence. Execution identity
// combines supervisor, workflow, node, activation, and optional attempt/run/
// runtime/session IDs. Completion additionally requires the active lease ID.
// Runtime termination, submitted completion, gate decisions, activation
// acceptance, and workflow completion remain distinct facts.
//
// The MVP persists instances on a local POSIX filesystem under one configured
// workflow root. It uses a locked append-only event log, an atomically replaced
// snapshot, immutable copied evidence, and deterministic generated projections.
// Missing snapshots are not cataloged. The manager is hosted by the stable
// supervisor and is driven through explicit claim/start commands; there is no
// automatic scheduler or broker routing for workflow events.
package workflow

//go:generate go run github.com/umpire-tools/umpire-go-gen@v0.1.1 -i ../../schemas/workflow.umpire.json -output-file workflow_schema.gen.go -pkg workflow
