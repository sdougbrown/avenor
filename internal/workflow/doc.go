// Package workflow owns Avenor's durable workflow state machine.
//
// # Contract boundary
//
// The contract boundary is owned by three cooperating layers. The JSON Schema
// Composition Profile v1 (schemas/workflow.profile.json) owns nested template
// structure (valueSchema): node, gate, output, loop, and child-workflow shapes,
// the action discriminator (a tagged union that rejects unknown and
// cross-variant fields), and additionalProperties rejection at every level. The
// embedded Umpire availability document owns top-level presence and required
// fields, plus the schema_version == 1 fairness rule. Typed Go owns the
// cross-node/graph/context rules and retains the checks the closed Profile
// vocabulary cannot express: the arbitrary-JSON leaves metadata, branches,
// outcome_map, and input_bindings[*].value, whose structural issues are
// suppressed by path so typed Go governs them; and the strict boundary
// guarantees of duplicate-key rejection, the 4 MiB size cap, the 64-level depth
// cap, and the 10,000-members-per-container cap, which the generated validator
// does not cover.
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
//
// Stage 4 builds the file-backed durable store: a locked append-only events.ndjson log, an atomically replaced workflow.json snapshot, POSIX flock serialization, malformed-tail-safe event replay, and a recovery catalog that replays events beyond the snapshot revision and expires only stale leases. Projection regeneration (Stage 5) is a no-op placeholder here.
package workflow

//go:generate go run github.com/umpire-tools/umpire-go-gen@v0.2.1 -profile ../../schemas/workflow.profile.json -output-file workflow_schema.gen.go -pkg workflow
