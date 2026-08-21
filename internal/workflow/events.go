package workflow

// EventKind names a workflow-store event. Workflow events are workflow-local
// and distinct from runtime session events (internal/events).
type EventKind string

const (
	EventInstantiated      EventKind = "workflow.event.instantiated"
	EventLeased            EventKind = "workflow.event.leased"
	EventStarted           EventKind = "workflow.event.started"
	EventAttemptTerminated EventKind = "workflow.event.attempt_terminated"
	EventCompleted         EventKind = "workflow.event.completed"
	EventGate              EventKind = "workflow.event.gate"
	EventSkipped           EventKind = "workflow.event.skipped"
	EventUnblocked         EventKind = "workflow.event.unblocked"
	EventRerouted          EventKind = "workflow.event.rerouted"
	EventHeartbeat         EventKind = "workflow.event.heartbeat"
	EventLeaseExpired      EventKind = "workflow.event.lease_expired"
	EventTransition        EventKind = "workflow.event.transition"
)

// InstanceRecord is the payload of EventInstantiated: enough immutable context
// to reconstruct the initial instance from the template entry contract.
type InstanceRecord struct {
	TemplateID       TemplateID       `json:"template_id"`
	TemplateVersion  TemplateVersion  `json:"template_version"`
	TerminalOutcomes []OutcomeName    `json:"terminal_outcomes"`
	EntryNodes       []NodeID         `json:"entry_nodes"`
	Children         []ChildReference `json:"children,omitempty"`
}

// Event is one record in the workflow store's NDJSON log. It carries its own
// event ID and workflow-scoped sequence (assigned by the manager/store in
// Stage 4). The batch produced for one command shares the command's
// idempotency key. Fields are optional per Kind.
type Event struct {
	ID             EventID             `json:"id"`
	Kind           EventKind           `json:"kind"`
	Sequence       int64               `json:"sequence"`
	CommandID      CommandID           `json:"command_id,omitempty"`
	IdempotencyKey string              `json:"idempotency_key,omitempty"`
	Identity       ExecutionIdentity   `json:"identity"`
	AttemptID      AttemptID           `json:"attempt_id,omitempty"`
	LeaseID        LeaseID             `json:"lease_id,omitempty"`
	Actor          string              `json:"actor,omitempty"`
	Reason         string              `json:"reason,omitempty"`
	Outcome        OutcomeName         `json:"outcome,omitempty"`
	AttemptStatus  AttemptStatus       `json:"attempt_status,omitempty"`
	MarkerKind     string              `json:"marker_kind,omitempty"`
	MarkerLabel    string              `json:"marker_label,omitempty"`
	Gate           *GateInstance       `json:"gate,omitempty"`
	Transition     *Transition         `json:"transition,omitempty"`
	Evidence       []Evidence          `json:"evidence,omitempty"`
	Outputs        []OutputValue       `json:"outputs,omitempty"`
	Iteration      int                 `json:"iteration,omitempty"`
	Selection      *ExecutionSelection `json:"selection,omitempty"`
	Instantiated   *InstanceRecord     `json:"instantiated,omitempty"`
	LeaseTargets   []NodeID            `json:"lease_targets,omitempty"`
	Lease          *Lease              `json:"lease,omitempty"`
}
