package workflow

// WorkflowStatus is the durable status of a workflow instance.
type WorkflowStatus string

const (
	WorkflowActive       WorkflowStatus = "active"
	WorkflowBlocked      WorkflowStatus = "blocked"
	WorkflowAwaitingGate WorkflowStatus = "awaiting_gate"
	WorkflowCompleted    WorkflowStatus = "completed"
	WorkflowFailed       WorkflowStatus = "failed"
	WorkflowCanceled     WorkflowStatus = "canceled"
)

// ActivationStatus is the lifecycle state of one visit to a node.
type ActivationStatus string

const (
	ActivationPending            ActivationStatus = "pending"
	ActivationReady              ActivationStatus = "ready"
	ActivationLeased             ActivationStatus = "leased"
	ActivationRunning            ActivationStatus = "running"
	ActivationSkipped            ActivationStatus = "skipped"
	ActivationAttemptFailed      ActivationStatus = "attempt_failed"
	ActivationAwaitingCompletion ActivationStatus = "awaiting_completion"
	ActivationAwaitingGate       ActivationStatus = "awaiting_gate"
	ActivationAwaitingChild      ActivationStatus = "awaiting_child"
	ActivationBlocked            ActivationStatus = "blocked"
	ActivationLeaseExpired       ActivationStatus = "lease_expired"
	ActivationSatisfied          ActivationStatus = "satisfied"
	ActivationRejected           ActivationStatus = "rejected"
)

// AttemptStatus records execution separately from activation acceptance.
type AttemptStatus string

const (
	AttemptStarting  AttemptStatus = "starting"
	AttemptRunning   AttemptStatus = "running"
	AttemptSucceeded AttemptStatus = "succeeded"
	AttemptFailed    AttemptStatus = "failed"
	AttemptCanceled  AttemptStatus = "canceled"
	AttemptTimedOut  AttemptStatus = "timed_out"
	AttemptPanicked  AttemptStatus = "panicked"
)

// ActionKind selects the executor attached to a node.
type ActionKind string

const (
	ActionRun      ActionKind = "run"
	ActionLoop     ActionKind = "loop"
	ActionTeam     ActionKind = "team"
	ActionManual   ActionKind = "manual"
	ActionExternal ActionKind = "external"
	ActionWorkflow ActionKind = "workflow"
)

// GateType identifies who or what has authority to decide a gate.
type GateType string

const (
	GateMachine  GateType = "machine"
	GateExternal GateType = "external"
	GateHuman    GateType = "human"
)

// GateStatus is the durable result of one gate instance.
type GateStatus string

const (
	GatePending          GateStatus = "pending"
	GatePassed           GateStatus = "passed"
	GateFailed           GateStatus = "failed"
	GateActionRequired   GateStatus = "action_required"
	GateChangesRequested GateStatus = "changes_requested"
	GateRejected         GateStatus = "rejected"
	GateWaived           GateStatus = "waived"
)

// EvidenceSource identifies the authority that supplied evidence.
type EvidenceSource string

const (
	EvidenceMachine  EvidenceSource = "machine"
	EvidenceAgent    EvidenceSource = "agent"
	EvidenceHuman    EvidenceSource = "human"
	EvidenceExternal EvidenceSource = "external"
)

// RetryExhaustionKind controls what happens after the activation retry budget.
type RetryExhaustionKind string

const (
	RetryExhaustionBlock   RetryExhaustionKind = "block"
	RetryExhaustionFail    RetryExhaustionKind = "fail"
	RetryExhaustionOutcome RetryExhaustionKind = "outcome"
)

// CompletionContractKind selects a safe machine completion evaluator.
type CompletionContractKind string

const (
	CompletionExplicit CompletionContractKind = "explicit"
	CompletionFiles    CompletionContractKind = "files"
	CompletionGit      CompletionContractKind = "git"
)

// OutputType is the portable type of a declared workflow output.
type OutputType string

const (
	OutputString  OutputType = "string"
	OutputNumber  OutputType = "number"
	OutputBoolean OutputType = "boolean"
	OutputJSON    OutputType = "json"
	OutputFile    OutputType = "file"
)

// CommandKind names reducer commands without importing control-plane types.
type CommandKind string

const (
	CommandInstantiate  CommandKind = "instantiate"
	CommandClaim        CommandKind = "claim"
	CommandStart        CommandKind = "start"
	CommandComplete     CommandKind = "complete"
	CommandGate         CommandKind = "gate"
	CommandSkip         CommandKind = "skip"
	CommandUnblock      CommandKind = "unblock"
	CommandReroute      CommandKind = "reroute"
	CommandHeartbeat    CommandKind = "heartbeat"
	CommandTerminate    CommandKind = "terminate_attempt"
	CommandChildAttach  CommandKind = "child_attach"
	CommandChildOutcome CommandKind = "child_outcome"
)
