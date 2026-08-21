package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// Template is an immutable, reusable workflow definition.
type Template struct {
	SchemaVersion     int                     `json:"schema_version"`
	TemplateID        TemplateID              `json:"template_id"`
	TemplateVersion   TemplateVersion         `json:"template_version"`
	Metadata          map[string]any          `json:"metadata,omitempty"`
	EntryNodes        []NodeID                `json:"entry_nodes"`
	Nodes             []NodeDefinition        `json:"nodes"`
	TerminalOutcomes  []OutcomeName           `json:"terminal_outcomes"`
	BoundedLoops      []BoundedLoopDefinition `json:"bounded_loops,omitempty"`
	DefaultLease      *LeasePolicy            `json:"default_lease_policy,omitempty"`
	DefaultRetry      *RetryPolicy            `json:"default_retry_policy,omitempty"`
	CompositionLimits *CompositionLimits      `json:"composition_limits,omitempty"`
}

// MarshalJSON guarantees that emitted templates satisfy the strict wire syntax.
func (template Template) MarshalJSON() ([]byte, error) {
	type templateWire Template
	data, err := json.Marshal(templateWire(template))
	if err != nil {
		return nil, err
	}
	if len(data) > maxTemplateBytes {
		return nil, fmt.Errorf("template exceeds %d-byte limit", maxTemplateBytes)
	}
	if err := preflightJSON(data); err != nil {
		return nil, err
	}
	if err := rejectNonCanonicalKeys(data); err != nil {
		return nil, err
	}
	return data, nil
}

// UnmarshalJSON preserves arbitrary-precision JSON numbers stored in metadata.
func (template *Template) UnmarshalJSON(data []byte) error {
	type templateWire Template
	var decoded templateWire
	if err := decodeStrict(data, &decoded); err != nil {
		return err
	}
	*template = Template(decoded)
	return nil
}

type NodeDefinition struct {
	ID           NodeID                 `json:"id"`
	Name         string                 `json:"name,omitempty"`
	Dependencies []NodeID               `json:"dependencies,omitempty"`
	Outcomes     []OutcomeDefinition    `json:"outcomes,omitempty"`
	Branches     map[OutcomeName]NodeID `json:"branches,omitempty"`
	Action       Action                 `json:"action"`
	Assignment   *Assignment            `json:"assignment,omitempty"`
	Completion   *CompletionContract    `json:"completion,omitempty"`
	Outputs      []OutputDefinition     `json:"outputs,omitempty"`
	Gates        []GateDefinition       `json:"gates,omitempty"`
	RetryPolicy  *RetryPolicy           `json:"retry_policy,omitempty"`
	LoopID       LoopID                 `json:"loop_id,omitempty"`
	Checkpoint   *CheckpointDefinition  `json:"checkpoint,omitempty"`
	LeasePolicy  *LeasePolicy           `json:"lease_policy,omitempty"`
	SkipRule     *AuthorityRule         `json:"skip_rule,omitempty"`
	WaiveRules   []AuthorityRule        `json:"waive_rules,omitempty"`
}

type OutcomeDefinition struct {
	Name         OutcomeName `json:"name"`
	TargetNodeID NodeID      `json:"target_node_id,omitempty"`
	Terminal     bool        `json:"terminal,omitempty"`
}

type BoundedLoopDefinition struct {
	ID           LoopID        `json:"id"`
	BodyNodes    []NodeID      `json:"body_nodes"`
	EntryNodeID  NodeID        `json:"entry_node_id"`
	CheckpointID NodeID        `json:"checkpoint_node_id"`
	MaximumRuns  int           `json:"maximum_iterations"`
	ExitOutcomes []OutcomeName `json:"exit_outcomes"`
}

type CheckpointDefinition struct {
	Path            string        `json:"path"`
	ExitOutcomes    []OutcomeName `json:"exit_outcomes"`
	RequiresRelease bool          `json:"requires_release,omitempty"`
}

type CompositionLimits struct {
	MaximumDepth    int `json:"max_depth"`
	MaximumChildren int `json:"max_children"`
}

type Assignment struct {
	Role        string `json:"role,omitempty"`
	RosterFile  string `json:"roster_file,omitempty"`
	RosterEntry string `json:"roster_entry,omitempty"`
	Backend     string `json:"backend,omitempty"`
	Agent       string `json:"agent,omitempty"`
	Model       string `json:"model,omitempty"`
	Thinking    string `json:"thinking,omitempty"`
}

type ExecutionSelection struct {
	Role         string `json:"role,omitempty"`
	Backend      string `json:"backend,omitempty"`
	Agent        string `json:"agent,omitempty"`
	Model        string `json:"model,omitempty"`
	Thinking     string `json:"thinking,omitempty"`
	RosterDigest string `json:"roster_digest,omitempty"`
}

// Action is a strict tagged union. Exactly one variant pointer corresponds to
// Kind; MarshalJSON and UnmarshalJSON use the flat {"type": ...} wire shape.
type Action struct {
	Kind     ActionKind      `json:"-"`
	Run      *RunAction      `json:"-"`
	Loop     *LoopAction     `json:"-"`
	Team     *TeamAction     `json:"-"`
	Manual   *ManualAction   `json:"-"`
	External *ExternalAction `json:"-"`
	Workflow *WorkflowAction `json:"-"`
}

type RunAction struct {
	Prompt     string `json:"prompt,omitempty"`
	PromptFile string `json:"prompt_file,omitempty"`
}

type LoopAction struct {
	LoopFile string `json:"loop_file"`
}

type TeamAction struct {
	TeamFile string `json:"team_file"`
}

type ManualAction struct {
	Instructions string `json:"instructions,omitempty"`
}

type ExternalAction struct {
	Source      string `json:"source"`
	SubjectType string `json:"subject_type,omitempty"`
}

type WorkflowAction struct {
	TemplateID      TemplateID                  `json:"template_id"`
	TemplateVersion TemplateVersion             `json:"template_version"`
	ChildKey        string                      `json:"child_key"`
	InputBindings   []InputBinding              `json:"input_bindings,omitempty"`
	OutputBindings  []OutputBinding             `json:"output_bindings,omitempty"`
	OutcomeMap      map[OutcomeName]OutcomeName `json:"outcome_map"`
}

type InputBinding struct {
	Input string                   `json:"input"`
	Value json.RawMessage          `json:"value,omitempty"`
	From  *TemplateOutputReference `json:"from,omitempty"`
}

// TemplateOutputReference identifies an output available from a parent node.
// Runtime OutputReference values add workflow, activation, and revision identity.
type TemplateOutputReference struct {
	NodeID   NodeID   `json:"node_id"`
	OutputID OutputID `json:"output_id"`
}

type OutputBinding struct {
	ChildOutput  string `json:"child_output"`
	ParentOutput string `json:"parent_output"`
}

func (action Action) MarshalJSON() ([]byte, error) {
	if err := validateAction(action); err != nil {
		return nil, err
	}
	switch action.Kind {
	case ActionRun:
		if action.Run == nil {
			return nil, missingActionVariant(action.Kind)
		}
		return json.Marshal(struct {
			Type ActionKind `json:"type"`
			*RunAction
		}{action.Kind, action.Run})
	case ActionLoop:
		if action.Loop == nil {
			return nil, missingActionVariant(action.Kind)
		}
		return json.Marshal(struct {
			Type ActionKind `json:"type"`
			*LoopAction
		}{action.Kind, action.Loop})
	case ActionTeam:
		if action.Team == nil {
			return nil, missingActionVariant(action.Kind)
		}
		return json.Marshal(struct {
			Type ActionKind `json:"type"`
			*TeamAction
		}{action.Kind, action.Team})
	case ActionManual:
		if action.Manual == nil {
			return nil, missingActionVariant(action.Kind)
		}
		return json.Marshal(struct {
			Type ActionKind `json:"type"`
			*ManualAction
		}{action.Kind, action.Manual})
	case ActionExternal:
		if action.External == nil {
			return nil, missingActionVariant(action.Kind)
		}
		return json.Marshal(struct {
			Type ActionKind `json:"type"`
			*ExternalAction
		}{action.Kind, action.External})
	case ActionWorkflow:
		if action.Workflow == nil {
			return nil, missingActionVariant(action.Kind)
		}
		return json.Marshal(struct {
			Type ActionKind `json:"type"`
			*WorkflowAction
		}{action.Kind, action.Workflow})
	default:
		return nil, fmt.Errorf("unsupported workflow action %q", action.Kind)
	}
}

func (action *Action) UnmarshalJSON(data []byte) error {
	if err := preflightJSON(data); err != nil {
		return fmt.Errorf("workflow action: %w", err)
	}
	if err := requireCanonicalActionKeys(data); err != nil {
		return fmt.Errorf("workflow action: %w", err)
	}
	var discriminator struct {
		Type ActionKind `json:"type"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return fmt.Errorf("workflow action: %w", err)
	}
	if strings.TrimSpace(string(discriminator.Type)) == "" {
		return fmt.Errorf("workflow action.type is required")
	}

	switch discriminator.Type {
	case ActionRun:
		var wire struct {
			Type ActionKind `json:"type"`
			RunAction
		}
		if err := decodeActionObject(data, &wire, "type", "prompt", "prompt_file"); err != nil {
			return err
		}
		*action = Action{Kind: ActionRun, Run: &wire.RunAction}
	case ActionLoop:
		var wire struct {
			Type ActionKind `json:"type"`
			LoopAction
		}
		if err := decodeActionObject(data, &wire, "type", "loop_file"); err != nil {
			return err
		}
		*action = Action{Kind: ActionLoop, Loop: &wire.LoopAction}
	case ActionTeam:
		var wire struct {
			Type ActionKind `json:"type"`
			TeamAction
		}
		if err := decodeActionObject(data, &wire, "type", "team_file"); err != nil {
			return err
		}
		*action = Action{Kind: ActionTeam, Team: &wire.TeamAction}
	case ActionManual:
		var wire struct {
			Type ActionKind `json:"type"`
			ManualAction
		}
		if err := decodeActionObject(data, &wire, "type", "instructions"); err != nil {
			return err
		}
		*action = Action{Kind: ActionManual, Manual: &wire.ManualAction}
	case ActionExternal:
		var wire struct {
			Type ActionKind `json:"type"`
			ExternalAction
		}
		if err := decodeActionObject(data, &wire, "type", "source", "subject_type"); err != nil {
			return err
		}
		*action = Action{Kind: ActionExternal, External: &wire.ExternalAction}
	case ActionWorkflow:
		var wire struct {
			Type ActionKind `json:"type"`
			WorkflowAction
		}
		if err := decodeActionObject(data, &wire, "type", "template_id", "template_version", "child_key", "input_bindings", "output_bindings", "outcome_map"); err != nil {
			return err
		}
		*action = Action{Kind: ActionWorkflow, Workflow: &wire.WorkflowAction}
	default:
		return fmt.Errorf("unsupported workflow action %q", discriminator.Type)
	}
	return validateAction(*action)
}

func decodeActionObject(data []byte, target any, fields ...string) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("workflow action: %w", err)
	}
	if err := requireCanonicalKeys(raw, fields); err != nil {
		return fmt.Errorf("workflow action: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("workflow action: %w", err)
	}
	return nil
}

func missingActionVariant(kind ActionKind) error {
	return fmt.Errorf("workflow action %q has no variant value", kind)
}

type LeasePolicy struct {
	TTLSeconds               int64 `json:"ttl_seconds"`
	HeartbeatIntervalSeconds int64 `json:"heartbeat_interval_seconds,omitempty"`
}

type RetryPolicy struct {
	MaximumAttempts int                 `json:"max_attempts"`
	Exhaustion      RetryExhaustionKind `json:"exhaustion"`
	Outcome         OutcomeName         `json:"outcome,omitempty"`
}

type CompletionContract struct {
	Kind      CompletionContractKind `json:"kind"`
	Artifacts []ArtifactRequirement  `json:"artifacts,omitempty"`
	Git       *GitRequirement        `json:"git,omitempty"`
}

type ArtifactRequirement struct {
	Path     string `json:"path"`
	NonEmpty bool   `json:"non_empty,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
}

type GitRequirement struct {
	Clean           bool   `json:"clean,omitempty"`
	Head            string `json:"head,omitempty"`
	ChangedFromBase bool   `json:"changed_from_base,omitempty"`
}

type OutputDefinition struct {
	ID       OutputID   `json:"id"`
	Name     string     `json:"name"`
	Type     OutputType `json:"type"`
	Required bool       `json:"required,omitempty"`
}

type OutputValue struct {
	ID           OutputID        `json:"id"`
	DefinitionID OutputID        `json:"definition_id"`
	ActivationID ActivationID    `json:"activation_id"`
	Revision     int64           `json:"revision"`
	Value        json.RawMessage `json:"value"`
	EvidenceIDs  []EvidenceID    `json:"evidence_ids,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

type OutputReference struct {
	WorkflowID   WorkflowID   `json:"workflow_id"`
	NodeID       NodeID       `json:"node_id"`
	ActivationID ActivationID `json:"activation_id"`
	OutputID     OutputID     `json:"output_id"`
	Revision     int64        `json:"revision"`
}

type GateDefinition struct {
	ID              GateID        `json:"id"`
	Name            string        `json:"name,omitempty"`
	Type            GateType      `json:"type"`
	Required        bool          `json:"required"`
	AllowedOutcomes []OutcomeName `json:"allowed_outcomes,omitempty"`
	SubjectType     string        `json:"subject_type,omitempty"`
}

type Subject struct {
	Type        string `json:"type"`
	Repository  string `json:"repository,omitempty"`
	PullRequest int    `json:"pull_request,omitempty"`
	Revision    string `json:"revision"`
}

type AuthorityRule struct {
	Authorities    []string `json:"authorities"`
	ReasonRequired bool     `json:"reason_required,omitempty"`
}

type WorkflowInstance struct {
	WorkflowID      WorkflowID       `json:"workflow_id"`
	InstanceID      InstanceID       `json:"instance_id"`
	TemplateID      TemplateID       `json:"template_id"`
	TemplateVersion TemplateVersion  `json:"template_version"`
	Revision        int64            `json:"revision"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
	Metadata        map[string]any   `json:"metadata,omitempty"`
	Status          WorkflowStatus   `json:"status"`
	TerminalOutcome OutcomeName      `json:"terminal_outcome,omitempty"`
	Activations     []Activation     `json:"activations"`
	Attempts        []Attempt        `json:"attempts,omitempty"`
	Evidence        []Evidence       `json:"evidence,omitempty"`
	Gates           []GateInstance   `json:"gates,omitempty"`
	Outputs         []OutputValue    `json:"outputs,omitempty"`
	Children        []ChildReference `json:"children,omitempty"`
}

type Activation struct {
	ID              ActivationID        `json:"activation_id"`
	NodeID          NodeID              `json:"node_id"`
	Iteration       int                 `json:"iteration"`
	IncomingOutcome OutcomeName         `json:"incoming_outcome,omitempty"`
	Status          ActivationStatus    `json:"status"`
	Selection       *ExecutionSelection `json:"selection,omitempty"`
	AttemptIDs      []AttemptID         `json:"attempt_ids,omitempty"`
	ActiveLease     *Lease              `json:"active_lease,omitempty"`
	SelectedOutcome OutcomeName         `json:"selected_outcome,omitempty"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
}

type ExecutionIdentity struct {
	SupervisorID string       `json:"supervisor_id"`
	WorkflowID   WorkflowID   `json:"workflow_id"`
	NodeID       NodeID       `json:"node_id"`
	ActivationID ActivationID `json:"activation_id"`
	AttemptID    AttemptID    `json:"attempt_id,omitempty"`
	RunID        string       `json:"run_id,omitempty"`
	RuntimeID    string       `json:"runtime_id,omitempty"`
	SessionID    string       `json:"session_id,omitempty"`
}

type Attempt struct {
	ID               AttemptID         `json:"attempt_id"`
	Identity         ExecutionIdentity `json:"identity"`
	Status           AttemptStatus     `json:"status"`
	Backend          string            `json:"backend,omitempty"`
	Agent            string            `json:"agent,omitempty"`
	Model            string            `json:"model,omitempty"`
	WorkingDirectory string            `json:"working_directory,omitempty"`
	Worktree         string            `json:"worktree,omitempty"`
	BaseGitSHA       string            `json:"base_git_sha,omitempty"`
	EndingGitSHA     string            `json:"ending_git_sha,omitempty"`
	EventPath        string            `json:"event_path,omitempty"`
	SentinelPath     string            `json:"sentinel_path,omitempty"`
	ArtifactPaths    []string          `json:"artifact_paths,omitempty"`
	StartedAt        time.Time         `json:"started_at"`
	EndedAt          *time.Time        `json:"ended_at,omitempty"`
	MarkerKind       string            `json:"marker_kind,omitempty"`
	MarkerLabel      string            `json:"marker_label,omitempty"`
	FailureClass     string            `json:"failure_class,omitempty"`
	Corrections      int               `json:"corrections,omitempty"`
}

type Lease struct {
	ID              LeaseID      `json:"lease_id"`
	ActivationID    ActivationID `json:"activation_id"`
	Owner           string       `json:"owner"`
	TokenDigest     string       `json:"token_digest"`
	AcquiredAt      time.Time    `json:"acquired_at"`
	ExpiresAt       time.Time    `json:"expires_at"`
	LastHeartbeatAt *time.Time   `json:"last_heartbeat_at,omitempty"`
	LastActivityAt  *time.Time   `json:"last_activity_at,omitempty"`
}

type Evidence struct {
	ID           EvidenceID      `json:"evidence_id"`
	Kind         string          `json:"kind"`
	Source       EvidenceSource  `json:"source"`
	Authority    string          `json:"authority"`
	OriginalPath string          `json:"original_path,omitempty"`
	StoredPath   string          `json:"stored_path,omitempty"`
	Size         int64           `json:"size,omitempty"`
	SHA256       string          `json:"sha256,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	ActivationID ActivationID    `json:"activation_id"`
	Subject      *Subject        `json:"subject,omitempty"`
}

type GateInstance struct {
	ID           GateInstanceID `json:"gate_instance_id"`
	GateID       GateID         `json:"gate_id"`
	ActivationID ActivationID   `json:"activation_id"`
	Status       GateStatus     `json:"status"`
	Outcome      OutcomeName    `json:"outcome,omitempty"`
	Actor        string         `json:"actor,omitempty"`
	Reason       string         `json:"reason,omitempty"`
	Subject      *Subject       `json:"subject,omitempty"`
	PollID       string         `json:"poll_id,omitempty"`
	Source       string         `json:"source,omitempty"`
	ResponseHash string         `json:"response_hash,omitempty"`
	EvidenceIDs  []EvidenceID   `json:"evidence_ids,omitempty"`
	ObservedAt   *time.Time     `json:"observed_at,omitempty"`
	DecidedAt    *time.Time     `json:"decided_at,omitempty"`
}

type ChildReference struct {
	ID               ChildReferenceID  `json:"child_reference_id"`
	NodeID           NodeID            `json:"node_id,omitempty"`
	ParentActivation ActivationID      `json:"parent_activation_id"`
	WorkflowID       WorkflowID        `json:"workflow_id"`
	TemplateID       TemplateID        `json:"template_id"`
	TemplateVersion  TemplateVersion   `json:"template_version"`
	Outputs          []OutputReference `json:"outputs,omitempty"`
	Outcome          OutcomeName       `json:"outcome,omitempty"`
}

type Transition struct {
	ActivationID ActivationID `json:"activation_id"`
	Outcome      OutcomeName  `json:"outcome"`
	TargetNodeID NodeID       `json:"target_node_id,omitempty"`
	CreatedAt    time.Time    `json:"created_at"`
}

// Command is the pure reducer input. Stage-specific handlers validate and fill
// only the fields relevant to Kind.
type Command struct {
	ID               CommandID           `json:"command_id"`
	Kind             CommandKind         `json:"kind"`
	Identity         ExecutionIdentity   `json:"identity"`
	ExpectedRevision int64               `json:"expected_revision"`
	IdempotencyKey   string              `json:"idempotency_key"`
	LeaseID          LeaseID             `json:"lease_id,omitempty"`
	Actor            string              `json:"actor,omitempty"`
	Reason           string              `json:"reason,omitempty"`
	Outcome          OutcomeName         `json:"outcome,omitempty"`
	AttemptStatus    AttemptStatus       `json:"attempt_status,omitempty"`
	MarkerKind       string              `json:"marker_kind,omitempty"`
	MarkerLabel      string              `json:"marker_label,omitempty"`
	Evidence         []Evidence          `json:"evidence,omitempty"`
	Outputs          []OutputValue       `json:"outputs,omitempty"`
	Gate             *GateInstance       `json:"gate,omitempty"`
	Lease            *Lease              `json:"lease,omitempty"`
	Selection        *ExecutionSelection `json:"selection,omitempty"`
	// ChildOutputs is the CommandChildOutcome selection of child output
	// references (identity only, no child state copied into the parent).
	ChildOutputs []OutputReference `json:"child_outputs,omitempty"`
	Payload      json.RawMessage   `json:"payload,omitempty"`
}

type Snapshot struct {
	SchemaVersion   int                  `json:"schema_version"`
	Instance        WorkflowInstance     `json:"instance"`
	AppliedEventIDs []EventID            `json:"applied_event_ids,omitempty"`
	Idempotency     map[string][]EventID `json:"idempotency,omitempty"`
}

// MarshalJSON guarantees that emitted snapshots can be strictly decoded.
func (snapshot Snapshot) MarshalJSON() ([]byte, error) {
	type snapshotWire Snapshot
	data, err := json.Marshal(snapshotWire(snapshot))
	if err != nil {
		return nil, err
	}
	if err := preflightJSON(data); err != nil {
		return nil, err
	}
	if err := requireCanonicalSnapshotKeys(data); err != nil {
		return nil, err
	}
	return data, nil
}

// UnmarshalJSON preserves arbitrary-precision JSON numbers stored in metadata.
func (snapshot *Snapshot) UnmarshalJSON(data []byte) error {
	if err := preflightJSON(data); err != nil {
		return err
	}
	if err := requireCanonicalSnapshotKeys(data); err != nil {
		return err
	}
	type snapshotWire Snapshot
	var decoded snapshotWire
	if err := decodeModelJSON(data, &decoded); err != nil {
		return err
	}
	*snapshot = Snapshot(decoded)
	return nil
}

func decodeModelJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}
