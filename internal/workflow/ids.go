package workflow

import "github.com/google/uuid"

type (
	TemplateID       string
	TemplateVersion  string
	WorkflowID       string
	InstanceID       string
	NodeID           string
	ActivationID     string
	AttemptID        string
	EventID          string
	CommandID        string
	LeaseID          string
	EvidenceID       string
	GateID           string
	GateInstanceID   string
	OutputID         string
	LoopID           string
	ChildReferenceID string
	OutcomeName      string
)

func newID(prefix string) string { return prefix + "_" + uuid.NewString() }

func NewWorkflowID() WorkflowID             { return WorkflowID(newID("wf")) }
func NewInstanceID() InstanceID             { return InstanceID(newID("wfi")) }
func NewActivationID() ActivationID         { return ActivationID(newID("act")) }
func NewAttemptID() AttemptID               { return AttemptID(newID("att")) }
func NewEventID() EventID                   { return EventID(newID("wfe")) }
func NewCommandID() CommandID               { return CommandID(newID("wfc")) }
func NewLeaseID() LeaseID                   { return LeaseID(newID("lease")) }
func NewEvidenceID() EvidenceID             { return EvidenceID(newID("ev")) }
func NewGateInstanceID() GateInstanceID     { return GateInstanceID(newID("gate")) }
func NewOutputID() OutputID                 { return OutputID(newID("out")) }
func NewChildReferenceID() ChildReferenceID { return ChildReferenceID(newID("child")) }
