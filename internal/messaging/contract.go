package messaging

import "github.com/sdougbrown/avenor/internal/runstate"

// CallerKind names who invoked a tool: the control socket's owner connection or
// a broker-authenticated run.
type CallerKind string

const (
	CallerOwner  CallerKind = "owner"
	CallerBroker CallerKind = "broker"
)

// Caller names who invoked a tool. RunID is the broker/runtime id the caller
// acts as; the Go host's owner caller passes the literal "supervisor", which is
// the supervisor session's broker run id.
type Caller struct {
	Kind  CallerKind // owner | broker
	RunID string     // broker/runtime id of the caller; the Go host owner passes the literal "supervisor"
}

// Surface names the model-facing surface that invoked the tool.
type Surface string

const (
	SurfaceMCP          Surface = "mcp"
	SurfaceChanneltools Surface = "channeltools"
	SurfaceSidecar      Surface = "sidecar"
	SurfacePiHost       Surface = "pi-host"
	SurfacePiChild      Surface = "pi-child"
	SurfaceOpenCode     Surface = "opencode"
	SurfaceMCPTS        Surface = "mcp-ts"
)

// ResolutionMatch reports how a resolver matched the addressed target.
type ResolutionMatch string

const (
	MatchUnique    ResolutionMatch = "unique"
	MatchAmbiguous ResolutionMatch = "ambiguous"
	MatchNone      ResolutionMatch = "none"
)

// Resolution is the resolved target of one tool call. RunID is the only id a
// transport accepts; PublicRunID is the host-surface id when the surface has
// one.
type Resolution struct {
	RunID       string          // broker/runtime id; the only id transports accept
	PublicRunID string          // host-surface run id; empty on child surfaces
	Match       ResolutionMatch // unique | ambiguous | none
	Label       string          // the target's display name: SessionInfo.Label, else its run id
	Candidates  []string        // run ids for an ambiguous label; empty when unique or none
}

// RunFacts are the runtime facts the route table reads.
type RunFacts struct {
	RunID     string
	Status    runstate.RuntimeStatus
	Phase     runstate.Phase
	Live      bool // a turn is in flight: !runstate.IsTurnComplete(Status, Phase)
	Resumable bool // a resumable sentinel status, or a live session id exists
}

// ResumeRequest continues a prior run in a new one. Target is the resolved
// broker/runtime id; RunID, SentinelFile and EventLog are minted by the calling
// provider, not by the router.
type ResumeRequest struct {
	RunID          string // new public run id, minted by the calling provider
	Target         string // prior run: the RESOLVED broker/runtime id
	Message        string
	Label          string
	IdempotencyKey string
	SentinelFile   string // path the provider minted for the new run
	EventLog       string // path the provider minted for the new run
}

// ResumeResult reports the new run a resume produced.
type ResumeResult struct {
	RunID        string // echoes ResumeRequest.RunID
	Label        string
	RuntimeID    string // new runtime id, rt_N
	SessionID    string
	SentinelFile string
	OnEvent      string
}

// RenameRequest renames one run's display name. An empty SessionID means the
// caller's own run for a broker caller and the supervisor session for the owner.
type RenameRequest struct {
	SessionID string
	Name      string // the new display name; becomes SessionInfo.Label
}

// RenameResult reports the one display-name field before and after the write.
// There is no second name field: Label is SessionInfo.Label and is what
// /sessions, /send's sender and the resolution ladder all read.
type RenameResult struct {
	RunID         string // resolved broker/runtime id
	Label         string // the new SessionInfo.Label
	PreviousLabel string // the label before the rename (the launch label if never renamed)
}

// PeerRecord is one peers `list` entry: the tool's projection of the broker's
// SessionInfo. There is no name key — Label is the display name, so the list is
// also the rename's read path — and last_seen is deliberately not part of the
// tool contract.
type PeerRecord struct {
	RunID   string `json:"run_id"`
	Label   string `json:"label,omitempty"`
	Kind    string `json:"kind,omitempty"` // "supervisor" | "agent", from the broker's SessionInfo
	Status  string `json:"status,omitempty"`
	Backend string `json:"backend,omitempty"`
	Model   string `json:"model,omitempty"`
	Dir     string `json:"dir,omitempty"`
}

// PeerLast is one peer's most recent completed turn, as the peers `last` action
// reports it. Text is already bounded by the producer.
type PeerLast struct {
	RunID      string // broker/runtime id of the target
	Label      string // target session label, else the run id
	HasRecord  bool   // false = no completed turn recorded for this run
	Text       string // empty when the turn produced no final text
	StopReason string // verbatim from the producer
	Truncated  bool   // Text hit the bound
}
