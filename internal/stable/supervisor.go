package stable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/diagnote"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askclient"
	"github.com/sdougbrown/avenor/internal/messaging/inprocess"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/rosterconfig"
	"github.com/sdougbrown/avenor/internal/runstate"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/factory"
	"github.com/sdougbrown/avenor/internal/spawnselection"
	"github.com/sdougbrown/avenor/internal/teamrunner"
	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

type Config struct {
	ControlSocket          string
	TombstoneFile          string
	HTTPDebug              string
	MaxRuntimes            int
	IdleTimeout            time.Duration
	ShutdownTimeout        time.Duration
	PermissionClaimTimeout time.Duration
	ChildQuestionTimeout   time.Duration

	// ParkedRuntimeTimeout reaps a runtime parked after a successful turn.
	// A parked runtime holds a live backend process for follow-up prompts but
	// does not count against MaxRuntimes. Zero disables reaping: parked
	// runtimes are kept until the supervisor shuts down or they are canceled.
	ParkedRuntimeTimeout time.Duration

	// MaxTreeBudget is the inherited descendant budget capacity for a root
	// supervisor tree. It bounds the total concurrent runtimes across the
	// whole tree, including nested supervisors. Zero uses
	// admission.DefaultTreeBudget. Ignored when TreeBudgetFile is set (a
	// nested supervisor joins an existing tree rather than creating one).
	MaxTreeBudget int

	// IdempotencyCapacity bounds the in-memory spawn idempotency store:
	// completed entries plus distinct in-flight reservations. Zero or negative
	// uses defaultIdempotencyCapacity.
	IdempotencyCapacity int

	// TreeBudgetFile is the path of an existing tree budget to join. When
	// empty, the supervisor is a root and creates a new budget file in
	// Avenor-owned runtime state. When set, the supervisor opens the existing
	// file as a nested participant sharing the root's capacity.
	TreeBudgetFile string

	// WorkflowRoot is the durable workflow-store root. When empty it defaults
	// to $XDG_STATE_HOME/avenor/workflows (or $HOME/.avenor/workflows). The
	// workflow manager creates the directory lazily on first workflow use.
	WorkflowRoot string

	// WorkflowAdapterDir is the host-owned directory of trusted external-gate
	// adapter manifests. When empty it defaults to
	// $XDG_CONFIG_HOME/avenor/workflow-adapters (or
	// $HOME/.config/avenor/workflow-adapters).
	WorkflowAdapterDir string

	// WorkflowLeaseSweepInterval is the cadence of the supervisor's live
	// workflow lease-expiry sweep (Manager.ExpireStaleLeases): an activation
	// whose lease stopped being heartbeated while the supervisor is running
	// is expired within one interval and its node re-dispatched, without a
	// restart. Zero disables the sweep: leases then expire only on restart
	// recovery. The CLI flag defaults to DefaultLeaseSweepInterval.
	WorkflowLeaseSweepInterval time.Duration

	// Stderr is the writer diagnostics and sentinel-write failures are
	// reported to. Nil means s.stderrWriter(). Tests inject a pipe to capture
	// output without touching the process-global s.stderrWriter(), which leaked
	// runChild goroutines would otherwise race against.
	Stderr io.Writer

	// MCPLaunchWrapperPolicy gates the launch_wrapper parameter on MCP-driven
	// spawns (the control spawn_mcp verb). Nil disables launch_wrapper for
	// MCP on this supervisor: any MCP spawn carrying a wrapper is rejected.
	// A non-nil policy permits only argv arrays that exactly match an entry in
	// the operator's allowlist. Control-socket spawns and the CLI are
	// operator-trusted and are not gated by this policy.
	MCPLaunchWrapperPolicy *MCPLaunchWrapperPolicy
}

// DefaultLeaseSweepInterval is the default live lease-expiry cadence: a
// third of workflow.DefaultLeaseTTL, so a dead lease is always detected
// well within one TTL window.
const DefaultLeaseSweepInterval = workflow.DefaultLeaseTTL / 3

type SpawnParams struct {
	Prompt       string `json:"prompt,omitempty"`
	PromptFile   string `json:"prompt_file,omitempty"`
	Dir          string `json:"dir"`
	Agent        string `json:"agent,omitempty"`
	AgentProfile string `json:"agent_profile,omitempty"`
	Label        string `json:"label,omitempty"`
	Model        string `json:"model,omitempty"`
	Thinking     string `json:"thinking,omitempty"`
	ServerURL    string `json:"server_url,omitempty"`
	Backend      string `json:"backend,omitempty"`
	// LaunchWrapper is an opaque argv prefix placed in front of the backend
	// process command at launch. Avenor does not interpret it; empty or
	// absent means the backend is launched unwrapped. Probes and helper
	// processes are never wrapped.
	LaunchWrapper     []string `json:"launch_wrapper,omitempty"`
	OnEvent           string   `json:"on_event,omitempty"`
	SentinelFile      string   `json:"sentinel_file,omitempty"`
	PermissionHandler string   `json:"permission_handler,omitempty"`
	AutoApprove       bool     `json:"auto_approve,omitempty"`
	// PermissionDecider routes this runtime's permission requests to a
	// deciding agent (the resolved permission policy's decider) instead of a
	// local resolver: no file permission handler is derived, and a pending
	// request spawns the decider run, which answers through the control
	// plane or defers (leaving the request pending for a human). Mutually
	// exclusive with AutoApprove.
	PermissionDecider *workflow.PermissionDecider `json:"permission_decider,omitempty"`
	// PermissionModelDecider resolves this runtime's permission requests
	// with a direct model call avenor itself makes: no runtime is spawned,
	// no file permission handler is derived, and an allow/reject answer is
	// applied through the control plane while a defer (or any failed or
	// unparseable call) leaves the request pending for a human. Mutually
	// exclusive with AutoApprove and PermissionDecider.
	PermissionModelDecider *workflow.PermissionModelDecider `json:"permission_model_decider,omitempty"`
	Timeout                int                              `json:"timeout,omitempty"`
	MaxRetries             int                              `json:"max_retries,omitempty"`
	LoopFile               string                           `json:"loop_file,omitempty"`
	TeamFile               string                           `json:"team_file,omitempty"`
	RosterFile             string                           `json:"roster_file,omitempty"`
	RosterEntry            string                           `json:"roster_entry,omitempty"`
	SessionID              string                           `json:"session_id,omitempty"`
	ParentID               string                           `json:"parent_id,omitempty"`     // runtime ID of the parent agent
	ParentRunID            string                           `json:"parent_run_id,omitempty"` // broker run ID of the parent, for channel messaging
	// IdempotencyKey makes a retried spawn return the stored result of the
	// first spawn under the same key instead of starting a second runtime.
	// Empty disables idempotency.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// LabelDerived marks Label as a per-attempt generated value (an MCP run
	// ID) rather than a semantic parameter; derived labels are excluded from
	// the idempotency parameter hash.
	LabelDerived bool `json:"label_derived,omitempty"`
	// Workflow execution identity attached to this run. Populated by the
	// workflow direct-run executor; the supervisor attaches its own identity
	// and callers cannot spoof it.
	WorkflowID   string `json:"workflow_id,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	ActivationID string `json:"activation_id,omitempty"`
	AttemptID    string `json:"attempt_id,omitempty"`
	// workflowTerminate is the workflow attempt's termination callback. It is
	// installed on the runtime at construction, before the runtime can run,
	// so a runtime that finishes before its executor returns still records
	// its terminal fact. Set only by workflow executors.
	workflowTerminate func(child *childRuntime, status workflow.AttemptStatus)
	// permissionDeciderRun marks the spawn as a supervisor-spawned permission
	// decider for one pending request. It carries no JSON tag and is set only
	// by spawnPermissionDecider, so a caller-supplied label cannot make an
	// ordinary runtime end after its first turn.
	permissionDeciderRun bool
	// permissionDeciderSlotKey is the decider concurrency-slot key reserved
	// for this run at routing. It carries no JSON tag and is set only by
	// spawnPermissionDecider, from the same key the slot was acquired with,
	// so teardown releases the run's own slot even if another runtime wears
	// the same label.
	permissionDeciderSlotKey string
}

type SpawnResult struct {
	RuntimeID        string `json:"runtime_id"`
	SessionID        string `json:"session_id"`
	OnEvent          string `json:"on_event"`
	SentinelFile     string `json:"sentinel_file"`
	BrokerURL        string `json:"broker_url,omitempty"`
	ParentToken      string `json:"parent_token,omitempty"`
	Agent            string `json:"agent,omitempty"`
	AgentProfile     string `json:"agent_profile,omitempty"`
	Model            string `json:"model,omitempty"`
	Backend          string `json:"backend,omitempty"`
	RosterFile       string `json:"roster_file,omitempty"`
	RosterEntry      string `json:"roster_entry,omitempty"`
	EffectiveAgent   string `json:"effective_agent,omitempty"`
	EffectiveModel   string `json:"effective_model,omitempty"`
	EffectiveBackend string `json:"effective_backend,omitempty"`
}

type permissionProviderLifecycle struct {
	provider                   runtime.Provider
	sessionID                  string
	answerCtx                  context.Context
	cancelAnswers              context.CancelFunc
	providerCalls              *cli.ProviderLifecycle
	turn                       *cli.ProviderTurn
	closing                    bool
	directAnswers              int
	directAnswersDone          chan struct{}
	permissionReservations     int
	permissionReservationsDone chan struct{}
}

type permissionProviderBinding struct {
	lifecycle *permissionProviderLifecycle
	turn      *cli.ProviderTurn
	sessionID string
	writer    *runtimeFanoutWriter
}

type directPermissionReservation struct {
	child     *childRuntime
	lifecycle *permissionProviderLifecycle
	turn      *cli.ProviderTurn
	sessionID string
	ctx       context.Context
	writer    *runtimeFanoutWriter
}

type childRuntime struct {
	id               string
	label            string
	agent            string
	agentProfile     string
	model            string
	thinking         string
	backend          string
	launchWrapper    []string
	rosterFile       string
	rosterEntry      string
	effectiveBackend string
	effectiveAgent   string
	effectiveModel   string
	roster           *rosterconfig.Config
	parentID         string   // runtime ID of the parent agent, empty for top-level
	children         []string // runtime IDs spawned by this runtime
	startedAt        int64
	provider         runtime.Provider
	session          runtime.Session
	eventWriter      cli.EventSink
	fileHandler      *permission.FileHandler
	autoApprove      bool
	permClaimTimeout time.Duration
	// permissionDecider carries the resolved permission policy's decider;
	// nil means the runtime uses the ordinary permission resolver behavior.
	permissionDecider *workflow.PermissionDecider
	// modelDecider carries the resolved direct-model decider; nil means no
	// direct-model resolution. Mutually exclusive with permissionDecider.
	modelDecider *workflow.PermissionModelDecider
	// modelDeciderKey is the resolved API key for modelDecider's provider,
	// resolved once at spawn and held only here, in supervisor memory: it
	// never enters the pinned selection, snapshots, or status surfaces.
	modelDeciderKey string
	// task and taskFile carry the spawn prompt for the direct-model
	// permission decider's prompt context: what the node was dispatched to
	// do, so the decision can judge whether the request plainly serves it.
	task     string
	taskFile string
	// modelDeciderCalls counts the direct-model decision calls this runtime
	// has made; past maxModelDeciderCallsPerRuntime further requests stay
	// pending for a human, so a worker in a permission-gated loop cannot
	// generate unbounded billable calls.
	modelDeciderCalls int
	// routedPermissionRequests records request IDs whose pending permission
	// request already spawned a decider run, so a repeated request event
	// cannot spawn a second decider for the same request.
	routedPermissionRequests map[string]struct{}
	// permissionDeciderFailures counts failed decider spawns per request ID.
	// Past maxPermissionDeciderSpawnFailures the request keeps its routed
	// mark: no further spawns fire and the request stays pending for a human.
	permissionDeciderFailures map[string]int
	// permissionDeciderExhausted is surfaced in runtime status once any
	// request on this runtime exhausted its decider retries (harness decider
	// spawns or direct-model decision calls).
	permissionDeciderExhausted bool
	runID                      string
	dir                        string
	onEvent                    string
	sentinelFile               string
	lifecycleCtx               context.Context
	cancelFn                   func()
	interruptFn                func()
	done                       chan struct{}
	doneOnce                   sync.Once
	runDoneOnce                sync.Once
	// runDone is a test-observable join point: it is closed when the child's
	// run goroutine exits (after its post-completion work, such as the
	// capacity release that follows complete) or by the rollback/finish path
	// when no run goroutine ever started. Production never reads it; test
	// teardown joins on it before asserting lifecycle counters.
	runDone     chan struct{}
	exitCode    int
	promptCh    chan struct{}
	promptQueue []string
	// turnWaiters are wait_turn callers blocked on the runtime's next
	// running→idle transition. Each waiter registers a channel under child.mu;
	// settleTurnLocked closes them when the active turn settles, so a settle
	// can never be missed by a waiter that registered before it.
	turnWaiters []chan struct{}
	// lastStopReason mirrors the stop_reason of the most recent session.end
	// observed on this runtime's event path (runtimeFanoutWriter.writeLocked).
	lastStopReason string
	latestSeq      int64
	usage          map[string]any
	// finalOutput is a bounded status preview; fullFinalOutput is returned
	// only through the explicit result control method.
	finalOutput          string
	finalOutputTruncated bool
	fullFinalOutput      string
	// Per-runtime status mirror of control.Snapshot fields. Updated from the
	// event fanout so RuntimeStatus can surface phase, phase_label, and
	// pending_permission without consulting the shared ControlState (which
	// reflects only the most recent event across ALL runtimes).
	phase      runstate.Phase
	phaseLabel string
	// unknownPhases gates the one-line-per-value diagnostics for raw phase
	// values the fanout could not parse. The gate serializes its own state
	// with its internal lock; its Admit decisions are made inside
	// writeLocked's child.mu critical section (every writeLocked caller also
	// holds writeMu), next to the status mirror update it deduplicates. The
	// diagnostic is logged after every lock is released.
	unknownPhases     diagnote.Gate
	pendingPermission bool
	permission        map[string]any
	directAttempt     *sessionAttempt
	// lc is the closed lifecycle state owned by the pure machine
	// (runtime_lifecycle.go) and committed field by field by the adapter
	// (runtime_lifecycle_apply.go). Read it through the field-specific
	// predicates under the locks each reader already holds.
	lc                lifecycleState
	mu                sync.Mutex
	writeMu           sync.Mutex
	fanoutWriter      *runtimeFanoutWriter
	providerLifecycle *permissionProviderLifecycle
	brokerToken       string // broker token for this run (mirrors StartOptions.BrokerToken)

	directAnswers     int
	directAnswersDone chan struct{}

	// workflow execution identity. Populated by the workflow direct-run executor;
	// the supervisor attaches its own identity and callers cannot spoof it.
	workflowID   string
	nodeID       string
	activationID string
	attemptID    string
	// workflowMarkerLabel is the terminal marker label a loop or team child
	// stashed before exiting (the aggregate run result does not otherwise
	// reach the termination path). Empty for direct runs and marker-less
	// loop/team exits.
	workflowMarkerLabel string
	// permissionDeciderRun marks this runtime as a supervisor-spawned
	// permission decider: its single turn is terminal and teardown releases
	// its decider slot and scratch directory. Only decider spawns set it.
	permissionDeciderRun bool
	// permissionDeciderSlotKey is the decider concurrency-slot key this run
	// reserved, recorded so teardown releases exactly that reservation. It
	// equals the decider's label today; the permissionDeciderRun flag, not
	// this key, keeps an ordinary runtime with a decider's label from
	// releasing a slot. Empty for ordinary runs.
	permissionDeciderSlotKey string
	// onWorkflowTerminate records the terminal workflow attempt fact before the
	// child's runtime state is cleaned up. Nil for non-workflow direct runs.
	onWorkflowTerminate func(workflow.AttemptStatus)

	// treeToken is the current tree-budget admission token held by this runtime.
	// It is non-empty while the runtime is actively executing a turn and empty
	// while parked between turns. Managed by runChild; released by complete()
	// as a safety net for terminal failure paths.
	treeToken string
	// treeBudgetRelease returns a token to the tree budget. Set by spawn when
	// admission is active. Nil in degraded mode (no tree budget).
	treeBudgetRelease func(token string)
}

type effectiveIdentity struct {
	Backend      string
	Agent        string
	Model        string
	AgentProfile string
	RosterFile   string
	RosterEntry  string
	// LaunchWrapper is the wrapper argv the owning process was launched
	// behind. It is part of the ownership record so a resume cannot hand a
	// session to a differently wrapped backend.
	LaunchWrapper []string
}

type sessionIdentityEntry struct {
	identity effectiveIdentity
}

// sessionAttempt is the ownership token for one provider/session attempt.
// Ownership is deliberately independent of childRuntime.provider/session: team
// phases run in parallel and the child fields are only an aggregate status
// view, not an authority boundary.
type sessionAttempt struct {
	provider             runtime.Provider
	identity             effectiveIdentity
	provisionalID        string
	authoritativeID      string
	resumeID             string
	provisionalWasMapped bool
	rejected             bool
}

type workflowSession struct {
	SessionID string
	Identity  effectiveIdentity
	Sequence  uint64
}

type workflowSessionTracker struct {
	mu       sync.Mutex
	sequence uint64
	byPhase  map[string]workflowSession
}

func newWorkflowSessionTracker() *workflowSessionTracker {
	return &workflowSessionTracker{byPhase: make(map[string]workflowSession)}
}

func (t *workflowSessionTracker) remember(phaseName, sessionID string, identity effectiveIdentity) {
	if sessionID == "" {
		return
	}
	t.mu.Lock()
	t.sequence++
	t.byPhase[phaseName] = workflowSession{SessionID: sessionID, Identity: identity, Sequence: t.sequence}
	t.mu.Unlock()
}

func (t *workflowSessionTracker) latest() (workflowSession, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var latest workflowSession
	for _, session := range t.byPhase {
		if session.Sequence > latest.Sequence {
			latest = session
		}
	}
	return latest, latest.Sequence != 0
}

func (t *workflowSessionTracker) final(groups ...[]phaseconfig.Phase) (workflowSession, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, phases := range groups {
		for i := len(phases) - 1; i >= 0; i-- {
			if session, ok := t.byPhase[phases[i].Name]; ok {
				return session, true
			}
		}
	}
	return workflowSession{}, false
}

type pendingChildQuestion struct {
	requestID string
	timer     *time.Timer
}

type handledChildQuestion struct {
	requestID string
	at        time.Time
}

// testHooks holds test-only seams used by tests to deterministically
// interleave commands with the supervisor's read windows. Every hook must be
// set before the goroutine that reads it starts (the workflow startup
// barrier or the loop's launch): the goroutines read hook fields without
// locking, and the set happens before the launching goroutine statement.
type testHooks struct {
	// controllerStatusPreNextPoll, when non-nil (set by tests), runs after
	// the controller record is read but before the next-poll re-read so a
	// test can corrupt the snapshot inside that window deterministically.
	// nil in production.
	controllerStatusPreNextPoll func()
	// afterAttemptSpawn, when non-nil (set by tests), runs in a workflow
	// executor right after the attempt's runtime is spawned, with its runtime
	// ID, so a test can let the runtime finish before Dispatch returns.
	afterAttemptSpawn func(runtimeID string)
	// completeAutoPre, when non-nil (set by tests), runs immediately before
	// the supervisor issues a node's auto-completion command. A non-nil error
	// stands in for that command failing. nil in production.
	completeAutoPre func() error
	// leaseSweepPost, when non-nil (set by tests), runs after every live
	// lease-expiry sweep tick with its summary. nil in production.
	leaseSweepPost func(summary workflow.LeaseExpirySummary)
	// leaseSweepTick, when non-nil (set by tests), replaces the sweep loop's
	// real ticker: the loop waits on this channel instead, so a test steps
	// sweeps explicitly and no tick ever fires on its own. nil in production.
	leaseSweepTick <-chan time.Time
	// leaseSweepLoopStart, when non-nil (set by tests), runs in
	// startLeaseSweepLoop immediately before the loop goroutine launches, so a
	// test can assert structurally that no loop was started. nil in production.
	leaseSweepLoopStart func()
	// workflowNow, when non-nil (set by tests before the workflow startup
	// barrier), becomes the workflow manager's lease-liveness clock (lease
	// claims, heartbeat renewals, and stale-lease sweeps). nil in production.
	workflowNow func() time.Time
	// lifecycleTransitionFailure, when non-nil (set by tests), is consulted by
	// the lifecycle transaction before the machine validates a signal: a true
	// return rejects the transition exactly like an illegal one (unchanged
	// state, no effects), so a test can drive the fault fail-safe at a chosen
	// site. nil in production.
	lifecycleTransitionFailure func(site signalKind) bool
	// directCleanupPost, when non-nil (set by tests before the goroutine that
	// runs the cleanup starts), runs inside a delivered direct answer's cleanup
	// after the request state is removed and before the claim is resolved. The
	// cleanup's control lock is held and the router lock is not. nil in
	// production.
	directCleanupPost func()
}

type Supervisor struct {
	config  Config
	runID   string
	control *control.ControlServer
	// idempotency gates retried spawns on their idempotency key so a retry
	// never starts a second runtime.
	idempotency *idempotencyStore
	// workflowOnce runs the startup barrier exactly once and publishes its
	// retained results below; reads after workflowOnce.Do are safe.
	workflowOnce        sync.Once
	workflowMgr         *workflow.Manager
	workflowBarrierErr  error
	workflowControllers *workflowcontroller.ControllerStore
	// workflowAdapters is the immutable adapter manifest registry loaded at
	// startup and on every enable; nil when no directory loaded (every
	// adapter reports unavailable).
	workflowAdapters atomic.Pointer[workflowcontroller.AdapterRegistry]
	// controllerLoopsMu guards controllerLoops and loopsStopped: one
	// leader-loop handle per enabled controller id in this process.
	controllerLoopsMu sync.Mutex
	controllerLoops   map[string]*controllerLoop
	loopsStopped      bool
	// workflowControllerMu serializes the enable/disable handler bodies so the
	// in-process loop state always matches the last persisted desired state.
	workflowControllerMu sync.Mutex
	// controllerRenewInterval is the leader loop's renew cadence; defaults to
	// workflowcontroller.RenewInterval.
	controllerRenewInterval time.Duration
	// candidateRefreshMu guards lastCandidateRebuild and
	// candidateRefreshWindow: the supervisor-wide share of candidate-index
	// rebuilds across every controller's runner deps. candidateRefreshWindow
	// matches the runner's anti-entropy interval so a tick's N enabled
	// controllers cause one catalog scan instead of N.
	candidateRefreshMu     sync.Mutex
	lastCandidateRebuild   time.Time
	candidateRefreshWindow time.Duration
	// candidateRebuilds counts every candidate-index rebuild performed by
	// stableRunnerDeps.Refresh (test instrumentation).
	candidateRebuilds atomic.Int64
	// lifecycleErrors counts every illegal lifecycle transition reported by
	// the lifecycle machine; it must be zero after teardown.
	lifecycleErrors atomic.Int64
	// controllerPollBaseDelay is the interval before a parked gate's first
	// adapter poll; defaults to 30s. Tests shorten it.
	controllerPollBaseDelay time.Duration
	// testHooks holds seams set only by tests. nil in production; tests set
	// them on the instance they drive to make concurrent commands land
	// deterministically inside read–commit windows.
	testHooks testHooks
	// leaseSweepMu guards leaseSweepStop, leaseSweepDone, and
	// leaseSweepStopped: the single live lease-expiry sweep goroutine started
	// once the workflow startup barrier succeeds and stopped on shutdown. Once
	// stopped, the sweep never starts again, so a lazy barrier that completes
	// during shutdown cannot launch a loop the shutdown join already passed.
	leaseSweepMu      sync.Mutex
	leaseSweepStop    chan struct{}
	leaseSweepDone    chan struct{}
	leaseSweepStopped bool
	// errWriter is the diagnostics writer: Config.Stderr when set, else the
	// process stderr. Set once at construction and only read afterwards, so
	// goroutines can use it without synchronization.
	errWriter io.Writer
	state     *control.ControlState
	controlMu sync.Mutex
	runtimes  map[string]*childRuntime
	nextID    int
	// outstandingReservations counts admission reservations that hold a local
	// slot but have not yet converted into a registered runtime. Guarded by
	// controlMu; the local capacity limit is enforced against active runtimes
	// plus this count.
	outstandingReservations int
	shuttingDown            bool
	shutdownCh              chan struct{}
	shutdownChOnce          sync.Once
	// heartbeatMu guards heartbeats, the registry of live executor lease
	// heartbeat goroutines stopped on supervisor shutdown.
	heartbeatMu      sync.Mutex
	heartbeats       map[*leaseHeartbeat]struct{}
	runtimeActivity  chan struct{}
	childQuestionSeq int
	pendingQuestions map[string]pendingChildQuestion // child runtime ID -> pending question
	handledQuestions map[string]handledChildQuestion // child runtime ID -> latest handled request
	// childQuestionTimeout is the deadline for a parent to answer a child's
	// question before the child is prompted to continue on its own.
	childQuestionTimeout          time.Duration
	childQuestionTimeoutDefaulted bool
	httpServer                    *control.HTTPDebugServer
	// permissions owns supervisor-level permission routing state, decider
	// accounting and the provider bindings direct answers target.
	permissions         *permissionRouter
	httpServers         map[string]*managedHTTPServer // dir → ready server
	httpStarting        map[string]struct{}           // dirs with a start in flight
	httpShutdownStarted bool
	httpServerMu        sync.Mutex
	httpServerCond      *sync.Cond
	fileSnapshots       map[string][]string // runtimeID → pre-run file list for output detection
	fileSnapMu          sync.Mutex
	broker              *broker.Broker
	// messaging is the supervisor's own transport over the broker's ask store.
	// It is the only transport the supervisor closes, and it never outlives
	// the broker.
	messaging                    messaging.Transport
	brokerRunID                  string
	brokerToken                  string
	brokerRunMu                  sync.Mutex
	brokerHTTPClientOnce         sync.Once
	brokerHTTPClient             *http.Client // private transport: never reuses another broker's pooled connections
	newProviderFunc              func(startOpts runtime.StartOptions, backend string) (runtime.Provider, error)
	sessionIdentityMu            sync.RWMutex
	sessionIdentities            map[string]sessionIdentityEntry
	sessionOwners                map[string]*sessionAttempt
	beforeChildWriterCloseWait   func()
	beforeChildProviderCloseWait func()
	afterShutdownAdmissionClosed func()

	// Tree-scoped admission controller. Nil when the budget could not be
	// created or opened (degraded mode: only the local MaxRuntimes limit is
	// enforced). Protected by treeBudgetMu for the reaper goroutine.
	treeBudgetMu   sync.Mutex
	treeBudget     *admission.Budget
	treeBudgetErr  string // non-empty when the optional budget is unavailable
	reaperStop     chan struct{}
	reaperDone     chan struct{}
	reaperInterval time.Duration
	// capacityMu guards capacityCh, a broadcast channel closed (and replaced) on
	// every tree-budget release so callers waiting for capacity can retry. It
	// does not schedule or prioritize work.
	capacityMu sync.Mutex
	capacityCh chan struct{}
	// capacitySubMu guards capacitySubs, a coalescing fan-out of buffered(1)
	// wake channels notified on every capacity change.
	capacitySubMu sync.Mutex
	capacitySubs  []chan struct{}
}

func NewSupervisor(cfg Config) *Supervisor {
	runID := cli.GenerateRunID()
	state := control.NewState(runID, "", 0)
	// The owner capability is generated before the control server is built so
	// construction, not the first mutation, fixes who may control this process.
	owner, err := control.GenerateOwnerCapability()
	if err != nil {
		panic("avenor: failed to generate owner capability: " + err.Error())
	}
	sup := &Supervisor{
		config:                  cfg,
		runID:                   runID,
		state:                   state,
		control:                 control.NewServer(state, owner),
		idempotency:             newIdempotencyStore(cfg.IdempotencyCapacity),
		runtimes:                map[string]*childRuntime{},
		controllerLoops:         map[string]*controllerLoop{},
		controllerRenewInterval: workflowcontroller.RenewInterval,
		candidateRefreshWindow:  workflowcontroller.DefaultAntiEntropy,
		shutdownCh:              make(chan struct{}),
		runtimeActivity:         make(chan struct{}),
		pendingQuestions:        map[string]pendingChildQuestion{},
		handledQuestions:        map[string]handledChildQuestion{},
		childQuestionTimeout:    cfg.ChildQuestionTimeout,
		reaperInterval:          5 * time.Second,
		httpServers:             map[string]*managedHTTPServer{},
		httpStarting:            map[string]struct{}{},
		fileSnapshots:           map[string][]string{},
		sessionIdentities:       map[string]sessionIdentityEntry{},
		sessionOwners:           map[string]*sessionAttempt{},
		heartbeats:              map[*leaseHeartbeat]struct{}{},
		errWriter:               cfg.Stderr,
	}
	sup.broker = broker.New("")
	if err := sup.broker.Start(); err != nil {
		// Non-fatal — broker is optional. Runs will still work without it.
	} else {
		// Register the supervisor run eagerly so sub-agents can target it for
		// asks before the host performs any outbound broker call. Without this,
		// a child asking its parent first would fail "to run not found".
		sup.registerBrokerRun()
		// Both the in-process transport and the HTTP router read this store,
		// so an ask the supervisor makes is the same ask a child can answer
		// over HTTP.
		sup.messaging = inprocess.New(sup.broker.Store())
	}
	sup.httpServerCond = sync.NewCond(&sup.httpServerMu)
	sup.capacityCh = make(chan struct{})
	if sup.childQuestionTimeout <= 0 {
		sup.childQuestionTimeout = 120 * time.Second
		sup.childQuestionTimeoutDefaulted = true
	}
	sup.control.SetStableHandler(sup)
	sup.control.SetWorkflowHandler(lazyWorkflowHandler{sup})
	sup.control.SetWorkflowControllerHandler(sup)
	sup.newProviderFunc = factory.NewProvider
	sup.permissions = newPermissionRouter(supervisorPermissionHost{sup: sup}, sup.control, &sup.testHooks)
	sup.initTreeBudget()
	return sup
}

// stderrWriter returns the writer supervisor diagnostics are reported to:
// Config.Stderr when set, else the process stderr.
func (s *Supervisor) stderrWriter() io.Writer {
	if s.errWriter != nil {
		return s.errWriter
	}
	return os.Stderr
}
func (s *Supervisor) Run() (exitCode int) {
	// NewSupervisor initializes root admission before the control socket is
	// bound. Close it on every exit, including a control-server start failure.
	defer s.closeTreeBudget()

	// Operators must be able to tell the effective child question timeout
	// and whether it was explicitly configured or defaulted.
	source := "configured"
	if s.childQuestionTimeoutDefaulted {
		source = "default"
	}
	fmt.Fprintf(s.stderrWriter(), "avenor stable: child question timeout %s (%s)\n", s.childQuestionTimeout, source)

	var reason string
	defer func() {
		if r := recover(); r != nil {
			s.writeTombstone("crashed")
			panic(r)
		}
		if reason != "" {
			s.writeTombstone(reason)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := s.control.Start(s.config.ControlSocket); err != nil {
		fmt.Fprintf(s.stderrWriter(), "avenor stable: start control server: %v\n", err)
		reason = "start_failed"
		return 1
	}
	defer func() {
		if err := s.control.Stop(); err != nil {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: stop control server: %v\n", err)
			if exitCode == 0 {
				exitCode = 1
			}
		}
	}()

	if s.config.HTTPDebug != "" {
		var err error
		s.httpServer, err = control.NewHTTPDebugServer(s.config.HTTPDebug, s.control)
		if err != nil {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: start http debug: %v\n", err)
			reason = "start_failed"
			return 1
		}
		s.httpServer.SetStableAdapter(s)
		if err := s.httpServer.Start(); err != nil {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: start http debug: %v\n", err)
			reason = "start_failed"
			return 1
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = s.httpServer.Stop(shutdownCtx)
		}()
	}

	var idleDeadline time.Time
	if s.config.IdleTimeout > 0 {
		idleDeadline = time.Now().Add(s.config.IdleTimeout)
	}

	s.startReaper()
	defer s.stopReaper()
	defer s.stopControllerLoops()

	// Exactly-once startup barrier: when durable workflow or controller state
	// already exists, recover it before any leader activity; otherwise the
	// barrier runs lazily on the first workflow RPC or controller command.
	if root := resolveWorkflowRoot(s.config.WorkflowRoot); hasExistingWorkflowState(root) {
		s.workflowOnce.Do(s.runWorkflowStartupBarrier)
	}

	for {
		idleCh := idleCheck(s.config.IdleTimeout, s.activeRuntimeCount(), &idleDeadline)
		select {
		case <-ctx.Done():
			reason = "signal"
			return s.shutdown("graceful")
		case <-s.shutdownCh:
			reason = "shutdown"
			return s.shutdown("graceful")
		case <-idleCh:
			reason = "idle"
			return s.shutdown("graceful")
		case <-s.runtimeActivity:
			continue
		}
	}
}

// initTreeBudget creates or joins the tree-scoped admission budget. A root
// supervisor (TreeBudgetFile empty) creates a new budget file in
// Avenor-owned runtime state. A nested supervisor (TreeBudgetFile set) opens
// the existing file to share the root's capacity. Failure is non-fatal: the
// supervisor continues with only its local MaxRuntimes limit enforced.
func (s *Supervisor) initTreeBudget() {
	path := s.config.TreeBudgetFile
	var (
		budget *admission.Budget
		err    error
	)
	if path == "" {
		budget, err = admission.CreateRootInRuntimeState(s.config.MaxTreeBudget)
	} else {
		budget, err = admission.Open(path)
	}
	if err != nil {
		action := "create"
		if path != "" {
			action = "join"
		}
		message := fmt.Sprintf("%s tree budget: %v", action, err)
		s.treeBudgetMu.Lock()
		s.treeBudgetErr = message
		s.treeBudgetMu.Unlock()
		// Admission is optional: preserve the local per-supervisor limit when
		// cross-process coordination state is unavailable, but make the weaker
		// guarantee visible through stderr and tree_budget status.
		fmt.Fprintf(s.stderrWriter(), "avenor stable: tree budget unavailable; using degraded local-only mode: %s\n", message)
		return
	}
	budget.AddNotifier(s.signalCapacityChange)
	s.treeBudgetMu.Lock()
	s.treeBudget = budget
	s.treeBudgetErr = ""
	s.treeBudgetMu.Unlock()
}

// closeTreeBudget closes the active budget. Closing a root also removes its
// Avenor-owned runtime-state file; nested supervisors only release their file
// handle. It is safe to call repeatedly.
func (s *Supervisor) closeTreeBudget() {
	s.treeBudgetMu.Lock()
	budget := s.treeBudget
	s.treeBudget = nil
	s.treeBudgetMu.Unlock()
	if budget != nil {
		_ = budget.Close()
	}
	s.signalCapacityChange()
}

// TreeBudgetPath returns the path of the tree budget file, or empty when no
// budget is active. The root command entry point sets AVENOR_TREE_BUDGET to
// this value so descendant processes inherit the tree identity.
func (s *Supervisor) TreeBudgetPath() string {
	s.treeBudgetMu.Lock()
	defer s.treeBudgetMu.Unlock()
	if s.treeBudget == nil {
		return ""
	}
	return s.treeBudget.Path()
}

// TreeBudgetStatus implements control.StableHandler and returns a
// diagnostic snapshot of the optional tree-scoped admission budget.
func (s *Supervisor) TreeBudgetStatus() any {
	s.treeBudgetMu.Lock()
	defer s.treeBudgetMu.Unlock()
	if s.treeBudget == nil {
		return map[string]any{
			"active":   0,
			"capacity": 0,
			"root_id":  "",
			"mode":     "degraded",
			"reason":   s.treeBudgetErr,
		}
	}
	active, capacity, rootID := s.treeBudget.Status()
	return map[string]any{
		"active":   active,
		"capacity": capacity,
		"root_id":  rootID,
		"mode":     "active",
	}
}

// startReaper launches a background goroutine that periodically reclaims
// capacity held by dead descendant processes. It runs only while the
// supervisor is active and stops before Run returns.
func (s *Supervisor) startReaper() {
	s.treeBudgetMu.Lock()
	if s.treeBudget == nil {
		s.treeBudgetMu.Unlock()
		return
	}
	s.reaperStop = make(chan struct{})
	s.reaperDone = make(chan struct{})
	stop := s.reaperStop
	s.treeBudgetMu.Unlock()
	go s.reapLoop(stop)
}

func (s *Supervisor) reapLoop(stop <-chan struct{}) {
	defer close(s.reaperDone)
	ticker := time.NewTicker(s.reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.treeBudgetMu.Lock()
			budget := s.treeBudget
			s.treeBudgetMu.Unlock()
			if budget == nil {
				return
			}
			if budget.Reap() > 0 {
				s.signalCapacityChange()
			}
		}
	}
}

// stopReaper halts the background reaper and waits for it to exit.
func (s *Supervisor) stopReaper() {
	s.treeBudgetMu.Lock()
	stop := s.reaperStop
	s.reaperStop = nil
	s.treeBudgetMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	<-s.reaperDone
}

// startLeaseSweepLoop launches the single live lease-expiry sweep goroutine.
// Call only after the workflow startup barrier has succeeded and only when
// Config.WorkflowLeaseSweepInterval is positive.
func (s *Supervisor) startLeaseSweepLoop() {
	s.leaseSweepMu.Lock()
	defer s.leaseSweepMu.Unlock()
	if s.leaseSweepStop != nil || s.leaseSweepStopped {
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	s.leaseSweepStop = stop
	s.leaseSweepDone = done
	if s.testHooks.leaseSweepLoopStart != nil {
		s.testHooks.leaseSweepLoopStart()
	}
	go s.leaseSweepLoop(s.config.WorkflowLeaseSweepInterval, stop, done)
}

// leaseSweepLoop runs Manager.ExpireStaleLeases on its ticker until stopped.
// An expired dead lease lands through the manager's change notification, so
// the controller wakes and re-dispatches without waiting for its anti-entropy
// cadence. Sweep errors are logged, never fatal: the next tick retries. When
// testHooks.leaseSweepTick is set, the test's channel replaces the ticker so
// a test steps every sweep explicitly.
func (s *Supervisor) leaseSweepLoop(interval time.Duration, stop, done chan struct{}) {
	defer close(done)
	var ticker *time.Ticker
	var tickC <-chan time.Time
	if s.testHooks.leaseSweepTick != nil {
		tickC = s.testHooks.leaseSweepTick
	} else {
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
		tickC = ticker.C
	}
	for {
		select {
		case <-stop:
			return
		case <-s.shutdownCh:
			return
		case <-tickC:
		}
		summary, err := s.workflowMgr.ExpireStaleLeases()
		if err != nil {
			log.Printf("workflow: live lease sweep failed: %v", err)
		} else if summary.Expired > 0 || len(summary.Errors) > 0 {
			log.Printf("workflow: live lease sweep: expired=%d retained=%d errors=%d",
				summary.Expired, summary.Retained, len(summary.Errors))
		}
		if s.testHooks.leaseSweepPost != nil {
			s.testHooks.leaseSweepPost(summary)
		}
	}
}

// leaseSweepRunning reports whether the live lease-expiry sweep loop is
// running: the barrier succeeded, the interval is positive, and it has not
// been stopped.
func (s *Supervisor) leaseSweepRunning() bool {
	s.leaseSweepMu.Lock()
	defer s.leaseSweepMu.Unlock()
	return s.leaseSweepStop != nil
}

// stopLeaseSweep halts the live lease-expiry sweep goroutine and waits for it
// to exit, so no sweep outlives Shutdown.
func (s *Supervisor) stopLeaseSweep() {
	s.leaseSweepMu.Lock()
	s.leaseSweepStopped = true
	stop, done := s.leaseSweepStop, s.leaseSweepDone
	s.leaseSweepStop = nil
	s.leaseSweepMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	<-done
}

// signalCapacityChange wakes callers waiting in WaitForCapacity. It does not
// choose which caller runs next; it only notifies that capacity may be
// available.
func (s *Supervisor) signalCapacityChange() {
	s.capacityMu.Lock()
	if s.capacityCh != nil {
		close(s.capacityCh)
	}
	s.capacityCh = make(chan struct{})
	s.capacityMu.Unlock()
	s.notifyCapacitySubscribers()
}

// notifyCapacitySubscribers performs a non-blocking send to every capacity
// change subscriber. A subscriber that falls behind coalesces to a single
// signal. It never fails or blocks the caller.
func (s *Supervisor) notifyCapacitySubscribers() {
	s.capacitySubMu.Lock()
	defer s.capacitySubMu.Unlock()
	for _, ch := range s.capacitySubs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// SubscribeCapacityChanges returns a coalescing wake channel and a cancel
// func, notified whenever admission capacity may have changed. Hints only.
func (s *Supervisor) SubscribeCapacityChanges() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	s.capacitySubMu.Lock()
	s.capacitySubs = append(s.capacitySubs, ch)
	s.capacitySubMu.Unlock()
	cancel := func() {
		s.capacitySubMu.Lock()
		defer s.capacitySubMu.Unlock()
		for i, c := range s.capacitySubs {
			if c == ch {
				s.capacitySubs = append(s.capacitySubs[:i], s.capacitySubs[i+1:]...)
				break
			}
		}
	}
	return ch, cancel
}

// acquireTreeAdmission reserves one tree-budget slot. It returns an empty
// token (no-op) when no tree budget is active (degraded mode) or a non-empty
// token on success. A tree-exhaustion failure returns a *admission.CapacityError
// with Source "tree".
func (s *Supervisor) acquireTreeAdmission() (string, error) {
	s.treeBudgetMu.Lock()
	budget := s.treeBudget
	s.treeBudgetMu.Unlock()
	if budget == nil {
		return "", nil
	}
	token, err := budget.Acquire(s.runID + ":pending")
	if err != nil {
		return "", err
	}
	return token, nil
}

// acquireChildTreeToken acquires a tree-budget slot for a resuming runtime and
// stores it on the child. It blocks until a slot is available, ctx is canceled,
// or the supervisor shuts down. Returns an error only when the wait is
// abandoned (cancel/shutdown); a nil error means the child holds a token (or
// is in degraded mode with no budget).
func (s *Supervisor) acquireChildTreeToken(ctx context.Context, child *childRuntime) error {
	s.treeBudgetMu.Lock()
	budget := s.treeBudget
	s.treeBudgetMu.Unlock()
	if budget == nil {
		return nil // degraded mode
	}
	for {
		token, err := budget.Acquire(child.id)
		if err == nil {
			child.mu.Lock()
			child.treeToken = token
			child.treeBudgetRelease = func(tok string) {
				s.treeBudgetMu.Lock()
				b := s.treeBudget
				s.treeBudgetMu.Unlock()
				if b != nil {
					b.Release(tok)
				}
				s.signalCapacityChange()
			}
			child.mu.Unlock()
			return nil
		}
		var ce *admission.CapacityError
		if !errors.As(err, &ce) {
			return err
		}
		// Tree is exhausted; wait for a capacity change before retrying.
		if err := s.WaitForCapacity(ctx); err != nil {
			return err
		}
	}
}

// releaseChildTreeToken releases the runtime's current tree-budget slot, if
// any. Safe to call when no token is held.
func (s *Supervisor) releaseChildTreeToken(child *childRuntime) {
	child.releaseTreeToken()
}

// WaitForCapacity blocks until a tree budget slot may be available, ctx is
// canceled, or the supervisor shuts down. It does not reserve capacity; the
// caller must retry Acquire/spawn after waking. This primitive lets callers
// that can wait avoid busy-polling without requiring the admission layer to
// schedule or prioritize their work.
func (s *Supervisor) WaitForCapacity(ctx context.Context) error {
	s.capacityMu.Lock()
	ch := s.capacityCh
	s.capacityMu.Unlock()
	// A bounded poll acts as a cross-process fallback: a nested supervisor in
	// a different process releases into the shared file but cannot signal this
	// supervisor's in-process channel directly.
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-ch:
		return nil // capacity changed; caller should retry
	case <-timer.C:
		return nil // poll fallback; caller should retry
	case <-ctx.Done():
		return ctx.Err()
	case <-s.shutdownCh:
		return errors.New("supervisor is shutting down")
	}
}

// WaitForCapacityMS implements control.StableHandler. It waits for up to
// timeoutMS milliseconds for a capacity change notification, then returns so the
// caller can retry spawn. It does not reserve capacity. A bounded wait keeps the
// control connection responsive; callers that need longer waits poll.
func (s *Supervisor) WaitForCapacityMS(timeoutMS int) error {
	if timeoutMS <= 0 {
		timeoutMS = 5000
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMS)*time.Millisecond)
	defer cancel()
	return s.WaitForCapacity(ctx)
}

func (s *Supervisor) writeTombstone(reason string) {
	if s.config.TombstoneFile == "" {
		return
	}
	content := fmt.Sprintf("STOPPED reason=%s pid=%d at=%s\n", reason, os.Getpid(), time.Now().Format(time.RFC3339))
	if err := os.WriteFile(s.config.TombstoneFile, []byte(content), 0o644); err != nil {
		fmt.Fprintf(s.stderrWriter(), "avenor stable: write tombstone: %v\n", err)
	}
}

func idleCheck(idleTimeout time.Duration, active int, deadline *time.Time) <-chan time.Time {
	if idleTimeout <= 0 {
		return nil
	}
	if active > 0 {
		*deadline = time.Now().Add(idleTimeout)
		return nil
	}
	return time.After(time.Until(*deadline))
}

// acquireLocalRuntimeSlot re-gates a parked runtime on the local
// MaxRuntimes cap before a follow-up turn resumes. Parking released the
// runtime's slot and another spawn may have claimed it, so block until a
// slot is free — the local mirror of acquireChildTreeToken.
func (s *Supervisor) acquireLocalRuntimeSlot(ctx context.Context, child *childRuntime) error {
	for {
		s.controlMu.Lock()
		if s.shuttingDown {
			s.controlMu.Unlock()
			return errors.New("supervisor is shutting down")
		}
		if s.activeRuntimeCountLocked()+s.outstandingReservations < s.config.MaxRuntimes {
			child.mu.Lock()
			// The adapter commits the parked-to-idle transition; a slot failure
			// leaves the child parked.
			_, _, err := s.applyLifecycle(child, lifecycleSignal{kind: signalLocalSlotAcquired, class: child.lc.class})
			child.mu.Unlock()
			s.controlMu.Unlock()
			if err != nil {
				s.reportLifecycleError(child, err)
				// A failed acquisition never grants the slot: the run ends
				// through the fault fail-safe instead of resuming while the
				// machine still reports the child parked. The admission
				// check granted nothing (it is a predicate), so there is no
				// slot accounting to release; the child stays parked until
				// the final publication, keeping it capacity-excluded. If the
				// context also ended, the fail-safe records the idle terminal
				// end (sentinel and session.end) the idle end performs on a
				// slot failure plus a cancel or deadline.
				s.failSafeRunChildTransition(child, idleEndFault(child, ctx.Err()))
				return fmt.Errorf("%w: %w", errLifecycleFaultRecovery, err)
			}
			return nil
		}
		s.controlMu.Unlock()
		if err := s.WaitForCapacity(ctx); err != nil {
			return err
		}
	}
}

// admissionReservation holds one runtime-start admission: a tree-budget token
// (empty in degraded local-only mode) plus one held local runtime slot.
// Exactly one owner holds a reservation at a time; it is either released back
// to the budget and local limit or converted into a registered child runtime
// by spawnReserved. Release is idempotent and safe from every pre-start
// failure, panic, cancellation, and shutdown path.
type admissionReservation struct {
	sup       *Supervisor
	treeToken string
	held      bool
	release   sync.Once
}

// Release returns the reservation's tree token and local slot. After the
// reservation has been converted into a runtime (or already released) it is a
// no-op.
func (r *admissionReservation) Release() {
	r.release.Do(func() {
		s := r.sup
		s.controlMu.Lock()
		if r.held {
			r.held = false
			if s.outstandingReservations > 0 {
				s.outstandingReservations--
			}
		}
		s.controlMu.Unlock()
		if r.treeToken != "" {
			s.treeBudgetMu.Lock()
			budget := s.treeBudget
			s.treeBudgetMu.Unlock()
			if budget != nil {
				budget.Release(r.treeToken)
			}
		}
		s.signalCapacityChange()
	})
}

// consumeLocked transfers the reservation into a freshly registered child
// under controlMu: the local slot becomes the registered runtime and the tree
// token moves to the child (returned so the caller can attach it). The
// reservation is marked released so a later Release is a no-op.
func (r *admissionReservation) consumeLocked() string {
	token := r.treeToken
	if r.held {
		r.held = false
		if r.sup.outstandingReservations > 0 {
			r.sup.outstandingReservations--
		}
	}
	r.treeToken = ""
	r.release.Do(func() {})
	return token
}

// reserveAdmission reserves admission for one runtime start: the tree-budget
// slot first (typed tree CapacityError when exhausted), then one held local
// slot under controlMu (typed local CapacityError when outstanding
// reservations plus active runtimes reach MaxRuntimes). In degraded
// local-only mode the reservation holds only the local slot. It must never be
// called while holding a workflow-store, controller-store, or dispatch lock.
func (s *Supervisor) reserveAdmission() (*admissionReservation, error) {
	treeToken, err := s.acquireTreeAdmission()
	if err != nil {
		return nil, err
	}
	res := &admissionReservation{sup: s, treeToken: treeToken, held: true}
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	fail := func(err error) (*admissionReservation, error) {
		if treeToken != "" {
			s.treeBudgetMu.Lock()
			budget := s.treeBudget
			s.treeBudgetMu.Unlock()
			if budget != nil {
				budget.Release(treeToken)
			}
		}
		return nil, err
	}
	if s.shuttingDown {
		return fail(fmt.Errorf("supervisor is shutting down"))
	}
	active := s.activeRuntimeCountLocked() + s.outstandingReservations
	if active >= s.config.MaxRuntimes {
		return fail(&admission.CapacityError{Source: "local", Limit: s.config.MaxRuntimes, Active: active})
	}
	s.outstandingReservations++
	return res, nil
}

func (s *Supervisor) activeRuntimeCountLocked() int {
	n := 0
	for _, rt := range s.runtimes {
		rt.mu.Lock()
		// Parked runtimes await an optional follow-up prompt; they hold no
		// turn in flight, so they must not consume a MaxRuntimes slot.
		if !rt.lc.isCompleted() && !rt.lc.isParked() {
			n++
		}
		rt.mu.Unlock()
	}
	return n
}

func (s *Supervisor) spawn(params SpawnParams) (SpawnResult, error) {
	res, err := s.reserveAdmission()
	if err != nil {
		return SpawnResult{}, err
	}
	return s.spawnReserved(params, res)
}

// labelHolderLocked returns the ID of a live (non-completed) runtime holding
// the given label, or "" if none. The caller must hold s.controlMu; each
// child's mu is locked and unlocked here.
func (s *Supervisor) labelHolderLocked(label string) string {
	for _, child := range s.runtimes {
		child.mu.Lock()
		holding := child.label == label && !child.lc.isCompleted()
		child.mu.Unlock()
		if holding {
			return child.id
		}
	}
	return ""
}

// spawnReserved starts a runtime using a pre-reserved admission. It never
// acquires admission itself: the reservation's tree token moves to the child
// on success and its local slot becomes the registered runtime. On any
// pre-start failure the reservation is released.
func (s *Supervisor) spawnReserved(params SpawnParams, res *admissionReservation) (SpawnResult, error) {
	defer res.Release()
	if err := runtime.ValidateLaunchWrapper(params.LaunchWrapper); err != nil {
		return SpawnResult{}, fmt.Errorf("launch_wrapper: %w", err)
	}
	if params.AutoApprove && (params.PermissionDecider != nil || params.PermissionModelDecider != nil) {
		return SpawnResult{}, fmt.Errorf("auto_approve and permission deciders are mutually exclusive")
	}
	if params.PermissionDecider != nil && params.PermissionModelDecider != nil {
		return SpawnResult{}, fmt.Errorf("permission_decider and permission_model_decider are mutually exclusive")
	}
	modelDeciderKey := ""
	if params.PermissionModelDecider != nil {
		key, err := resolvePermissionModelDeciderKey(params.PermissionModelDecider)
		if err != nil {
			return SpawnResult{}, err
		}
		modelDeciderKey = key
	}
	workflowMode := spawnIsWorkflow(params)
	if workflowMode {
		if params.RosterEntry != "" {
			return SpawnResult{}, fmt.Errorf("roster_entry is not valid with loop_file or team_file")
		}
	} else {
		if err := spawnselection.Validate(spawnselection.Input{
			Agent:       params.Agent,
			Model:       params.Model,
			Backend:     params.Backend,
			RosterFile:  params.RosterFile,
			RosterEntry: params.RosterEntry,
		}, false); err != nil {
			return SpawnResult{}, err
		}
	}

	params, backend, err := s.resolveDirectSpawnIdentity(params)
	if err != nil {
		return SpawnResult{}, err
	}

	// The admission reservation already holds the tree slot and one local
	// slot; convert it into this child instead of acquiring again.
	s.controlMu.Lock()
	if s.shuttingDown {
		s.controlMu.Unlock()
		return SpawnResult{}, fmt.Errorf("supervisor is shutting down")
	}
	s.nextID++
	rtID := fmt.Sprintf("rt_%d", s.nextID)
	childCtx, childCancel := context.WithCancel(context.Background())

	// Reserve the slot to prevent TOCTOU bypass of the max-runtime limit.
	// Every child mode shares this lifecycle context so direct provider calls
	// can be canceled before writer teardown waits for them.
	// Keyed spawns enforce label uniqueness atomically with registration:
	// two concurrent keyed first-use spawns with the same label cannot both
	// register (the MCP pre-check is a fast path, not the authority).
	if params.IdempotencyKey != "" && params.Label != "" {
		if holder := s.labelHolderLocked(params.Label); holder != "" {
			childCancel()
			s.controlMu.Unlock()
			return SpawnResult{}, fmt.Errorf("label already in use: %s (runtime %s)", params.Label, holder)
		}
	}
	child := &childRuntime{
		id: rtID,
		// Set before publication into s.runtimes so the label is visible
		// under s.controlMu at the moment the child becomes visible to the
		// label scans, instead of being written unlocked after publication.
		label:                    params.Label,
		startedAt:                time.Now().UnixMilli(),
		lifecycleCtx:             childCtx,
		cancelFn:                 childCancel,
		done:                     make(chan struct{}),
		runDone:                  make(chan struct{}),
		promptCh:                 make(chan struct{}, 1),
		autoApprove:              params.AutoApprove,
		permissionDecider:        params.PermissionDecider,
		modelDecider:             params.PermissionModelDecider,
		modelDeciderKey:          modelDeciderKey,
		task:                     params.Prompt,
		taskFile:                 params.PromptFile,
		launchWrapper:            params.LaunchWrapper,
		workflowID:               params.WorkflowID,
		nodeID:                   params.NodeID,
		activationID:             params.ActivationID,
		attemptID:                params.AttemptID,
		permissionDeciderRun:     params.permissionDeciderRun,
		permissionDeciderSlotKey: params.permissionDeciderSlotKey,
		// Explicit reserved lifecycle state: the class is immutable and set
		// before publication; published is sent immediately before map
		// insertion below.
		lc: lifecycleState{class: childAttemptClass(params)},
	}
	if terminate := params.workflowTerminate; terminate != nil {
		child.onWorkflowTerminate = func(status workflow.AttemptStatus) { terminate(child, status) }
	}
	treeToken := res.consumeLocked()
	if treeToken != "" {
		child.treeToken = treeToken
		child.treeBudgetRelease = func(token string) {
			s.treeBudgetMu.Lock()
			budget := s.treeBudget
			s.treeBudgetMu.Unlock()
			if budget != nil {
				budget.Release(token)
			}
		}
	}
	// Predict-and-commit the published transition immediately before map
	// insertion, under controlMu -> child.mu.
	child.mu.Lock()
	_, _, pubErr := s.applyLifecycle(child, lifecycleSignal{kind: signalPublished, class: child.lc.class})
	child.mu.Unlock()
	s.runtimes[rtID] = child
	s.controlMu.Unlock()
	if pubErr != nil {
		// Reported after both child.mu and controlMu are released.
		s.reportLifecycleError(child, pubErr)
	}

	// Track parent-child relationship.
	if params.ParentID != "" {
		child.mu.Lock()
		child.parentID = params.ParentID
		child.mu.Unlock()
		s.controlMu.Lock()
		if parent, ok := s.runtimes[params.ParentID]; ok {
			parent.mu.Lock()
			parent.children = append(parent.children, rtID)
			parent.mu.Unlock()
		}
		s.controlMu.Unlock()
	}

	releaseReservation := func() {
		childCancel()
		s.controlMu.Lock()
		delete(s.runtimes, rtID)
		s.controlMu.Unlock()
		// The rollback publication cue runs the completion boundary itself; a
		// failed transition still publishes, so external waiters are never
		// stranded on a machine error.
		child.mu.Lock()
		_, eff, err := s.applyLifecycle(child, lifecycleSignal{
			kind: signalSpawnRolledBack, class: child.lc.class, boundary: afterSpawnRollback,
		})
		child.mu.Unlock()
		if err != nil {
			s.reportLifecycleError(child, err)
			child.complete(s, afterSpawnRollback)
		} else {
			s.executeDeferredLifecycleEffects(child, eff)
		}
		// No run goroutine was started for a rolled-back child; close the join
		// point so test teardown never waits on it.
		child.closeRunDone()
	}
	defer func() {
		if releaseReservation != nil {
			releaseReservation()
		}
	}()

	if params.Dir == "" {
		params.Dir = "."
	}

	promptText := params.Prompt
	if promptText == "" && params.PromptFile != "" {
		data, err := os.ReadFile(params.PromptFile)
		if err != nil {
			return SpawnResult{}, fmt.Errorf("read prompt file: %w", err)
		}
		promptText = string(data)
	}

	// Ensure the parent has a broker run for channel messaging and prepend
	// the parent run ID to the child's prompt so it knows where to reply.
	var parentToken string
	if params.ParentRunID != "" && s.broker != nil {
		parentToken, _ = s.broker.EnsureRun(params.ParentRunID, "")
		parentNote := fmt.Sprintf("Your parent's broker run ID is %q. Use avenor_send with this run ID to send status updates, findings, or questions back to your parent.\n\n", params.ParentRunID)
		promptText = parentNote + promptText
	}

	if params.LoopFile != "" && params.TeamFile != "" {
		return SpawnResult{}, fmt.Errorf("loop_file and team_file are mutually exclusive")
	}
	if promptText == "" && params.LoopFile == "" && params.TeamFile == "" {
		return SpawnResult{}, fmt.Errorf("prompt, prompt_file, loop_file, or team_file is required")
	}

	// Per-runtime artifact directory.
	artifactDir := filepath.Join(os.TempDir(), "avenor-stable", s.runID, rtID)
	if err := os.MkdirAll(artifactDir, 0o700); err != nil {
		return SpawnResult{}, fmt.Errorf("create artifact dir: %w", err)
	}

	onEvent := params.OnEvent
	if onEvent == "" {
		onEvent = filepath.Join(artifactDir, "events.ndjson")
	}
	sentinelFile := params.SentinelFile
	if sentinelFile == "" {
		sentinelFile = filepath.Join(artifactDir, "sentinel.env")
	}

	// Create event writer for this runtime.
	writer, err := cli.NewEventWriter(onEvent)
	if err != nil {
		return SpawnResult{}, fmt.Errorf("open event stream: %w", err)
	}

	var fileHandler *permission.FileHandler
	permHandler := params.PermissionHandler
	// A permission decider replaces the local resolvers: no file permission
	// handler is derived, so a pending request stays pending (surfacing to
	// the decider and, on defer, to a human) instead of stalling in the
	// file handler or being approved blindly.
	if permHandler == "" && !params.AutoApprove && params.PermissionDecider == nil && params.PermissionModelDecider == nil && sentinelFile != "" {
		permHandler = "file:" + cli.DerivePermBase(sentinelFile)
	}
	if permHandler != "" {
		fh, err := cli.ParsePermissionHandler(permHandler)
		if err != nil {
			_ = writer.Close()
			return SpawnResult{}, fmt.Errorf("permission handler: %w", err)
		}
		fileHandler = fh
	}

	// Loop spawn path — uses looprunner instead of a single provider/session.
	if params.LoopFile != "" {
		fallbackRosterPath := params.RosterFile
		if fallbackRosterPath != "" && !filepath.IsAbs(fallbackRosterPath) {
			fallbackRosterPath = filepath.Join(params.Dir, fallbackRosterPath)
		}
		cfg, rootRoster, err := looprunner.LoadLoopConfigWithRoster(params.LoopFile, nil, fallbackRosterPath)
		if err != nil {
			_ = writer.Close()
			return SpawnResult{}, fmt.Errorf("spawn: load loop config: %w", err)
		}
		rosterMetadataPath := params.RosterFile
		if rosterMetadataPath == "" && cfg.RosterFile != "" {
			rosterMetadataPath = cfg.RosterFile
			if !filepath.IsAbs(rosterMetadataPath) {
				rosterMetadataPath = filepath.Join(filepath.Dir(params.LoopFile), rosterMetadataPath)
			}
		}
		if promptText != "" {
			cfg.InsertInitialPrompt(promptText)
		}

		// For loop spawns, skip the normal provider/session pre-start.
		// SpawnResult.SessionID is empty — phase session IDs are in events.
		result := SpawnResult{
			RuntimeID:    rtID,
			OnEvent:      onEvent,
			SentinelFile: sentinelFile,
			BrokerURL:    s.brokerURL(),
			AgentProfile: params.AgentProfile,
		}

		child.agent = params.Agent
		child.agentProfile = params.AgentProfile
		child.model = params.Model
		child.thinking = params.Thinking
		child.backend = backend
		child.effectiveBackend = backend
		child.effectiveAgent = params.Agent
		child.effectiveModel = params.Model
		child.rosterFile = rosterMetadataPath
		child.roster = rootRoster
		child.permClaimTimeout = s.config.PermissionClaimTimeout
		child.eventWriter = writer
		child.fileHandler = fileHandler
		child.runID = s.runID
		child.dir = params.Dir
		child.onEvent = onEvent
		child.sentinelFile = sentinelFile
		// provider and session remain zero-valued for loop children

		select {
		case s.runtimeActivity <- struct{}{}:
		default:
		}

		releaseReservation = nil
		go s.runLoopChild(childCtx, child, cfg, params.Timeout, params.MaxRetries, params.Agent, params.AgentProfile, params.Model, params.Thinking, params.ServerURL, backend)

		return result, nil
	}

	if params.TeamFile != "" {
		fallbackRosterPath := params.RosterFile
		if fallbackRosterPath != "" && !filepath.IsAbs(fallbackRosterPath) {
			fallbackRosterPath = filepath.Join(params.Dir, fallbackRosterPath)
		}
		cfg, rootRoster, err := teamrunner.LoadTeamConfigWithRoster(params.TeamFile, nil, fallbackRosterPath)
		if err != nil {
			_ = writer.Close()
			return SpawnResult{}, fmt.Errorf("spawn: load team config: %w", err)
		}
		rosterMetadataPath := params.RosterFile
		if rosterMetadataPath == "" && cfg.RosterFile != "" {
			rosterMetadataPath = cfg.RosterFile
			if !filepath.IsAbs(rosterMetadataPath) {
				rosterMetadataPath = filepath.Join(filepath.Dir(params.TeamFile), rosterMetadataPath)
			}
		}
		if promptText != "" {
			cfg.InsertInitialPrompt(promptText)
		}

		result := SpawnResult{
			RuntimeID:    rtID,
			OnEvent:      onEvent,
			SentinelFile: sentinelFile,
			BrokerURL:    s.brokerURL(),
			AgentProfile: params.AgentProfile,
		}

		child.agent = params.Agent
		child.agentProfile = params.AgentProfile
		child.model = params.Model
		child.thinking = params.Thinking
		child.backend = backend
		child.effectiveBackend = backend
		child.effectiveAgent = params.Agent
		child.effectiveModel = params.Model
		child.rosterFile = rosterMetadataPath
		child.roster = rootRoster
		child.permClaimTimeout = s.config.PermissionClaimTimeout
		child.eventWriter = writer
		child.fileHandler = fileHandler
		child.runID = s.runID
		child.dir = params.Dir
		child.onEvent = onEvent
		child.sentinelFile = sentinelFile

		select {
		case s.runtimeActivity <- struct{}{}:
		default:
		}

		releaseReservation = nil
		go s.runTeamChild(childCtx, child, cfg, params.Timeout, params.MaxRetries, params.Agent, params.AgentProfile, params.Model, params.Thinking, params.ServerURL, backend)

		return result, nil
	}

	// Start provider and session.
	// Register the child's broker run and capture its token so the pi provider
	// can hand broker credentials to the sub-process for inbound ask polling and
	// avenor_reply. Idempotent; the run may already exist for tasking.
	if s.broker != nil {
		s.ensureChildBrokerRun(child)
	}
	startOpts := runtime.StartOptions{
		Agent:         params.Agent,
		AgentProfile:  params.AgentProfile,
		Label:         params.Label,
		Dir:           params.Dir,
		Model:         params.Model,
		Thinking:      params.Thinking,
		LaunchWrapper: params.LaunchWrapper,
		RuntimeID:     rtID,
		Broker:        s.broker,
		BrokerToken:   child.brokerToken,
		Messaging:     s.messaging,
		Provenance:    runtime.DirectLaunchProvenance(rtID, params.Label, 1),
	}
	discovery := cli.DiscoverServer(params.ServerURL, os.Getenv)
	startOpts.ServerURL = discovery.URL

	if backend == "opencode-http" && startOpts.ServerURL == "" {
		if discovery.Mode == "subprocess" {
			server, err := s.getOrCreateHTTPServer(params.Dir)
			if err != nil {
				_ = writer.Close()
				return SpawnResult{}, fmt.Errorf("start opencode serve: %w", err)
			}
			startOpts.ServerURL = server.url
			// Clear Dir so supportedDir("") passes — the server is already
			// running in the target directory.
			startOpts.Dir = ""
		} else {
			_ = writer.Close()
			return SpawnResult{}, fmt.Errorf("--server-url is required for backend opencode-http")
		}
	}
	provider, err := s.newProviderFunc(startOpts, backend)
	if err != nil {
		_ = writer.Close()
		return SpawnResult{}, fmt.Errorf("create provider: %w", err)
	}
	session, err := cli.StartSession(childCtx, provider, backend, startOpts, params.SessionID)
	if err != nil {
		if closer, ok := provider.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		_ = writer.Close()
		return SpawnResult{}, fmt.Errorf("start session: %w", err)
	}

	// Populate child with fully-initialised state.
	child.agent = params.Agent
	child.agentProfile = params.AgentProfile
	child.model = params.Model
	child.thinking = params.Thinking
	child.backend = backend
	child.effectiveBackend = backend
	child.effectiveAgent = params.Agent
	child.effectiveModel = params.Model
	child.rosterFile = params.RosterFile
	child.rosterEntry = params.RosterEntry
	providerLifecycle := s.installChildProvider(child, provider, session)
	child.eventWriter = writer
	child.fileHandler = fileHandler
	child.permClaimTimeout = s.config.PermissionClaimTimeout
	child.runID = s.runID
	child.dir = params.Dir
	child.onEvent = onEvent
	child.sentinelFile = sentinelFile
	attempt, err := s.registerSessionAttempt(session.SessionID, effectiveIdentity{
		Backend:       backend,
		Agent:         params.Agent,
		Model:         params.Model,
		AgentProfile:  params.AgentProfile,
		RosterFile:    params.RosterFile,
		RosterEntry:   params.RosterEntry,
		LaunchWrapper: params.LaunchWrapper,
	}, provider, params.SessionID)
	if err != nil {
		s.closeChildProvider(child, providerLifecycle)
		_ = writer.Close()
		return SpawnResult{}, err
	}
	child.directAttempt = attempt

	// Take a file snapshot for output file detection.
	s.fileSnapMu.Lock()
	s.fileSnapshots[rtID] = s.takeFileSnapshot(params.Dir)
	s.fileSnapMu.Unlock()

	// Start the child event loop in a goroutine.
	releaseReservation = nil
	go s.runChild(childCtx, child, promptText, params.Timeout, params.MaxRetries)

	select {
	case s.runtimeActivity <- struct{}{}:
	default:
	}

	brokerURL := s.brokerURL()
	return SpawnResult{
		RuntimeID:        rtID,
		SessionID:        session.SessionID,
		OnEvent:          onEvent,
		SentinelFile:     sentinelFile,
		BrokerURL:        brokerURL,
		ParentToken:      parentToken,
		Agent:            params.Agent,
		AgentProfile:     params.AgentProfile,
		Model:            params.Model,
		Backend:          backend,
		RosterFile:       params.RosterFile,
		RosterEntry:      params.RosterEntry,
		EffectiveAgent:   params.Agent,
		EffectiveModel:   params.Model,
		EffectiveBackend: backend,
	}, nil
}

func (s *Supervisor) brokerURL() string {
	if s.broker == nil {
		return ""
	}
	return fmt.Sprintf("http://%s", s.broker.Addr())
}

// registerBrokerRun ensures the supervisor has its own run registered in the
// broker for authentication. Returns the run ID and token.
func (s *Supervisor) registerBrokerRun() (string, string) {
	s.brokerRunMu.Lock()
	defer s.brokerRunMu.Unlock()
	if s.brokerRunID != "" {
		return s.brokerRunID, s.brokerToken
	}
	if s.broker == nil {
		return "", ""
	}
	// EnsureRun is idempotent, so concurrent first-use calls settle on the
	// same run and token instead of racing CreateRun and failing "already
	// registered" on the second call.
	token, _ := s.broker.EnsureRun("supervisor", "supervisor")
	if token == "" {
		return "", ""
	}
	s.brokerRunID = "supervisor"
	s.brokerToken = token
	return s.brokerRunID, token
}

// ensureChildBrokerRun registers a child's broker run under its runtime ID and
// captures the token so the child can poll for and reply to inbound asks. The
// run may already exist (e.g. pre-registered for tasking), in which case EnsureRun
// returns the existing token.
func (s *Supervisor) ensureChildBrokerRun(child *childRuntime) string {
	if s.broker == nil {
		return ""
	}
	token, _ := s.broker.EnsureRun(child.id, child.label)
	child.mu.Lock()
	child.brokerToken = token
	child.mu.Unlock()
	return token
}

// brokerPost sends an authenticated POST to the broker HTTP endpoint.
func (s *Supervisor) brokerPost(path string, body map[string]any) ([]byte, error) {
	return s.brokerPostContext(context.Background(), path, body)
}

// brokerHTTPClientDo sends req through a client whose transport is private
// to this supervisor. Sharing http.DefaultClient here pools idle keep-alive
// connections by host:port across every supervisor in the process, so a
// connection left behind by one broker can be reused against another (or
// against a recycled loopback port) and surface as EOF or connection-reset
// instead of connection refused.
func (s *Supervisor) brokerHTTPClientDo(req *http.Request) (*http.Response, error) {
	s.brokerHTTPClientOnce.Do(func() {
		s.brokerHTTPClient = &http.Client{
			Transport: http.DefaultTransport.(*http.Transport).Clone(),
		}
	})
	return s.brokerHTTPClient.Do(req)
}

// brokerPostContext sends an authenticated POST with a request context so
// callers can abandon long-polls (e.g. wait_reply) when their own caller
// gives up.
func (s *Supervisor) brokerPostContext(ctx context.Context, path string, body map[string]any) ([]byte, error) {
	runID, token := s.registerBrokerRun()
	if runID == "" {
		return nil, fmt.Errorf("broker not available")
	}
	body["run_id"] = runID
	body["token"] = token
	url := s.brokerURL() + path
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("broker %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.brokerHTTPClientDo(req)
	if err != nil {
		return nil, fmt.Errorf("broker %s: %w", path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read broker %s: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("broker %s: %s: %s", path, resp.Status, strings.TrimSpace(string(respBody)))
	}
	return respBody, nil
}

// brokerGet sends an authenticated GET to the broker HTTP endpoint.
func (s *Supervisor) brokerGet(path string) ([]byte, error) {
	runID, token := s.registerBrokerRun()
	if runID == "" {
		return nil, fmt.Errorf("broker not available")
	}
	url := fmt.Sprintf("%s%s?run_id=%s&token=%s", s.brokerURL(), path, url.QueryEscape(runID), url.QueryEscape(token))
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("broker %s: %w", path, err)
	}
	resp, err := s.brokerHTTPClientDo(req)
	if err != nil {
		return nil, fmt.Errorf("broker %s: %w", path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read broker %s: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("broker %s: %s: %s", path, resp.Status, strings.TrimSpace(string(respBody)))
	}
	return respBody, nil
}

func (s *Supervisor) BrokerSend(fromRunID, toRunID, message, role string) error {
	payload := map[string]any{
		"from":        fromRunID,
		"from_run_id": fromRunID,
		"to_run_id":   toRunID,
		"message":     message,
		"role":        role,
	}
	_, err := s.brokerPost("/send", map[string]any{
		"from_run_id": fromRunID,
		"to_run_id":   toRunID,
		"type":        "agent_message",
		"payload":     payload,
	})
	return err
}

// BrokerAsk asks another run a question and waits for its reply. The ask is
// sent over the supervisor's messaging transport under the supervisor's own
// registered identity.
func (s *Supervisor) BrokerAsk(ctx context.Context, toRunID, message, role string) (any, error) {
	if s.messaging == nil {
		return nil, &control.AskError{
			Outcome: messaging.OutcomeNotSent,
			Err:     fmt.Errorf("broker not available"),
		}
	}
	runID, _ := s.registerBrokerRun()
	sender := messaging.Identity{RunID: runID, Sender: "supervisor"}
	client := askclient.New(s.messaging, askclient.Options{})
	send, wait, err := client.AskRequest(sender, "supervisor", toRunID, message, role)
	if err != nil {
		return nil, &control.AskError{MessageID: send.MessageID, Outcome: messaging.OutcomeNotSent, Err: err}
	}
	result, err := client.Ask(ctx, send, wait)
	if err != nil {
		pending := false
		var askErr *askclient.Error
		if errors.As(err, &askErr) {
			pending = askErr.Pending()
		}
		return nil, &control.AskError{
			MessageID: result.MessageID,
			Outcome:   result.Outcome,
			Pending:   pending,
			Err:       err,
		}
	}
	return map[string]any{
		"message_id":  result.MessageID,
		"from_run_id": result.Reply.FromRunID,
		"payload":     result.Reply.Payload,
	}, nil
}

func (s *Supervisor) BrokerPeers() (any, error) {
	body, err := s.brokerGet("/sessions")
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse sessions: %w", err)
	}
	return result, nil
}

// BrokerCancel withdraws an outstanding ask and reports whether a terminal
// fence now covers it.
func (s *Supervisor) BrokerCancel(messageID string) error {
	if s.messaging == nil {
		return fmt.Errorf("broker not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), messaging.CleanupDeadline)
	defer cancel()
	runID, _ := s.registerBrokerRun()
	withdrawn, err := askclient.New(s.messaging, askclient.Options{}).Withdraw(ctx, messaging.WithdrawRequest{
		Sender:    messaging.Identity{RunID: runID, Sender: "supervisor"},
		MessageID: messageID,
	})
	if err != nil {
		return err
	}
	if withdrawn.Outcome == messaging.OutcomeEdgeAbsent {
		return fmt.Errorf("no pending ask for %s", messageID)
	}
	return nil
}

// BrokerReceive drains inbound agent_message control messages (asks from
// sub-agents) queued for the supervisor run, returning them structured so the
// top-level Pi host can surface them in the session and reply.
func (s *Supervisor) BrokerReceive() (any, error) {
	if s.broker == nil {
		return nil, fmt.Errorf("broker not available")
	}
	runID, _ := s.registerBrokerRun()
	msgs, err := s.broker.DrainAgentMessages(runID, 0)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0, len(msgs))
	for _, msg := range msgs {
		var am broker.AgentMessage
		if err := json.Unmarshal(msg.Payload, &am); err != nil {
			continue
		}
		entry := map[string]any{
			"from_run_id": msg.FromRunID,
			"message_id":  am.ID,
			"message":     am.Message,
			"role":        am.Role,
		}
		result = append(result, entry)
	}
	return result, nil
}

// BrokerReply sends an answer (as the supervisor run) back to the asker for the
// given ask. replyTo is the original ask message id; toRunID the asker run.
func (s *Supervisor) BrokerReply(toRunID, replyTo, message string) error {
	runID, _ := s.registerBrokerRun()
	payload := map[string]any{
		"id":          broker.MakeToken()[:16],
		"from":        runID,
		"from_run_id": runID,
		"to_run_id":   toRunID,
		"message":     message,
		"role":        "supervisor",
		"reply_to":    replyTo,
	}
	_, err := s.brokerPost("/send", map[string]any{
		"from_run_id": runID,
		"to_run_id":   toRunID,
		"type":        "agent_message",
		"payload":     payload,
	})
	return err
}

// deadlineFromContext returns the context's deadline, or the zero time when
// the context carries none.
func deadlineFromContext(ctx context.Context) time.Time {
	deadline, _ := ctx.Deadline()
	return deadline
}

// finishRunEnded commits the final teardown transition (S/I -> E, P -> PE)
// and emits the publishCompletion cue that runs the completion boundary.
// It requires attempts 0, so it runs after every attempt defer has settled.
// The caller must not hold child.mu. A failed transition still publishes, so
// external waiters are never stranded on a machine error.
func (s *Supervisor) finishRunEnded(child *childRuntime) {
	child.mu.Lock()
	_, eff, err := s.applyLifecycle(child, lifecycleSignal{
		kind: signalRunEnded, class: child.lc.class, boundary: afterRunTeardown,
	})
	child.mu.Unlock()
	if err != nil {
		s.reportLifecycleError(child, err)
		child.complete(s, afterRunTeardown)
	} else {
		s.executeDeferredLifecycleEffects(child, eff)
	}
}

func (s *Supervisor) runChild(ctx context.Context, child *childRuntime, promptText string, timeoutSec, maxRetries int) {
	// Registered first so it runs last: the join closes only after the
	// teardown defer's post-completion work (such as the capacity release) is
	// done, so test teardown can join this goroutine before reading counters.
	defer child.closeRunDone()
	defer func() {
		panicked := false
		if r := recover(); r != nil {
			panicked = true
			fmt.Fprintf(s.stderrWriter(), "avenor stable: child %s panic: %v\n", child.id, r)
			s.emitChildError(child, fmt.Sprintf("panic: %v", r), "error")
			if child.sentinelFile != "" {
				cli.WriteSentinel(child.sentinelFile, 1, child.sessionID(), "error", s.runID, s.stderrWriter())
			}
		}
		s.recordWorkflowTermination(child, panicked, ctx)
		providerLifecycle := s.beginChildShutdown(child)
		s.closeChildEventWriter(child)
		s.permissions.clearRuntimePermissionOptions(child.id)
		s.closeChildProvider(child, providerLifecycle)
		// A claim reservation admitted just before shutdown may finish while
		// provider close drains it. Sweep again so no late binding survives.
		s.permissions.clearRuntimePermissionOptions(child.id)
		if child.permissionDeciderRun {
			// A permission-decider run holds a decider concurrency slot and a
			// neutral scratch directory; release both so blocked requests can
			// retry and no scratch dir leaks. Only a run marked as a decider
			// reaches this release, so an ordinary runtime wearing a decider's
			// exact label never frees the live decider's slot.
			s.permissions.releasePermissionDecider(child.permissionDeciderSlotKey)
		}
		s.finishRunEnded(child)
		s.signalCapacityChange()
	}()

	if s.broker != nil {
		s.ensureChildBrokerRun(child)
	}

	// The run deadline bounds every attempt and retry backoff of this child:
	// each WaitForSession derives its own timer from it, so a retry cannot
	// consume or miss a single one-shot tick.
	if timeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(time.Duration(timeoutSec)*time.Second))
		defer cancel()
	}

	resumeID := ""
	needAcquire := false // tree token held from spawn for the first attempt
	for attempt := 1; ; attempt++ {
		if attempt > 1 && resumeID == "" {
			resumeID = child.sessionID()
		}
		if needAcquire {
			if err := s.acquireChildTreeToken(ctx, child); err != nil {
				// The run context ending during the budget wait is an idle end,
				// like a deadline while parked; only that case records an end.
				if ctx.Err() != nil {
					s.writeIdleContextEnd(child, ctx.Err())
				}
				return
			}
			needAcquire = false
		}
		result := s.runChildAttempt(ctx, child, resumeID, promptText, attempt)
		// Sample the immutable result data the continuation selection needs.
		retryable := runtime.IsRetryableFailure(result.exitCode, result.stopReason)
		retriesRemaining := attempt <= maxRetries
		// A hinted wait this child cannot honor leaves no attempt worth making:
		// the machine records the terminal result instead of parking the run.
		var retryWait <-chan time.Time
		if retryable && retriesRemaining {
			var honored bool
			retryWait, honored = runtime.RetryWait(ctx, retryWaitTimer, backoffDelay(attempt), result.retryAfter)
			retriesRemaining = honored
		}
		runKind := runKindOrdinaryDirect
		if child.permissionDeciderRun {
			runKind = runKindDecider
		} else if child.workflowID != "" {
			runKind = runKindWorkflowDirect
		}
		conflict := result.stopReason == runtime.SessionIDConflictStopReason
		stopReason := result.stopReason
		if stopReason == "" {
			stopReason = runtime.StopReasonForExitCode(result.exitCode)
		}
		// Release tree admission after the turn so a parked runtime does not
		// consume descendant budget while idle. A subsequent prompt
		// re-acquires before its turn.
		child.mu.Lock()
		_, retEff, retErr := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptReturned, class: child.lc.class})
		child.mu.Unlock()
		if retErr != nil {
			// No predicted release ran; the fail-safe records the outcome exit,
			// returns the tree token, writes the sentinel and ends the run.
			s.reportLifecycleError(child, retErr)
			s.failSafeRunChildTransition(child, resultFault(result, stopReason))
			return
		}
		s.executeDeferredLifecycleEffects(child, retEff)
		needAcquire = true

		// The machine owns the result policy: it records the exit code under
		// the lock and selects the continuation this switch executes.
		child.mu.Lock()
		cont, eff, err := s.applyLifecycle(child, lifecycleSignal{
			kind:               signalAttemptOutcome,
			class:              child.lc.class,
			runKind:            runKind,
			exitCode:           result.exitCode,
			stopReason:         result.stopReason,
			sentinelReason:     stopReason,
			sentinelRetryAfter: result.retryAfter,
			resultSessionID:    result.sessionID,
			sessionIDConflict:  conflict,
			retryable:          retryable,
			retriesRemaining:   retriesRemaining,
		})
		child.mu.Unlock()
		if err != nil {
			// The predicted record and sentinel never ran; the fail-safe records
			// the exit, writes the sentinel and ends the run.
			s.reportLifecycleError(child, err)
			s.failSafeRunChildTransition(child, resultFault(result, stopReason))
			return
		}
		switch cont.kind {
		case contEnd:
			// Terminal result. The recorded exit's sentinel runs at the old
			// post-record site. A successful workflow-direct or decider turn
			// presents done before returning, exactly as the success branch
			// did; failed terminals never present.
			s.executeDeferredLifecycleEffects(child, eff)
			if result.exitCode == 0 {
				child.mu.Lock()
				_, _, sucErr := s.applyLifecycle(child, lifecycleSignal{
					kind: signalSuccessPresented, class: child.lc.class, exitCode: result.exitCode,
				})
				child.mu.Unlock()
				if sucErr != nil {
					s.reportLifecycleError(child, sucErr)
				}
			}
			return
		case contPark:
			// Ordinary direct success: sentinel, success presentation, park.
			s.executeDeferredLifecycleEffects(child, eff)
			child.mu.Lock()
			_, _, sucErr := s.applyLifecycle(child, lifecycleSignal{
				kind: signalSuccessPresented, class: child.lc.class, exitCode: result.exitCode,
			})
			child.mu.Unlock()
			if sucErr != nil {
				s.reportLifecycleError(child, sucErr)
			}
			child.mu.Lock()
			_, parkEff, parkErr := s.applyLifecycle(child, lifecycleSignal{
				kind: signalParkRequested, class: child.lc.class, runKind: runKindOrdinaryDirect,
			})
			child.mu.Unlock()
			if parkErr != nil {
				// The outcome sentinel already ran on the park path; the fail-
				// safe only signals the capacity the park would have freed and
				// ends the run.
				s.reportLifecycleError(child, parkErr)
				fault := resultFault(result, stopReason)
				fault.sentinelWritten = true
				fault.parkPath = true
				s.failSafeRunChildTransition(child, fault)
				return
			}
			// Parking frees a local slot; wake spawn waiters blocked on the cap.
			s.executeDeferredLifecycleEffects(child, parkEff)
			nextPrompt, ok := child.waitForNextPrompt(s, ctx, s.config.ParkedRuntimeTimeout)
			if !ok {
				// parkWaitEnded is only the not-ok wait path, never the
				// local-slot failure; a slot failure leaves the child parked.
				child.mu.Lock()
				_, _, pweErr := s.applyLifecycle(child, lifecycleSignal{kind: signalParkWaitEnded, class: child.lc.class})
				child.mu.Unlock()
				if pweErr != nil {
					// The outcome sentinel already ran on the park path; the fail-
					// safe signals the capacity the park would have freed and ends
					// the run, recording the idle terminal end when the context
					// ended the wait.
					s.reportLifecycleError(child, pweErr)
					fault := resultFault(result, stopReason)
					fault.sentinelWritten = true
					if ctxErr := ctx.Err(); ctxErr != nil {
						fault = idleEndFault(child, ctxErr)
					}
					fault.parkPath = true
					s.failSafeRunChildTransition(child, fault)
					return
				}
				if ctx.Err() != nil {
					s.writeIdleContextEnd(child, ctx.Err())
				}
				return
			}
			if err := s.acquireLocalRuntimeSlot(ctx, child); err != nil {
				// A fault fail-safe already ended the run; the idle-context end
				// belongs only to a context or shutdown wait failure.
				if !errors.Is(err, errLifecycleFaultRecovery) && ctx.Err() != nil {
					s.writeIdleContextEnd(child, ctx.Err())
				}
				return
			}
			promptText = nextPrompt
			resumeID = result.sessionID
			if resumeID == "" {
				resumeID = child.sessionID()
			}
			attempt = 0
			continue
		case contProbeTerminalPrompt:
			// Sample the context before any dequeue; the dequeue itself runs
			// once outside any held mu with no context recheck after it.
			probeCtxErr := ctx.Err()
			probeResumed := false
			var probeNext string
			if probeCtxErr == nil {
				if nextPrompt, ok := child.dequeuePrompt(); ok {
					probeResumed = true
					probeNext = nextPrompt
				}
			}
			if probeResumed {
				promptText = probeNext
				resumeID = result.sessionID
				if resumeID == "" {
					resumeID = child.sessionID()
				}
				attempt = 0
			}
			child.mu.Lock()
			probeCont, probeEff, probeErr := s.applyLifecycle(child, lifecycleSignal{
				kind:               signalPromptContinuation,
				class:              child.lc.class,
				probeKind:          probeTerminalPrompt,
				terminalContextErr: probeCtxErr,
				promptConsumed:     probeResumed,
				exitCode:           result.exitCode,
				sentinelReason:     stopReason,
				sentinelRetryAfter: result.retryAfter,
				resultSessionID:    result.sessionID,
			})
			child.mu.Unlock()
			if probeErr != nil {
				// The probe carried this result's sentinel; the fail-safe writes
				// it and ends the run. A prompt dequeued before the failure is
				// consumed by the failed path.
				s.reportLifecycleError(child, probeErr)
				s.failSafeRunChildTransition(child, resultFault(result, stopReason))
				return
			}
			// Whether resuming or ending, this result's sentinel writes
			// after the dequeue.
			s.executeDeferredLifecycleEffects(child, probeEff)
			if probeCont.kind == contResumeQueued {
				continue
			}
			return
		case contProbeRetryPrompt:
			if result.sessionID != "" {
				resumeID = result.sessionID
			}
			// The retry probe dequeues before checking the context; a prompt
			// consumed against a dead context is discarded into the idle end.
			retryConsumed := false
			if nextPrompt, ok := child.dequeuePrompt(); ok && ctx.Err() == nil {
				retryConsumed = true
				promptText = nextPrompt
				attempt = 0
			}
			resumeCtxErr := ctx.Err()
			child.mu.Lock()
			probeCont, _, probeErr := s.applyLifecycle(child, lifecycleSignal{
				kind:             signalPromptContinuation,
				class:            child.lc.class,
				probeKind:        probeRetryPrompt,
				promptConsumed:   retryConsumed,
				resumeContextErr: resumeCtxErr,
				idleContextErr:   resumeCtxErr,
			})
			child.mu.Unlock()
			if probeErr != nil {
				// The failed prediction must not fall into the backoff timer:
				// the fail-safe records the exit, writes the terminal sentinel
				// and ends the run.
				s.reportLifecycleError(child, probeErr)
				s.failSafeRunChildTransition(child, resultFault(result, stopReason))
				return
			}
			if probeCont.kind == contIdleContextEnd {
				s.writeIdleContextEnd(child, resumeCtxErr)
				return
			}
			if probeCont.kind == contRetry {
				continue
			}
			// contBackoff runs the retry timer sized before the outcome
			// transaction: a hint the run cannot honor never reaches it.
			select {
			case <-retryWait:
			case <-ctx.Done():
				s.writeIdleContextEnd(child, ctx.Err())
				return
			}
		}
	}
}

// workflowPhaseAttempt owns the pre-activity attempt protocol shared by the
// loop and team PhaseAttempt callbacks: the attemptPreparing transaction, the
// roster snapshot, and the pre-activity failure guard.
type workflowPhaseAttempt struct {
	s     *Supervisor
	child *childRuntime
	// active records that the attempt handed off to the activity-based
	// settle path; once set, finish sends nothing.
	active bool
}

// beginWorkflowPhaseAttempt returns the pre-activity attempt guard for child.
// The callback defers finish before running preparing, so the guard fires
// even if preparing panics.
func (s *Supervisor) beginWorkflowPhaseAttempt(child *childRuntime) *workflowPhaseAttempt {
	return &workflowPhaseAttempt{s: s, child: child}
}

// preparing sends attemptPreparing under child.mu and returns the roster
// snapshot for the attempt. The locked section owns its Unlock via defer so
// a panic between Lock and the startFailed defer cannot leave child.mu held.
func (p *workflowPhaseAttempt) preparing() (*rosterconfig.Config, string) {
	roster, rosterFile, prepErr := func() (*rosterconfig.Config, string, error) {
		p.child.mu.Lock()
		defer p.child.mu.Unlock()
		// The adapter owns attemptPreparing: it clears the phase
		// presentation even while another parallel attempt runs.
		_, _, prepErr := p.s.applyLifecycle(p.child, lifecycleSignal{kind: signalAttemptPreparing, class: p.child.lc.class})
		return p.child.roster, p.child.rosterFile, prepErr
	}()
	if prepErr != nil {
		p.s.reportLifecycleError(p.child, prepErr)
	}
	return roster, rosterFile
}

// markActive records that beginWorkflowAttempt incremented activity: later
// failures unwind through the attempt's own settle path, not this
// pre-activity signal.
func (p *workflowPhaseAttempt) markActive() {
	p.active = true
}

// finish records the pre-activity failure, including on panic: a prepared
// attempt that returns before beginWorkflowAttempt incremented activity
// never started.
func (p *workflowPhaseAttempt) finish() {
	if p.active {
		return
	}
	p.child.mu.Lock()
	_, _, startFailErr := p.s.applyLifecycle(p.child, lifecycleSignal{kind: signalAttemptStartFailed, class: p.child.lc.class})
	p.child.mu.Unlock()
	if startFailErr != nil {
		p.s.reportLifecycleError(p.child, startFailErr)
	}
}

func (s *Supervisor) runLoopChild(ctx context.Context, child *childRuntime, cfg *looprunner.LoopConfig, timeoutSec, maxRetries int, agent, agentProfile, model, thinking, serverURL, backend string) {
	// Registered first so it runs last: the join closes only after the
	// teardown defer's post-completion work is done (test teardown joins on
	// it before reading lifecycle counters).
	defer child.closeRunDone()
	// SpawnParams.Timeout is an enforceable execution deadline for loop
	// children, not just a direct-run feature: the whole loop — every phase
	// attempt, retry, and nudge — stops at the shared wall-clock limit.
	if timeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(time.Duration(timeoutSec)*time.Second))
		defer cancel()
	}
	var brokerAttemptIDs []string
	var brokerAttemptIDsMu sync.Mutex
	sessions := newWorkflowSessionTracker()
	defer func() {
		panicked := false
		if r := recover(); r != nil {
			panicked = true
			fmt.Fprintf(s.stderrWriter(), "avenor stable: child %s panic: %v\n", child.id, r)
			s.emitChildError(child, fmt.Sprintf("panic: %v", r), "error")
			if final, ok := sessions.latest(); ok {
				s.finalizeWorkflowChild(child, final)
			}
			if child.sentinelFile != "" {
				cli.WriteSentinel(child.sentinelFile, 1, child.sessionID(), "error", s.runID, s.stderrWriter())
			}
		}
		s.recordWorkflowTermination(child, panicked, ctx)
		s.beginChildShutdown(child)
		s.closeChildEventWriter(child)
		s.permissions.clearRuntimePermissionOptions(child.id)
		// Keep the completed workflow runtime as a status tombstone. Its final
		// authoritative phase identity is required by status and follow-up after
		// all live phase providers have been removed.
		if s.broker != nil {
			brokerAttemptIDsMu.Lock()
			ids := make([]string, len(brokerAttemptIDs))
			copy(ids, brokerAttemptIDs)
			brokerAttemptIDsMu.Unlock()
			for _, rid := range ids {
				s.broker.DeleteRun(rid)
			}
			s.broker.DeleteRun(child.id)
		}
		s.finishRunEnded(child)
	}()

	if s.broker != nil {
		s.ensureChildBrokerRun(child)
	}
	taggedWriter := newRuntimeFanoutWriter(s, child,
		cli.NewEventMetadata(s.runID, child.label, child.id), newRecorderFor(s, child.id))
	s.setFanoutWriter(child, taggedWriter)

	var selectionMu sync.Mutex
	resolvedSelections := make(map[string]rosterconfig.ResolvedSelection)
	var opts looprunner.RunOptions
	opts = looprunner.RunOptions{
		WorkDir:    child.dir,
		RunID:      s.runID,
		EventSink:  taggedWriter,
		Config:     cfg,
		MaxRetries: maxRetries,
		Broker:     s.broker,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (looprunner.PhaseAttemptResult, error) {
			prep := s.beginWorkflowPhaseAttempt(child)
			defer prep.finish()
			phaseRoster, phaseRosterFile := prep.preparing()

			selectionMu.Lock()
			selection, cached := resolvedSelections[phase.Name]
			selectionMu.Unlock()
			if !cached {
				runBackend := backend
				if runBackend == "" {
					runBackend = cli.DefaultBackend
				}
				var resolveErr error
				selection, resolveErr = resolveStablePhaseSelection(phaseRoster, runBackend, agent, model, phase, true)
				if resolveErr != nil {
					return looprunner.PhaseAttemptResult{ExitCode: 1}, resolveErr
				}
				selectionMu.Lock()
				resolvedSelections[phase.Name] = selection
				selectionMu.Unlock()
			}
			// A roster entry supplies a default; an explicit run-level level
			// wins. The merge runs on the cached path too, and the merged
			// value is what ValidateThinkingForBackend and StartOptions see.
			phaseThinking := thinking
			if thinking == "" {
				phaseThinking = selection.Thinking
			}
			if resolveErr := runtime.ValidateThinkingForBackend(selection.Backend, phaseThinking); resolveErr != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1}, resolveErr
			}

			identity := effectiveIdentity{
				Backend: selection.Backend, Agent: selection.Agent, Model: selection.Model,
				AgentProfile: agentProfile, RosterFile: phaseRosterFile, RosterEntry: phase.RosterEntry,
				LaunchWrapper: child.launchWrapper,
			}
			var resumeErr error
			identity, resumeErr = s.resolveWorkflowResumeIdentity(prevSessionID, identity)
			if resumeErr != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1}, resumeErr
			}

			startOpts := runtime.StartOptions{
				Agent:         identity.Agent,
				AgentProfile:  identity.AgentProfile,
				Label:         child.label,
				Model:         identity.Model,
				Thinking:      phaseThinking,
				Dir:           child.dir,
				ServerURL:     serverURL,
				RuntimeID:     child.id,
				Broker:        s.broker,
				BrokerToken:   child.brokerToken,
				Messaging:     s.messaging,
				LaunchWrapper: child.launchWrapper,
				Provenance:    runtime.PhaseLaunchProvenance(child.id, runtime.LaunchKindLoop, phase.Name, child.label, &iteration, attemptNum),
			}

			resumeID := prevSessionID

			var brokerRunID string
			attemptWriter := taggedWriter
			if s.broker != nil {
				brokerRunID = broker.MakeToken()
				s.broker.CreateRun(brokerRunID, "")
				brokerAttemptIDsMu.Lock()
				brokerAttemptIDs = append(brokerAttemptIDs, brokerRunID)
				brokerAttemptIDsMu.Unlock()
				attemptWriter = taggedWriter.withRecorder(broker.NewRecorder(s.broker, brokerRunID))
			}
			s.setFanoutWriter(child, attemptWriter)

			if opts.SeedMessage != nil && s.broker != nil && brokerRunID != "" {
				payload, _ := json.Marshal(opts.SeedMessage)
				_ = s.broker.SendTo(opts.SeedMessage.FromRunID, brokerRunID, "agent_message", payload, "")
			}

			provider, err := s.newProviderFunc(startOpts, identity.Backend)
			if err != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("create provider: %w", err)
			}
			var providerLifecycle *permissionProviderLifecycle
			defer func() {
				if providerLifecycle == nil {
					if closer, ok := provider.(interface{ Close() error }); ok {
						_ = closer.Close()
					}
				}
			}()

			session, err := cli.StartSession(ctx, provider, identity.Backend, startOpts, resumeID)
			if err != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("start session: %w", err)
			}
			attempt, err := s.registerSessionAttempt(session.SessionID, identity, provider, resumeID)
			if err != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, err
			}
			defer s.releaseSessionAttempt(attempt)
			identity = attempt.identity
			providerLifecycle = s.beginWorkflowAttempt(child, provider, session, identity)
			prep.markActive()
			providerTurn, admitted := s.beginChildProviderTurn(child, providerLifecycle)
			if !admitted {
				s.closeChildProvider(child, providerLifecycle)
				s.endWorkflowAttempt(child, provider)
				return looprunner.PhaseAttemptResult{ExitCode: 130, SessionID: session.SessionID, StopReason: "cancelled", BrokerRunID: brokerRunID}, nil
			}
			defer func() {
				s.closeChildProvider(child, providerLifecycle)
				s.endWorkflowAttempt(child, provider)
			}()

			eventCtx, cancelEvents := context.WithCancel(ctx)
			defer cancelEvents()

			eventCh, err := provider.Events(eventCtx, session.SessionID)
			if err != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("subscribe events: %w", err)
			}

			preAdoptionID := session.SessionID

			promptDone := make(chan error, 1)
			go func() {
				promptDone <- providerTurn.Prompt(context.Background(), session.SessionID, phase.Prompt)
			}()

			// Each phase attempt derives its own timer from the shared run
			// deadline, so this waiter cannot consume or miss another
			// attempt's tick.
			result := cli.WaitForSession(ctx, provider, cli.SessionWaitConfig{
				EventCh:                eventCh,
				PromptDone:             promptDone,
				SessionID:              session.SessionID,
				RunID:                  s.runID,
				RunLabel:               child.label,
				PermissionClaimScope:   child.id,
				AutoApprove:            child.autoApprove,
				PermissionClaimTimeout: child.permClaimTimeout,
				Deadline:               deadlineFromContext(ctx),
				// adoptSessionID propagates the authoritative conversation id onto
				// the phase-local session, which is authoritative for retries,
				// resume, and the terminal sentinel. It also updates the aggregate
				// child record when this phase is still the active one.
				AcceptSessionID: func(externalID string) bool {
					return s.adoptSessionAttempt(child, attempt, preAdoptionID, externalID)
				},
				AdoptSessionID: func(externalID string) {
					session.SessionID = externalID
				},
			}, cli.SessionWaitDeps{
				Writer:        attemptWriter,
				FileHandler:   child.fileHandler,
				ControlServer: s.control,
				PreparePermissionClaim: func(ctx context.Context, scope, requestID string, state control.PermissionResolverState, options []any) bool {
					return s.prepareChildPermissionClaim(ctx, child, providerLifecycle, attemptWriter, session.SessionID, scope, requestID, state, options)
				},
				Stderr:       s.stderrWriter(),
				ProviderTurn: providerTurn,
			})

			sessions.remember(phase.Name, session.SessionID, identity)
			return looprunner.PhaseAttemptResult{
				ExitCode:      result.ExitCode,
				SessionID:     session.SessionID,
				StopReason:    result.StopReason,
				RetryAfter:    result.RetryAfter,
				LoopDirective: result.LoopDirective,
				LoopLabel:     result.LoopLabel,
				BrokerRunID:   brokerRunID,
			}, nil
		},
	}

	result, err := looprunner.Run(ctx, opts)

	// Loop phases are sequential, so completion sequence identifies the final
	// authoritative phase across iterations, early markers, failures, and post
	// phases. Finalize before every error return so the tombstone and sentinel
	// cannot retain whichever aggregate attempt happened to update child last.
	finalSession, hasFinal := sessions.latest()
	if result.SessionID != "" && result.ExitCode != 0 {
		if identity, ok := s.sessionIdentity(result.SessionID); ok {
			finalSession, hasFinal = workflowSession{SessionID: result.SessionID, Identity: identity}, true
		}
	}
	if hasFinal {
		s.finalizeWorkflowChild(child, finalSession)
	}
	if err != nil {
		child.mu.Lock()
		child.exitCode = 1
		child.mu.Unlock()
		if child.sentinelFile != "" {
			cli.WriteSentinel(child.sentinelFile, 1, child.sessionID(), "error", s.runID, s.stderrWriter())
		}
		return
	}
	child.mu.Lock()
	child.exitCode = result.ExitCode
	child.mu.Unlock()
	child.stashWorkflowTerminalMarker(result.MarkerDirective, result.MarkerLabel)

	// When every iteration phase returns end_turn, looprunner stops at
	// max_iterations with result.SessionID == "". Use the latest adopted child
	// session ID to populate the terminal sentinel.
	if result.SessionID == "" {
		result.SessionID = child.sessionID()
	}

	if child.sentinelFile != "" {
		if result.Reason != "" {
			cli.WriteSentinelResult(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, result.RetryAfter, result.Reason, "", s.stderrWriter())
		} else {
			cli.WriteSentinelResult(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, result.RetryAfter, "", "", s.stderrWriter())
		}
	}
}

func (s *Supervisor) runTeamChild(ctx context.Context, child *childRuntime, cfg *teamrunner.TeamConfig, timeoutSec, maxRetries int, agent, agentProfile, model, thinking, serverURL, backend string) {
	// Registered first so it runs last: the join closes only after the
	// teardown defer's post-completion work is done (test teardown joins on
	// it before reading lifecycle counters).
	defer child.closeRunDone()
	// SpawnParams.Timeout is an enforceable execution deadline for team
	// children: every member, retry, and nudge stops at the shared
	// wall-clock limit, while ordinary member failure still leaves peers
	// running (the deadline, not member failure, is what cancels).
	if timeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(time.Duration(timeoutSec)*time.Second))
		defer cancel()
	}
	var brokerAttemptIDs []string
	var brokerAttemptIDsMu sync.Mutex
	sessions := newWorkflowSessionTracker()
	defer func() {
		panicked := false
		if r := recover(); r != nil {
			panicked = true
			fmt.Fprintf(s.stderrWriter(), "avenor stable: child %s panic: %v\n", child.id, r)
			s.emitChildError(child, fmt.Sprintf("panic: %v", r), "error")
			if final, ok := sessions.final(cfg.Post, cfg.Team, cfg.Pre); ok {
				s.finalizeWorkflowChild(child, final)
			}
			if child.sentinelFile != "" {
				cli.WriteSentinel(child.sentinelFile, 1, child.sessionID(), "error", s.runID, s.stderrWriter())
			}
		}
		s.recordWorkflowTermination(child, panicked, ctx)
		s.beginChildShutdown(child)
		s.closeChildEventWriter(child)
		s.permissions.clearRuntimePermissionOptions(child.id)
		// Keep the completed workflow runtime as a status tombstone. Its final
		// authoritative phase identity is required by status and follow-up after
		// all live phase providers have been removed.
		if s.broker != nil {
			brokerAttemptIDsMu.Lock()
			ids := make([]string, len(brokerAttemptIDs))
			copy(ids, brokerAttemptIDs)
			brokerAttemptIDsMu.Unlock()
			for _, rid := range ids {
				s.broker.DeleteRun(rid)
			}
			s.broker.DeleteRun(child.id)
		}
		s.finishRunEnded(child)
	}()

	if s.broker != nil {
		s.ensureChildBrokerRun(child)
	}
	taggedWriter := newRuntimeFanoutWriter(s, child,
		cli.NewEventMetadata(s.runID, child.label, child.id), newRecorderFor(s, child.id))
	s.setFanoutWriter(child, taggedWriter)

	var selectionMu sync.Mutex
	resolvedSelections := make(map[string]rosterconfig.ResolvedSelection)
	var opts teamrunner.RunOptions
	opts = teamrunner.RunOptions{
		WorkDir:    child.dir,
		RunID:      s.runID,
		EventSink:  taggedWriter,
		Config:     cfg,
		MaxRetries: maxRetries,
		Broker:     s.broker,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (teamrunner.PhaseAttemptResult, error) {
			prep := s.beginWorkflowPhaseAttempt(child)
			defer prep.finish()
			phaseRoster, phaseRosterFile := prep.preparing()

			selectionMu.Lock()
			selection, cached := resolvedSelections[phase.Name]
			selectionMu.Unlock()
			if !cached {
				runBackend := backend
				if runBackend == "" {
					runBackend = cli.DefaultBackend
				}
				var resolveErr error
				selection, resolveErr = resolveStablePhaseSelection(phaseRoster, runBackend, agent, model, phase, false)
				if resolveErr != nil {
					return teamrunner.PhaseAttemptResult{ExitCode: 1}, resolveErr
				}
				selectionMu.Lock()
				resolvedSelections[phase.Name] = selection
				selectionMu.Unlock()
			}
			// A roster entry supplies a default; an explicit run-level level
			// wins. The merge runs on the cached path too, and the merged
			// value is what ValidateThinkingForBackend and StartOptions see.
			phaseThinking := thinking
			if thinking == "" {
				phaseThinking = selection.Thinking
			}
			if resolveErr := runtime.ValidateThinkingForBackend(selection.Backend, phaseThinking); resolveErr != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1}, resolveErr
			}
			identity := effectiveIdentity{
				Backend: selection.Backend, Agent: selection.Agent, Model: selection.Model,
				AgentProfile: agentProfile, RosterFile: phaseRosterFile, RosterEntry: phase.RosterEntry,
				LaunchWrapper: child.launchWrapper,
			}
			var resumeErr error
			identity, resumeErr = s.resolveWorkflowResumeIdentity(prevSessionID, identity)
			if resumeErr != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1}, resumeErr
			}
			startOpts := runtime.StartOptions{
				Agent:         identity.Agent,
				AgentProfile:  identity.AgentProfile,
				Label:         child.label,
				Model:         identity.Model,
				Thinking:      phaseThinking,
				Dir:           child.dir,
				ServerURL:     serverURL,
				RuntimeID:     child.id,
				Broker:        s.broker,
				BrokerToken:   child.brokerToken,
				Messaging:     s.messaging,
				LaunchWrapper: child.launchWrapper,
				Provenance:    runtime.PhaseLaunchProvenance(child.id, runtime.LaunchKindTeam, phase.Name, child.label, nil, attemptNum),
			}

			resumeID := prevSessionID

			var brokerRunID string
			attemptWriter := taggedWriter
			if s.broker != nil {
				brokerRunID = broker.MakeToken()
				s.broker.CreateRun(brokerRunID, "")
				brokerAttemptIDsMu.Lock()
				brokerAttemptIDs = append(brokerAttemptIDs, brokerRunID)
				brokerAttemptIDsMu.Unlock()
				attemptWriter = taggedWriter.withRecorder(broker.NewRecorder(s.broker, brokerRunID))
			}
			s.setFanoutWriter(child, attemptWriter)

			if opts.SeedMessage != nil && s.broker != nil && brokerRunID != "" {
				payload, _ := json.Marshal(opts.SeedMessage)
				_ = s.broker.SendTo(opts.SeedMessage.FromRunID, brokerRunID, "agent_message", payload, "")
			}

			provider, err := s.newProviderFunc(startOpts, identity.Backend)
			if err != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("create provider: %w", err)
			}
			var providerLifecycle *permissionProviderLifecycle
			defer func() {
				if providerLifecycle == nil {
					if closer, ok := provider.(interface{ Close() error }); ok {
						_ = closer.Close()
					}
				}
			}()

			session, err := cli.StartSession(ctx, provider, identity.Backend, startOpts, resumeID)
			if err != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("start session: %w", err)
			}
			attempt, err := s.registerSessionAttempt(session.SessionID, identity, provider, resumeID)
			if err != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, err
			}
			defer s.releaseSessionAttempt(attempt)
			identity = attempt.identity
			providerLifecycle = s.beginWorkflowAttempt(child, provider, session, identity)
			prep.markActive()
			providerTurn, admitted := s.beginChildProviderTurn(child, providerLifecycle)
			if !admitted {
				s.closeChildProvider(child, providerLifecycle)
				s.endWorkflowAttempt(child, provider)
				return teamrunner.PhaseAttemptResult{ExitCode: 130, SessionID: session.SessionID, StopReason: "cancelled", BrokerRunID: brokerRunID}, nil
			}
			defer func() {
				s.closeChildProvider(child, providerLifecycle)
				s.endWorkflowAttempt(child, provider)
			}()

			eventCtx, cancelEvents := context.WithCancel(ctx)
			defer cancelEvents()

			eventCh, err := provider.Events(eventCtx, session.SessionID)
			if err != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("subscribe events: %w", err)
			}

			preAdoptionID := session.SessionID

			promptDone := make(chan error, 1)
			go func() {
				promptDone <- providerTurn.Prompt(context.Background(), session.SessionID, phase.Prompt)
			}()

			// Each member attempt derives its own timer from the shared run
			// deadline, so this waiter cannot consume or miss another
			// attempt's tick.
			result := cli.WaitForSession(ctx, provider, cli.SessionWaitConfig{
				EventCh:                eventCh,
				PromptDone:             promptDone,
				SessionID:              session.SessionID,
				RunID:                  s.runID,
				RunLabel:               child.label,
				PermissionClaimScope:   child.id,
				AutoApprove:            child.autoApprove,
				PermissionClaimTimeout: child.permClaimTimeout,
				Deadline:               deadlineFromContext(ctx),
				// adoptSessionID propagates the authoritative conversation id onto
				// the phase-local session, which is authoritative for retries,
				// resume, and the terminal sentinel. It also updates the aggregate
				// child record when this phase is still the active one.
				AcceptSessionID: func(externalID string) bool {
					return s.adoptSessionAttempt(child, attempt, preAdoptionID, externalID)
				},
				AdoptSessionID: func(externalID string) {
					session.SessionID = externalID
				},
			}, cli.SessionWaitDeps{
				Writer:        attemptWriter,
				FileHandler:   child.fileHandler,
				ControlServer: s.control,
				PreparePermissionClaim: func(ctx context.Context, scope, requestID string, state control.PermissionResolverState, options []any) bool {
					return s.prepareChildPermissionClaim(ctx, child, providerLifecycle, attemptWriter, session.SessionID, scope, requestID, state, options)
				},
				Stderr:       s.stderrWriter(),
				ProviderTurn: providerTurn,
			})

			sessions.remember(phase.Name, session.SessionID, identity)
			return teamrunner.PhaseAttemptResult{
				ExitCode:      result.ExitCode,
				SessionID:     session.SessionID,
				StopReason:    result.StopReason,
				RetryAfter:    result.RetryAfter,
				LoopDirective: result.LoopDirective,
				LoopLabel:     result.LoopLabel,
				Output:        result.Output,
				FinalReply:    result.FinalReply,
				BrokerRunID:   brokerRunID,
			}, nil
		},
	}

	result, err := teamrunner.Run(ctx, opts)

	// Parallel member completion order is nondeterministic. Team configuration
	// order (with post/pre precedence) defines the final authoritative phase on
	// both success and failure. Finalize before every error return so callback
	// timing cannot choose the tombstone identity.
	finalSession, hasFinal := sessions.final(cfg.Post, cfg.Team, cfg.Pre)
	if result.SessionID != "" && result.ExitCode != 0 {
		if identity, ok := s.sessionIdentity(result.SessionID); ok {
			finalSession, hasFinal = workflowSession{SessionID: result.SessionID, Identity: identity}, true
		}
	}
	if hasFinal {
		s.finalizeWorkflowChild(child, finalSession)
	}
	if err != nil {
		child.mu.Lock()
		child.exitCode = 1
		child.mu.Unlock()
		if child.sentinelFile != "" {
			cli.WriteSentinel(child.sentinelFile, 1, child.sessionID(), "error", s.runID, s.stderrWriter())
		}
		return
	}
	child.mu.Lock()
	child.exitCode = result.ExitCode
	child.mu.Unlock()
	child.stashWorkflowTerminalMarker(result.MarkerDirective, result.MarkerLabel)

	// A successful team can finish all members and post phases with end_turn.
	// The aggregate RunResult then has no authoritative SessionID. Use the latest
	// adopted child session ID to populate the terminal sentinel.
	if result.SessionID == "" {
		result.SessionID = child.sessionID()
	}

	if child.sentinelFile != "" {
		if result.Reason != "" {
			cli.WriteSentinelResult(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, result.RetryAfter, result.Reason, "", s.stderrWriter())
		} else {
			cli.WriteSentinelResult(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, result.RetryAfter, "", "", s.stderrWriter())
		}
	}
}

type childAttemptResult struct {
	exitCode   int
	sessionID  string
	stopReason string
	retryAfter int64
}

func (c *childRuntime) dequeuePrompt() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lc.isCompleted() || len(c.promptQueue) == 0 {
		return "", false
	}
	prompt := c.promptQueue[0]
	c.promptQueue = c.promptQueue[1:]
	return prompt, true
}

func (c *childRuntime) waitForNextPrompt(s *Supervisor, ctx context.Context, parkedTimeout time.Duration) (string, bool) {
	var parkedTimeoutCh <-chan time.Time
	if parkedTimeout > 0 {
		timer := time.NewTimer(parkedTimeout)
		defer timer.Stop()
		parkedTimeoutCh = timer.C
	}
	for {
		if prompt, ok := c.dequeuePrompt(); ok {
			// promptDequeued is observational: no activity or admission effect
			// (continuation probes use their own signals).
			c.mu.Lock()
			_, _, err := s.applyLifecycle(c, lifecycleSignal{kind: signalPromptDequeued, class: c.lc.class})
			c.mu.Unlock()
			if err != nil {
				s.reportLifecycleError(c, err)
			}
			return prompt, true
		}
		c.mu.Lock()
		if c.lc.isCompleted() {
			c.mu.Unlock()
			return "", false
		}
		ch := c.promptCh
		c.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return "", false
		case <-parkedTimeoutCh:
			// The grace window for a follow-up prompt elapsed. Whether its
			// session survives for a later cross-process resume is
			// backend-dependent, not guaranteed. Take a prompt that raced the
			// deadline and end the runtime in one critical section, so a
			// prompt queued before the deadline is honored while later
			// RuntimePrompt calls see the runtime ended instead of queueing
			// onto a runtime about to reap.
			c.mu.Lock()
			if len(c.promptQueue) > 0 {
				prompt := c.promptQueue[0]
				c.promptQueue = c.promptQueue[1:]
				_, _, err := s.applyLifecycle(c, lifecycleSignal{kind: signalPromptDequeued, class: c.lc.class})
				c.mu.Unlock()
				if err != nil {
					s.reportLifecycleError(c, err)
				}
				return prompt, true
			}
			// parkTimedOut commits completion -> admission-closed under the
			// same mu as the queue check. No waiter settle, no done close.
			_, _, err := s.applyLifecycle(c, lifecycleSignal{kind: signalParkTimedOut, class: c.lc.class})
			if err != nil {
				s.parkTimeoutFailSafe(c)
			}
			c.mu.Unlock()
			if err != nil {
				s.reportLifecycleError(c, err)
			}
			return "", false
		}
	}
}

func (c *childRuntime) signalPrompt() {
	select {
	case c.promptCh <- struct{}{}:
	default:
	}
}

func (c *childRuntime) sessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session.SessionID
}

// authoritativeIdentityEqual compares the ownership fields that every resume
// path resolves to a complete value. LaunchWrapper is compared separately: it
// is optional on direct resumes (omission inherits), so equality semantics
// differ per path.
func authoritativeIdentityEqual(left, right effectiveIdentity) bool {
	return left.Backend == right.Backend && left.Agent == right.Agent && left.Model == right.Model && left.AgentProfile == right.AgentProfile
}

// launchWrapperSupplied reports whether a resume request carries an explicit
// wrapper. Roster entries do not carry wrappers (docs/backends.md), so an
// omitted wrapper must inherit the session's authoritative one instead of
// reading as a mismatch against it.
func launchWrapperSupplied(wrapper []string) bool {
	return len(wrapper) > 0
}

func resumeIdentityConflict(sessionID, field, supplied, authoritative string) error {
	return fmt.Errorf("cannot resume session %q with %s %q: session belongs to %s %q", sessionID, field, supplied, field, authoritative)
}

// launchWrapperDisplay renders a wrapper argv for identity-conflict messages.
func launchWrapperDisplay(wrapper []string) string {
	if len(wrapper) == 0 {
		return "(none)"
	}
	return strings.Join(wrapper, " ")
}

// resolveDirectResumeIdentity restores omitted identity fields from the
// authoritative session mapping and rejects explicit conflicts. A roster
// selector is complete: it must still resolve to the stored identity, and its
// logical selector must match, so a changed roster can never rewrite ownership.
func (s *Supervisor) resolveDirectResumeIdentity(sessionID string, supplied, resolved effectiveIdentity) (effectiveIdentity, error) {
	mapped, ok := s.sessionIdentity(sessionID)
	if !ok {
		return resolved, nil
	}
	// Both direct paths share this rule: a supplied wrapper that differs
	// conflicts, an omitted one inherits the authoritative value. Roster
	// entries never carry wrappers, so the roster path cannot rely on
	// resolved.LaunchWrapper being non-empty.
	if launchWrapperSupplied(supplied.LaunchWrapper) && !slices.Equal(supplied.LaunchWrapper, mapped.LaunchWrapper) {
		return effectiveIdentity{}, resumeIdentityConflict(sessionID, "launch_wrapper",
			launchWrapperDisplay(supplied.LaunchWrapper), launchWrapperDisplay(mapped.LaunchWrapper))
	}
	if supplied.RosterFile != "" || supplied.RosterEntry != "" {
		if supplied.RosterFile != mapped.RosterFile || supplied.RosterEntry != mapped.RosterEntry {
			return effectiveIdentity{}, fmt.Errorf("cannot resume session %q with a different roster selector", sessionID)
		}
		if !authoritativeIdentityEqual(resolved, mapped) {
			return effectiveIdentity{}, fmt.Errorf("cannot resume session %q: roster identity conflicts with the authoritative session identity", sessionID)
		}
		return mapped, nil
	}
	for _, check := range []struct{ field, value, authoritative string }{
		{"backend", supplied.Backend, mapped.Backend},
		{"agent", supplied.Agent, mapped.Agent},
		{"model", supplied.Model, mapped.Model},
		{"agent_profile", supplied.AgentProfile, mapped.AgentProfile},
	} {
		if check.value != "" && check.value != check.authoritative {
			return effectiveIdentity{}, resumeIdentityConflict(sessionID, check.field, check.value, check.authoritative)
		}
	}
	return mapped, nil
}

// resolveWorkflowResumeIdentity treats a phase selection as complete. Resuming
// with a changed phase agent/model/profile is as unsafe as changing backend, so
// reject it before provider creation rather than silently replacing the map.
// The wrapper comparison stays strict: resolved.LaunchWrapper carries the
// run's configured wrapper on every attempt (phases cannot override it and
// there is no omission channel), so any difference is a real config change.
func (s *Supervisor) resolveWorkflowResumeIdentity(sessionID string, resolved effectiveIdentity) (effectiveIdentity, error) {
	if sessionID == "" {
		return resolved, nil
	}
	mapped, ok := s.sessionIdentity(sessionID)
	if !ok {
		return resolved, nil
	}
	if !authoritativeIdentityEqual(resolved, mapped) {
		return effectiveIdentity{}, fmt.Errorf("cannot resume session %q: phase identity conflicts with the authoritative session identity", sessionID)
	}
	if !slices.Equal(resolved.LaunchWrapper, mapped.LaunchWrapper) {
		return effectiveIdentity{}, resumeIdentityConflict(sessionID, "launch_wrapper",
			launchWrapperDisplay(resolved.LaunchWrapper), launchWrapperDisplay(mapped.LaunchWrapper))
	}
	return mapped, nil
}

// registerSessionAttempt atomically claims a session ID for one provider
// attempt. Identity mappings are immutable, while active ownership is released
// at attempt cleanup. This rejects provisional and authoritative ID collisions
// even when team providers start concurrently.
func (s *Supervisor) registerSessionAttempt(sessionID string, identity effectiveIdentity, provider runtime.Provider, resumeID string) (*sessionAttempt, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("provider returned an empty session ID")
	}
	s.sessionIdentityMu.Lock()
	defer s.sessionIdentityMu.Unlock()
	if s.sessionIdentities == nil {
		s.sessionIdentities = make(map[string]sessionIdentityEntry)
	}
	if s.sessionOwners == nil {
		s.sessionOwners = make(map[string]*sessionAttempt)
	}
	if _, owned := s.sessionOwners[sessionID]; owned {
		return nil, fmt.Errorf("session ID %q is already owned by another active provider attempt", sessionID)
	}
	mapped, wasMapped := s.sessionIdentities[sessionID]
	if wasMapped {
		if resumeID != sessionID {
			return nil, fmt.Errorf("session ID %q collides with an existing authoritative session", sessionID)
		}
		if !authoritativeIdentityEqual(mapped.identity, identity) {
			return nil, fmt.Errorf("session ID %q is already mapped to a different authoritative identity", sessionID)
		}
		identity = mapped.identity
	} else {
		s.sessionIdentities[sessionID] = sessionIdentityEntry{identity: identity}
	}
	attempt := &sessionAttempt{
		provider: provider, identity: identity, provisionalID: sessionID,
		authoritativeID: sessionID, resumeID: resumeID, provisionalWasMapped: wasMapped,
	}
	s.sessionOwners[sessionID] = attempt
	return attempt, nil
}

// adoptSessionAttempt remaps one attempt from its provisional ID to the
// backend's authoritative ID. The map/owner update completes before
// session.start is forwarded. Updating childRuntime is best-effort presentation
// only; a parallel phase occupying the aggregate slot cannot invalidate this
// attempt's adoption.
func (s *Supervisor) adoptSessionAttempt(child *childRuntime, attempt *sessionAttempt, expectedOldID, externalID string) bool {
	if attempt == nil || externalID == "" || externalID == expectedOldID {
		return false
	}
	s.sessionIdentityMu.Lock()
	if s.sessionOwners[expectedOldID] != attempt || attempt.authoritativeID != expectedOldID {
		s.sessionIdentityMu.Unlock()
		return false
	}
	if owner, exists := s.sessionOwners[externalID]; exists && owner != attempt {
		attempt.rejected = true
		s.sessionIdentityMu.Unlock()
		return false
	}
	if mapped, exists := s.sessionIdentities[externalID]; exists {
		if externalID != attempt.resumeID || !authoritativeIdentityEqual(mapped.identity, attempt.identity) {
			attempt.rejected = true
			s.sessionIdentityMu.Unlock()
			return false
		}
	}
	s.sessionIdentities[externalID] = sessionIdentityEntry{identity: attempt.identity}
	s.sessionOwners[externalID] = attempt
	attempt.authoritativeID = externalID
	s.sessionIdentityMu.Unlock()

	child.mu.Lock()
	if child.provider == attempt.provider && child.session.SessionID == expectedOldID {
		child.session.SessionID = externalID
		child.effectiveBackend = attempt.identity.Backend
		child.effectiveAgent = attempt.identity.Agent
		child.effectiveModel = attempt.identity.Model
		child.agentProfile = attempt.identity.AgentProfile
		child.rosterFile = attempt.identity.RosterFile
		child.rosterEntry = attempt.identity.RosterEntry
	}
	child.mu.Unlock()
	return true
}

func (s *Supervisor) releaseSessionAttempt(attempt *sessionAttempt) {
	if attempt == nil {
		return
	}
	s.sessionIdentityMu.Lock()
	for sessionID, owner := range s.sessionOwners {
		if owner == attempt {
			delete(s.sessionOwners, sessionID)
		}
	}
	if !attempt.provisionalWasMapped && (attempt.rejected || attempt.provisionalID != attempt.authoritativeID) {
		if entry, ok := s.sessionIdentities[attempt.provisionalID]; ok && authoritativeIdentityEqual(entry.identity, attempt.identity) {
			delete(s.sessionIdentities, attempt.provisionalID)
		}
	}
	s.sessionIdentityMu.Unlock()
}

func resolveStablePhaseSelection(roster *rosterconfig.Config, backend, agent, model string, phase phaseconfig.Phase, loop bool) (rosterconfig.ResolvedSelection, error) {
	var entry *rosterconfig.Entry
	if phase.RosterEntry != "" {
		if roster == nil {
			return rosterconfig.ResolvedSelection{}, fmt.Errorf("phase %q roster entry %q requires a roster", phase.Name, phase.RosterEntry)
		}
		loaded, err := roster.Lookup(phase.RosterEntry)
		if err != nil {
			return rosterconfig.ResolvedSelection{}, err
		}
		entry = &loaded
	}
	return rosterconfig.Resolve(rosterconfig.ResolveInput{
		Backend:     backend,
		Agent:       agent,
		Model:       model,
		InlineAgent: phase.Agent,
		InlineModel: phase.Model,
		Roster:      entry,
		Loop:        loop,
	})
}

func (s *Supervisor) rememberSessionIdentity(sessionID string, identity effectiveIdentity, _ runtime.Provider) {
	if sessionID == "" {
		return
	}
	s.sessionIdentityMu.Lock()
	if s.sessionIdentities == nil {
		s.sessionIdentities = make(map[string]sessionIdentityEntry)
	}
	if existing, ok := s.sessionIdentities[sessionID]; !ok || authoritativeIdentityEqual(existing.identity, identity) {
		if ok {
			identity = existing.identity
		}
		s.sessionIdentities[sessionID] = sessionIdentityEntry{identity: identity}
	}
	s.sessionIdentityMu.Unlock()
}

func (s *Supervisor) sessionIdentity(sessionID string) (effectiveIdentity, bool) {
	entry, ok := s.sessionIdentityEntry(sessionID)
	if !ok {
		return effectiveIdentity{}, false
	}
	return entry.identity, true
}

func (s *Supervisor) sessionIdentityEntry(sessionID string) (sessionIdentityEntry, bool) {
	if sessionID == "" {
		return sessionIdentityEntry{}, false
	}
	s.sessionIdentityMu.RLock()
	entry, ok := s.sessionIdentities[sessionID]
	s.sessionIdentityMu.RUnlock()
	return entry, ok
}

func (s *Supervisor) beginWorkflowAttempt(child *childRuntime, provider runtime.Provider, session runtime.Session, identity effectiveIdentity) *permissionProviderLifecycle {
	lifecycle := newPermissionProviderLifecycle(child, provider, session.SessionID)
	child.writeMu.Lock()
	lifecycle.closing = child.lc.isShuttingDown()
	if lifecycle.closing {
		lifecycle.cancelAnswers()
	}
	child.mu.Lock()
	// attemptStarted is owned by the adapter: it commits the count. The
	// provider pairing below is imperative and runs in the same critical
	// section.
	var deferredStartErr error
	_, _, deferredStartErr = s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptStarted, class: child.lc.class})
	// These fields are a presentation slot only. Attempt ownership lives in
	// sessionOwners, so parallel replacement cannot reject another provider.
	// Keep the provider and its permission lifecycle generation paired under
	// writeMu so concurrent team phases cannot route an answer across attempts.
	child.provider = provider
	child.session = session
	child.providerLifecycle = lifecycle
	child.effectiveBackend = identity.Backend
	child.effectiveAgent = identity.Agent
	child.effectiveModel = identity.Model
	child.agentProfile = identity.AgentProfile
	child.rosterFile = identity.RosterFile
	child.rosterEntry = identity.RosterEntry
	child.mu.Unlock()
	child.writeMu.Unlock()
	if deferredStartErr != nil {
		s.reportLifecycleError(child, deferredStartErr)
	}
	return lifecycle
}

func (s *Supervisor) endWorkflowAttempt(child *childRuntime, provider runtime.Provider) {
	child.mu.Lock()
	// The adapter owns attemptSettled: it decrements the count, clears the
	// presentation and releases the waiters on the transition to zero. The
	// provider/session teardown below is imperative and runs in the same
	// critical section.
	_, _, settledErr := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptSettled, class: child.lc.class})
	if child.provider == provider {
		sessionID := child.session.SessionID
		child.provider = nil
		// Retain only the authoritative ID. The provider's PID and other fields
		// become invalid when it closes.
		child.session = runtime.Session{SessionID: sessionID}
	}
	child.mu.Unlock()
	if settledErr != nil {
		s.reportLifecycleError(child, settledErr)
	}
}

// finalizeWorkflowChild resets the workflow child's activity to the finalized
// state and settles its waiters. On paths that record an exit code, the caller
// writes it after this returns; panic teardown writes none.
func (s *Supervisor) finalizeWorkflowChild(child *childRuntime, final workflowSession) {
	child.mu.Lock()
	// The adapter owns workflowFinalized: it resets the count, settles the
	// waiters unconditionally and never clears presentation. The provider and
	// identity teardown below is imperative and runs in the same critical
	// section.
	sig := lifecycleSignal{kind: signalWorkflowFinalized, class: child.lc.class}
	_, _, finErr := s.applyLifecycle(child, sig)
	child.provider = nil
	child.providerLifecycle = nil
	child.session = runtime.Session{SessionID: final.SessionID}
	child.effectiveBackend = final.Identity.Backend
	child.effectiveAgent = final.Identity.Agent
	child.effectiveModel = final.Identity.Model
	child.agentProfile = final.Identity.AgentProfile
	child.rosterFile = final.Identity.RosterFile
	child.rosterEntry = final.Identity.RosterEntry
	child.mu.Unlock()
	if finErr != nil {
		s.reportLifecycleError(child, finErr)
	}
}

func (s *Supervisor) statusIdentity(child *childRuntime, sessionID string) effectiveIdentity {
	if identity, ok := s.sessionIdentity(sessionID); ok {
		return identity
	}
	identity := effectiveIdentity{
		Backend:       child.effectiveBackend,
		Agent:         child.effectiveAgent,
		Model:         child.effectiveModel,
		AgentProfile:  child.agentProfile,
		RosterFile:    child.rosterFile,
		RosterEntry:   child.rosterEntry,
		LaunchWrapper: child.launchWrapper,
	}
	if identity.Backend == "" {
		identity.Backend = child.backend
	}
	if identity.Agent == "" {
		identity.Agent = child.agent
	}
	if identity.Model == "" {
		identity.Model = child.model
	}
	return identity
}

func (s *Supervisor) runChildAttempt(ctx context.Context, child *childRuntime, resumeID, promptText string, launchAttempt int) childAttemptResult {
	deadline, _ := ctx.Deadline()
	child.mu.Lock()
	// The adapter owns attemptPreparing: it clears the phase presentation.
	_, _, prepErr := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptPreparing, class: child.lc.class})
	child.mu.Unlock()
	if prepErr != nil {
		s.reportLifecycleError(child, prepErr)
	}

	session, err := s.attemptSession(ctx, child, resumeID, launchAttempt)
	if err != nil {
		s.emitChildError(child, fmt.Sprintf("start session: %v", err), "error")
		// attemptStartFailed is only for a prepared attempt that never
		// incremented activity; the pre-activity return records nothing.
		child.mu.Lock()
		_, _, failErr := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptStartFailed, class: child.lc.class})
		child.mu.Unlock()
		if failErr != nil {
			s.reportLifecycleError(child, failErr)
		}
		return childAttemptResult{exitCode: 1}
	}
	child.mu.Lock()
	child.session = session
	attemptProvider := child.provider
	child.mu.Unlock()

	turnCtx, cancelTurn := context.WithCancel(ctx)
	child.mu.Lock()
	// The adapter owns attemptStarted: it commits the count.
	_, _, startErr := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptStarted, class: child.lc.class})
	child.interruptFn = cancelTurn
	child.mu.Unlock()
	if startErr != nil {
		s.reportLifecycleError(child, startErr)
	}
	defer func() {
		cancelTurn()
		child.mu.Lock()
		// The adapter owns attemptSettled: it decrements the count to zero,
		// clears the presentation and releases the waiters. The interrupt
		// teardown below runs in the same critical section.
		_, _, settledErr := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptSettled, class: child.lc.class})
		child.interruptFn = nil
		child.mu.Unlock()
		if settledErr != nil {
			s.reportLifecycleError(child, settledErr)
		}
	}()

	eventCtx, cancelEvents := context.WithCancel(turnCtx)
	defer cancelEvents()

	child.mu.Lock()
	attempt := child.directAttempt
	identity := effectiveIdentity{
		Backend: child.effectiveBackend, Agent: child.effectiveAgent, Model: child.effectiveModel,
		AgentProfile: child.agentProfile, RosterFile: child.rosterFile, RosterEntry: child.rosterEntry,
		LaunchWrapper: child.launchWrapper,
	}
	child.mu.Unlock()
	if mapped, ok := s.sessionIdentity(session.SessionID); ok {
		identity = mapped
	}
	if attempt == nil || attempt.authoritativeID != session.SessionID {
		attempt, err = s.registerSessionAttempt(session.SessionID, identity, attemptProvider, resumeID)
		if err != nil {
			s.emitChildError(child, err.Error(), "error")
			return childAttemptResult{exitCode: 1, sessionID: session.SessionID, stopReason: runtime.SessionIDConflictStopReason}
		}
		child.mu.Lock()
		child.directAttempt = attempt
		child.mu.Unlock()
	}
	defer func() {
		s.releaseSessionAttempt(attempt)
		child.mu.Lock()
		if child.directAttempt == attempt {
			child.directAttempt = nil
		}
		child.mu.Unlock()
	}()
	eventCh, err := attemptProvider.Events(eventCtx, session.SessionID)
	if err != nil {
		s.emitChildError(child, fmt.Sprintf("subscribe events: %v", err), "error")
		return childAttemptResult{exitCode: 1, sessionID: session.SessionID}
	}

	preAdoptionID := session.SessionID
	promptSessionID := session.SessionID

	providerLifecycle := s.ensureChildProviderLifecycle(child)
	providerTurn, admitted := s.beginChildProviderTurn(child, providerLifecycle)
	if !admitted {
		return childAttemptResult{exitCode: 130, sessionID: session.SessionID}
	}
	promptDone := make(chan error, 1)
	go func() {
		defer func() { recover() }()
		promptDone <- providerTurn.Prompt(turnCtx, promptSessionID, promptText)
	}()

	// Tag events with runtime_id and fan out to both file and control subscribers.
	taggedWriter := newRuntimeFanoutWriter(s, child,
		workflowEventMetadata(s, child), newRecorderFor(s, child.id))
	s.setFanoutWriter(child, taggedWriter)
	result := cli.WaitForSession(turnCtx, attemptProvider, cli.SessionWaitConfig{
		EventCh:                eventCh,
		PromptDone:             promptDone,
		SessionID:              session.SessionID,
		RunID:                  s.runID,
		RunLabel:               child.label,
		PermissionClaimScope:   child.id,
		AutoApprove:            child.autoApprove,
		PermissionClaimTimeout: child.permClaimTimeout,
		Deadline:               deadline,
		// adoptSessionID fires inside WaitForSession before the session.start
		// event is forwarded, so the stable status identity cannot lag the
		// authoritative event. The attempt-local session is always updated.
		// The aggregate child record is updated only when this attempt is
		// still the active one, so a late retry cannot overwrite a newer
		// session.
		AcceptSessionID: func(externalID string) bool {
			return s.adoptSessionAttempt(child, attempt, preAdoptionID, externalID)
		},
		AdoptSessionID: func(externalID string) {
			session.SessionID = externalID
		},
	}, cli.SessionWaitDeps{
		Writer:        taggedWriter,
		FileHandler:   child.fileHandler,
		ControlServer: s.control,
		PreparePermissionClaim: func(ctx context.Context, scope, requestID string, state control.PermissionResolverState, options []any) bool {
			return s.prepareChildPermissionClaim(ctx, child, providerLifecycle, taggedWriter, session.SessionID, scope, requestID, state, options)
		},
		Stderr:       s.stderrWriter(),
		ProviderTurn: providerTurn,
	})
	exitCode := result.ExitCode
	return childAttemptResult{exitCode: exitCode, sessionID: session.SessionID, stopReason: result.StopReason, retryAfter: result.RetryAfter}
}

func (s *Supervisor) attemptSession(ctx context.Context, child *childRuntime, resumeID string, launchAttempt int) (runtime.Session, error) {
	if resumeID == "" {
		child.mu.Lock()
		session := child.session
		child.mu.Unlock()
		if session.SessionID != "" {
			return session, nil
		}
	}
	child.mu.Lock()
	identity := effectiveIdentity{
		Backend:      child.effectiveBackend,
		Agent:        child.effectiveAgent,
		Model:        child.effectiveModel,
		AgentProfile: child.agentProfile,
	}
	provider := child.provider
	fallbackBackend := child.backend
	fallbackAgent, fallbackModel := child.agent, child.model
	fallbackProfile := child.agentProfile
	label, dir, thinking := child.label, child.dir, child.thinking
	brokerToken := child.brokerToken
	child.mu.Unlock()
	_, mapped := s.sessionIdentity(resumeID)
	if mappedIdentity, ok := s.sessionIdentity(resumeID); ok {
		identity = mappedIdentity
	}
	if !mapped {
		if identity.Backend == "" {
			identity.Backend = fallbackBackend
		}
		if identity.AgentProfile == "" {
			identity.AgentProfile = fallbackProfile
		}
		if identity.Agent == "" && identity.Model == "" {
			// Preserve legacy fields only when no authoritative mapping exists.
			identity.Agent, identity.Model = fallbackAgent, fallbackModel
		}
	}
	return cli.StartSession(ctx, provider, identity.Backend, runtime.StartOptions{
		Agent:        identity.Agent,
		AgentProfile: identity.AgentProfile,
		Label:        label,
		Dir:          dir,
		Model:        identity.Model,
		Thinking:     thinking,
		RuntimeID:    child.id,
		Broker:       s.broker,
		BrokerToken:  brokerToken,
		Messaging:    s.messaging,
		Provenance:   runtime.DirectLaunchProvenance(child.id, label, launchAttempt),
	}, resumeID)
}

// takeFileSnapshot records the set of files (relative paths) in dir for
// later output-file diffing. Returns the snapshot as a sorted string slice.
func (s *Supervisor) takeFileSnapshot(dir string) []string {
	if dir == "" {
		return nil
	}
	var files []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return nil
		}
		files = append(files, rel)
		return nil
	})
	sort.Strings(files)
	return files
}

// computeOutputFiles computes the diff between the pre-run snapshot and the
// current file state in the runtime's working directory. Clears the snapshot
// after computing so results are returned only once.
func (s *Supervisor) computeOutputFiles(runtimeID string) []string {
	s.fileSnapMu.Lock()
	snapshot, ok := s.fileSnapshots[runtimeID]
	delete(s.fileSnapshots, runtimeID)
	s.fileSnapMu.Unlock()
	if !ok {
		return nil
	}

	// Find the runtime's working directory.
	s.controlMu.Lock()
	rt := s.runtimes[runtimeID]
	s.controlMu.Unlock()
	if rt == nil || rt.dir == "" {
		return nil
	}

	current := s.takeFileSnapshot(rt.dir)
	if len(current) == 0 {
		return nil
	}

	// Return anything in current that wasn't in the pre-snapshot.
	snapSet := make(map[string]struct{}, len(snapshot))
	for _, f := range snapshot {
		snapSet[f] = struct{}{}
	}
	var output []string
	for _, f := range current {
		if _, seen := snapSet[f]; !seen {
			output = append(output, f)
		}
	}
	return output
}

func (s *Supervisor) emitSessionEnd(child *childRuntime, exitCode int, stopReason string) {
	fields := map[string]any{
		"stop_reason": stopReason,
		"runtime_id":  child.id,
		"exit_code":   exitCode,
		"ts":          time.Now().UnixMilli(),
	}
	// Compute output file diff if pre-snapshot exists.
	outputFiles := s.computeOutputFiles(child.id)
	if len(outputFiles) > 0 {
		fields["output_files"] = outputFiles
	}
	s.control.PublishEvent(events.Event{
		Event:     "session.end",
		SessionID: child.sessionID(),
		Fields:    fields,
	})
}

// stopReasonForCtxErr maps a run context error to the stop reason the idle
// terminal end records: a deadline is a timeout, a cancel is a kill.
func stopReasonForCtxErr(ctxErr error) string {
	if errors.Is(ctxErr, context.DeadlineExceeded) {
		return "timeout"
	}
	return "cancelled"
}

// idleEndFault builds the terminal payload the fault fail-safe applies when a
// run ends while the child has no running attempt: the idle terminal end
// (exit code, stop reason, sentinel, session.end) when the context ended, or
// the bare sentinel-written fault when it did not. The stop reason and exit
// code come from the canonical context-error and stop-reason mappings.
func idleEndFault(child *childRuntime, ctxErr error) lifecycleFault {
	if ctxErr == nil {
		return lifecycleFault{sentinelWritten: true}
	}
	stopReason := stopReasonForCtxErr(ctxErr)
	return lifecycleFault{
		exitCode: runtime.ExitCodeForStopReason(stopReason), reason: stopReason, sessionID: child.sessionID(),
		recordExit: true, idleEnd: true,
	}
}

// writeIdleContextEnd records the run context ending while the child has no
// running attempt (parked, waiting for a slot, or in retry backoff), matching
// how the same end reports itself during an attempt: a deadline is a timeout
// (timeout sentinel, timeout session.end) and a cancel is a kill (cancelled
// sentinel, cancelled session.end). The exit code comes from the canonical
// stop-reason mapping. The session.end is published straight to the control
// plane, bypassing the fanout writer that records an attempt's session.end in
// child state, so the terminal phase and stop reason are recorded here for
// status and WaitTurn snapshots.
func (s *Supervisor) writeIdleContextEnd(child *childRuntime, ctxErr error) {
	stopReason := stopReasonForCtxErr(ctxErr)
	exitCode := runtime.ExitCodeForStopReason(stopReason)
	sessionID := child.sessionID()
	child.mu.Lock()
	// The adapter owns idleContextEnded's locked data writes (exit code,
	// phase, stop reason) and the machine's deferred sentinel and session.end
	// effects execute below, after the lock is released.
	_, eff, err := s.applyLifecycle(child, lifecycleSignal{
		kind: signalIdleContextEnded, class: child.lc.class,
		idleExitCode: exitCode, idleStopReason: stopReason, resultSessionID: sessionID,
	})
	child.mu.Unlock()
	if err != nil {
		// The dropped idle-end writes are recovered by the fault fail-safe:
		// terminal data, sentinel and session.end.
		s.reportLifecycleError(child, err)
		s.failSafeRunChildTransition(child, idleEndFault(child, ctxErr))
		return
	}
	s.executeDeferredLifecycleEffects(child, eff)
}

func (s *Supervisor) emitChildError(child *childRuntime, message, source string) {
	s.control.PublishEvent(events.Event{
		Event:     "avenor.error",
		SessionID: child.sessionID(),
		Fields: map[string]any{
			"message":    message,
			"source":     source,
			"runtime_id": child.id,
			"ts":         time.Now().UnixMilli(),
		},
	})
}

func (s *Supervisor) shutdown(mode string) int {
	// Always release the root-owned budget file, including the zero-timeout and
	// kill paths below. Nested supervisors only close their inherited handle.
	defer func() {
		if s.broker != nil {
			_ = s.broker.Stop()
		}
		if s.messaging != nil {
			_ = s.messaging.Close()
		}
		s.closeTreeBudget()
	}()

	// This lock is the reservation boundary: a spawn either registers before
	// shutdown and is included below, or observes shuttingDown and is rejected.
	s.controlMu.Lock()
	s.shuttingDown = true
	runtimes := make([]*childRuntime, 0, len(s.runtimes))
	for _, rt := range s.runtimes {
		runtimes = append(runtimes, rt)
	}
	s.controlMu.Unlock()
	s.stopLeaseHeartbeats()
	s.stopLeaseSweep()

	if s.afterShutdownAdmissionClosed != nil {
		s.afterShutdownAdmissionClosed()
	}
	s.shutdownManagedHTTPServers()

	for _, rt := range runtimes {
		s.beginChildShutdown(rt)
	}

	timeout := s.config.ShutdownTimeout
	if mode == "kill" || timeout == 0 {
		var wg sync.WaitGroup
		for _, rt := range runtimes {
			wg.Add(1)
			go func(r *childRuntime) {
				defer wg.Done()
				<-r.done
			}(rt)
		}
		wg.Wait()
		return 0
	}

	var wg sync.WaitGroup
	for _, rt := range runtimes {
		wg.Add(1)
		go func(r *childRuntime) {
			defer wg.Done()
			<-r.done
		}(rt)
	}
	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()

	select {
	case <-allDone:
	case <-time.After(timeout):
		remaining := 0
		for _, rt := range runtimes {
			select {
			case <-rt.done:
			default:
				remaining++
			}
		}
		if remaining > 0 {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: %d runtimes did not finish within %v\n", remaining, timeout)
		}
	}
	return 0
}

func (s *Supervisor) activeRuntimeCount() int {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	return s.activeRuntimeCountLocked()
}

func (s *Supervisor) listRuntimes() []map[string]any {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	out := make([]map[string]any, 0, len(s.runtimes))
	for _, rt := range s.runtimes {
		rt.mu.Lock()
		status := rt.lc.runtimeStatus()
		identity := s.statusIdentity(rt, rt.session.SessionID)
		entry := map[string]any{
			"runtime_id":         rt.id,
			"session_id":         rt.session.SessionID,
			"run_id":             rt.runID,
			"label":              rt.label,
			"agent":              identity.Agent,
			"agent_profile":      identity.AgentProfile,
			"model":              identity.Model,
			"thinking":           rt.thinking,
			"backend":            identity.Backend,
			"roster_file":        identity.RosterFile,
			"roster_entry":       identity.RosterEntry,
			"effective_agent":    identity.Agent,
			"effective_model":    identity.Model,
			"effective_backend":  identity.Backend,
			"dir":                rt.dir,
			"status":             string(status),
			"started_at":         rt.startedAt,
			"exit_code":          rt.exitCode,
			"on_event":           rt.onEvent,
			"event_path":         rt.onEvent,
			"sentinel_file":      rt.sentinelFile,
			"parent_id":          rt.parentID,
			"children":           rt.children,
			"pid":                rt.session.PID,
			"phase":              string(rt.phase),
			"phase_label":        rt.phaseLabel,
			"pending_permission": rt.pendingPermission,
			"latest_seq":         rt.latestSeq,
			"auto_approve":       rt.autoApprove,
			// True once a pending request exhausted its decider retries (harness
			// spawns or direct-model calls); it stays pending for a human gate.
			"permission_decider_exhausted": rt.permissionDeciderExhausted,
		}
		if rt.permissionDecider != nil {
			entry["permission_decider"] = rt.permissionDecider.Entry
		}
		if rt.modelDecider != nil {
			entry["permission_model_decider"] = rt.modelDecider.Provider + "/" + rt.modelDecider.Model
		}
		if rt.permission != nil {
			perm := make(map[string]any, len(rt.permission))
			for k, v := range rt.permission {
				perm[k] = v
			}
			entry["permission"] = perm
		}
		if rt.usage != nil {
			usage := make(map[string]any, len(rt.usage))
			for k, v := range rt.usage {
				usage[k] = v
			}
			entry["usage"] = usage
		}
		if rt.finalOutput != "" {
			entry["final_output"] = rt.finalOutput
		}
		if rt.finalOutputTruncated {
			entry["final_output_truncated"] = true
		}
		rt.mu.Unlock()
		out = append(out, entry)
	}
	return out
}

func (s *Supervisor) cancelRuntime(rtID string) error {
	s.controlMu.Lock()
	rt := s.runtimes[rtID]
	s.controlMu.Unlock()
	if rt == nil {
		return fmt.Errorf("runtime %q not found", rtID)
	}
	s.beginChildShutdown(rt)
	return nil
}

// cancelWorkflowRuntimes begins shutdown for every child runtime belonging to
// the given workflow instance, signaling each attempt's runtime so its
// termination records the canceled attempt fact. It returns the number of
// runtimes signaled; the registry is read under the control lock and the
// shutdowns run outside it (mirroring cancelRuntime).
func (s *Supervisor) cancelWorkflowRuntimes(workflowID string) int {
	s.controlMu.Lock()
	children := make([]*childRuntime, 0, 1)
	for _, rt := range s.runtimes {
		if rt.workflowID == workflowID {
			children = append(children, rt)
		}
	}
	s.controlMu.Unlock()
	for _, child := range children {
		s.beginChildShutdown(child)
	}
	return len(children)
}

// settleTurnLocked wakes wait_turn callers that registered before the
// runtime's current turn settled. Callers must hold child.mu.
func (child *childRuntime) settleTurnLocked() {
	if len(child.turnWaiters) == 0 {
		return
	}
	waiters := child.turnWaiters
	child.turnWaiters = nil
	for _, wait := range waiters {
		close(wait)
	}
}

// removeTurnWaiterLocked drops a waiter that is still registered. It is a
// no-op when settleTurnLocked already consumed it. Callers must hold
// child.mu.
func (child *childRuntime) removeTurnWaiterLocked(wait chan struct{}) {
	for i, w := range child.turnWaiters {
		if w == wait {
			child.turnWaiters = append(child.turnWaiters[:i], child.turnWaiters[i+1:]...)
			break
		}
	}
}

// complete publishes terminal completion after the caller has finished all
// teardown. It is safe for reservation rollback and a child goroutine to race.
// The owning supervisor and the attested completion boundary are explicit
// arguments; runEnded/spawnRolledBack emit the publishCompletion cue that runs
// this boundary, and the completionPublished transaction runs here.
func (child *childRuntime) complete(s *Supervisor, boundary completionBoundary) {
	child.doneOnce.Do(func() {
		child.releaseTreeToken()
		child.mu.Lock()
		// The adapter owns the publication: it commits the completion stage
		// and settles the turn waiters.
		_, pubEff, pubErr := s.applyLifecycle(child, lifecycleSignal{
			kind: signalCompletionPublished, class: child.lc.class, boundary: boundary,
		})
		if pubErr != nil {
			// A failed publication must not strand registered turn waiters or
			// leave the capacity reader counting the ended runtime as active:
			s.completePublicationFailSafe(child)
		}
		child.mu.Unlock()
		if pubErr != nil {
			s.reportLifecycleError(child, pubErr)
			// A failed publication must still close done: a machine error can
			// never leave external waiters blocked on done forever.
			close(child.done)
		} else {
			// closeDone runs inside doneOnce, after the publication transaction.
			s.executeDeferredLifecycleEffects(child, pubEff)
		}
	})
}

// closeRunDone closes the run-goroutine join point exactly once. The close
// owner varies — spawn rollback, the run-goroutine teardown defers, and the
// test finish helpers — so the close must be idempotent. A nil channel has no
// join point to close.
func (child *childRuntime) closeRunDone() {
	if child.runDone == nil {
		return
	}
	child.runDoneOnce.Do(func() { close(child.runDone) })
}

// releaseTreeToken returns the runtime's current tree-budget slot, if any. It
// is safe to call when no token is held and to call more than once.
func (child *childRuntime) releaseTreeToken() {
	child.mu.Lock()
	tok := child.treeToken
	child.treeToken = ""
	child.mu.Unlock()
	if tok == "" || child.treeBudgetRelease == nil {
		return
	}
	child.treeBudgetRelease(tok)
}

func newPermissionProviderLifecycle(child *childRuntime, provider runtime.Provider, sessionID string) *permissionProviderLifecycle {
	parent := child.lifecycleCtx
	if parent == nil {
		parent = context.Background()
	}
	answerCtx, cancelAnswers := context.WithCancel(parent)
	providerCalls := cli.NewProviderLifecycle(provider)
	return &permissionProviderLifecycle{
		provider:      provider,
		sessionID:     sessionID,
		answerCtx:     answerCtx,
		cancelAnswers: cancelAnswers,
		providerCalls: providerCalls,
		turn:          providerCalls.NewTurn(),
	}
}

func (s *Supervisor) installChildProvider(child *childRuntime, provider runtime.Provider, session runtime.Session) *permissionProviderLifecycle {
	lifecycle := newPermissionProviderLifecycle(child, provider, session.SessionID)
	child.writeMu.Lock()
	lifecycle.closing = child.lc.isShuttingDown()
	if lifecycle.closing {
		lifecycle.cancelAnswers()
	}
	child.mu.Lock()
	child.provider = provider
	child.session = session
	child.providerLifecycle = lifecycle
	child.mu.Unlock()
	child.writeMu.Unlock()
	return lifecycle
}

func (s *Supervisor) ensureChildProviderLifecycle(child *childRuntime) *permissionProviderLifecycle {
	child.writeMu.Lock()
	defer child.writeMu.Unlock()
	child.mu.Lock()
	defer child.mu.Unlock()
	if child.provider == nil {
		return nil
	}
	if child.providerLifecycle == nil || child.providerLifecycle.provider != child.provider {
		child.providerLifecycle = newPermissionProviderLifecycle(child, child.provider, child.session.SessionID)
		child.providerLifecycle.closing = child.lc.isShuttingDown()
		if child.providerLifecycle.closing {
			child.providerLifecycle.cancelAnswers()
		}
	}
	return child.providerLifecycle
}

// beginChildProviderTurn gives WaitForSession and stable direct answers the
// same cancellation and live-call coordinator for this provider generation.
// Turn admission is rejected once provider or child shutdown begins, so no new
// Prompt can enter after the teardown boundary.
func (s *Supervisor) beginChildProviderTurn(child *childRuntime, lifecycle *permissionProviderLifecycle) (*cli.ProviderTurn, bool) {
	child.writeMu.Lock()
	defer child.writeMu.Unlock()
	if lifecycle == nil || lifecycle.closing || child.lc.isShuttingDown() || child.lc.isWriterClosing() {
		return nil, false
	}
	lifecycle.turn = lifecycle.providerCalls.NewTurn()
	return lifecycle.turn, true
}

// prepareChildPermissionClaim linearizes a new claim reservation against both
// supervisor shutdown and phase-provider close. A reservation admitted first
// is tracked until control registration and exact-provider binding finish, so
// provider close cannot overtake it; later reservations are rejected.
func (s *Supervisor) prepareChildPermissionClaim(ctx context.Context, child *childRuntime, lifecycle *permissionProviderLifecycle, writer *runtimeFanoutWriter, sessionID, scope, requestID string, state control.PermissionResolverState, options []any) bool {
	child.writeMu.Lock()
	// The direct-answer path uses writeMu -> controlMu. Checking the supervisor
	// gate in that order closes the interval after supervisor shutdown starts
	// but before beginChildShutdown reaches this child (including managed HTTP
	// provider teardown).
	s.controlMu.Lock()
	supervisorClosing := s.shuttingDown
	s.controlMu.Unlock()
	if supervisorClosing || child.lc.isShuttingDown() || child.lc.isWriterClosing() || lifecycle == nil || lifecycle.closing || writer == nil || ctx.Err() != nil {
		child.writeMu.Unlock()
		return false
	}
	// Session adoption may replace the provisional ID after provider install.
	// Keep provider cancellation on the same authoritative session generation
	// that this claim binds for direct answer delivery.
	lifecycle.sessionID = sessionID
	if lifecycle.permissionReservations == 0 {
		lifecycle.permissionReservationsDone = make(chan struct{})
	}
	lifecycle.permissionReservations++
	turn := lifecycle.turn
	child.writeMu.Unlock()

	prepared := s.control.PreparePermissionClaimAfterDirectDeliveryWith(ctx, scope, requestID, state, options, func() {
		s.permissions.bindProvider(scope, requestID, permissionProviderBinding{lifecycle: lifecycle, turn: turn, sessionID: sessionID, writer: writer})
	})

	child.writeMu.Lock()
	lifecycle.permissionReservations--
	if lifecycle.permissionReservations == 0 {
		close(lifecycle.permissionReservationsDone)
	}
	child.writeMu.Unlock()
	return prepared
}

// beginChildShutdown establishes the permission-admission boundary before
// cancellation starts. The returned provider lifecycle can then be drained and
// closed without admitting a late direct answer in the cancellation window.
func (s *Supervisor) beginChildShutdown(child *childRuntime) *permissionProviderLifecycle {
	child.writeMu.Lock()
	child.mu.Lock()
	// First-shutdown comes from the pre-state: the parent context cancel fires
	// only when no earlier shutdown request committed.
	alreadyShuttingDown := child.lc.isShuttingDown()
	// shutdownRequested is a dedicated dual-locked signal (writeMu ->
	// child.mu); the adapter commits the requested stage. The
	// provider-cancel consideration and the first-only parent cancel execute
	// below, after every lock is released.
	_, shutdownEff, shutdownErr := s.applyLifecycleWriteLocked(child, lifecycleSignal{
		kind: signalShutdownRequested, class: child.lc.class,
	})
	if shutdownErr != nil {
		// A failed transition must not leave admission open or allow a second
		// parent cancel: commit the requested stage.
		s.shutdownFailSafe(child)
	}
	lifecycle := child.providerLifecycle
	if child.provider != nil && (lifecycle == nil || lifecycle.provider != child.provider) {
		lifecycle = newPermissionProviderLifecycle(child, child.provider, child.session.SessionID)
		child.providerLifecycle = lifecycle
	}
	if lifecycle != nil {
		lifecycle.closing = true
		// Preserve this admission-boundary order: answerCtx must interrupt
		// admitted answers before shared RequestCancel and the parent cancelFn
		// run outside writeMu. The parent context reaches answerCtx later.
		lifecycle.cancelAnswers()
	}
	cancel := child.cancelFn
	child.mu.Unlock()
	cancelProvider := lifecycle != nil && lifecycle.directAnswers > 0
	var turn *cli.ProviderTurn
	var sessionID string
	if lifecycle != nil {
		turn = lifecycle.turn
		sessionID = lifecycle.sessionID
	}
	child.writeMu.Unlock()
	if shutdownErr != nil {
		// The transition failure must not skip the provider request cancel or
		// the first parent cancel always performed: emit the same effects a
		// valid shutdownRequested transaction would have.
		shutdownEff = shutdownRequestedEffects(alreadyShuttingDown)
	}
	// The provider-cancel consideration runs on every shutdown request; whether
	// a RequestCancel fires is captured eligibility, not a lifecycle dimension.
	s.executeShutdownDeferredEffects(child, shutdownEff, turn, sessionID, cancelProvider, cancel)
	if shutdownErr != nil {
		s.reportLifecycleError(child, shutdownErr)
	}
	return lifecycle
}

// closeChildProvider removes this exact provider generation from answer
// routing, drains answers admitted before that boundary, and only then closes
// the provider. Exact generations matter for reused providers and concurrent
// team phases: stale cleanup cannot clear or wait on a newer active phase.
func (s *Supervisor) closeChildProvider(child *childRuntime, lifecycle *permissionProviderLifecycle) {
	if lifecycle == nil {
		return
	}
	child.writeMu.Lock()
	lifecycle.closing = true
	lifecycle.cancelAnswers()
	child.mu.Lock()
	if child.providerLifecycle == lifecycle && child.provider == lifecycle.provider {
		sessionID := child.session.SessionID
		child.provider = nil
		child.providerLifecycle = nil
		// Retain only the authoritative ID. The provider's PID and other fields
		// become invalid when its connection closes.
		child.session = runtime.Session{SessionID: sessionID}
	}
	child.mu.Unlock()
	var waits []<-chan struct{}
	if lifecycle.directAnswers > 0 {
		waits = append(waits, lifecycle.directAnswersDone)
	}
	if lifecycle.permissionReservations > 0 {
		waits = append(waits, lifecycle.permissionReservationsDone)
	}
	if len(waits) > 0 && s.beforeChildProviderCloseWait != nil {
		s.beforeChildProviderCloseWait()
	}
	cancelProvider := lifecycle.directAnswers > 0
	turn := lifecycle.turn
	sessionID := lifecycle.sessionID
	child.writeMu.Unlock()
	if cancelProvider && turn != nil {
		turn.RequestCancel(sessionID, directPermissionJoinTimeout)
	}
	for _, done := range waits {
		<-done
	}
	// RequestClose is non-blocking while an AnswerPermission or Cancel call is
	// still live. The shared lifecycle closes later, after the last such call
	// returns, rather than racing the abandoned provider operation.
	lifecycle.providerCalls.RequestClose()
}

func (s *Supervisor) beginDirectPermissionAnswer(child *childRuntime, binding *permissionProviderBinding) (*directPermissionReservation, bool) {
	child.writeMu.Lock()
	defer child.writeMu.Unlock()
	s.controlMu.Lock()
	supervisorClosing := s.shuttingDown
	s.controlMu.Unlock()
	if supervisorClosing || child.lc.isShuttingDown() || child.lc.isWriterClosing() {
		return nil, false
	}
	child.mu.Lock()
	var provider runtime.Provider
	var sessionID string
	var writer *runtimeFanoutWriter
	var lifecycle *permissionProviderLifecycle
	var turn *cli.ProviderTurn
	if binding != nil {
		lifecycle = binding.lifecycle
		turn = binding.turn
		provider = lifecycle.provider
		sessionID = binding.sessionID
		writer = binding.writer
	} else {
		provider = child.provider
		sessionID = child.session.SessionID
		writer = child.fanoutWriter
		lifecycle = child.providerLifecycle
		if provider != nil && (lifecycle == nil || lifecycle.provider != provider) {
			lifecycle = newPermissionProviderLifecycle(child, provider, sessionID)
			child.providerLifecycle = lifecycle
		}
		if lifecycle != nil {
			turn = lifecycle.turn
		}
	}
	if writer == nil {
		writer = s.fanoutWriterForChildLocked(child)
	}
	child.mu.Unlock()
	if provider == nil || sessionID == "" || lifecycle == nil || turn == nil || lifecycle.closing || writer == nil {
		return nil, false
	}
	lifecycle.sessionID = sessionID
	ctx := lifecycle.answerCtx
	if ctx == nil || ctx.Err() != nil {
		return nil, false
	}
	if child.directAnswers == 0 {
		child.directAnswersDone = make(chan struct{})
	}
	if lifecycle.directAnswers == 0 {
		lifecycle.directAnswersDone = make(chan struct{})
	}
	child.directAnswers++
	lifecycle.directAnswers++
	return &directPermissionReservation{child: child, lifecycle: lifecycle, turn: turn, sessionID: sessionID, ctx: ctx, writer: writer}, true
}

const directPermissionJoinTimeout = 5 * time.Second

// callDirectPermissionProvider contains an untrusted provider call in its own
// goroutine. Once the exact provider generation starts closing, Cancel is
// issued before a bounded join. A provider that ignores both context and
// Cancel may leak its own call, but it cannot deadlock child teardown or hold
// the runtime write lock indefinitely.
func (s *Supervisor) callDirectPermissionProvider(reservation *directPermissionReservation, requestID string, response runtime.PermissionResponse) error {
	result := make(chan error, 1)
	go func() {
		result <- reservation.turn.AnswerPermission(reservation.ctx, reservation.sessionID, requestID, response)
	}()

	select {
	case err := <-result:
		return err
	default:
	}
	select {
	case err := <-result:
		return err
	case <-reservation.ctx.Done():
	}

	reservation.turn.RequestCancel(reservation.sessionID, directPermissionJoinTimeout)
	timer := time.NewTimer(directPermissionJoinTimeout)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		return reservation.ctx.Err()
	}
}

func (s *Supervisor) finishDirectPermissionAnswerLocked(child *childRuntime, reservation *directPermissionReservation) {
	child.directAnswers--
	if child.directAnswers == 0 {
		close(child.directAnswersDone)
	}
	lifecycle := reservation.lifecycle
	lifecycle.directAnswers--
	if lifecycle.directAnswers == 0 {
		close(lifecycle.directAnswersDone)
	}
}

// closeChildEventWriter closes the durable sink only after every direct answer
// admitted before teardown has written its canonical permission.response.
func (s *Supervisor) closeChildEventWriter(child *childRuntime) {
	child.writeMu.Lock()
	// writerCloseBegun is a dedicated dual-locked signal (writeMu ->
	// child.mu); the adapter commits the closing stage before the drain. The wait for direct answers happens outside writeMu.
	child.mu.Lock()
	_, _, begunErr := s.applyLifecycleWriteLocked(child, lifecycleSignal{
		kind: signalWriterCloseBegun, class: child.lc.class,
	})
	if begunErr != nil {
		// A failed transition must still mark the writer closing, so admission
		// gates and the completion precondition observe the drain boundary.
		s.writerCloseBegunFailSafe(child)
	}
	child.mu.Unlock()
	var finishedErr error
	if child.directAnswers > 0 {
		done := child.directAnswersDone
		if s.beforeChildWriterCloseWait != nil {
			s.beforeChildWriterCloseWait()
		}
		child.writeMu.Unlock()
		<-done
		child.writeMu.Lock()
	}
	// Close runs under writeMu only when the writer is not already closed and
	// the sink exists; a nil sink stays writerClosing.
	if !child.lc.isWriterClosed() && child.eventWriter != nil {
		_ = child.eventWriter.Close()
		// writerCloseFinished is sent only after Close was actually called on a
		// nonnil sink, regardless of Close's ignored error. No signal when the
		// sink is nil (the caller's guard stays writerClosing).
		child.mu.Lock()
		_, _, finishedErr = s.applyLifecycleWriteLocked(child, lifecycleSignal{
			kind: signalWriterCloseFinished, class: child.lc.class, closeCalled: true,
		})
		if finishedErr != nil {
			// Close already ran, so the closed stage must still commit; a repeat
			// close must never call Close on the sink again.
			s.writerCloseFinishedFailSafe(child)
		}
		child.mu.Unlock()
	}
	child.writeMu.Unlock()
	if begunErr != nil {
		s.reportLifecycleError(child, begunErr)
	}
	if finishedErr != nil {
		s.reportLifecycleError(child, finishedErr)
	}
}

// workflowEventMetadata returns event metadata threaded with workflow execution
// identity when the child is part of a workflow run, otherwise plain metadata.
// The per-runtime event seq is owned by the control server's canonical event
// state, so no per-turn counter seeding is needed here.
func workflowEventMetadata(s *Supervisor, child *childRuntime) *cli.EventMetadata {
	meta := cli.NewEventMetadata(s.runID, child.label, child.id)
	if child.workflowID != "" {
		meta = meta.WithWorkflow(child.workflowID, child.nodeID, child.activationID, child.attemptID)
	}
	return meta
}

func (s *Supervisor) setFanoutWriter(child *childRuntime, writer *runtimeFanoutWriter) {
	child.writeMu.Lock()
	child.mu.Lock()
	child.fanoutWriter = writer
	child.mu.Unlock()
	child.writeMu.Unlock()
}

// fanoutWriterLocked returns the writer associated with the active runtime.
// Production children install one before they can receive permission answers;
// the fallback keeps focused unit tests with hand-built children observable
// without reintroducing a direct ControlServer publication path.
func (s *Supervisor) fanoutWriterLocked(child *childRuntime) *runtimeFanoutWriter {
	child.mu.Lock()
	defer child.mu.Unlock()
	return s.fanoutWriterForChildLocked(child)
}

// fanoutWriterForChildLocked requires child.mu. Callers that also need
// child.writeMu must acquire writeMu first.
func (s *Supervisor) fanoutWriterForChildLocked(child *childRuntime) *runtimeFanoutWriter {
	writer := child.fanoutWriter
	if writer == nil {
		writer = newRuntimeFanoutWriter(s, child, cli.NewEventMetadata(s.runID, child.label, child.id), nil)
	}
	return writer
}

// newRuntimeFanoutWriter builds the fanout writer for child under supervisor s.
// The supervisor is a required parameter: a child-bearing writer always has
// the supervisor that reports its lifecycle diagnostics, so production
// construction goes through this constructor and reportLifecycleDiagnostics's
// nil-owner invariant panic stays unreachable for built writers.
func newRuntimeFanoutWriter(s *Supervisor, child *childRuntime, metadata *cli.EventMetadata, recorder *broker.Recorder) *runtimeFanoutWriter {
	return &runtimeFanoutWriter{
		base:            child.eventWriter,
		runtimeID:       child.id,
		child:           child,
		supervisor:      s,
		control:         s.control,
		metadata:        metadata,
		onPermissionReq: s.permissions.cachePermissionOptions,
		recorder:        recorder,
	}
}

type runtimeFanoutWriter struct {
	base      cli.EventSink
	runtimeID string
	child     *childRuntime
	// supervisor is the owner used for lifecycle transactions; it must
	// be nonnil whenever child is nonnil.
	supervisor      *Supervisor
	control         *control.ControlServer
	metadata        *cli.EventMetadata
	onPermissionReq func(runtimeID, requestID string, options []any)
	recorder        *broker.Recorder
	beforeWriteLock func()
}

func (w *runtimeFanoutWriter) Write(ev events.Event) error {
	if w.child != nil {
		if w.beforeWriteLock != nil {
			w.beforeWriteLock()
		}
		// The diagnostic slice lives in the outer scope and is reported after
		// the locked helper unlocks, including on a sink panic.
		diagnostics := make([]error, 0, 2)
		defer func() { reportLifecycleDiagnostics(w.supervisor, w.child, diagnostics) }()
		err := w.writeHoldingWriteLock(ev, &diagnostics)
		return err
	}
	// Nil-child writers emit no child lifecycle signals, but they keep a
	// collector so an unexpected diagnostic can never be dropped.
	diagnostics := make([]error, 0, 2)
	defer func() { reportLifecycleDiagnostics(w.supervisor, w.child, diagnostics) }()
	return w.writeLocked(ev, &diagnostics)
}

// writeHoldingWriteLock scopes the writeMu acquisition so it releases before
// the outer owner reports diagnostics and returns the original IO error.
func (w *runtimeFanoutWriter) writeHoldingWriteLock(ev events.Event, diagnostics *[]error) error {
	w.child.writeMu.Lock()
	defer w.child.writeMu.Unlock()
	return w.writeLocked(ev, diagnostics)
}

// reportLifecycleDiagnostics reports collected lifecycle errors outside all
// locks. It is idempotent per call site: callers report each slice once. A
// nil owner is an invariant violation, never a tolerated path: a
// child-bearing writer is built only through newRuntimeFanoutWriter, which
// requires the supervisor, and diagnostics must not be lost.
func reportLifecycleDiagnostics(s *Supervisor, child *childRuntime, diagnostics []error) {
	if s == nil {
		if child != nil || len(diagnostics) > 0 {
			id := ""
			if child != nil {
				id = child.id
			}
			panic(fmt.Sprintf("lifecycle: nil supervisor would drop %d lifecycle diagnostics for runtime %q", len(diagnostics), id))
		}
		return
	}
	for _, err := range diagnostics {
		if note, ok := err.(unrecognizedPhaseDiagnostic); ok {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: %v\n", note)
			continue
		}
		s.reportLifecycleError(child, err)
	}
}

// unrecognizedPhaseDiagnostic carries a fanout unrecognized-phase note out of
// writeLocked so it is logged after every lock is released. An empty phase
// marks the one-time suppression line.
type unrecognizedPhaseDiagnostic struct {
	runtimeID string
	phase     string
}

func (e unrecognizedPhaseDiagnostic) Error() string {
	if e.phase == "" {
		return fmt.Sprintf("runtime %s %s", e.runtimeID, diagnote.SuppressedNote)
	}
	return fmt.Sprintf("runtime %s reported unrecognized phase %q", e.runtimeID, e.phase)
}

// writeLocked is the canonical event path for callers that already hold the
// child write lock. In particular, a direct permission answer must mark the
// old claim, clear its status, and publish its response as one ordered write
// relative to a reused request ID. Lifecycle errors are appended to the
// diagnostics slice before the event proceeds; publication is never suppressed
// because of a lifecycle diagnostic.
func (w *runtimeFanoutWriter) writeLocked(ev events.Event, diagnostics *[]error) error {
	// stamped is the lossless durable event (written to file/broker);
	// presentation is the bounded copy for control/status surfaces.
	stamped := cli.StampFanoutEvent(ev, w.runtimeID, w.metadata, w.control)
	presentation := events.BoundFinalOutput(stamped)
	if presentation.Event == "permission.request" && w.onPermissionReq != nil {
		if requestID, _ := presentation.Fields["request_id"].(string); requestID != "" {
			if options, _ := presentation.Fields["options"].([]any); options != nil {
				w.onPermissionReq(w.runtimeID, requestID, options)
			}
		}
	}
	// Mirror bounded presentation state onto RuntimeStatus, retaining the
	// complete reply separately for RuntimeResult.
	if w.child != nil {
		w.child.mu.Lock()
		if seq, ok := events.Int64(presentation.Fields["seq"]); ok {
			w.child.latestSeq = seq
		}
		if usage, ok := presentation.Fields["usage"].(map[string]any); ok {
			w.child.usage = make(map[string]any, len(usage))
			for k, v := range usage {
				w.child.usage[k] = v
			}
		}
		switch presentation.Event {
		case "agent.status":
			if phase, _ := presentation.Fields["phase"].(string); phase != "" && w.child.phase != runstate.PhaseDone {
				// The adapter owns the agent-status presentation write; the caller's
				// done guard is the only dedup point. The label is only replaced
				// when nonempty.
				label, _ := presentation.Fields["label"].(string)
				signalPhase, recognized := runstate.ParsePhase(phase)
				if !recognized {
					signalPhase = runstate.PhaseUnknown
					if log, suppressed := w.child.unknownPhases.Admit(phase); log {
						*diagnostics = append(*diagnostics, unrecognizedPhaseDiagnostic{runtimeID: w.runtimeID, phase: phase})
					} else if suppressed {
						*diagnostics = append(*diagnostics, unrecognizedPhaseDiagnostic{runtimeID: w.runtimeID})
					}
				}
				_, _, statusErr := w.supervisor.applyLifecycle(w.child, lifecycleSignal{
					kind: signalAgentStatusObserved, class: w.child.lc.class,
					phase: signalPhase, label: label,
				})
				if statusErr != nil {
					*diagnostics = append(*diagnostics, statusErr)
				}
			}
		case "permission.request":
			w.child.pendingPermission = true
			w.child.permission = map[string]any{}
			for k, v := range presentation.Fields {
				if k == "runtime_id" || k == "run_id" || k == "run_label" || k == "ts" || k == "seq" {
					continue
				}
				w.child.permission[k] = v
			}
			// Stamp the working directory so permission resolvers know the
			// context the command runs in.
			if w.child.dir != "" {
				w.child.permission["cwd"] = w.child.dir
			}
			// If a command is available (from tool-call correlation or
			// passthrough), analyze whether target paths escape cwd.
			// Always set path_escapes_cwd explicitly so a passthrough
			// value from the backend payload cannot persist unchallenged.
			if cmd, _ := w.child.permission["command"].(string); cmd != "" && w.child.dir != "" {
				resolved, escapes := analyzeCommandPaths(cmd, w.child.dir)
				if len(resolved) > 0 {
					w.child.permission["resolved_paths"] = resolved
				}
				w.child.permission["path_escapes_cwd"] = escapes
			}
		case "permission.response":
			w.child.pendingPermission = false
			w.child.permission = nil
		case "session.end":
			stopReason, _ := presentation.Fields["stop_reason"].(string)
			// The adapter owns the session-end presentation (phase done and a
			// nonempty stop reason); the final-output data updates below are
			// imperative writes beside it.
			_, _, endErr := w.supervisor.applyLifecycle(w.child, lifecycleSignal{
				kind: signalSessionEndObserved, class: w.child.lc.class, stopReason: stopReason,
			})
			if endErr != nil {
				*diagnostics = append(*diagnostics, endErr)
			}
			if finalOutput, _ := presentation.Fields["final_output"].(string); finalOutput != "" {
				w.child.finalOutput = finalOutput
			}
			if fullFinalOutput, _ := stamped.Fields["final_output"].(string); fullFinalOutput != "" {
				w.child.fullFinalOutput = fullFinalOutput
				w.child.finalOutputTruncated = events.FinalOutputTruncated(fullFinalOutput)
			}
		}
		w.child.mu.Unlock()
	}
	rec := w.recorder
	if rec != nil {
		rec.Feed(presentation)
	}
	if w.base != nil {
		if err := w.base.Write(stamped); err != nil {
			return err
		}
	}
	if w.control != nil {
		w.control.PublishCanonicalEvent(stamped)
	}
	return nil
}

func (w *runtimeFanoutWriter) Close() error { return w.base.Close() }

func (w *runtimeFanoutWriter) withRecorder(r *broker.Recorder) *runtimeFanoutWriter {
	clone := *w
	clone.recorder = r
	return &clone
}

// Broker helpers.

func newRecorderFor(s *Supervisor, runID string) *broker.Recorder {
	if s.broker == nil {
		return nil
	}
	return broker.NewRecorder(s.broker, runID)
}

// resolveDirectSpawnIdentity applies workflow/roster/resume identity
// resolution to a spawn: it validates the workflow-mode selector, applies
// roster defaults for backend/agent/model/thinking, and resolves resume
// conflicts. The returned params carry the effective launch wrapper — the
// argv that will actually be exec'd — in LaunchWrapper.
func (s *Supervisor) resolveDirectSpawnIdentity(params SpawnParams) (SpawnParams, string, error) {
	workflowMode := spawnIsWorkflow(params)
	backend := params.Backend
	if backend == "" {
		backend = cli.DefaultBackend
	}
	if workflowMode {
		if params.RosterEntry != "" {
			return params, backend, fmt.Errorf("roster_entry is not valid with loop_file or team_file")
		}
		return params, backend, nil
	}
	directSupplied := effectiveIdentity{
		Backend: params.Backend, Agent: params.Agent, Model: params.Model,
		AgentProfile: params.AgentProfile, RosterFile: params.RosterFile, RosterEntry: params.RosterEntry,
		LaunchWrapper: params.LaunchWrapper,
	}
	if params.RosterFile != "" {
		roster, err := rosterconfig.Load(params.RosterFile)
		if err != nil {
			return params, backend, err
		}
		entry, err := roster.Lookup(params.RosterEntry)
		if err != nil {
			return params, backend, err
		}
		backend, params.Agent, params.Model = entry.Backend, entry.Agent, entry.Model
		// Roster supplies a default; an explicitly spawned thinking level
		// wins. ValidateThinkingForBackend below sees the merged value.
		if params.Thinking == "" {
			params.Thinking = entry.Thinking
		}
	}
	resolved, err := s.resolveDirectResumeIdentity(params.SessionID, directSupplied, effectiveIdentity{
		Backend: backend, Agent: params.Agent, Model: params.Model,
		AgentProfile: params.AgentProfile, RosterFile: params.RosterFile, RosterEntry: params.RosterEntry,
		LaunchWrapper: params.LaunchWrapper,
	})
	if err != nil {
		return params, backend, err
	}
	backend, params.Agent, params.Model, params.AgentProfile = resolved.Backend, resolved.Agent, resolved.Model, resolved.AgentProfile
	params.LaunchWrapper = resolved.LaunchWrapper
	params.RosterFile, params.RosterEntry = resolved.RosterFile, resolved.RosterEntry
	if err := runtime.ValidateThinkingForBackend(backend, params.Thinking); err != nil {
		return params, backend, err
	}
	return params, backend, nil
}

// spawnMCPAuthorizationDisabled is the exact error an MCP spawn receives when
// the supervisor has no launch wrapper policy configured.
var errMCPLaunchWrapperDisabled = errors.New("launch_wrapper: disabled for MCP on this supervisor")

// authorizeMCPLaunchWrapper enforces the supervisor's MCP launch wrapper
// policy for a spawn_mcp request. It resolves the effective wrapper argv —
// the final argv that will be exec'd, after roster and resume identity
// resolution — and requires an exact allowlist match. It must be called
// before reservations, idempotency, or any process creation.
func (s *Supervisor) authorizeMCPLaunchWrapper(params SpawnParams) error {
	if len(params.LaunchWrapper) > 0 {
		if err := runtime.ValidateLaunchWrapper(params.LaunchWrapper); err != nil {
			return fmt.Errorf("launch_wrapper: %w", err)
		}
	}
	// Resolve even when the request omits the wrapper: a resume inherits the
	// session's authoritative wrapper, and the inherited argv is the argv that
	// will be exec'd — it needs the same gate.
	resolved, _, err := s.resolveDirectSpawnIdentity(params)
	if err != nil {
		return err
	}
	if len(resolved.LaunchWrapper) == 0 {
		return nil
	}
	if s.config.MCPLaunchWrapperPolicy == nil {
		return errMCPLaunchWrapperDisabled
	}
	if !mcpLaunchWrapperAllowed(s.config.MCPLaunchWrapperPolicy, resolved.LaunchWrapper) {
		return errors.New("launch_wrapper: argv is not allowed by this supervisor")
	}
	return nil
}

// SpawnMCP implements control.StableHandler for the spawn_mcp verb: an
// MCP-driven spawn whose launch_wrapper is subject to the supervisor's
// operator allowlist. Everything else about the spawn is identical to Spawn.
func (s *Supervisor) SpawnMCP(raw json.RawMessage) (any, error) {
	p, err := parseStableSpawn(raw)
	if err != nil {
		return nil, err
	}
	s.applySpawnCallerParent(raw, &p)
	if err := s.authorizeMCPLaunchWrapper(p); err != nil {
		return nil, err
	}
	return s.finishSpawn(p)
}

// parseStableSpawn parses and sanitizes a control-socket spawn payload: it
// strips workflow identity (only the internal direct-run executor may set it)
// and auto-populates ParentID from a registered caller runtime.
func parseStableSpawn(raw json.RawMessage) (SpawnParams, error) {
	var p SpawnParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, fmt.Errorf("invalid spawn params: %w", err)
	}
	p.WorkflowID = ""
	p.NodeID = ""
	p.ActivationID = ""
	p.AttemptID = ""
	return p, nil
}

// applySpawnCallerParent auto-populates ParentID when the spawn is called by
// a known runtime: the caller embeds its own runtime_id in the spawn params;
// if it maps to a registered runtime, treat it as the parent.
func (s *Supervisor) applySpawnCallerParent(raw json.RawMessage, p *SpawnParams) {
	if p.ParentID != "" {
		return
	}
	var caller struct {
		RuntimeID string `json:"runtime_id"`
	}
	if err := json.Unmarshal(raw, &caller); err == nil && caller.RuntimeID != "" {
		s.controlMu.Lock()
		if _, ok := s.runtimes[caller.RuntimeID]; ok {
			p.ParentID = caller.RuntimeID
		}
		s.controlMu.Unlock()
	}
}

// finishSpawn applies idempotency and starts the runtime for a
// control-socket spawn (shared by the spawn and spawn_mcp verbs).
func (s *Supervisor) finishSpawn(p SpawnParams) (any, error) {
	if p.IdempotencyKey != "" {
		hash, err := IdempotencyHash(p)
		if err != nil {
			return nil, fmt.Errorf("idempotency hash: %w", err)
		}
		return s.idempotentSpawn(p, hash)
	}
	return s.spawn(p)
}

// StableHandler implementation.

func (s *Supervisor) Spawn(raw json.RawMessage) (any, error) {
	p, err := parseStableSpawn(raw)
	if err != nil {
		return nil, err
	}
	s.applySpawnCallerParent(raw, &p)
	return s.finishSpawn(p)
}
func (s *Supervisor) List() any {
	return s.listRuntimes()
}

func (s *Supervisor) Shutdown(mode string) error {
	if mode != "graceful" && mode != "kill" {
		return fmt.Errorf("shutdown mode must be graceful or kill")
	}
	s.shutdown(mode)
	s.shutdownChOnce.Do(func() { close(s.shutdownCh) })
	return nil
}

func (s *Supervisor) RuntimeStatus(rtID string) (any, error) {
	s.controlMu.Lock()
	rt := s.runtimes[rtID]
	s.controlMu.Unlock()
	if rt == nil {
		return nil, fmt.Errorf("runtime %q not found", rtID)
	}
	rt.mu.Lock()
	status := rt.lc.runtimeStatus()
	identity := s.statusIdentity(rt, rt.session.SessionID)
	entry := map[string]any{
		"runtime_id":         rt.id,
		"session_id":         rt.session.SessionID,
		"run_id":             rt.runID,
		"label":              rt.label,
		"agent":              identity.Agent,
		"agent_profile":      identity.AgentProfile,
		"model":              identity.Model,
		"thinking":           rt.thinking,
		"backend":            identity.Backend,
		"roster_file":        identity.RosterFile,
		"roster_entry":       identity.RosterEntry,
		"effective_agent":    identity.Agent,
		"effective_model":    identity.Model,
		"effective_backend":  identity.Backend,
		"dir":                rt.dir,
		"status":             string(status),
		"started_at":         rt.startedAt,
		"exit_code":          rt.exitCode,
		"on_event":           rt.onEvent,
		"event_path":         rt.onEvent,
		"sentinel_file":      rt.sentinelFile,
		"parent_id":          rt.parentID,
		"children":           rt.children,
		"pid":                rt.session.PID,
		"phase":              string(rt.phase),
		"phase_label":        rt.phaseLabel,
		"pending_permission": rt.pendingPermission,
		"latest_seq":         rt.latestSeq,
		"auto_approve":       rt.autoApprove,
		// True once a pending request exhausted its decider retries (harness
		// spawns or direct-model calls); it stays pending for a human gate.
		"permission_decider_exhausted": rt.permissionDeciderExhausted,
	}
	if rt.permissionDecider != nil {
		entry["permission_decider"] = rt.permissionDecider.Entry
	}
	if rt.modelDecider != nil {
		entry["permission_model_decider"] = rt.modelDecider.Provider + "/" + rt.modelDecider.Model
	}
	if rt.permission != nil {
		perm := make(map[string]any, len(rt.permission))
		for k, v := range rt.permission {
			perm[k] = v
		}
		entry["permission"] = perm
	}
	if rt.usage != nil {
		usage := make(map[string]any, len(rt.usage))
		for k, v := range rt.usage {
			usage[k] = v
		}
		entry["usage"] = usage
	}
	if rt.finalOutput != "" {
		entry["final_output"] = rt.finalOutput
	}
	if rt.finalOutputTruncated {
		entry["final_output_truncated"] = true
	}
	if rt.workflowID != "" {
		entry["workflow_id"] = rt.workflowID
		entry["node_id"] = rt.nodeID
		entry["activation_id"] = rt.activationID
		entry["attempt_id"] = rt.attemptID
	}
	rt.mu.Unlock()
	return entry, nil
}

// RuntimeResult exposes the complete terminal reply without widening the
// bounded RuntimeStatus presentation surface.
func (s *Supervisor) RuntimeResult(rtID string) (any, error) {
	s.controlMu.Lock()
	rt := s.runtimes[rtID]
	s.controlMu.Unlock()
	if rt == nil {
		return nil, fmt.Errorf("runtime %q not found", rtID)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return map[string]any{"final_output": rt.fullFinalOutput}, nil
}

// RuntimeWaitTurn blocks until the runtime's next running→idle transition
// settles: an in-flight turn is waited out, and a turn prompted while the
// runtime is idle is covered too — a pre-existing idle state never returns
// on its own. The result mirrors the supervisor's own runtime state (the
// durable fullFinalOutput, the session.end stop_reason, and the session ID)
// rather than any event-delivery stream. A non-positive timeout waits
// indefinitely; control.ErrWaitTurnTimeout is returned when the timeout
// elapses first. Cancelling ctx abandons the wait, returning ctx.Err().
func (s *Supervisor) RuntimeWaitTurn(ctx context.Context, rtID string, timeout time.Duration) (any, error) {
	s.controlMu.Lock()
	child := s.runtimes[rtID]
	s.controlMu.Unlock()
	if child == nil {
		return nil, fmt.Errorf("runtime %q not found", rtID)
	}

	child.mu.Lock()
	if child.lc.isCompleted() {
		// An ended runtime has no further turn to observe.
		result := s.waitTurnResultLocked(child)
		child.mu.Unlock()
		return result, nil
	}
	// Register before deciding what to wait for: a settle cannot slip between
	// the state snapshot and the wait.
	wait := make(chan struct{})
	child.turnWaiters = append(child.turnWaiters, wait)
	child.mu.Unlock()

	var timerC <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timerC = timer.C
	}
	select {
	case <-wait:
	case <-timerC:
		// The settle and the timer can become readable together; select picks
		// randomly between them, so prefer the settled result when it is
		// already available.
		select {
		case <-wait:
		default:
			child.mu.Lock()
			child.removeTurnWaiterLocked(wait)
			child.mu.Unlock()
			return nil, control.ErrWaitTurnTimeout
		}
	case <-ctx.Done():
		// The settle and the cancellation can become readable together; select
		// picks randomly between them, so prefer the settled result when it is
		// already available.
		select {
		case <-wait:
		default:
			child.mu.Lock()
			child.removeTurnWaiterLocked(wait)
			child.mu.Unlock()
			return nil, ctx.Err()
		}
	case <-child.done:
		// The runtime ended without a turn settling since registration (e.g.
		// an aborted start attempt); report the terminal snapshot.
	}
	child.mu.Lock()
	defer child.mu.Unlock()
	return s.waitTurnResultLocked(child), nil
}

// waitTurnResultLocked snapshots the turn result from the supervisor's own
// runtime state. Callers must hold child.mu.
func (s *Supervisor) waitTurnResultLocked(child *childRuntime) map[string]any {
	return map[string]any{
		"runtime_id":   child.id,
		"session_id":   child.session.SessionID,
		"stop_reason":  child.lastStopReason,
		"final_output": child.fullFinalOutput,
	}
}

func (s *Supervisor) RuntimeCancel(rtID string) error {
	return s.cancelRuntime(rtID)
}

func (s *Supervisor) RuntimePrompt(rtID, text, requestID string) error {
	if requestID != "" {
		s.controlMu.Lock()
		if handled, ok := s.handledQuestions[rtID]; ok && handled.requestID == requestID {
			s.controlMu.Unlock()
			return nil
		}
		s.handledQuestions[rtID] = handledChildQuestion{requestID: requestID, at: time.Now()}
		s.controlMu.Unlock()
	}

	s.clearPendingChildQuestion(rtID, requestID)

	s.controlMu.Lock()
	rt := s.runtimes[rtID]
	s.controlMu.Unlock()
	if rt == nil {
		return fmt.Errorf("runtime %q not found", rtID)
	}
	rt.mu.Lock()
	if rt.lc.isCompleted() {
		rt.mu.Unlock()
		return fmt.Errorf("runtime %q has ended", rtID)
	}
	rt.promptQueue = append(rt.promptQueue, text)
	rt.mu.Unlock()
	rt.signalPrompt()
	return nil
}

func (s *Supervisor) clearPendingChildQuestion(childID, requestID string) {
	s.controlMu.Lock()
	pq, ok := s.pendingQuestions[childID]
	if ok && requestID != "" && pq.requestID != requestID {
		s.controlMu.Unlock()
		return
	}
	if ok {
		delete(s.pendingQuestions, childID)
	}
	s.controlMu.Unlock()
	if ok && pq.timer != nil {
		pq.timer.Stop()
	}
}

func (s *Supervisor) RuntimeAnswerPermission(rtID, requestID, optionID, message string) error {
	return s.permissions.answerPermission(rtID, requestID, optionID, message)
}

func (s *Supervisor) RuntimeInterruptAndPrompt(rtID, text string, keepQueue bool) error {
	s.controlMu.Lock()
	rt := s.runtimes[rtID]
	s.controlMu.Unlock()
	if rt == nil {
		return fmt.Errorf("runtime %q not found", rtID)
	}
	rt.mu.Lock()
	if rt.lc.isCompleted() {
		rt.mu.Unlock()
		return fmt.Errorf("runtime %q has ended", rtID)
	}
	if !keepQueue {
		rt.promptQueue = nil
	}
	// Prepend the interrupt prompt to the front of the queue so it runs next.
	rt.promptQueue = append([]string{text}, rt.promptQueue...)
	interruptFn := rt.interruptFn
	rt.mu.Unlock()
	rt.signalPrompt()
	if interruptFn != nil {
		interruptFn()
	}
	return nil
}

func (s *Supervisor) RuntimeSendToParent(rtID, message string) error {
	s.controlMu.Lock()
	child := s.runtimes[rtID]
	s.controlMu.Unlock()
	if child == nil {
		return fmt.Errorf("runtime %q not found", rtID)
	}
	child.mu.Lock()
	parentID := child.parentID
	childSessionID := child.session.SessionID
	child.mu.Unlock()
	if parentID == "" {
		return fmt.Errorf("runtime %q has no parent", rtID)
	}
	s.controlMu.Lock()
	parent := s.runtimes[parentID]
	s.controlMu.Unlock()
	if parent == nil {
		return fmt.Errorf("parent runtime %q not found", parentID)
	}
	parent.mu.Lock()
	parentSessionID := parent.session.SessionID
	parent.mu.Unlock()
	s.controlMu.Lock()
	s.childQuestionSeq++
	requestID := fmt.Sprintf("cq_%d", s.childQuestionSeq)
	if existing, ok := s.pendingQuestions[rtID]; ok {
		if existing.timer != nil {
			existing.timer.Stop()
		}
		delete(s.pendingQuestions, rtID)
	}
	timer := time.AfterFunc(s.childQuestionTimeout, func() {
		s.controlMu.Lock()
		pq, ok := s.pendingQuestions[rtID]
		if !ok || pq.requestID != requestID {
			s.controlMu.Unlock()
			return
		}
		delete(s.pendingQuestions, rtID)
		s.controlMu.Unlock()
		_ = s.RuntimePrompt(rtID, fmt.Sprintf("No parent response received within %s. Continue with your best judgment and state assumptions.", s.childQuestionTimeout), requestID)
	})
	s.pendingQuestions[rtID] = pendingChildQuestion{requestID: requestID, timer: timer}
	s.controlMu.Unlock()
	s.control.PublishEvent(events.Event{
		Event:     runtime.EventChildQuestion,
		SessionID: parentSessionID,
		Fields: func() map[string]any {
			fields := runtime.ChildQuestionPayload{
				RuntimeID: parentID,
				SessionID: childSessionID,
				ChildID:   rtID,
				Message:   message,
				RequestID: requestID,
			}.Fields()
			fields["parent_id"] = parentID
			fields["ts"] = time.Now().UnixMilli()
			return fields
		}(),
	})
	return nil
}

func backoffDelay(attempt int) time.Duration {
	seconds := 2 << uint(attempt-1)
	if seconds > 30 {
		seconds = 30
	}
	return time.Duration(seconds) * time.Second
}

// retryWaitTimer is the supervisor's retry wait. Tests substitute it to observe
// the delay a provider's hint produces.
var retryWaitTimer = time.After

// StableAdapter implementation — used by HTTPDebugServer for per-runtime HTTP endpoints.

// HTTPRuntimeStatus implements control.StableAdapter.  Returns the runtime
// snapshot when runtimeID is known.  Returns control.ErrRuntimeNotFound when
// the ID does not exist.
func (s *Supervisor) HTTPRuntimeStatus(runtimeID string) (any, error) {
	s.controlMu.Lock()
	rt := s.runtimes[runtimeID]
	s.controlMu.Unlock()
	if rt == nil {
		return nil, control.ErrRuntimeNotFound
	}
	snap, err := s.RuntimeStatus(runtimeID)
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// HTTPCancelRuntime implements control.StableAdapter.  Returns
// control.ErrRuntimeNotFound when the ID does not exist.
func (s *Supervisor) HTTPCancelRuntime(runtimeID string) error {
	s.controlMu.Lock()
	rt := s.runtimes[runtimeID]
	s.controlMu.Unlock()
	if rt == nil {
		return control.ErrRuntimeNotFound
	}
	return s.cancelRuntime(runtimeID)
}

// analyzeCommandPaths performs a best-effort analysis of a command string to
// identify path-like arguments and determine whether any resolved path
// escapes the working directory. This is used to enrich permission.request
// metadata so resolvers can make informed safety decisions.
//
// The analysis is intentionally conservative: it only flags potential escapes
// and does not attempt to fully parse shell syntax. False positives (flagging
// a safe path) are acceptable; false negatives (missing a dangerous path) are
// not. Does not resolve symlinks; a symlink inside cwd pointing outside will
// not be flagged as an escape.
func analyzeCommandPaths(command, cwd string) (resolved []string, escapes bool) {
	cwdAbs, err := filepath.Abs(cwd)
	if err != nil {
		return nil, false
	}

	tokens := strings.Fields(command)
	for _, tok := range tokens {
		// Skip flags and options.
		if strings.HasPrefix(tok, "-") {
			continue
		}
		// Skip obvious command names (first non-flag token is usually the
		// executable).
		if len(resolved) == 0 && len(tokens) > 0 && tok == tokens[0] {
			// Only skip if it looks like a bare command name, not a path.
			if !strings.Contains(tok, "/") {
				continue
			}
		}
		// Check if the token looks like a path (contains / or starts with .
		// or ~).
		if !strings.Contains(tok, "/") && !strings.HasPrefix(tok, "~") && !strings.HasPrefix(tok, ".") {
			continue
		}

		var resolvedPath string
		if filepath.IsAbs(tok) {
			resolvedPath = filepath.Clean(tok)
		} else {
			resolvedPath = filepath.Clean(filepath.Join(cwdAbs, tok))
		}
		resolved = append(resolved, resolvedPath)

		// Check if the resolved path escapes cwd.
		if !strings.HasPrefix(resolvedPath+"/", cwdAbs+"/") && resolvedPath != cwdAbs {
			escapes = true
		}
	}
	return resolved, escapes
}

// resolveWorkflowRoot returns the configured workflow root, or the default
// under XDG_STATE_HOME (falling back to $HOME/.avenor) when unset. It never
// creates directories; the manager creates them on first workflow use.
func resolveWorkflowRoot(configured string) string {
	if configured != "" {
		return configured
	}
	if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" && filepath.IsAbs(xdg) {
		return filepath.Join(xdg, "avenor", "workflows")
	}
	home := os.Getenv("HOME")
	if home == "" {
		if u, err := os.UserHomeDir(); err == nil && u != "" {
			home = u
		}
	}
	if home == "" {
		return "avenor/workflows"
	}
	return filepath.Join(home, ".avenor", "workflows")
}

// resolveWorkflowAdapterDir returns the configured adapter manifest directory,
// or the default under the shared avenor config home (XDG_CONFIG_HOME/avenor,
// falling back to ~/.config/avenor) when unset. It never creates directories.
func resolveWorkflowAdapterDir(configured string) string {
	if configured != "" {
		return configured
	}
	return filepath.Join(appdir.ConfigHome(), "workflow-adapters")
}

// loadWorkflowAdapters loads the adapter manifest registry from the
// configured (or default) adapter directory into the supervisor's immutable
// registry slot. A directory-level load failure leaves the registry nil —
// every adapter reports unavailable — and is logged. Per-manifest failures
// (invalid, untrusted, or duplicate IDs) are logged and skip only their own
// file; manifests are host configuration and a broken one must not disable
// the other adapters.
func (s *Supervisor) loadWorkflowAdapters() {
	dir := resolveWorkflowAdapterDir(s.config.WorkflowAdapterDir)
	reg, err := workflowcontroller.LoadAdapterRegistry(dir)
	if err != nil {
		log.Printf("workflow: adapter registry load from %s failed (adapters unavailable): %v", dir, err)
		s.workflowAdapters.Store(nil)
		return
	}
	for _, loadErr := range reg.Errors() {
		log.Printf("workflow: adapter manifest %s not loaded: %v", loadErr.File, loadErr.Err)
	}
	s.workflowAdapters.Store(reg)
}

// workflowManager returns the workflow manager for the configured workflow
// root, running the exactly-once startup barrier on first use. Safe for
// concurrent use; every caller waits on the same barrier and shares its
// retained results.
func (s *Supervisor) workflowManager() *workflow.Manager {
	s.workflowOnce.Do(s.runWorkflowStartupBarrier)
	return s.workflowMgr
}

// hasExistingWorkflowState reports whether durable workflow or controller
// state already exists under root. It is a pure stat check and never creates
// anything.
func hasExistingWorkflowState(root string) bool {
	for _, dir := range []string{
		filepath.Join(root, "instances"),
		filepath.Join(root, "controllers"),
	} {
		if _, err := os.Stat(dir); err == nil {
			return true
		}
	}
	return false
}

// runWorkflowStartupBarrier is the exactly-once startup barrier. It constructs
// and registers the workflow manager, recovers the workflow catalog, rebuilds
// the candidate index, and recovers controller records, starting a leader loop
// for every recovered controller whose desired state is enabled. A recovery
// failure is retained as the barrier error: workflow RPCs keep serving, the
// candidate index stays not-recovered, and every controller method fails until
// the process restarts.
func (s *Supervisor) runWorkflowStartupBarrier() {
	root := resolveWorkflowRoot(s.config.WorkflowRoot)
	// Orphaned adapter staging files predate every live state; sweep them
	// before recovery hands leadership back out.
	sweepOrphanedAdapterFiles(root)
	var m *workflow.Manager
	if s.testHooks.workflowNow != nil {
		m = workflow.NewManagerWithClock(workflow.NewStoreWithClock(root, s.testHooks.workflowNow), s.testHooks.workflowNow)
	} else {
		m = workflow.NewManager(workflow.New(root))
	}
	m.RegisterExecutor(workflow.ActionRun, s.directRunExecutor())
	m.RegisterExecutor(workflow.ActionLoop, s.loopExecutor())
	m.RegisterExecutor(workflow.ActionTeam, s.teamExecutor())
	s.workflowMgr = m
	// Keep the lazy wrapper registered, not the bare manager: only the wrapper
	// routes workflow.cancel through cancelWorkflowRuntimes to terminate
	// in-flight attempt runtimes owned by this supervisor.
	s.control.SetWorkflowHandler(lazyWorkflowHandler{s})
	s.workflowControllers = workflowcontroller.NewStore(root)

	var barrierErr error
	// Catalog recovery is skipped when the workflow root does not exist: the
	// catalog is empty, and construction must stay side-effect free (Catalog
	// creates the root). The candidate index is still marked recovered-empty
	// so workflow.ready serves immediately instead of reporting a permanent
	// not-recovered error. ResumeAwaitingChildren performs the catalog
	// recovery internally, so it sequences before RebuildCandidateIndex.
	if _, err := os.Stat(root); err == nil {
		summary, err := m.ResumeAwaitingChildren()
		if err != nil {
			barrierErr = fmt.Errorf("workflow catalog recovery: %w", err)
		} else {
			log.Printf("workflow: startup composition resume: resolved=%d still_awaiting=%d errors=%d", summary.Resolved, summary.StillAwaiting, len(summary.Errors))
			if err := m.RebuildCandidateIndex(s.supervisorIdentity()); err != nil {
				barrierErr = fmt.Errorf("workflow candidate index rebuild: %w", err)
			}
		}
	} else {
		m.MarkCandidateIndexEmpty(s.supervisorIdentity())
	}
	if barrierErr == nil {
		recovered, err := s.workflowControllers.Recover()
		if err != nil {
			barrierErr = fmt.Errorf("workflow controller recovery: %w", err)
		} else {
			for _, failure := range recovered.Failures {
				log.Printf("workflow: controller recovery skipped: %v", failure)
			}
			for _, rec := range recovered.Records {
				if rec.DesiredState == workflowcontroller.DesiredEnabled {
					s.startControllerLoop(s.workflowControllers, rec.ControllerID)
				}
			}
		}
	}
	// Recovered enabled controllers poll adapters; load the registry here so
	// an enabled startup never runs without it.
	s.loadWorkflowAdapters()
	if barrierErr == nil && s.config.WorkflowLeaseSweepInterval > 0 {
		// The loop reads workflowMgr directly: it is assigned above and only
		// starts after the barrier has fully succeeded, so re-entering the
		// barrier's sync.Once is neither needed nor safe.
		s.startLeaseSweepLoop()
	}
	if barrierErr != nil {
		s.workflowBarrierErr = barrierErr
		log.Printf("workflow: startup barrier failed (workflow RPCs remain available; controllers disabled for this process): %v", barrierErr)
	}
}

// workflowBarrierResult returns the retained barrier outcome. It always
// includes the manager (workflow RPCs serve regardless of barrier errors) and
// reports the barrier error so callers can refuse controller activity.
func (s *Supervisor) workflowBarrierResult() (*workflow.Manager, *workflowcontroller.ControllerStore, error) {
	s.workflowOnce.Do(s.runWorkflowStartupBarrier)
	return s.workflowMgr, s.workflowControllers, s.workflowBarrierErr
}

// supervisorIdentity is this supervisor's stable owner identity, combining the
// run ID with the current PID. It stamps rebuilt candidates and identifies the
// leader-loop lease owner.
func (s *Supervisor) supervisorIdentity() string {
	return fmt.Sprintf("%s:%d", s.runID, os.Getpid())
}

// controllerLoop tracks one controller runner for a single controller in
// this process. The runner is constructed before the handle is published, so
// stop always reaches a live runner; the handle's goroutine only waits for
// the runner to exit.
type controllerLoop struct {
	done   chan struct{}
	runner *workflowcontroller.Runner
}

// stop stops the loop's runner and waits for it to release its lease and
// finish. Safe to call concurrently and on an already-stopped runner.
func (l *controllerLoop) stop() {
	l.runner.Stop()
}

// runnerStatus returns the runner's current status.
func (l *controllerLoop) runnerStatus() workflowcontroller.RunnerStatus {
	return l.runner.Status()
}

// startControllerLoop starts the process's single leader loop for controllerID
// if one is not already running. A map entry whose loop has already exited is
// replaced. Call only after a successful barrier.
func (s *Supervisor) startControllerLoop(store *workflowcontroller.ControllerStore, controllerID string) {
	s.controllerLoopsMu.Lock()
	defer s.controllerLoopsMu.Unlock()
	if s.loopsStopped {
		return
	}
	if existing, ok := s.controllerLoops[controllerID]; ok {
		select {
		case <-existing.done:
			// Stale entry from a loop that already exited on its own; the
			// exited loop's own cleanup races with us, so replace it here.
		default:
			return
		}
	}
	// The runner is constructed before the handle is published so a
	// concurrent stop always reaches a live runner. Its goroutine starts
	// before the map insert; a disable that misses the entry persists the
	// disabled state first, so the runner self-exits on its next
	// acquisition attempt. workflowMgr is read directly because both call
	// sites (the startup barrier and enable) run after the barrier has
	// assigned it; workflowManager here would re-enter the barrier's
	// sync.Once and deadlock.
	changeCh, cancelChange := s.workflowMgr.SubscribeChanges()
	capacityCh, cancelCapacity := s.SubscribeCapacityChanges()
	deps := &stableRunnerDeps{s: s, controllerID: controllerID}
	runner := workflowcontroller.NewRunner(workflowcontroller.RunnerConfig{
		Deps:          deps,
		Store:         store,
		ControllerID:  controllerID,
		OwnerID:       s.supervisorIdentity(),
		RenewInterval: s.controllerRenewInterval,
		ChangeCh:      changeCh,
		CapacityCh:    capacityCh,
		Poll:          &stablePoller{s: s, controllerID: controllerID},
		PollBaseDelay: s.controllerPollBaseDelay,
	})
	loop := &controllerLoop{done: make(chan struct{}), runner: runner}
	s.controllerLoops[controllerID] = loop
	go s.runControllerLoop(controllerID, loop, cancelChange, cancelCapacity)
}

// stopControllerLoop signals the controller's leader loop to exit and waits
// for it to release its lease and finish.
func (s *Supervisor) stopControllerLoop(controllerID string) {
	s.controllerLoopsMu.Lock()
	loop := s.controllerLoops[controllerID]
	delete(s.controllerLoops, controllerID)
	s.controllerLoopsMu.Unlock()
	if loop == nil {
		return
	}
	loop.stop()
	<-loop.done
}

// stopControllerLoops signals every leader loop to exit and waits for them to
// release their leases and finish. It is idempotent.
func (s *Supervisor) stopControllerLoops() {
	s.controllerLoopsMu.Lock()
	loops := s.controllerLoops
	s.controllerLoops = map[string]*controllerLoop{}
	s.loopsStopped = true
	s.controllerLoopsMu.Unlock()
	for _, loop := range loops {
		loop.stop()
	}
	for _, loop := range loops {
		<-loop.done
	}
}

// runControllerLoop waits for the controller's runner to exit (desired
// disabled, stop request, or a lease loss it declines to retry) and performs
// the loop's teardown. The runner releases a still-held lease on exit.
func (s *Supervisor) runControllerLoop(controllerID string, loop *controllerLoop, cancelChange, cancelCapacity func()) {
	// Self-exit cleanup: remove this loop's map entry, but only while the
	// entry still refers to this loop — a concurrent disable (or shutdown)
	// may have already deleted it, or an enable may have replaced it with a
	// fresh loop. Registration order runs this removal before close(done),
	// so a waiter in stopControllerLoop never observes a deleted-but-live
	// window.
	defer func() {
		s.controllerLoopsMu.Lock()
		if s.controllerLoops[controllerID] == loop {
			delete(s.controllerLoops, controllerID)
		}
		s.controllerLoopsMu.Unlock()
	}()
	defer close(loop.done)
	defer cancelCapacity()
	defer cancelChange()

	<-loop.runner.Done()
}

var _ control.WorkflowControllerHandler = (*Supervisor)(nil)

// controllerBarrierStore returns the barrier-recovered controller store, or
// the retained barrier error when recovery failed.
func (s *Supervisor) controllerBarrierStore() (*workflowcontroller.ControllerStore, error) {
	_, store, err := s.workflowBarrierResult()
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Supervisor) WorkflowControllerCreate(raw json.RawMessage) (any, error) {
	var p struct {
		ControllerID string `json:"controller_id"`
		MaxInflight  int    `json:"max_inflight"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("workflow.controller.create params: %w", err)
	}
	if p.ControllerID == "" {
		return nil, errors.New("workflow.controller.create: controller_id is required")
	}
	if p.MaxInflight <= 0 {
		return nil, errors.New("workflow.controller.create: max_inflight must be positive")
	}
	store, err := s.controllerBarrierStore()
	if err != nil {
		return nil, err
	}
	rec, err := store.Create(p.ControllerID, p.MaxInflight)
	if err != nil {
		return nil, err
	}
	return rec, nil
}

func (s *Supervisor) WorkflowControllerEnable(id string) (any, error) {
	store, err := s.controllerBarrierStore()
	if err != nil {
		return nil, err
	}
	// The adapter registry is loaded at enable and at startup and stays
	// immutable while the controller is enabled.
	s.loadWorkflowAdapters()
	// Serialize against disable (and other enables) so the final in-process
	// loop state always matches the last persisted desired state.
	s.workflowControllerMu.Lock()
	defer s.workflowControllerMu.Unlock()
	rec, err := store.SetDesiredState(id, workflowcontroller.DesiredEnabled, "")
	if err != nil {
		return nil, err
	}
	s.startControllerLoop(store, id)
	return rec, nil
}

func (s *Supervisor) WorkflowControllerDisable(id string, raw json.RawMessage) (any, error) {
	var p struct {
		Reason string `json:"reason"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("workflow.controller.disable params: %w", err)
		}
	}
	if p.Reason == "" {
		return nil, errors.New("workflow.controller.disable: reason is required")
	}
	store, err := s.controllerBarrierStore()
	if err != nil {
		return nil, err
	}
	// Serialize against enable (and other disables): persist desired=disabled
	// (which durably releases any live lease) before stopping this process's
	// loop, so the final loop state matches the last persisted desired state.
	// Workflow nodes are never touched.
	s.workflowControllerMu.Lock()
	defer s.workflowControllerMu.Unlock()
	rec, err := store.SetDesiredState(id, workflowcontroller.DesiredDisabled, p.Reason)
	if err != nil {
		return nil, err
	}
	s.stopControllerLoop(id)
	return rec, nil
}

func (s *Supervisor) WorkflowControllerStatus(id string) (any, error) {
	store, err := s.controllerBarrierStore()
	if err != nil {
		return nil, err
	}
	rec, ok, err := store.Get(id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("controller %s: %w", id, workflowcontroller.ErrNotFound)
	}
	if h := s.testHooks.controllerStatusPreNextPoll; h != nil {
		h()
	}
	var nextPollAt any
	var nextPollError string
	if earliest, has, err := store.NextPollTime(id); err == nil && has {
		nextPollAt = earliest
	} else if err != nil {
		// A failed poll-time read is not a failed controller: report the
		// record with a nil next_poll_at and the read error alongside it.
		nextPollError = err.Error()
	}
	status := controllerStatusMap(rec, s.supervisorIdentity(), s.controllerRunnerStatus(id, rec), nextPollAt)
	if nextPollError != "" {
		status["next_poll_error"] = nextPollError
	}
	return status, nil
}

// controllerRunnerStatus returns this process's live runner status for the
// controller when the record's leader is owned by this process; otherwise it
// returns nil.
func (s *Supervisor) controllerRunnerStatus(id string, rec workflowcontroller.ControllerRecord) *workflowcontroller.RunnerStatus {
	if rec.Leader == nil || rec.Leader.OwnerID != s.supervisorIdentity() {
		return nil
	}
	s.controllerLoopsMu.Lock()
	loop := s.controllerLoops[id]
	s.controllerLoopsMu.Unlock()
	if loop == nil {
		return nil
	}
	status := loop.runnerStatus()
	return &status
}

func (s *Supervisor) WorkflowControllerList() (any, error) {
	store, err := s.controllerBarrierStore()
	if err != nil {
		return nil, err
	}
	listed, err := store.List()
	if err != nil {
		return nil, err
	}
	controllers := make([]map[string]any, 0, len(listed.Records))
	for _, rec := range listed.Records {
		controllers = append(controllers, map[string]any{
			"controller_id": rec.ControllerID,
			"desired_state": string(rec.DesiredState),
			"leader":        controllerLeaderMap(rec, s.supervisorIdentity()),
		})
	}
	result := map[string]any{"controllers": controllers}
	if len(listed.Failures) > 0 {
		failures := make([]string, 0, len(listed.Failures))
		for _, failure := range listed.Failures {
			failures = append(failures, failure.Error())
		}
		result["load_failures"] = failures
	}
	return result, nil
}

func (s *Supervisor) WorkflowReady(id string, limit int) (any, error) {
	mgr, _, err := s.workflowBarrierResult()
	if err != nil {
		return nil, err
	}
	candidates, err := mgr.CandidatesForController(id, limit)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"advisory":      true,
		"controller_id": id,
		"candidates":    candidates,
	}, nil
}

// controllerLeaderMap renders a record's lease for RPC results, or nil when
// the controller holds no lease.
func controllerLeaderMap(rec workflowcontroller.ControllerRecord, ownIdentity string) any {
	if rec.Leader == nil {
		return nil
	}
	return map[string]any{
		"lease_id":        rec.Leader.LeaseID,
		"owner_id":        rec.Leader.OwnerID,
		"owner_epoch":     rec.Leader.OwnerEpoch,
		"acquired_at":     rec.Leader.AcquiredAt,
		"renewed_at":      rec.Leader.RenewedAt,
		"expires_at":      rec.Leader.ExpiresAt,
		"is_this_process": rec.Leader.OwnerID == ownIdentity,
	}
}

// controllerStatusMap renders the controller status RPC result. When the
// record's leader is owned by this process and this process has a live
// runner for the controller, the reconciliation fields come from the
// runner's status; otherwise they stay nil/0.
func controllerStatusMap(rec workflowcontroller.ControllerRecord, ownIdentity string, runner *workflowcontroller.RunnerStatus, nextPollAt any) map[string]any {
	var lastReconcile any
	inflight := 0
	var capacityBlocked any
	if runner != nil {
		if !runner.LastReconcile.IsZero() {
			lastReconcile = runner.LastReconcile
		}
		inflight = runner.Inflight
		if runner.CapacityBlocked != "" {
			capacityBlocked = map[string]any{
				"source": runner.CapacityBlocked,
				"detail": runner.CapacityDetail,
			}
		}
	}
	return map[string]any{
		"controller_id":    rec.ControllerID,
		"desired_state":    string(rec.DesiredState),
		"max_inflight":     rec.MaxInflight,
		"revision":         rec.Revision,
		"owner_epoch":      rec.OwnerEpoch,
		"leader":           controllerLeaderMap(rec, ownIdentity),
		"last_reconcile":   lastReconcile,
		"inflight":         inflight,
		"capacity_blocked": capacityBlocked,
		"next_poll_at":     nextPollAt,
	}
}

// lazyWorkflowHandler forwards workflow.* methods to the supervisor's
// lazily-constructed workflow manager so the manager is only built when a
// workflow command is first issued.
type lazyWorkflowHandler struct{ s *Supervisor }

// directRunExecutor returns the Executor that dispatches a workflow run action
// to a real stable direct run, threading workflow identity through the run and
// recording attempt termination on every terminal path.
func (s *Supervisor) directRunExecutor() workflow.Executor {
	return &directRunExecutor{sup: s}
}

type directRunExecutor struct{ sup *Supervisor }

// Replacement path: when a lease expires (live detector or restart
// recovery), the activation is durably left lease_expired with no lease and
// is re-claimable; a replacement claim + start through the manager
// dispatches a new attempt to the same executor, and the kernel appends it
// to the SAME activation (prior attempts preserved). Executors are
// attempt-agnostic, so the replacement attempt is dispatchable with no
// executor change.
//
// Lease liveness: every executor starts one heartbeat goroutine per attempt
// just before spawning its runtime (see startLeaseHeartbeat). The heartbeat
// renews the attempt's lease every TTL/3 via
// workflowManager().Heartbeat(workflowID, nodeID, activationID, leaseID,
// ec.OwnerToken) and stops when the runtime reaches a terminal state (via the
// workflowTerminator callback the runtime carries from construction), when
// the spawn fails, when the lease is no longer held, or on supervisor
// shutdown — it never outlives the runtime.

// attemptSpawnDir resolves the spawn directory for one workflow attempt:
// the dispatch-resolved working directory when the template declares one,
// else the supervisor process's working directory (the pre-existing
// behavior for templates without a working_directory declaration). A
// declared directory must exist at dispatch time — the resolved value comes
// from immutable instance params, but the directory itself may be created
// after instantiation.
func attemptSpawnDir(ec workflow.ExecutorContext) (string, error) {
	if ec.WorkingDirectory == "" {
		if cwd, err := os.Getwd(); err == nil {
			return cwd, nil
		}
		return ".", nil
	}
	if err := workflow.CheckWorkingDirectory(ec.WorkingDirectory); err != nil {
		return "", err
	}
	return ec.WorkingDirectory, nil
}

func (e *directRunExecutor) Dispatch(ctx context.Context, ec workflow.ExecutorContext) error {
	params := SpawnParams{
		WorkflowID:   string(ec.WorkflowID),
		NodeID:       string(ec.NodeID),
		ActivationID: string(ec.ActivationID),
		AttemptID:    string(ec.AttemptID),
		Dir:          ".",
	}
	if ec.Selection != nil {
		params.Agent = ec.Selection.Agent
		params.Model = ec.Selection.Model
		params.Backend = ec.Selection.Backend
		params.Thinking = ec.Selection.Thinking
		params.AutoApprove = ec.Selection.AutoApprove
		params.PermissionDecider = ec.Selection.PermissionDecider
		params.PermissionModelDecider = ec.Selection.PermissionModelDecider
	}
	if ec.Action.Run != nil {
		params.Prompt = ec.Action.Run.Prompt
		params.PromptFile = ec.Action.Run.PromptFile
		params.Timeout = ec.Action.Run.Timeout
	}
	dir, err := attemptSpawnDir(ec)
	if err != nil {
		// Pre-start failure: the attempt was already started durably by the
		// manager; record termination before returning so the attempt is
		// never left dangling.
		_ = e.sup.workflowManager().RecordAttemptTerminated(
			ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, workflow.AttemptFailed)
		return err
	}
	params.Dir = dir

	hb := e.sup.startLeaseHeartbeat(ec)
	params.workflowTerminate = e.sup.workflowTerminator(ec, hb)
	result, err := e.sup.spawnForAttempt(params, ec.Admission)
	if err != nil {
		// Provider-start (or any synchronous spawn) error: the attempt was
		// already started durably by the manager; record termination before
		// returning so the attempt is never left dangling.
		hb.StopAndWait()
		_ = e.sup.workflowManager().RecordAttemptTerminated(
			ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, workflow.AttemptFailed)
		return err
	}
	if e.sup.testHooks.afterAttemptSpawn != nil {
		e.sup.testHooks.afterAttemptSpawn(result.RuntimeID)
	}
	return nil
}

// loopExecutor returns the Executor that dispatches a workflow loop action to
// a real stable loop child, threading workflow identity through the spawn and
// recording attempt termination on every terminal path.
func (s *Supervisor) loopExecutor() workflow.Executor {
	return &loopExecutor{sup: s}
}

type loopExecutor struct{ sup *Supervisor }

func (e *loopExecutor) Dispatch(ctx context.Context, ec workflow.ExecutorContext) error {
	params := SpawnParams{
		WorkflowID:   string(ec.WorkflowID),
		NodeID:       string(ec.NodeID),
		ActivationID: string(ec.ActivationID),
		AttemptID:    string(ec.AttemptID),
		Dir:          ".",
	}
	if ec.Selection != nil {
		params.Agent = ec.Selection.Agent
		params.Model = ec.Selection.Model
		params.Backend = ec.Selection.Backend
		params.Thinking = ec.Selection.Thinking
		params.AutoApprove = ec.Selection.AutoApprove
		params.PermissionDecider = ec.Selection.PermissionDecider
		params.PermissionModelDecider = ec.Selection.PermissionModelDecider
	}
	if ec.Action.Loop != nil {
		params.LoopFile = ec.Action.Loop.LoopFile
		params.Timeout = ec.Action.Loop.Timeout
	}
	dir, err := attemptSpawnDir(ec)
	if err != nil {
		// Pre-start failure: the attempt was already started durably by the
		// manager; record termination before returning so the attempt is
		// never left dangling.
		kind, label := workflowMarkerForKind(ec.Action.Kind)
		_ = e.sup.workflowManager().RecordAttemptTerminated(
			ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, workflow.AttemptFailed, kind, label)
		return err
	}
	params.Dir = dir

	hb := e.sup.startLeaseHeartbeat(ec)
	params.workflowTerminate = e.sup.workflowTerminator(ec, hb)
	result, err := e.sup.spawnForAttempt(params, ec.Admission)
	if err != nil {
		// Provider-start (or any synchronous spawn) error: the attempt was
		// already started durably by the manager; record termination before
		// returning so the attempt is never left dangling.
		hb.StopAndWait()
		kind, label := workflowMarkerForKind(ec.Action.Kind)
		_ = e.sup.workflowManager().RecordAttemptTerminated(
			ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, workflow.AttemptFailed, kind, label)
		return err
	}
	if e.sup.testHooks.afterAttemptSpawn != nil {
		e.sup.testHooks.afterAttemptSpawn(result.RuntimeID)
	}
	return nil
}

// teamExecutor returns the Executor that dispatches a workflow team action to
// a real stable team child, threading workflow identity through the spawn and
// recording attempt termination on every terminal path.
func (s *Supervisor) teamExecutor() workflow.Executor {
	return &teamExecutor{sup: s}
}

type teamExecutor struct{ sup *Supervisor }

func (e *teamExecutor) Dispatch(ctx context.Context, ec workflow.ExecutorContext) error {
	params := SpawnParams{
		WorkflowID:   string(ec.WorkflowID),
		NodeID:       string(ec.NodeID),
		ActivationID: string(ec.ActivationID),
		AttemptID:    string(ec.AttemptID),
		Dir:          ".",
	}
	if ec.Selection != nil {
		params.Agent = ec.Selection.Agent
		params.Model = ec.Selection.Model
		params.Backend = ec.Selection.Backend
		params.Thinking = ec.Selection.Thinking
		params.AutoApprove = ec.Selection.AutoApprove
		params.PermissionDecider = ec.Selection.PermissionDecider
		params.PermissionModelDecider = ec.Selection.PermissionModelDecider
	}
	if ec.Action.Team != nil {
		params.TeamFile = ec.Action.Team.TeamFile
		params.Timeout = ec.Action.Team.Timeout
	}
	dir, err := attemptSpawnDir(ec)
	if err != nil {
		// Pre-start failure: the attempt was already started durably by the
		// manager; record termination before returning so the attempt is
		// never left dangling.
		kind, label := workflowMarkerForKind(ec.Action.Kind)
		_ = e.sup.workflowManager().RecordAttemptTerminated(
			ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, workflow.AttemptFailed, kind, label)
		return err
	}
	params.Dir = dir

	hb := e.sup.startLeaseHeartbeat(ec)
	params.workflowTerminate = e.sup.workflowTerminator(ec, hb)
	result, err := e.sup.spawnForAttempt(params, ec.Admission)
	if err != nil {
		// Provider-start (or any synchronous spawn) error: the attempt was
		// already started durably by the manager; record termination before
		// returning so the attempt is never left dangling.
		hb.StopAndWait()
		kind, label := workflowMarkerForKind(ec.Action.Kind)
		_ = e.sup.workflowManager().RecordAttemptTerminated(
			ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, workflow.AttemptFailed, kind, label)
		return err
	}
	if e.sup.testHooks.afterAttemptSpawn != nil {
		e.sup.testHooks.afterAttemptSpawn(result.RuntimeID)
	}
	return nil
}

// spawnForAttempt starts one workflow attempt's runtime. When the executor
// context carries a stable admission reservation, the start consumes it
// instead of reserving admission a second time; manual and legacy starts
// self-reserve through the ordinary spawn path.
func (s *Supervisor) spawnForAttempt(params SpawnParams, admission workflow.AdmissionHandle) (SpawnResult, error) {
	if res, ok := admission.(*admissionReservation); ok {
		return s.spawnReserved(params, res)
	}
	return s.spawn(params)
}

// workflowMarkerForKind returns the action-level marker evidence recorded on
// a workflow attempt at termination. Direct runs (run) have no marker. Loop
// and team record the action-declared constant as MarkerKind because the
// aggregate child RunResult does not expose the terminal marker
// directive/label (looprunner/teamrunner keep those internal) — this is the
// decision-2 documented fallback; the parsed exit/abort/continue marker
// remains action-local and is never a kernel command.
func workflowMarkerForKind(kind workflow.ActionKind) (string, string) {
	switch kind {
	case workflow.ActionLoop:
		return "loop", ""
	case workflow.ActionTeam:
		return "team", ""
	}
	return "", ""
}

// stashWorkflowTerminalMarker records the terminal marker label a loop or
// team child observed on its exit, so the attempt's termination path can
// select a declared outcome from it. Only the exit directive is terminal.
func (child *childRuntime) stashWorkflowTerminalMarker(directive, label string) {
	if directive != "exit" || label == "" {
		return
	}
	child.mu.Lock()
	child.workflowMarkerLabel = label
	child.mu.Unlock()
}

// terminalMarkerLabel returns the stashed terminal marker label, if any.
func (child *childRuntime) terminalMarkerLabel() string {
	child.mu.Lock()
	defer child.mu.Unlock()
	return child.workflowMarkerLabel
}

// workflowTerminator returns the termination callback a workflow attempt's
// runtime carries from construction. It runs finishWorkflowAttempt: on a
// successful auto-provider exit the lease heartbeat keeps running through the
// terminal fact and the supervisor's completion, so the lease cannot expire in
// between; on every other path it stops the heartbeat before recording the
// terminal fact.
func (s *Supervisor) workflowTerminator(ec workflow.ExecutorContext, hb *leaseHeartbeat) func(*childRuntime, workflow.AttemptStatus) {
	return func(child *childRuntime, status workflow.AttemptStatus) {
		s.finishWorkflowAttempt(ec, hb, status, child)
	}
}

// recordWorkflowTermination records the terminal workflow attempt fact for a
// direct-run child before its runtime state is cleaned up. No-op when the
// child is not part of a workflow run.
func (s *Supervisor) recordWorkflowTermination(child *childRuntime, panicked bool, ctx context.Context) {
	child.mu.Lock()
	cb := child.onWorkflowTerminate
	workflowID := child.workflowID
	exitCode := child.exitCode
	child.mu.Unlock()
	if workflowID == "" || cb == nil {
		return
	}
	status := workflow.AttemptFailed
	switch {
	case panicked:
		status = workflow.AttemptPanicked
	case exitCode == 124:
		status = workflow.AttemptTimedOut
	case ctx.Err() != nil || exitCode == 130:
		status = workflow.AttemptCanceled
	case exitCode == 0:
		status = workflow.AttemptSucceeded
	}
	cb(status)
}

func (h lazyWorkflowHandler) WorkflowCreate(p json.RawMessage) (any, error) {
	return h.s.workflowManager().WorkflowCreate(p)
}
func (h lazyWorkflowHandler) WorkflowInstantiate(p json.RawMessage) (any, error) {
	return h.s.workflowManager().WorkflowInstantiate(p)
}
func (h lazyWorkflowHandler) WorkflowStatus(id string) (any, error) {
	return h.s.workflowManager().WorkflowStatus(id)
}
func (h lazyWorkflowHandler) WorkflowWait(id string, d time.Duration) (any, error) {
	return h.s.workflowManager().WorkflowWait(id, d)
}
func (h lazyWorkflowHandler) WorkflowInspect(id string) (any, error) {
	return h.s.workflowManager().WorkflowInspect(id)
}
func (h lazyWorkflowHandler) WorkflowEvents(id string, afterSeq int64, limit int) (any, error) {
	return h.s.workflowManager().WorkflowEvents(id, afterSeq, limit)
}

// WorkflowCancel cancels a workflow instance through the kernel and then
// cancels every in-flight attempt runtime this supervisor still owns for that
// instance. The durable cancel lands first: a runtime killed afterwards
// records its late attempt_terminated fact harmlessly on the already-canceled
// instance (the reducer records the attempt fact and skips lease/retry work).
func (h lazyWorkflowHandler) WorkflowCancel(id string, p json.RawMessage) (any, error) {
	result, err := h.s.workflowManager().WorkflowCancel(id, p)
	if err != nil {
		return nil, err
	}
	if m, ok := result.(map[string]any); ok {
		m["canceled_runtimes"] = h.s.cancelWorkflowRuntimes(id)
		result = m
	}
	return result, nil
}
func (h lazyWorkflowHandler) WorkflowCommand(id string, p json.RawMessage) (any, error) {
	return h.s.workflowManager().WorkflowCommand(id, p)
}
