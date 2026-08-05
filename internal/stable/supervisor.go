package stable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/rosterconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/factory"
	"github.com/sdougbrown/avenor/internal/spawnselection"
	"github.com/sdougbrown/avenor/internal/teamrunner"
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

	// MaxTreeBudget is the inherited descendant budget capacity for a root
	// supervisor tree. It bounds the total concurrent runtimes across the
	// whole tree, including nested supervisors. Zero uses
	// admission.DefaultTreeBudget. Ignored when TreeBudgetFile is set (a
	// nested supervisor joins an existing tree rather than creating one).
	MaxTreeBudget int

	// TreeBudgetFile is the path of an existing tree budget to join. When
	// empty, the supervisor is a root and creates a new budget file in
	// Avenor-owned runtime state. When set, the supervisor opens the existing
	// file as a nested participant sharing the root's capacity.
	TreeBudgetFile string
}

type SpawnParams struct {
	Prompt            string `json:"prompt,omitempty"`
	PromptFile        string `json:"prompt_file,omitempty"`
	Dir               string `json:"dir"`
	Agent             string `json:"agent,omitempty"`
	AgentProfile      string `json:"agent_profile,omitempty"`
	Label             string `json:"label,omitempty"`
	Model             string `json:"model,omitempty"`
	Thinking          string `json:"thinking,omitempty"`
	ServerURL         string `json:"server_url,omitempty"`
	Backend           string `json:"backend,omitempty"`
	OnEvent           string `json:"on_event,omitempty"`
	SentinelFile      string `json:"sentinel_file,omitempty"`
	PermissionHandler string `json:"permission_handler,omitempty"`
	AutoApprove       bool   `json:"auto_approve,omitempty"`
	Timeout           int    `json:"timeout,omitempty"`
	MaxRetries        int    `json:"max_retries,omitempty"`
	LoopFile          string `json:"loop_file,omitempty"`
	TeamFile          string `json:"team_file,omitempty"`
	RosterFile        string `json:"roster_file,omitempty"`
	RosterEntry       string `json:"roster_entry,omitempty"`
	SessionID         string `json:"session_id,omitempty"`
	ParentID          string `json:"parent_id,omitempty"`     // runtime ID of the parent agent
	ParentRunID       string `json:"parent_run_id,omitempty"` // broker run ID of the parent, for channel messaging
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
	rosterFile       string
	rosterEntry      string
	effectiveBackend string
	effectiveAgent   string
	effectiveModel   string
	roster           *rosterconfig.Config
	parentID         string   // runtime ID of the parent agent, empty for top-level
	children         []string // runtime IDs spawned by this runtime
	provider         runtime.Provider
	session          runtime.Session
	eventWriter      cli.EventSink
	fileHandler      *permission.FileHandler
	autoApprove      bool
	permClaimTimeout time.Duration
	runID            string
	dir              string
	onEvent          string
	sentinelFile     string
	lifecycleCtx     context.Context
	cancelFn         func()
	interruptFn      func()
	done             chan struct{}
	doneOnce         sync.Once
	exitCode         int
	completed        bool
	active           bool
	activeAttempts   int
	promptCh         chan struct{}
	promptQueue      []string
	latestSeq        int64
	usage            map[string]any
	// finalOutput is a bounded status preview; fullFinalOutput is returned
	// only through the explicit result control method.
	finalOutput          string
	finalOutputTruncated bool
	fullFinalOutput      string
	// Per-runtime status mirror of control.Snapshot fields. Updated from the
	// event fanout so RuntimeStatus can surface phase, phase_label, and
	// pending_permission without consulting the shared ControlState (which
	// reflects only the most recent event across ALL runtimes).
	phase             string
	phaseLabel        string
	pendingPermission bool
	permission        map[string]any
	directAttempt     *sessionAttempt
	mu                sync.Mutex
	writeMu           sync.Mutex
	fanoutWriter      *runtimeFanoutWriter
	providerLifecycle *permissionProviderLifecycle
	shuttingDown      bool
	writerClosing     bool
	writerClosed      bool
	directAnswers     int
	directAnswersDone chan struct{}

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

type Supervisor struct {
	config                       Config
	runID                        string
	control                      *control.ControlServer
	state                        *control.ControlState
	controlMu                    sync.Mutex
	runtimes                     map[string]*childRuntime
	nextID                       int
	shuttingDown                 bool
	shutdownCh                   chan struct{}
	shutdownChOnce               sync.Once
	runtimeActivity              chan struct{}
	childQuestionSeq             int
	pendingQuestions             map[string]pendingChildQuestion // child runtime ID -> pending question
	handledQuestions             map[string]handledChildQuestion // child runtime ID -> latest handled request
	childQuestionTimeout         time.Duration
	httpServer                   *control.HTTPDebugServer
	permOptions                  map[string][]any // keyed by "runtimeID:requestID"
	permissionProviderMu         sync.Mutex
	permissionProviders          map[string]permissionProviderBinding // same key; exact direct-answer target
	httpServers                  map[string]any                       // dir → *managedHTTPServer or errHTTPServerStarting sentinel
	httpServerMu                 sync.Mutex
	httpServerCond               *sync.Cond
	fileSnapshots                map[string][]string // runtimeID → pre-run file list for output detection
	fileSnapMu                   sync.Mutex
	broker                       *broker.Broker
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
}

func NewSupervisor(cfg Config) *Supervisor {
	runID := cli.GenerateRunID()
	state := control.NewState(runID, "", 0)
	sup := &Supervisor{
		config:               cfg,
		runID:                runID,
		state:                state,
		control:              control.NewServer(state),
		runtimes:             map[string]*childRuntime{},
		shutdownCh:           make(chan struct{}),
		runtimeActivity:      make(chan struct{}),
		pendingQuestions:     map[string]pendingChildQuestion{},
		handledQuestions:     map[string]handledChildQuestion{},
		childQuestionTimeout: cfg.ChildQuestionTimeout,
		permOptions:          map[string][]any{},
		reaperInterval:       5 * time.Second,
		permissionProviders:  map[string]permissionProviderBinding{},
		httpServers:          map[string]any{},
		fileSnapshots:        map[string][]string{},
		sessionIdentities:    map[string]sessionIdentityEntry{},
		sessionOwners:        map[string]*sessionAttempt{},
	}
	sup.broker = broker.New("")
	if err := sup.broker.Start(); err != nil {
		// Non-fatal — broker is optional. Runs will still work without it.
	}
	sup.httpServerCond = sync.NewCond(&sup.httpServerMu)
	sup.capacityCh = make(chan struct{})
	if sup.childQuestionTimeout <= 0 {
		sup.childQuestionTimeout = 120 * time.Second
	}
	sup.control.SetStableHandler(sup)
	sup.newProviderFunc = factory.NewProvider
	sup.initTreeBudget()
	return sup
}

func (s *Supervisor) Run() int {
	// NewSupervisor initializes root admission before the control socket is
	// bound. Close it on every exit, including a control-server start failure.
	defer s.closeTreeBudget()

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
		fmt.Fprintf(os.Stderr, "avenor stable: start control server: %v\n", err)
		reason = "start_failed"
		return 1
	}
	defer s.control.Stop()

	if s.config.HTTPDebug != "" {
		var err error
		s.httpServer, err = control.NewHTTPDebugServer(s.config.HTTPDebug, s.control)
		if err != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: start http debug: %v\n", err)
			reason = "start_failed"
			return 1
		}
		s.httpServer.SetStableAdapter(s)
		if err := s.httpServer.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: start http debug: %v\n", err)
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
		fmt.Fprintf(os.Stderr, "avenor stable: tree budget unavailable; using degraded local-only mode: %s\n", message)
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
		fmt.Fprintf(os.Stderr, "avenor stable: write tombstone: %v\n", err)
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

func (s *Supervisor) activeRuntimeCountLocked() int {
	n := 0
	for _, rt := range s.runtimes {
		rt.mu.Lock()
		if !rt.completed {
			n++
		}
		rt.mu.Unlock()
	}
	return n
}

func (s *Supervisor) spawn(params SpawnParams) (SpawnResult, error) {
	workflowMode := params.LoopFile != "" || params.TeamFile != ""
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

	directSupplied := effectiveIdentity{
		Backend: params.Backend, Agent: params.Agent, Model: params.Model,
		AgentProfile: params.AgentProfile, RosterFile: params.RosterFile, RosterEntry: params.RosterEntry,
	}
	backend := params.Backend
	if backend == "" {
		backend = cli.DefaultBackend
	}
	roster := (*rosterconfig.Config)(nil)
	if !workflowMode && params.RosterFile != "" {
		loaded, err := rosterconfig.Load(params.RosterFile)
		if err != nil {
			return SpawnResult{}, err
		}
		roster = loaded
		entry, err := roster.Lookup(params.RosterEntry)
		if err != nil {
			return SpawnResult{}, err
		}
		backend, params.Agent, params.Model = entry.Backend, entry.Agent, entry.Model
	}
	if !workflowMode {
		resolved, err := s.resolveDirectResumeIdentity(params.SessionID, directSupplied, effectiveIdentity{
			Backend: backend, Agent: params.Agent, Model: params.Model,
			AgentProfile: params.AgentProfile, RosterFile: params.RosterFile, RosterEntry: params.RosterEntry,
		})
		if err != nil {
			return SpawnResult{}, err
		}
		backend, params.Agent, params.Model, params.AgentProfile = resolved.Backend, resolved.Agent, resolved.Model, resolved.AgentProfile
		params.RosterFile, params.RosterEntry = resolved.RosterFile, resolved.RosterEntry
		if err := runtime.ValidateThinkingForBackend(backend, params.Thinking); err != nil {
			return SpawnResult{}, err
		}
	}

	// Acquire tree-scoped admission first. The tree budget is a cross-process
	// authority, so it must be reserved before the local slot to avoid
	// transiently inflating the local count for a spawn that will fail. If the
	// tree budget is unavailable the error is typed and retryable; the local
	// limit is not consumed.
	treeToken, err := s.acquireTreeAdmission()
	if err != nil {
		return SpawnResult{}, err
	}
	// releaseTree releases the tree slot for the early-failure paths before a
	// child exists to own it. Once the child is created it owns the release.
	releaseTree := func() {
		s.treeBudgetMu.Lock()
		budget := s.treeBudget
		s.treeBudgetMu.Unlock()
		if budget != nil {
			budget.Release(treeToken)
		}
	}

	s.controlMu.Lock()
	if s.shuttingDown {
		s.controlMu.Unlock()
		releaseTree()
		return SpawnResult{}, fmt.Errorf("supervisor is shutting down")
	}
	activeRuntimes := s.activeRuntimeCountLocked()
	if activeRuntimes >= s.config.MaxRuntimes {
		s.controlMu.Unlock()
		releaseTree()
		return SpawnResult{}, &admission.CapacityError{Source: "local", Limit: s.config.MaxRuntimes, Active: activeRuntimes}
	}
	s.nextID++
	rtID := fmt.Sprintf("rt_%d", s.nextID)
	childCtx, childCancel := context.WithCancel(context.Background())

	// Reserve the slot to prevent TOCTOU bypass of the max-runtime limit.
	// Every child mode shares this lifecycle context so direct provider calls
	// can be canceled before writer teardown waits for them.
	child := &childRuntime{
		id:           rtID,
		lifecycleCtx: childCtx,
		cancelFn:     childCancel,
		done:         make(chan struct{}),
		promptCh:     make(chan struct{}, 1),
		autoApprove:  params.AutoApprove,
	}
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
	s.runtimes[rtID] = child
	s.controlMu.Unlock()

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
		child.complete()
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
		parentToken, _ = s.broker.EnsureRun(params.ParentRunID)
		parentNote := fmt.Sprintf("Your parent's broker run ID is %q. Use avenor_upsend with this run ID to send status updates, findings, or questions back to your parent.\n\n", params.ParentRunID)
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
	if permHandler == "" && !params.AutoApprove && sentinelFile != "" {
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

		child.label = params.Label
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
		go s.runLoopChild(childCtx, child, cfg, params.MaxRetries, params.Agent, params.AgentProfile, params.Model, params.Thinking, params.ServerURL, backend)

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

		child.label = params.Label
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
		go s.runTeamChild(childCtx, child, cfg, params.MaxRetries, params.Agent, params.AgentProfile, params.Model, params.Thinking, params.ServerURL, backend)

		return result, nil
	}

	// Start provider and session.
	startOpts := runtime.StartOptions{
		Agent:        params.Agent,
		AgentProfile: params.AgentProfile,
		Label:        params.Label,
		Dir:          params.Dir,
		Model:        params.Model,
		Thinking:     params.Thinking,
		RuntimeID:    rtID,
		Broker:       s.broker,
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
	child.label = params.Label
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
		Backend:      backend,
		Agent:        params.Agent,
		Model:        params.Model,
		AgentProfile: params.AgentProfile,
		RosterFile:   params.RosterFile,
		RosterEntry:  params.RosterEntry,
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

func (s *Supervisor) runChild(ctx context.Context, child *childRuntime, promptText string, timeoutSec, maxRetries int) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: child %s panic: %v\n", child.id, r)
			s.emitChildError(child, fmt.Sprintf("panic: %v", r), "error")
			if child.sentinelFile != "" {
				cli.WriteSentinel(child.sentinelFile, 1, child.sessionID(), "error", s.runID, os.Stderr)
			}
		}
		providerLifecycle := s.beginChildShutdown(child)
		s.closeChildEventWriter(child)
		s.clearRuntimePermissionOptions(child.id)
		s.closeChildProvider(child, providerLifecycle)
		// A claim reservation admitted just before shutdown may finish while
		// provider close drains it. Sweep again so no late binding survives.
		s.clearRuntimePermissionOptions(child.id)
		child.complete()
	}()

	if s.broker != nil {
		s.broker.CreateRun(child.id)
	}

	var timer <-chan time.Time
	if timeoutSec > 0 {
		t := time.NewTimer(time.Duration(timeoutSec) * time.Second)
		defer t.Stop()
		timer = t.C
	}

	resumeID := ""
	needAcquire := false // tree token held from spawn for the first attempt
	for attempt := 1; ; attempt++ {
		if attempt > 1 && resumeID == "" {
			resumeID = child.sessionID()
		}
		if needAcquire {
			if err := s.acquireChildTreeToken(ctx, child); err != nil {
				return
			}
			needAcquire = false
		}
		result := s.runChildAttempt(ctx, child, resumeID, promptText, timer)
		// Release tree admission after the turn so a parked runtime does not
		// consume descendant budget while idle. A subsequent prompt
		// re-acquires before its turn.
		s.releaseChildTreeToken(child)
		needAcquire = true
		if !runtime.IsRetryableFailure(result.exitCode, result.stopReason) || attempt > maxRetries {
			stopReason := result.stopReason
			if stopReason == "" {
				stopReason = runtime.StopReasonForExitCode(result.exitCode)
			}
			child.mu.Lock()
			child.exitCode = result.exitCode
			child.mu.Unlock()

			if result.exitCode == 0 {
				if child.sentinelFile != "" {
					cli.WriteSentinel(child.sentinelFile, result.exitCode, result.sessionID, stopReason, s.runID, os.Stderr)
				}
				child.mu.Lock()
				child.phase = "done"
				child.mu.Unlock()
				nextPrompt, ok := child.waitForNextPrompt(ctx)
				if !ok {
					if ctx.Err() != nil {
						s.writeIdleCancelled(child)
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
			}

			if ctx.Err() == nil && result.stopReason != runtime.SessionIDConflictStopReason {
				if nextPrompt, ok := child.dequeuePrompt(); ok {
					if child.sentinelFile != "" {
						cli.WriteSentinel(child.sentinelFile, result.exitCode, result.sessionID, stopReason, s.runID, os.Stderr)
					}
					promptText = nextPrompt
					resumeID = result.sessionID
					if resumeID == "" {
						resumeID = child.sessionID()
					}
					attempt = 0
					continue
				}
			}

			if child.sentinelFile != "" {
				cli.WriteSentinel(child.sentinelFile, result.exitCode, result.sessionID, stopReason, s.runID, os.Stderr)
			}
			return
		}
		if result.sessionID != "" {
			resumeID = result.sessionID
		}
		if nextPrompt, ok := child.dequeuePrompt(); ok && ctx.Err() == nil {
			promptText = nextPrompt
			attempt = 0
			continue
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-time.After(backoffDelay(attempt)):
		case <-ctx.Done():
			return
		}
	}
}

func (s *Supervisor) runLoopChild(ctx context.Context, child *childRuntime, cfg *looprunner.LoopConfig, maxRetries int, agent, agentProfile, model, thinking, serverURL, backend string) {
	var brokerAttemptIDs []string
	var brokerAttemptIDsMu sync.Mutex
	sessions := newWorkflowSessionTracker()
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: child %s panic: %v\n", child.id, r)
			s.emitChildError(child, fmt.Sprintf("panic: %v", r), "error")
			if final, ok := sessions.latest(); ok {
				s.finalizeWorkflowChild(child, final)
			}
			if child.sentinelFile != "" {
				cli.WriteSentinel(child.sentinelFile, 1, child.sessionID(), "error", s.runID, os.Stderr)
			}
		}
		s.beginChildShutdown(child)
		s.closeChildEventWriter(child)
		s.clearRuntimePermissionOptions(child.id)
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
		child.complete()
	}()

	if s.broker != nil {
		s.broker.CreateRun(child.id)
	}
	taggedWriter := &runtimeFanoutWriter{
		base:            child.eventWriter,
		runtimeID:       child.id,
		child:           child,
		control:         s.control,
		metadata:        cli.NewEventMetadata(s.runID, child.label, child.id),
		onPermissionReq: s.cachePermissionOptions,
		recorder:        newRecorderFor(s, child.id),
	}
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
			child.mu.Lock()
			child.phase = ""
			child.phaseLabel = ""
			phaseRoster := child.roster
			phaseRosterFile := child.rosterFile
			child.mu.Unlock()

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
				if resolveErr = runtime.ValidateThinkingForBackend(selection.Backend, thinking); resolveErr != nil {
					return looprunner.PhaseAttemptResult{ExitCode: 1}, resolveErr
				}
				selectionMu.Lock()
				resolvedSelections[phase.Name] = selection
				selectionMu.Unlock()
			}

			identity := effectiveIdentity{
				Backend: selection.Backend, Agent: selection.Agent, Model: selection.Model,
				AgentProfile: agentProfile, RosterFile: phaseRosterFile, RosterEntry: phase.RosterEntry,
			}
			var resumeErr error
			identity, resumeErr = s.resolveWorkflowResumeIdentity(prevSessionID, identity)
			if resumeErr != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1}, resumeErr
			}

			startOpts := runtime.StartOptions{
				Agent:        identity.Agent,
				AgentProfile: identity.AgentProfile,
				Label:        child.label,
				Model:        identity.Model,
				Thinking:     thinking,
				Dir:          child.dir,
				ServerURL:    serverURL,
				RuntimeID:    child.id,
				Broker:       s.broker,
			}

			resumeID := prevSessionID

			var brokerRunID string
			attemptWriter := taggedWriter
			if s.broker != nil {
				brokerRunID = broker.MakeToken()
				s.broker.CreateRun(brokerRunID)
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

			result := cli.WaitForSession(ctx, provider, cli.SessionWaitConfig{
				EventCh:                eventCh,
				PromptDone:             promptDone,
				SessionID:              session.SessionID,
				RunID:                  s.runID,
				RunLabel:               child.label,
				PermissionClaimScope:   child.id,
				AutoApprove:            child.autoApprove,
				PermissionClaimTimeout: child.permClaimTimeout,
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
				Stderr:       os.Stderr,
				ProviderTurn: providerTurn,
			})

			sessions.remember(phase.Name, session.SessionID, identity)
			return looprunner.PhaseAttemptResult{
				ExitCode:      result.ExitCode,
				SessionID:     session.SessionID,
				StopReason:    result.StopReason,
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
			cli.WriteSentinel(child.sentinelFile, 1, child.sessionID(), "error", s.runID, os.Stderr)
		}
		return
	}
	child.mu.Lock()
	child.exitCode = result.ExitCode
	child.mu.Unlock()

	// When every iteration phase returns end_turn, looprunner stops at
	// max_iterations with result.SessionID == "". Use the latest adopted child
	// session ID to populate the terminal sentinel.
	if result.SessionID == "" {
		result.SessionID = child.sessionID()
	}

	if child.sentinelFile != "" {
		if result.Reason != "" {
			cli.WriteSentinelWithReason(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, result.Reason, os.Stderr)
		} else {
			cli.WriteSentinel(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, os.Stderr)
		}
	}
}

func (s *Supervisor) runTeamChild(ctx context.Context, child *childRuntime, cfg *teamrunner.TeamConfig, maxRetries int, agent, agentProfile, model, thinking, serverURL, backend string) {
	var brokerAttemptIDs []string
	var brokerAttemptIDsMu sync.Mutex
	sessions := newWorkflowSessionTracker()
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: child %s panic: %v\n", child.id, r)
			s.emitChildError(child, fmt.Sprintf("panic: %v", r), "error")
			if final, ok := sessions.final(cfg.Post, cfg.Team, cfg.Pre); ok {
				s.finalizeWorkflowChild(child, final)
			}
			if child.sentinelFile != "" {
				cli.WriteSentinel(child.sentinelFile, 1, child.sessionID(), "error", s.runID, os.Stderr)
			}
		}
		s.beginChildShutdown(child)
		s.closeChildEventWriter(child)
		s.clearRuntimePermissionOptions(child.id)
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
		child.complete()
	}()

	if s.broker != nil {
		s.broker.CreateRun(child.id)
	}
	taggedWriter := &runtimeFanoutWriter{
		base:            child.eventWriter,
		runtimeID:       child.id,
		child:           child,
		control:         s.control,
		metadata:        cli.NewEventMetadata(s.runID, child.label, child.id),
		onPermissionReq: s.cachePermissionOptions,
		recorder:        newRecorderFor(s, child.id),
	}
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
			child.mu.Lock()
			child.phase = ""
			child.phaseLabel = ""
			phaseRoster := child.roster
			phaseRosterFile := child.rosterFile
			child.mu.Unlock()

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
				if resolveErr = runtime.ValidateThinkingForBackend(selection.Backend, thinking); resolveErr != nil {
					return teamrunner.PhaseAttemptResult{ExitCode: 1}, resolveErr
				}
				selectionMu.Lock()
				resolvedSelections[phase.Name] = selection
				selectionMu.Unlock()
			}
			identity := effectiveIdentity{
				Backend: selection.Backend, Agent: selection.Agent, Model: selection.Model,
				AgentProfile: agentProfile, RosterFile: phaseRosterFile, RosterEntry: phase.RosterEntry,
			}
			var resumeErr error
			identity, resumeErr = s.resolveWorkflowResumeIdentity(prevSessionID, identity)
			if resumeErr != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1}, resumeErr
			}
			startOpts := runtime.StartOptions{
				Agent:        identity.Agent,
				AgentProfile: identity.AgentProfile,
				Label:        child.label,
				Model:        identity.Model,
				Thinking:     thinking,
				Dir:          child.dir,
				ServerURL:    serverURL,
				RuntimeID:    child.id,
				Broker:       s.broker,
			}

			resumeID := prevSessionID

			var brokerRunID string
			attemptWriter := taggedWriter
			if s.broker != nil {
				brokerRunID = broker.MakeToken()
				s.broker.CreateRun(brokerRunID)
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

			result := cli.WaitForSession(ctx, provider, cli.SessionWaitConfig{
				EventCh:                eventCh,
				PromptDone:             promptDone,
				SessionID:              session.SessionID,
				RunID:                  s.runID,
				RunLabel:               child.label,
				PermissionClaimScope:   child.id,
				AutoApprove:            child.autoApprove,
				PermissionClaimTimeout: child.permClaimTimeout,
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
				Stderr:       os.Stderr,
				ProviderTurn: providerTurn,
			})

			sessions.remember(phase.Name, session.SessionID, identity)
			return teamrunner.PhaseAttemptResult{
				ExitCode:      result.ExitCode,
				SessionID:     session.SessionID,
				StopReason:    result.StopReason,
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
			cli.WriteSentinel(child.sentinelFile, 1, child.sessionID(), "error", s.runID, os.Stderr)
		}
		return
	}
	child.mu.Lock()
	child.exitCode = result.ExitCode
	child.mu.Unlock()

	// A successful team can finish all members and post phases with end_turn.
	// The aggregate RunResult then has no authoritative SessionID. Use the latest
	// adopted child session ID to populate the terminal sentinel.
	if result.SessionID == "" {
		result.SessionID = child.sessionID()
	}

	if child.sentinelFile != "" {
		if result.Reason != "" {
			cli.WriteSentinelWithReason(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, result.Reason, os.Stderr)
		} else {
			cli.WriteSentinel(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, os.Stderr)
		}
	}
}

type childAttemptResult struct {
	exitCode   int
	sessionID  string
	stopReason string
}

func (c *childRuntime) dequeuePrompt() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.completed || len(c.promptQueue) == 0 {
		return "", false
	}
	prompt := c.promptQueue[0]
	c.promptQueue = c.promptQueue[1:]
	return prompt, true
}

func (c *childRuntime) waitForNextPrompt(ctx context.Context) (string, bool) {
	for {
		if prompt, ok := c.dequeuePrompt(); ok {
			return prompt, true
		}
		c.mu.Lock()
		if c.completed {
			c.mu.Unlock()
			return "", false
		}
		ch := c.promptCh
		c.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
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

func authoritativeIdentityEqual(left, right effectiveIdentity) bool {
	return left.Backend == right.Backend && left.Agent == right.Agent && left.Model == right.Model && left.AgentProfile == right.AgentProfile
}

func resumeIdentityConflict(sessionID, field, supplied, authoritative string) error {
	return fmt.Errorf("cannot resume session %q with %s %q: session belongs to %s %q", sessionID, field, supplied, field, authoritative)
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
	lifecycle.closing = child.shuttingDown
	if lifecycle.closing {
		lifecycle.cancelAnswers()
	}
	child.mu.Lock()
	child.activeAttempts++
	child.active = true
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
	return lifecycle
}

func (s *Supervisor) endWorkflowAttempt(child *childRuntime, provider runtime.Provider) {
	child.mu.Lock()
	if child.activeAttempts > 0 {
		child.activeAttempts--
	}
	if child.provider == provider {
		sessionID := child.session.SessionID
		child.provider = nil
		// Retain only the authoritative ID. The provider's PID and other fields
		// become invalid when it closes.
		child.session = runtime.Session{SessionID: sessionID}
	}
	child.active = child.activeAttempts > 0
	if !child.active {
		child.phase = ""
		child.phaseLabel = ""
	}
	child.mu.Unlock()
}

func (s *Supervisor) finalizeWorkflowChild(child *childRuntime, final workflowSession) {
	child.mu.Lock()
	child.provider = nil
	child.providerLifecycle = nil
	child.session = runtime.Session{SessionID: final.SessionID}
	child.effectiveBackend = final.Identity.Backend
	child.effectiveAgent = final.Identity.Agent
	child.effectiveModel = final.Identity.Model
	child.agentProfile = final.Identity.AgentProfile
	child.rosterFile = final.Identity.RosterFile
	child.rosterEntry = final.Identity.RosterEntry
	child.activeAttempts = 0
	child.active = false
	child.mu.Unlock()
}

func (s *Supervisor) statusIdentity(child *childRuntime, sessionID string) effectiveIdentity {
	if identity, ok := s.sessionIdentity(sessionID); ok {
		return identity
	}
	identity := effectiveIdentity{
		Backend:      child.effectiveBackend,
		Agent:        child.effectiveAgent,
		Model:        child.effectiveModel,
		AgentProfile: child.agentProfile,
		RosterFile:   child.rosterFile,
		RosterEntry:  child.rosterEntry,
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

func (s *Supervisor) runChildAttempt(ctx context.Context, child *childRuntime, resumeID, promptText string, timer <-chan time.Time) childAttemptResult {
	child.mu.Lock()
	child.phase = ""
	child.phaseLabel = ""
	child.mu.Unlock()

	session, err := s.attemptSession(ctx, child, resumeID)
	if err != nil {
		s.emitChildError(child, fmt.Sprintf("start session: %v", err), "error")
		return childAttemptResult{exitCode: 1}
	}
	child.mu.Lock()
	child.session = session
	attemptProvider := child.provider
	child.mu.Unlock()

	turnCtx, cancelTurn := context.WithCancel(ctx)
	child.mu.Lock()
	child.active = true
	child.interruptFn = cancelTurn
	child.mu.Unlock()
	defer func() {
		cancelTurn()
		child.mu.Lock()
		child.active = false
		child.interruptFn = nil
		child.phase = ""
		child.phaseLabel = ""
		child.mu.Unlock()
	}()

	eventCtx, cancelEvents := context.WithCancel(turnCtx)
	defer cancelEvents()

	child.mu.Lock()
	attempt := child.directAttempt
	identity := effectiveIdentity{
		Backend: child.effectiveBackend, Agent: child.effectiveAgent, Model: child.effectiveModel,
		AgentProfile: child.agentProfile, RosterFile: child.rosterFile, RosterEntry: child.rosterEntry,
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
	taggedWriter := &runtimeFanoutWriter{
		base:            child.eventWriter,
		runtimeID:       child.id,
		child:           child,
		control:         s.control,
		metadata:        cli.NewEventMetadata(s.runID, child.label, child.id),
		onPermissionReq: s.cachePermissionOptions,
		recorder:        newRecorderFor(s, child.id),
	}
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
		Timeout:                timer,
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
		Stderr:       os.Stderr,
		ProviderTurn: providerTurn,
	})
	exitCode := result.ExitCode
	return childAttemptResult{exitCode: exitCode, sessionID: session.SessionID, stopReason: result.StopReason}
}

func (s *Supervisor) attemptSession(ctx context.Context, child *childRuntime, resumeID string) (runtime.Session, error) {
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

func (s *Supervisor) writeIdleCancelled(child *childRuntime) {
	if child.sentinelFile != "" {
		cli.WriteSentinel(child.sentinelFile, 130, child.sessionID(), "cancelled", s.runID, os.Stderr)
	}
	s.emitSessionEnd(child, 130, "cancelled")
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
			fmt.Fprintf(os.Stderr, "avenor stable: %d runtimes did not finish within %v\n", remaining, timeout)
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
		status := "idle"
		if rt.active {
			status = "running"
		}
		if rt.completed {
			status = "ended"
		}
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
			"status":             status,
			"exit_code":          rt.exitCode,
			"on_event":           rt.onEvent,
			"event_path":         rt.onEvent,
			"sentinel_file":      rt.sentinelFile,
			"parent_id":          rt.parentID,
			"children":           rt.children,
			"pid":                rt.session.PID,
			"phase":              rt.phase,
			"phase_label":        rt.phaseLabel,
			"pending_permission": rt.pendingPermission,
			"latest_seq":         rt.latestSeq,
			"auto_approve":       rt.autoApprove,
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

func (s *Supervisor) answerPermission(rtID, requestID, optionID, message string) error {
	s.controlMu.Lock()
	rt := s.runtimes[rtID]
	s.controlMu.Unlock()
	if rt == nil {
		return fmt.Errorf("runtime %q not found", rtID)
	}
	if s.control.PermissionResolverState(rtID, requestID) == control.PermissionResolverResolved {
		return nil
	}

	// Validate message before consuming the pending claim so oversized
	// or invalid input does not deplete the resolver.
	if err := runtime.ValidatePermissionMessage(message); err != nil {
		return err
	}

	s.controlMu.Lock()
	key := rtID + ":" + requestID
	options := s.permOptions[key]
	s.controlMu.Unlock()
	if options == nil {
		if s.control.PermissionResolverState(rtID, requestID) == control.PermissionResolverResolved {
			return nil
		}
		return fmt.Errorf("permission request %q not found for runtime %q", requestID, rtID)
	}

	kind := ""
	requiresMessage := false
	found := false
	for _, opt := range options {
		m, ok := opt.(map[string]any)
		if !ok {
			continue
		}
		oid, _ := m["optionId"].(string)
		if oid == optionID {
			k, _ := m["kind"].(string)
			kind = permission.NormalizeOptionKind(k)
			requiresMessage, _ = m["requiresMessage"].(bool)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("unknown option_id %q for request %q on runtime %q", optionID, requestID, rtID)
	}
	if requiresMessage && message == "" {
		return fmt.Errorf("option_id %q for request %q requires a message", optionID, requestID)
	}
	switch kind {
	case "allow", "reject":
	default:
		return fmt.Errorf("unsupported option kind %q for option_id %q on request %q", kind, optionID, requestID)
	}

	// Stable runtimes use the same CLI permission resolver as top-level runs.
	// If it has an active control claim, feed the answer through that claim so
	// the resolver can call the provider, emit permission.response, clear the
	// waiting status, and release its in-flight guard. Calling the provider
	// directly here leaves the resolver owned until client disconnect or an
	// explicit claim timeout and makes the next request appear to overlap.
	switch s.control.DeliverPendingPermission(rtID, requestID, optionID, message) {
	case control.PermissionAnswerDelivered:
		s.controlMu.Lock()
		delete(s.permOptions, key)
		s.controlMu.Unlock()
		s.permissionProviderMu.Lock()
		delete(s.permissionProviders, key)
		s.permissionProviderMu.Unlock()
		return nil
	case control.PermissionAnswerAlreadyResolved:
		return nil
	case control.PermissionAnswerChannelFull:
		return fmt.Errorf("permission request %q for runtime %q already has an answer pending delivery", requestID, rtID)
	case control.PermissionAnswerResolverOwned:
		return fmt.Errorf("permission request %q for runtime %q is owned by another resolver", requestID, rtID)
	case control.PermissionAnswerNotFound:
		return fmt.Errorf("permission request %q for runtime %q has no registered resolver state", requestID, rtID)
	case control.PermissionAnswerNoResolver:
		// No control, automatic, or file resolver owns this request. The direct
		// provider path below is the only remaining way to answer it.
	case control.PermissionAnswerInvalid:
		return fmt.Errorf("permission request %q for runtime %q rejected an invalid answer", requestID, rtID)
	}

	// A request emitted without any configured resolver remains backend-owned.
	s.permissionProviderMu.Lock()
	providerBinding, hasProviderBinding := s.permissionProviders[key]
	s.permissionProviderMu.Unlock()
	var providerBindingPtr *permissionProviderBinding
	if hasProviderBinding {
		providerBindingPtr = &providerBinding
	}
	reservation, active := s.beginDirectPermissionAnswer(rt, providerBindingPtr)
	if !active {
		s.control.RetryDirectPermissionDelivery(rtID, requestID)
		return fmt.Errorf("runtime %q has no active permission provider or is closing", rtID)
	}
	providerErr := s.callDirectPermissionProvider(reservation, requestID, runtime.PermissionResponse{
		Allow:    kind == "allow",
		OptionID: optionID,
		Message:  message,
	})

	// No supervisor lock is held across the provider call. Completion resumes
	// the established writeMu -> controlMu -> pendingMu order. The direct claim
	// remains non-terminal until the canonical response write returns, so a
	// reused request ID cannot replace it before durable publication.
	rt.writeMu.Lock()
	defer func() {
		s.finishDirectPermissionAnswerLocked(rt, reservation)
		rt.writeMu.Unlock()
	}()
	if providerErr != nil {
		s.control.RetryDirectPermissionDelivery(rtID, requestID)
		return providerErr
	}
	if err := reservation.writer.writeLocked(events.Event{
		Event:     "permission.response",
		SessionID: reservation.sessionID,
		Fields: map[string]any{
			"request_id": requestID,
			"option_id":  optionID,
			"kind":       kind,
			// A stable answer_permission call is a control-plane answer even
			// when it had to deliver directly because no resolver owned the
			// request. Keep the public source vocabulary documented.
			"source": "control",
			"ts":     time.Now().UnixMilli(),
		},
	}); err != nil {
		// Keep a non-terminal tombstone. The same request may be retried, but
		// a reused ID cannot replace it without a durable canonical response.
		s.control.RetryDirectPermissionDelivery(rtID, requestID)
		return err
	}
	s.cleanupDirectPermission(rtID, requestID, nil)
	return nil
}

// Acquire controlMu to prevent replacements from publishing options while
// options are removed and the claim is marked resolved.
func (s *Supervisor) cleanupDirectPermission(runtimeID, requestID string, beforeRelease func()) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	delete(s.permOptions, runtimeID+":"+requestID)
	s.permissionProviderMu.Lock()
	delete(s.permissionProviders, runtimeID+":"+requestID)
	s.permissionProviderMu.Unlock()
	if beforeRelease != nil {
		beforeRelease()
	}
	s.control.MarkPermissionClaimResolved(runtimeID, requestID, "direct")
}

func (s *Supervisor) cachePermissionOptions(runtimeID, requestID string, options []any) {
	s.controlMu.Lock()
	s.permOptions[runtimeID+":"+requestID] = options
	s.controlMu.Unlock()
}

func (s *Supervisor) clearRuntimePermissionOptions(runtimeID string) {
	s.controlMu.Lock()
	prefix := runtimeID + ":"
	for k := range s.permOptions {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(s.permOptions, k)
		}
	}
	s.controlMu.Unlock()
	s.permissionProviderMu.Lock()
	for k := range s.permissionProviders {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(s.permissionProviders, k)
		}
	}
	s.permissionProviderMu.Unlock()
	s.control.ClearPermissionClaims(runtimeID)
}

// complete publishes terminal completion after the caller has finished all
// teardown. It is safe for reservation rollback and a child goroutine to race.
func (child *childRuntime) complete() {
	child.doneOnce.Do(func() {
		child.releaseTreeToken()
		child.mu.Lock()
		child.completed = true
		child.mu.Unlock()
		close(child.done)
	})
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
	lifecycle.closing = child.shuttingDown
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
		child.providerLifecycle.closing = child.shuttingDown
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
	if lifecycle == nil || lifecycle.closing || child.shuttingDown || child.writerClosing {
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
	if supervisorClosing || child.shuttingDown || child.writerClosing || lifecycle == nil || lifecycle.closing || writer == nil || ctx.Err() != nil {
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
		s.permissionProviderMu.Lock()
		s.permissionProviders[scope+":"+requestID] = permissionProviderBinding{lifecycle: lifecycle, turn: turn, sessionID: sessionID, writer: writer}
		s.permissionProviderMu.Unlock()
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
	alreadyShuttingDown := child.shuttingDown
	child.shuttingDown = true
	child.mu.Lock()
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
	if cancelProvider && turn != nil {
		turn.RequestCancel(sessionID, directPermissionJoinTimeout)
	}
	if !alreadyShuttingDown && cancel != nil {
		cancel()
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
	if supervisorClosing || child.shuttingDown || child.writerClosing {
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
	return &directPermissionReservation{lifecycle: lifecycle, turn: turn, sessionID: sessionID, ctx: ctx, writer: writer}, true
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
	child.writerClosing = true
	if child.directAnswers > 0 {
		done := child.directAnswersDone
		if s.beforeChildWriterCloseWait != nil {
			s.beforeChildWriterCloseWait()
		}
		child.writeMu.Unlock()
		<-done
		child.writeMu.Lock()
	}
	if !child.writerClosed && child.eventWriter != nil {
		_ = child.eventWriter.Close()
		child.writerClosed = true
	}
	child.writeMu.Unlock()
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
		writer = &runtimeFanoutWriter{
			base:            child.eventWriter,
			runtimeID:       child.id,
			child:           child,
			control:         s.control,
			metadata:        cli.NewEventMetadata(s.runID, child.label, child.id),
			onPermissionReq: s.cachePermissionOptions,
		}
	}
	return writer
}

type runtimeFanoutWriter struct {
	base            cli.EventSink
	runtimeID       string
	child           *childRuntime
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
		w.child.writeMu.Lock()
		defer w.child.writeMu.Unlock()
	}
	return w.writeLocked(ev)
}

// writeLocked is the canonical event path for callers that already hold the
// child write lock. In particular, a direct permission answer must mark the
// old claim, clear its status, and publish its response as one ordered write
// relative to a reused request ID.
func (w *runtimeFanoutWriter) writeLocked(ev events.Event) error {
	// stamped is the lossless durable event (written to file/broker);
	// presentation is the bounded copy for control/status surfaces.
	stamped := ev
	if w.metadata != nil {
		stamped = w.metadata.Stamp(ev)
	} else {
		if stamped.Fields == nil {
			stamped.Fields = map[string]any{}
		}
		if _, ok := stamped.Fields["runtime_id"]; !ok && w.runtimeID != "" {
			stamped.Fields["runtime_id"] = w.runtimeID
		}
		if w.control != nil {
			stamped = w.control.CanonicalizeEvent(stamped)
		}
	}
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
			if phase, _ := presentation.Fields["phase"].(string); phase != "" && w.child.phase != "done" {
				w.child.phase = phase
				if label, _ := presentation.Fields["label"].(string); label != "" {
					w.child.phaseLabel = label
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
			w.child.phase = "done"
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

// StableHandler implementation.

func (s *Supervisor) Spawn(raw json.RawMessage) (any, error) {
	var p SpawnParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("invalid spawn params: %w", err)
	}
	// Auto-populate ParentID when the spawn is called by a known runtime.
	// The caller embeds its own runtime_id in the spawn params; if it maps to
	// a registered runtime, treat it as the parent.
	if p.ParentID == "" {
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
	return s.spawn(p)
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
	status := "idle"
	if rt.active {
		status = "running"
	}
	if rt.completed {
		status = "ended"
	}
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
		"status":             status,
		"exit_code":          rt.exitCode,
		"on_event":           rt.onEvent,
		"event_path":         rt.onEvent,
		"sentinel_file":      rt.sentinelFile,
		"parent_id":          rt.parentID,
		"children":           rt.children,
		"pid":                rt.session.PID,
		"phase":              rt.phase,
		"phase_label":        rt.phaseLabel,
		"pending_permission": rt.pendingPermission,
		"latest_seq":         rt.latestSeq,
		"auto_approve":       rt.autoApprove,
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
	if rt.completed {
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
	return s.answerPermission(rtID, requestID, optionID, message)
}

func (s *Supervisor) RuntimeInterruptAndPrompt(rtID, text string, keepQueue bool) error {
	s.controlMu.Lock()
	rt := s.runtimes[rtID]
	s.controlMu.Unlock()
	if rt == nil {
		return fmt.Errorf("runtime %q not found", rtID)
	}
	rt.mu.Lock()
	if rt.completed {
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
