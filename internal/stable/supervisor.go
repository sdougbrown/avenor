package stable

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/factory"
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
}

type SpawnParams struct {
	Prompt            string `json:"prompt,omitempty"`
	PromptFile        string `json:"prompt_file,omitempty"`
	Dir               string `json:"dir"`
	Agent             string `json:"agent,omitempty"`
	Label             string `json:"label,omitempty"`
	Model             string `json:"model,omitempty"`
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
	SessionID         string `json:"session_id,omitempty"`
	ParentID          string `json:"parent_id,omitempty"`     // runtime ID of the parent agent
	ParentRunID       string `json:"parent_run_id,omitempty"` // broker run ID of the parent, for channel messaging
}

type SpawnResult struct {
	RuntimeID    string `json:"runtime_id"`
	SessionID    string `json:"session_id"`
	OnEvent      string `json:"on_event"`
	SentinelFile string `json:"sentinel_file"`
	BrokerURL    string `json:"broker_url,omitempty"`
	ParentToken  string `json:"parent_token,omitempty"`
}

type childRuntime struct {
	id               string
	label            string
	agent            string
	model            string
	backend          string
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
	cancelFn         func()
	interruptFn      func()
	done             chan struct{}
	exitCode         int
	completed        bool
	active           bool
	promptCh         chan struct{}
	promptQueue      []string
	latestSeq        int64
	usage            map[string]any
	finalOutput      string
	// Per-runtime status mirror of control.Snapshot fields. Updated from the
	// event fanout so RuntimeStatus can surface phase, phase_label, and
	// pending_permission without consulting the shared ControlState (which
	// reflects only the most recent event across ALL runtimes).
	phase             string
	phaseLabel        string
	pendingPermission bool
	permission        map[string]any
	mu                sync.Mutex
	writeMu           sync.Mutex
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
	config               Config
	runID                string
	control              *control.ControlServer
	state                *control.ControlState
	controlMu            sync.Mutex
	runtimes             map[string]*childRuntime
	nextID               int
	shutdownCh           chan struct{}
	runtimeActivity      chan struct{}
	childQuestionSeq     int
	pendingQuestions     map[string]pendingChildQuestion // child runtime ID -> pending question
	handledQuestions     map[string]handledChildQuestion // child runtime ID -> latest handled request
	childQuestionTimeout time.Duration
	httpServer           *control.HTTPDebugServer
	permOptions          map[string][]any // keyed by "runtimeID:requestID"
	httpServers          map[string]any   // dir → *managedHTTPServer or errHTTPServerStarting sentinel
	httpServerMu         sync.Mutex
	httpServerCond       *sync.Cond
	fileSnapshots        map[string][]string // runtimeID → pre-run file list for output detection
	fileSnapMu           sync.Mutex
	broker               *broker.Broker
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
		httpServers:          map[string]any{},
		fileSnapshots:        map[string][]string{},
	}
	sup.broker = broker.New("")
	if err := sup.broker.Start(); err != nil {
		// Non-fatal — broker is optional. Runs will still work without it.
	}
	sup.httpServerCond = sync.NewCond(&sup.httpServerMu)
	if sup.childQuestionTimeout <= 0 {
		sup.childQuestionTimeout = 120 * time.Second
	}
	sup.control.SetStableHandler(sup)
	return sup
}

func (s *Supervisor) Run() int {
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
	s.controlMu.Lock()
	if s.activeRuntimeCountLocked() >= s.config.MaxRuntimes {
		s.controlMu.Unlock()
		return SpawnResult{}, fmt.Errorf("max runtimes (%d) reached", s.config.MaxRuntimes)
	}
	s.nextID++
	rtID := fmt.Sprintf("rt_%d", s.nextID)

	// Reserve the slot to prevent TOCTOU bypass of the max-runtime limit.
	child := &childRuntime{
		id:       rtID,
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
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
		s.controlMu.Lock()
		delete(s.runtimes, rtID)
		s.controlMu.Unlock()
	}
	defer func() {
		if releaseReservation != nil {
			releaseReservation()
		}
	}()

	if params.Dir == "" {
		params.Dir = "."
	}
	backend := params.Backend
	if backend == "" {
		backend = cli.DefaultBackend
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
		cfg, err := looprunner.LoadLoopConfig(params.LoopFile)
		if err != nil {
			_ = writer.Close()
			return SpawnResult{}, fmt.Errorf("spawn: load loop config: %w", err)
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
		}

		childCtx, childCancel := context.WithCancel(context.Background())

		child.label = params.Label
		child.agent = params.Agent
		child.model = params.Model
		child.backend = backend
		child.cancelFn = childCancel
		child.autoApprove = params.AutoApprove
		child.permClaimTimeout = s.config.PermissionClaimTimeout
		if child.permClaimTimeout == 0 {
			child.permClaimTimeout = cli.DefaultPermissionClaimTimeout
		}
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
		go s.runLoopChild(childCtx, child, cfg, params.MaxRetries, params.Agent, params.Model, params.ServerURL, params.Backend)

		return result, nil
	}

	if params.TeamFile != "" {
		cfg, err := teamrunner.LoadTeamConfig(params.TeamFile)
		if err != nil {
			_ = writer.Close()
			return SpawnResult{}, fmt.Errorf("spawn: load team config: %w", err)
		}
		if promptText != "" {
			cfg.InsertInitialPrompt(promptText)
		}

		result := SpawnResult{
			RuntimeID:    rtID,
			OnEvent:      onEvent,
			SentinelFile: sentinelFile,
			BrokerURL:    s.brokerURL(),
		}

		childCtx, childCancel := context.WithCancel(context.Background())

		child.label = params.Label
		child.agent = params.Agent
		child.model = params.Model
		child.backend = backend
		child.cancelFn = childCancel
		child.autoApprove = params.AutoApprove
		child.permClaimTimeout = s.config.PermissionClaimTimeout
		if child.permClaimTimeout == 0 {
			child.permClaimTimeout = cli.DefaultPermissionClaimTimeout
		}
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
		go s.runTeamChild(childCtx, child, cfg, params.MaxRetries, params.Agent, params.Model, params.ServerURL, params.Backend)

		return result, nil
	}

	// Start provider and session.
	startOpts := runtime.StartOptions{
		Agent:     params.Agent,
		Label:     params.Label,
		Dir:       params.Dir,
		Model:     params.Model,
		RuntimeID: rtID,
		Broker:    s.broker,
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
	provider, err := factory.NewProvider(startOpts, backend)
	if err != nil {
		_ = writer.Close()
		return SpawnResult{}, fmt.Errorf("create provider: %w", err)
	}
	session, err := cli.StartSession(context.Background(), provider, startOpts, params.SessionID)
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
	child.model = params.Model
	child.backend = backend
	child.provider = provider
	child.session = session
	child.eventWriter = writer
	child.fileHandler = fileHandler
	child.autoApprove = params.AutoApprove
	child.permClaimTimeout = s.config.PermissionClaimTimeout
	if child.permClaimTimeout == 0 {
		child.permClaimTimeout = cli.DefaultPermissionClaimTimeout
	}
	child.runID = s.runID
	child.dir = params.Dir
	child.onEvent = onEvent
	child.sentinelFile = sentinelFile

	// Initialise cancelFn in spawn so it's never nil when cancelRuntime reads it.
	childCtx, childCancel := context.WithCancel(context.Background())
	child.cancelFn = childCancel

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
		RuntimeID:    rtID,
		SessionID:    session.SessionID,
		OnEvent:      onEvent,
		SentinelFile: sentinelFile,
		BrokerURL:    brokerURL,
		ParentToken:  parentToken,
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
		close(child.done)
		s.clearRuntimePermissionOptions(child.id)
		_ = child.eventWriter.Close()
		if closer, ok := child.provider.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		child.mu.Lock()
		child.completed = true
		child.mu.Unlock()
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
	for attempt := 1; ; attempt++ {
		if attempt > 1 && resumeID == "" {
			resumeID = child.sessionID()
		}
		result := s.runChildAttempt(ctx, child, resumeID, promptText, timer)
		if result.exitCode != 1 || attempt > maxRetries {
			child.mu.Lock()
			child.exitCode = result.exitCode
			child.mu.Unlock()

			if result.exitCode == 0 {
				if child.sentinelFile != "" {
					cli.WriteSentinel(child.sentinelFile, result.exitCode, result.sessionID, runtime.StopReasonForExitCode(result.exitCode), s.runID, os.Stderr)
				}
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

			if ctx.Err() == nil {
				if nextPrompt, ok := child.dequeuePrompt(); ok {
					if child.sentinelFile != "" {
						cli.WriteSentinel(child.sentinelFile, result.exitCode, result.sessionID, runtime.StopReasonForExitCode(result.exitCode), s.runID, os.Stderr)
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
				cli.WriteSentinel(child.sentinelFile, result.exitCode, result.sessionID, runtime.StopReasonForExitCode(result.exitCode), s.runID, os.Stderr)
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

func (s *Supervisor) runLoopChild(ctx context.Context, child *childRuntime, cfg *looprunner.LoopConfig, maxRetries int, agent, model, serverURL, backend string) {
	var brokerAttemptIDs []string
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: child %s panic: %v\n", child.id, r)
			s.emitChildError(child, fmt.Sprintf("panic: %v", r), "error")
			if child.sentinelFile != "" {
				cli.WriteSentinel(child.sentinelFile, 1, "", "error", s.runID, os.Stderr)
			}
		}
		if child.cancelFn != nil {
			child.cancelFn()
		}
		if child.eventWriter != nil {
			_ = child.eventWriter.Close()
		}
		child.mu.Lock()
		child.completed = true
		child.mu.Unlock()
		close(child.done)
		s.clearRuntimePermissionOptions(child.id)
		s.controlMu.Lock()
		delete(s.runtimes, child.id)
		s.controlMu.Unlock()
		if s.broker != nil {
			for _, rid := range brokerAttemptIDs {
				s.broker.DeleteRun(rid)
			}
			s.broker.DeleteRun(child.id)
		}
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

	var opts looprunner.RunOptions
	opts = looprunner.RunOptions{
		WorkDir:    child.dir,
		RunID:      s.runID,
		EventSink:  taggedWriter,
		Config:     cfg,
		MaxRetries: maxRetries,
		Broker:     s.broker,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (looprunner.PhaseAttemptResult, error) {
			startOpts := runtime.StartOptions{
				Agent:     agent,
				Model:     model,
				Dir:       child.dir,
				ServerURL: serverURL,
				Broker:    s.broker,
			}

			resumeID := prevSessionID

			var brokerRunID string
			attemptWriter := taggedWriter
			if s.broker != nil {
				brokerRunID = broker.MakeToken()
				s.broker.CreateRun(brokerRunID)
				brokerAttemptIDs = append(brokerAttemptIDs, brokerRunID)
				attemptWriter = taggedWriter.withRecorder(broker.NewRecorder(s.broker, brokerRunID))
			}

			if opts.SeedMessage != nil && s.broker != nil && brokerRunID != "" {
				payload, _ := json.Marshal(opts.SeedMessage)
				_ = s.broker.SendTo(opts.SeedMessage.FromRunID, brokerRunID, "agent_message", payload, "")
			}

			provider, err := factory.NewProvider(startOpts, backend)
			if err != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("create provider: %w", err)
			}
			defer func() {
				if closer, ok := provider.(interface{ Close() error }); ok {
					_ = closer.Close()
				}
			}()

			session, err := cli.StartSession(ctx, provider, startOpts, resumeID)
			if err != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("start session: %w", err)
			}
			child.mu.Lock()
			child.provider = provider
			child.session = session
			child.active = true
			child.mu.Unlock()
			defer func() {
				child.mu.Lock()
				if child.provider == provider {
					child.provider = nil
					child.session = runtime.Session{}
				}
				child.active = false
				child.mu.Unlock()
			}()

			eventCtx, cancelEvents := context.WithCancel(ctx)
			defer cancelEvents()

			eventCh, err := provider.Events(eventCtx, session.SessionID)
			if err != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("subscribe events: %w", err)
			}

			promptDone := make(chan error, 1)
			go func() {
				promptDone <- provider.Prompt(context.Background(), session.SessionID, phase.Prompt)
			}()

			result := cli.WaitForSession(ctx, provider, cli.SessionWaitConfig{
				EventCh:                eventCh,
				PromptDone:             promptDone,
				SessionID:              session.SessionID,
				RunID:                  s.runID,
				RunLabel:               child.label,
				AutoApprove:            child.autoApprove,
				PermissionClaimTimeout: child.permClaimTimeout,
			}, cli.SessionWaitDeps{
				Writer:      attemptWriter,
				FileHandler: child.fileHandler,
				Stderr:      os.Stderr,
			})

			return looprunner.PhaseAttemptResult{
				ExitCode:      result.ExitCode,
				SessionID:     session.SessionID,
				LoopDirective: result.LoopDirective,
				LoopLabel:     result.LoopLabel,
				BrokerRunID:   brokerRunID,
			}, nil
		},
	}

	result, err := looprunner.Run(ctx, opts)
	if err != nil {
		child.mu.Lock()
		child.exitCode = 1
		child.mu.Unlock()
		if child.sentinelFile != "" {
			cli.WriteSentinel(child.sentinelFile, 1, "", "error", s.runID, os.Stderr)
		}
		return
	}

	child.mu.Lock()
	child.exitCode = result.ExitCode
	child.mu.Unlock()

	if child.sentinelFile != "" {
		if result.Reason != "" {
			cli.WriteSentinelWithReason(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, result.Reason, os.Stderr)
		} else {
			cli.WriteSentinel(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, os.Stderr)
		}
	}
}

func (s *Supervisor) runTeamChild(ctx context.Context, child *childRuntime, cfg *teamrunner.TeamConfig, maxRetries int, agent, model, serverURL, backend string) {
	var brokerAttemptIDs []string
	var brokerAttemptIDsMu sync.Mutex
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: child %s panic: %v\n", child.id, r)
			s.emitChildError(child, fmt.Sprintf("panic: %v", r), "error")
			if child.sentinelFile != "" {
				cli.WriteSentinel(child.sentinelFile, 1, "", "error", s.runID, os.Stderr)
			}
		}
		if child.cancelFn != nil {
			child.cancelFn()
		}
		if child.eventWriter != nil {
			_ = child.eventWriter.Close()
		}
		child.mu.Lock()
		child.completed = true
		child.mu.Unlock()
		close(child.done)
		s.clearRuntimePermissionOptions(child.id)
		s.controlMu.Lock()
		delete(s.runtimes, child.id)
		s.controlMu.Unlock()
		if s.broker != nil {
			for _, rid := range brokerAttemptIDs {
				s.broker.DeleteRun(rid)
			}
			s.broker.DeleteRun(child.id)
		}
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

	var opts teamrunner.RunOptions
	opts = teamrunner.RunOptions{
		WorkDir:    child.dir,
		RunID:      s.runID,
		EventSink:  taggedWriter,
		Config:     cfg,
		MaxRetries: maxRetries,
		Broker:     s.broker,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (teamrunner.PhaseAttemptResult, error) {
			a := agent
			m := model
			if phase.Agent != "" {
				a = phase.Agent
			}
			if phase.Model != "" {
				m = phase.Model
			}
			startOpts := runtime.StartOptions{
				Agent:     a,
				Model:     m,
				Dir:       child.dir,
				ServerURL: serverURL,
				Broker:    s.broker,
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

			if opts.SeedMessage != nil && s.broker != nil && brokerRunID != "" {
				payload, _ := json.Marshal(opts.SeedMessage)
				_ = s.broker.SendTo(opts.SeedMessage.FromRunID, brokerRunID, "agent_message", payload, "")
			}

			provider, err := factory.NewProvider(startOpts, backend)
			if err != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("create provider: %w", err)
			}
			defer func() {
				if closer, ok := provider.(interface{ Close() error }); ok {
					_ = closer.Close()
				}
			}()

			session, err := cli.StartSession(ctx, provider, startOpts, resumeID)
			if err != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("start session: %w", err)
			}
			child.mu.Lock()
			child.provider = provider
			child.session = session
			child.active = true
			child.mu.Unlock()
			defer func() {
				child.mu.Lock()
				if child.provider == provider {
					child.provider = nil
					child.session = runtime.Session{}
				}
				child.active = false
				child.mu.Unlock()
			}()

			eventCtx, cancelEvents := context.WithCancel(ctx)
			defer cancelEvents()

			eventCh, err := provider.Events(eventCtx, session.SessionID)
			if err != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1, BrokerRunID: brokerRunID}, fmt.Errorf("subscribe events: %w", err)
			}

			promptDone := make(chan error, 1)
			go func() {
				promptDone <- provider.Prompt(context.Background(), session.SessionID, phase.Prompt)
			}()

			result := cli.WaitForSession(ctx, provider, cli.SessionWaitConfig{
				EventCh:                eventCh,
				PromptDone:             promptDone,
				SessionID:              session.SessionID,
				RunID:                  s.runID,
				RunLabel:               child.label,
				AutoApprove:            child.autoApprove,
				PermissionClaimTimeout: child.permClaimTimeout,
			}, cli.SessionWaitDeps{
				Writer:      attemptWriter,
				FileHandler: child.fileHandler,
				Stderr:      os.Stderr,
			})

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
	if err != nil {
		child.mu.Lock()
		child.exitCode = 1
		child.mu.Unlock()
		if child.sentinelFile != "" {
			cli.WriteSentinel(child.sentinelFile, 1, "", "error", s.runID, os.Stderr)
		}
		return
	}

	child.mu.Lock()
	child.exitCode = result.ExitCode
	child.mu.Unlock()

	if child.sentinelFile != "" {
		if result.Reason != "" {
			cli.WriteSentinelWithReason(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, result.Reason, os.Stderr)
		} else {
			cli.WriteSentinel(child.sentinelFile, result.ExitCode, result.SessionID, result.StopReason, s.runID, os.Stderr)
		}
	}
}

type childAttemptResult struct {
	exitCode  int
	sessionID string
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

func (s *Supervisor) runChildAttempt(ctx context.Context, child *childRuntime, resumeID, promptText string, timer <-chan time.Time) childAttemptResult {
	session, err := s.attemptSession(ctx, child, resumeID)
	if err != nil {
		s.emitChildError(child, fmt.Sprintf("start session: %v", err), "error")
		return childAttemptResult{exitCode: 1}
	}
	child.mu.Lock()
	child.session = session
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
		child.mu.Unlock()
	}()

	eventCtx, cancelEvents := context.WithCancel(turnCtx)
	defer cancelEvents()

	eventCh, err := child.provider.Events(eventCtx, session.SessionID)
	if err != nil {
		s.emitChildError(child, fmt.Sprintf("subscribe events: %v", err), "error")
		return childAttemptResult{exitCode: 1, sessionID: session.SessionID}
	}

	promptDone := make(chan error, 1)
	go func() {
		defer func() { recover() }()
		promptDone <- child.provider.Prompt(turnCtx, session.SessionID, promptText)
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
	result := cli.WaitForSession(turnCtx, child.provider, cli.SessionWaitConfig{
		EventCh:                eventCh,
		PromptDone:             promptDone,
		SessionID:              session.SessionID,
		RunID:                  s.runID,
		RunLabel:               child.label,
		PermissionClaimScope:   child.id,
		AutoApprove:            child.autoApprove,
		PermissionClaimTimeout: child.permClaimTimeout,
		Timeout:                timer,
	}, cli.SessionWaitDeps{
		Writer:      taggedWriter,
		FileHandler: child.fileHandler,
		Stderr:      os.Stderr,
	})
	exitCode := result.ExitCode
	return childAttemptResult{exitCode: exitCode, sessionID: session.SessionID}
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
	return cli.StartSession(ctx, child.provider, runtime.StartOptions{
		Agent: "",
		Label: child.label,
		Dir:   child.dir,
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
	s.shutdownManagedHTTPServers()

	s.controlMu.Lock()
	runtimes := make([]*childRuntime, 0, len(s.runtimes))
	for _, rt := range s.runtimes {
		runtimes = append(runtimes, rt)
	}
	s.controlMu.Unlock()

	for _, rt := range runtimes {
		if rt.cancelFn != nil {
			rt.cancelFn()
		}
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
	if s.broker != nil {
		_ = s.broker.Stop()
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
		entry := map[string]any{
			"runtime_id":         rt.id,
			"session_id":         rt.session.SessionID,
			"run_id":             rt.runID,
			"label":              rt.label,
			"agent":              rt.agent,
			"model":              rt.model,
			"backend":            rt.backend,
			"dir":                rt.dir,
			"status":             status,
			"exit_code":          rt.exitCode,
			"on_event":           rt.onEvent,
			"event_path":         rt.onEvent,
			"sentinel_file":      rt.sentinelFile,
			"parent_id":          rt.parentID,
			"children":           rt.children,
			"phase":              rt.phase,
			"phase_label":        rt.phaseLabel,
			"pending_permission": rt.pendingPermission,
			"latest_seq":         rt.latestSeq,
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
	if rt.cancelFn != nil {
		rt.cancelFn()
	}
	return nil
}

func (s *Supervisor) answerPermission(rtID, requestID, optionID string) error {
	s.controlMu.Lock()
	rt := s.runtimes[rtID]
	s.controlMu.Unlock()
	if rt == nil {
		return fmt.Errorf("runtime %q not found", rtID)
	}

	s.controlMu.Lock()
	key := rtID + ":" + requestID
	options := s.permOptions[key]
	s.controlMu.Unlock()
	if options == nil {
		return fmt.Errorf("permission request %q not found for runtime %q", requestID, rtID)
	}

	kind := ""
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
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("unknown option_id %q for request %q on runtime %q", optionID, requestID, rtID)
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
	// directly here leaves the resolver stuck until its timeout and causes the
	// next backend permission to fail as an overlapping request.
	switch s.control.DeliverPendingPermission(rtID, requestID, optionID) {
	case control.PermissionAnswerDelivered:
		s.controlMu.Lock()
		delete(s.permOptions, key)
		s.controlMu.Unlock()
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
	}

	// A request emitted without any configured resolver remains backend-owned.
	rt.mu.Lock()
	provider := rt.provider
	sessionID := rt.session.SessionID
	rt.mu.Unlock()
	if provider == nil || sessionID == "" {
		s.control.RetryDirectPermissionDelivery(rtID, requestID)
		return fmt.Errorf("runtime %q has no active session for permission response", rtID)
	}
	if err := provider.AnswerPermission(context.Background(), sessionID, requestID, runtime.PermissionResponse{
		Allow:    kind == "allow",
		OptionID: optionID,
	}); err != nil {
		s.control.RetryDirectPermissionDelivery(rtID, requestID)
		return err
	}
	s.cleanupDirectPermission(rtID, requestID, nil)
	return nil
}

// cleanupDirectPermission removes stale option metadata before releasing the
// claim for reuse. controlMu remains held across EndPermissionClaim so a
// replacement cannot publish new options between those operations.
func (s *Supervisor) cleanupDirectPermission(runtimeID, requestID string, beforeRelease func()) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	delete(s.permOptions, runtimeID+":"+requestID)
	if beforeRelease != nil {
		beforeRelease()
	}
	s.control.EndPermissionClaim(runtimeID, requestID)
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
	s.control.ClearPermissionClaims(runtimeID)
}

type runtimeFanoutWriter struct {
	base            cli.EventSink
	runtimeID       string
	child           *childRuntime
	control         *control.ControlServer
	metadata        *cli.EventMetadata
	onPermissionReq func(runtimeID, requestID string, options []any)
	recorder        *broker.Recorder
}

func (w *runtimeFanoutWriter) Write(ev events.Event) error {
	if w.child != nil {
		w.child.writeMu.Lock()
		defer w.child.writeMu.Unlock()
	}
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
	stamped = events.BoundFinalOutput(stamped)
	if stamped.Event == "permission.request" && w.onPermissionReq != nil {
		if requestID, _ := stamped.Fields["request_id"].(string); requestID != "" {
			if options, _ := stamped.Fields["options"].([]any); options != nil {
				w.onPermissionReq(w.runtimeID, requestID, options)
			}
		}
	}
	// Mirror phase, phase_label, pending_permission, usage, final_output, and
	// latest_seq onto the child so RuntimeStatus can report them per-runtime.
	if w.child != nil {
		w.child.mu.Lock()
		if seq, ok := events.Int64(stamped.Fields["seq"]); ok {
			w.child.latestSeq = seq
		}
		if usage, ok := stamped.Fields["usage"].(map[string]any); ok {
			w.child.usage = make(map[string]any, len(usage))
			for k, v := range usage {
				w.child.usage[k] = v
			}
		}
		switch stamped.Event {
		case "agent.status":
			if phase, _ := stamped.Fields["phase"].(string); phase != "" && w.child.phase != "done" {
				w.child.phase = phase
				if label, _ := stamped.Fields["label"].(string); label != "" {
					w.child.phaseLabel = label
				}
			}
		case "permission.request":
			w.child.pendingPermission = true
			w.child.permission = map[string]any{}
			for k, v := range stamped.Fields {
				if k == "runtime_id" || k == "run_id" || k == "run_label" || k == "ts" || k == "seq" {
					continue
				}
				w.child.permission[k] = v
			}
		case "permission.response":
			w.child.pendingPermission = false
			w.child.permission = nil
		case "session.end":
			w.child.phase = "done"
			if finalOutput, _ := stamped.Fields["final_output"].(string); finalOutput != "" {
				w.child.finalOutput = finalOutput
			}
		}
		w.child.mu.Unlock()
	}
	rec := w.recorder
	if rec != nil {
		rec.Feed(stamped)
	}
	if err := w.base.Write(stamped); err != nil {
		return err
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
	select {
	case <-s.shutdownCh:
	default:
		close(s.shutdownCh)
	}
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
	entry := map[string]any{
		"runtime_id":         rt.id,
		"session_id":         rt.session.SessionID,
		"run_id":             rt.runID,
		"label":              rt.label,
		"agent":              rt.agent,
		"model":              rt.model,
		"backend":            rt.backend,
		"dir":                rt.dir,
		"status":             status,
		"exit_code":          rt.exitCode,
		"on_event":           rt.onEvent,
		"event_path":         rt.onEvent,
		"sentinel_file":      rt.sentinelFile,
		"parent_id":          rt.parentID,
		"children":           rt.children,
		"phase":              rt.phase,
		"phase_label":        rt.phaseLabel,
		"pending_permission": rt.pendingPermission,
		"latest_seq":         rt.latestSeq,
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
	rt.mu.Unlock()
	return entry, nil
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

func (s *Supervisor) RuntimeAnswerPermission(rtID, requestID, optionID string) error {
	return s.answerPermission(rtID, requestID, optionID)
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
