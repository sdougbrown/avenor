package stable

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/factory"
)

type Config struct {
	ControlSocket          string
	TombstoneFile          string
	HTTPDebug              string
	MaxRuntimes            int
	IdleTimeout            time.Duration
	ShutdownTimeout        time.Duration
	PermissionClaimTimeout time.Duration
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
}

type SpawnResult struct {
	RuntimeID    string `json:"runtime_id"`
	SessionID    string `json:"session_id"`
	OnEvent      string `json:"on_event"`
	SentinelFile string `json:"sentinel_file"`
}

type childRuntime struct {
	id               string
	label            string
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
	mu               sync.Mutex
}

type Supervisor struct {
	config          Config
	runID           string
	control         *control.ControlServer
	state           *control.ControlState
	controlMu       sync.Mutex
	runtimes        map[string]*childRuntime
	nextID          int
	shutdownCh      chan struct{}
	runtimeActivity chan struct{}
	httpServer      *control.HTTPDebugServer
	permOptions     map[string][]any // keyed by "runtimeID:requestID"
}

func NewSupervisor(cfg Config) *Supervisor {
	runID := cli.GenerateRunID()
	state := control.NewState(runID, "", 0)
	sup := &Supervisor{
		config:          cfg,
		runID:           runID,
		state:           state,
		control:         control.NewServer(state),
		runtimes:        map[string]*childRuntime{},
		shutdownCh:      make(chan struct{}),
		runtimeActivity: make(chan struct{}),
		permOptions:     map[string][]any{},
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

	promptText := params.Prompt
	if promptText == "" && params.PromptFile != "" {
		data, err := os.ReadFile(params.PromptFile)
		if err != nil {
			return SpawnResult{}, fmt.Errorf("read prompt file: %w", err)
		}
		promptText = string(data)
	}
	if promptText == "" && params.LoopFile == "" {
		return SpawnResult{}, fmt.Errorf("prompt, prompt_file, or loop_file is required")
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
		}

		childCtx, childCancel := context.WithCancel(context.Background())

		child.label = params.Label
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

	// Start provider and session.
	startOpts := runtime.StartOptions{
		Agent: params.Agent,
		Label: params.Label,
		Dir:   params.Dir,
		Model: params.Model,
	}
	discovery := cli.DiscoverServer(params.ServerURL, os.Getenv)
	startOpts.ServerURL = discovery.URL

	backend := params.Backend
	if backend == "" {
		backend = "opencode-acp"
	}
	if backend == "opencode-http" && startOpts.ServerURL == "" {
		_ = writer.Close()
		return SpawnResult{}, fmt.Errorf("--server-url is required for backend opencode-http")
	}
	provider, err := factory.NewProvider(startOpts, backend)
	if err != nil {
		_ = writer.Close()
		return SpawnResult{}, fmt.Errorf("create provider: %w", err)
	}
	session, err := cli.StartSession(context.Background(), provider, startOpts, "")
	if err != nil {
		if closer, ok := provider.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		_ = writer.Close()
		return SpawnResult{}, fmt.Errorf("start session: %w", err)
	}

	// Populate child with fully-initialised state.
	child.label = params.Label
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

	// Start the child event loop in a goroutine.
	releaseReservation = nil
	go s.runChild(childCtx, child, promptText, params.Timeout, params.MaxRetries)

	select {
	case s.runtimeActivity <- struct{}{}:
	default:
	}

	return SpawnResult{
		RuntimeID:    rtID,
		SessionID:    session.SessionID,
		OnEvent:      onEvent,
		SentinelFile: sentinelFile,
	}, nil
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
	}()
	defer func() {
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
	}()

	taggedWriter := &runtimeFanoutWriter{
		base:            child.eventWriter,
		runtimeID:       child.id,
		control:         s.control,
		onPermissionReq: s.cachePermissionOptions,
	}

	opts := looprunner.RunOptions{
		WorkDir:    child.dir,
		RunID:      s.runID,
		EventSink:  taggedWriter,
		Config:     cfg,
		MaxRetries: maxRetries,
		PhaseAttempt: func(ctx context.Context, phase looprunner.Phase, attemptNum int, iteration int, prevSessionID string) (looprunner.PhaseAttemptResult, error) {
			startOpts := runtime.StartOptions{
				Agent:     agent,
				Model:     model,
				Dir:       child.dir,
				ServerURL: serverURL,
			}

			var resumeID string
			if prevSessionID != "" {
				resumeID = prevSessionID
			}

			provider, err := factory.NewProvider(startOpts, backend)
			if err != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1}, fmt.Errorf("create provider: %w", err)
			}
			defer func() {
				if closer, ok := provider.(interface{ Close() error }); ok {
					_ = closer.Close()
				}
			}()

			session, err := cli.StartSession(ctx, provider, startOpts, resumeID)
			if err != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1}, fmt.Errorf("start session: %w", err)
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
				return looprunner.PhaseAttemptResult{ExitCode: 1}, fmt.Errorf("subscribe events: %w", err)
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
				Writer:      taggedWriter,
				FileHandler: child.fileHandler,
				Stderr:      os.Stderr,
			})

			return looprunner.PhaseAttemptResult{
				ExitCode:      result.ExitCode,
				SessionID:     session.SessionID,
				LoopDirective: result.LoopDirective,
				LoopLabel:     result.LoopLabel,
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
		control:         s.control,
		onPermissionReq: s.cachePermissionOptions,
	}
	result := cli.WaitForSession(turnCtx, child.provider, cli.SessionWaitConfig{
		EventCh:                eventCh,
		PromptDone:             promptDone,
		SessionID:              session.SessionID,
		RunID:                  s.runID,
		RunLabel:               child.label,
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

func (s *Supervisor) emitSessionEnd(child *childRuntime, exitCode int, stopReason string) {
	s.control.PublishEvent(events.Event{
		Event:     "session.end",
		SessionID: child.sessionID(),
		Fields: map[string]any{
			"stop_reason": stopReason,
			"runtime_id":  child.id,
			"exit_code":   exitCode,
			"ts":          time.Now().UnixMilli(),
		},
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
			"runtime_id":    rt.id,
			"session_id":    rt.session.SessionID,
			"label":         rt.label,
			"dir":           rt.dir,
			"status":        status,
			"exit_code":     rt.exitCode,
			"on_event":      rt.onEvent,
			"sentinel_file": rt.sentinelFile,
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
			kind = strings.ToLower(k)
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

	rt.mu.Lock()
	provider := rt.provider
	sessionID := rt.session.SessionID
	rt.mu.Unlock()
	if provider == nil || sessionID == "" {
		return fmt.Errorf("runtime %q has no active session for permission response", rtID)
	}
	if err := provider.AnswerPermission(context.Background(), sessionID, requestID, runtime.PermissionResponse{
		Allow:    kind == "allow",
		OptionID: optionID,
	}); err != nil {
		return err
	}
	s.controlMu.Lock()
	delete(s.permOptions, key)
	s.controlMu.Unlock()
	return nil
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
}

type runtimeFanoutWriter struct {
	base            cli.EventSink
	runtimeID       string
	control         *control.ControlServer
	onPermissionReq func(runtimeID, requestID string, options []any)
}

func (w *runtimeFanoutWriter) Write(ev events.Event) error {
	if ev.Fields == nil {
		ev.Fields = map[string]any{}
	}
	ev.Fields["runtime_id"] = w.runtimeID
	if ev.Event == "permission.request" && w.onPermissionReq != nil {
		if requestID, _ := ev.Fields["request_id"].(string); requestID != "" {
			if options, _ := ev.Fields["options"].([]any); options != nil {
				w.onPermissionReq(w.runtimeID, requestID, options)
			}
		}
	}
	if w.control != nil {
		w.control.PublishEvent(ev)
	}
	return w.base.Write(ev)
}

func (w *runtimeFanoutWriter) Close() error { return w.base.Close() }

// StableHandler implementation.

func (s *Supervisor) Spawn(raw json.RawMessage) (any, error) {
	var p SpawnParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("invalid spawn params: %w", err)
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
		"runtime_id":    rt.id,
		"session_id":    rt.session.SessionID,
		"label":         rt.label,
		"dir":           rt.dir,
		"status":        status,
		"exit_code":     rt.exitCode,
		"on_event":      rt.onEvent,
		"sentinel_file": rt.sentinelFile,
	}
	rt.mu.Unlock()
	return entry, nil
}

func (s *Supervisor) RuntimeCancel(rtID string) error {
	return s.cancelRuntime(rtID)
}

func (s *Supervisor) RuntimePrompt(rtID, text string) error {
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
