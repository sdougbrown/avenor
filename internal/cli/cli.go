package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/digest"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime"
)

const (
	DefaultBackend        = "opencode-http"
	backendOpenCodeACP    = "opencode-acp"
	backendOpenCodeHTTP   = "opencode-http"
	backendCodexAppServer = "codex-app-server"
)

// DefaultPermissionClaimTimeout is the default value for --permission-claim-timeout:
// how long resolvePermission will wait for a connected socket client to send
// answer_permission before falling through to the file-handler (or the
// no-resolver path). This bounds the TOCTOU window where HasClients() was true
// at the gate but the client disconnects or goes silent before answering.
const DefaultPermissionClaimTimeout = 30 * time.Second

type ServerDiscovery struct {
	URL    string
	Source string
	Mode   string
}

func DiscoverServer(flagURL string, getenv func(string) string) ServerDiscovery {
	if strings.TrimSpace(flagURL) != "" {
		return ServerDiscovery{URL: flagURL, Source: "flag", Mode: "external"}
	}
	if getenv != nil {
		if envURL := strings.TrimSpace(getenv("AVENOR_OPENCODE_URL")); envURL != "" {
			return ServerDiscovery{URL: envURL, Source: "env", Mode: "external"}
		}
	}
	return ServerDiscovery{Source: "subprocess", Mode: "subprocess"}
}

func OverrideStopReason(stopReason string, cancelled bool, timedOut bool) string {
	switch {
	case timedOut:
		return "timeout"
	case cancelled:
		return "cancelled"
	default:
		return stopReason
	}
}

func Run(args []string) int {
	return run(args, os.Getenv, os.Stderr)
}

func run(args []string, getenv func(string) string, stderr io.Writer) int {
	fs := flag.NewFlagSet("avenor", flag.ContinueOnError)
	fs.SetOutput(stderr)

	agent := fs.String("agent", "", "agent name")
	label := fs.String("label", "", "free-form label for log correlation")
	promptFile := fs.String("prompt-file", "", "path to prompt file (mutually exclusive with --prompt)")
	prompt := fs.String("prompt", "", "inline prompt text (mutually exclusive with --prompt-file)")
	dir := fs.String("dir", ".", "working directory for the agent")
	resume := fs.String("resume", "", "resume an existing session id")
	serverURL := fs.String("server-url", "", "long-lived ACP server endpoint")
	onEvent := fs.String("on-event", "", "path to write NDJSON events (optional; events are discarded if unset)")
	permissionHandler := fs.String("permission-handler", "", "permission handler, supports file:<path>")
	sentinelFile := fs.String("sentinel-file", "", "path to write a completion sentinel (also derives permission base unless --permission-handler is set)")
	timeout := fs.Duration("timeout", 0, "overall session timeout")
	progressTimeout := fs.Duration("progress-timeout", 0, "session progress timeout (fires if no event for duration)")
	model := fs.String("model", "", "backend-specific model id")
	backend := fs.String("backend", DefaultBackend, "runtime backend")
	runIDFlag := fs.String("run-id", "", "correlation id for this run (generated if not set)")
	maxRetries := fs.Int("max-retries", 0, "maximum retry attempts on transient failure (0 = no retry)")
	autoApprove := fs.Bool("auto-approve", false, "automatically approve all permission requests")
	controlSocket := fs.String("control-socket", "", "unix socket path for control plane")
	httpDebug := fs.String("http-debug", "", "http debug adapter bind address")
	permClaimTimeout := fs.Duration("permission-claim-timeout", 0, fmt.Sprintf("how long to wait for a connected socket client to answer a permission request before falling through to the file handler or 'none' resolver (0 uses the default: %v)", DefaultPermissionClaimTimeout))
	loopFile := fs.String("loop-file", "", "path to loop config JSON (optional; enables multi-phase mode)")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if *permClaimTimeout == 0 {
		*permClaimTimeout = DefaultPermissionClaimTimeout
	}

	runID := *runIDFlag
	if runID == "" {
		runID = GenerateRunID()
	}

	// finalSessionID is updated after each attempt so exitWithSentinel always
	// writes the most recent session ID regardless of which return path fires.
	var finalSessionID string
	var finalStopReason string

	exitWithSentinel := func(code int) int {
		if *sentinelFile != "" {
			sr := finalStopReason
			if sr == "" {
				sr = runtime.StopReasonForExitCode(code)
			}
			WriteSentinel(*sentinelFile, code, finalSessionID, sr, runID, stderr)
		}
		return code
	}

	discovery := DiscoverServer(*serverURL, getenv)
	switch *backend {
	case backendOpenCodeACP:
	case backendOpenCodeHTTP:
		if discovery.URL == "" {
			fmt.Fprintf(stderr, "avenor: --server-url is required for backend opencode-http\n")
			return exitWithSentinel(1)
		}
	case backendCodexAppServer:
	default:
		fmt.Fprintf(stderr, "avenor: unknown backend %q\n", *backend)
		return exitWithSentinel(1)
	}
	if *prompt != "" && *promptFile != "" {
		fmt.Fprintln(stderr, "avenor: --prompt and --prompt-file are mutually exclusive")
		return exitWithSentinel(1)
	}
	if *prompt == "" && *promptFile == "" && *loopFile == "" {
		fmt.Fprintln(stderr, "avenor: --prompt, --prompt-file, or --loop-file is required")
		return exitWithSentinel(1)
	}
	if *loopFile != "" && *resume != "" {
		fmt.Fprintln(stderr, "avenor: --loop-file and --resume are mutually exclusive")
		return exitWithSentinel(1)
	}

	effectivePermHandler := effectivePermissionHandler(*sentinelFile, *permissionHandler, *autoApprove)
	cleanupPermBase := permissionCleanupBase(*sentinelFile, *permissionHandler)

	// Pre-run cleanup: truncate event log and remove stale sentinel/perm files.
	// Only when --sentinel-file is active to avoid changing behavior for callers
	// that don't use it. The event log is recreated below via newEventWriter.
	// Use the cleanup permission base (derived or explicit) for cleanup.
	if *sentinelFile != "" {
		cleanupSentinelFiles(*sentinelFile, cleanupPermBase, stderr)
	}

	var promptText []byte
	if *prompt != "" {
		promptText = []byte(*prompt)
	} else if *promptFile != "" {
		p, err := os.ReadFile(*promptFile)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: read prompt file: %v\n", err)
			return exitWithSentinel(1)
		}
		promptText = p
	}

	baseWriter, err := NewEventWriter(*onEvent)
	if err != nil {
		fmt.Fprintf(stderr, "avenor: open event stream: %v\n", err)
		return exitWithSentinel(1)
	}
	var writer EventSink = baseWriter
	defer writer.Close()

	state := control.NewState(runID, *label, *maxRetries)
	var controlServer *control.ControlServer
	var httpServer *control.HTTPDebugServer
	if *controlSocket != "" {
		controlServer = control.NewServer(state)
		if err := controlServer.Start(*controlSocket); err != nil {
			fmt.Fprintf(stderr, "avenor: start control server: %v\n", err)
			return exitWithSentinel(1)
		}
		defer controlServer.Stop()
		writer = newFanoutWriter(writer, controlServer)
	}
	if *httpDebug != "" {
		if controlServer == nil {
			controlServer = control.NewServer(state)
			writer = newFanoutWriter(writer, controlServer)
		}
		httpServer, err = control.NewHTTPDebugServer(*httpDebug, controlServer)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: start http debug server: %v\n", err)
			return exitWithSentinel(1)
		}
		if err := httpServer.Start(); err != nil {
			fmt.Fprintf(stderr, "avenor: start http debug server: %v\n", err)
			return exitWithSentinel(1)
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = httpServer.Stop(shutdownCtx)
		}()
	}

	var fileHandler *permission.FileHandler
	if effectivePermHandler != "" {
		fileHandler, err = ParsePermissionHandler(effectivePermHandler)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: %v\n", err)
			return exitWithSentinel(1)
		}
	}

	startOptions := runtime.StartOptions{
		Agent:     *agent,
		Label:     *label,
		Dir:       *dir,
		ServerURL: discovery.URL,
		Model:     *model,
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stopSignals()
	if controlServer != nil {
		controlServer.SetCancelFunc(stopSignals)
	}

	var timer <-chan time.Time
	if *timeout > 0 {
		t := time.NewTimer(*timeout)
		defer t.Stop()
		timer = t.C
	}

	if *loopFile != "" {
		cfg, err := looprunner.LoadLoopConfig(*loopFile)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: load loop config: %v\n", err)
			if *sentinelFile != "" {
				WriteSentinel(*sentinelFile, 1, finalSessionID, "error", runID, stderr)
			}
			return 1
		}

		if len(promptText) > 0 {
			cfg.InsertInitialPrompt(string(promptText))
		}

		opts := looprunner.RunOptions{
			WorkDir:    *dir,
			RunID:      runID,
			EventSink:  writer,
			Config:     cfg,
			MaxRetries: *maxRetries,
			PhaseAttempt: func(ctx context.Context, phase looprunner.Phase, attemptNum int, iteration int, prevSessionID string) (looprunner.PhaseAttemptResult, error) {
				startOpts := runtime.StartOptions{
					Agent:     *agent,
					Model:     *model,
					Dir:       *dir,
					ServerURL: discovery.URL,
				}

				var resumeID string
				if prevSessionID != "" {
					resumeID = prevSessionID
				}

				result := runSingleAttempt(ctx, attemptConfig{
					startOptions:           startOpts,
					backend:                *backend,
					resumeID:               resumeID,
					initialPrompt:          phase.Prompt,
					runID:                  runID,
					runLabel:               *label,
					autoApprove:            *autoApprove,
					permissionClaimTimeout: *permClaimTimeout,
					progressTimeout:        *progressTimeout,
					timer:                  timer,
				}, attemptDeps{
					writer:        writer,
					fileHandler:   fileHandler,
					controlServer: controlServer,
					stderr:        os.Stderr,
				})

				return looprunner.PhaseAttemptResult{
					ExitCode:      result.exitCode,
					SessionID:     result.sessionID,
					StopReason:    result.stopReason,
					LoopDirective: result.loopDirective,
					LoopLabel:     result.loopLabel,
				}, nil
			},
		}

		lrCtx, lrCancel := context.WithCancel(ctx)
		defer lrCancel()

		result, err := looprunner.Run(lrCtx, opts)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: loop run: %v\n", err)
			if *sentinelFile != "" {
				WriteSentinel(*sentinelFile, 1, result.SessionID, "error", runID, stderr)
			}
			return 1
		}

		writeSentinelForResult(result, *sentinelFile, runID, stderr)

		return result.ExitCode
	}

	resumeID := *resume
	var result attemptResult
	for attempt := 1; ; attempt++ {
		if controlServer != nil {
			state.Update(func(s *control.Snapshot) { s.RetryAttempt = attempt })
		}
		result = runAttempt(ctx, attemptConfig{
			startOptions:           startOptions,
			backend:                *backend,
			resumeID:               resumeID,
			initialPrompt:          string(promptText),
			runID:                  runID,
			runLabel:               *label,
			autoApprove:            *autoApprove,
			permissionClaimTimeout: *permClaimTimeout,
			progressTimeout:        *progressTimeout,
			timer:                  timer,
		}, attemptDeps{
			writer:        writer,
			fileHandler:   fileHandler,
			controlServer: controlServer,
			stderr:        stderr,
		})
		finalSessionID = result.sessionID
		finalStopReason = result.stopReason

		if result.exitCode != 1 || attempt > *maxRetries {
			break
		}

		if result.sessionID != "" {
			resumeID = result.sessionID
		}

		if err := writeRetryEvent(writer, result.sessionID, runID, attempt+1, *maxRetries, *label); err != nil {
			fmt.Fprintf(stderr, "avenor: write retry event: %v\n", err)
			result.exitCode = 1
			break
		}
		if *sentinelFile != "" {
			cleanupSentinelFiles(*sentinelFile, cleanupPermBase, stderr)
		}

		select {
		case <-retryAfter(backoffDelay(attempt)):
		case <-timer:
			return exitWithSentinel(124)
		case <-ctx.Done():
			return exitWithSentinel(130)
		}
	}

	return exitWithSentinel(result.exitCode)
}

func StartSession(ctx context.Context, provider runtime.Provider, opts runtime.StartOptions, resumeID string) (runtime.Session, error) {
	if resumeID != "" {
		return provider.Resume(ctx, resumeID)
	}
	return provider.Start(ctx, opts)
}

var runAttempt = runSingleAttempt
var retryAfter = time.After

func effectivePermissionHandler(sentinelPath, permissionHandler string, autoApprove bool) string {
	if permissionHandler != "" {
		return permissionHandler
	}
	if sentinelPath != "" && !autoApprove {
		return "file:" + DerivePermBase(sentinelPath)
	}
	return ""
}

func permissionCleanupBase(sentinelPath, permissionHandler string) string {
	const filePrefix = "file:"
	if strings.HasPrefix(permissionHandler, filePrefix) {
		return strings.TrimPrefix(permissionHandler, filePrefix)
	}
	if sentinelPath != "" && permissionHandler == "" {
		return DerivePermBase(sentinelPath)
	}
	return ""
}

func writeSentinelForResult(result looprunner.RunResult, sentinelFile, runID string, stderr io.Writer) {
	if sentinelFile == "" {
		return
	}
	if result.Reason != "" {
		WriteSentinelWithReason(sentinelFile, result.ExitCode, result.SessionID, result.StopReason, runID, result.Reason, stderr)
	} else {
		WriteSentinel(sentinelFile, result.ExitCode, result.SessionID, result.StopReason, runID, stderr)
	}
}

// permissionResult carries the outcome of an async permission resolution.
type permissionResult struct {
	err       error
	requestID string
	optionID  string
	kind      string
	cancelled bool
	// source is "avenor", "control", or "file".
	source string
}

// sessionResult is the return value of WaitForSession, carrying both the exit
// code and any loop directive that was detected during the session.
type sessionResult struct {
	ExitCode      int
	StopReason    string
	LoopDirective string
	LoopLabel     string
	Usage         map[string]any
}

func loopDirectiveSeverity(d string) int {
	switch d {
	case "abort":
		return 3
	case "exit":
		return 2
	case "continue":
		return 1
	default:
		return 0
	}
}

type SessionWaitConfig struct {
	EventCh                <-chan events.Event
	PromptDone             <-chan error
	InterruptCh            <-chan struct{}
	SessionID              string
	RunID                  string
	RunLabel               string
	AutoApprove            bool
	PermissionClaimTimeout time.Duration
	ProgressTimeout        time.Duration
	Timeout                <-chan time.Time
}

type SessionWaitDeps struct {
	Writer        EventSink
	FileHandler   *permission.FileHandler
	ControlServer *control.ControlServer
	Stderr        io.Writer
}

func WaitForSession(ctx context.Context, provider runtime.Provider, cfg SessionWaitConfig, deps SessionWaitDeps) sessionResult {
	var finalStopReason string
	var bufferedUsage map[string]any
	promptReturned := false
	var permissionDone <-chan permissionResult
	var loopDirective string
	var loopLabel string
	eventChClosed := false
	tracker := newStatusTracker(cfg.SessionID, cfg.RunID, cfg.RunLabel)

	chunkBuf := digest.NewChunkBuffer()

	var progressTimerC <-chan time.Time
	var progressTimer *time.Timer
	if cfg.ProgressTimeout > 0 {
		progressTimer = time.NewTimer(cfg.ProgressTimeout)
		defer progressTimer.Stop()
		progressTimerC = progressTimer.C
	}

	writeStatus := func(ev events.Event, ok bool) bool {
		if !ok {
			return true
		}
		if err := deps.Writer.Write(ev); err != nil {
			fmt.Fprintf(deps.Stderr, "avenor: write event: %v\n", err)
			return false
		}
		return true
	}

	emitPermissionResponse := func(requestID, optionID, kind, source string) {
		fields := map[string]any{
			"request_id": requestID,
			"option_id":  optionID,
			"kind":       kind,
			"source":     source,
			"ts":         time.Now().UnixMilli(),
		}
		if cfg.RunID != "" {
			fields["run_id"] = cfg.RunID
		}
		if cfg.RunLabel != "" {
			fields["run_label"] = cfg.RunLabel
		}
		if err := deps.Writer.Write(events.Event{
			Event:     "permission.response",
			SessionID: cfg.SessionID,
			Fields:    fields,
		}); err != nil {
			fmt.Fprintf(deps.Stderr, "avenor: write permission.response event: %v\n", err)
		}
	}

	for {
		select {
		case event, ok := <-cfg.EventCh:
			if !ok {
				// Nil the channel so it no longer fires on subsequent iterations.
				cfg.EventCh = nil
				eventChClosed = true
				// Wait for any in-flight AnswerPermission goroutine before exiting.
				if permissionDone != nil {
					break
				}
				if finalStopReason == "" {
					return sessionResult{ExitCode: 1}
				}
				return sessionResult{ExitCode: runtime.ExitCodeForStopReason(finalStopReason), LoopDirective: loopDirective, LoopLabel: loopLabel, Usage: bufferedUsage}
			} else if progressTimer != nil {
				if !progressTimer.Stop() {
					select {
					case <-progressTimer.C:
					default:
					}
				}
				progressTimer.Reset(cfg.ProgressTimeout)
			}
			if usage, ok := event.Fields["usage"]; ok {
				if usageMap, ok := usage.(map[string]any); ok {
					bufferedUsage = usageMap
				}
			}
			markerHandled := false
			if event.Event == "agent.message_chunk" || event.Event == "agent.thought_chunk" {
				if text := chunkText(event); text != "" {
					chunkBuf.Append(text)
					if phase, label, ok := chunkBuf.ScanStatusMarker(); ok {
						if !writeStatus(tracker.ObserveMarker(phase, label)) {
							return sessionResult{ExitCode: 1}
						}
						markerHandled = true
					}
					// Loop markers are intentionally left out of markerHandled.
					// The raw event still reaches tracker.Observe so the caller
					// sees the full message text including the marker.
					if dir, lbl, ok := chunkBuf.ScanLoopMarker(); ok {
						if loopDirectiveSeverity(dir) > loopDirectiveSeverity(loopDirective) {
							loopDirective = dir
							loopLabel = lbl
						}
					}
				}
			}
			if !markerHandled {
				if !writeStatus(tracker.Observe(event)) {
					return sessionResult{ExitCode: 1}
				}
			}
			if event.Event == "permission.request" {
				// Auto-answer / control-owner / file fallback resolver.
				if err := deps.Writer.Write(event); err != nil {
					fmt.Fprintf(deps.Stderr, "avenor: write event: %v\n", err)
					return sessionResult{ExitCode: 1}
				}
				requestID, _ := event.Fields["request_id"].(string)
				if requestID == "" {
					// Malformed request from backend: no request_id means we cannot
					// call AnswerPermission. Emit an error and stay in waiting state.
					emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "permission", "permission.request missing request_id", deps.Stderr, cfg.RunLabel)
					continue
				}
				if permissionDone != nil {
					emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "permission", "another permission request is already pending", deps.Stderr, cfg.RunLabel)
					return sessionResult{ExitCode: 1}
				}
				done := make(chan permissionResult, 1)
				permissionDone = done
				go func() {
					res := resolvePermission(ctx, provider, deps.FileHandler, deps.ControlServer, event, cfg.SessionID, requestID, cfg.AutoApprove, cfg.PermissionClaimTimeout, deps.Writer.Write)
					done <- res
				}()
				continue
			}
			if event.Event == "session.end" {
				finalStopReason, _ = event.Fields["stop_reason"].(string)
			}
			if err := deps.Writer.Write(event); err != nil {
				fmt.Fprintf(deps.Stderr, "avenor: write event: %v\n", err)
				return sessionResult{ExitCode: 1}
			}
			if event.Event == "session.end" && promptReturned && permissionDone == nil {
				return sessionResult{ExitCode: runtime.ExitCodeForStopReason(finalStopReason), LoopDirective: loopDirective, LoopLabel: loopLabel, Usage: bufferedUsage}
			}
		case err := <-cfg.PromptDone:
			promptReturned = true
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return cancelAndEnd(provider, deps.Writer, cfg.SessionID, cfg.RunID, cfg.RunLabel, "cancelled", deps.Stderr, bufferedUsage)
				}
				emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "prompt", fmt.Sprintf("prompt: %v", err), deps.Stderr, cfg.RunLabel)
				return sessionResult{ExitCode: 1}
			}
			if finalStopReason != "" && permissionDone == nil {
				return sessionResult{ExitCode: runtime.ExitCodeForStopReason(finalStopReason), LoopDirective: loopDirective, LoopLabel: loopLabel, Usage: bufferedUsage}
			}
		case res := <-permissionDone:
			permissionDone = nil
			if res.cancelled {
				return cancelAndEnd(provider, deps.Writer, cfg.SessionID, cfg.RunID, cfg.RunLabel, "cancelled", deps.Stderr, bufferedUsage)
			}
			if res.err != nil {
				emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "permission", fmt.Sprintf("permission handler: %v", res.err), deps.Stderr, cfg.RunLabel)
				return sessionResult{ExitCode: 1}
			}
			if res.requestID != "" {
				emitPermissionResponse(res.requestID, res.optionID, res.kind, res.source)
				if !writeStatus(tracker.PermissionAnswered()) {
					return sessionResult{ExitCode: 1}
				}
			}
			// If session.end + promptDone already arrived while we were waiting for
			// AnswerPermission, exit now that the permission goroutine has resolved.
			if finalStopReason != "" && promptReturned {
				return sessionResult{ExitCode: runtime.ExitCodeForStopReason(finalStopReason), LoopDirective: loopDirective, LoopLabel: loopLabel, Usage: bufferedUsage}
			}
			if eventChClosed && finalStopReason == "" {
				return sessionResult{ExitCode: 1}
			}
		case <-cfg.InterruptCh:
			cancelCtx, cfn := context.WithTimeout(context.Background(), 5*time.Second)
			if err := provider.Cancel(cancelCtx, cfg.SessionID); err != nil {
				emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "cancel", fmt.Sprintf("cancel session: %v", err), deps.Stderr, cfg.RunLabel)
			}
			cfn()
			finalStopReason = "cancelled"
		case <-ctx.Done():
			return cancelAndEnd(provider, deps.Writer, cfg.SessionID, cfg.RunID, cfg.RunLabel, "cancelled", deps.Stderr, bufferedUsage)
		case <-progressTimerC:
			return cancelAndEnd(provider, deps.Writer, cfg.SessionID, cfg.RunID, cfg.RunLabel, "progress_timeout", deps.Stderr, bufferedUsage)
		case <-cfg.Timeout:
			return cancelAndEnd(provider, deps.Writer, cfg.SessionID, cfg.RunID, cfg.RunLabel, "timeout", deps.Stderr, bufferedUsage)
		}
	}
}

func cancelAndEnd(provider runtime.Provider, writer EventSink, sessionID, runID, runLabel, stopReason string, stderr io.Writer, usage map[string]any) sessionResult {
	cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := provider.Cancel(cancelCtx, sessionID); err != nil {
		emitErrorEvent(writer, sessionID, runID, "cancel", fmt.Sprintf("cancel session: %v", err), stderr, runLabel)
	}
	fields := map[string]any{
		"stop_reason": stopReason,
	}
	if usage != nil {
		fields["usage"] = usage
	}
	if err := writer.Write(events.Event{
		Event:     "session.end",
		SessionID: sessionID,
		Fields:    fields,
	}); err != nil {
		fmt.Fprintf(stderr, "avenor: write terminal event: %v\n", err)
		return sessionResult{ExitCode: 1}
	}
	return sessionResult{ExitCode: runtime.ExitCodeForStopReason(stopReason), StopReason: stopReason, Usage: usage}
}

type eventWriter struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
}

type EventSink interface {
	Write(events.Event) error
	Close() error
}

type fanoutWriter struct {
	base    EventSink
	control *control.ControlServer
}

func newFanoutWriter(base EventSink, cs *control.ControlServer) EventSink {
	if cs == nil {
		return base
	}
	return &fanoutWriter{base: base, control: cs}
}

func (f *fanoutWriter) Write(event events.Event) error {
	if err := f.base.Write(event); err != nil {
		return err
	}
	f.control.PublishEvent(event)
	return nil
}

func (f *fanoutWriter) Close() error { return f.base.Close() }

func permissionKindFromOptionID(optionID string, options []any) string {
	for _, opt := range options {
		m, ok := opt.(map[string]any)
		if !ok {
			continue
		}
		oid, _ := m["optionId"].(string)
		if oid == optionID {
			kind, _ := m["kind"].(string)
			return strings.ToLower(kind)
		}
	}
	return "unknown"
}

// firstOptionKind returns the optionId and kind of the first option matching
// the given kind. Used for logging in auto-approve flows.
func firstOptionKind(options []any, wantKind string) (string, string) {
	for _, opt := range options {
		m, ok := opt.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := m["kind"].(string)
		if strings.ToLower(kind) == wantKind {
			optID, _ := m["optionId"].(string)
			return optID, wantKind
		}
	}
	return "", ""
}

func resolvePermission(
	ctx context.Context,
	provider runtime.Provider,
	fileHandler *permission.FileHandler,
	controlServer *control.ControlServer,
	event events.Event,
	sessionID string,
	requestID string,
	autoApprove bool,
	claimTimeout time.Duration,
	emit func(events.Event) error,
) permissionResult {
	options, _ := event.Fields["options"].([]any)
	if autoApprove {
		optionID, kind := firstOptionKind(options, "allow")
		resp := runtime.PermissionResponse{Allow: true, OptionID: optionID}
		if err := provider.AnswerPermission(ctx, sessionID, requestID, resp); err != nil {
			return permissionResult{err: err}
		}
		return permissionResult{requestID: requestID, optionID: optionID, kind: kind, source: "avenor"}
	}

	// Only consult the control plane when a client is actually connected to
	// the socket. Without this gate, --http-debug-only (or a socket with no
	// connected client) would silently swallow permission requests and the
	// file-handler fallback below would be unreachable.
	//
	// TOCTOU note: HasClients() may have been true when checked but the client
	// can disconnect (or go silent) before we reach BeginPermissionClaim or
	// before it sends answer_permission. We bound the wait with claimTimeout
	// so that on disconnect or a silent client we fall through to the
	// file-handler rather than blocking until SIGINT.
	if controlServer != nil && controlServer.HasClients() {
		answerCh, ok := controlServer.BeginPermissionClaim(requestID)
		// If !ok, another claim is already in flight; fall through to the
		// file-handler block below.
		if ok {
			claimTimer := time.NewTimer(claimTimeout)
			select {
			case ans := <-answerCh:
				claimTimer.Stop()
				controlServer.EndPermissionClaim(requestID)
				kind := permissionKindFromOptionID(ans.OptionID, options)
				switch kind {
				case "allow", "reject":
				default:
					return permissionResult{err: fmt.Errorf("unknown option_id %q for request %q", ans.OptionID, requestID)}
				}
				resp := runtime.PermissionResponse{Allow: kind == "allow", OptionID: ans.OptionID}
				if err := provider.AnswerPermission(ctx, sessionID, requestID, resp); err != nil {
					return permissionResult{err: err}
				}
				return permissionResult{requestID: requestID, optionID: ans.OptionID, kind: kind, source: "control"}
			case <-ctx.Done():
				claimTimer.Stop()
				controlServer.EndPermissionClaim(requestID)
				select {
				case ans := <-answerCh:
					kind := permissionKindFromOptionID(ans.OptionID, options)
					switch kind {
					case "allow", "reject":
					default:
						return permissionResult{err: fmt.Errorf("unknown option_id %q for request %q", ans.OptionID, requestID)}
					}
					resp := runtime.PermissionResponse{Allow: kind == "allow", OptionID: ans.OptionID}
					if err := provider.AnswerPermission(ctx, sessionID, requestID, resp); err != nil {
						return permissionResult{err: err}
					}
					return permissionResult{requestID: requestID, optionID: ans.OptionID, kind: kind, source: "control"}
				default:
				}
				return permissionResult{err: ctx.Err()}
			case <-claimTimer.C:
				controlServer.EndPermissionClaim(requestID)
				select {
				case ans := <-answerCh:
					kind := permissionKindFromOptionID(ans.OptionID, options)
					switch kind {
					case "allow", "reject":
					default:
						return permissionResult{err: fmt.Errorf("unknown option_id %q for request %q", ans.OptionID, requestID)}
					}
					resp := runtime.PermissionResponse{Allow: kind == "allow", OptionID: ans.OptionID}
					if err := provider.AnswerPermission(ctx, sessionID, requestID, resp); err != nil {
						return permissionResult{err: err}
					}
					return permissionResult{requestID: requestID, optionID: ans.OptionID, kind: kind, source: "control"}
				default:
					// Nothing in the channel; fall through to the file-handler.
				}
			}
		}
	}

	if fileHandler != nil {
		res, err := fileHandler.Handle(ctx, provider, event, emit)
		if err != nil {
			return permissionResult{err: err}
		}
		if res.Cancelled {
			return permissionResult{requestID: res.RequestID, optionID: res.OptionID, kind: permissionKindFromOptionID(res.OptionID, options), cancelled: true, source: "file"}
		}
		return permissionResult{requestID: res.RequestID, optionID: res.OptionID, kind: permissionKindFromOptionID(res.OptionID, options), source: "file"}
	}

	// no resolver: leave backend waiting
	return permissionResult{kind: "", source: "none"}
}

func NewEventWriter(path string) (*eventWriter, error) {
	if path == "" {
		return &eventWriter{encoder: json.NewEncoder(io.Discard)}, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &eventWriter{
		file:    file,
		encoder: json.NewEncoder(file),
	}, nil
}

func (w *eventWriter) Write(event events.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.encoder.Encode(event)
}

func (w *eventWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

func ParsePermissionHandler(value string) (*permission.FileHandler, error) {
	const prefix = "file:"
	if !strings.HasPrefix(value, prefix) {
		return nil, fmt.Errorf("--permission-handler supports file:<path> only")
	}
	path := strings.TrimPrefix(value, prefix)
	if path == "" {
		return nil, fmt.Errorf("--permission-handler file path is required")
	}
	return permission.NewFileHandler(path), nil
}
