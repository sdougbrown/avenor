package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/digest"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/rosterconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/pony"
	"github.com/sdougbrown/avenor/internal/runtime/pony/model/openai"
	"github.com/sdougbrown/avenor/internal/teamrunner"
)

const (
	DefaultBackend        = "opencode-acp"
	backendOpenCodeACP    = "opencode-acp"
	backendOpenCodeHTTP   = "opencode-http"
	backendCodexAppServer = "codex-app-server"
	backendGeminiACP      = "gemini-acp"
	backendAgy            = "agy"
	backendCursorACP      = "cursor-acp"
	backendPony           = "pony"
	backendClaude         = "claude"
	backendPi             = "pi"
)

// DefaultPermissionClaimTimeout is the zero value, meaning "no timeout":
// the resolver waits indefinitely for a connected socket client to answer,
// falling through to the file-handler (or no-resolver) only when the client
// disconnects. A non-zero --permission-claim-timeout arms an explicit timer
// for unattended automation scenarios.
const DefaultPermissionClaimTimeout = 0

const permissionResolverJoinTimeout = 5 * time.Second

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

func run(args []string, getenv func(string) string, stderr io.Writer) (exitCode int) {
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
	resultFile := fs.String("result-file", "", "path to write the run's final assistant message as plain text (direct runs only; written before the sentinel)")
	pidFile := fs.String("pid-file", "", "path to write this process's pid at startup (removed on exit)")
	timeout := fs.Duration("timeout", 0, "overall session timeout")
	progressTimeout := fs.Duration("progress-timeout", 0, "session progress timeout (fires if no event for duration)")
	model := fs.String("model", "", "backend-specific model id")
	thinking := fs.String("thinking", "", "thinking level (off, minimal, low, medium, high, xhigh, max)")
	claudeMCPWait := fs.Duration("claude-mcp-wait", 0, "hold the first prompt until this long after launch so MCP servers connect first (claude backend, direct runs only; 0 = off, max 60s)")
	backend := fs.String("backend", DefaultBackend, "runtime backend")
	runIDFlag := fs.String("run-id", "", "correlation id for this run (generated if not set)")
	maxRetries := fs.Int("max-retries", 0, "maximum retry attempts on transient failure (0 = no retry)")
	autoApprove := fs.Bool("auto-approve", false, "automatically approve all permission requests")
	controlSocket := fs.String("control-socket", "", "unix socket path for control plane")
	httpDebug := fs.String("http-debug", "", "http debug adapter bind address")
	permClaimTimeout := fs.Duration("permission-claim-timeout", 0, "how long to wait for a connected socket client to answer a permission request before falling through to the file handler or 'none' resolver (0 = disabled: fall through only when all clients disconnect; use a non-zero value for unattended automation where client processes may hang)")
	loopFile := fs.String("loop-file", "", "path to loop config JSON (optional; enables multi-phase mode)")
	teamFile := fs.String("team-file", "", "path to team config JSON (optional; enables parallel-team mode)")
	rosterFile := fs.String("roster-file", "", "path to roster JSON (direct selector or workflow fallback)")
	rosterEntry := fs.String("roster-entry", "", "roster entry name for direct selection")
	ponyConfig := fs.String("pony-config", "", "path to pony backend JSON config (required for --backend pony)")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	runID := *runIDFlag
	if runID == "" {
		runID = GenerateRunID()
	}

	workflowMode := *loopFile != "" || *teamFile != ""
	backendExplicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "backend" {
			backendExplicit = true
		}
	})

	// finalSessionID is updated after each attempt so exitWithSentinel always
	// writes the most recent session ID regardless of which return path fires.
	var finalSessionID string
	var finalStopReason string
	var finalRetryAfter int64
	var finalResult string

	exitWithSentinel := func(code int) int {
		resultPath := ""
		if *resultFile != "" && finalResult != "" {
			if err := writeResultFile(*resultFile, finalResult); err != nil {
				fmt.Fprintf(stderr, "avenor: result-file: %v\n", err)
			} else {
				resultPath = *resultFile
			}
		}
		if *sentinelFile != "" {
			sr := finalStopReason
			if sr == "" {
				sr = runtime.StopReasonForExitCode(code)
			}
			WriteSentinelResult(*sentinelFile, code, finalSessionID, sr, runID, finalRetryAfter, "", resultPath, stderr)
		}
		return code
	}

	if workflowMode {
		if *claudeMCPWait != 0 {
			fmt.Fprintln(stderr, "avenor: --claude-mcp-wait is not valid with --loop-file or --team-file")
			return exitWithSentinel(1)
		}
		if *rosterEntry != "" {
			fmt.Fprintln(stderr, "avenor: --roster-entry is not valid with --loop-file or --team-file")
			return exitWithSentinel(1)
		}
	} else if *rosterFile != "" || *rosterEntry != "" {
		if *rosterFile == "" || *rosterEntry == "" {
			fmt.Fprintln(stderr, "avenor: --roster-file and --roster-entry must be supplied together for direct selection")
			return exitWithSentinel(1)
		}
		if *agent != "" || *model != "" || backendExplicit {
			fmt.Fprintln(stderr, "avenor: direct identity fields are mutually exclusive with roster selection")
			return exitWithSentinel(1)
		}
		roster, err := rosterconfig.Load(*rosterFile)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: %v\n", err)
			return exitWithSentinel(1)
		}
		entry, err := roster.Lookup(*rosterEntry)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: %v\n", err)
			return exitWithSentinel(1)
		}
		*backend = entry.Backend
		*agent = entry.Agent
		*model = entry.Model
		// Roster supplies a default; an explicit --thinking wins. Validation
		// against the backend happens below, once the roster has set it.
		if *thinking == "" {
			*thinking = entry.Thinking
		}
		if *agent != "" && *model == "" {
			resolved, resolveErr := resolveAgentModel(*agent, *backend, getenv)
			if resolveErr != nil {
				fmt.Fprintf(stderr, "avenor: %v\n", resolveErr)
				return exitWithSentinel(1)
			}
			if resolved != "" {
				*model = resolved
			}
		}
	}

	discovery := DiscoverServer(*serverURL, getenv)
	if !workflowMode {
		if err := validateCLISelection(*backend, discovery, *thinking, backendExplicit || *rosterEntry != ""); err != nil {
			fmt.Fprintf(stderr, "avenor: %v\n", err)
			return exitWithSentinel(1)
		}
		if err := runtime.ValidateClaudeMCPWait(*backend, *claudeMCPWait); err != nil {
			fmt.Fprintf(stderr, "avenor: %v\n", err)
			return exitWithSentinel(1)
		}
		if *claudeMCPWait != 0 && *progressTimeout != 0 && *claudeMCPWait >= *progressTimeout {
			fmt.Fprintf(stderr, "avenor: --claude-mcp-wait %s must be shorter than --progress-timeout %s, which fires while the first prompt is held\n", *claudeMCPWait, *progressTimeout)
			return exitWithSentinel(1)
		}
	}

	// Pony backend setup
	if *backend == backendPony {
		if *ponyConfig == "" {
			fmt.Fprintln(stderr, "avenor: --pony-config is required for backend pony")
			return exitWithSentinel(1)
		}
		ponyCfg, err := pony.LoadPonyConfig(*ponyConfig)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: %v\n", err)
			return exitWithSentinel(1)
		}
		profileConfigs := make(map[string]pony.Config, len(ponyCfg.Profiles))
		configDir := filepath.Dir(*ponyConfig)
		for name, profile := range ponyCfg.Profiles {
			resolvedBaseURL := ponyCfg.BaseURL
			resolvedAPIKeyEnv := ponyCfg.APIKeyEnv
			if profile.BaseURL != "" {
				resolvedBaseURL = profile.BaseURL
			}
			if profile.APIKeyEnv != "" {
				resolvedAPIKeyEnv = profile.APIKeyEnv
			}

			adapter, err := openai.New(openai.Config{
				BaseURL: resolvedBaseURL,
				APIKey:  getenv(resolvedAPIKeyEnv),
			})
			if err != nil {
				fmt.Fprintf(stderr, "avenor: %v\n", err)
				return exitWithSentinel(1)
			}

			var executor pony.OrchestratorExecutor
			if profile.Tools.Orchestration && *controlSocket != "" {
				executor, err = pony.NewControlSocketExecutor(*controlSocket)
				if err != nil {
					fmt.Fprintf(stderr, "avenor: %v\n", err)
					return exitWithSentinel(1)
				}
			}

			systemPrompt := profile.SystemPrompt
			initialPrompt := profile.InitialPrompt
			var allowedDirs []string
			if len(profile.Skills) > 0 {
				skills, err := pony.LoadSkills(configDir, profile.Skills)
				if err != nil {
					fmt.Fprintf(stderr, "avenor: %v\n", err)
					return exitWithSentinel(1)
				}
				systemPrompt, initialPrompt = pony.MergeSkills(systemPrompt, initialPrompt, skills)

				if skillsDir := os.Getenv("PONY_SKILLS_DIR"); skillsDir != "" {
					allowedDirs = append(allowedDirs, skillsDir)
				}
				if configDir != "" {
					allowedDirs = append(allowedDirs, filepath.Join(configDir, ".pony"))
				}
			}

			profileConfigs[name] = pony.Config{
				Adapter:          adapter,
				Model:            profile.Model,
				MaxTokens:        profile.MaxTokens,
				SystemPrompt:     systemPrompt,
				InitialPrompt:    initialPrompt,
				Executor:         executor,
				LocalTools:       profile.Tools.Local,
				OrchTools:        profile.Tools.Orchestration,
				WorkingDir:       *dir,
				InjectAgentsMD:   profile.InjectAgentsMD,
				ToolApproval:     profile.ToolApproval,
				ShellConfig:      profile.ShellConfig,
				FileReadConfig:   profile.FileReadConfig,
				AllowedReadDirs:  allowedDirs,
				Context:          profile.Context,
				CompactionPrompt: profile.CompactionPrompt,
			}
		}

		pony.SetGlobalProfiles(ponyCfg.DefaultAgent, profileConfigs)
		if *agent != "" {
			if _, ok := profileConfigs[*agent]; !ok {
				fmt.Fprintf(stderr, "avenor: pony config: unknown profile %q\n", *agent)
				return exitWithSentinel(1)
			}
		}
	}

	if *prompt != "" && *promptFile != "" {
		fmt.Fprintln(stderr, "avenor: --prompt and --prompt-file are mutually exclusive")
		return exitWithSentinel(1)
	}
	if *loopFile != "" && *teamFile != "" {
		fmt.Fprintln(stderr, "avenor: --loop-file and --team-file are mutually exclusive")
		return exitWithSentinel(1)
	}
	if *resultFile != "" && workflowMode {
		fmt.Fprintln(stderr, "avenor: --result-file is not supported with --loop-file or --team-file")
		return exitWithSentinel(1)
	}
	if err := validateArtifactPaths(*resultFile, *pidFile, *sentinelFile, *onEvent, *permissionHandler, *autoApprove); err != nil {
		fmt.Fprintf(stderr, "avenor: %v\n", err)
		return exitWithSentinel(1)
	}
	if *loopFile != "" && *resume != "" {
		fmt.Fprintln(stderr, "avenor: --loop-file and --resume are mutually exclusive")
		return exitWithSentinel(1)
	}
	if *teamFile != "" && *resume != "" {
		fmt.Fprintln(stderr, "avenor: --team-file and --resume are mutually exclusive")
		return exitWithSentinel(1)
	}
	if *prompt == "" && *promptFile == "" && *loopFile == "" && *teamFile == "" {
		fmt.Fprintln(stderr, "avenor: --prompt, --prompt-file, --loop-file, or --team-file is required")
		return exitWithSentinel(1)
	}

	effectivePermHandler := effectivePermissionHandler(*sentinelFile, *permissionHandler, *autoApprove)
	cleanupPermBase := permissionCleanupBase(*sentinelFile, *permissionHandler)

	// The signal trap is installed before the pid file is published, so a
	// first signal landing during setup still has a cleanup path. Once the
	// signal has cancelled the trap context, stopSignals restores the default
	// disposition so a second signal still ends a teardown that will not finish.
	// The run context is derived from the trap context so a control-plane cancel
	// stops the run without unregistering the trap: a signal arriving during that
	// teardown is still handled, and only a signal restores the default.
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	stopAfterCancel := context.AfterFunc(signalCtx, stopSignals)
	defer stopAfterCancel()
	ctx, cancelRun := context.WithCancel(signalCtx)
	defer cancelRun()

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
	defer baseWriter.Close()
	metadata := NewEventMetadata(runID, *label, runID)

	state := control.NewState(runID, *label, *maxRetries)
	var controlServer *control.ControlServer
	var httpServer *control.HTTPDebugServer
	if *controlSocket != "" || *httpDebug != "" {
		owner, err := control.GenerateOwnerCapability()
		if err != nil {
			fmt.Fprintf(stderr, "avenor: %v\n", err)
			return exitWithSentinel(1)
		}
		controlServer = control.NewServer(state, owner)
	}
	if *controlSocket != "" {
		if err := controlServer.Start(*controlSocket); err != nil {
			fmt.Fprintf(stderr, "avenor: start control server: %v\n", err)
			return exitWithSentinel(1)
		}
		defer func() {
			if err := controlServer.Stop(); err != nil {
				fmt.Fprintf(stderr, "avenor: stop control server: %v\n", err)
				if exitCode == 0 {
					exitCode = 1
				}
			}
		}()
	}
	if *httpDebug != "" {
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
	var writer EventSink = newFanoutWriter(baseWriter, controlServer, metadata)

	var fileHandler *permission.FileHandler
	if effectivePermHandler != "" {
		fileHandler, err = ParsePermissionHandler(effectivePermHandler)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: %v\n", err)
			return exitWithSentinel(1)
		}
	}

	if *agent != "" && *model == "" {
		resolved, err := resolveAgentModel(*agent, *backend, getenv)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: %v\n", err)
		}
		if resolved != "" {
			*model = resolved
		}
	}

	startOptions := runtime.StartOptions{
		Agent:     *agent,
		Label:     *label,
		Dir:       *dir,
		ServerURL: discovery.URL,
		Model:     *model,
		Thinking:  *thinking,

		ClaudeMCPWait: *claudeMCPWait,
	}

	if controlServer != nil {
		controlServer.SetCancelFunc(cancelRun)
	}

	// The run deadline (not a one-shot timer) bounds every attempt, retry,
	// and nudge of this CLI execution. Each WaitForSession derives its own
	// timer from the shared deadline, so no waiter can consume another's
	// only tick and no later attempt can outlive the limit.
	runCtx := ctx
	if *timeout > 0 {
		var cancelDeadline context.CancelFunc
		runCtx, cancelDeadline = context.WithDeadline(ctx, time.Now().Add(*timeout))
		defer cancelDeadline()
	}

	// Stable supervisor metadata is unavailable in this execution path. Keep
	// the authoritative session/backend relationship local to this CLI run.
	sessionBackends := newSessionBackendMap()

	execAttempt := func(ctx context.Context, phase phaseSelection, resumeID, prompt string, provenance *runtime.LaunchProvenance) attemptResult {
		selection := phase.selection
		if err := validateCLISelection(selection.Backend, discovery, selection.Thinking, phase.backendSupplied); err != nil {
			fmt.Fprintf(stderr, "avenor: %v\n", err)
			return attemptResult{exitCode: 1}
		}
		startOptions := runtime.StartOptions{Agent: selection.Agent, Model: selection.Model, Label: *label, Dir: *dir, ServerURL: discovery.URL, Thinking: selection.Thinking, Provenance: provenance}
		return runAttempt(ctx, attemptConfig{
			startOptions:           startOptions,
			backend:                selection.Backend,
			resumeID:               resumeID,
			sessionBackends:        sessionBackends,
			initialPrompt:          prompt,
			runID:                  runID,
			runLabel:               *label,
			autoApprove:            *autoApprove,
			permissionClaimTimeout: *permClaimTimeout,
			progressTimeout:        *progressTimeout,
		}, attemptDeps{
			writer:        writer,
			fileHandler:   fileHandler,
			controlServer: controlServer,
			stderr:        stderr,
		})
	}

	preparePhaseSelection := func(selection rosterconfig.ResolvedSelection, backendSupplied bool) (rosterconfig.ResolvedSelection, error) {
		if selection.Agent != "" && selection.Model == "" {
			resolved, err := resolveAgentModel(selection.Agent, selection.Backend, getenv)
			if err != nil {
				return rosterconfig.ResolvedSelection{}, err
			}
			if resolved != "" {
				selection.Model = resolved
			}
		}
		// A roster entry supplies a default; an explicit --thinking wins.
		// The merged level is carried on the selection so execAttempt sees it.
		if *thinking != "" {
			selection.Thinking = *thinking
		}
		if err := validateCLISelection(selection.Backend, discovery, selection.Thinking, backendSupplied); err != nil {
			return rosterconfig.ResolvedSelection{}, err
		}
		return selection, nil
	}

	makePhaseAttemptForLoop := func(roster *rosterconfig.Config) func(context.Context, phaseconfig.Phase, int, int, string) (looprunner.PhaseAttemptResult, error) {
		var selectionMu sync.Mutex
		resolvedSelections := make(map[string]phaseSelection)
		return func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (looprunner.PhaseAttemptResult, error) {
			selectionMu.Lock()
			resolved, cached := resolvedSelections[phase.Name]
			selectionMu.Unlock()
			if !cached {
				var entry *rosterconfig.Entry
				if phase.RosterEntry != "" {
					if roster == nil {
						return looprunner.PhaseAttemptResult{ExitCode: 1}, fmt.Errorf("phase %q roster entry %q requires a roster", phase.Name, phase.RosterEntry)
					}
					loaded, err := roster.Lookup(phase.RosterEntry)
					if err != nil {
						return looprunner.PhaseAttemptResult{ExitCode: 1}, err
					}
					entry = &loaded
				}
				var err error
				selection, err := rosterconfig.Resolve(rosterconfig.ResolveInput{
					Backend: *backend,
					Agent:   *agent,
					Model:   *model,
					Roster:  entry,
					Loop:    true,
				})
				if err != nil {
					return looprunner.PhaseAttemptResult{ExitCode: 1}, err
				}
				backendSupplied := entry != nil || backendExplicit
				selection, err = preparePhaseSelection(selection, backendSupplied)
				if err != nil {
					return looprunner.PhaseAttemptResult{ExitCode: 1}, err
				}
				resolved = phaseSelection{selection: selection, backendSupplied: backendSupplied}
				selectionMu.Lock()
				resolvedSelections[phase.Name] = resolved
				selectionMu.Unlock()
			}
			r := execAttempt(ctx, resolved, prevSessionID, phase.Prompt, runtime.PhaseLaunchProvenance(
				runID, runtime.LaunchKindLoop, phase.Name, *label, &iteration, attemptNum))
			if r.err != nil {
				return looprunner.PhaseAttemptResult{ExitCode: 1}, r.err
			}
			return looprunner.PhaseAttemptResult{
				ExitCode:      r.exitCode,
				SessionID:     r.sessionID,
				StopReason:    r.stopReason,
				RetryAfter:    r.retryAfter,
				LoopDirective: r.loopDirective,
				LoopLabel:     r.loopLabel,
				BrokerRunID:   "",
			}, nil
		}
	}

	makePhaseAttemptForTeam := func(roster *rosterconfig.Config) func(context.Context, phaseconfig.Phase, int, string) (teamrunner.PhaseAttemptResult, error) {
		var selectionMu sync.Mutex
		resolvedSelections := make(map[string]phaseSelection)
		return func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (teamrunner.PhaseAttemptResult, error) {
			selectionMu.Lock()
			resolved, cached := resolvedSelections[phase.Name]
			selectionMu.Unlock()
			if !cached {
				var entry *rosterconfig.Entry
				if phase.RosterEntry != "" {
					if roster == nil {
						return teamrunner.PhaseAttemptResult{ExitCode: 1}, fmt.Errorf("phase %q roster entry %q requires a roster", phase.Name, phase.RosterEntry)
					}
					loaded, err := roster.Lookup(phase.RosterEntry)
					if err != nil {
						return teamrunner.PhaseAttemptResult{ExitCode: 1}, err
					}
					entry = &loaded
				}
				var err error
				selection, err := rosterconfig.Resolve(rosterconfig.ResolveInput{
					Backend:     *backend,
					Agent:       *agent,
					Model:       *model,
					InlineAgent: phase.Agent,
					InlineModel: phase.Model,
					Roster:      entry,
				})
				if err != nil {
					return teamrunner.PhaseAttemptResult{ExitCode: 1}, err
				}
				if entry == nil && phase.Agent != "" && phase.Model == "" {
					resolved, resolveErr := resolveAgentModel(phase.Agent, selection.Backend, getenv)
					if resolveErr != nil {
						return teamrunner.PhaseAttemptResult{ExitCode: 1}, resolveErr
					}
					if resolved != "" {
						selection.Model = resolved
					}
				}
				backendSupplied := entry != nil || backendExplicit
				selection, err = preparePhaseSelection(selection, backendSupplied)
				if err != nil {
					return teamrunner.PhaseAttemptResult{ExitCode: 1}, err
				}
				resolved = phaseSelection{selection: selection, backendSupplied: backendSupplied}
				selectionMu.Lock()
				resolvedSelections[phase.Name] = resolved
				selectionMu.Unlock()
			}
			r := execAttempt(ctx, resolved, prevSessionID, phase.Prompt, runtime.PhaseLaunchProvenance(
				runID, runtime.LaunchKindTeam, phase.Name, *label, nil, attemptNum))
			if r.err != nil {
				return teamrunner.PhaseAttemptResult{ExitCode: 1}, r.err
			}
			return teamrunner.PhaseAttemptResult{
				ExitCode:      r.exitCode,
				SessionID:     r.sessionID,
				StopReason:    r.stopReason,
				RetryAfter:    r.retryAfter,
				LoopDirective: r.loopDirective,
				LoopLabel:     r.loopLabel,
				Output:        r.output,
				FinalReply:    r.finalReply,
				BrokerRunID:   "",
			}, nil
		}
	}

	if *loopFile != "" {
		fallbackRosterPath := *rosterFile
		if fallbackRosterPath != "" && !filepath.IsAbs(fallbackRosterPath) {
			fallbackRosterPath = filepath.Join(*dir, fallbackRosterPath)
		}
		cfg, rootRoster, err := looprunner.LoadLoopConfigWithRoster(*loopFile, nil, fallbackRosterPath)
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

		var makeNestedRun func(*rosterconfig.Config) func(context.Context, string, string) (looprunner.NestedResult, error)
		makeNestedRun = func(inherited *rosterconfig.Config) func(context.Context, string, string) (looprunner.NestedResult, error) {
			return func(ctx context.Context, configPath string, runType string) (looprunner.NestedResult, error) {
				if runType == "loop" {
					subCfg, subRoster, err := looprunner.LoadLoopConfigWithRoster(configPath, inherited, "")
					if err != nil {
						return looprunner.NestedResult{}, fmt.Errorf("load nested loop config: %w", err)
					}
					subOpts := looprunner.RunOptions{
						WorkDir:      *dir,
						RunID:        runID,
						EventSink:    writer,
						Config:       subCfg,
						MaxRetries:   *maxRetries,
						Broker:       nil,
						ConfigDir:    filepath.Dir(configPath),
						Roster:       subRoster,
						PhaseAttempt: makePhaseAttemptForLoop(subRoster),
						NestedRun:    makeNestedRun(subRoster),
					}
					subResult, err := looprunner.Run(ctx, subOpts)
					if err != nil {
						return looprunner.NestedResult{}, err
					}
					return looprunner.NestedResult{
						ExitCode:   subResult.ExitCode,
						StopReason: subResult.StopReason,
						SessionID:  subResult.SessionID,
						Reason:     subResult.Reason,
						RetryAfter: subResult.RetryAfter,
					}, nil
				}
				if runType == "team" {
					subCfg, subRoster, err := teamrunner.LoadTeamConfigWithRoster(configPath, inherited, "")
					if err != nil {
						return looprunner.NestedResult{}, fmt.Errorf("load nested team config: %w", err)
					}
					teamNestedRun := func(ctx context.Context, configPath string, runType string) (teamrunner.NestedResult, error) {
						nr, err := makeNestedRun(subRoster)(ctx, configPath, runType)
						return teamrunner.NestedResult{
							ExitCode:   nr.ExitCode,
							StopReason: nr.StopReason,
							SessionID:  nr.SessionID,
							Reason:     nr.Reason,
							RetryAfter: nr.RetryAfter,
						}, err
					}
					subOpts := teamrunner.RunOptions{
						WorkDir:      *dir,
						RunID:        runID,
						EventSink:    writer,
						Config:       subCfg,
						MaxRetries:   *maxRetries,
						Broker:       nil,
						ConfigDir:    filepath.Dir(configPath),
						Roster:       subRoster,
						PhaseAttempt: makePhaseAttemptForTeam(subRoster),
						NestedRun:    teamNestedRun,
					}
					subResult, err := teamrunner.Run(ctx, subOpts)
					if err != nil {
						return looprunner.NestedResult{}, err
					}
					return looprunner.NestedResult{
						ExitCode:   subResult.ExitCode,
						StopReason: subResult.StopReason,
						SessionID:  subResult.SessionID,
						Reason:     subResult.Reason,
						RetryAfter: subResult.RetryAfter,
					}, nil
				}
				return looprunner.NestedResult{}, fmt.Errorf("unknown run type %q", runType)
			}
		}
		nestedRun := makeNestedRun(rootRoster)

		opts := looprunner.RunOptions{
			WorkDir:      *dir,
			RunID:        runID,
			EventSink:    writer,
			Config:       cfg,
			MaxRetries:   *maxRetries,
			Broker:       nil,
			ConfigDir:    filepath.Dir(*loopFile),
			Roster:       rootRoster,
			PhaseAttempt: makePhaseAttemptForLoop(rootRoster),
			NestedRun:    nestedRun,
		}

		stopPID, ok := publishPIDFile(*pidFile, stderr)
		if !ok {
			return exitWithSentinel(1)
		}
		defer stopPID()

		lrCtx, lrCancel := context.WithCancel(runCtx)
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

	if *teamFile != "" {
		fallbackRosterPath := *rosterFile
		if fallbackRosterPath != "" && !filepath.IsAbs(fallbackRosterPath) {
			fallbackRosterPath = filepath.Join(*dir, fallbackRosterPath)
		}
		cfg, rootRoster, err := teamrunner.LoadTeamConfigWithRoster(*teamFile, nil, fallbackRosterPath)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: load team config: %v\n", err)
			if *sentinelFile != "" {
				WriteSentinel(*sentinelFile, 1, finalSessionID, "error", runID, stderr)
			}
			return 1
		}

		if len(promptText) > 0 {
			cfg.InsertInitialPrompt(string(promptText))
		}

		var makeNestedRun func(*rosterconfig.Config) func(context.Context, string, string) (teamrunner.NestedResult, error)
		makeNestedRun = func(inherited *rosterconfig.Config) func(context.Context, string, string) (teamrunner.NestedResult, error) {
			return func(ctx context.Context, configPath string, runType string) (teamrunner.NestedResult, error) {
				if runType == "loop" {
					subCfg, subRoster, err := looprunner.LoadLoopConfigWithRoster(configPath, inherited, "")
					if err != nil {
						return teamrunner.NestedResult{}, fmt.Errorf("load nested loop config: %w", err)
					}
					loopNestedRun := func(ctx context.Context, configPath string, runType string) (looprunner.NestedResult, error) {
						nr, err := makeNestedRun(subRoster)(ctx, configPath, runType)
						return looprunner.NestedResult{
							ExitCode:   nr.ExitCode,
							StopReason: nr.StopReason,
							SessionID:  nr.SessionID,
							Reason:     nr.Reason,
							RetryAfter: nr.RetryAfter,
						}, err
					}
					subOpts := looprunner.RunOptions{
						WorkDir:      *dir,
						RunID:        runID,
						EventSink:    writer,
						Config:       subCfg,
						MaxRetries:   *maxRetries,
						Broker:       nil,
						ConfigDir:    filepath.Dir(configPath),
						Roster:       subRoster,
						PhaseAttempt: makePhaseAttemptForLoop(subRoster),
						NestedRun:    loopNestedRun,
					}
					subResult, err := looprunner.Run(ctx, subOpts)
					if err != nil {
						return teamrunner.NestedResult{}, err
					}
					return teamrunner.NestedResult{
						ExitCode:   subResult.ExitCode,
						StopReason: subResult.StopReason,
						SessionID:  subResult.SessionID,
						Reason:     subResult.Reason,
						RetryAfter: subResult.RetryAfter,
					}, nil
				}
				if runType == "team" {
					subCfg, subRoster, err := teamrunner.LoadTeamConfigWithRoster(configPath, inherited, "")
					if err != nil {
						return teamrunner.NestedResult{}, fmt.Errorf("load nested team config: %w", err)
					}
					subOpts := teamrunner.RunOptions{
						WorkDir:      *dir,
						RunID:        runID,
						EventSink:    writer,
						Config:       subCfg,
						MaxRetries:   *maxRetries,
						Broker:       nil,
						ConfigDir:    filepath.Dir(configPath),
						Roster:       subRoster,
						PhaseAttempt: makePhaseAttemptForTeam(subRoster),
						NestedRun:    makeNestedRun(subRoster),
					}
					subResult, err := teamrunner.Run(ctx, subOpts)
					if err != nil {
						return teamrunner.NestedResult{}, err
					}
					return teamrunner.NestedResult{
						ExitCode:   subResult.ExitCode,
						StopReason: subResult.StopReason,
						SessionID:  subResult.SessionID,
						Reason:     subResult.Reason,
						RetryAfter: subResult.RetryAfter,
					}, nil
				}
				return teamrunner.NestedResult{}, fmt.Errorf("unknown run type %q", runType)
			}
		}
		nestedRun := makeNestedRun(rootRoster)

		opts := teamrunner.RunOptions{
			WorkDir:      *dir,
			RunID:        runID,
			EventSink:    writer,
			Config:       cfg,
			MaxRetries:   *maxRetries,
			Broker:       nil,
			ConfigDir:    filepath.Dir(*teamFile),
			Roster:       rootRoster,
			PhaseAttempt: makePhaseAttemptForTeam(rootRoster),
			NestedRun:    nestedRun,
		}

		stopPID, ok := publishPIDFile(*pidFile, stderr)
		if !ok {
			return exitWithSentinel(1)
		}
		defer stopPID()

		trCtx, trCancel := context.WithCancel(runCtx)
		defer trCancel()

		result, err := teamrunner.Run(trCtx, opts)
		if err != nil {
			fmt.Fprintf(stderr, "avenor: team run: %v\n", err)
			if *sentinelFile != "" {
				WriteSentinel(*sentinelFile, 1, result.SessionID, "error", runID, stderr)
			}
			return 1
		}

		if *sentinelFile != "" {
			if result.Reason != "" {
				WriteSentinelResult(*sentinelFile, result.ExitCode, result.SessionID, result.StopReason, runID, result.RetryAfter, result.Reason, "", stderr)
			} else {
				WriteSentinelResult(*sentinelFile, result.ExitCode, result.SessionID, result.StopReason, runID, result.RetryAfter, "", "", stderr)
			}
		}

		return result.ExitCode
	}

	// The pid file is published once every setup step that can fail has passed,
	// so a rejected invocation never touches a pid file another run owns.
	stopPID, ok := publishPIDFile(*pidFile, stderr)
	if !ok {
		return exitWithSentinel(1)
	}
	defer stopPID()

	resumeID := *resume
	if resumeID != "" {
		// The notice derives from the drop itself, so the two cannot drift:
		// anything the resume path will silently ignore is named here.
		before := runtime.StartOptions{Thinking: *thinking, ClaudeMCPWait: *claudeMCPWait}
		after := runtime.DropStartOnlyOptions(*backend, before)
		var dropped []string
		if after.Thinking != before.Thinking {
			dropped = append(dropped, "--thinking "+before.Thinking)
		}
		if after.ClaudeMCPWait != before.ClaudeMCPWait {
			dropped = append(dropped, "--claude-mcp-wait "+before.ClaudeMCPWait.String())
		}
		if len(dropped) > 0 {
			fmt.Fprintf(stderr, "avenor: ignoring start-only options on resume: %s\n", strings.Join(dropped, " "))
		}
	}
	// A cancelled or timed-out run stopped mid-answer, so the reply collected so
	// far is not the run's final message and must not be published.
	exitForRunContext := func(code int, stopReason string) int {
		finalStopReason = stopReason
		finalResult = ""
		return exitWithSentinel(code)
	}
	var result attemptResult
	for attempt := 1; ; attempt++ {
		// A signal can land between the trap and the first session, so the run
		// context decides the outcome before an attempt starts.
		if code, stopReason, done := runContextOutcome(runCtx); done {
			return exitForRunContext(code, stopReason)
		}
		if controlServer != nil {
			state.Update(func(s *control.Snapshot) { s.RetryAttempt = attempt })
		}
		opts := startOptions
		opts.Provenance = runtime.DirectLaunchProvenance(runID, *label, attempt)
		result = runAttempt(runCtx, attemptConfig{
			startOptions:           opts,
			backend:                *backend,
			resumeID:               resumeID,
			sessionBackends:        sessionBackends,
			initialPrompt:          string(promptText),
			resultFile:             *resultFile,
			runID:                  runID,
			runLabel:               *label,
			autoApprove:            *autoApprove,
			permissionClaimTimeout: *permClaimTimeout,
			progressTimeout:        *progressTimeout,
		}, attemptDeps{
			writer:        writer,
			fileHandler:   fileHandler,
			controlServer: controlServer,
			stderr:        stderr,
		})
		finalSessionID = result.sessionID
		finalStopReason = result.stopReason
		finalRetryAfter = result.retryAfter
		finalResult = settledResultText(result)

		// An attempt that failed while the run context was already finished
		// reports the cancel or the deadline, not a failed attempt.
		if result.exitCode != 0 {
			if code, stopReason, done := runContextOutcome(runCtx); done {
				return exitForRunContext(code, stopReason)
			}
		}

		if result.err != nil || !runtime.IsRetryableFailure(result.exitCode, result.stopReason) || attempt > *maxRetries {
			break
		}

		if result.sessionID != "" {
			resumeID = result.sessionID
		}

		// A hinted wait this run cannot honor leaves no attempt worth making, so
		// the provider's own class and RETRY_AFTER stand.
		wait, retry := runtime.RetryWait(runCtx, retryAfter, backoffDelay(attempt), result.retryAfter)
		if !retry {
			break
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
		case <-wait:
		case <-runCtx.Done():
			// The loop's context check reports the stop; do not start another
			// attempt after the run context finished.
			continue
		}
	}

	return exitWithSentinel(result.exitCode)
}

// phaseSelection pairs a resolved phase identity with whether configuration the
// caller named chose its backend, rather than the CLI default.
type phaseSelection struct {
	selection       rosterconfig.ResolvedSelection
	backendSupplied bool
}

func validateCLISelection(backend string, discovery ServerDiscovery, thinking string, backendSupplied bool) error {
	switch backend {
	case backendPony, backendOpenCodeACP, backendGeminiACP, backendAgy, backendCursorACP,
		backendCodexAppServer, backendClaude, backendPi:
	case backendOpenCodeHTTP:
		if discovery.URL == "" {
			return fmt.Errorf("--server-url is required for backend opencode-http")
		}
	default:
		return fmt.Errorf("unknown backend %q", backend)
	}
	if !backendSupplied {
		return runtime.ValidateThinkingForBackendOmitted(backend, thinking)
	}
	return runtime.ValidateThinkingForBackend(backend, thinking)
}

type resumeWithOptionser interface {
	ResumeWithOptions(context.Context, string, runtime.StartOptions) (runtime.Session, error)
}

func StartSession(ctx context.Context, provider runtime.Provider, backend string, opts runtime.StartOptions, resumeID string) (runtime.Session, error) {
	if err := runtime.ValidateThinkingForBackend(backend, opts.Thinking); err != nil {
		return runtime.Session{}, err
	}
	if err := runtime.ValidateClaudeMCPWait(backend, opts.ClaudeMCPWait); err != nil {
		return runtime.Session{}, err
	}
	if resumeID == "" {
		return provider.Start(ctx, opts)
	}
	if resumer, ok := provider.(resumeWithOptionser); ok {
		return resumer.ResumeWithOptions(ctx, resumeID, opts)
	}
	if opts.Thinking != "" {
		if err := runtime.ValidateThinkingForBackendResume(backend, opts.Thinking); err != nil {
			return runtime.Session{}, err
		}
	}
	if opts.ClaudeMCPWait != 0 && runtime.ClaudeMCPWaitStartOnly(backend) {
		return runtime.Session{}, runtime.NewStartOnlyClaudeMCPWaitError(backend, opts.ClaudeMCPWait)
	}
	return provider.Resume(ctx, resumeID)
}

var runAttempt = runSingleAttempt
var retryAfter = time.After

// deadlineFromContext returns the context's deadline, or the zero time when
// the context carries none.
func deadlineFromContext(ctx context.Context) time.Time {
	deadline, _ := ctx.Deadline()
	return deadline
}

// runContextOutcome maps a finished run context to the run's outcome: a
// cancelled signal ends the run at 130, an expired --timeout at 124. It reports
// false while the context is still live.
func runContextOutcome(ctx context.Context) (int, string, bool) {
	switch ctx.Err() {
	case context.Canceled:
		return 130, runtime.StopReasonForExitCode(130), true
	case context.DeadlineExceeded:
		return 124, runtime.StopReasonForExitCode(124), true
	}
	return 0, "", false
}

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
		WriteSentinelResult(sentinelFile, result.ExitCode, result.SessionID, result.StopReason, runID, result.RetryAfter, result.Reason, "", stderr)
	} else {
		WriteSentinelResult(sentinelFile, result.ExitCode, result.SessionID, result.StopReason, runID, result.RetryAfter, "", "", stderr)
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
	RetryAfter    int64
	LoopDirective string
	LoopLabel     string
	Output        string
	// FinalOutput is the run's final assistant message: the provider's
	// session.end final_output when it supplied one, otherwise the text
	// accumulated from the terminal turn.
	FinalOutput string
	FinalReply  string
	Usage       map[string]any
}

type SessionWaitConfig struct {
	EventCh                <-chan events.Event
	PromptDone             <-chan error
	InterruptCh            <-chan struct{}
	SessionID              string
	RunID                  string
	RunLabel               string
	PermissionClaimScope   string
	AutoApprove            bool
	PermissionClaimTimeout time.Duration
	ProgressTimeout        time.Duration
	// Deadline is the shared run deadline for the whole bounded run (zero
	// means unbounded). Every WaitForSession invocation derives its own timer
	// from it, so parallel team members, sequential phases, retries, and
	// nudges all stop at the same wall-clock limit instead of competing for
	// a single one-shot timer tick.
	Deadline time.Time
	// AcceptSessionID validates an authoritative provider ID before the legacy
	// AdoptSessionID callback is invoked. Rejection is a fatal session identity
	// conflict; continuing on the provisional ID would report false success for
	// a conversation owned by another attempt.
	AcceptSessionID func(string) bool
	AdoptSessionID  func(string)
}

type SessionWaitDeps struct {
	Writer        EventSink
	FileHandler   *permission.FileHandler
	ControlServer *control.ControlServer
	Stderr        io.Writer
	// ProgressTimerC overrides the progress timer channel; nil preserves the
	// normal wall-clock timer behavior.
	ProgressTimerC <-chan time.Time
	// permissionResultSent is a test synchronization hook invoked after a
	// resolver publishes its result to permissionDone.
	permissionResultSent func()
	// permissionJoinTimeout shortens bounded provider joins in tests.
	permissionJoinTimeout time.Duration
	// PreparePermissionClaim lets stable runtimes serialize a reused request ID
	// with direct provider completion. Nil uses ControlServer directly.
	PreparePermissionClaim func(context.Context, string, string, control.PermissionResolverState, []any) bool
	// ProviderTurn coordinates permission answers, cancellation, and eventual
	// provider close with the caller's provider lifecycle. Nil creates a turn
	// suitable for callers that do not own provider teardown.
	ProviderTurn *ProviderTurn
}

// promptSettlementGrace is how long WaitForSession waits for an authoritative
// session.end after a prompt error before classifying the run as a generic
// exit-1 failure.
const promptSettlementGrace = 500 * time.Millisecond

func WaitForSession(ctx context.Context, provider runtime.Provider, cfg SessionWaitConfig, deps SessionWaitDeps) sessionResult {
	var finalStopReason string
	var finalRetryAfter int64
	var promptFailureText string
	var bufferedUsage map[string]any
	// terminalEvent holds a consumed session.end until the prompt settles, so
	// the classification sees both failure sources and the written stop reason
	// never depends on which arrived first.
	var terminalEvent *events.Event
	terminalWritten := false
	sessionEnded := false
	promptReturned := false
	var permissionDone <-chan permissionResult
	permissionCtx, cancelPermission := context.WithCancel(ctx)
	var loopDirective string
	var loopLabel string
	var output strings.Builder
	var finalReply strings.Builder
	var fullReply strings.Builder
	var sessionEndFinalOutput string
	var sessionEndFinalOutputSet bool
	eventChClosed := false
	// interruptedToolFailure records a tool call the backend aborted
	// (tool.call_update status=failed with rawOutput.metadata.interrupted).
	// OpenCode aborts tool execution on a configured permission deny and then
	// still reports stopReason end_turn, so a clean-looking stop reason is not
	// evidence of completion when an abort happened in the same turn.
	interruptedToolFailure := false
	// deniedPermission records an approval request this resolver explicitly
	// rejected. A turn that ends end_turn right after an explicit rejection
	// is the same masquerade, with the denial's provenance on the record.
	deniedPermission := false
	// promptSettlementGrace bounds how long WaitForSession keeps draining the
	// event channel after a prompt error, so a provider's authoritative
	// session.end can still win the run classification. See the PromptDone
	// branch in the wait loop.
	var settlementTimerC <-chan time.Time
	tracker := newStatusTracker(cfg.SessionID, cfg.RunID, cfg.RunLabel)
	permissionJoinTimeout := permissionResolverJoinTimeout
	if deps.permissionJoinTimeout > 0 {
		permissionJoinTimeout = deps.permissionJoinTimeout
	}
	providerTurn := deps.ProviderTurn
	if providerTurn == nil {
		providerTurn = NewProviderLifecycle(provider).NewTurn()
	}
	permissionProvider := permissionTurnProvider{Provider: provider, turn: providerTurn}

	// Context cancellation stops cooperative resolvers; the bound prevents a
	// third-party provider that ignores context from deadlocking teardown.
	synchronizePermission := func() {
		cancelPermission()
		if permissionDone == nil {
			return
		}
		timer := time.NewTimer(permissionJoinTimeout)
		defer timer.Stop()
		select {
		case <-permissionDone:
		case <-timer.C:
		}
		permissionDone = nil
	}

	// Default to stopping the provider. The only return that can skip Cancel
	// is a fully observed provider completion: session.end was authoritative,
	// Prompt returned (successfully, or with an error — a retry-exhausted turn
	// settles both ways), and no permission resolver remains live. A settled
	// turn needs no Cancel, which is why the prompt-error grace path that
	// completes through the session.end event also skips it. The mirror
	// ordering (session.end already forwarded when a teardown branch wins)
	// still Cancels defensively via completeAuthoritativeAfterStop.
	cleanReturn := false
	var stopOnce sync.Once
	stopProvider := func() {
		stopOnce.Do(func() {
			// Cancel before joining: some providers ignore the permission-answer
			// context and only unblock AnswerPermission when Cancel is called.
			cancelPermission()
			cancelProvider(providerTurn, deps.Writer, cfg.SessionID, cfg.RunID, cfg.RunLabel, deps.Stderr, permissionJoinTimeout)
			synchronizePermission()
		})
	}
	defer func() {
		cancelPermission()
		if !cleanReturn {
			stopProvider()
		}
	}()
	settleStopReason := func() {
		if finalStopReason != "end_turn" || (!interruptedToolFailure && !deniedPermission) {
			return
		}
		// A backend aborted a tool call (configured deny, or an explicitly
		// rejected approval request) and still reported a clean end_turn. The
		// turn did not complete its work: report the denial instead of letting
		// it count as success. Ordinary failed tool calls without an abort
		// remain recoverable and keep their own classification.
		emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "stop_reason", "turn reported end_turn after an aborted or rejected tool call; classified as "+runtime.ToolDeniedStopReason, deps.Stderr, cfg.RunLabel)
		finalStopReason = runtime.ToolDeniedStopReason
	}
	complete := func() sessionResult {
		settleStopReason()
		cleanReturn = sessionEnded && promptReturned && permissionDone == nil
		// Surface the authoritative stop reason: callers derive the sentinel's
		// classification from it, and an empty value degrades to exit_1 even
		// when session.end named the real reason.
		finalOutput := fullReply.String()
		if sessionEndFinalOutputSet {
			finalOutput = sessionEndFinalOutput
		}
		return sessionResult{ExitCode: runtime.ExitCodeForStopReason(finalStopReason), StopReason: finalStopReason, RetryAfter: finalRetryAfter, LoopDirective: loopDirective, LoopLabel: loopLabel, Output: output.String(), FinalOutput: finalOutput, FinalReply: finalReply.String(), Usage: bufferedUsage}
	}
	// flushTerminal settles the turn and writes its authoritative session.end.
	// The held terminal record's provider text and the prompt error describe the
	// same failure, so they are classified together once both have settled:
	// neither arrival order decides the outcome, the durable record matches the
	// exit code, and it stays the last event.
	flushTerminal := func() sessionResult {
		if terminalEvent == nil {
			// No terminal record arrived, so the prompt error is the only
			// provider text left; a limit there still writes session.end.
			if reason, retryAfter := runtime.ClassifyProviderLimit("error", promptFailureText); reason != "" {
				return endSession(deps.Writer, cfg.SessionID, cfg.RunID, cfg.RunLabel, reason, retryAfter, deps.Stderr, bufferedUsage, fullReply.String(), finalReply.String())
			}
			return complete()
		}
		if terminalWritten {
			return complete()
		}
		event := *terminalEvent
		backend, _ := event.Fields["stop_reason"].(string)
		settleStopReason()
		text := failureText(event.Fields)
		if promptFailureText != "" {
			if text != "" {
				text += " "
			}
			text += promptFailureText
		}
		if reason, retryAfter := runtime.ClassifyProviderLimit(finalStopReason, text); reason != "" {
			finalStopReason = reason
			if retryAfter > 0 {
				finalRetryAfter = retryAfter
				event.Fields["retry_after"] = retryAfter
			}
		}
		if finalStopReason != backend {
			if backend != "" {
				// Keep the backend's own verdict visible next to the value
				// Avenor adopted.
				event.Fields["classified_from"] = backend
			}
			event.Fields["stop_reason"] = finalStopReason
		}
		if _, ok := event.Fields["final_output"]; !ok && fullReply.Len() > 0 {
			event.Fields["final_output"] = fullReply.String()
		}
		// The held terminal record is the run's last word, so its final_output
		// is the message --result-file may publish.
		if text, ok := event.Fields["final_output"].(string); ok {
			sessionEndFinalOutput, sessionEndFinalOutputSet = text, true
		}
		runtime.SetSessionEndExitCode(event.Fields, finalStopReason)
		if err := deps.Writer.Write(event); err != nil {
			fmt.Fprintf(deps.Stderr, "avenor: write event: %v\n", err)
			return sessionResult{ExitCode: 1}
		}
		terminalWritten = true
		return complete()
	}
	completeAuthoritativeAfterStop := func() (sessionResult, bool) {
		if !sessionEnded {
			return sessionResult{}, false
		}
		// Once the provider's terminal event has been received, teardown may
		// still need Cancel, but it must not supersede or duplicate that result.
		stopProvider()
		return flushTerminal(), true
	}
	terminate := func(stopReason string) sessionResult {
		// The termination branch owns the outcome unless the provider's terminal
		// event was already forwarded. In particular, an expected context.Canceled
		// from the resolver must not replace "cancelled" with a generic handler
		// failure or replace an earlier authoritative result.
		if result, ok := completeAuthoritativeAfterStop(); ok {
			return result
		}
		stopProvider()
		return endSession(deps.Writer, cfg.SessionID, cfg.RunID, cfg.RunLabel, stopReason, 0, deps.Stderr, bufferedUsage, fullReply.String(), finalReply.String())
	}

	chunkBuf := digest.NewChunkBuffer()

	var progressTimerC <-chan time.Time
	var progressTimer *time.Timer
	if deps.ProgressTimerC != nil {
		progressTimerC = deps.ProgressTimerC
	} else if cfg.ProgressTimeout > 0 {
		progressTimer = time.NewTimer(cfg.ProgressTimeout)
		defer progressTimer.Stop()
		progressTimerC = progressTimer.C
	}

	var deadlineTimerC <-chan time.Time
	var deadlineTimer *time.Timer
	if !cfg.Deadline.IsZero() {
		remaining := time.Until(cfg.Deadline)
		if remaining < 0 {
			remaining = 0
		}
		deadlineTimer = time.NewTimer(remaining)
		defer deadlineTimer.Stop()
		deadlineTimerC = deadlineTimer.C
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

	handlePermissionResult := func(res permissionResult) (sessionResult, bool) {
		if res.cancelled {
			return terminate("cancelled"), true
		}
		if res.err != nil {
			// Permission handlers share the session cancellation context. A
			// wrapped context.Canceled therefore preserves the run's
			// cancellation classification even if the permission result wins
			// the select at the same time as ctx.Done.
			if errors.Is(res.err, context.Canceled) {
				return terminate("cancelled"), true
			}
			// The permission error always leaves a diagnostic in the event log,
			// even when the authoritative session-end result owns the outcome.
			emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "permission", fmt.Sprintf("permission handler: %v", res.err), deps.Stderr, cfg.RunLabel)
			if result, ok := completeAuthoritativeAfterStop(); ok {
				return result, true
			}
			return sessionResult{ExitCode: 1}, true
		}
		if res.requestID != "" {
			emitPermissionResponse(res.requestID, res.optionID, res.kind, res.source)
			if !writeStatus(tracker.PermissionAnswered()) {
				return sessionResult{ExitCode: 1}, true
			}
		}
		if res.kind == "reject" {
			deniedPermission = true
		}
		// If session.end + promptDone already arrived while we were waiting for
		// AnswerPermission, exit now that the permission goroutine has resolved.
		if finalStopReason != "" && promptReturned {
			return flushTerminal(), true
		}
		if eventChClosed && finalStopReason == "" {
			return flushTerminal(), true
		}
		return sessionResult{}, false
	}

	for {
		select {
		case event, ok := <-cfg.EventCh:
			if !ok {
				// Nil the channel so it no longer fires on subsequent iterations.
				// If permission resolution is still in flight, keep waiting for its
				// result instead of repeatedly selecting the closed event channel.
				cfg.EventCh = nil
				eventChClosed = true
				if permissionDone != nil {
					continue
				}
				return flushTerminal()
			}
			if event.Event == "session.start" {
				externalID, _ := event.Fields["conversation_id"].(string)
				if externalID != "" && externalID != cfg.SessionID {
					adopted := true
					if cfg.AcceptSessionID != nil {
						adopted = cfg.AcceptSessionID(externalID)
					}
					if !adopted {
						emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "session", fmt.Sprintf("authoritative session ID %q conflicts with an existing session", externalID), deps.Stderr, cfg.RunLabel)
						return sessionResult{ExitCode: 1, StopReason: runtime.SessionIDConflictStopReason}
					}
					cfg.SessionID = externalID
					tracker.sessionID = externalID
					if cfg.AdoptSessionID != nil {
						cfg.AdoptSessionID(externalID)
					}
				}
			}
			if ok && progressTimer != nil {
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
			if event.Event == "tool.call" {
				finalReply.Reset()
			}
			if event.Event == "tool.call_update" {
				if status, _ := event.Fields["status"].(string); status == "failed" {
					if rawOutput, ok := event.Fields["rawOutput"].(map[string]any); ok {
						if meta, ok := rawOutput["metadata"].(map[string]any); ok {
							if interrupted, _ := meta["interrupted"].(bool); interrupted {
								interruptedToolFailure = true
							}
						}
					}
				}
			}
			if event.Event == "agent.message_chunk" || event.Event == "agent.thought_chunk" {
				if text := chunkText(event); text != "" {
					chunkBuf.Append(text)
					if event.Event == "agent.message_chunk" {
						output.WriteString(text)
						finalReply.WriteString(text)
						fullReply.WriteString(text)
					}
					if phase, label, ok := chunkBuf.ScanStatusAngle(); ok {
						if !writeStatus(tracker.ObserveMarker(phase, label)) {
							return sessionResult{ExitCode: 1}
						}
						markerHandled = true
					} else if phase, label, ok := chunkBuf.ScanStatusMarker(); ok {
						if !writeStatus(tracker.ObserveMarker(phase, label)) {
							return sessionResult{ExitCode: 1}
						}
						markerHandled = true
					}
					// Loop markers are intentionally left out of markerHandled.
					// The raw event still reaches tracker.Observe so the caller
					// sees the full message text including the marker.
					if dir, lbl, ok := chunkBuf.ScanLoopMarker(); ok {
						if phaseconfig.LoopDirectiveSeverity(dir) > phaseconfig.LoopDirectiveSeverity(loopDirective) {
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
				event = events.Clone(event)
				if event.Fields == nil {
					event.Fields = map[string]any{}
				}
				requestID, _ := event.Fields["request_id"].(string)
				requiresUserInput, _ := event.Fields["requires_user_input"].(bool)
				resolverState := effectivePermissionResolverState(
					cfg.AutoApprove,
					requiresUserInput,
					deps.ControlServer != nil && deps.ControlServer.HasClients(),
					deps.FileHandler != nil,
				)
				event.Fields["resolver"] = resolverState.String()
				if permissionDone != nil {
					// A non-nil channel may hold a completed result that has not won
					// the outer select. Consume only ready results; never wait for an
					// active resolver or allow the new request to overlap it.
					select {
					case res := <-permissionDone:
						permissionDone = nil
						if result, done := handlePermissionResult(res); done {
							return result
						}
					default:
						emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "permission", "another permission request is already pending", deps.Stderr, cfg.RunLabel)
						return sessionResult{ExitCode: 1}
					}
				}
				claimConflict := false
				if requestID != "" && deps.ControlServer != nil {
					options, _ := event.Fields["options"].([]any)
					if deps.PreparePermissionClaim != nil {
						claimConflict = !deps.PreparePermissionClaim(permissionCtx, cfg.PermissionClaimScope, requestID, resolverState, options)
					} else {
						claimConflict = !deps.ControlServer.PreparePermissionClaim(cfg.PermissionClaimScope, requestID, resolverState, options)
					}
				}
				if claimConflict && permissionCtx.Err() != nil {
					return terminate("cancelled")
				}
				if claimConflict {
					emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "permission", "another permission request is already pending", deps.Stderr, cfg.RunLabel)
					return sessionResult{ExitCode: 1}
				}
				if err := deps.Writer.Write(event); err != nil {
					if requestID != "" && deps.ControlServer != nil {
						deps.ControlServer.EndPermissionClaim(cfg.PermissionClaimScope, requestID)
					}
					fmt.Fprintf(deps.Stderr, "avenor: write event: %v\n", err)
					return sessionResult{ExitCode: 1}
				}
				if requestID == "" {
					// Malformed request from backend: no request_id means we cannot
					// call AnswerPermission. Emit an error and stay in waiting state.
					emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "permission", "permission.request missing request_id", deps.Stderr, cfg.RunLabel)
					continue
				}
				done := make(chan permissionResult, 1)
				permissionDone = done
				go func() {
					res := resolvePermission(permissionCtx, permissionProvider, deps.FileHandler, deps.ControlServer, event, cfg.SessionID, cfg.PermissionClaimScope, requestID, cfg.AutoApprove, cfg.PermissionClaimTimeout)
					done <- res
					if deps.permissionResultSent != nil {
						deps.permissionResultSent()
					}
				}()
				continue
			}
			if event.Event == "session.end" {
				event = events.Clone(event)
				if event.Fields == nil {
					event.Fields = map[string]any{}
				}
				finalStopReason, _ = event.Fields["stop_reason"].(string)
				terminalEvent = &event
				sessionEnded = true
				if promptReturned && permissionDone == nil {
					return flushTerminal()
				}
				continue
			}
			if err := deps.Writer.Write(event); err != nil {
				fmt.Fprintf(deps.Stderr, "avenor: write event: %v\n", err)
				return sessionResult{ExitCode: 1}
			}
		case err := <-cfg.PromptDone:
			promptReturned = true
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return terminate("cancelled")
				}
				promptFailureText = err.Error()
				// The prompt error always leaves a diagnostic in the event log,
				// even when the authoritative session-end result owns the outcome.
				emitErrorEvent(deps.Writer, cfg.SessionID, cfg.RunID, "prompt", fmt.Sprintf("prompt: %v", err), deps.Stderr, cfg.RunLabel)
				if result, ok := completeAuthoritativeAfterStop(); ok {
					return result
				}
				// The provider may be about to deliver the authoritative
				// session.end: pi's retry exhaustion settles the turn right around
				// the prompt error. Keep draining under a short grace window so the
				// terminal event's provider text is classified with the prompt error
				// instead of falling back to a generic exit-1 failure.
				if cfg.EventCh != nil && settlementTimerC == nil {
					settlementTimer := time.NewTimer(promptSettlementGrace)
					defer settlementTimer.Stop()
					settlementTimerC = settlementTimer.C
					continue
				}
				return flushTerminal()
			}
			if finalStopReason != "" && permissionDone == nil {
				return flushTerminal()
			}
		case <-settlementTimerC:
			// The grace expired: flush whatever settled, including a terminal
			// record that arrived after the prompt error.
			return flushTerminal()
		case res := <-permissionDone:
			permissionDone = nil
			if result, done := handlePermissionResult(res); done {
				return result
			}
		case <-cfg.InterruptCh:
			return terminate("cancelled")
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return terminate("timeout")
			}
			return terminate("cancelled")
		case <-progressTimerC:
			return terminate("progress_timeout")
		case <-deadlineTimerC:
			return terminate("timeout")
		}
	}
}

func cancelProvider(turn *ProviderTurn, writer EventSink, sessionID, runID, runLabel string, stderr io.Writer, timeout time.Duration) {
	done := turn.RequestCancel(sessionID, timeout)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var err error
	select {
	case <-done:
		err = turn.cancelError()
	case <-timer.C:
		err = context.DeadlineExceeded
	}
	if err != nil {
		emitErrorEvent(writer, sessionID, runID, "cancel", fmt.Sprintf("cancel session: %v", err), stderr, runLabel)
	}
}

// failureText returns the terminal failure text a backend owns on session.end.
// Only provider-owned fields count: model-authored prose in a finish summary
// never classifies a run.
func failureText(fields map[string]any) string {
	for _, key := range []string{"error_message", "error", "message"} {
		if text, _ := fields[key].(string); text != "" {
			return text
		}
	}
	return ""
}

func endSession(writer EventSink, sessionID, runID, runLabel, stopReason string, retryAfter int64, stderr io.Writer, usage map[string]any, fullFinalOutput, lastBlockFinalReply string) sessionResult {
	fields := map[string]any{"stop_reason": stopReason}
	exitCode := runtime.SetSessionEndExitCode(fields, stopReason)
	if retryAfter > 0 {
		fields["retry_after"] = retryAfter
	}
	if usage != nil {
		fields["usage"] = usage
	}
	if fullFinalOutput != "" {
		fields["final_output"] = fullFinalOutput
	}
	if err := writer.Write(events.Event{
		Event:     "session.end",
		SessionID: sessionID,
		Fields:    fields,
	}); err != nil {
		fmt.Fprintf(stderr, "avenor: write terminal event: %v\n", err)
		return sessionResult{ExitCode: 1}
	}
	return sessionResult{ExitCode: exitCode, StopReason: stopReason, RetryAfter: retryAfter, Output: fullFinalOutput, FinalOutput: fullFinalOutput, FinalReply: lastBlockFinalReply, Usage: usage}
}

type eventWriter struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
}

type EventMetadata struct {
	mu           sync.Mutex
	runID        string
	runLabel     string
	runtimeID    string
	latestSeq    int64
	workflowID   string
	nodeID       string
	activationID string
	attemptID    string
	now          func() time.Time
}

func NewEventMetadata(runID, runLabel, runtimeID string) *EventMetadata {
	return &EventMetadata{
		runID:     runID,
		runLabel:  runLabel,
		runtimeID: runtimeID,
		now:       time.Now,
	}
}

// WithWorkflow attaches workflow execution identity to the metadata so every
// stamped event carries it as workflow metadata on the run. It must be called
// once, before the metadata is used concurrently.
func (m *EventMetadata) WithWorkflow(workflowID, nodeID, activationID, attemptID string) *EventMetadata {
	m.workflowID = workflowID
	m.nodeID = nodeID
	m.activationID = activationID
	m.attemptID = attemptID
	return m
}

// Tag enriches the event with run identity, workflow execution identity, and
// a timestamp, without assigning a sequence number. Callers that publish
// through a ControlServer let CanonicalizeEvent own the seq so every emitter
// shares one per-runtime counter.
func (m *EventMetadata) Tag(event events.Event) events.Event {
	if m == nil {
		return events.Clone(event)
	}
	// Tagging happens before durable NDJSON persistence, so it must not alter
	// the terminal reply. Control/status paths apply their own preview bound.
	out := events.Clone(event)
	if out.Fields == nil {
		out.Fields = map[string]any{}
	}
	if out.SessionID == "" {
		if sessionID, _ := out.Fields["session_id"].(string); sessionID != "" {
			out.SessionID = sessionID
		}
	}
	if _, ok := out.Fields["runtime_id"]; !ok && m.runtimeID != "" {
		out.Fields["runtime_id"] = m.runtimeID
	}
	if _, ok := out.Fields["run_id"]; !ok && m.runID != "" {
		out.Fields["run_id"] = m.runID
	}
	if _, ok := out.Fields["run_label"]; !ok && m.runLabel != "" {
		out.Fields["run_label"] = m.runLabel
	}
	if _, ok := out.Fields["workflow_id"]; !ok && m.workflowID != "" {
		out.Fields["workflow_id"] = m.workflowID
	}
	if _, ok := out.Fields["node_id"]; !ok && m.nodeID != "" {
		out.Fields["node_id"] = m.nodeID
	}
	if _, ok := out.Fields["activation_id"]; !ok && m.activationID != "" {
		out.Fields["activation_id"] = m.activationID
	}
	if _, ok := out.Fields["attempt_id"]; !ok && m.attemptID != "" {
		out.Fields["attempt_id"] = m.attemptID
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := events.Int64(out.Fields["ts"]); !ok {
		now := time.Now
		if m.now != nil {
			now = m.now
		}
		out.Fields["ts"] = now().UnixMilli()
	}
	return out
}

func (m *EventMetadata) Stamp(event events.Event) events.Event {
	out := m.Tag(event)
	m.mu.Lock()
	defer m.mu.Unlock()
	if seq, ok := events.Int64(out.Fields["seq"]); ok {
		if seq > m.latestSeq {
			m.latestSeq = seq
		}
	} else {
		m.latestSeq++
		out.Fields["seq"] = m.latestSeq
	}
	return out
}

type EventSink interface {
	Write(events.Event) error
	Close() error
}

type fanoutWriter struct {
	mu       sync.Mutex
	base     EventSink
	control  *control.ControlServer
	metadata *EventMetadata
}

func newFanoutWriter(base EventSink, cs *control.ControlServer, metadata *EventMetadata) EventSink {
	return &fanoutWriter{base: base, control: cs, metadata: metadata}
}

// StampFanoutEvent is the single authority for assigning a per-runtime seq on
// the fan-out path. When a control server is present it owns the per-runtime
// seq counter: the event is tagged with run identity and then canonicalized
// there, so the durable log and the control stream carry one shared sequence
// per runtime and subscriber dedup, replay cursors, and persistence can never
// disagree about event order. Without a control server, the EventMetadata
// stamps the seq itself; with neither, the event only carries its runtime
// identity. The returned event is the lossless durable form. A session.end
// leaves here without an end_kind: only the run-level writer tags that, after
// stamping, so a backend's own turn end cannot claim the run's terminal record.
func StampFanoutEvent(ev events.Event, runtimeID string, metadata *EventMetadata, cs *control.ControlServer) events.Event {
	if _, tagged := ev.Fields["end_kind"]; tagged && ev.Event == "session.end" {
		ev.Fields = maps.Clone(ev.Fields)
		delete(ev.Fields, "end_kind")
	}
	if cs != nil {
		if metadata != nil {
			ev = metadata.Tag(ev)
		} else if runtimeID != "" {
			if ev.Fields == nil {
				ev.Fields = map[string]any{}
			}
			if _, ok := ev.Fields["runtime_id"]; !ok {
				ev.Fields["runtime_id"] = runtimeID
			}
		}
		return cs.CanonicalizeEvent(ev)
	}
	if metadata != nil {
		return metadata.Stamp(ev)
	}
	if runtimeID != "" {
		if ev.Fields == nil {
			ev.Fields = map[string]any{}
		}
		if _, ok := ev.Fields["runtime_id"]; !ok {
			ev.Fields["runtime_id"] = runtimeID
		}
	}
	return ev
}

func (f *fanoutWriter) Write(event events.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	stamped := StampFanoutEvent(event, "", f.metadata, f.control)
	if err := f.base.Write(stamped); err != nil {
		return err
	}
	if f.control != nil {
		f.control.PublishCanonicalEvent(stamped)
	}
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
			return permission.NormalizeOptionKind(kind)
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
		if permission.NormalizeOptionKind(kind) == wantKind {
			optID, _ := m["optionId"].(string)
			return optID, wantKind
		}
	}
	return "", ""
}

func effectivePermissionResolverState(autoApprove, requiresUserInput, hasControlClient, hasFileHandler bool) control.PermissionResolverState {
	switch {
	case autoApprove && !requiresUserInput:
		return control.PermissionResolverAutomatic
	case hasControlClient:
		return control.PermissionResolverReserved
	case hasFileHandler:
		return control.PermissionResolverFile
	default:
		return control.PermissionResolverNoResolver
	}
}

func resolvePermission(
	ctx context.Context,
	provider runtime.Provider,
	fileHandler *permission.FileHandler,
	controlServer *control.ControlServer,
	event events.Event,
	sessionID string,
	claimScope string,
	requestID string,
	autoApprove bool,
	claimTimeout time.Duration,
) permissionResult {
	options, _ := event.Fields["options"].([]any)
	answerControl := func(ans control.PermissionAnswer) permissionResult {
		kind := permissionKindFromOptionID(ans.OptionID, options)
		switch kind {
		case "allow", "reject":
		default:
			if controlServer != nil {
				controlServer.EndPermissionClaim(claimScope, requestID)
			}
			return permissionResult{err: fmt.Errorf("unknown option_id %q for request %q", ans.OptionID, requestID)}
		}
		resp := runtime.PermissionResponse{Allow: kind == "allow", OptionID: ans.OptionID, Message: ans.Message}
		if err := provider.AnswerPermission(ctx, sessionID, requestID, resp); err != nil {
			if controlServer != nil {
				controlServer.EndPermissionClaim(claimScope, requestID)
			}
			return permissionResult{err: err}
		}
		if controlServer != nil {
			controlServer.MarkPermissionClaimResolved(claimScope, requestID, "control", ans.OptionID, ans.Message)
		}
		return permissionResult{requestID: requestID, optionID: ans.OptionID, kind: kind, source: "control"}
	}
	requiresUserInput, _ := event.Fields["requires_user_input"].(bool)
	if autoApprove && !requiresUserInput {
		optionID, kind := firstOptionKind(options, "allow")
		resp := runtime.PermissionResponse{Allow: true, OptionID: optionID}
		if err := provider.AnswerPermission(ctx, sessionID, requestID, resp); err != nil {
			if controlServer != nil {
				controlServer.EndPermissionClaim(claimScope, requestID)
			}
			return permissionResult{err: err}
		}
		if controlServer != nil {
			controlServer.MarkPermissionClaimResolved(claimScope, requestID, "avenor", optionID, "")
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
	// before it sends answer_permission. When all clients disconnect, the
	// control server closes disconnectCh so we fall through to the file-handler
	// rather than blocking indefinitely. An explicit claimTimeout > 0 also
	// arms a wall-clock timer for unattended automation.
	resolverState := control.PermissionResolverUnknown
	if controlServer != nil {
		resolverState = controlServer.PermissionResolverState(claimScope, requestID)
	}
	if controlServer != nil &&
		(resolverState == control.PermissionResolverReserved ||
			(resolverState == control.PermissionResolverUnknown && controlServer.HasClients())) {
		answerCh, disconnectCh, ok := controlServer.BeginPermissionClaim(claimScope, requestID)
		// If !ok, this scope already has a claim for the same request; fall
		// through to the file-handler block below.
		if ok {
			var claimTimer <-chan time.Time
			var timerStop func()
			if claimTimeout > 0 {
				t := time.NewTimer(claimTimeout)
				claimTimer = t.C
				timerStop = func() { t.Stop() }
			}
			// handoffToFallback transitions the claim to the file-handler or
			// no-resolver state and returns a result if a queued answer was
			// consumed. When no answer is queued, returns a zero-value
			// permissionResult as a sentinel: the caller checks source != ""
			// || err != nil to distinguish it from real results. This works
			// because answerControl always sets source or err on every path.
			handoffToFallback := func() permissionResult {
				next := control.PermissionResolverNoResolver
				if fileHandler != nil {
					next = control.PermissionResolverFile
				}
				if ans, answered := controlServer.HandoffPermissionClaim(claimScope, requestID, next); answered {
					return answerControl(ans)
				}
				return permissionResult{} // sentinel: continue to file-handler block
			}
			select {
			case ans := <-answerCh:
				if timerStop != nil {
					timerStop()
				}
				return answerControl(ans)
			case <-ctx.Done():
				if timerStop != nil {
					timerStop()
				}
				if ans, answered := controlServer.HandoffPermissionClaim(claimScope, requestID, control.PermissionResolverResolved); answered {
					return answerControl(ans)
				}
				return permissionResult{err: ctx.Err()}
			case <-disconnectCh:
				if timerStop != nil {
					timerStop()
				}
				if res := handoffToFallback(); res.source != "" || res.err != nil {
					return res
				}
			case <-claimTimer:
				timerStop()
				if res := handoffToFallback(); res.source != "" || res.err != nil {
					return res
				}
			}
		}
	}

	if fileHandler != nil {
		if controlServer != nil {
			controlServer.SetPermissionResolverState(claimScope, requestID, control.PermissionResolverFile)
		}
		// WaitForSession owns the canonical permission.request event and its
		// effective resolver hint. The file handler only writes its protocol
		// file and waits for the response; it must not emit a duplicate event.
		res, err := fileHandler.Handle(ctx, provider, event)
		if err != nil {
			if controlServer != nil {
				controlServer.EndPermissionClaim(claimScope, requestID)
			}
			return permissionResult{err: err}
		}
		if res.Cancelled {
			if controlServer != nil {
				controlServer.EndPermissionClaim(claimScope, requestID)
			}
			return permissionResult{requestID: res.RequestID, optionID: res.OptionID, kind: permissionKindFromOptionID(res.OptionID, options), cancelled: true, source: "file"}
		}
		if controlServer != nil {
			controlServer.MarkPermissionClaimResolved(claimScope, requestID, "file", res.OptionID, res.Message)
		}
		return permissionResult{requestID: res.RequestID, optionID: res.OptionID, kind: permissionKindFromOptionID(res.OptionID, options), source: "file"}
	}

	// no resolver: leave backend waiting
	if controlServer != nil {
		if claimScope == "" {
			controlServer.EndPermissionClaim(claimScope, requestID)
		} else {
			controlServer.SetPermissionResolverState(claimScope, requestID, control.PermissionResolverNoResolver)
		}
	}
	return permissionResult{kind: "", source: "none"}
}

func NewEventWriter(path string) (*eventWriter, error) {
	if path == "" {
		return &eventWriter{encoder: json.NewEncoder(io.Discard)}, nil
	}
	file, err := openEventLogFile(path, os.O_TRUNC)
	if err != nil {
		return nil, err
	}
	return &eventWriter{
		file:    file,
		encoder: json.NewEncoder(file),
	}, nil
}

// NewEventWriterExclusive opens an event log that must not exist yet. A
// resumed turn starts a new log file: O_EXCL guarantees the writer never
// truncates a prior turn's log (including a hard link to one) or follows a
// symlink left at the path.
func NewEventWriterExclusive(path string) (*eventWriter, error) {
	file, err := openEventLogFile(path, os.O_EXCL)
	if err != nil {
		return nil, err
	}
	return &eventWriter{
		file:    file,
		encoder: json.NewEncoder(file),
	}, nil
}

// openEventLogFile opens an event log with the constructor's shared hardening:
// O_NOFOLLOW refuses a final symlink, so a link left at --on-event cannot
// redirect this run's event log into another artifact's file. The log carries
// prompt and turn content, so it is owner-only like every other run artifact;
// a leftover file keeps its mode, so the opened file is tightened as well.
// extra carries the caller's truncation discipline: O_TRUNC for a fresh run's
// log, O_EXCL when the path must not already exist.
func openEventLogFile(path string, extra int) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|extra|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("event log path %s is a symlink; symlinks are refused", path)
		}
		return nil, err
	}
	// Exclusive creation already bounds the mode to 0600; only a reused file
	// needs tightening after open.
	if extra&os.O_EXCL == 0 {
		if err := file.Chmod(0o600); err != nil {
			file.Close()
			return nil, err
		}
	}
	return file, nil
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

// resolveAgentModel resolves the configured model for the given agent name
// based on the active backend.
//
//   - opencode* backends: reads opencode.json / opencode.jsonc
//   - pi backend:          reads pi's agents.json
//   - all other backends:  returns ("", nil) (caller provides --model explicitly)
//
// Returns ("", nil) if not found. Returns a non-nil error only when an
// opencode config file exists but cannot be parsed (malformed JSON); pi
// config errors are silently treated as not-found.
func resolveAgentModel(agentName string, backend string, getenv func(string) string) (string, error) {
	if strings.HasPrefix(backend, "opencode") {
		for _, path := range opencodeConfigPaths(getenv) {
			model, err := readAgentModel(path, agentName)
			if err != nil {
				return "", err
			}
			if model != "" {
				return model, nil
			}
		}
	}
	if backend == "pi" {
		model, err := readPiAgentModel(agentName, getenv)
		if err != nil || model == "" {
			return "", nil
		}
		return model, nil
	}
	return "", nil
}

func opencodeConfigPaths(getenv func(string) string) []string {
	if getenv == nil {
		getenv = os.Getenv
	}
	if path := getenv("OPENCODE_CONFIG"); path != "" {
		return []string{path}
	}
	if dir := getenv("OPENCODE_CONFIG_DIR"); dir != "" {
		return []string{filepath.Join(dir, "opencode.json"), filepath.Join(dir, "opencode.jsonc")}
	}
	if xdg := getenv("XDG_CONFIG_HOME"); xdg != "" {
		dir := filepath.Join(xdg, "opencode")
		return []string{filepath.Join(dir, "opencode.json"), filepath.Join(dir, "opencode.jsonc")}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil
	}
	dir := filepath.Join(home, ".config", "opencode")
	return []string{filepath.Join(dir, "opencode.json"), filepath.Join(dir, "opencode.jsonc")}
}

func readAgentModel(path, agentName string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil // file doesn't exist — not an error
		}
		return "", fmt.Errorf("read opencode config %s: %w", path, err)
	}
	var config struct {
		Agent map[string]struct {
			Model string `json:"model"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return "", fmt.Errorf("parse opencode config %s: %w", path, err)
	}
	if agent, ok := config.Agent[agentName]; ok && agent.Model != "" {
		return agent.Model, nil
	}
	return "", nil
}

// readPiAgentModel reads pi's agents.json and returns the configured model
// for the given agent name. Returns ("", nil) if not found.
func readPiAgentModel(agentName string, getenv func(string) string) (string, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	agentDir := getenv("PI_CODING_AGENT_DIR")
	if agentDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", nil
		}
		agentDir = filepath.Join(home, ".pi", "agent")
	}
	path := filepath.Join(agentDir, "agents.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("read pi agents.json %s: %w", path, err)
	}
	var config map[string]struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return "", fmt.Errorf("parse pi agents.json %s: %w", path, err)
	}
	if agent, ok := config[agentName]; ok && agent.Model != "" {
		return agent.Model, nil
	}
	return "", nil
}
