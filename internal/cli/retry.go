package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/factory"
)

var newProvider = factory.NewProvider

// attemptResult holds the outcome of a single session attempt.
type attemptResult struct {
	exitCode      int
	sessionID     string
	stopReason    string
	retryAfter    int64
	loopDirective string
	loopLabel     string
	output        string
	finalOutput   string
	finalReply    string
	usage         map[string]any
	err           error
}

type attemptConfig struct {
	startOptions           runtime.StartOptions
	backend                string
	resumeID               string
	sessionBackends        *sessionBackendMap
	initialPrompt          string
	resultFile             string
	runID                  string
	runLabel               string
	autoApprove            bool
	permissionClaimTimeout time.Duration
	progressTimeout        time.Duration
}

type attemptDeps struct {
	writer        EventSink
	fileHandler   *permission.FileHandler
	controlServer *control.ControlServer
	stderr        io.Writer
}

// resumeSession resumes an existing session after cancellation so a follow-up
// prompt can be sent without respawning the provider.
func resumeSession(ctx context.Context, provider runtime.Provider, backend string, opts runtime.StartOptions, sessionID string) (runtime.Session, error) {
	return StartSession(ctx, provider, backend, dropStartOnlyOptions(backend, opts), sessionID)
}

// dropStartOnlyOptions removes options a resumed session cannot honor. Only
// the claude backends resume without per-call options: they drop their
// start-only options so an internal resume is not aborted. Backends that
// resume with options keep them.
func dropStartOnlyOptions(backend string, opts runtime.StartOptions) runtime.StartOptions {
	if backend == backendClaude || backend == backendClaudeChannel {
		opts.Thinking = ""
		opts.ClaudeMCPWait = 0
	}
	return opts
}

// settledResultText is the text --result-file may publish: the final message of
// a run whose last turn settled on the backend's terminal event. A cancelled or
// timed-out run stopped mid-answer, and an attempt or resume that failed never
// reached that event, so none of them has a final message for a reader.
func settledResultText(outcome attemptResult) string {
	if outcome.err != nil {
		return ""
	}
	switch outcome.stopReason {
	case "", "cancelled", "timeout", "progress_timeout":
		return ""
	}
	return outcome.finalOutput
}

// runSingleAttempt creates a fresh provider, starts or resumes a session,
// sends the initial prompt, and runs a multi-turn event loop. When the
// control server queues or interrupts a prompt, the loop restarts without
// returning.
func runSingleAttempt(
	ctx context.Context,
	cfg attemptConfig,
	deps attemptDeps,
) attemptResult {
	identity, err := cfg.sessionBackends.resolveResume(cfg.resumeID, cliSessionIdentity{
		backend: cfg.backend, agent: cfg.startOptions.Agent, model: cfg.startOptions.Model,
		agentProfile: cfg.startOptions.AgentProfile, launchWrapper: cfg.startOptions.LaunchWrapper,
	})
	if err != nil {
		fmt.Fprintf(deps.stderr, "avenor: %v\n", err)
		return attemptResult{exitCode: 1, err: err}
	}
	cfg.backend = identity.backend
	cfg.startOptions.Agent = identity.agent
	cfg.startOptions.Model = identity.model
	cfg.startOptions.AgentProfile = identity.agentProfile

	provider, err := newProvider(cfg.startOptions, cfg.backend)
	if err != nil {
		fmt.Fprintf(deps.stderr, "avenor: create provider: %v\n", err)
		return attemptResult{exitCode: 1}
	}
	providerLifecycle := NewProviderLifecycle(provider)
	defer providerLifecycle.RequestClose()

	session, err := StartSession(ctx, provider, cfg.backend, cfg.startOptions, cfg.resumeID)
	if err != nil {
		fmt.Fprintf(deps.stderr, "avenor: start session: %v\n", err)
		return attemptResult{exitCode: 1}
	}

	attempt := &cliSessionAttempt{}
	if err := cfg.sessionBackends.claim(session.SessionID, identity, attempt, cfg.resumeID); err != nil {
		fmt.Fprintf(deps.stderr, "avenor: %v\n", err)
		return attemptResult{exitCode: 1, err: err}
	}
	provisionalID := session.SessionID
	defer func() { cfg.sessionBackends.finish(provisionalID, session.SessionID, attempt) }()

	prompt := cfg.initialPrompt

	// Check for interrupts that arrived between turns (during backoff).
	if deps.controlServer != nil {
		if text := deps.controlServer.ConsumeInterrupt(); text != "" {
			prompt = text
			if _, err := resumeSession(ctx, provider, cfg.backend, cfg.startOptions, session.SessionID); err != nil {
				fmt.Fprintf(deps.stderr, "avenor: resume for interrupt: %v\n", err)
				return attemptResult{exitCode: 1, sessionID: session.SessionID}
			}
		}
	}

	// The attempt is about to send its first prompt, so an answer from an
	// earlier run at the same path must not outlive this one. A run whose
	// provider or session never started leaves that answer alone.
	if cfg.resultFile != "" {
		removeOrLog(cfg.resultFile, deps.stderr)
	}

	var accDirective string
	var accLabel string

	for {
		// Fresh event context per turn so interrupt_and_prompt cancels
		// the subscription cleanly.
		eventCtx, cancelEvents := context.WithCancel(ctx)
		eventCh, err := provider.Events(eventCtx, session.SessionID)
		if err != nil {
			cancelEvents()
			fmt.Fprintf(deps.stderr, "avenor: subscribe events: %v\n", err)
			return attemptResult{exitCode: 1, sessionID: session.SessionID}
		}

		var interruptCh <-chan struct{}
		if deps.controlServer != nil {
			deps.controlServer.ResetInterrupt()
			interruptCh = deps.controlServer.InterruptChan()
		}

		// Use a background context for Prompt so it survives process-cancellation
		// (interrupt_and_prompt must not kill the process).
		providerTurn := providerLifecycle.NewTurn()
		promptDone := make(chan error, 1)
		go func() {
			promptDone <- providerTurn.Prompt(context.Background(), session.SessionID, prompt)
		}()

		result := WaitForSession(ctx, provider, SessionWaitConfig{
			EventCh:                eventCh,
			PromptDone:             promptDone,
			InterruptCh:            interruptCh,
			SessionID:              session.SessionID,
			RunID:                  cfg.runID,
			RunLabel:               cfg.runLabel,
			AutoApprove:            cfg.autoApprove,
			PermissionClaimTimeout: cfg.permissionClaimTimeout,
			ProgressTimeout:        cfg.progressTimeout,
			// The run deadline travels on the attempt context (see the CLI's
			// run-scoped context.WithDeadline): each waiter derives its own
			// timer, so attempts never compete for a single timer tick.
			Deadline: deadlineFromContext(ctx),
			AcceptSessionID: func(externalID string) bool {
				return cfg.sessionBackends.adopt(session.SessionID, externalID, attempt)
			},
			AdoptSessionID: func(externalID string) {
				session.SessionID = externalID
			},
		}, SessionWaitDeps{
			Writer:        deps.writer,
			FileHandler:   deps.fileHandler,
			ControlServer: deps.controlServer,
			Stderr:        deps.stderr,
			ProviderTurn:  providerTurn,
		})
		exitCode := result.ExitCode
		stopReason := result.StopReason
		cancelEvents()

		if phaseconfig.LoopDirectiveSeverity(result.LoopDirective) > phaseconfig.LoopDirectiveSeverity(accDirective) {
			accDirective = result.LoopDirective
			accLabel = result.LoopLabel
		}

		// An authoritative ID conflict invalidates this attempt. Do not service
		// queued control prompts by resuming its provisional session ID.
		if result.StopReason == runtime.SessionIDConflictStopReason {
			return attemptResult{exitCode: exitCode, sessionID: session.SessionID, stopReason: result.StopReason, loopDirective: accDirective, loopLabel: accLabel, output: result.Output, finalOutput: result.FinalOutput, finalReply: result.FinalReply, usage: result.Usage}
		}

		if deps.controlServer != nil {
			// Check interrupt first (priority over queued prompts) to avoid
			// losing the interrupt when exitCode==0 overlaps with a queued prompt.
			if interruptText := deps.controlServer.ConsumeInterrupt(); interruptText != "" {
				if _, err := resumeSession(ctx, provider, cfg.backend, cfg.startOptions, session.SessionID); err != nil {
					fmt.Fprintf(deps.stderr, "avenor: resume after cancel: %v\n", err)
					// A failed resume leaves the interrupted reply a prefix, not a
					// final message.
					return attemptResult{exitCode: 1, sessionID: session.SessionID, stopReason: stopReason, loopDirective: accDirective, loopLabel: accLabel, output: result.Output, finalReply: result.FinalReply, usage: result.Usage}
				}
				prompt = interruptText
				continue
			}
			if exitCode == 0 {
				if nextPrompt := deps.controlServer.DequeuePrompt(); nextPrompt != "" {
					if _, err := resumeSession(ctx, provider, cfg.backend, cfg.startOptions, session.SessionID); err != nil {
						fmt.Fprintf(deps.stderr, "avenor: resume after end_turn: %v\n", err)
						return attemptResult{exitCode: 1, sessionID: session.SessionID, stopReason: stopReason, loopDirective: accDirective, loopLabel: accLabel, output: result.Output, finalReply: result.FinalReply, usage: result.Usage}
					}
					prompt = nextPrompt
					continue
				}
			}
		}

		return attemptResult{exitCode: exitCode, sessionID: session.SessionID, stopReason: stopReason, retryAfter: result.RetryAfter, loopDirective: accDirective, loopLabel: accLabel, output: result.Output, finalOutput: result.FinalOutput, finalReply: result.FinalReply, usage: result.Usage}
	}
}

// backoffDelay returns the retry sleep duration for the nth attempt (1-indexed).
// Starts at 2s, doubles each time, capped at 30s.
func backoffDelay(attempt int) time.Duration {
	seconds := 2 << uint(attempt-1)
	if seconds > 30 {
		seconds = 30
	}
	return time.Duration(seconds) * time.Second
}

// writeRetryEvent emits an avenor.retry event to the writer.
func writeRetryEvent(writer EventSink, sessionID, runID string, attempt, maxRetries int, runLabel ...string) error {
	fields := map[string]any{
		"attempt":     attempt,
		"max_retries": maxRetries,
		"ts":          time.Now().UnixMilli(),
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	if len(runLabel) > 0 && runLabel[0] != "" {
		fields["run_label"] = runLabel[0]
	}
	return writer.Write(events.Event{
		Event:     "avenor.retry",
		SessionID: sessionID,
		Fields:    fields,
	})
}
