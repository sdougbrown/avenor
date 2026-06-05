package looprunner

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

type NestedResult struct {
	ExitCode   int
	StopReason string
	SessionID  string
	Reason     string
}

type RunOptions struct {
	WorkDir    string
	RunID      string
	EventSink  phaseconfig.EventWriter
	Config     *LoopConfig
	MaxRetries int
	Broker     *broker.Broker
	SeedMessage *broker.AgentMessage // pushed to the attempt's brokerRunID after creation
	PhaseAttempt func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error)
	NestedRun  func(ctx context.Context, configPath string, runType string) (NestedResult, error)
	ConfigDir  string
}

type PhaseAttemptResult struct {
	ExitCode      int
	SessionID     string
	StopReason    string
	LoopDirective string
	LoopLabel     string
	BrokerRunID   string
}

type RunResult struct {
	ExitCode   int
	StopReason string
	SessionID  string
	Reason     string
}

func Run(ctx context.Context, opts RunOptions) (RunResult, error) {
	if err := emitLoopStart(opts.EventSink, opts.RunID, opts.Config.MaxIterations, len(opts.Config.Pre), len(opts.Config.Loop), len(opts.Config.Post)); err != nil {
		return RunResult{}, err
	}

	prevPhaseCommit := phaseconfig.CaptureHeadCommit(opts.WorkDir)

	if early, err := runSequentialPhases(ctx, opts, opts.Config.Pre, "pre", 0, &prevPhaseCommit); err != nil {
		return RunResult{}, err
	} else if early != nil {
		return *early, nil
	}

	if len(opts.Config.Loop) == 0 {
		if early, err := runSequentialPhases(ctx, opts, opts.Config.Post, "post", 0, &prevPhaseCommit); err != nil {
			return RunResult{}, err
		} else if early != nil {
			return *early, nil
		}
		_ = emitLoopEnd(opts.EventSink, opts.RunID, "end_turn", "", 0)
		return RunResult{ExitCode: 0, StopReason: "end_turn"}, nil
	}

	prevSessionIDs := make(map[int]string)
	iterationsCompleted := 0

	for iteration := 1; iteration <= opts.Config.MaxIterations; iteration++ {
		if err := ctx.Err(); err != nil {
			return cancelledRunResult(ctx, opts, iterationsCompleted)
		}

		for phaseIdx, phase := range opts.Config.Loop {
			prevSessionID := ""
			if phase.ResumeFromPrevious && phaseIdx > 0 {
				prevSessionID = prevSessionIDs[phaseIdx-1]
			}

			result, err := executePhase(ctx, opts, phase, "loop", iteration, prevSessionID, prevPhaseCommit)
			if err != nil {
				_ = emitLoopEnd(opts.EventSink, opts.RunID, "phase_failure", "", iterationsCompleted)
				return RunResult{}, err
			}

			enrichFromBroker(opts, &result)

			prevSessionIDs[phaseIdx] = result.SessionID
			prevPhaseCommit = phaseconfig.CaptureHeadCommit(opts.WorkDir)

			if err := ctx.Err(); err != nil {
				return cancelledRunResult(ctx, opts, iterationsCompleted)
			}

			sr := runtime.StopReasonForExitCode(result.ExitCode)

			switch result.LoopDirective {
			case "abort":
				_ = emitLoopEnd(opts.EventSink, opts.RunID, "abort", result.LoopLabel, iterationsCompleted)
				return RunResult{
					ExitCode:   5,
					StopReason: "blocked",
					SessionID:  result.SessionID,
					Reason:     result.LoopLabel,
				}, nil
			case "exit":
				if early, err := runSequentialPhases(ctx, opts, opts.Config.Post, "post", iterationsCompleted, &prevPhaseCommit); err != nil {
					return RunResult{}, err
				} else if early != nil {
					return *early, nil
				}
				_ = emitLoopEnd(opts.EventSink, opts.RunID, "marker", result.LoopLabel, iterationsCompleted)
				return RunResult{
					ExitCode:   0,
					StopReason: "end_turn",
					SessionID:  result.SessionID,
				}, nil
			}

			if sr != "end_turn" {
				_ = emitLoopEnd(opts.EventSink, opts.RunID, "phase_failure", "", iterationsCompleted)
				stopReason := result.StopReason
				if stopReason == "" {
					stopReason = sr
				}
				return RunResult{
					ExitCode:   result.ExitCode,
					StopReason: stopReason,
					SessionID:  result.SessionID,
				}, nil
			}
		}

		iterationsCompleted = iteration
	}

	if early, err := runSequentialPhases(ctx, opts, opts.Config.Post, "post", iterationsCompleted, &prevPhaseCommit); err != nil {
		return RunResult{}, err
	} else if early != nil {
		return *early, nil
	}
	_ = emitLoopEnd(opts.EventSink, opts.RunID, "max_iterations", "", iterationsCompleted)
	return RunResult{ExitCode: 0, StopReason: "end_turn"}, nil
}

func runSequentialPhases(ctx context.Context, opts RunOptions, phases []phaseconfig.Phase, kind string, iterationsCompleted int, prevCommit *string) (*RunResult, error) {
	var prevSessionID string
	for _, phase := range phases {
		if err := ctx.Err(); err != nil {
			r, err := cancelledRunResult(ctx, opts, iterationsCompleted)
			return &r, err
		}

		sessionID := ""
		if phase.ResumeFromPrevious {
			sessionID = prevSessionID
		}

		result, err := executePhase(ctx, opts, phase, kind, 0, sessionID, *prevCommit)
		if err != nil {
			_ = emitLoopEnd(opts.EventSink, opts.RunID, "phase_failure", "", iterationsCompleted)
			return nil, err
		}

		enrichFromBroker(opts, &result)

		prevSessionID = result.SessionID
		*prevCommit = phaseconfig.CaptureHeadCommit(opts.WorkDir)

		if err := ctx.Err(); err != nil {
			r, err := cancelledRunResult(ctx, opts, iterationsCompleted)
			return &r, err
		}

		if result.LoopDirective == "abort" {
			_ = emitLoopEnd(opts.EventSink, opts.RunID, "abort", result.LoopLabel, iterationsCompleted)
			r := RunResult{ExitCode: 5, StopReason: "blocked", SessionID: result.SessionID, Reason: result.LoopLabel}
			return &r, nil
		}

		sr := runtime.StopReasonForExitCode(result.ExitCode)
		if sr != "end_turn" {
			_ = emitLoopEnd(opts.EventSink, opts.RunID, "phase_failure", "", iterationsCompleted)
			stopReason := result.StopReason
			if stopReason == "" {
				stopReason = sr
			}
			r := RunResult{ExitCode: result.ExitCode, StopReason: stopReason, SessionID: result.SessionID}
			return &r, nil
		}
	}
	return nil, nil
}

func executePhase(ctx context.Context, opts RunOptions, phase phaseconfig.Phase, kind string, iteration int, prevSessionID string, prevPhaseCommit string) (result PhaseAttemptResult, rerr error) {
	if phase.LoopFile != "" || phase.TeamFile != "" {
		if opts.NestedRun == nil {
			return PhaseAttemptResult{}, fmt.Errorf("loop config: phase %q has loop_file or team_file but NestedRun is not configured", phase.Name)
		}
		var configPath string
		if phase.LoopFile != "" {
			configPath = phase.LoopFile
		} else {
			configPath = phase.TeamFile
		}
		if !filepath.IsAbs(configPath) && opts.ConfigDir != "" {
			configPath = filepath.Join(opts.ConfigDir, configPath)
		}
		runType := "loop"
		if phase.TeamFile != "" {
			runType = "team"
		}
		nestedResult, err := opts.NestedRun(ctx, configPath, runType)
		if err != nil {
			return PhaseAttemptResult{}, fmt.Errorf("loop config: phase %q nested run: %w", phase.Name, err)
		}
		return PhaseAttemptResult{
			ExitCode:      nestedResult.ExitCode,
			SessionID:     nestedResult.SessionID,
			StopReason:    nestedResult.StopReason,
			LoopDirective: nestedLoopDirective(nestedResult),
			LoopLabel:     nestedResult.Reason,
		}, nil
	}

	diffStat, changedFiles, _ := phaseconfig.CaptureGitDelta(opts.WorkDir, prevPhaseCommit) // best-effort; empty in non-git contexts

	tmplCtx := phaseconfig.TemplateContext{
		RunID:           opts.RunID,
		Phase:           phase.Name,
		Iteration:       iteration,
		MaxIterations:   opts.Config.MaxIterations,
		WorkDir:         opts.WorkDir,
		PrevPhaseCommit: prevPhaseCommit,
		DiffStat:        diffStat,
		ChangedFiles:    changedFiles,
	}

	rendered, err := phaseconfig.RenderPrompt(phase.Prompt, tmplCtx)
	if err != nil {
		return PhaseAttemptResult{}, fmt.Errorf("render prompt for phase %q: %w", phase.Name, err)
	}

	renderedPhase := phase
	renderedPhase.Prompt = rendered

	if err := phaseconfig.EmitPhaseStart(opts.EventSink, opts.RunID, phase.Name, iteration, kind); err != nil {
		return PhaseAttemptResult{}, err
	}

	defer func() {
		_ = phaseconfig.EmitPhaseEnd(opts.EventSink, opts.RunID, phase.Name, iteration, phaseStopReason(result), markerFromResult(result))
	}()

	var retryCount int
	var accDirective string
	var accLabel string

	wrappedAttempt := func(ctx context.Context) (PhaseAttemptResult, error) {
		if retryCount > 0 && retryCount <= opts.MaxRetries {
			phaseconfig.EmitRetry(opts.EventSink, opts.RunID, retryCount, opts.MaxRetries)
		}
		r, err := opts.PhaseAttempt(ctx, renderedPhase, retryCount, iteration, prevSessionID)
		retryCount++
		if err != nil {
			return r, err
		}
		if r.ExitCode != 1 {
			accDirective = r.LoopDirective
			accLabel = r.LoopLabel
		} else if phaseconfig.LoopDirectiveSeverity(r.LoopDirective) > phaseconfig.LoopDirectiveSeverity(accDirective) {
			accDirective = r.LoopDirective
			accLabel = r.LoopLabel
		}
		r.LoopDirective = accDirective
		r.LoopLabel = accLabel
		return r, nil
	}

	result, rerr = runPhaseWithRetry(ctx, wrappedAttempt, opts.MaxRetries)
	return
}

func runPhaseWithRetry(ctx context.Context, attemptFn func(ctx context.Context) (PhaseAttemptResult, error), maxRetries int) (PhaseAttemptResult, error) {
	result, err := attemptFn(ctx)
	if err != nil {
		return result, err
	}

	for retry := 1; retry <= maxRetries; retry++ {
		if result.ExitCode != 1 {
			return result, nil
		}

		select {
		case <-ctx.Done():
			return result, nil
		case <-phaseRetryAfter(phaseconfig.BackoffDuration(retry - 1)):
		}

		result, err = attemptFn(ctx)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

var phaseRetryAfter = time.After

func cancelledRunResult(ctx context.Context, opts RunOptions, iterationsCompleted int) (RunResult, error) {
	exitReason := "cancelled"
	code := 130
	if ctx.Err() == context.DeadlineExceeded {
		exitReason = "timeout"
		code = 124
	}
	_ = emitLoopEnd(opts.EventSink, opts.RunID, exitReason, "", iterationsCompleted)
	return RunResult{ExitCode: code, StopReason: exitReason}, nil
}

func nestedLoopDirective(result NestedResult) string {
	if result.StopReason == "blocked" {
		return "abort"
	}
	return ""
}

func phaseStopReason(result PhaseAttemptResult) string {
	if result.StopReason != "" {
		return result.StopReason
	}
	return runtime.StopReasonForExitCode(result.ExitCode)
}

func markerFromResult(result PhaseAttemptResult) *phaseconfig.LoopMarker {
	if result.LoopDirective == "" {
		return nil
	}
	return &phaseconfig.LoopMarker{Directive: result.LoopDirective, Label: result.LoopLabel}
}

func emitLoopStart(w phaseconfig.EventWriter, runID string, maxIterations, preCount, loopCount, postCount int) error {
	fields := map[string]any{
		"ts":                time.Now().UnixMilli(),
		"max_iterations":    maxIterations,
		"pre_phase_count":   preCount,
		"loop_phase_count":  loopCount,
		"post_phase_count":  postCount,
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	return w.Write(events.Event{
		Event:  "avenor.loop.start",
		Fields: fields,
	})
}

func emitLoopEnd(w phaseconfig.EventWriter, runID string, exitReason, exitLabel string, iterationsCompleted int) error {
	fields := map[string]any{
		"ts":                   time.Now().UnixMilli(),
		"exit_reason":          exitReason,
		"iterations_completed": iterationsCompleted,
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	if exitLabel != "" {
		fields["exit_label"] = exitLabel
	}
	return w.Write(events.Event{
		Event:  "avenor.loop.end",
		Fields: fields,
	})
}

// enrichFromBroker supplements a PhaseAttemptResult with broker state
// when available. It is a no-op when no broker is configured or the
// attempt did not produce a brokerRunID.
func enrichFromBroker(opts RunOptions, result *PhaseAttemptResult) {
	broker.EnrichStopReason(opts.Broker, result.BrokerRunID, &result.StopReason)
}
