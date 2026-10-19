package teamrunner

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/rosterconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

type RunResult struct {
	ExitCode   int
	StopReason string
	SessionID  string
	Reason     string
	RetryAfter int64
	// MarkerDirective/MarkerLabel carry the terminal marker the team exited
	// on (the exit directive and its label) so the workflow termination path
	// can select a declared outcome from it.
	MarkerDirective string
	MarkerLabel     string
}

type NestedResult struct {
	ExitCode   int
	StopReason string
	SessionID  string
	Reason     string
	RetryAfter int64
}

type RunOptions struct {
	WorkDir      string
	RunID        string
	EventSink    phaseconfig.EventWriter
	Config       *TeamConfig
	MaxRetries   int
	Broker       *broker.Broker
	SeedMessage  *broker.AgentMessage // pushed to the attempt's brokerRunID after creation
	PhaseAttempt func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error)
	NestedRun    func(ctx context.Context, configPath string, runType string) (NestedResult, error)
	ConfigDir    string
	Roster       *rosterconfig.Config
}

type PhaseAttemptResult struct {
	ExitCode      int
	SessionID     string
	StopReason    string
	RetryAfter    int64
	LoopDirective string
	LoopLabel     string
	Output        string
	FinalReply    string
	BrokerRunID   string
}

func Run(ctx context.Context, opts RunOptions) (RunResult, error) {
	if err := emitTeamStart(opts.EventSink, opts.RunID, len(opts.Config.Pre), len(opts.Config.Team), len(opts.Config.Post)); err != nil {
		return RunResult{}, err
	}

	prevPhaseCommit := phaseconfig.CaptureHeadCommit(opts.WorkDir)

	injectSkipBlockIntoLastPre(opts.Config)

	preOutput, early, err := runPrePhases(ctx, opts, &prevPhaseCommit)
	if err != nil {
		return RunResult{}, err
	}
	if early != nil {
		return *early, nil
	}

	members := buildMemberList(opts.Config.Team, preOutput)

	if len(preOutput) > 0 && opts.Broker != nil {
		opts.SeedMessage = &broker.AgentMessage{
			Message: preOutput,
			Role:    "pre-phase",
		}
	}

	if len(members) == 0 {
		marker, early, err := runSequentialPhases(ctx, opts, opts.Config.Post, "post", &prevPhaseCommit, "", "")
		if err != nil {
			return RunResult{}, err
		}
		if early != nil {
			return *early, nil
		}
		_ = emitTeamEnd(opts.EventSink, opts.RunID, "end_turn", "", 0, 0)
		final := RunResult{ExitCode: 0, StopReason: "end_turn"}
		if marker != nil {
			final.MarkerDirective, final.MarkerLabel = marker.Directive, marker.Label
		}
		return final, nil
	}

	membersCompleted, membersAborted, result, teamOutput, teamFinalOutput := runTeamMembers(ctx, opts, members, &prevPhaseCommit)
	if result != nil {
		return *result, nil
	}

	marker, early, err := runSequentialPhases(ctx, opts, opts.Config.Post, "post", &prevPhaseCommit, teamOutput, teamFinalOutput)
	if err != nil {
		return RunResult{}, err
	}
	if early != nil {
		return *early, nil
	}
	_ = emitTeamEnd(opts.EventSink, opts.RunID, "end_turn", "", membersCompleted, membersAborted)
	final := RunResult{ExitCode: 0, StopReason: "end_turn"}
	if marker != nil {
		final.MarkerDirective, final.MarkerLabel = marker.Directive, marker.Label
	}
	return final, nil
}

func injectSkipBlockIntoLastPre(cfg *TeamConfig) {
	hasConditional := false
	for _, m := range cfg.Team {
		if m.Conditional {
			hasConditional = true
			break
		}
	}
	if !hasConditional || len(cfg.Pre) == 0 {
		return
	}
	var conditional []string
	for _, m := range cfg.Team {
		if m.Conditional {
			conditional = append(conditional, m.Name)
		}
	}
	// Clone the Pre slice to avoid mutating the caller's config.
	pre := make([]phaseconfig.Phase, len(cfg.Pre))
	copy(pre, cfg.Pre)
	cfg.Pre = pre

	lastPre := &cfg.Pre[len(cfg.Pre)-1]
	lastPre.Prompt += fmt.Sprintf(
		"\n\nThe following team members may be skipped. For each that should not run, emit exactly one line:\n<|team: skip | <name>|>\nConditional members: %s",
		strings.Join(conditional, ", "),
	)
}

func buildMemberList(teamPhases []phaseconfig.Phase, preOutput string) []phaseconfig.Phase {
	skippedNames := extractTeamSkipMarkers(preOutput)
	if len(skippedNames) == 0 {
		return teamPhases
	}
	skipSet := make(map[string]struct{}, len(skippedNames))
	for _, n := range skippedNames {
		skipSet[strings.ToLower(n)] = struct{}{}
	}
	var filtered []phaseconfig.Phase
	for _, p := range teamPhases {
		if !p.Conditional {
			filtered = append(filtered, p)
			continue
		}
		if _, ok := skipSet[strings.ToLower(p.Name)]; ok {
			continue
		}
		filtered = append(filtered, p)
	}
	return filtered
}

func runPrePhases(ctx context.Context, opts RunOptions, prevCommit *string) (string, *RunResult, error) {
	var output strings.Builder
	var prevSessionID string
	for _, phase := range opts.Config.Pre {
		if err := ctx.Err(); err != nil {
			r, err := cancelledRunResult(ctx, opts)
			return output.String(), &r, err
		}

		sessionID := ""
		if phase.ResumeFromPrevious {
			sessionID = prevSessionID
		}

		result, err := executePhase(ctx, opts, phase, "pre", sessionID, *prevCommit, "", "")
		if err != nil {
			_ = emitTeamEnd(opts.EventSink, opts.RunID, "phase_failure", "", 0, 0)
			return output.String(), nil, err
		}

		enrichFromBroker(opts, &result)

		prevSessionID = result.SessionID
		*prevCommit = phaseconfig.CaptureHeadCommit(opts.WorkDir)
		output.WriteString(result.Output)

		if err := ctx.Err(); err != nil {
			r, err := cancelledRunResult(ctx, opts)
			return output.String(), &r, err
		}

		if result.LoopDirective == "abort" {
			_ = emitTeamEnd(opts.EventSink, opts.RunID, "abort", result.LoopLabel, 0, 0)
			r := RunResult{ExitCode: 5, StopReason: "blocked", SessionID: result.SessionID, Reason: result.LoopLabel}
			return output.String(), &r, nil
		}

		sr := runtime.StopReasonForExitCode(result.ExitCode)
		if sr != "end_turn" {
			stopReason := result.StopReason
			if stopReason == "" {
				stopReason = sr
			}
			if stopReason == "timeout" {
				_ = emitTeamEnd(opts.EventSink, opts.RunID, "timeout", "", 0, 0)
			} else {
				_ = emitTeamEnd(opts.EventSink, opts.RunID, "pre_failure", "", 0, 0)
			}
			r := RunResult{ExitCode: result.ExitCode, StopReason: stopReason, SessionID: result.SessionID, RetryAfter: result.RetryAfter}
			return output.String(), &r, nil
		}
	}
	return output.String(), nil, nil
}

func runTeamMembers(ctx context.Context, opts RunOptions, members []phaseconfig.Phase, prevCommit *string) (int, int, *RunResult, string, string) {
	teamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type memberResult struct {
		index  int
		result PhaseAttemptResult
	}

	results := make([]memberResult, len(members))
	var wg sync.WaitGroup
	firstAbortLabel := ""
	firstAbortSessionID := ""
	abortSeen := false
	var abortMu sync.Mutex

	for i, member := range members {
		wg.Add(1)
		go func(idx int, phase phaseconfig.Phase) {
			defer wg.Done()
			r, err := executePhase(teamCtx, opts, phase, "team", "", *prevCommit, "", "")
			if err != nil {
				// Internal error (template rendering, event emission failure).
				// Treat as a non-clean stop so the team result reflects the failure.
				r = PhaseAttemptResult{ExitCode: 1, StopReason: "phase_error"}
				if teamCtx.Err() == context.DeadlineExceeded {
					// The run deadline expired: classify the member as timed
					// out instead of an internal phase error.
					r = PhaseAttemptResult{ExitCode: runtime.ExitCodeForStopReason("timeout"), StopReason: "timeout"}
				}
			}
			results[idx] = memberResult{index: idx, result: r}

			if r.LoopDirective == "abort" {
				abortMu.Lock()
				if !abortSeen {
					firstAbortLabel = r.LoopLabel
					firstAbortSessionID = r.SessionID
					abortSeen = true
					cancel()
				}
				abortMu.Unlock()
			}
		}(i, member)
	}

	wg.Wait()

	for i := range results {
		enrichFromBroker(opts, &results[i].result)
	}

	*prevCommit = phaseconfig.CaptureHeadCommit(opts.WorkDir)

	membersCompleted := 0
	membersAborted := 0
	var failureResult *RunResult

	for _, mr := range results {
		r := mr.result
		sr := runtime.StopReasonForExitCode(r.ExitCode)
		if r.LoopDirective == "abort" {
			membersAborted++
		} else {
			membersCompleted++
			if sr != "end_turn" {
				if failureResult == nil {
					stopReason := r.StopReason
					if stopReason == "" {
						stopReason = sr
					}
					failureResult = &RunResult{
						ExitCode:   r.ExitCode,
						StopReason: stopReason,
						SessionID:  r.SessionID,
						RetryAfter: r.RetryAfter,
					}
				}
			}
		}
	}

	var memberOutput strings.Builder
	var memberFinalOutput strings.Builder
	for _, mr := range results {
		if mr.result.Output != "" {
			if memberOutput.Len() > 0 {
				memberOutput.WriteByte('\n')
			}
			memberOutput.WriteString(mr.result.Output)
		}
		if mr.result.FinalReply != "" {
			if memberFinalOutput.Len() > 0 {
				memberFinalOutput.WriteByte('\n')
			}
			memberFinalOutput.WriteString(mr.result.FinalReply)
		}
	}

	if abortSeen {
		_ = emitTeamEnd(opts.EventSink, opts.RunID, "abort", firstAbortLabel, membersCompleted, membersAborted)
		return membersCompleted, membersAborted, &RunResult{
			ExitCode:   5,
			StopReason: "blocked",
			SessionID:  firstAbortSessionID,
			Reason:     firstAbortLabel,
		}, "", ""
	}

	if failureResult != nil {
		exitReason := "phase_failure"
		if failureResult.StopReason == "timeout" {
			exitReason = "timeout"
		}
		_ = emitTeamEnd(opts.EventSink, opts.RunID, exitReason, "", membersCompleted, membersAborted)
		return membersCompleted, membersAborted, failureResult, "", ""
	}

	return membersCompleted, membersAborted, nil, memberOutput.String(), memberFinalOutput.String()
}

func runSequentialPhases(ctx context.Context, opts RunOptions, phases []phaseconfig.Phase, kind string, prevCommit *string, teamOutput, teamFinalOutput string) (*phaseconfig.LoopMarker, *RunResult, error) {
	var prevSessionID string
	var exitMarker *phaseconfig.LoopMarker
	for _, phase := range phases {
		if err := ctx.Err(); err != nil {
			r, err := cancelledRunResult(ctx, opts)
			return nil, &r, err
		}

		sessionID := ""
		if phase.ResumeFromPrevious {
			sessionID = prevSessionID
		}

		result, err := executePhase(ctx, opts, phase, kind, sessionID, *prevCommit, teamOutput, teamFinalOutput)
		if err != nil {
			_ = emitTeamEnd(opts.EventSink, opts.RunID, kind+"_failure", "", 0, 0)
			return nil, nil, err
		}

		enrichFromBroker(opts, &result)

		prevSessionID = result.SessionID
		*prevCommit = phaseconfig.CaptureHeadCommit(opts.WorkDir)

		if err := ctx.Err(); err != nil {
			r, err := cancelledRunResult(ctx, opts)
			return nil, &r, err
		}

		if result.LoopDirective == "abort" {
			_ = emitTeamEnd(opts.EventSink, opts.RunID, "abort", result.LoopLabel, 0, 0)
			r := RunResult{ExitCode: 5, StopReason: "blocked", SessionID: result.SessionID, Reason: result.LoopLabel}
			return nil, &r, nil
		}
		if result.LoopDirective == "exit" {
			exitMarker = &phaseconfig.LoopMarker{Directive: "exit", Label: result.LoopLabel}
		}

		sr := runtime.StopReasonForExitCode(result.ExitCode)
		if sr != "end_turn" {
			stopReason := result.StopReason
			if stopReason == "" {
				stopReason = sr
			}
			if stopReason == "timeout" {
				_ = emitTeamEnd(opts.EventSink, opts.RunID, "timeout", "", 0, 0)
			} else {
				_ = emitTeamEnd(opts.EventSink, opts.RunID, "post_failure", "", 0, 0)
			}
			r := RunResult{ExitCode: result.ExitCode, StopReason: stopReason, SessionID: result.SessionID, RetryAfter: result.RetryAfter}
			return nil, &r, nil
		}
	}
	return exitMarker, nil, nil
}

func executePhase(ctx context.Context, opts RunOptions, phase phaseconfig.Phase, kind string, prevSessionID string, prevPhaseCommit string, teamOutput, teamFinalOutput string) (result PhaseAttemptResult, rerr error) {
	if phase.LoopFile != "" || phase.TeamFile != "" {
		if opts.NestedRun == nil {
			return PhaseAttemptResult{}, fmt.Errorf("team config: phase %q has loop_file or team_file but NestedRun is not configured", phase.Name)
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
			return PhaseAttemptResult{}, fmt.Errorf("team config: phase %q nested run: %w", phase.Name, err)
		}
		return PhaseAttemptResult{
			ExitCode:      nestedResult.ExitCode,
			SessionID:     nestedResult.SessionID,
			StopReason:    nestedResult.StopReason,
			RetryAfter:    nestedResult.RetryAfter,
			LoopDirective: nestedLoopDirective(nestedResult),
			LoopLabel:     nestedResult.Reason,
		}, nil
	}

	diffStat, changedFiles, _ := phaseconfig.CaptureGitDelta(opts.WorkDir, prevPhaseCommit) // best-effort; empty in non-git contexts

	tmplCtx := phaseconfig.TemplateContext{
		RunID:           opts.RunID,
		Phase:           phase.Name,
		WorkDir:         opts.WorkDir,
		PrevPhaseCommit: prevPhaseCommit,
		DiffStat:        diffStat,
		ChangedFiles:    changedFiles,
		TeamOutput:      teamOutput,
		TeamFinalOutput: teamFinalOutput,
	}

	rendered, err := phaseconfig.RenderPrompt(phase.Prompt, tmplCtx)
	if err != nil {
		return PhaseAttemptResult{}, fmt.Errorf("render prompt for phase %q: %w", phase.Name, err)
	}

	renderedPhase := phase
	renderedPhase.Prompt = rendered

	if err := phaseconfig.EmitPhaseStart(opts.EventSink, opts.RunID, phase.Name, 0, kind, runtime.LaunchKindTeam); err != nil {
		return PhaseAttemptResult{}, err
	}

	defer func() {
		_ = phaseconfig.EmitPhaseEnd(opts.EventSink, opts.RunID, phase.Name, 0, runtime.LaunchKindTeam, phaseStopReason(result), markerFromResult(result))
	}()

	var retryCount int
	var launchCount int
	var accDirective string
	var accLabel string

	wrappedAttempt := func(ctx context.Context) (PhaseAttemptResult, error) {
		if retryCount > 0 && retryCount <= opts.MaxRetries {
			phaseconfig.EmitRetry(opts.EventSink, opts.RunID, retryCount, opts.MaxRetries)
		}
		launch := launchCount
		launchCount++
		r, err := opts.PhaseAttempt(ctx, renderedPhase, launch, prevSessionID)
		retryCount++
		if err != nil {
			return r, err
		}
		// Non-retryable exit takes the current attempt's directive; retryable failure (code 1) accumulates by severity.
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
	if rerr != nil {
		return
	}
	result, rerr = completePhaseRequirements(ctx, opts, renderedPhase, result, &launchCount)
	return
}

func completePhaseRequirements(ctx context.Context, opts RunOptions, phase phaseconfig.Phase, result PhaseAttemptResult, launchCount *int) (PhaseAttemptResult, error) {
	missing := phaseconfig.MissingRequiredFiles(opts.WorkDir, phase.Requires)
	if len(missing) == 0 || result.ExitCode != 0 || phaseStopReason(result) != "end_turn" {
		return result, nil
	}

	maxNudges := phase.OnIncomplete.MaxNudges
	if maxNudges == 0 {
		maxNudges = 1
	}
	customNudge := phase.OnIncomplete.Nudge

	sessionID := result.SessionID
	for i := 0; i < maxNudges; i++ {
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// The run deadline expired before recovery could start:
				// report the timeout instead of an incomplete-output failure.
				result.ExitCode = runtime.ExitCodeForStopReason("timeout")
				result.StopReason = "timeout"
				return result, nil
			}
			// The parent context was cancelled (supervisor shutdown, workflow
			// cancellation): report the cancellation instead of propagating a
			// generic error that would be classified as a phase failure.
			result.ExitCode = runtime.ExitCodeForStopReason("cancelled")
			result.StopReason = "cancelled"
			return result, nil
		}
		nudgePhase := phase
		nudgePhase.Prompt = phaseconfig.IncompleteNudgePrompt(customNudge, missing)
		launch := *launchCount
		*launchCount = launch + 1
		result, err := opts.PhaseAttempt(ctx, nudgePhase, launch, sessionID)
		if err != nil {
			return result, err
		}
		sessionID = result.SessionID
		missing = phaseconfig.MissingRequiredFiles(opts.WorkDir, phase.Requires)
		if len(missing) == 0 || result.ExitCode != 0 || phaseStopReason(result) != "end_turn" {
			return result, nil
		}
	}

	return PhaseAttemptResult{
		ExitCode:    1,
		SessionID:   sessionID,
		StopReason:  "incomplete_output",
		Output:      result.Output,
		FinalReply:  result.FinalReply,
		BrokerRunID: result.BrokerRunID,
	}, nil
}

func incompleteNudgePrompt(custom string, missing []string) string {
	return phaseconfig.IncompleteNudgePrompt(custom, missing)
}

func missingRequiredFiles(workDir string, req phaseconfig.PhaseRequirements) []string {
	return phaseconfig.MissingRequiredFiles(workDir, req)
}

func runPhaseWithRetry(ctx context.Context, attemptFn func(ctx context.Context) (PhaseAttemptResult, error), maxRetries int) (PhaseAttemptResult, error) {
	result, err := attemptFn(ctx)
	if err != nil {
		return result, err
	}

	for retry := 1; retry <= maxRetries; retry++ {
		if !runtime.IsRetryableFailure(result.ExitCode, result.StopReason) {
			return result, nil
		}

		deadline, _ := ctx.Deadline()
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				// The run deadline expired during the retry backoff: the
				// bounded run stops here with a consistent timeout class.
				result.ExitCode = runtime.ExitCodeForStopReason("timeout")
				result.StopReason = "timeout"
			}
			return result, nil
		case <-runtime.RetryWait(phaseRetryAfter, phaseconfig.BackoffDuration(retry-1), result.RetryAfter, deadline):
		}

		result, err = attemptFn(ctx)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

var phaseRetryAfter = time.After

func cancelledRunResult(ctx context.Context, opts RunOptions) (RunResult, error) {
	exitReason := "cancelled"
	code := 130
	if ctx.Err() == context.DeadlineExceeded {
		exitReason = "timeout"
		code = 124
	}
	_ = emitTeamEnd(opts.EventSink, opts.RunID, exitReason, "", 0, 0)
	return RunResult{ExitCode: code, StopReason: exitReason}, nil
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

var teamSkipRe = regexp.MustCompile(`(?i)^\s*\[team:\s*skip\s*\|\s*([^\]]+)\]\s*$`)

// teamSkipAngleRe matches <|team: skip | member-name|> full-line, case-insensitively.
var teamSkipAngleRe = regexp.MustCompile(`(?i)^\s*<\|team:\s*skip\s*\|\s*(.+)\|>\s*$`)

func extractTeamSkipMarkers(text string) []string {
	var names []string
	for _, line := range strings.Split(text, "\n") {
		m := teamSkipRe.FindStringSubmatch(line)
		if m != nil {
			names = append(names, strings.TrimSpace(m[1]))
			continue
		}
		m = teamSkipAngleRe.FindStringSubmatch(line)
		if m != nil {
			names = append(names, strings.TrimSpace(m[1]))
		}
	}
	return names
}

func emitTeamStart(w phaseconfig.EventWriter, runID string, preCount, memberCount, postCount int) error {
	fields := map[string]any{
		"ts":                time.Now().UnixMilli(),
		"pre_phase_count":   preCount,
		"team_member_count": memberCount,
		"post_phase_count":  postCount,
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	return w.Write(events.Event{
		Event:  "avenor.team.start",
		Fields: fields,
	})
}

func emitTeamEnd(w phaseconfig.EventWriter, runID string, exitReason, exitLabel string, membersCompleted, membersAborted int) error {
	fields := map[string]any{
		"ts":                time.Now().UnixMilli(),
		"exit_reason":       exitReason,
		"members_completed": membersCompleted,
		"members_aborted":   membersAborted,
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	if exitLabel != "" {
		fields["exit_label"] = exitLabel
	}
	return w.Write(events.Event{
		Event:  "avenor.team.end",
		Fields: fields,
	})
}

func nestedLoopDirective(result NestedResult) string {
	if result.StopReason == "blocked" {
		return "abort"
	}
	return ""
}

// enrichFromBroker supplements a PhaseAttemptResult with broker state
// when available. It is a no-op when no broker is configured or the
// attempt did not produce a brokerRunID.
func enrichFromBroker(opts RunOptions, result *PhaseAttemptResult) {
	broker.EnrichStopReason(opts.Broker, result.BrokerRunID, &result.StopReason)
}
