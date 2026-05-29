package teamrunner

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type EventWriter interface {
	Write(event events.Event) error
}

type RunResult struct {
	ExitCode   int
	StopReason string
	SessionID  string
	Reason     string
}

type RunOptions struct {
	WorkDir      string
	RunID        string
	EventSink    EventWriter
	Config       *TeamConfig
	MaxRetries   int
	PhaseAttempt func(ctx context.Context, phase Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error)
}

type PhaseAttemptResult struct {
	ExitCode      int
	SessionID     string
	StopReason    string
	LoopDirective string
	LoopLabel     string
	Output        string // agent text output, scanned for [team: skip | name] in pre-phases
}

type TemplateContext struct {
	RunID           string
	Phase           string
	WorkDir         string
	PrevPhaseCommit string
	DiffStat        string
	ChangedFiles    string
}

// ---- Template & Git helpers ----

func RenderPrompt(tmpl string, ctx TemplateContext) (string, error) {
	t, err := template.New("prompt").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, ctx); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func CaptureGitDelta(workDir, prevCommit string) (diffStat, changedFiles string, err error) {
	if prevCommit == "" {
		return "", "", nil
	}
	diffStat, err = runGit(workDir, "diff", "--stat", prevCommit)
	if err != nil {
		return "", "", nil
	}
	changedFiles, err = runGit(workDir, "diff", "--name-only", prevCommit)
	if err != nil {
		return "", "", nil
	}
	return diffStat, changedFiles, nil
}

func runGit(workDir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = workDir
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(bytes.TrimRight(out, "\n")), nil
}

func captureHeadCommit(workDir string) string {
	cmd := exec.Command("git", "-C", workDir, "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// ---- Main Run ----

func Run(ctx context.Context, opts RunOptions) (RunResult, error) {
	if err := emitTeamStart(opts.EventSink, opts.RunID, len(opts.Config.Pre), len(opts.Config.Team), len(opts.Config.Post)); err != nil {
		return RunResult{}, err
	}

	prevPhaseCommit := captureHeadCommit(opts.WorkDir)

	// If any team members are conditional and there are pre-phases, inject the
	// skip-syntax block into the last pre-phase before running pre-phases.
	injectSkipBlockIntoLastPre(opts.Config)

	// Run pre-phases serially. Collect output for [team: skip] scanning.
	preOutput, early, err := runPrePhases(ctx, opts, &prevPhaseCommit)
	if err != nil {
		return RunResult{}, err
	}
	if early != nil {
		return *early, nil
	}

	// Determine which conditional members to skip based on pre-phase output.
	members := buildMemberList(opts.Config.Team, preOutput)

	// If team is empty after skipping, run post-phases only.
	if len(members) == 0 {
		if early, err := runSequentialPhases(ctx, opts, opts.Config.Post, "post", &prevPhaseCommit); err != nil {
			return RunResult{}, err
		} else if early != nil {
			return *early, nil
		}
		_ = emitTeamEnd(opts.EventSink, opts.RunID, "end_turn", "", 0, 0)
		return RunResult{ExitCode: 0, StopReason: "end_turn"}, nil
	}

	if len(opts.Config.Team) == 0 {
		// No team section at all — just pre + post.
		if early, err := runSequentialPhases(ctx, opts, opts.Config.Post, "post", &prevPhaseCommit); err != nil {
			return RunResult{}, err
		} else if early != nil {
			return *early, nil
		}
		_ = emitTeamEnd(opts.EventSink, opts.RunID, "end_turn", "", 0, 0)
		return RunResult{ExitCode: 0, StopReason: "end_turn"}, nil
	}

	membersCompleted, membersAborted, result := runTeamMembers(ctx, opts, members, &prevPhaseCommit)
	if result != nil {
		return *result, nil
	}

	if early, err := runSequentialPhases(ctx, opts, opts.Config.Post, "post", &prevPhaseCommit); err != nil {
		return RunResult{}, err
	} else if early != nil {
		return *early, nil
	}
	_ = emitTeamEnd(opts.EventSink, opts.RunID, "end_turn", "", membersCompleted, membersAborted)
	return RunResult{ExitCode: 0, StopReason: "end_turn"}, nil
}

// injectSkipBlockIntoLastPre mutates the last pre-phase prompt to include the
// [team: skip | name] instruction block when any team member has conditional: true.
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
	lastPre := &cfg.Pre[len(cfg.Pre)-1]
	lastPre.Prompt += fmt.Sprintf(
		"\n\nThe following team members may be skipped. For each that should not run, emit exactly one line:\n[team: skip | <name>]\nConditional members: %s",
		strings.Join(conditional, ", "),
	)
}

// buildMemberList filters the team members, removing any that appear in the
// [team: skip | name] markers found in pre-phase output.
func buildMemberList(teamPhases []Phase, preOutput string) []Phase {
	skippedNames := extractTeamSkipMarkers(preOutput)
	if len(skippedNames) == 0 {
		return teamPhases
	}
	skipSet := make(map[string]struct{}, len(skippedNames))
	for _, n := range skippedNames {
		skipSet[strings.TrimSpace(n)] = struct{}{}
	}
	var filtered []Phase
	for _, p := range teamPhases {
		if _, ok := skipSet[p.Name]; ok {
			continue
		}
		filtered = append(filtered, p)
	}
	return filtered
}

// runPrePhases runs pre-phases serially and returns the concatenated Output
// from all pre-phase results (for [team: skip] scanning), plus any early
// exit result (abort, failure).
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

		result, err := executePhase(ctx, opts, phase, "pre", sessionID, *prevCommit)
		if err != nil {
			_ = emitTeamEnd(opts.EventSink, opts.RunID, "phase_failure", "", 0, 0)
			return output.String(), nil, err
		}

		prevSessionID = result.SessionID
		*prevCommit = captureHeadCommit(opts.WorkDir)
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
			_ = emitTeamEnd(opts.EventSink, opts.RunID, "pre_failure", "", 0, 0)
			stopReason := result.StopReason
			if stopReason == "" {
				stopReason = sr
			}
			r := RunResult{ExitCode: result.ExitCode, StopReason: stopReason, SessionID: result.SessionID}
			return output.String(), &r, nil
		}
	}
	return output.String(), nil, nil
}

// runTeamMembers executes team members in parallel. It returns the count of
// completed and aborted members, and a non-nil RunResult if the team should
// stop (abort or member failure).
func runTeamMembers(ctx context.Context, opts RunOptions, members []Phase, prevCommit *string) (int, int, *RunResult) {
	teamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type memberResult struct {
		index  int
		result PhaseAttemptResult
	}

	results := make([]memberResult, len(members))
	var wg sync.WaitGroup
	firstAbortLabel := ""
	abortSeen := false
	var abortMu sync.Mutex

	for i, member := range members {
		wg.Add(1)
		go func(idx int, phase Phase) {
			defer wg.Done()
			r, _ := executePhase(teamCtx, opts, phase, "team", "", *prevCommit)
			results[idx] = memberResult{index: idx, result: r}

			if r.LoopDirective == "abort" {
				abortMu.Lock()
				if !abortSeen {
					firstAbortLabel = r.LoopLabel
					abortSeen = true
					cancel()
				}
				abortMu.Unlock()
			}
		}(i, member)
	}

	wg.Wait()

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
					}
				}
			}
		}
	}

	if abortSeen {
		_ = emitTeamEnd(opts.EventSink, opts.RunID, "abort", firstAbortLabel, membersCompleted, membersAborted)
		return membersCompleted, membersAborted, &RunResult{
			ExitCode:   5,
			StopReason: "blocked",
			Reason:     firstAbortLabel,
		}
	}

	if failureResult != nil {
		_ = emitTeamEnd(opts.EventSink, opts.RunID, "phase_failure", "", membersCompleted, membersAborted)
		return membersCompleted, membersAborted, failureResult
	}

	return membersCompleted, membersAborted, nil
}

// runSequentialPhases runs phases in order (used for post-phases).
// Returns a non-nil RunResult on early exit (abort, failure, cancellation).
func runSequentialPhases(ctx context.Context, opts RunOptions, phases []Phase, kind string, prevCommit *string) (*RunResult, error) {
	var prevSessionID string
	for _, phase := range phases {
		if err := ctx.Err(); err != nil {
			r, err := cancelledRunResult(ctx, opts)
			return &r, err
		}

		sessionID := ""
		if phase.ResumeFromPrevious {
			sessionID = prevSessionID
		}

		result, err := executePhase(ctx, opts, phase, kind, sessionID, *prevCommit)
		if err != nil {
			_ = emitTeamEnd(opts.EventSink, opts.RunID, kind+"_failure", "", 0, 0)
			return nil, err
		}

		prevSessionID = result.SessionID
		*prevCommit = captureHeadCommit(opts.WorkDir)

		if err := ctx.Err(); err != nil {
			r, err := cancelledRunResult(ctx, opts)
			return &r, err
		}

		if result.LoopDirective == "abort" {
			_ = emitTeamEnd(opts.EventSink, opts.RunID, "abort", result.LoopLabel, 0, 0)
			r := RunResult{ExitCode: 5, StopReason: "blocked", SessionID: result.SessionID, Reason: result.LoopLabel}
			return &r, nil
		}

		sr := runtime.StopReasonForExitCode(result.ExitCode)
		if sr != "end_turn" {
			_ = emitTeamEnd(opts.EventSink, opts.RunID, "post_failure", "", 0, 0)
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

// ---- Phase execution with retry ----

func executePhase(ctx context.Context, opts RunOptions, phase Phase, kind string, prevSessionID string, prevPhaseCommit string) (result PhaseAttemptResult, rerr error) {
	diffStat, changedFiles, _ := CaptureGitDelta(opts.WorkDir, prevPhaseCommit)

	tmplCtx := TemplateContext{
		RunID:           opts.RunID,
		Phase:           phase.Name,
		WorkDir:         opts.WorkDir,
		PrevPhaseCommit: prevPhaseCommit,
		DiffStat:        diffStat,
		ChangedFiles:    changedFiles,
	}

	rendered, err := RenderPrompt(phase.Prompt, tmplCtx)
	if err != nil {
		return PhaseAttemptResult{}, fmt.Errorf("render prompt for phase %q: %w", phase.Name, err)
	}

	renderedPhase := phase
	renderedPhase.Prompt = rendered

	if err := emitPhaseStart(opts.EventSink, opts.RunID, phase.Name, kind); err != nil {
		return PhaseAttemptResult{}, err
	}

	defer func() {
		_ = emitPhaseEnd(opts.EventSink, opts.RunID, phase.Name, kind, phaseStopReason(result), markerFromResult(result))
	}()

	var retryCount int
	var accDirective string
	var accLabel string

	wrappedAttempt := func(ctx context.Context) (PhaseAttemptResult, error) {
		if retryCount > 0 && retryCount <= opts.MaxRetries {
			emitRetry(opts.EventSink, opts.RunID, retryCount, opts.MaxRetries)
		}
		r, err := opts.PhaseAttempt(ctx, renderedPhase, retryCount, prevSessionID)
		retryCount++
		if err != nil {
			return r, err
		}
		if loopDirectiveSeverity(r.LoopDirective) > loopDirectiveSeverity(accDirective) {
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
		case <-phaseRetryAfter(backoffDuration(retry - 1)):
		}

		result, err = attemptFn(ctx)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

var phaseRetryAfter = time.After

func backoffDuration(retry int) time.Duration {
	d := time.Duration(1<<uint(retry+1)) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// ---- Helper functions ----

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

func markerFromResult(result PhaseAttemptResult) *teamMarker {
	if result.LoopDirective == "" {
		return nil
	}
	return &teamMarker{directive: result.LoopDirective, label: result.LoopLabel}
}

// ---- [team: skip] marker extraction ----

var teamSkipRe = regexp.MustCompile(`(?i)^\s*\[team:\s*skip\s*\|\s*([^\]]+)\]\s*$`)

func extractTeamSkipMarkers(text string) []string {
	var names []string
	for _, line := range strings.Split(text, "\n") {
		m := teamSkipRe.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}
		names = append(names, strings.TrimSpace(m[1]))
	}
	return names
}

// ---- Event emission ----

func emitRetry(w EventWriter, runID string, attempt, maxRetries int) {
	fields := map[string]any{
		"attempt":     attempt,
		"max_retries": maxRetries,
		"ts":          time.Now().UnixMilli(),
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	_ = w.Write(events.Event{
		Event:  "avenor.retry",
		Fields: fields,
	})
}

func emitTeamStart(w EventWriter, runID string, preCount, memberCount, postCount int) error {
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

func emitTeamEnd(w EventWriter, runID string, exitReason, exitLabel string, membersCompleted, membersAborted int) error {
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

func emitPhaseStart(w EventWriter, runID, phase, kind string) error {
	fields := map[string]any{
		"ts":    time.Now().UnixMilli(),
		"phase": phase,
		"kind":  kind,
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	return w.Write(events.Event{
		Event:  "avenor.phase.start",
		Fields: fields,
	})
}

func emitPhaseEnd(w EventWriter, runID, phase, kind, stopReason string, marker *teamMarker) error {
	fields := map[string]any{
		"ts":          time.Now().UnixMilli(),
		"phase":       phase,
		"stop_reason": stopReason,
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	if marker != nil {
		if marker.directive == "abort" {
			fields["abort_marker"] = true
			if marker.label != "" {
				fields["abort_marker_label"] = marker.label
			}
		} else if marker.directive == "exit" {
			fields["exit_marker"] = true
			if marker.label != "" {
				fields["exit_marker_label"] = marker.label
			}
		}
	}
	return w.Write(events.Event{
		Event:  "avenor.phase.end",
		Fields: fields,
	})
}

type teamMarker struct {
	directive string
	label     string
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