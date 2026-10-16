package stable

// Public-API tests for the stable Supervisor's loop and team children, the
// workflow-mode siblings of the single-runtime scenarios in
// supervisor_public_api_test.go. Every scenario spawns through Spawn with
// loop_file or team_file and asserts only what a caller can see:
// RuntimeStatus translated through runstate.Translate with the completion
// sentinel applied, RuntimeResult, RuntimeWaitTurn, List rows, and the
// events published on the control stream. The one seam is the provider
// factory (newProviderFunc), which stands in for the backend; loop phases
// are sequential, so one scripted provider serves the attempts in order, and
// team members are keyed by their inline agent so each member's provider is
// addressed deterministically despite concurrent attempts.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// itestWorkflowConfig writes a loop or team config file in the JSON shape
// configs use on disk and returns its path.
func itestWorkflowConfig(t *testing.T, kind string, cfg map[string]any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), kind+".json")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal %s config: %v", kind, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s config: %v", kind, err)
	}
	return path
}

// spawnWorkflow spawns a loop or team child through Spawn. The spawn prompt
// is cleared: a non-empty prompt is injected as an extra "(initial)" pre
// phase, which would add a provider attempt to every scenario.
func (h *itestHarness) spawnWorkflow(kind, file string, params map[string]any) string {
	h.t.Helper()
	merged := map[string]any{"prompt": ""}
	for k, v := range params {
		merged[k] = v
	}
	merged[kind+"_file"] = file
	return h.spawn(merged)
}

// itestMemberEndIs matches a session.end carrying a given session ID and stop
// reason. Team and loop phase attempts are distinguished by their session IDs
// because every event of the child shares one runtime id.
func itestMemberEndIs(sessionID, stopReason string) func(events.Event) bool {
	return func(ev events.Event) bool {
		if ev.Event != "session.end" {
			return false
		}
		if got, _ := ev.Fields["stop_reason"].(string); got != stopReason {
			return false
		}
		id := ev.SessionID
		if id == "" {
			id, _ = ev.Fields["session_id"].(string)
		}
		return id == sessionID
	}
}

// itestMemberWorkingIs matches a working agent.status carrying a given session
// ID, so a barrier can require one specific member or phase attempt to be in
// flight: team members and loop phases share one runtime id, so the runtime
// id alone cannot tell them apart.
func itestMemberWorkingIs(sessionID string) func(events.Event) bool {
	return func(ev events.Event) bool {
		if ev.Event != "agent.status" {
			return false
		}
		if phase, _ := ev.Fields["phase"].(string); phase != "working" {
			return false
		}
		id := ev.SessionID
		if id == "" {
			id, _ = ev.Fields["session_id"].(string)
		}
		return id == sessionID
	}
}

// itestTeamProviders routes provider requests to per-member providers keyed
// by the member's inline agent: the only per-member field visible in
// StartOptions, since concurrent members cannot be ordered.
func itestTeamProviders(members map[string]*itestProvider) func(runtime.StartOptions, string) (runtime.Provider, error) {
	return func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		p, ok := members[opts.Agent]
		if !ok {
			return nil, fmt.Errorf("unexpected provider request for agent %q", opts.Agent)
		}
		return p, nil
	}
}

// itestWorking marks the provider's session as working before its next step,
// so a test can observe that a phase attempt is in flight.
func itestWorking(sessionID string) stableScriptedEvent {
	return stableScriptedEvent{event: events.Event{Event: "agent.status", SessionID: sessionID, Fields: map[string]any{"phase": "working"}}}
}

// sentinelText returns the completion sentinel's content, which callers read
// from disk at the sentinel_file path RuntimeStatus reports.
func (h *itestHarness) sentinelText(rtID string) string {
	h.t.Helper()
	out, err := h.sup.RuntimeStatus(rtID)
	if err != nil {
		h.t.Fatalf("RuntimeStatus(%s): %v", rtID, err)
	}
	raw, _ := out.(map[string]any)
	path, _ := raw["sentinel_file"].(string)
	if path == "" {
		h.t.Fatalf("runtime %s reports no sentinel_file", rtID)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		h.t.Fatalf("read sentinel for %s: %v", rtID, err)
	}
	return string(data)
}

// Loop scenarios.

// requireWorkflowEnd asserts a loop or team child's caller-visible end state:
// the translated status carries stopReason, the raw RuntimeStatus exit code
// matches exitCode, and WaitTurn reports stopReason. Unlike requireIdleEnd it
// does not read the exit code from the terminal session.end: a workflow
// child's session.end events are its attempts' own and carry no exit_code.
func (h *itestHarness) requireWorkflowEnd(rtID, status, stopReason string, exitCode int) itestView {
	h.t.Helper()
	v := h.awaitStatus(rtID, status)
	if v.StopReason != stopReason {
		h.t.Fatalf("%s view stop reason = %q, want %s", status, v.StopReason, stopReason)
	}
	if code := h.rawExitCode(rtID); code != exitCode {
		h.t.Fatalf("raw RuntimeStatus exit_code = %d, want %d", code, exitCode)
	}
	turn := h.waitTurn(rtID)
	if got, _ := turn["stop_reason"].(string); got != stopReason {
		h.t.Fatalf("WaitTurn stop_reason = %q, want %s", got, stopReason)
	}
	return v
}

// TestSupervisorPublicAPILoopPhasesSucceed proves a loop child that runs
// every phase to end_turn is caller-visible as done: one session.end per
// phase, the loop lifecycle events on the stream, the final phase's output in
// RuntimeResult, the terminal sentinel on disk, and an ended List row with
// the zero exit code.
func TestSupervisorPublicAPILoopPhasesSucceed(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(
		itestAttempt("ses_loop_prep", itestSessionEnd("ses_loop_prep", "end_turn", "prep done")),
		itestAttempt("ses_loop_work", itestSessionEnd("ses_loop_work", "end_turn", "work done")),
		itestAttempt("ses_loop_wrap", itestSessionEnd("ses_loop_wrap", "end_turn", "wrap done")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"pre":            []map[string]any{{"name": "prep", "prompt": "prep work"}},
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
		"post":           []map[string]any{{"name": "wrap", "prompt": "wrap up"}},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-ok"})

	// Each phase attempt is a caller-visible session ending on the stream.
	for _, out := range []string{"prep done", "work done", "wrap done"} {
		h.log.waitFor(t, out+" session.end", func(ev events.Event) bool {
			o, _ := ev.Fields["final_output"].(string)
			return itestEventIs("session.end", rt)(ev) && o == out
		})
	}
	// The loop runner's lifecycle events reach the control stream, and the
	// single iteration that ran to end_turn reports max_iterations.
	end := h.log.waitFor(t, "avenor.loop.end", itestEventIs("avenor.loop.end", rt))
	if reason, _ := end.Fields["exit_reason"].(string); reason != "max_iterations" {
		t.Fatalf("avenor.loop.end exit_reason = %q, want max_iterations", reason)
	}

	v := h.requireWorkflowEnd(rt, "done", "end_turn", 0)
	if !v.TurnComplete {
		t.Fatal("done view TurnComplete = false, want true")
	}
	// The last settled phase is the authoritative final output.
	if v.FinalOutput != "wrap done" {
		t.Fatalf("status final_output = %q, want %q", v.FinalOutput, "wrap done")
	}
	if got := h.result(rt); got != "wrap done" {
		t.Fatalf("RuntimeResult final_output = %q, want %q", got, "wrap done")
	}
	// The terminal sentinel declares the success on disk.
	if status, _, _ := itestSentinelStatus(h.sentinelText(rt)); status != "done" {
		t.Fatalf("sentinel status = %q, want done", status)
	}
	if starts, resumes := provider.counts(); starts != 3 || resumes != 0 {
		t.Fatalf("provider starts/resumes = %d/%d, want 3/0 (one attempt per phase)", starts, resumes)
	}
	// A finished loop child is an ended row, not a parked idle row.
	rows := h.listed("loop-ok")
	if len(rows) != 1 || rows[0]["runtime_id"] != rt {
		t.Fatalf("List = %v, want the one runtime %s", rows, rt)
	}
	if got := rows[0]["status"]; got != "ended" {
		t.Fatalf("List status = %v, want ended", got)
	}
	if got := rows[0]["exit_code"]; got != 0 {
		t.Fatalf("List exit_code = %v, want 0", got)
	}
}

// TestSupervisorPublicAPILoopRunsEveryIteration proves max_iterations drives
// the loop phase once per iteration before the child ends: two provider
// starts for the loop phase plus one for the post phase, two completed
// iterations on the loop end event, and the post phase's output as the
// result.
func TestSupervisorPublicAPILoopRunsEveryIteration(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(
		itestAttempt("ses_loop_it1", itestSessionEnd("ses_loop_it1", "end_turn", "iteration one")),
		itestAttempt("ses_loop_it2", itestSessionEnd("ses_loop_it2", "end_turn", "iteration two")),
		itestAttempt("ses_loop_wrap", itestSessionEnd("ses_loop_wrap", "end_turn", "wrap done")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 2,
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
		"post":           []map[string]any{{"name": "wrap", "prompt": "wrap up"}},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-iterations"})

	// The single loop end reports the two iterations it completed.
	end := h.log.waitFor(t, "avenor.loop.end", itestEventIs("avenor.loop.end", rt))
	completed := 0
	switch got := end.Fields["iterations_completed"].(type) {
	case int:
		completed = got
	case float64:
		completed = int(got)
	}
	if completed != 2 {
		t.Fatalf("avenor.loop.end iterations_completed = %v, want 2", end.Fields["iterations_completed"])
	}

	v := h.requireWorkflowEnd(rt, "done", "end_turn", 0)
	if v.FinalOutput != "wrap done" {
		t.Fatalf("status final_output = %q, want %q", v.FinalOutput, "wrap done")
	}
	if got := h.result(rt); got != "wrap done" {
		t.Fatalf("RuntimeResult final_output = %q, want %q", got, "wrap done")
	}
	if starts, resumes := provider.counts(); starts != 3 || resumes != 0 {
		t.Fatalf("provider starts/resumes = %d/%d, want 3/0 (one attempt per iteration plus the post phase)", starts, resumes)
	}
}

// TestSupervisorPublicAPILoopPhasePermissionRequest proves a permission
// request raised mid-attempt inside a loop phase surfaces to the caller as a
// pending permission on the child and, once answered, lets the loop run to
// completion.
func TestSupervisorPublicAPILoopPhasePermissionRequest(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	provider := newItestProvider(
		itestAttempt("ses_loop_perm",
			itestPermissionRequest("ses_loop_perm", "req_loop_perm"),
			gate.step(),
			itestSessionEnd("ses_loop_perm", "end_turn", "answered work")),
		itestAttempt("ses_loop_wrap", itestSessionEnd("ses_loop_wrap", "end_turn", "wrap done")),
	)
	provider.onAnswer = func(string, runtime.PermissionResponse) { gate.open() }
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))
	h.connectClient()

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
		"post":           []map[string]any{{"name": "wrap", "prompt": "wrap up"}},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-permission"})

	h.log.waitFor(t, "permission.request", itestEventIs("permission.request", rt))
	v := h.view(rt)
	if !v.PendingPermission || v.Status != "running" {
		t.Fatalf("view with a pending request = %+v, want running with pending_permission", v)
	}
	// The phase cannot settle before its request is answered.
	if n := h.log.count(itestStopReasonIs(rt, "end_turn")); n != 0 {
		t.Fatalf("session.end events before the answer = %d, want 0", n)
	}
	if err := h.sup.RuntimeAnswerPermission(rt, "req_loop_perm", "allow_once", ""); err != nil {
		t.Fatalf("RuntimeAnswerPermission: %v", err)
	}
	h.log.waitFor(t, "permission.response", itestEventIs("permission.response", rt))
	// The loop completes: the released phase ends and the post phase runs.
	h.log.waitFor(t, "post session.end", itestMemberEndIs("ses_loop_wrap", "end_turn"))
	h.requireWorkflowEnd(rt, "done", "end_turn", 0)
	if got := h.result(rt); got != "wrap done" {
		t.Fatalf("RuntimeResult final_output = %q, want %q", got, "wrap done")
	}
	if v := h.view(rt); v.PendingPermission {
		t.Fatal("pending_permission still set after the answer")
	}
	answers := provider.recordedAnswers()
	if len(answers) != 1 || !answers[0].Allow || answers[0].OptionID != "allow_once" {
		t.Fatalf("backend permission answers = %+v, want one allow_once", answers)
	}
}

func TestSupervisorPublicAPILoopPhaseFailureExhaustsRetries(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(
		itestAttempt("ses_loop_rex1", itestSessionEnd("ses_loop_rex1", "error", "")),
		itestAttempt("ses_loop_rex2", itestSessionEnd("ses_loop_rex2", "error", "")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-retry-exhaust", "max_retries": 1})

	// Both the initial attempt and the one retry end with a failed session.end.
	h.log.waitFor(t, "first failure session.end", itestMemberEndIs("ses_loop_rex1", "error"))
	h.log.waitFor(t, "retry failure session.end", itestMemberEndIs("ses_loop_rex2", "error"))
	if n := h.log.count(func(ev events.Event) bool {
		return itestMemberEndIs("ses_loop_rex1", "error")(ev) || itestMemberEndIs("ses_loop_rex2", "error")(ev)
	}); n != 2 {
		t.Fatalf("failed session.end events = %d, want 2 (initial attempt plus one retry)", n)
	}
	// The child's terminal session.end is the failed attempt's own event and
	// carries no exit_code, so the end state is asserted field by field.
	h.awaitStatus(rt, "failed")
	v := h.view(rt)
	if v.StopReason != "error" {
		t.Fatalf("failed view stop reason = %q, want error", v.StopReason)
	}
	if code := h.rawExitCode(rt); code != 1 {
		t.Fatalf("raw RuntimeStatus exit_code = %d, want 1", code)
	}
	turn := h.waitTurn(rt)
	if got, _ := turn["stop_reason"].(string); got != "error" {
		t.Fatalf("WaitTurn stop_reason = %q, want error", got)
	}
	if starts, resumes := provider.counts(); starts != 2 || resumes != 0 {
		t.Fatalf("provider starts/resumes = %d/%d, want 2/0 (exhausted budget starts no third attempt)", starts, resumes)
	}
	rows := h.listed("loop-retry-exhaust")
	if len(rows) != 1 || rows[0]["status"] != "ended" || rows[0]["exit_code"] != 1 {
		t.Fatalf("List row = %v, want ended with exit code 1", rows)
	}
}

// TestSupervisorPublicAPILoopRunDeadlineDuringPhase proves a caller-set run
// deadline ends a loop phase that never settles: the child ends as a timeout
// with exit code 124 and the phase attempt is never retried.
func TestSupervisorPublicAPILoopRunDeadlineDuringPhase(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	// The scripted attempt never reaches a session.end: only the run
	// deadline can stop the phase.
	provider := newItestProvider(itestAttempt("ses_loop_deadline", gate.step()))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-deadline", "timeout": 1})

	h.log.waitFor(t, "timeout session.end", itestStopReasonIs(rt, "timeout"))
	h.requireWorkflowEnd(rt, "timeout", "timeout", 124)
	if starts, _ := provider.counts(); starts != 1 {
		t.Fatalf("provider starts = %d, want 1 (a timed-out phase is not retried)", starts)
	}
}

// TestSupervisorPublicAPILoopCancelDuringPhase proves RuntimeCancel during a
// running phase kills the loop child with exit code 130 and no further phase
// ever starts.
func TestSupervisorPublicAPILoopCancelDuringPhase(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	provider := newItestProvider(
		itestAttempt("ses_loop_c1", itestWorking("ses_loop_c1"), gate.step(),
			itestSessionEnd("ses_loop_c1", "end_turn", "never reached")),
		itestAttempt("ses_loop_c2", itestSessionEnd("ses_loop_c2", "end_turn", "never started")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop": []map[string]any{
			{"name": "first", "prompt": "first work"},
			{"name": "second", "prompt": "second work"},
		},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-cancel"})

	h.log.waitFor(t, "working status", itestEventIs("agent.status", rt))
	if v := h.view(rt); v.Status != "running" || v.Phase != "working" {
		t.Fatalf("view before cancel = %+v, want running in the working phase", v)
	}
	if err := h.sup.RuntimeCancel(rt); err != nil {
		t.Fatalf("RuntimeCancel: %v", err)
	}
	h.requireWorkflowEnd(rt, "killed", "cancelled", 130)
	// The cancelled run must not wake into the second phase: once the child
	// has ended, the attempt count is final.
	if starts, _ := provider.counts(); starts != 1 {
		t.Fatalf("provider starts = %d, want 1 (the second phase never starts)", starts)
	}
	if got := h.result(rt); got == "never started" {
		t.Fatal("cancelled loop child produced the output of a phase it never ran")
	}
}

// TestSupervisorPublicAPILoopCancelDuringRetryBackoff proves a cancel issued
// while the loop is inside a retry backoff ends the child as killed without
// waking into the retry attempt: the caller sees the killed status with exit
// code 130, and the retry never starts.
//
// Characterization note: unlike an ordinary runtime (whose idle-time cancel
// records a cancelled session.end and reports "cancelled" through WaitTurn),
// a loop child killed in backoff reports the failed attempt's own stop
// reason through WaitTurn, because the loop's cancel path writes only the
// sentinel. This test pins the caller-visible end state that both paths
// share: killed status, exit code 130, and no retry attempt.
func TestSupervisorPublicAPILoopCancelDuringRetryBackoff(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(itestAttempt("ses_loop_cb", itestSessionEnd("ses_loop_cb", "error", "")))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
	})
	// The first retry's backoff is phaseconfig.BackoffDuration(0) long, so the
	// cancel issued right after the failed attempt's session.end lands inside
	// the backoff window.
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-backoff-cancel", "max_retries": 1})
	h.log.waitFor(t, "failed attempt session.end", func(ev events.Event) bool {
		return itestMemberEndIs("ses_loop_cb", "error")(ev)
	})
	// The cancel is level-triggered on the run context, so issuing it now is
	// guaranteed to be in force by the time the backoff select runs.
	if err := h.sup.RuntimeCancel(rt); err != nil {
		t.Fatalf("RuntimeCancel: %v", err)
	}
	h.awaitStatus(rt, "killed")
	if v := h.view(rt); v.StopReason != "cancelled" {
		t.Fatalf("killed view stop reason = %q, want cancelled", v.StopReason)
	}
	if code := h.rawExitCode(rt); code != 130 {
		t.Fatalf("raw RuntimeStatus exit_code = %d, want 130", code)
	}
	// The cancelled backoff must not wake into the retry attempt: once the
	// child has ended, the attempt count is final.
	if starts, _ := provider.counts(); starts != 1 {
		t.Fatalf("provider starts = %d, want 1 (the retry never starts)", starts)
	}
}

// TestSupervisorPublicAPILoopPhaseResumesPreviousSession proves a loop phase
// marked resume_from_previous resumes the exact session ID the previous phase
// attempt produced: the provider sees one Resume carrying that ID, and the
// runtime adopts the session IDs Start and Resume returned.
func TestSupervisorPublicAPILoopPhaseResumesPreviousSession(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(
		itestAttempt("ses_loop_r1", itestSessionEnd("ses_loop_r1", "end_turn", "first phase")),
		itestAttempt("ses_loop_r2", itestSessionEnd("ses_loop_r2", "end_turn", "second phase")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop": []map[string]any{
			{"name": "first", "prompt": "first work"},
			{"name": "second", "prompt": "second work", "resume_from_previous": true},
		},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-resume"})

	h.requireWorkflowEnd(rt, "done", "end_turn", 0)
	if got := h.result(rt); got != "second phase" {
		t.Fatalf("RuntimeResult final_output = %q, want %q", got, "second phase")
	}
	if starts, resumes := provider.counts(); starts != 1 || resumes != 1 {
		t.Fatalf("provider starts/resumes = %d/%d, want 1/1 (the second phase resumes)", starts, resumes)
	}
	if ids := provider.resumeIDs(); len(ids) != 1 || ids[0] != "ses_loop_r1" {
		t.Fatalf("Resume arguments = %v, want [ses_loop_r1] (the first attempt's session ID)", ids)
	}
	if ids := provider.returnedSessionIDs(); len(ids) != 2 || ids[0] != "ses_loop_r1" || ids[1] != "ses_loop_r2" {
		t.Fatalf("Start/Resume session IDs = %v, want [ses_loop_r1 ses_loop_r2]", ids)
	}
}

// TestSupervisorPublicAPILoopRequiresNudgeResumesOwnSession proves a loop
// phase whose required files stay missing is nudged as a resume of its own
// attempt: the provider sees one Resume carrying the first attempt's session
// ID, and the still-unmet requirement fails the child.
func TestSupervisorPublicAPILoopRequiresNudgeResumesOwnSession(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(
		itestAttempt("ses_loop_n1", itestSessionEnd("ses_loop_n1", "end_turn", "claimed done")),
		itestAttempt("ses_loop_n2", itestSessionEnd("ses_loop_n2", "end_turn", "still no file")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop": []map[string]any{{
			"name": "work", "prompt": "loop work",
			"requires": map[string]any{"files": []string{"never-written.txt"}},
		}},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-nudge"})

	v := h.awaitStatus(rt, "failed")
	if v.StopReason != "incomplete_output" {
		t.Fatalf("failed view stop reason = %q, want incomplete_output", v.StopReason)
	}
	// The sentinel classifies the unmet requirement the same way callers do.
	if status, stopReason, code := itestSentinelStatus(h.sentinelText(rt)); status != "failed" || stopReason != "incomplete_output" || code != 1 {
		t.Fatalf("sentinel = (%q, %q, %d), want (failed, incomplete_output, 1)", status, stopReason, code)
	}
	if starts, resumes := provider.counts(); starts != 1 || resumes != 1 {
		t.Fatalf("provider starts/resumes = %d/%d, want 1/1 (the nudge resumes the attempt)", starts, resumes)
	}
	if ids := provider.resumeIDs(); len(ids) != 1 || ids[0] != "ses_loop_n1" {
		t.Fatalf("Resume arguments = %v, want [ses_loop_n1] (the nudged attempt's session ID)", ids)
	}
	if ids := provider.returnedSessionIDs(); len(ids) != 2 || ids[0] != "ses_loop_n1" || ids[1] != "ses_loop_n2" {
		t.Fatalf("Start/Resume session IDs = %v, want [ses_loop_n1 ses_loop_n2]", ids)
	}
	rows := h.listed("loop-nudge")
	if len(rows) != 1 || rows[0]["status"] != "ended" || rows[0]["exit_code"] != 1 {
		t.Fatalf("List row = %v, want ended with exit code 1", rows)
	}
}

// TestSupervisorPublicAPITeamPrePhaseResumesPreviousSession proves a team pre
// phase marked resume_from_previous resumes the exact session ID the previous
// pre phase produced, while the team member that follows still starts fresh.
func TestSupervisorPublicAPITeamPrePhaseResumesPreviousSession(t *testing.T) {
	t.Parallel()
	prep := newItestProvider(
		itestAttempt("ses_team_r1", itestSessionEnd("ses_team_r1", "end_turn", "prep done")),
		itestAttempt("ses_team_r2", itestSessionEnd("ses_team_r2", "end_turn", "polish done")),
	)
	worker := newItestProvider(itestAttempt("ses_team_r3", itestSessionEnd("ses_team_r3", "end_turn", "team done")))
	h := newItestHarness(t, Config{}, itestTeamProviders(map[string]*itestProvider{
		"prep-agent":   prep,
		"worker-agent": worker,
	}))

	file := itestWorkflowConfig(t, "team", map[string]any{
		"pre": []map[string]any{
			{"name": "prep", "prompt": "prep work", "agent": "prep-agent"},
			{"name": "polish", "prompt": "polish work", "agent": "prep-agent", "resume_from_previous": true},
		},
		"team": []map[string]any{{"name": "worker", "prompt": "team work", "agent": "worker-agent"}},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-resume"})

	h.requireWorkflowEnd(rt, "done", "end_turn", 0)
	if ids := prep.resumeIDs(); len(ids) != 1 || ids[0] != "ses_team_r1" {
		t.Fatalf("prep Resume arguments = %v, want [ses_team_r1] (the first pre phase's session ID)", ids)
	}
	if ids := prep.returnedSessionIDs(); len(ids) != 2 || ids[0] != "ses_team_r1" || ids[1] != "ses_team_r2" {
		t.Fatalf("prep Start/Resume session IDs = %v, want [ses_team_r1 ses_team_r2]", ids)
	}
	if ids := worker.returnedSessionIDs(); len(ids) != 1 || ids[0] != "ses_team_r3" {
		t.Fatalf("worker Start/Resume session IDs = %v, want [ses_team_r3] (the member starts fresh)", ids)
	}
	if starts, resumes := worker.counts(); starts != 1 || resumes != 0 {
		t.Fatalf("worker starts/resumes = %d/%d, want 1/0 (the member starts fresh after the resumed pre phases)", starts, resumes)
	}
}

// TestSupervisorPublicAPILoopProviderFactoryFailure proves a provider factory
// error during a loop phase fails the child without any attempt: the caller
// sees the failed status with exit code 1, the error sentinel, and an ended
// List row, and no session was ever started.
func TestSupervisorPublicAPILoopProviderFactoryFailure(t *testing.T) {
	t.Parallel()
	var factoryCalls atomic.Int32
	h := newItestHarness(t, Config{}, func(runtime.StartOptions, string) (runtime.Provider, error) {
		factoryCalls.Add(1)
		return nil, fmt.Errorf("provider factory offline")
	})

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-factory-fail"})

	v := h.awaitStatus(rt, "failed")
	if v.StopReason != "error" {
		t.Fatalf("failed view stop reason = %q, want error", v.StopReason)
	}
	// The sentinel classifies the start failure the same way callers do.
	if status, stopReason, code := itestSentinelStatus(h.sentinelText(rt)); status != "failed" || stopReason != "error" || code != 1 {
		t.Fatalf("sentinel = (%q, %q, %d), want (failed, error, 1)", status, stopReason, code)
	}
	if got := factoryCalls.Load(); got != 1 {
		t.Fatalf("factory calls = %d, want 1 (the failed factory is not retried)", got)
	}
	rows := h.listed("loop-factory-fail")
	if len(rows) != 1 || rows[0]["status"] != "ended" || rows[0]["exit_code"] != 1 {
		t.Fatalf("List row = %v, want ended with exit code 1", rows)
	}
}

// TestSupervisorPublicAPITeamMemberStartFailure proves a member's scripted
// Start error fails the team child: the caller sees the failed status with
// exit code 1, the error sentinel, and an ended List row.
func TestSupervisorPublicAPITeamMemberStartFailure(t *testing.T) {
	t.Parallel()
	failer := newItestProvider(stableScriptedAttempt{sessionID: "ses_team_startfail", startErr: fmt.Errorf("start exploded")})
	h := newItestHarness(t, Config{}, itestTeamProviders(map[string]*itestProvider{"fail-agent": failer}))

	file := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{{"name": "solo", "prompt": "solo work", "agent": "fail-agent"}},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-start-fail"})

	// A team member whose provider start fails is classified by the team
	// runner as phase_error, not error: the internal error becomes the member's
	// stop reason before the sentinel is written.
	v := h.awaitStatus(rt, "failed")
	if v.StopReason != "phase_error" {
		t.Fatalf("failed view stop reason = %q, want phase_error", v.StopReason)
	}
	if status, stopReason, code := itestSentinelStatus(h.sentinelText(rt)); status != "failed" || stopReason != "phase_error" || code != 1 {
		t.Fatalf("sentinel = (%q, %q, %d), want (failed, phase_error, 1)", status, stopReason, code)
	}
	if starts, _ := failer.counts(); starts != 1 {
		t.Fatalf("provider starts = %d, want 1 (the failed start is not retried)", starts)
	}
	rows := h.listed("team-start-fail")
	if len(rows) != 1 || rows[0]["status"] != "ended" || rows[0]["exit_code"] != 1 {
		t.Fatalf("List row = %v, want ended with exit code 1", rows)
	}
}

// Team scenarios.

// TestSupervisorPublicAPITeamMembersSucceed proves a team child whose members
// all end end_turn is caller-visible as done: one session.end per member, a
// zero exit code, an ended row, and a final output produced by one of the
// members (the members settle concurrently, so either member's output is a
// legitimate final).
func TestSupervisorPublicAPITeamMembersSucceed(t *testing.T) {
	t.Parallel()
	alpha := newItestProvider(itestAttempt("ses_team_alpha", itestSessionEnd("ses_team_alpha", "end_turn", "alpha done")))
	beta := newItestProvider(itestAttempt("ses_team_beta", itestSessionEnd("ses_team_beta", "end_turn", "beta done")))
	h := newItestHarness(t, Config{}, itestTeamProviders(map[string]*itestProvider{
		"alpha-agent": alpha,
		"beta-agent":  beta,
	}))

	file := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{
			{"name": "alpha", "prompt": "alpha work", "agent": "alpha-agent"},
			{"name": "beta", "prompt": "beta work", "agent": "beta-agent"},
		},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-ok"})

	for _, sessionID := range []string{"ses_team_alpha", "ses_team_beta"} {
		h.log.waitFor(t, sessionID+" session.end", itestMemberEndIs(sessionID, "end_turn"))
	}
	h.log.waitFor(t, "avenor.team.end", itestEventIs("avenor.team.end", rt))

	h.requireWorkflowEnd(rt, "done", "end_turn", 0)
	got := h.result(rt)
	if got != "alpha done" && got != "beta done" {
		t.Fatalf("RuntimeResult final_output = %q, want a member output", got)
	}
	alphaStarts, _ := alpha.counts()
	betaStarts, _ := beta.counts()
	if alphaStarts != 1 || betaStarts != 1 {
		t.Fatalf("member starts = %d/%d, want 1/1", alphaStarts, betaStarts)
	}
	rows := h.listed("team-ok")
	if len(rows) != 1 || rows[0]["status"] != "ended" || rows[0]["exit_code"] != 0 {
		t.Fatalf("List row = %v, want ended with exit code 0", rows)
	}
}

// TestSupervisorPublicAPITeamMemberFailureLeavesPeersRunning proves the
// team's failure semantics a caller sees: one member's ordinary failure does
// not stop the peers — they run to completion — and the child then ends
// failed with the failed member's exit code and stop reason.
func TestSupervisorPublicAPITeamMemberFailureLeavesPeersRunning(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	alpha := newItestProvider(itestAttempt("ses_team_fail_alpha", gate.step(),
		itestSessionEnd("ses_team_fail_alpha", "end_turn", "alpha finished")))
	beta := newItestProvider(itestAttempt("ses_team_fail_beta", itestSessionEnd("ses_team_fail_beta", "error", "")))
	h := newItestHarness(t, Config{}, itestTeamProviders(map[string]*itestProvider{
		"alpha-agent": alpha,
		"beta-agent":  beta,
	}))

	file := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{
			{"name": "alpha", "prompt": "alpha work", "agent": "alpha-agent"},
			{"name": "beta", "prompt": "beta work", "agent": "beta-agent"},
		},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-failure"})

	// Beta fails while alpha is still working.
	h.log.waitFor(t, "beta failure session.end", itestMemberEndIs("ses_team_fail_beta", "error"))
	// The peer keeps running: an ordinary member failure does not cancel the
	// team. If a failure tore down the team, the child would end within this
	// window and the assertion fails.
	h.consistentlyFor(300*time.Millisecond, "the peer keeps running after a member failure", func() bool {
		return h.view(rt).Status == "running"
	})
	// Released, the peer runs to its own end_turn.
	gate.open()
	h.log.waitFor(t, "alpha session.end", itestMemberEndIs("ses_team_fail_alpha", "end_turn"))
	h.awaitStatus(rt, "failed")
	if v := h.view(rt); v.StopReason != "error" {
		t.Fatalf("failed view stop reason = %q, want error", v.StopReason)
	}
	if code := h.rawExitCode(rt); code != 1 {
		t.Fatalf("raw RuntimeStatus exit_code = %d, want 1", code)
	}
	// WaitTurn's stop reason for a failed team is deliberately not pinned:
	// it reflects the last member to settle rather than the team result.
	alphaStarts, _ := alpha.counts()
	betaStarts, _ := beta.counts()
	if alphaStarts != 1 || betaStarts != 1 {
		t.Fatalf("member starts = %d/%d, want 1/1", alphaStarts, betaStarts)
	}
}

// TestSupervisorPublicAPITeamMemberRetrySucceeds proves a member's retryable
// failure is retried inside the team: the member runs a second attempt after
// the backoff while its peer is unaffected, and the team ends done once the
// retry succeeds.
func TestSupervisorPublicAPITeamMemberRetrySucceeds(t *testing.T) {
	t.Parallel()
	alpha := newItestProvider(itestAttempt("ses_team_retry_alpha", itestSessionEnd("ses_team_retry_alpha", "end_turn", "alpha done")))
	beta := newItestProvider(
		itestAttempt("ses_team_retry_beta1", itestSessionEnd("ses_team_retry_beta1", "error", "")),
		itestAttempt("ses_team_retry_beta2", itestSessionEnd("ses_team_retry_beta2", "end_turn", "beta done")),
	)
	h := newItestHarness(t, Config{}, itestTeamProviders(map[string]*itestProvider{
		"alpha-agent": alpha,
		"beta-agent":  beta,
	}))

	file := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{
			{"name": "alpha", "prompt": "alpha work", "agent": "alpha-agent"},
			{"name": "beta", "prompt": "beta work", "agent": "beta-agent"},
		},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-member-retry", "max_retries": 1})

	h.log.waitFor(t, "failed member session.end", itestMemberEndIs("ses_team_retry_beta1", "error"))
	h.log.waitFor(t, "retry session.end", itestMemberEndIs("ses_team_retry_beta2", "end_turn"))
	h.requireWorkflowEnd(rt, "done", "end_turn", 0)
	got := h.result(rt)
	if got != "alpha done" && got != "beta done" {
		t.Fatalf("RuntimeResult final_output = %q, want a member output", got)
	}
	alphaStarts, _ := alpha.counts()
	betaStarts, _ := beta.counts()
	if alphaStarts != 1 || betaStarts != 2 {
		t.Fatalf("member starts = %d/%d, want 1/2 (the failed member retried once)", alphaStarts, betaStarts)
	}
}

// TestSupervisorPublicAPITeamMemberPermissionRequest proves a permission
// request raised by a team member surfaces as a pending permission on the
// team child and, once answered, lets the team run to completion.
func TestSupervisorPublicAPITeamMemberPermissionRequest(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	alpha := newItestProvider(
		itestAttempt("ses_team_perm_alpha",
			itestPermissionRequest("ses_team_perm_alpha", "req_team_perm"),
			gate.step(),
			itestSessionEnd("ses_team_perm_alpha", "end_turn", "alpha answered")),
	)
	alpha.onAnswer = func(string, runtime.PermissionResponse) { gate.open() }
	beta := newItestProvider(itestAttempt("ses_team_perm_beta", itestSessionEnd("ses_team_perm_beta", "end_turn", "beta done")))
	h := newItestHarness(t, Config{}, itestTeamProviders(map[string]*itestProvider{
		"alpha-agent": alpha,
		"beta-agent":  beta,
	}))
	h.connectClient()

	file := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{
			{"name": "alpha", "prompt": "alpha work", "agent": "alpha-agent"},
			{"name": "beta", "prompt": "beta work", "agent": "beta-agent"},
		},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-permission"})

	h.log.waitFor(t, "permission.request", itestEventIs("permission.request", rt))
	v := h.view(rt)
	if !v.PendingPermission || v.Status != "running" {
		t.Fatalf("view with a pending request = %+v, want running with pending_permission", v)
	}
	// The requesting member cannot settle before its request is answered.
	if n := h.log.count(itestMemberEndIs("ses_team_perm_alpha", "end_turn")); n != 0 {
		t.Fatalf("requesting member session.end events before the answer = %d, want 0", n)
	}
	if err := h.sup.RuntimeAnswerPermission(rt, "req_team_perm", "allow_once", ""); err != nil {
		t.Fatalf("RuntimeAnswerPermission: %v", err)
	}
	h.log.waitFor(t, "permission.response", itestEventIs("permission.response", rt))
	for _, sessionID := range []string{"ses_team_perm_alpha", "ses_team_perm_beta"} {
		h.log.waitFor(t, sessionID+" session.end", itestMemberEndIs(sessionID, "end_turn"))
	}
	h.requireWorkflowEnd(rt, "done", "end_turn", 0)
	if v := h.view(rt); v.PendingPermission {
		t.Fatal("pending_permission still set after the answer")
	}
	answers := alpha.recordedAnswers()
	if len(answers) != 1 || !answers[0].Allow || answers[0].OptionID != "allow_once" {
		t.Fatalf("backend permission answers = %+v, want one allow_once", answers)
	}
}

// TestSupervisorPublicAPITeamRunDeadline proves a caller-set run deadline
// ends a team whose members never settle: the child ends as a timeout with
// exit code 124, and every member attempt is stopped with its own timeout
// session.end.
func TestSupervisorPublicAPITeamRunDeadline(t *testing.T) {
	t.Parallel()
	alphaGate := newItestGate()
	defer alphaGate.open()
	betaGate := newItestGate()
	defer betaGate.open()
	alpha := newItestProvider(itestAttempt("ses_team_deadline_alpha", alphaGate.step()))
	beta := newItestProvider(itestAttempt("ses_team_deadline_beta", betaGate.step()))
	h := newItestHarness(t, Config{}, itestTeamProviders(map[string]*itestProvider{
		"alpha-agent": alpha,
		"beta-agent":  beta,
	}))

	file := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{
			{"name": "alpha", "prompt": "alpha work", "agent": "alpha-agent"},
			{"name": "beta", "prompt": "beta work", "agent": "beta-agent"},
		},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-deadline", "timeout": 1})

	// Both members were stopped by the shared deadline, each with its own
	// timeout session.end. Waiting for each member's session.end first keeps
	// the async event-log goroutine from lagging the count below.
	for _, sessionID := range []string{"ses_team_deadline_alpha", "ses_team_deadline_beta"} {
		h.log.waitFor(t, sessionID+" timeout session.end", itestMemberEndIs(sessionID, "timeout"))
	}
	h.requireWorkflowEnd(rt, "timeout", "timeout", 124)
	if n := h.log.count(itestStopReasonIs(rt, "timeout")); n != 2 {
		t.Fatalf("timeout session.end events = %d, want 2 (one per member)", n)
	}
	alphaStarts, _ := alpha.counts()
	betaStarts, _ := beta.counts()
	if alphaStarts != 1 || betaStarts != 1 {
		t.Fatalf("member starts = %d/%d, want 1/1", alphaStarts, betaStarts)
	}
}

// TestSupervisorPublicAPITeamCancel proves RuntimeCancel stops every member
// and ends the team child as killed with exit code 130.
func TestSupervisorPublicAPITeamCancel(t *testing.T) {
	t.Parallel()
	alphaGate := newItestGate()
	defer alphaGate.open()
	betaGate := newItestGate()
	defer betaGate.open()
	alpha := newItestProvider(itestAttempt("ses_team_cancel_alpha",
		itestWorking("ses_team_cancel_alpha"), alphaGate.step()))
	beta := newItestProvider(itestAttempt("ses_team_cancel_beta",
		itestWorking("ses_team_cancel_beta"), betaGate.step()))
	h := newItestHarness(t, Config{}, itestTeamProviders(map[string]*itestProvider{
		"alpha-agent": alpha,
		"beta-agent":  beta,
	}))

	file := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{
			{"name": "alpha", "prompt": "alpha work", "agent": "alpha-agent"},
			{"name": "beta", "prompt": "beta work", "agent": "beta-agent"},
		},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-cancel"})

	// Both members are in flight before the cancel lands: each member's
	// working status is matched by session ID, since every event of the child
	// shares one runtime id and a runtime-id match would return early on the
	// first member alone.
	for _, sessionID := range []string{"ses_team_cancel_alpha", "ses_team_cancel_beta"} {
		h.log.waitFor(t, sessionID+" working status", itestMemberWorkingIs(sessionID))
	}
	if err := h.sup.RuntimeCancel(rt); err != nil {
		t.Fatalf("RuntimeCancel: %v", err)
	}
	// Waiting for each member's cancelled session.end first keeps the async
	// event-log goroutine from lagging the count below. The members'
	// session.end events carry no exit_code, so the 130 is validated through
	// RuntimeStatus alone.
	for _, sessionID := range []string{"ses_team_cancel_alpha", "ses_team_cancel_beta"} {
		h.log.waitFor(t, sessionID+" cancelled session.end", itestMemberEndIs(sessionID, "cancelled"))
	}
	h.requireWorkflowEnd(rt, "killed", "cancelled", 130)
	if n := h.log.count(itestStopReasonIs(rt, "cancelled")); n != 2 {
		t.Fatalf("cancelled session.end events = %d, want 2 (one per member)", n)
	}
}

// TestSupervisorPublicAPITeamWaitTurnWakesAfterLastMember proves a waiter
// registered while every member is still running wakes when the last member
// settles: the child's turn settle fires only once no attempt is active, so
// a member that settles first cannot wake the waiter alone.
func TestSupervisorPublicAPITeamWaitTurnWakesAfterLastMember(t *testing.T) {
	t.Parallel()
	alphaGate := newItestGate()
	defer alphaGate.open()
	betaGate := newItestGate()
	defer betaGate.open()
	alpha := newItestProvider(itestAttempt("ses_team_wait_alpha",
		itestWorking("ses_team_wait_alpha"), alphaGate.step(),
		itestSessionEnd("ses_team_wait_alpha", "end_turn", "alpha done")))
	beta := newItestProvider(itestAttempt("ses_team_wait_beta",
		itestWorking("ses_team_wait_beta"), betaGate.step(),
		itestSessionEnd("ses_team_wait_beta", "end_turn", "beta done")))
	h := newItestHarness(t, Config{}, itestTeamProviders(map[string]*itestProvider{
		"alpha-agent": alpha,
		"beta-agent":  beta,
	}))

	file := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{
			{"name": "alpha", "prompt": "alpha work", "agent": "alpha-agent"},
			{"name": "beta", "prompt": "beta work", "agent": "beta-agent"},
		},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-waitturn"})

	for _, sessionID := range []string{"ses_team_wait_alpha", "ses_team_wait_beta"} {
		h.log.waitFor(t, sessionID+" working status", itestMemberWorkingIs(sessionID))
	}
	// Register the waiter while both members are still running.
	returned := make(chan map[string]any, 1)
	waitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	go func() {
		out, err := h.sup.RuntimeWaitTurn(waitCtx, rt, 0)
		if err != nil {
			return
		}
		if res, ok := out.(map[string]any); ok {
			returned <- res
		}
	}()

	// Alpha settles first; the waiter must not wake while beta is still an
	// active attempt.
	alphaGate.open()
	h.log.waitFor(t, "alpha session.end", itestMemberEndIs("ses_team_wait_alpha", "end_turn"))
	h.consistentlyFor(300*time.Millisecond, "the waiter stays blocked while a member still runs", func() bool {
		select {
		case <-returned:
			return false
		default:
			return true
		}
	})
	// The last member settling wakes the waiter.
	betaGate.open()
	res := itestRecv(t, returned, "the waiter waking after the last member settles")
	if got, _ := res["stop_reason"].(string); got != "end_turn" {
		t.Fatalf("WaitTurn stop_reason = %q, want end_turn", got)
	}
}

// Shutdown and List scenarios.

// TestSupervisorPublicAPILoopShutdownGracefulEndsActiveChild proves graceful
// Shutdown tears down a loop child mid-phase and leaves the killed end state
// callers observe.
func TestSupervisorPublicAPILoopShutdownGracefulEndsActiveChild(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	provider := newItestProvider(itestAttempt("ses_loop_sd",
		itestWorking("ses_loop_sd"), gate.step(),
		itestSessionEnd("ses_loop_sd", "end_turn", "never reached")))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	file := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
	})
	rt := h.spawnWorkflow("loop", file, map[string]any{"label": "loop-shutdown"})
	h.log.waitFor(t, "working status", itestEventIs("agent.status", rt))
	if v := h.view(rt); v.Status != "running" || v.Phase != "working" {
		t.Fatalf("view before shutdown = %+v, want running in the working phase", v)
	}
	h.shutdown("graceful")
	h.awaitStatus(rt, "killed")
	if v := h.view(rt); v.StopReason != "cancelled" {
		t.Fatalf("killed view stop reason = %q, want cancelled", v.StopReason)
	}
	if code := h.rawExitCode(rt); code != 130 {
		t.Fatalf("raw RuntimeStatus exit_code = %d, want 130", code)
	}
	if got := h.result(rt); got == "never reached" {
		t.Fatal("shutdown loop child produced the output of a phase it never finished")
	}
}

// TestSupervisorPublicAPITeamShutdownKillEndsActiveChild proves kill-mode
// Shutdown tears down a team child mid-member and leaves the killed end state
// callers observe.
func TestSupervisorPublicAPITeamShutdownKillEndsActiveChild(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	solo := newItestProvider(itestAttempt("ses_team_sd",
		itestWorking("ses_team_sd"), gate.step(),
		itestSessionEnd("ses_team_sd", "end_turn", "never reached")))
	h := newItestHarness(t, Config{}, itestTeamProviders(map[string]*itestProvider{"solo-agent": solo}))

	file := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{{"name": "solo", "prompt": "solo work", "agent": "solo-agent"}},
	})
	rt := h.spawnWorkflow("team", file, map[string]any{"label": "team-shutdown"})
	h.log.waitFor(t, "working status", itestEventIs("agent.status", rt))
	if v := h.view(rt); v.Status != "running" || v.Phase != "working" {
		t.Fatalf("view before shutdown = %+v, want running in the working phase", v)
	}
	h.shutdown("kill")
	h.awaitStatus(rt, "killed")
	if v := h.view(rt); v.StopReason != "cancelled" {
		t.Fatalf("killed view stop reason = %q, want cancelled", v.StopReason)
	}
	if code := h.rawExitCode(rt); code != 130 {
		t.Fatalf("raw RuntimeStatus exit_code = %d, want 130", code)
	}
}

// TestSupervisorPublicAPILoopAndTeamListRows proves List rows for loop and
// team children carry the statuses callers see: running while a phase or
// member is in flight, ended with the run's exit code once the child
// completes.
func TestSupervisorPublicAPILoopAndTeamListRows(t *testing.T) {
	t.Parallel()
	loopGate := newItestGate()
	defer loopGate.open()
	loop := newItestProvider(itestAttempt("ses_list_loop",
		itestWorking("ses_list_loop"), loopGate.step(),
		itestSessionEnd("ses_list_loop", "end_turn", "loop list done")))
	teamGate := newItestGate()
	defer teamGate.open()
	solo := newItestProvider(itestAttempt("ses_list_team",
		itestWorking("ses_list_team"), teamGate.step(),
		itestSessionEnd("ses_list_team", "end_turn", "team list done")))
	h := newItestHarness(t, Config{}, func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		if opts.Agent == "solo-agent" {
			return solo, nil
		}
		return loop, nil
	})

	loopFile := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
	})
	teamFile := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{{"name": "solo", "prompt": "solo work", "agent": "solo-agent"}},
	})
	loopRT := h.spawnWorkflow("loop", loopFile, map[string]any{"label": "rows-loop"})
	teamRT := h.spawnWorkflow("team", teamFile, map[string]any{"label": "rows-team"})

	// Both children are live mid-attempt.
	h.log.waitFor(t, "loop working status", itestEventIs("agent.status", loopRT))
	h.log.waitFor(t, "team working status", itestEventIs("agent.status", teamRT))
	rows := h.listed("rows-")
	if len(rows) != 2 {
		t.Fatalf("List = %d rows, want 2", len(rows))
	}
	for _, row := range rows {
		if got := row["status"]; got != "running" {
			t.Fatalf("List status before completion = %v, want running", got)
		}
		if got := row["exit_code"]; got != 0 {
			t.Fatalf("List exit_code before completion = %v, want 0", got)
		}
	}

	loopGate.open()
	teamGate.open()
	h.awaitStatus(loopRT, "done")
	h.awaitStatus(teamRT, "done")
	rows = h.listed("rows-")
	if len(rows) != 2 {
		t.Fatalf("List after completion = %d rows, want 2", len(rows))
	}
	byID := map[string]map[string]any{}
	for _, row := range rows {
		id, _ := row["runtime_id"].(string)
		byID[id] = row
	}
	for _, id := range []string{loopRT, teamRT} {
		row, ok := byID[id]
		if !ok {
			t.Fatalf("List after completion lost runtime %s: %v", id, rows)
		}
		if got := row["status"]; got != "ended" {
			t.Fatalf("List status after completion = %v, want ended", got)
		}
		if got := row["exit_code"]; got != 0 {
			t.Fatalf("List exit_code after completion = %v, want 0", got)
		}
	}
}
