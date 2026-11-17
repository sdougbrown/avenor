package stable

// workflow_command_completion_e2e_test.go runs the command completion contract
// over a real supervisor. That means a real auto-dispatched worker attempt, the
// real workflow.Manager, a real enabled controller, and a real check process.
// Only the inference provider is replaced, through the packaged
// stableScriptedProvider seam.
//
// It proves the supervisor-side external-state check meets the contract on exit 0
// and leaves it unmet on a non-zero exit. The node's retry behavior stays
// intact.
// Both outcomes record the same durable inspection evidence: the capped streams,
// their digests, and a run summary naming what ran and how it ended.

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/workflow"
)

// commandCheckScript writes an executable check into dir and returns its
// absolute path.
func commandCheckScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatalf("write check script: %v", err)
	}
	return path
}

// commandContractTemplate is a two-node auto chain whose produce node completes
// from a command check over the attempt's working directory.
func commandContractTemplate(t *testing.T, templateID, executable string, maxAttempts int) []byte {
	t.Helper()
	return mustJSON(t, map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"produce"},
		"nodes": []any{
			map[string]any{
				"id":       "produce",
				"action":   map[string]any{"type": "run", "prompt": "produce the work product"},
				"dispatch": map[string]any{"mode": "auto", "controller_id": "c1"},
				"completion": map[string]any{"kind": "command", "command": map[string]any{
					"argv":            []any{map[string]any{"type": "literal", "value": executable}},
					"timeout_seconds": 10,
				}},
				"outputs": []any{map[string]any{
					"id": "state", "name": "State", "type": "string", "required": true,
					"source": map[string]any{"command": "stdout", "pointer": "/state"},
				}},
				"branches":     map[string]any{"done": "consume"},
				"retry_policy": map[string]any{"max_attempts": maxAttempts, "exhaustion": "block"},
			},
			map[string]any{
				"id":           "consume",
				"dependencies": []string{"produce"},
				"action":       map[string]any{"type": "run", "prompt": "consume"},
				"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
				"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
			},
		},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 900},
	})
}

// produceCheckEvidence returns the recorded evidence for one activation with
// the given stored-path suffix, failing the test when it is missing.
func produceCheckEvidence(t *testing.T, inst *workflow.WorkflowInstance, activationID workflow.ActivationID, suffix string) *workflow.Evidence {
	t.Helper()
	for i := range inst.Evidence {
		if inst.Evidence[i].ActivationID == activationID && strings.HasSuffix(inst.Evidence[i].StoredPath, suffix) {
			return &inst.Evidence[i]
		}
	}
	t.Fatalf("no %q evidence for activation %s; recorded evidence %+v", suffix, activationID, inst.Evidence)
	return nil
}

// produceCheckEvidenceForAttempt returns the evidence one exact attempt
// recorded, proving the evidence set distinguishes attempts of one activation.
func produceCheckEvidenceForAttempt(t *testing.T, inst *workflow.WorkflowInstance, activationID workflow.ActivationID, attemptID workflow.AttemptID, suffix string) *workflow.Evidence {
	t.Helper()
	for i := range inst.Evidence {
		e := &inst.Evidence[i]
		if e.ActivationID == activationID && e.AttemptID == attemptID && strings.HasSuffix(e.StoredPath, suffix) {
			return e
		}
	}
	t.Fatalf("no %q evidence for activation %s attempt %s; recorded evidence %+v", suffix, activationID, attemptID, inst.Evidence)
	return nil
}

// decodeCheckSummary decodes the run summary recorded on a check's stdout
// evidence entry.
func decodeCheckSummary(t *testing.T, evidence *workflow.Evidence) checkSummaryWire {
	t.Helper()
	var summary checkSummaryWire
	if len(evidence.Result) == 0 {
		t.Fatalf("evidence %s records no run summary", evidence.StoredPath)
	}
	if err := json.Unmarshal(evidence.Result, &summary); err != nil {
		t.Fatalf("evidence %s summary %s is not JSON: %v", evidence.StoredPath, evidence.Result, err)
	}
	return summary
}

// checkSummaryWire is the run summary shape as an operator reads it back from
// workflow inspect.
type checkSummaryWire struct {
	Argv         []string `json:"argv"`
	Termination  string   `json:"termination"`
	ExitCode     *int     `json:"exit_code"`
	DurationMS   int64    `json:"duration_ms"`
	StdoutBytes  int64    `json:"stdout_bytes"`
	StdoutSHA256 string   `json:"stdout_sha256"`
	StderrBytes  int64    `json:"stderr_bytes"`
	StderrSHA256 string   `json:"stderr_sha256"`
	Diagnostic   string   `json:"diagnostic"`
}

// summarySHA reports the stdout digest the summary records.
func summarySHA(t *testing.T, evidence *workflow.Evidence) string {
	t.Helper()
	return decodeCheckSummary(t, evidence).StdoutSHA256
}

// readStagedEvidence reads one evidence entry's immutable copy from the store.
func readStagedEvidence(t *testing.T, root string, wf string, evidence *workflow.Evidence) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "instances", wf, evidence.StoredPath))
	if err != nil {
		t.Fatalf("read staged evidence %s: %v", evidence.StoredPath, err)
	}
	return data
}

// TestAutoHandoffCommandContractCompletes proves a clean worker exit whose
// declared check exits 0 satisfies the node. The check's stdout is staged as
// evidence with its digest, its pointer output is recorded, and the dependent
// node dispatches.
func TestAutoHandoffCommandContractCompletes(t *testing.T) {
	workDir := t.TempDir()
	t.Chdir(workDir)
	// The check observes external state: the marker file the work produced.
	executable := commandCheckScript(t, workDir, "check.sh",
		`[ -f marker.txt ] || { printf 'marker.txt is missing\n' >&2; exit 4; }
printf '{"state":"ready"}\n'`)

	provider := &stableScriptedProvider{attempt: -1}
	produceWorkerDeclaredResult(t, provider, "ses_produce", "marker.txt", "ready")
	appendSessionScript(t, provider, "ses_consume", nil, "end_turn")

	f := newAutoHandoffFixture(t, "auto-handoff-command", provider)
	wf := f.addWorkflow(t, "tmpl-command-completes", commandContractTemplate(t, "tmpl-command-completes", executable, 2))
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the workflow to complete", func(inst *workflow.WorkflowInstance) bool {
		return inst.Status == workflow.WorkflowCompleted
	})
	inst := f.instance(t, wf)
	produce := activationFor(&inst, "produce")
	if produce == nil || produce.Status != workflow.ActivationSatisfied {
		t.Fatalf("produce not satisfied by its completion command; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}

	var state *workflow.OutputValue
	for i := range inst.Outputs {
		if inst.Outputs[i].DefinitionID == "state" {
			state = &inst.Outputs[i]
			break
		}
	}
	if state == nil {
		t.Fatalf("state output not recorded; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}
	var value string
	if err := json.Unmarshal(state.Value, &value); err != nil {
		t.Fatalf("state output %s is not a JSON string: %v", state.Value, err)
	}
	if value != "ready" {
		t.Fatalf("state output = %q, want the value the check's stdout named", value)
	}

	// The check's capped stdout is durable, inspectable evidence: the exact
	// bytes, their size, their digest, the attempt it belongs to, and the run's
	// provenance.
	attempts := attemptsForNode(&inst, "produce")
	if len(attempts) != 1 {
		t.Fatalf("produce recorded %d attempts, want one; observed %s", len(attempts), describeInstance(&inst, f.providerCalls.Load()))
	}
	evidence := produceCheckEvidence(t, &inst, produce.ID, "completion-command.stdout")
	if evidence.AttemptID != attempts[0].ID {
		t.Fatalf("evidence attempt = %s, want the succeeded attempt %s", evidence.AttemptID, attempts[0].ID)
	}
	if evidence.SHA256 != summarySHA(t, evidence) {
		t.Fatalf("stdout evidence digest %s does not match the bytes it staged", evidence.SHA256)
	}
	summary := decodeCheckSummary(t, evidence)
	if len(summary.Argv) != 1 || summary.Argv[0] != executable {
		t.Fatalf("check summary argv = %v, want the declared executable", summary.Argv)
	}
	if summary.Termination != "exited" || summary.ExitCode == nil || *summary.ExitCode != 0 {
		t.Fatalf("check summary = %+v, want a clean exit", summary)
	}
	if summary.StdoutSHA256 != evidence.SHA256 || summary.StdoutBytes != evidence.Size {
		t.Fatalf("check summary stdout = %d bytes/%s, want the staged stream", summary.StdoutBytes, summary.StdoutSHA256)
	}
	if summary.Diagnostic != "" {
		t.Fatalf("check summary diagnostic = %q, want none for a met contract", summary.Diagnostic)
	}
	stored := readStagedEvidence(t, f.mgr.Store().Root(), wf, evidence)
	if string(stored) != "{\"state\":\"ready\"}\n" {
		t.Fatalf("staged stdout evidence = %q, want the check's exact bytes", stored)
	}
}

// TestAutoHandoffCommandContractUnmetRetriesThenExhausts proves a check that exits
// non-zero leaves the contract unmet. The attempt is failed with the
// contract_unmet marker. The check's captured streams and summary are recorded
// with that failed attempt, with no output and no completion. The node's retry
// policy runs its remaining attempts, and exhaustion leaves the activation blocked
// with the dependent never dispatched.
func TestAutoHandoffCommandContractUnmetRetriesThenExhausts(t *testing.T) {
	workDir := t.TempDir()
	t.Chdir(workDir)
	executable := commandCheckScript(t, workDir, "check.sh",
		`printf 'the external state is not ready\n' >&2; exit 9`)

	provider := &stableScriptedProvider{attempt: -1}
	appendSessionScript(t, provider, "ses_try1", nil, "end_turn")
	appendSessionScript(t, provider, "ses_try2", nil, "end_turn")

	f := newAutoHandoffFixture(t, "auto-handoff-command-unmet", provider)
	wf := f.addWorkflow(t, "tmpl-command-unmet", commandContractTemplate(t, "tmpl-command-unmet", executable, 2))
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the produce node to exhaust its retries and block", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "produce")
		return act != nil && act.Status == workflow.ActivationBlocked
	})
	inst := f.instance(t, wf)
	if calls := f.providerCalls.Load(); calls != 2 {
		t.Fatalf("provider invoked %d times, want exactly 2 (max_attempts); observed %s", calls, describeInstance(&inst, calls))
	}
	attempts := attemptsForNode(&inst, "produce")
	if len(attempts) != 2 {
		t.Fatalf("produce recorded %d attempts, want exactly 2; observed %s", len(attempts), describeInstance(&inst, f.providerCalls.Load()))
	}
	for _, attempt := range attempts {
		if attempt.Status != workflow.AttemptFailed || attempt.MarkerLabel != "contract_unmet" {
			t.Fatalf("produce attempt %s = %s/%s, want failed/contract_unmet; observed %s",
				attempt.ID, attempt.Status, attempt.MarkerLabel, describeInstance(&inst, f.providerCalls.Load()))
		}
	}
	if len(inst.Outputs) != 0 {
		t.Fatalf("a failed check recorded outputs %+v", inst.Outputs)
	}
	// Every failed attempt keeps its own check's evidence: the exact bounded
	// streams plus one summary naming what ran and how it ended. Evidence from
	// the first retry is never mistaken for the second's.
	seenEvidence := make(map[workflow.EvidenceID]string, len(attempts))
	seenStored := make(map[string]string, len(attempts))
	for _, attempt := range attempts {
		stdout := produceCheckEvidenceForAttempt(t, &inst, attempt.Identity.ActivationID, attempt.ID, "completion-command.stdout")
		if previous, duplicate := seenEvidence[stdout.ID]; duplicate {
			t.Fatalf("attempts %s and %s share evidence %s", previous, attempt.ID, stdout.ID)
		}
		seenEvidence[stdout.ID] = string(attempt.ID)
		if previous, duplicate := seenStored[stdout.StoredPath]; duplicate {
			t.Fatalf("attempts %s and %s share stored path %s", previous, attempt.ID, stdout.StoredPath)
		}
		seenStored[stdout.StoredPath] = string(attempt.ID)
		summary := decodeCheckSummary(t, stdout)
		if summary.Termination != "exited" || summary.ExitCode == nil || *summary.ExitCode != 9 {
			t.Fatalf("check summary for attempt %s = %+v, want exit 9", attempt.ID, summary)
		}
		if !strings.Contains(summary.Diagnostic, "the external state is not ready") {
			t.Fatalf("check summary for attempt %s = %q, want the check's report", attempt.ID, summary.Diagnostic)
		}
		if stored := readStagedEvidence(t, f.mgr.Store().Root(), wf, stdout); len(stored) != 0 {
			t.Fatalf("a failing check staged stdout %q, want an empty stream", stored)
		}
		stderr := produceCheckEvidenceForAttempt(t, &inst, attempt.Identity.ActivationID, attempt.ID, "completion-command.stderr")
		if stored := readStagedEvidence(t, f.mgr.Store().Root(), wf, stderr); !strings.Contains(string(stored), "the external state is not ready") {
			t.Fatalf("staged stderr = %q, want the check's report", stored)
		}
	}
	if act := activationFor(&inst, "consume"); act != nil {
		t.Fatalf("consume dispatched despite produce never meeting its check; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}
}

// TestAutoHandoffCommandOutputStaysOutOfTheLog proves a check's output is
// persisted as evidence and never quoted into the supervisor's log. The rejection
// line names the exit status, while the captured secret appears only in the
// recorded evidence.
func TestAutoHandoffCommandOutputStaysOutOfTheLog(t *testing.T) {
	workDir := t.TempDir()
	t.Chdir(workDir)
	executable := commandCheckScript(t, workDir, "check.sh",
		`printf 'checked with token=s3cr3t-check-token\n' >&2; exit 4`)

	provider := &stableScriptedProvider{attempt: -1}
	appendSessionScript(t, provider, "ses_try1", nil, "end_turn")
	var log bytes.Buffer
	f := newAutoHandoffFixture(t, "auto-handoff-command-log", provider, func(f *autoHandoffFixture) {
		f.sup.errWriter = &log
	})
	wf := f.addWorkflow(t, "tmpl-command-log", commandContractTemplate(t, "tmpl-command-log", executable, 1))
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the produce node to exhaust its check budget and block", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "produce")
		return act != nil && act.Status == workflow.ActivationBlocked
	})
	inst := f.instance(t, wf)
	attempt := attemptsForNode(&inst, "produce")[0]

	if !strings.Contains(log.String(), "exited with status 4") {
		t.Fatalf("supervisor log = %q, want the rejection's exit status", log.String())
	}
	if strings.Contains(log.String(), "s3cr3t-check-token") {
		t.Fatalf("supervisor log quotes the check's output: %q", log.String())
	}
	stdout := produceCheckEvidenceForAttempt(t, &inst, attempt.Identity.ActivationID, attempt.ID, "completion-command.stdout")
	summary := decodeCheckSummary(t, stdout)
	if !strings.Contains(summary.Diagnostic, "s3cr3t-check-token") {
		t.Fatalf("evidence summary = %q, want the check's report persisted", summary.Diagnostic)
	}
	stderr := produceCheckEvidenceForAttempt(t, &inst, attempt.Identity.ActivationID, attempt.ID, "completion-command.stderr")
	if stored := readStagedEvidence(t, f.mgr.Store().Root(), wf, stderr); !strings.Contains(string(stored), "s3cr3t-check-token") {
		t.Fatalf("staged stderr = %q, want the check's exact output", stored)
	}
}

// TestAutoHandoffCommandCheckCanceledMidRun proves an attempt's cancellation
// reaches the running check. The worker has already exited successfully and the
// check is mid-run when a real workflow.cancel lands. The cancel path is then the
// only thing that can end it.
func TestAutoHandoffCommandCheckCanceledMidRun(t *testing.T) {
	workDir := t.TempDir()
	t.Chdir(workDir)
	pidFile := filepath.Join(workDir, "check.pid")
	executable := commandCheckScript(t, workDir, "check.sh",
		`echo $$ > check.pid; printf 'checking\n'; sleep 60`)

	provider := &stableScriptedProvider{attempt: -1}
	produceWorkerDeclaredResult(t, provider, "ses_produce", "marker.txt", "ready")
	f := newAutoHandoffFixture(t, "auto-handoff-command-cancel", provider)
	wf := f.addWorkflow(t, "tmpl-command-cancel", commandContractTemplate(t, "tmpl-command-cancel", executable, 120))
	f.enableController(t, 2)

	// The worker exited and the check is running: wait for the check's own pid.
	waitFor(t, "the completion check to start", func() bool {
		data, err := os.ReadFile(pidFile)
		return err == nil && strings.TrimSpace(string(data)) != ""
	})
	checkPID, err := strconv.Atoi(strings.TrimSpace(string(mustReadFile(t, pidFile))))
	if err != nil {
		t.Fatalf("check pid %q: %v", mustReadFile(t, pidFile), err)
	}

	// Cancel through the real control RPC, exactly as production does.
	socket := f.sup.config.ControlSocket
	if err := f.sup.control.Start(socket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(func() { f.sup.control.Stop() })
	cl, err := client.Dial(socket)
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer cl.Close()
	var cancelResult map[string]any
	if err := cl.Call("workflow.cancel", map[string]any{
		"workflow_id": wf, "actor": "test", "reason": "cancel while the check runs",
	}, &cancelResult); err != nil {
		t.Fatalf("workflow.cancel: %v", err)
	}
	if n, _ := cancelResult["canceled_runtimes"].(float64); n != 1 {
		t.Fatalf("workflow.cancel result = %#v, want canceled_runtimes=1 (the attempt runtime still owns the check)", cancelResult)
	}

	// The cancellation killed the check's process group: nothing of the check
	// outlives the attempt.
	waitFor(t, "the check's process group to be killed by the cancel", func() bool {
		return syscall.Kill(checkPID, 0) == syscall.ESRCH
	})
	if checkPID <= 1 {
		t.Fatalf("check recorded an implausible pid %d", checkPID)
	}

	f.waitForInstance(t, wf, "the workflow to be canceled with a canceled attempt", func(inst *workflow.WorkflowInstance) bool {
		if inst.Status != workflow.WorkflowCanceled {
			return false
		}
		attempts := attemptsForNode(inst, "produce")
		return len(attempts) == 1 && attempts[0].Status == workflow.AttemptCanceled
	})
	inst := f.instance(t, wf)
	attempt := attemptsForNode(&inst, "produce")[0]
	if attempt.MarkerLabel != "canceled" {
		t.Fatalf("attempt marker = %q, want the cancellation recorded", attempt.MarkerLabel)
	}
	// The check's own evidence records that it was canceled, not judged.
	stdout := produceCheckEvidenceForAttempt(t, &inst, attempt.Identity.ActivationID, attempt.ID, "completion-command.stdout")
	if summary := decodeCheckSummary(t, stdout); summary.Termination != "canceled" {
		t.Fatalf("check summary = %+v, want the cancellation recorded as its termination", summary)
	}
	if stored := readStagedEvidence(t, f.mgr.Store().Root(), wf, stdout); string(stored) != "checking\n" {
		t.Fatalf("staged stdout = %q, want the output the check produced before the cancel", stored)
	}
	// Cancellation is not a retry: the provider ran exactly once.
	if calls := f.providerCalls.Load(); calls != 1 {
		t.Fatalf("provider invoked %d times, want exactly 1 (a cancel never retries); observed %s", calls, describeInstance(&inst, calls))
	}
}

// TestAutoHandoffCommandDurableCompletionIsNotRedispatched proves the durable
// branch at the supervisor level. The completion's events reach the log but its
// snapshot replace fails. The completion is then still committed. The workflow
// completes, its evidence survives with the exact bytes, and the provider is not
// asked for a replacement attempt. The diagnostic says so instead of promising a
// re-dispatch.
func TestAutoHandoffCommandDurableCompletionIsNotRedispatched(t *testing.T) {
	workDir := t.TempDir()
	t.Chdir(workDir)
	executable := commandCheckScript(t, workDir, "check.sh", `printf '{"state":"ready"}\n'`)

	provider := &stableScriptedProvider{attempt: -1}
	produceWorkerDeclaredResult(t, provider, "ses_produce", "marker.txt", "ready")
	appendSessionScript(t, provider, "ses_consume", nil, "end_turn")
	var log bytes.Buffer
	f := newAutoHandoffFixture(t, "auto-handoff-command-durable", provider, func(f *autoHandoffFixture) {
		f.sup.errWriter = &log
	})
	wf := f.addWorkflow(t, "tmpl-command-durable", commandContractTemplate(t, "tmpl-command-durable", executable, 2))

	// Fail the snapshot replace of the first write that carries evidence, which is
	// the completion. The attempt's success fact carries no evidence, and every
	// later command materializes normally.
	var once sync.Once
	f.mgr.Store().SetSnapshotWriteFault(func(_ workflow.WorkflowID, snap workflow.Snapshot) error {
		if len(snap.Instance.Evidence) == 0 {
			return nil
		}
		fail := false
		once.Do(func() { fail = true })
		if fail {
			return errors.New("injected snapshot replace failure")
		}
		return nil
	})
	t.Cleanup(func() { f.mgr.Store().SetSnapshotWriteFault(nil) })

	f.enableController(t, 2)
	f.waitForInstance(t, wf, "the workflow to complete from the durable completion", func(inst *workflow.WorkflowInstance) bool {
		return inst.Status == workflow.WorkflowCompleted
	})

	inst := f.instance(t, wf)
	if calls := f.providerCalls.Load(); calls != 2 {
		t.Fatalf("provider invoked %d times, want exactly 2 (produce + consume, no replacement attempt); observed %s",
			calls, describeInstance(&inst, calls))
	}
	if attempts := attemptsForNode(&inst, "produce"); len(attempts) != 1 {
		t.Fatalf("produce recorded %d attempts, want one (the durable completion needs no retry); observed %s",
			len(attempts), describeInstance(&inst, f.providerCalls.Load()))
	}
	if produce := activationFor(&inst, "produce"); produce == nil || produce.Status != workflow.ActivationSatisfied {
		t.Fatalf("produce activation = %+v, want satisfied by the committed completion", produce)
	}
	if !strings.Contains(log.String(), "recorded durably") {
		t.Fatalf("supervisor log = %q, want the durable-completion diagnostic", log.String())
	}
	if strings.Contains(log.String(), "re-dispatch") {
		t.Fatalf("supervisor log = %q, want no re-dispatch promise for a committed completion", log.String())
	}
	// The evidence the durable completion references is on disk, byte for byte.
	produce := activationFor(&inst, "produce")
	evidence := produceCheckEvidence(t, &inst, produce.ID, "completion-command.stdout")
	if stored := readStagedEvidence(t, f.mgr.Store().Root(), wf, evidence); string(stored) != "{\"state\":\"ready\"}\n" {
		t.Fatalf("staged stdout = %q, want the captured bytes", stored)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}
