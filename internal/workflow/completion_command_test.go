package workflow

// completion_command_test.go covers the command completion contract: the
// supervisor-side external-state check that runs after a worker exits
// successfully. Its argv is a declared template of literal and instance-param
// elements. It runs with no shell and an empty environment in the attempt's
// working directory. Exit 0 meets the contract. Every other outcome leaves it
// unmet with bounded diagnostics, while the capped stdout is staged as durable
// inspection evidence.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// commandScript writes an executable check script into dir and returns its
// absolute path.
func commandScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// commandNode builds one auto run node declaring a command completion
// contract over path with the given literal arguments.
func commandNode(path string, args []string, timeoutSeconds int64, outputs []OutputDefinition) *NodeDefinition {
	argv := []ArgvElement{{Type: ArgvElementKindLiteral, Value: path}}
	for _, arg := range args {
		argv = append(argv, ArgvElement{Type: ArgvElementKindLiteral, Value: arg})
	}
	return &NodeDefinition{
		ID:         "produce",
		Action:     Action{Kind: ActionRun, Run: &RunAction{Prompt: "work"}},
		Dispatch:   &DispatchPolicy{Mode: DispatchAuto, ControllerID: "c1"},
		Outputs:    outputs,
		Completion: &CompletionContract{Kind: CompletionCommand, Command: &CommandRequirement{Argv: argv, TimeoutSeconds: timeoutSeconds}},
		Branches:   map[OutcomeName]NodeID{"done": "next"},
	}
}

// checkSummary decodes the run summary a capture carries, failing the test
// when the capture records none.
func checkSummary(t *testing.T, capture CommandCapture) completionCommandSummary {
	t.Helper()
	if len(capture.Result) == 0 {
		t.Fatalf("capture %q records no run summary", capture.StoredPath)
	}
	var summary completionCommandSummary
	if err := json.Unmarshal(capture.Result, &summary); err != nil {
		t.Fatalf("capture %q summary %s is not JSON: %v", capture.StoredPath, capture.Result, err)
	}
	return summary
}

// captureNamed returns the capture with the given stored path.
func captureNamed(captures []CommandCapture, storedPath string) *CommandCapture {
	for i := range captures {
		if captures[i].StoredPath == storedPath {
			return &captures[i]
		}
	}
	return nil
}

// contractUnmet asserts that err is a contract rejection and returns it.
func contractUnmet(t *testing.T, err error) *ContractUnmetError {
	t.Helper()
	if err == nil {
		t.Fatal("the contract was met, want a rejection")
	}
	var unmet *ContractUnmetError
	if !errors.As(err, &unmet) {
		t.Fatalf("error = %v (%T), want *ContractUnmetError", err, err)
	}
	return unmet
}

func TestEvaluateAutoCompletionCommandCapturesStdoutEvidence(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `printf '{"state":"open"}\n'`)
	node := commandNode(path, nil, 10, nil)
	plan, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("evaluateAuto: %v", err)
	}
	if len(plan.Artifacts) != 0 {
		t.Fatalf("plan artifacts = %+v, want the captures to stay in memory", plan.Artifacts)
	}
	if len(plan.Captures) != 1 {
		t.Fatalf("captures = %+v, want exactly the captured stdout", plan.Captures)
	}
	stdout := plan.Captures[0]
	if stdout.StoredPath != completionCommandStdoutStoredPath {
		t.Fatalf("stored path = %q, want %q", stdout.StoredPath, completionCommandStdoutStoredPath)
	}
	if got := string(stdout.Data); got != "{\"state\":\"open\"}\n" {
		t.Fatalf("captured stdout = %q, want the check's exact bytes", got)
	}
	summary := checkSummary(t, stdout)
	if len(summary.Argv) != 1 || summary.Argv[0] != path {
		t.Fatalf("summary argv = %v, want the resolved argv", summary.Argv)
	}
	if summary.Termination != completionCommandExited {
		t.Fatalf("summary termination = %q, want %q", summary.Termination, completionCommandExited)
	}
	if summary.ExitCode == nil || *summary.ExitCode != 0 {
		t.Fatalf("summary exit code = %v, want 0", summary.ExitCode)
	}
	if summary.StdoutBytes != len(stdout.Data) || summary.StdoutSHA256 != sha256Hex(string(stdout.Data)) {
		t.Fatalf("summary stdout = %d bytes/%s, want the captured stream's size and digest", summary.StdoutBytes, summary.StdoutSHA256)
	}
	if summary.StderrBytes != 0 {
		t.Fatalf("summary stderr bytes = %d, want 0", summary.StderrBytes)
	}
	if summary.Diagnostic != "" {
		t.Fatalf("summary diagnostic = %q, want none for a met contract", summary.Diagnostic)
	}
}

// TestEvaluateAutoCompletionCommandCapturesStderrWhenWritten proves a check
// that explains itself on stderr has those bytes captured too, on a passing run
// as well as a failing one.
func TestEvaluateAutoCompletionCommandCapturesStderrWhenWritten(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `printf 'checked the branch\n' >&2; printf '{"state":"ready"}\n'`)
	node := commandNode(path, nil, 10, nil)
	plan, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("evaluateAuto: %v", err)
	}
	stderr := captureNamed(plan.Captures, completionCommandStderrStoredPath)
	if stderr == nil {
		t.Fatalf("captures = %+v, want the captured stderr", plan.Captures)
	}
	if got := string(stderr.Data); got != "checked the branch\n" {
		t.Fatalf("captured stderr = %q, want the check's exact bytes", got)
	}
	summary := checkSummary(t, *captureNamed(plan.Captures, completionCommandStdoutStoredPath))
	if summary.StderrBytes != len(stderr.Data) || summary.StderrSHA256 != sha256Hex(string(stderr.Data)) {
		t.Fatalf("summary stderr = %d bytes/%s, want the captured stream's size and digest", summary.StderrBytes, summary.StderrSHA256)
	}
}

func TestEvaluateAutoCompletionCommandPointerResolvesOutput(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `printf '{"checks":[{"state":"green"}],"n":3}\n'`)
	node := commandNode(path, nil, 10, []OutputDefinition{
		stringOutput("state", true, &OutputSource{Command: "stdout", Pointer: "/checks/0/state"}),
		{ID: "count", Name: "count", Type: OutputNumber, Required: true,
			Source: &OutputSource{Command: "stdout", Pointer: "/n"}},
	})
	plan, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("evaluateAuto: %v", err)
	}
	values := make(map[OutputID]string, len(plan.Outputs))
	for _, output := range plan.Outputs {
		values[output.DefinitionID] = string(output.Value)
	}
	if values["state"] != `"green"` || values["count"] != "3" {
		t.Fatalf("outputs = %+v, want the pointers resolved from the check's stdout", values)
	}
}

// TestEvaluateAutoCompletionCommandInstanceParamIsOneArgvElement proves an
// instance-param element reaches the check as exactly one argv element: no
// splitting on whitespace, no shell interpretation of metacharacters.
func TestEvaluateAutoCompletionCommandInstanceParamIsOneArgvElement(t *testing.T) {
	dir := t.TempDir()
	record := filepath.Join(dir, "argv.txt")
	path := commandScript(t, dir, "record.sh", "printf '%s\\n' \"$#\" \"$@\" > "+record)
	node := commandNode(path, nil, 10, nil)
	node.Completion.Command.Argv = append(node.Completion.Command.Argv,
		ArgvElement{Type: ArgvElementKindInstanceParam, FromInstanceParam: "target"})
	hostile := `two words; touch pwned $(id) > out`
	plan, err := EvaluateAutoCompletion(context.Background(), AutoCompletionInput{
		Template:   evalTemplate(node, "done"),
		Node:       node,
		Params:     map[string]string{"target": hostile},
		WorkingDir: dir,
	})
	if err != nil {
		t.Fatalf("EvaluateAutoCompletion: %v", err)
	}
	argv, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("read recorded argv: %v", err)
	}
	want := "1\n" + hostile + "\n"
	if string(argv) != want {
		t.Fatalf("recorded argv = %q, want %q", argv, want)
	}
	if _, err := os.Stat(filepath.Join(dir, "pwned")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the check ran a shell command from an argv element: %v", err)
	}
	summary := checkSummary(t, plan.Captures[0])
	if len(summary.Argv) != 2 || summary.Argv[1] != hostile {
		t.Fatalf("summary argv = %v, want the instance param as one element", summary.Argv)
	}
}

// TestEvaluateAutoCompletionCommandRejectionCarriesEvidence proves a non-zero
// check leaves the contract unmet and still records what it printed. The evidence
// holds the bounded streams, the exit code, the duration, and the stdout digest.
func TestEvaluateAutoCompletionCommandRejectionCarriesEvidence(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `printf 'looked\n'; printf 'remote branch missing\n' >&2; exit 3`)
	node := commandNode(path, nil, 10, nil)
	plan, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if plan != nil {
		t.Fatalf("plan = %+v, want no plan", plan)
	}
	unmet := contractUnmet(t, err)
	if !strings.Contains(unmet.Reason, "exited with status 3") {
		t.Fatalf("rejection = %v, want it to name the exit status", unmet.Reason)
	}
	// A check's output can hold sensitive data, so it is evidence, not log text.
	if strings.Contains(unmet.Reason, "remote branch missing") || strings.Contains(unmet.Reason, "looked") {
		t.Fatalf("rejection = %v, want no check output in the logged reason", unmet.Reason)
	}
	stdout := captureNamed(unmet.Evidence, completionCommandStdoutStoredPath)
	if stdout == nil {
		t.Fatalf("evidence = %+v, want the captured stdout", unmet.Evidence)
	}
	if got := string(stdout.Data); got != "looked\n" {
		t.Fatalf("captured stdout = %q, want the check's exact bytes", got)
	}
	stderr := captureNamed(unmet.Evidence, completionCommandStderrStoredPath)
	if stderr == nil || string(stderr.Data) != "remote branch missing\n" {
		t.Fatalf("evidence = %+v, want the captured stderr", unmet.Evidence)
	}
	summary := checkSummary(t, *stdout)
	if summary.Termination != completionCommandExited || summary.ExitCode == nil || *summary.ExitCode != 3 {
		t.Fatalf("summary = %+v, want an exit status 3", summary)
	}
	if summary.StdoutSHA256 != sha256Hex(string(stdout.Data)) {
		t.Fatalf("summary stdout digest = %s, want the captured bytes' digest", summary.StdoutSHA256)
	}
	if !strings.Contains(summary.Diagnostic, "remote branch missing") {
		t.Fatalf("summary diagnostic = %q, want the check's report", summary.Diagnostic)
	}
}

// TestEvaluateAutoCompletionCommandStdoutExplainsASilentFailure proves a check
// that fails without writing stderr still yields a diagnostic and a captured
// stdout that explains the rejection.
func TestEvaluateAutoCompletionCommandStdoutExplainsASilentFailure(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `printf 'branch has no commits\n'; exit 2`)
	node := commandNode(path, nil, 10, nil)
	_, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	unmet := contractUnmet(t, err)
	if !strings.Contains(unmet.Reason, "exited with status 2") || strings.Contains(unmet.Reason, "branch has no commits") {
		t.Fatalf("rejection = %v, want the exit status without the check's output", unmet.Reason)
	}
	summary := checkSummary(t, *captureNamed(unmet.Evidence, completionCommandStdoutStoredPath))
	if !strings.Contains(summary.Diagnostic, "branch has no commits") {
		t.Fatalf("summary diagnostic = %q, want the check's report persisted as evidence", summary.Diagnostic)
	}
}

// TestEvaluateAutoCompletionCommandDiagnosticIsBounded proves a chatty failed
// check cannot grow the rejection or its recorded summary without bound.
func TestEvaluateAutoCompletionCommandDiagnosticIsBounded(t *testing.T) {
	dir := t.TempDir()
	// ~9 KiB of stderr stays under the capture cap, so the diagnostic bound is
	// what trims it.
	path := commandScript(t, dir, "check.sh",
		`i=0; while [ "$i" -lt 400 ]; do printf 'noisy diagnostic line\n' >&2; i=$((i+1)); done; exit 1`)
	node := commandNode(path, nil, 20, nil)
	_, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	unmet := contractUnmet(t, err)
	if len(unmet.Reason) > 2*completionCommandDiagnosticBytes {
		t.Fatalf("rejection is %d bytes, want it bounded near %d", len(unmet.Reason), completionCommandDiagnosticBytes)
	}
	if strings.Contains(unmet.Reason, "noisy diagnostic line") {
		t.Fatalf("rejection = %v, want no check output in the logged reason", unmet.Reason)
	}
	summary := checkSummary(t, *captureNamed(unmet.Evidence, completionCommandStdoutStoredPath))
	if len(summary.Diagnostic) > completionCommandDiagnosticBytes+len("…") {
		t.Fatalf("summary diagnostic is %d bytes, want it bounded near %d", len(summary.Diagnostic), completionCommandDiagnosticBytes)
	}
	if summary.StderrBytes <= completionCommandDiagnosticBytes {
		t.Fatalf("summary stderr bytes = %d, want the whole captured stream recorded", summary.StderrBytes)
	}
}

func TestEvaluateAutoCompletionCommandStdoutLimitKillsTheCheck(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `echo $$ > shell.pid; exec yes`)
	node := commandNode(path, nil, 30, nil)
	start := time.Now()
	_, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	unmet := contractUnmet(t, err)
	if !strings.Contains(unmet.Reason, "stdout exceeded") {
		t.Fatalf("rejection = %v, want a bounded-stdout rejection", unmet.Reason)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("evaluation returned after %v, want the check killed at the limit", elapsed)
	}
	summary := checkSummary(t, *captureNamed(unmet.Evidence, completionCommandStdoutStoredPath))
	if summary.Termination != completionCommandTooLarge {
		t.Fatalf("summary termination = %q, want %q", summary.Termination, completionCommandTooLarge)
	}
	assertCheckProcessGone(t, dir)
}

// TestEvaluateAutoCompletionCommandTimeoutKillsTheCheck proves a hung check is
// killed at its deadline and still contributes the output it produced first. The
// deadline is fired through the caller's timeout channel once the check is
// running. The kill is then observed against a live child rather than racing a
// wall-clock timer against process startup.
func TestEvaluateAutoCompletionCommandTimeoutKillsTheCheck(t *testing.T) {
	dir := t.TempDir()
	// The stdout marker precedes the pid file the test waits on, so firing the
	// deadline can never precede the output it asserts.
	path := commandScript(t, dir, "check.sh", `printf 'waiting\n'; echo $$ > shell.pid; sleep 30`)
	node := commandNode(path, nil, 30, nil)
	timeoutC := make(chan time.Time, 1)
	done := make(chan error, 1)
	go func() {
		_, err := EvaluateAutoCompletion(context.Background(), AutoCompletionInput{
			Template:   evalTemplate(node, "done"),
			Node:       node,
			WorkingDir: dir,
			TimeoutC:   timeoutC,
		})
		done <- err
	}()
	waitForCheckStart(t, filepath.Join(dir, "shell.pid"), done)
	timeoutC <- time.Now()
	unmet := contractUnmet(t, <-done)
	if !strings.Contains(unmet.Reason, "timed out after 30s") {
		t.Fatalf("rejection = %v, want a timeout rejection naming the deadline", unmet.Reason)
	}
	stdout := captureNamed(unmet.Evidence, completionCommandStdoutStoredPath)
	if stdout == nil || string(stdout.Data) != "waiting\n" {
		t.Fatalf("evidence = %+v, want the output the check produced before its deadline", unmet.Evidence)
	}
	summary := checkSummary(t, *stdout)
	if summary.Termination != completionCommandTimedOut {
		t.Fatalf("summary termination = %q, want %q", summary.Termination, completionCommandTimedOut)
	}
	if summary.ExitCode != nil {
		t.Fatalf("summary exit code = %v, want none for a killed check", *summary.ExitCode)
	}
	assertCheckProcessGone(t, dir)
}

func TestEvaluateAutoCompletionCommandCancellationKillsTheCheck(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `echo $$ > shell.pid; sleep 30`)
	node := commandNode(path, nil, 30, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := EvaluateAutoCompletion(ctx, AutoCompletionInput{
			Template:   evalTemplate(node, "done"),
			Node:       node,
			WorkingDir: dir,
		})
		done <- err
	}()
	waitForCheckStart(t, filepath.Join(dir, "shell.pid"), done)
	cancel()
	err := <-done
	if err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("rejection = %v, want a cancellation rejection", err)
	}
	unmet := contractUnmet(t, err)
	summary := checkSummary(t, *captureNamed(unmet.Evidence, completionCommandStdoutStoredPath))
	if summary.Termination != completionCommandCanceled {
		t.Fatalf("summary termination = %q, want %q", summary.Termination, completionCommandCanceled)
	}
	assertCheckProcessGone(t, dir)
}

// TestEvaluateAutoCompletionCommandStartFailureIsContractUnmet proves a check
// that cannot start records what it tried to run instead of failing silently.
func TestEvaluateAutoCompletionCommandStartFailureIsContractUnmet(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "absent-check")
	node := commandNode(missing, nil, 10, nil)
	_, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	unmet := contractUnmet(t, err)
	if !strings.Contains(unmet.Reason, "start") {
		t.Fatalf("rejection = %v, want a start failure", unmet.Reason)
	}
	summary := checkSummary(t, *captureNamed(unmet.Evidence, completionCommandStdoutStoredPath))
	if summary.Termination != completionCommandStartFailed {
		t.Fatalf("summary termination = %q, want %q", summary.Termination, completionCommandStartFailed)
	}
	if len(summary.Argv) != 1 || summary.Argv[0] != missing {
		t.Fatalf("summary argv = %v, want the argv that could not start", summary.Argv)
	}
}

// TestEvaluateAutoCompletionCommandSignalIsNotAnExit proves a check that dies on
// a signal is not reported as having exited. Its summary says `signaled` and
// records no exit code of its own.
func TestEvaluateAutoCompletionCommandSignalIsNotAnExit(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `printf 'about to die\n'; kill -TERM $$`)
	node := commandNode(path, nil, 10, nil)
	_, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	unmet := contractUnmet(t, err)
	if strings.Contains(unmet.Reason, "exited with status") {
		t.Fatalf("rejection = %v, want a signal death named as such", unmet.Reason)
	}
	stdout := captureNamed(unmet.Evidence, completionCommandStdoutStoredPath)
	if stdout == nil || string(stdout.Data) != "about to die\n" {
		t.Fatalf("evidence = %+v, want the output written before the signal", unmet.Evidence)
	}
	summary := checkSummary(t, *stdout)
	if summary.Termination != completionCommandSignaled {
		t.Fatalf("summary termination = %q, want %q", summary.Termination, completionCommandSignaled)
	}
	if summary.ExitCode != nil {
		t.Fatalf("summary exit code = %v, want none for a signal death", *summary.ExitCode)
	}
}

// TestEvaluateAutoCompletionCommandSummaryFitsTheEvidenceBound proves a check
// declared with a very large argv still records evidence. The run summary drops
// the argv rather than failing the run or growing the snapshot without bound.
func TestEvaluateAutoCompletionCommandSummaryFitsTheEvidenceBound(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `printf 'ok\n'`)
	huge := strings.Repeat("x", maxEvidenceResultBytes)
	node := commandNode(path, []string{huge, huge}, 10, nil)
	plan, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("evaluateAuto: %v", err)
	}
	stdout := plan.Captures[0]
	if len(stdout.Result) == 0 || len(stdout.Result) > maxEvidenceResultBytes {
		t.Fatalf("summary is %d bytes, want it within the %d-byte bound", len(stdout.Result), maxEvidenceResultBytes)
	}
	summary := checkSummary(t, stdout)
	if !summary.ArgvDropped || len(summary.Argv) != 0 {
		t.Fatalf("summary argv = %v (dropped %v), want the oversized argv dropped", summary.Argv, summary.ArgvDropped)
	}
	if summary.StdoutSHA256 != sha256Hex(string(stdout.Data)) || summary.Termination != completionCommandExited {
		t.Fatalf("summary = %+v, want the run's stream digest and termination kept", summary)
	}
}

// TestEvaluateAutoCompletionCommandRejectionCarriesEvidenceWhenAnOutputFails
// proves a passing check's capture is not lost when the rejection comes later. An
// output that cannot be resolved from the check's stdout still records what the
// check printed.
func TestEvaluateAutoCompletionCommandRejectionCarriesEvidenceWhenAnOutputFails(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `printf '{"ready":true}\n'`)
	node := commandNode(path, nil, 10, []OutputDefinition{
		stringOutput("missing", true, &OutputSource{Command: "stdout", Pointer: "/absent"}),
	})
	_, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	unmet := contractUnmet(t, err)
	if !strings.Contains(unmet.Reason, `member "absent" not found`) {
		t.Fatalf("rejection = %v, want the unresolved-pointer reason", unmet.Reason)
	}
	stdout := captureNamed(unmet.Evidence, completionCommandStdoutStoredPath)
	if stdout == nil || string(stdout.Data) != "{\"ready\":true}\n" {
		t.Fatalf("evidence = %+v, want the passing check's captured stdout", unmet.Evidence)
	}
	if summary := checkSummary(t, *stdout); summary.Termination != completionCommandExited || summary.ExitCode == nil || *summary.ExitCode != 0 {
		t.Fatalf("summary = %+v, want the check's clean run recorded", summary)
	}
}

// TestEvaluateAutoCompletionCommandRejectionCarriesEvidenceWhenValidationFails
// is the same guarantee for a declared-output type mismatch after the check
// exited 0.
func TestEvaluateAutoCompletionCommandRejectionCarriesEvidenceWhenValidationFails(t *testing.T) {
	dir := t.TempDir()
	path := commandScript(t, dir, "check.sh", `printf '{"state":"green"}\n'`)
	node := commandNode(path, nil, 10, []OutputDefinition{
		{ID: "state", Name: "state", Type: OutputNumber, Required: true,
			Source: &OutputSource{Command: "stdout", Pointer: "/state"}},
	})
	_, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	unmet := contractUnmet(t, err)
	if !strings.Contains(unmet.Reason, "is not a finite number") {
		t.Fatalf("rejection = %v, want the declared-type rejection", unmet.Reason)
	}
	if captureNamed(unmet.Evidence, completionCommandStdoutStoredPath) == nil {
		t.Fatalf("evidence = %+v, want the passing check's capture preserved", unmet.Evidence)
	}
}

// heldStreamsScript exits 0 after writing its report, while leaving a descendant
// in its own session holding the inherited output pipes. The runner must then cut
// the capture short.
const heldStreamsScript = `printf '{"state":"ready"}\n'
setsid sh -c 'echo $$ > escaped.pid; sleep 30' &
while [ ! -f escaped.pid ]; do sleep 0.01; done
exit 0
`

// cleanupEscaped kills the escaped descendant a held-stream fixture leaves, by
// PID group, so a failing assertion does not leak it.
func cleanupEscaped(t *testing.T, dir string) {
	t.Helper()
	pidPath := filepath.Join(dir, "escaped.pid")
	t.Cleanup(func() {
		data, err := os.ReadFile(pidPath)
		if err != nil {
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err == nil && pid > 1 {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
	})
}

// TestEvaluateAutoCompletionCommandHeldStreamsRejectStdoutOutputs proves a check
// whose output was left open by an escaped descendant cannot satisfy a node that
// reads a declared output from that stdout. The value could be a truncation
// artifact, so the contract is unmet and the capture is still recorded.
func TestEvaluateAutoCompletionCommandHeldStreamsRejectStdoutOutputs(t *testing.T) {
	dir := t.TempDir()
	cleanupEscaped(t, dir)
	path := commandScript(t, dir, "check.sh", heldStreamsScript)
	node := commandNode(path, nil, 20, []OutputDefinition{
		stringOutput("state", true, &OutputSource{Command: "stdout", Pointer: "/state"}),
	})
	_, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	unmet := contractUnmet(t, err)
	if !strings.Contains(unmet.Reason, "left its output streams open") {
		t.Fatalf("rejection = %v, want the held-stream rejection", unmet.Reason)
	}
	stdout := captureNamed(unmet.Evidence, completionCommandStdoutStoredPath)
	if stdout == nil {
		t.Fatalf("evidence = %+v, want the capture preserved", unmet.Evidence)
	}
	if summary := checkSummary(t, *stdout); !summary.StreamsTruncated {
		t.Fatalf("summary = %+v, want the truncation recorded", summary)
	}
}

// TestEvaluateAutoCompletionCommandHeldStreamsMeetWithoutStdoutOutputs pins the
// other half of the rule. With no output read from the check's stdout, the exit
// status is the whole contract. The evidence records the cut-short streams instead
// of hiding them.
func TestEvaluateAutoCompletionCommandHeldStreamsMeetWithoutStdoutOutputs(t *testing.T) {
	dir := t.TempDir()
	cleanupEscaped(t, dir)
	path := commandScript(t, dir, "check.sh", heldStreamsScript)
	node := commandNode(path, nil, 20, nil)
	plan, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("evaluateAuto: %v", err)
	}
	stdout := captureNamed(plan.Captures, completionCommandStdoutStoredPath)
	if stdout == nil {
		t.Fatalf("captures = %+v, want the check's capture", plan.Captures)
	}
	summary := checkSummary(t, *stdout)
	if !summary.StreamsTruncated {
		t.Fatalf("summary = %+v, want the truncation recorded", summary)
	}
	if summary.Termination != completionCommandExited || summary.ExitCode == nil || *summary.ExitCode != 0 {
		t.Fatalf("summary = %+v, want the check's clean exit", summary)
	}
}

// TestEvaluateAutoCompletionCommandStdoutMustBeOneJSONDocument proves a check's
// captured stdout is read as exactly one JSON document. Trailing content after a
// complete value is rejected rather than silently ignored, while trailing
// whitespace, such as a newline, stays valid.
func TestEvaluateAutoCompletionCommandStdoutMustBeOneJSONDocument(t *testing.T) {
	dir := t.TempDir()
	trailing := commandScript(t, dir, "trailing.sh", `printf '{"state":"ready"} {"state":"other"}\n'`)
	node := commandNode(trailing, nil, 10, []OutputDefinition{
		stringOutput("state", true, &OutputSource{Command: "stdout", Pointer: "/state"}),
	})
	_, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	unmet := contractUnmet(t, err)
	if !strings.Contains(unmet.Reason, "not valid JSON") || !strings.Contains(unmet.Reason, "multiple JSON values") {
		t.Fatalf("rejection = %v, want the trailing-content rejection", unmet.Reason)
	}
	if captureNamed(unmet.Evidence, completionCommandStdoutStoredPath) == nil {
		t.Fatalf("evidence = %+v, want the capture preserved", unmet.Evidence)
	}

	// Trailing whitespace is still one document.
	trailingSpace := commandScript(t, dir, "space.sh", `printf '{"state":"ready"}\n\n  \n'`)
	node = commandNode(trailingSpace, nil, 10, []OutputDefinition{
		stringOutput("state", true, &OutputSource{Command: "stdout", Pointer: "/state"}),
	})
	plan, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("evaluateAuto: %v", err)
	}
	if len(plan.Outputs) != 1 || string(plan.Outputs[0].Value) != `"ready"` {
		t.Fatalf("outputs = %+v, want the pointer resolved from the document", plan.Outputs)
	}
}

// TestEvaluateAutoCompletionArtifactPointerMustBeOneJSONDocument proves the shared
// document reader applies the same rule to an artifact pointer source. A files
// contract and a command contract cannot disagree about what "valid JSON" means.
func TestEvaluateAutoCompletionArtifactPointerMustBeOneJSONDocument(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "data.json"), []byte("{\"n\":3}\nnot json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	node := autoEvalNode([]OutputDefinition{
		{ID: "n", Name: "n", Type: OutputNumber, Required: true, Source: &OutputSource{Artifact: "data.json", Pointer: "/n"}},
	}, &CompletionContract{Kind: CompletionFiles, Artifacts: []ArtifactRequirement{{Path: "data.json"}}},
		map[OutcomeName]NodeID{"done": "next"})
	_, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if err == nil || !strings.Contains(err.Error(), "not valid JSON") {
		t.Fatalf("error = %v, want the trailing-content rejection for the artifact", err)
	}

	// The same artifact with only trailing whitespace still resolves.
	if err := os.WriteFile(filepath.Join(dir, "data.json"), []byte("{\"n\":3}\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	plan, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("evaluateAuto: %v", err)
	}
	if len(plan.Outputs) != 1 || string(plan.Outputs[0].Value) != "3" {
		t.Fatalf("outputs = %+v, want the pointer resolved", plan.Outputs)
	}
}

// TestEvaluateAutoCompletionCommandHasNoCredentials proves the check observes
// external state with an empty environment: no supervisor variable, and none
// of the ambient process environment, reaches it.
func TestEvaluateAutoCompletionCommandHasNoCredentials(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AVENOR_WORKFLOW_CREDENTIAL", "workflow-secret-value")
	dump := filepath.Join(dir, "env.txt")
	path := commandScript(t, dir, "check.sh", `env > `+dump)
	node := commandNode(path, nil, 10, nil)
	if _, err := evaluateAuto(evalTemplate(node, "done"), node, dir, ""); err != nil {
		t.Fatalf("evaluateAuto: %v", err)
	}
	env, err := os.ReadFile(dump)
	if err != nil {
		t.Fatalf("read check environment: %v", err)
	}
	for _, banned := range []string{"workflow-secret-value", "AVENOR_WORKFLOW_CREDENTIAL=", "PATH=", "HOME="} {
		if strings.Contains(string(env), banned) {
			t.Fatalf("check environment %q contains %s", env, banned)
		}
	}
}

// TestEvaluateAutoCompletionCommandRunsInAttemptWorkingDirectory pins the
// check's working directory to the attempt's, never the supervisor's.
func TestEvaluateAutoCompletionCommandRunsInAttemptWorkingDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "state.txt"), []byte("ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "cwd.txt")
	path := commandScript(t, dir, "check.sh", `pwd > `+out+`; cat state.txt`)
	node := commandNode(path, nil, 10, nil)
	if _, err := evaluateAuto(evalTemplate(node, "done"), node, dir, ""); err != nil {
		t.Fatalf("evaluateAuto: %v", err)
	}
	cwd, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read check cwd: %v", err)
	}
	if got := strings.TrimSpace(string(cwd)); got != dir {
		t.Fatalf("check cwd = %q, want the attempt working directory %q", got, dir)
	}
}

// TestEvaluateAutoCompletionCommandLeavesNoTempFiles proves the check's captures
// travel in memory. A passing run and a rejected run both leave the host's temp
// directory untouched, so no evaluation can strand a scratch file.
func TestEvaluateAutoCompletionCommandLeavesNoTempFiles(t *testing.T) {
	scratch := t.TempDir()
	t.Setenv("TMPDIR", scratch)
	dir := t.TempDir()
	passing := commandNode(commandScript(t, dir, "pass.sh", `printf '{"state":"ready"}\n'`), nil, 10, nil)
	if _, err := evaluateAuto(evalTemplate(passing, "done"), passing, dir, ""); err != nil {
		t.Fatalf("evaluateAuto: %v", err)
	}
	failing := commandNode(commandScript(t, dir, "fail.sh", `exit 1`), nil, 10, nil)
	if _, err := evaluateAuto(evalTemplate(failing, "done"), failing, dir, ""); err == nil {
		t.Fatal("a failing check met the contract")
	}
	entries, err := os.ReadDir(scratch)
	if err != nil {
		t.Fatalf("read scratch dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("scratch dir holds %d files, want the captures to stay in memory", len(entries))
	}
}

// TestEvaluateAutoCompletionRejectsUnknownCompletionKind pins the fail-closed
// guard: a kind the supervisor cannot evaluate is a rejection, never a
// silently met contract.
func TestEvaluateAutoCompletionRejectsUnknownCompletionKind(t *testing.T) {
	dir := t.TempDir()
	node := autoEvalNode(nil, &CompletionContract{Kind: CompletionContractKind("judge")}, map[OutcomeName]NodeID{"done": "next"})
	plan, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if plan != nil {
		t.Fatalf("plan = %+v, want no plan for an unknown completion kind", plan)
	}
	if err == nil || !strings.Contains(err.Error(), `completion.kind "judge" cannot be evaluated`) {
		t.Fatalf("error = %v, want an unknown-kind rejection", err)
	}
}

func TestValidateCompletionCommandContract(t *testing.T) {
	literal := func(value string) map[string]any { return map[string]any{"type": "literal", "value": value} }
	param := func(name string) map[string]any {
		return map[string]any{"type": "instance_param", "from_instance_param": name}
	}
	command := func(mutate func(c map[string]any)) map[string]any {
		block := map[string]any{
			"argv":            []any{literal("/usr/local/bin/check"), literal("--read-only")},
			"timeout_seconds": 30,
		}
		if mutate != nil {
			mutate(block)
		}
		return block
	}
	node := func(mutate func(n map[string]any), commandBlock map[string]any) map[string]any {
		n := map[string]any{
			"id":       "produce",
			"action":   map[string]any{"type": "run", "prompt": "work"},
			"dispatch": map[string]any{"mode": "auto", "controller_id": "c1"},
			"branches": map[string]any{"done": "next"},
		}
		if commandBlock != nil {
			n["completion"] = map[string]any{"kind": "command", "command": commandBlock}
		}
		if mutate != nil {
			mutate(n)
		}
		return n
	}

	tests := []struct {
		name    string
		node    map[string]any
		params  []any
		wantErr string
	}{
		{name: "valid", node: node(nil, command(nil))},
		{
			name: "instance param element",
			node: node(nil, command(func(c map[string]any) {
				c["argv"] = []any{literal("/usr/local/bin/check"), param("target")}
			})),
		},
		{
			name:    "kind command without the command block",
			node:    node(func(n map[string]any) { n["completion"] = map[string]any{"kind": "command"} }, nil),
			wantErr: `requires a completion.command block`,
		},
		{
			name: "command block with another kind",
			node: node(func(n map[string]any) {
				n["completion"] = map[string]any{"kind": "files", "artifacts": []any{map[string]any{"path": "out.md"}}, "command": command(nil)}
			}, nil),
			wantErr: `completion.command requires completion.kind "command"`,
		},
		{
			name: "unknown kind",
			node: node(func(n map[string]any) {
				n["completion"] = map[string]any{"kind": "judge"}
			}, nil),
			wantErr: `completion.kind "judge" is not a completion contract kind`,
		},
		{
			name: "artifacts alongside a command contract",
			node: node(func(n map[string]any) {
				n["completion"].(map[string]any)["artifacts"] = []any{map[string]any{"path": "out.md"}}
			}, command(nil)),
			wantErr: `cannot declare completion.artifacts or completion.git`,
		},
		{
			name:    "manual dispatch",
			node:    node(func(n map[string]any) { n["dispatch"] = map[string]any{"mode": "manual"} }, command(nil)),
			wantErr: `requires dispatch.mode "auto"`,
		},
		{
			name:    "relative executable",
			node:    node(nil, command(func(c map[string]any) { c["argv"] = []any{literal("check")} })),
			wantErr: `argv[0] must be a literal absolute executable path`,
		},
		{
			name:    "instance param executable",
			node:    node(nil, command(func(c map[string]any) { c["argv"] = []any{param("target")} })),
			wantErr: `argv[0] must be a literal absolute executable path`,
		},
		{
			name: "undeclared instance param",
			node: node(nil, command(func(c map[string]any) {
				c["argv"] = []any{literal("/usr/local/bin/check"), param("absent")}
			})),
			wantErr: `names undeclared instance param "absent"`,
		},
		{
			name: "optional instance param",
			node: node(nil, command(func(c map[string]any) {
				c["argv"] = []any{literal("/usr/local/bin/check"), param("target")}
			})),
			params:  []any{map[string]any{"id": "target", "type": "string"}},
			wantErr: `which must be declared required`,
		},
		{
			name:    "zero timeout",
			node:    node(nil, command(func(c map[string]any) { c["timeout_seconds"] = 0 })),
			wantErr: `completion.command.timeout_seconds must be in [1, 300]`,
		},
		{
			name:    "timeout past the bound",
			node:    node(nil, command(func(c map[string]any) { c["timeout_seconds"] = maxCompletionCommandTimeoutSeconds + 1 })),
			wantErr: `completion.command.timeout_seconds must be in [1, 300]`,
		},
		{
			name:    "empty argv",
			node:    node(nil, command(func(c map[string]any) { c["argv"] = []any{} })),
			wantErr: `argv must not be empty`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := test.params
			if params == nil {
				params = []any{map[string]any{"id": "target", "type": "string", "required": true}}
			}
			tmpl := map[string]any{
				"schema_version":    1,
				"template_id":       "command-contract",
				"template_version":  "1",
				"entry_nodes":       []string{"produce"},
				"params":            params,
				"nodes":             []any{test.node, map[string]any{"id": "next", "action": map[string]any{"type": "manual"}}},
				"terminal_outcomes": []string{"done"},
			}
			data, err := json.Marshal(tmpl)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			// The semantic validator is the subject here: decode into the
			// Template struct and validate it directly.
			var decoded Template
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("decode template: %v", err)
			}
			err = ValidateTemplate(decoded)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTemplate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, test.wantErr)
			}
		})
	}
}

// TestCompletionCommandTimeoutBoundAgreesAcrossSchemas proves the structural
// profile and the semantic validator accept and reject the same command deadline.
// The declared maximum passes both, and one second past it fails both. A template
// therefore cannot be valid on one path and invalid on the other.
func TestCompletionCommandTimeoutBoundAgreesAcrossSchemas(t *testing.T) {
	t.Parallel()
	template := func(timeoutSeconds int64) []byte {
		return mustMarshal(t, map[string]any{
			"schema_version":   1,
			"template_id":      "command-timeout",
			"template_version": "1",
			"entry_nodes":      []string{"produce"},
			"nodes": []any{
				map[string]any{
					"id":       "produce",
					"action":   map[string]any{"type": "run", "prompt": "work"},
					"dispatch": map[string]any{"mode": "auto", "controller_id": "c1"},
					"branches": map[string]any{"done": "next"},
					"completion": map[string]any{"kind": "command", "command": map[string]any{
						"argv":            []any{map[string]any{"type": "literal", "value": "/usr/local/bin/check"}},
						"timeout_seconds": timeoutSeconds,
					}},
				},
				map[string]any{"id": "next", "action": map[string]any{"type": "manual"}},
			},
			"terminal_outcomes": []string{"done"},
		})
	}
	valid := template(maxCompletionCommandTimeoutSeconds)
	if err := ValidateTemplateJSON(valid); err != nil {
		t.Fatalf("the declared maximum must pass the structure and the semantics: %v", err)
	}
	over := template(maxCompletionCommandTimeoutSeconds + 1)
	if err := ValidateTemplateJSON(over); err == nil {
		t.Fatal("a deadline past the declared maximum passed validation")
	}
	var decoded Template
	if err := json.Unmarshal(over, &decoded); err != nil {
		t.Fatalf("decode template: %v", err)
	}
	if err := ValidateTemplate(decoded); err == nil ||
		!strings.Contains(err.Error(), "completion.command.timeout_seconds must be in") {
		t.Fatalf("semantic error = %v, want the deadline rejection", err)
	}
}

// mustMarshal is the JSON encoder for tests that build a raw template document.
func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

// TestValidateRejectsCommandContractOnAnAutoExternalNode proves the guard runs on
// the branch that is actually reachable. An auto external node passes dispatch
// validation, because it resolves from its gates, so a command contract declared
// there would never be evaluated. The template is valid without the contract, and
// the same node with the contract is rejected.
func TestValidateRejectsCommandContractOnAnAutoExternalNode(t *testing.T) {
	t.Parallel()
	// The bound-gate fixture's `review` node is a valid auto external node: one
	// required external gate with a subject binding, on a causal output.
	decode := func(t *testing.T, data []byte) Template {
		t.Helper()
		var tmpl Template
		if err := json.Unmarshal(data, &tmpl); err != nil {
			t.Fatalf("decode template: %v", err)
		}
		return tmpl
	}
	if err := ValidateTemplate(decode(t, []byte(boundGateTemplateJSON))); err != nil {
		t.Fatalf("the fixture auto external node is not valid to begin with: %v", err)
	}

	withContract := mutateBoundTemplate(boundGateTemplateJSON, func(template map[string]any) {
		for _, raw := range template["nodes"].([]any) {
			node := raw.(map[string]any)
			if node["id"] != "review" {
				continue
			}
			node["completion"] = map[string]any{"kind": "command", "command": map[string]any{
				"argv":            []any{map[string]any{"type": "literal", "value": "/usr/local/bin/check"}},
				"timeout_seconds": 30,
			}}
		}
	})
	wantErr := `completion.kind "command" requires a run, loop, or team action ("external" nodes are never evaluated as a completion command)`
	if err := ValidateTemplate(decode(t, withContract)); err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("ValidateTemplate error = %v, want the external-action rejection", err)
	}
	// The same rejection on the ingress path templates actually arrive through.
	if err := ValidateTemplateJSON(withContract); err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("ValidateTemplateJSON error = %v, want the external-action rejection", err)
	}
}

func TestValidateOutputSourceCommandRules(t *testing.T) {
	source := func(src map[string]any, outputType OutputType) map[string]any {
		return map[string]any{
			"schema_version":   1,
			"template_id":      "command-source",
			"template_version": "1",
			"entry_nodes":      []string{"produce"},
			"nodes": []any{
				map[string]any{
					"id":       "produce",
					"action":   map[string]any{"type": "run", "prompt": "work"},
					"dispatch": map[string]any{"mode": "auto", "controller_id": "c1"},
					"branches": map[string]any{"done": "next"},
					"completion": map[string]any{"kind": "command", "command": map[string]any{
						"argv":            []any{map[string]any{"type": "literal", "value": "/usr/local/bin/check"}},
						"timeout_seconds": 30,
					}},
					"outputs": []any{map[string]any{
						"id": "state", "name": "state", "type": string(outputType), "required": true, "source": src,
					}},
				},
				map[string]any{"id": "next", "action": map[string]any{"type": "manual"}},
			},
			"terminal_outcomes": []string{"done"},
		}
	}
	tests := []struct {
		name    string
		src     map[string]any
		typ     OutputType
		wantErr string
	}{
		{name: "pointer into stdout", src: map[string]any{"command": "stdout", "pointer": "/state"}, typ: OutputString},
		{name: "number from stdout", src: map[string]any{"command": "stdout", "pointer": "/n"}, typ: OutputNumber},
		{
			name:    "pointer required",
			src:     map[string]any{"command": "stdout"},
			typ:     OutputString,
			wantErr: `command source requires a pointer`,
		},
		{
			name:    "unknown selector",
			src:     map[string]any{"command": "stderr", "pointer": "/state"},
			typ:     OutputString,
			wantErr: `command source "stderr" must be "stdout"`,
		},
		{
			name:    "file output",
			src:     map[string]any{"command": "stdout", "pointer": "/state"},
			typ:     OutputFile,
			wantErr: `a pointer source requires a non-file output`,
		},
		{
			name:    "two sources",
			src:     map[string]any{"command": "stdout", "pointer": "/state", "git": "head"},
			typ:     OutputString,
			wantErr: `declares more than one of artifact, git, or command source`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(source(test.src, test.typ))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			err = ValidateTemplateJSON(data)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTemplateJSON: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, test.wantErr)
			}
		})
	}
}

// TestValidateOutputSourceCommandRequiresContract rejects reading a value from
// a stdout document no command contract produces.
func TestValidateOutputSourceCommandRequiresContract(t *testing.T) {
	tmpl := map[string]any{
		"schema_version":   1,
		"template_id":      "command-source-nocontract",
		"template_version": "1",
		"entry_nodes":      []string{"produce"},
		"nodes": []any{
			map[string]any{
				"id":     "produce",
				"action": map[string]any{"type": "run", "prompt": "work"},
				"branches": map[string]any{
					"done": "next",
				},
				"outputs": []any{map[string]any{
					"id": "state", "name": "state", "type": "string", "required": true,
					"source": map[string]any{"command": "stdout", "pointer": "/state"},
				}},
			},
			map[string]any{"id": "next", "action": map[string]any{"type": "manual"}},
		},
		"terminal_outcomes": []string{"done"},
	}
	data, err := json.Marshal(tmpl)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := ValidateTemplateJSON(data); err == nil ||
		!strings.Contains(err.Error(), `command source requires the node's "command" completion contract`) {
		t.Fatalf("error = %v, want the missing-contract rejection", err)
	}
}

// waitForCheckStart blocks until the check script recorded its pid, failing
// early when the evaluation finished (or failed) before the check started.
func waitForCheckStart(t *testing.T, pidPath string, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if data, err := os.ReadFile(pidPath); err == nil && strings.TrimSpace(string(data)) != "" {
			return
		}
		select {
		case err := <-done:
			t.Fatalf("the evaluation finished before the check started: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the check script never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertCheckProcessGone proves the check's process group was killed.
func assertCheckProcessGone(t *testing.T, dir string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "shell.pid"))
	if err != nil {
		t.Fatalf("read shell pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("bad pid %q: %v", data, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("check process group leader %d still alive", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
