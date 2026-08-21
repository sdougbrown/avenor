package main

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/workflow"
)

const workflowCLITemplateJSON = `{
  "schema_version": 1,
  "template_id": "cli-test",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [{"id": "start", "action": {"type": "manual"}}],
  "terminal_outcomes": ["done"]
}`

// TestWorkflowCLIEndToEnd drives the workflow CLI through the actual
// in-process control server and workflow manager: create → instantiate →
// status → wait → inspect → events. No provider or network involved.
func TestWorkflowCLIEndToEnd(t *testing.T) {
	root := t.TempDir()
	store := workflow.New(root)
	mgr := workflow.NewManager(store)

	srv := control.NewServer(control.NewState("run_cli", "", 0))
	srv.SetWorkflowHandler(mgr)
	sock := filepath.Join(t.TempDir(), "control.sock")
	if err := srv.Start(sock); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(srv.Stop)

	dir := t.TempDir()
	tmplPath := filepath.Join(dir, "template.json")
	if err := os.WriteFile(tmplPath, []byte(workflowCLITemplateJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	instPath := filepath.Join(dir, "instance.json")
	if err := os.WriteFile(instPath, []byte(`{"metadata":{"a":1}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) (string, int) {
		var out, errBuf bytes.Buffer
		code := runWorkflowTo(append([]string{"--socket", sock}, args...), &out, &errBuf)
		if code != 0 {
			t.Logf("stderr for %v: %s", args, errBuf.String())
		}
		return out.String(), code
	}

	out, code := run("create", "--request-file", tmplPath)
	if code != 0 {
		t.Fatalf("create: exit %d", code)
	}
	if !strings.Contains(out, "cli-test") {
		t.Fatalf("create output missing template id: %s", out)
	}

	out, code = run("instantiate", "--template-id", "cli-test", "--template-version", "1", "--request-file", instPath)
	if code != 0 {
		t.Fatalf("instantiate: exit %d", code)
	}
	if !strings.Contains(out, "workflow_id") {
		t.Fatalf("instantiate output missing workflow_id: %s", out)
	}
	var inst struct {
		WorkflowID string `json:"workflow_id"`
	}
	if err := json.Unmarshal([]byte(out), &inst); err != nil {
		t.Fatalf("parse instantiate output: %v\n%s", err, out)
	}
	if inst.WorkflowID == "" {
		t.Fatalf("instantiate output has empty workflow_id: %s", out)
	}
	id := inst.WorkflowID

	out, code = run("status", id)
	if code != 0 {
		t.Fatalf("status: exit %d", code)
	}
	if !strings.Contains(out, "active") {
		t.Fatalf("status output not active: %s", out)
	}

	out, code = run("wait", id, "--timeout", "100ms")
	if code != 0 {
		t.Fatalf("wait: exit %d", code)
	}
	if !strings.Contains(out, "active") {
		t.Fatalf("wait output not active: %s", out)
	}

	out, code = run("inspect", id)
	if code != 0 {
		t.Fatalf("inspect: exit %d", code)
	}
	if !strings.Contains(out, "activations") {
		t.Fatalf("inspect output missing activations: %s", out)
	}

	out, code = run("events", id)
	if code != 0 {
		t.Fatalf("events: exit %d", code)
	}
	if !strings.Contains(out, "workflow_id") {
		t.Fatalf("events output missing workflow_id: %s", out)
	}
}

const workflowCLIGatedTemplateJSON = `{
  "schema_version": 1,
  "template_id": "cli-gated",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [
    {"id": "start", "action": {"type": "manual"}, "branches": {"done": "next"}, "gates": [{"id": "review", "type": "human", "required": true}]},
    {"id": "next", "action": {"type": "manual"}}
  ],
  "terminal_outcomes": ["done"]
}`

// cliWorkflowEnv spins up the in-process control server + manager (same
// harness as TestWorkflowCLIEndToEnd), stores and instantiates the gated CLI
// template, and returns a CLI runner plus the workflow id.
func cliWorkflowEnv(t *testing.T) (func(args ...string) (string, int), *workflow.Manager, string) {
	t.Helper()
	root := t.TempDir()
	store := workflow.New(root)
	mgr := workflow.NewManager(store)

	srv := control.NewServer(control.NewState("run_cli", "", 0))
	srv.SetWorkflowHandler(mgr)
	sock := filepath.Join(t.TempDir(), "control.sock")
	if err := srv.Start(sock); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(srv.Stop)

	dir := t.TempDir()
	tmplPath := filepath.Join(dir, "template.json")
	if err := os.WriteFile(tmplPath, []byte(workflowCLIGatedTemplateJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	instPath := filepath.Join(dir, "instance.json")
	if err := os.WriteFile(instPath, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) (string, int) {
		var out, errBuf bytes.Buffer
		code := runWorkflowTo(append([]string{"--socket", sock}, args...), &out, &errBuf)
		if code != 0 {
			t.Logf("stderr for %v: %s", args, errBuf.String())
		}
		return out.String(), code
	}
	out, code := run("create", "--request-file", tmplPath)
	if code != 0 {
		t.Fatalf("create: exit %d", code)
	}
	out, code = run("instantiate", "--template-id", "cli-gated", "--template-version", "1", "--request-file", instPath)
	if code != 0 {
		t.Fatalf("instantiate: exit %d", code)
	}
	var inst struct {
		WorkflowID string `json:"workflow_id"`
	}
	if err := json.Unmarshal([]byte(out), &inst); err != nil || inst.WorkflowID == "" {
		t.Fatalf("instantiate output: %s", out)
	}
	return run, mgr, inst.WorkflowID
}

// cliStartActivation claims and starts the entry activation through the
// manager (the claim/start CLI surface does not exist yet) and returns the
// attempt/lease/owner identity needed to complete it.
func cliStartActivation(t *testing.T, mgr *workflow.Manager, wfID, activationID string) (attemptID, leaseID, ownerToken string) {
	t.Helper()
	claim, err := mgr.WorkflowCommand(wfID, []byte(`{"op":"claim","node_id":"start","activation_id":"`+activationID+`","actor":"alice"}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	claimMM := claim.(map[string]any)
	start, err := mgr.WorkflowCommand(wfID, []byte(`{"op":"start","node_id":"start","activation_id":"`+activationID+`","lease_id":"`+claimMM["lease_id"].(string)+`","owner_token":"`+claimMM["owner_token"].(string)+`"}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	startMM := start.(map[string]any)
	return startMM["attempt_id"].(string), claimMM["lease_id"].(string), claimMM["owner_token"].(string)
}

// cliInspectStart returns the start activation's (id, status) from a CLI
// inspect call.
func cliInspectStart(t *testing.T, run func(args ...string) (string, int), wfID string) (string, string) {
	t.Helper()
	out, code := run("inspect", wfID)
	if code != 0 {
		t.Fatalf("inspect: exit %d", code)
	}
	var res struct {
		Activations []struct {
			NodeID       string `json:"node_id"`
			ActivationID string `json:"activation_id"`
			Status       string `json:"status"`
		} `json:"activations"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("parse inspect: %v", err)
	}
	for _, a := range res.Activations {
		if a.NodeID == "start" {
			return a.ActivationID, a.Status
		}
	}
	t.Fatal("no start activation in inspect output")
	return "", ""
}

func cliWriteFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestWorkflowCLIGateRoundTrip drives the full gate control surface through
// the in-process control server: complete (CLI) parks awaiting_gate, gate
// satisfy (CLI) resolves the parked branch and its re-issue is idempotent,
// skip (CLI) waives and resolves, and unblock (CLI) returns a blocked
// activation to ready. Park states are prepared through the manager because
// the claim/start CLI surface does not exist yet.
func TestWorkflowCLIGateRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// -- gate satisfy: complete (CLI) parks awaiting_gate, then gate (CLI)
	// resolves the parked branch.
	run, mgr, wf := cliWorkflowEnv(t)
	actID, status := cliInspectStart(t, run, wf)
	if status != "pending" {
		t.Fatalf("start status = %q, want pending", status)
	}
	attemptID, leaseID, ownerToken := cliStartActivation(t, mgr, wf, actID)

	completeReq := cliWriteFile(t, dir, "complete.json", `{"owner_token":"`+ownerToken+`","outcome":"done","outputs":[],"artifacts":[]}`)
	out, code := run("complete", wf, "start", "--activation-id", actID, "--attempt-id", attemptID, "--lease-id", leaseID, "--request-file", completeReq)
	if code != 0 {
		t.Fatalf("complete: exit %d: %s", code, out)
	}
	if _, status = cliInspectStart(t, run, wf); status != "awaiting_gate" {
		t.Fatalf("start status after complete = %q, want awaiting_gate", status)
	}

	gateReq := cliWriteFile(t, dir, "gate.json", `{"actor":"alice","reason":"reviewed","evidence_ids":["ev_1"]}`)
	out, code = run("gate", wf, "start", "review", "--activation-id", actID, "--operation", "satisfy", "--request-file", gateReq)
	if code != 0 {
		t.Fatalf("gate satisfy: exit %d: %s", code, out)
	}
	if _, status = cliInspectStart(t, run, wf); status != "satisfied" {
		t.Fatalf("start status after gate = %q, want satisfied", status)
	}
	// The branch target activation must exist.
	if insOut, code := run("inspect", wf); code != 0 || !strings.Contains(insOut, `"next"`) {
		t.Fatalf("inspect output missing next activation (exit %d): %s", code, insOut)
	}
	// An idempotent re-issue of the same decision is a no-op.
	out, code = run("gate", wf, "start", "review", "--activation-id", actID, "--operation", "satisfy", "--request-file", gateReq)
	if code != 0 {
		t.Fatalf("gate re-issue: exit %d: %s", code, out)
	}
	if !strings.Contains(out, `"idempotent": true`) {
		t.Fatalf("gate re-issue not idempotent: %s", out)
	}

	// -- skip: a fresh gated instance parks, then skip (CLI) waives and
	// resolves.
	run2, mgr2, wf2 := cliWorkflowEnv(t)
	act2, _ := cliInspectStart(t, run2, wf2)
	att2, lease2, tok2 := cliStartActivation(t, mgr2, wf2, act2)
	completeReq2 := cliWriteFile(t, dir, "complete2.json", `{"owner_token":"`+tok2+`","outcome":"done","outputs":[],"artifacts":[]}`)
	if out, code := run2("complete", wf2, "start", "--activation-id", act2, "--attempt-id", att2, "--lease-id", lease2, "--request-file", completeReq2); code != 0 {
		t.Fatalf("complete 2: exit %d: %s", code, out)
	}
	skipReq := cliWriteFile(t, dir, "skip.json", `{"actor":"bob","reason":"not needed","evidence_ids":["ev_2"]}`)
	out, code = run2("skip", wf2, "start", "--request-file", skipReq)
	if code != 0 {
		t.Fatalf("skip: exit %d: %s", code, out)
	}
	if !strings.Contains(out, `"skipped": true`) {
		t.Fatalf("skip result = %s", out)
	}
	if _, status = cliInspectStart(t, run2, wf2); status != "satisfied" {
		t.Fatalf("start status after skip = %q, want satisfied", status)
	}

	// -- unblock: block the entry activation via a failed attempt, then
	// unblock (CLI) returns it to ready.
	run3, mgr3, wf3 := cliWorkflowEnv(t)
	act3, _ := cliInspectStart(t, run3, wf3)
	att3, lease3, _ := cliStartActivation(t, mgr3, wf3, act3)
	if err := mgr3.RecordAttemptTerminated(workflow.WorkflowID(wf3), "start", workflow.ActivationID(act3), workflow.AttemptID(att3), workflow.LeaseID(lease3), workflow.AttemptFailed); err != nil {
		t.Fatalf("terminate failed: %v", err)
	}
	if _, status = cliInspectStart(t, run3, wf3); status != "blocked" {
		t.Fatalf("start status after failed attempt = %q, want blocked", status)
	}
	unblockReq := cliWriteFile(t, dir, "unblock.json", `{"actor":"carol","reason":"root cause fixed"}`)
	out, code = run3("unblock", wf3, "start", "--request-file", unblockReq)
	if code != 0 {
		t.Fatalf("unblock: exit %d: %s", code, out)
	}
	if _, status = cliInspectStart(t, run3, wf3); status != "ready" {
		t.Fatalf("start status after unblock = %q, want ready", status)
	}
}

// TestWorkflowCLIGateUnknownOperation pins the CLI-level fail-fast: an
// unknown --operation exits nonzero with a clear message before any server
// call, so nothing is mutated.
func TestWorkflowCLIGateUnknownOperation(t *testing.T) {
	dir := t.TempDir()
	gateReq := cliWriteFile(t, dir, "gate.json", `{"actor":"alice","reason":"r","evidence_ids":["ev_1"]}`)
	// A dialable (but unhandled) socket: the CLI validates --operation before
	// issuing any RPC, so no server response is ever needed.
	sock := filepath.Join(dir, "control.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	defer os.Remove(sock)
	var out, errBuf bytes.Buffer
	code := runWorkflowTo([]string{"--socket", sock, "gate", "wf_1", "start", "review", "--activation-id", "act_1", "--operation", "bogus", "--request-file", gateReq}, &out, &errBuf)
	if code != 1 {
		t.Fatalf("gate with unknown operation: want exit 1, got %d", code)
	}
	if !strings.Contains(errBuf.String(), "unknown operation") {
		t.Fatalf("stderr = %q, want unknown operation", errBuf.String())
	}
}

// TestWorkflowCLIGateMissingActorUnchanged pins the CLI-level missing-fields
// pre-mutation guarantee: a gate satisfy with a request file that omits
// `actor` exits nonzero and does NOT mutate the parked activation (it stays
// awaiting_gate, no gate recorded).
func TestWorkflowCLIGateMissingActorUnchanged(t *testing.T) {
	dir := t.TempDir()
	run, mgr, wf := cliWorkflowEnv(t)
	actID, status := cliInspectStart(t, run, wf)
	if status != "pending" {
		t.Fatalf("start status = %q, want pending", status)
	}
	attemptID, leaseID, ownerToken := cliStartActivation(t, mgr, wf, actID)
	completeReq := cliWriteFile(t, dir, "complete.json", `{"owner_token":"`+ownerToken+`","outcome":"done","outputs":[],"artifacts":[]}`)
	if out, code := run("complete", wf, "start", "--activation-id", actID, "--attempt-id", attemptID, "--lease-id", leaseID, "--request-file", completeReq); code != 0 {
		t.Fatalf("complete: exit %d: %s", code, out)
	}
	if _, status = cliInspectStart(t, run, wf); status != "awaiting_gate" {
		t.Fatalf("start status after complete = %q, want awaiting_gate", status)
	}

	// A gate request missing the required `actor` field.
	gateReq := cliWriteFile(t, dir, "gate.json", `{"reason":"r","evidence_ids":["ev"]}`)
	if out, code := run("gate", wf, "start", "review", "--activation-id", actID, "--operation", "satisfy", "--request-file", gateReq); code == 0 {
		t.Fatalf("gate satisfy without actor: want nonzero exit, got 0: %s", out)
	}
	if _, status = cliInspectStart(t, run, wf); status != "awaiting_gate" {
		t.Fatalf("start status after rejected gate = %q, want awaiting_gate (no mutation)", status)
	}
}

// TestWorkflowCLIArgErrors covers the missing-socket, unknown-subcommand, and
// missing-workflow-id paths.
func TestWorkflowCLIArgErrors(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runWorkflowTo([]string{"create", "--request-file", "x.json"}, &out, &errBuf); code != 1 {
		t.Fatalf("missing --socket: want exit 1, got %d", code)
	}
	if !strings.Contains(errBuf.String(), "--socket is required") {
		t.Fatalf("missing --socket stderr: %q", errBuf.String())
	}

	errBuf.Reset()
	if code := runWorkflowTo([]string{"--socket", "/nonexistent.sock", "bogus"}, &out, &errBuf); code != 1 {
		t.Fatalf("bad socket: want exit 1, got %d", code)
	}

	errBuf.Reset()
	if code := runWorkflowTo([]string{"--socket", "/nonexistent.sock"}, &out, &errBuf); code != 1 {
		t.Fatalf("no subcommand: want exit 1, got %d", code)
	}
	if !strings.Contains(errBuf.String(), "command required") {
		t.Fatalf("no-subcommand stderr: %q", errBuf.String())
	}
}
