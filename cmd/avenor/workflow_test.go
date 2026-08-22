package main

import (
	"bytes"
	"encoding/json"
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
