package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/control"
)

// fakeControllerHandler is a recording stub implementing
// control.WorkflowControllerHandler for CLI round-trip tests.
type fakeControllerHandler struct {
	mu            sync.Mutex
	createParams  json.RawMessage
	enableID      string
	disableID     string
	disableParams json.RawMessage
	statusID      string
	listCalled    bool
	readyID       string
	readyLimit    int
}

var _ control.WorkflowControllerHandler = (*fakeControllerHandler)(nil)

func (f *fakeControllerHandler) WorkflowControllerCreate(params json.RawMessage) (any, error) {
	f.mu.Lock()
	f.createParams = params
	f.mu.Unlock()
	return map[string]any{"created": true, "controller_id": "c_new"}, nil
}

func (f *fakeControllerHandler) WorkflowControllerEnable(id string) (any, error) {
	f.mu.Lock()
	f.enableID = id
	f.mu.Unlock()
	return map[string]any{"enabled": id}, nil
}

func (f *fakeControllerHandler) WorkflowControllerDisable(id string, params json.RawMessage) (any, error) {
	f.mu.Lock()
	f.disableID = id
	f.disableParams = params
	f.mu.Unlock()
	return map[string]any{"disabled": id}, nil
}

func (f *fakeControllerHandler) WorkflowControllerStatus(id string) (any, error) {
	f.mu.Lock()
	f.statusID = id
	f.mu.Unlock()
	return map[string]any{"status": "enabled"}, nil
}

func (f *fakeControllerHandler) WorkflowControllerList() (any, error) {
	f.mu.Lock()
	f.listCalled = true
	f.mu.Unlock()
	return map[string]any{"controllers": []any{"c1"}}, nil
}

func (f *fakeControllerHandler) WorkflowReady(id string, limit int) (any, error) {
	f.mu.Lock()
	f.readyID = id
	f.readyLimit = limit
	f.mu.Unlock()
	return map[string]any{"ready": id, "count": limit}, nil
}

// cliControllerEnv spins up the in-process control server with a recording
// controller handler (same harness as the workflow CLI tests) and returns a
// CLI runner plus the handler.
func cliControllerEnv(t *testing.T) (func(args ...string) (string, int), *fakeControllerHandler) {
	t.Helper()
	srv := control.NewServer(control.NewState("run_cli", "", 0))
	fake := &fakeControllerHandler{}
	srv.SetWorkflowControllerHandler(fake)
	sock := filepath.Join(t.TempDir(), "control.sock")
	if err := srv.Start(sock); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(srv.Stop)

	run := func(args ...string) (string, int) {
		var out, errBuf bytes.Buffer
		code := runWorkflowTo(append([]string{"--socket", sock}, args...), &out, &errBuf)
		if code != 0 {
			t.Logf("stderr for %v: %s", args, errBuf.String())
		}
		return out.String(), code
	}
	return run, fake
}

// TestWorkflowControllerCLIRoundTrip drives the controller CLI through the
// in-process control server: create (request file) → enable → disable →
// status → list → ready. No provider or network involved.
func TestWorkflowControllerCLIRoundTrip(t *testing.T) {
	dir := t.TempDir()
	run, fake := cliControllerEnv(t)

	// create via request file.
	reqPath := filepath.Join(dir, "controller.json")
	if err := os.WriteFile(reqPath, []byte(`{"name":"c1"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	out, code := run("controller", "create", "--request-file", reqPath)
	if code != 0 {
		t.Fatalf("create: exit %d", code)
	}
	if !strings.Contains(out, "created") {
		t.Fatalf("create output missing created: %s", out)
	}
	if string(fake.createParams) != `{"name":"c1"}` {
		t.Fatalf("create raw params = %s, want the file bytes", fake.createParams)
	}

	// enable.
	out, code = run("controller", "enable", "c1")
	if code != 0 {
		t.Fatalf("enable: exit %d", code)
	}
	if fake.enableID != "c1" {
		t.Fatalf("enable id = %q, want c1", fake.enableID)
	}
	if !strings.Contains(out, `"enabled": "c1"`) {
		t.Fatalf("enable output missing enabled c1: %s", out)
	}

	// disable.
	out, code = run("controller", "disable", "c1", "--reason", "r")
	if code != 0 {
		t.Fatalf("disable: exit %d", code)
	}
	if fake.disableID != "c1" {
		t.Fatalf("disable id = %q, want c1", fake.disableID)
	}
	if !strings.Contains(string(fake.disableParams), `"reason":"r"`) {
		t.Fatalf("disable raw params = %s, want reason passed through", fake.disableParams)
	}
	if !strings.Contains(out, `"disabled": "c1"`) {
		t.Fatalf("disable output missing disabled c1: %s", out)
	}

	// status.
	out, code = run("controller", "status", "c1")
	if code != 0 {
		t.Fatalf("status: exit %d", code)
	}
	if fake.statusID != "c1" {
		t.Fatalf("status id = %q, want c1", fake.statusID)
	}
	if !strings.Contains(out, `"status": "enabled"`) {
		t.Fatalf("status output missing status: %s", out)
	}

	// list.
	out, code = run("controller", "list")
	if code != 0 {
		t.Fatalf("list: exit %d", code)
	}
	if !fake.listCalled {
		t.Fatal("list did not reach the handler")
	}
	if !strings.Contains(out, "controllers") {
		t.Fatalf("list output missing controllers: %s", out)
	}

	// ready.
	out, code = run("ready", "c1", "--limit", "3")
	if code != 0 {
		t.Fatalf("ready: exit %d", code)
	}
	if fake.readyID != "c1" || fake.readyLimit != 3 {
		t.Fatalf("ready (id=%q limit=%d), want (c1 3)", fake.readyID, fake.readyLimit)
	}
	if !strings.Contains(out, `"ready": "c1"`) {
		t.Fatalf("ready output missing ready c1: %s", out)
	}
}

// TestWorkflowControllerCLIArgErrors pins the CLI-level validation: a missing
// controller subcommand and a disable without --reason both exit nonzero
// before any server mutation.
func TestWorkflowControllerCLIArgErrors(t *testing.T) {
	run, fake := cliControllerEnv(t)

	if _, code := run("controller"); code != 1 {
		t.Fatalf("controller with no subcommand: want exit 1, got %d", code)
	}
	if _, code := run("controller", "bogus"); code != 1 {
		t.Fatalf("controller with unknown subcommand: want exit 1, got %d", code)
	}
	if _, code := run("controller", "disable", "c1"); code == 0 {
		t.Fatal("disable without --reason: want nonzero exit, got 0")
	}
	if _, code := run("ready"); code != 1 {
		t.Fatalf("ready with no controller id: want exit 1, got %d", code)
	}
	if fake.disableID != "" {
		t.Fatalf("disable without --reason should not reach the handler, got %q", fake.disableID)
	}
}
