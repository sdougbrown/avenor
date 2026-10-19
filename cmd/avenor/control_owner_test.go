package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/control/controltest"
)

// gatedHandler records the owner-gated methods a control command reaches.
// Every method it does not override fails the test by panicking, so an
// unexpected call path cannot pass silently.
type gatedHandler struct {
	control.StableHandler
	calls []string
}

func (h *gatedHandler) record(method string) { h.calls = append(h.calls, method) }

func (h *gatedHandler) List() any {
	return []map[string]any{{"runtime_id": "rt_1"}}
}

func (h *gatedHandler) Spawn(json.RawMessage) (any, error) {
	h.record("spawn")
	return map[string]any{"run_id": "run_spawned"}, nil
}

func (h *gatedHandler) RuntimeCancel(string) error {
	h.record("cancel")
	return nil
}

func (h *gatedHandler) RuntimePrompt(_, _, _ string) error {
	h.record("prompt")
	return nil
}

func (h *gatedHandler) RuntimeInterruptAndPrompt(_, _ string, _ bool) error {
	h.record("interrupt-and-prompt")
	return nil
}

func (h *gatedHandler) RuntimeAnswerPermission(_, _, _, _ string) error {
	h.record("answer-permission")
	return nil
}

func (h *gatedHandler) Shutdown(string) error {
	h.record("shutdown")
	return nil
}

// startOwnerControlServer starts a control server that has published its owner
// record and returns its socket path.
func startOwnerControlServer(t *testing.T, handler control.StableHandler) string {
	t.Helper()
	srv, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	srv.SetStableHandler(handler)
	dir, err := os.MkdirTemp("", "ctl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "control.sock")
	if err := srv.Start(socket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	return socket
}

// TestControlReadVerbsNeedNoOwnerRecord proves the read-only verbs reach a
// live socket that never published an owner record: they dial without
// credentials, so no record is loaded or transmitted.
func TestControlReadVerbsNeedNoOwnerRecord(t *testing.T) {
	socket := startOwnerControlServer(t, &gatedHandler{})
	if err := os.Remove(control.OwnerRecordPath(socket)); err != nil {
		t.Fatalf("remove owner record: %v", err)
	}
	for _, verb := range []string{"status", "list"} {
		if code := runControl([]string{"--socket", socket, verb}); code != 0 {
			t.Fatalf("runControl(%s) = %d, want 0 without an owner record", verb, code)
		}
	}
}

// TestControlCommandsAttachAsOwner proves every gated verb the control command
// reaches still works now that a bare connection carries no owner authority.
func TestControlCommandsAttachAsOwner(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"spawn", []string{"spawn", "--prompt", "work"}, "spawn"},
		{"cancel", []string{"cancel", "rt_1"}, "cancel"},
		{"prompt", []string{"prompt", "hello", "rt_1"}, "prompt"},
		{"interrupt-and-prompt", []string{"interrupt-and-prompt", "hello", "rt_1"}, "interrupt-and-prompt"},
		{"answer-permission", []string{"answer-permission", "req_1", "allow", "rt_1"}, "answer-permission"},
		{"shutdown", []string{"shutdown"}, "shutdown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := &gatedHandler{}
			socket := startOwnerControlServer(t, handler)
			args := append([]string{"--socket", socket}, tc.args...)
			if code := runControl(args); code != 0 {
				t.Fatalf("runControl(%v) = %d, want 0", tc.args, code)
			}
			if len(handler.calls) != 1 || handler.calls[0] != tc.want {
				t.Fatalf("handler calls = %v, want [%s]", handler.calls, tc.want)
			}
		})
	}
}
