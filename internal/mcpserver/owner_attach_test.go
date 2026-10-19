package mcpserver

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/control/controltest"
)

// ownerStub records the owner-gated control methods an MCP tool reaches.
type ownerStub struct {
	control.StableHandler
	calls []string
}

func (s *ownerStub) record(method string) { s.calls = append(s.calls, method) }

func (s *ownerStub) Spawn(json.RawMessage) (any, error) {
	s.record("spawn")
	return map[string]any{"runtime_id": "rt_spawned"}, nil
}

func (s *ownerStub) SpawnMCP(json.RawMessage) (any, error) {
	s.record("spawn_mcp")
	return map[string]any{"runtime_id": "rt_spawned"}, nil
}

func (s *ownerStub) RuntimeStatus(string) (any, error) {
	return map[string]any{"status": "running"}, nil
}

func (s *ownerStub) RuntimeCancel(runtimeID string) error {
	s.record("cancel:" + runtimeID)
	return nil
}

func (s *ownerStub) RuntimeAnswerPermission(runtimeID, requestID, optionID, _ string) error {
	s.record("answer_permission:" + runtimeID + ":" + requestID + ":" + optionID)
	return nil
}

func (s *ownerStub) Shutdown(string) error {
	s.record("shutdown")
	return nil
}

// TestMCPControlToolsAttachAsOwner proves the MCP control tools still reach
// their owner-gated verbs now that a bare connection carries no authority.
func TestMCPControlToolsAttachAsOwner(t *testing.T) {
	stub := &ownerStub{}
	srv, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	srv.SetStableHandler(stub)
	dir, err := os.MkdirTemp("", "mcps")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "control.sock")
	if err := srv.Start(socket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	s, err := NewServer(Options{Transport: "stdio", SupervisorSocket: socket, NoAutostart: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.registry.Store(&RunInfo{RunID: "run_1", RuntimeID: "rt_1", SupervisorID: socket}); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if _, _, err := s.handleAvenorSpawn(ctx, nil, spawnArgs{RepoDir: dir, Prompt: "work"}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if _, _, err := s.handleAvenorSpawn(ctx, nil, spawnArgs{RepoDir: dir, Prompt: "work", LaunchWrapper: []string{"nice"}}); err != nil {
		t.Fatalf("spawn_mcp: %v", err)
	}
	if _, _, err := s.handleAvenorKill(ctx, nil, killArgs{RunID: "run_1"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, _, err := s.handleAvenorAnswerPermission(ctx, nil, permissionArgs{RunID: "run_1", RequestID: "req_1", OptionID: "allow"}); err != nil {
		t.Fatalf("answer_permission: %v", err)
	}
	if _, _, err := s.handleAvenorShutdown(ctx, nil, shutdownArgs{}); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	want := []string{"spawn", "spawn_mcp", "cancel:rt_1", "answer_permission:rt_1:req_1:allow", "shutdown"}
	if len(stub.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", stub.calls, want)
	}
	for i := range want {
		if stub.calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", stub.calls, want)
		}
	}
}
