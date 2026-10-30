package pony

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/control/controltest"
)

// controlStub records the owner-gated calls the executor reaches.
type controlStub struct {
	control.StableHandler
	spawned  []string
	prompted []string
}

func (s *controlStub) Spawn(json.RawMessage) (any, error) {
	s.spawned = append(s.spawned, "spawn")
	return map[string]any{"runtime_id": "rt_child"}, nil
}

func (s *controlStub) RuntimePrompt(runtimeID, text, _ string) error {
	s.prompted = append(s.prompted, runtimeID+":"+text)
	return nil
}

// startControlSocket starts a control server that published its owner record.
func startControlSocket(t *testing.T, stub control.StableHandler) string {
	t.Helper()
	srv, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	srv.SetStableHandler(stub)
	dir, err := os.MkdirTemp("", "pony")
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

// TestPonyExecutorAttachesAsOwner proves the local executor still reaches the
// gated spawn and prompt verbs.
func TestPonyExecutorAttachesAsOwner(t *testing.T) {
	t.Parallel()
	stub := &controlStub{}
	socket := startControlSocket(t, stub)
	exec, err := NewControlSocketExecutor(socket)
	if err != nil {
		t.Fatalf("NewControlSocketExecutor: %v", err)
	}
	ctx := context.Background()
	runtimeID, err := exec.SpawnAgent(ctx, map[string]any{"prompt": "work"})
	if err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if runtimeID != "rt_child" {
		t.Fatalf("SpawnAgent = %q, want rt_child", runtimeID)
	}
	if err := exec.SendPrompt(ctx, runtimeID, "more", ""); err != nil {
		t.Fatalf("SendPrompt: %v", err)
	}
	if len(stub.spawned) != 1 || len(stub.prompted) != 1 || stub.prompted[0] != "rt_child:more" {
		t.Fatalf("spawned = %v prompted = %v", stub.spawned, stub.prompted)
	}
}

// TestPonyExecutorForeignTargetRejected proves the executor refuses a socket it
// cannot prove is the operator's owner: a missing record is an error, never a
// credential-free fallback that would later fail on a gated verb.
func TestPonyExecutorForeignTargetRejected(t *testing.T) {
	t.Parallel()
	socket := startControlSocket(t, &controlStub{})
	if err := os.Remove(control.OwnerRecordPath(socket)); err != nil {
		t.Fatal(err)
	}
	if _, err := NewControlSocketExecutor(socket); err == nil {
		t.Fatal("executor attached to a socket with no owner record")
	}
}
