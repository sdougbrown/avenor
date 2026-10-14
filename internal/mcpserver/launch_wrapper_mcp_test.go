package mcpserver

import (
	"context"
	"errors"
	"testing"

	"github.com/sdougbrown/avenor/client"
)

// failingSpawnMCPClient simulates an older supervisor that does not know the
// spawn_mcp verb, while recording any ordinary spawn attempts.
type failingSpawnMCPClient struct {
	fakeClient
	spawnCalls int
}

func (f *failingSpawnMCPClient) SpawnMCP(params map[string]any) (map[string]any, error) {
	return nil, &client.RPCError{Code: -32601, Message: "method not found"}
}

func (f *failingSpawnMCPClient) Spawn(params map[string]any) (map[string]any, error) {
	f.spawnCalls++
	return nil, errors.New("ordinary spawn must not be reached")
}

// stringMethodNotFoundClient exercises the string-match fallback for control
// clients that surface method-not-found as a plain error.
type stringMethodNotFoundClient struct {
	fakeClient
	spawnCalls int
}

func (f *stringMethodNotFoundClient) SpawnMCP(params map[string]any) (map[string]any, error) {
	return nil, errors.New("rpc error [-32601]: method not found")
}

func (f *stringMethodNotFoundClient) Spawn(params map[string]any) (map[string]any, error) {
	f.spawnCalls++
	return nil, errors.New("ordinary spawn must not be reached")
}

func TestAvenorSpawnFailClosedOnUnsupportedVerb(t *testing.T) {
	for _, tc := range []struct {
		name string
		cl   ControlClient
	}{
		{"rpc error code", &failingSpawnMCPClient{}},
		{"string error", &stringMethodNotFoundClient{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: tc.cl})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
				RepoDir:       "/tmp/test-repo",
				LaunchWrapper: []string{"nice", "-n", "5"},
			})
			if err == nil {
				t.Fatal("spawn error = nil, want disabled error")
			}
			if err.Error() != "launch_wrapper: disabled for MCP on this supervisor" {
				t.Fatalf("spawn error = %q, want exact disabled error", err)
			}
			switch cl := tc.cl.(type) {
			case *failingSpawnMCPClient:
				if cl.spawnCalls != 0 {
					t.Fatalf("ordinary Spawn called %d times, want 0 (no fallback)", cl.spawnCalls)
				}
			case *stringMethodNotFoundClient:
				if cl.spawnCalls != 0 {
					t.Fatalf("ordinary Spawn called %d times, want 0 (no fallback)", cl.spawnCalls)
				}
			}
		})
	}
}

// TestAvenorSpawnMethodNotFoundUnwrappedStillErrors proves the fail-closed
// translation only applies to wrapped requests: an unwrapped spawn on an old
// supervisor surfaces the underlying error.
func TestAvenorSpawnMethodNotFoundUnwrappedStillErrors(t *testing.T) {
	cl := &failingSpawnMCPClient{}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{RepoDir: "/tmp/test-repo"})
	if err == nil || err.Error() != "spawn: rpc error [-32601]: method not found" {
		t.Fatalf("spawn error = %v, want underlying method-not-found error", err)
	}
}
