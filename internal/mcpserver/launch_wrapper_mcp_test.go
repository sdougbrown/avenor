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

// routingSpyClient records which spawn verb the server routes a request
// through, succeeding on both paths.
type routingSpyClient struct {
	fakeClient
	spawnCalls    int
	spawnMCPCalls int
}

func (f *routingSpyClient) Spawn(params map[string]any) (map[string]any, error) {
	f.spawnCalls++
	return map[string]any{"runtime_id": "rt_route", "session_id": "ses_route"}, nil
}

func (f *routingSpyClient) SpawnMCP(params map[string]any) (map[string]any, error) {
	f.spawnMCPCalls++
	return map[string]any{"runtime_id": "rt_route", "session_id": "ses_route"}, nil
}

// TestAvenorSpawnRoutesByLaunchWrapper pins the spawn verb routing: an
// unwrapped request goes through the plain spawn verb (which pre-spawn_mcp
// supervisors have), while a wrapped request goes through the gated spawn_mcp
// verb.
func TestAvenorSpawnRoutesByLaunchWrapper(t *testing.T) {
	for _, tc := range []struct {
		name          string
		launchWrapper []string
		wantSpawn     int
		wantSpawnMCP  int
	}{
		{"unwrapped routes to spawn", nil, 1, 0},
		{"wrapped routes to spawn_mcp", []string{"nice", "-n", "5"}, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := &routingSpyClient{}
			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
				RepoDir:       "/tmp/test-repo",
				LaunchWrapper: tc.launchWrapper,
			})
			if err != nil {
				t.Fatalf("spawn error = %v, want nil", err)
			}
			if cl.spawnCalls != tc.wantSpawn || cl.spawnMCPCalls != tc.wantSpawnMCP {
				t.Fatalf("routing: Spawn calls = %d (want %d), SpawnMCP calls = %d (want %d)",
					cl.spawnCalls, tc.wantSpawn, cl.spawnMCPCalls, tc.wantSpawnMCP)
			}
		})
	}
}
