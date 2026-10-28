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

// TestAvenorSpawnFailClosedOnUnsupportedVerb pins that a supervisor without
// the gated spawn_mcp verb gets no fallback to the ungated one, wrapper or not:
// the plain verb cannot gate either the launch fields or the wrapper.
func TestAvenorSpawnFailClosedOnUnsupportedVerb(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cl            ControlClient
		launchWrapper []string
		want          string
	}{
		{"rpc error code, wrapped", &failingSpawnMCPClient{}, []string{"nice", "-n", "5"}, "launch_wrapper: disabled for MCP on this supervisor"},
		{"string error, wrapped", &stringMethodNotFoundClient{}, []string{"nice", "-n", "5"}, "launch_wrapper: disabled for MCP on this supervisor"},
		{"rpc error code, unwrapped", &failingSpawnMCPClient{}, nil, "spawn: this supervisor predates the MCP-gated spawn_mcp verb"},
		{"string error, unwrapped", &stringMethodNotFoundClient{}, nil, "spawn: this supervisor predates the MCP-gated spawn_mcp verb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: tc.cl})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
				RepoDir:       "/tmp/test-repo",
				LaunchWrapper: tc.launchWrapper,
			})
			if err == nil {
				t.Fatal("spawn error = nil, want a disabled error")
			}
			if err.Error() != tc.want {
				t.Fatalf("spawn error = %q, want %q", err, tc.want)
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
// through and the params it forwarded, succeeding on both paths.
type routingSpyClient struct {
	fakeClient
	spawnCalls    int
	spawnMCPCalls int
	spawnMCPArgs  []map[string]any
}

func (f *routingSpyClient) Spawn(params map[string]any) (map[string]any, error) {
	f.spawnCalls++
	return map[string]any{"runtime_id": "rt_route", "session_id": "ses_route"}, nil
}

func (f *routingSpyClient) SpawnMCP(params map[string]any) (map[string]any, error) {
	f.spawnMCPCalls++
	f.spawnMCPArgs = append(f.spawnMCPArgs, params)
	return map[string]any{"runtime_id": "rt_route", "session_id": "ses_route"}, nil
}

// TestAvenorSpawnAlwaysUsesGatedVerb pins the spawn verb routing: every MCP
// spawn goes through the gated spawn_mcp verb, wrapper or not. A request that
// took the plain spawn verb would skip the launch-field gate as well as the
// wrapper allowlist, so an unwrapped request carrying a caller-written roster
// must arrive at the gated verb with its selectors intact.
func TestAvenorSpawnAlwaysUsesGatedVerb(t *testing.T) {
	for _, tc := range []struct {
		name string
		args spawnArgs
	}{
		{"unwrapped", spawnArgs{RepoDir: "/tmp/test-repo"}},
		{"roster selectors", spawnArgs{RepoDir: "/tmp/test-repo", RosterFile: "/tmp/caller-written-roster.json", RosterEntry: "tool"}},
		{"wrapped", spawnArgs{RepoDir: "/tmp/test-repo", LaunchWrapper: []string{"nice", "-n", "5"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := &routingSpyClient{}
			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = s.handleAvenorSpawn(context.Background(), nil, tc.args)
			if err != nil {
				t.Fatalf("spawn error = %v, want nil", err)
			}
			if cl.spawnCalls != 0 || cl.spawnMCPCalls != 1 {
				t.Fatalf("routing: Spawn calls = %d (want 0), SpawnMCP calls = %d (want 1)",
					cl.spawnCalls, cl.spawnMCPCalls)
			}
			if tc.args.RosterFile != "" {
				params := cl.spawnMCPArgs[0]
				if params["roster_file"] != tc.args.RosterFile || params["roster_entry"] != tc.args.RosterEntry {
					t.Fatalf("forwarded params = %v, want the caller's roster_file and roster_entry carried verbatim", params)
				}
			}
		})
	}
}
