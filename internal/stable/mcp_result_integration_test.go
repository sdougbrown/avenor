// This file holds the deliberate cross-boundary integration tests in the
// stable package: they import internal/mcpserver (a surface) so stable's test
// binary links a surface binary. They prove two properties of the MCP surface
// against a real supervisor and a real control socket:
//
//   - a runtime result carries the per-turn sentinel_file through control to
//     the MCP result tool: a run spawned with its own sentinel file and
//     per-turn event log answers with its complete output and no truncation
//     flag, even after the event log is moved out from under the runtime;
//   - the MCP spawn tool refuses a roster entry's launch fields.
//
// New stable tests must not import internal/mcpserver; keep the surface
// boundary here.
package stable

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/mcpserver"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type resultAuthTransport struct{}

func (resultAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer test-token")
	return http.DefaultTransport.RoundTrip(req)
}

func TestMCPResultUsesRealSupervisorTurnOwnership(t *testing.T) {
	const runID = "11111111-1111-1111-1111-111111111111"
	output := strings.Repeat("complete output ", 3000)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "avenor-run-"+runID+".done")
	eventPath := filepath.Join(dir, "events.ndjson")
	provider := newItestProvider(itestAttempt("ses_mcp", itestSessionEnd("ses_mcp", "end_turn", output)))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))
	rt := h.spawn(map[string]any{"sentinel_file": sentinel, "on_event": eventPath, "label": "mcp-result"})
	h.waitTurn(rt)
	if err := os.Remove(eventPath); err != nil {
		t.Fatal(err)
	}
	cl := h.connectClient()
	server, err := mcpserver.NewServer(mcpserver.Options{Transport: "http", Addr: "127.0.0.1:0", AuthToken: "test-token", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	httpServer := httptest.NewServer(server.HTTPHandler())
	defer httpServer.Close()
	c := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil)
	session, err := c.Connect(context.Background(), &mcpsdk.StreamableClientTransport{Endpoint: httpServer.URL, HTTPClient: &http.Client{Transport: resultAuthTransport{}}, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: "result", Arguments: map[string]any{"run_id": runID, "wait": false}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("tool error: %#v", result.Content)
	}
	data, ok := result.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured content: %T", result.StructuredContent)
	}
	got, _ := data["output"].(string)
	if got != output || data["output_truncated"] == true {
		t.Fatalf("result output length = %d, truncated = %v", len(got), data["output_truncated"])
	}
}

// resultToolText returns the single text content of a tool result.
func resultToolText(t *testing.T, res *mcpsdk.CallToolResult) string {
	t.Helper()
	if len(res.Content) != 1 {
		t.Fatalf("content parts = %d, want 1", len(res.Content))
	}
	text, ok := res.Content[0].(*mcpsdk.TextContent)
	if !ok {
		t.Fatalf("content = %T, want text content", res.Content[0])
	}
	return text.Text
}

// TestMCPToolSpawnRefusesCallerChosenLaunch drives the surface the reviewed
// bypass used end to end: the MCP spawn tool, over the real control socket,
// with a roster file the caller wrote. The tool takes the gated verb for every
// request now, so the roster entry's command is refused and no provider is
// built.
func TestMCPToolSpawnRefusesCallerChosenLaunch(t *testing.T) {
	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search","command":"/opt/bin/evil-poo"}}`)
	var created atomic.Int32
	h := newItestHarness(t, Config{}, func(runtime.StartOptions, string) (runtime.Provider, error) {
		created.Add(1)
		return nil, errors.New("a refused spawn must not build a provider")
	})
	cl := h.connectClient()
	server, err := mcpserver.NewServer(mcpserver.Options{Transport: "http", Addr: "127.0.0.1:0", AuthToken: "test-token", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	httpServer := httptest.NewServer(server.HTTPHandler())
	defer httpServer.Close()
	c := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil)
	session, err := c.Connect(context.Background(), &mcpsdk.StreamableClientTransport{Endpoint: httpServer.URL, HTTPClient: &http.Client{Transport: resultAuthTransport{}}, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	result, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: "spawn", Arguments: map[string]any{
		"repo_dir":     dir,
		"prompt":       "hi",
		"roster_file":  rosterPath,
		"roster_entry": "tool",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("spawn tool = success, want the launch-field refusal")
	}
	if got := resultToolText(t, result); !strings.Contains(got, `roster entry "tool": launch fields are disabled for MCP spawns (command)`) {
		t.Fatalf("spawn tool error = %q, want the launch-field refusal", got)
	}
	if n := created.Load(); n != 0 {
		t.Fatalf("provider creations = %d, want 0", n)
	}
}
