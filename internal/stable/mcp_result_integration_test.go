// This file is the one deliberate cross-boundary integration test in the
// stable package: it imports internal/mcpserver (a surface) so stable's test
// binary links a surface binary. It proves that a runtime result carries the
// per-turn sentinel_file through control to the MCP result surface: a run
// spawned with its own sentinel file and per-turn event log answers the MCP
// `result` tool with its complete output and no truncation flag, even after
// the event log is moved out from under the runtime. New stable tests must
// not import internal/mcpserver; keep the surface boundary here.
package stable

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/mcpserver"
	"github.com/sdougbrown/avenor/internal/runstate"
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
	// waitTurn returns when the turn settles, which is before the success
	// presentation sets the terminal phase: the settle clears the phase and
	// the presentation sets it again. Wait for the terminal status so the
	// result tool reads a completed run rather than a mid-transition one.
	waitForTerminalStatus(t, h.sup, rt)
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

// waitForTerminalStatus blocks until the runtime's consumer-facing status is
// terminal. A settled turn clears its phase before the success presentation
// sets it again, so the raw status reads idle with no phase in between.
func waitForTerminalStatus(t *testing.T, sup *Supervisor, rtID string) {
	t.Helper()
	waitFor(t, "terminal status for "+rtID, func() bool {
		raw, err := sup.RuntimeStatus(rtID)
		if err != nil {
			return false
		}
		status, _ := raw.(map[string]any)
		rawStatus, _ := status["status"].(string)
		rawPhase, _ := status["phase"].(string)
		return runstate.IsTerminalStatus(runstate.RuntimeStatus(rawStatus)) ||
			runstate.Translate(runstate.RuntimeStatus(rawStatus), runstate.Phase(rawPhase)).TurnComplete
	})
}
