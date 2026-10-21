package mcpserver

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// peersArgs is the Go-side projection of the peers input contract. The tool is
// registered with the published JSON Schema (In is a map), so this struct
// exists to keep TestSchemaFieldParity honest about the wire field set.
type peersArgs struct {
	Action    string `json:"action,omitempty" jsonschema:"optional action: list (default), rename, or last"`
	Name      string `json:"name,omitempty" jsonschema:"optional new display name (rename)"`
	SessionID string `json:"session_id,omitempty" jsonschema:"optional run to rename; omitted renames the caller's own run"`
	Target    string `json:"target,omitempty" jsonschema:"optional peer to read the last message of (last)"`
}

// handleAvenorPeers is a thin wrapper over the one peers executor. This surface
// owns only its rendering: the list block, the rename triple, or last's text
// plus structured object.
func (s *Server) handleAvenorPeers(ctx context.Context, req *mcp.CallToolRequest, args map[string]any) (*mcp.CallToolResult, any, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, nil, err
	}
	provider, cleanup, err := s.messageSurfaceProvider()
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	result, err := router.ExecutePeers(ctx, provider, raw)
	if err != nil {
		var toolErr *router.ToolError
		if errors.As(err, &toolErr) {
			return nil, toolErr.Output(), nil
		}
		return nil, nil, err
	}
	return textAndStructured(router.PeersContent(result), router.PeersDetails(result)), router.PeersDetails(result), nil
}

// textAndStructured returns the surface's human text with the structured
// object, so `last`'s content is exactly router.Result.Text while `details`
// carries the object.
func textAndStructured(text string, details map[string]any) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: text}},
		StructuredContent: details,
	}
}

// handleAvenorMessage routes the merged message tool through the shared router.
// The result object is both the tool's text output and its structured result;
// a tool error returns the same object shape with only code and text set.
func (s *Server) handleAvenorMessage(ctx context.Context, req *mcp.CallToolRequest, args map[string]any) (*mcp.CallToolResult, any, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, nil, err
	}
	provider, cleanup, err := s.messageSurfaceProvider()
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	rt, err := router.NewRouter(provider)
	if err != nil {
		return nil, nil, err
	}
	result, err := rt.Execute(ctx, raw)
	if err != nil {
		var toolErr *router.ToolError
		if errors.As(err, &toolErr) {
			return nil, toolErr.Output(), nil
		}
		return nil, nil, err
	}
	return nil, result.Output(), nil
}

// messageSurfaceProvider acquires the owner control client for the default
// supervisor and binds it to the router.
func (s *Server) messageSurfaceProvider() (*mcpMessageProvider, func(), error) {
	cl, cleanup, supervisorPath, err := s.getClientForSupervisorWithPath("")
	if err != nil {
		return nil, nil, err
	}
	provider, err := newMCPMessageProvider(s, cl, supervisorPath)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return provider, cleanup, nil
}
