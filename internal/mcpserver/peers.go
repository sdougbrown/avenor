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
func (s *Server) handleAvenorPeers(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	raw := toolArguments(req)
	provider, cleanup, err := s.messageSurfaceProvider(s.messageSupervisorPath(raw))
	if err != nil {
		return nil, err
	}
	defer cleanup()
	result, err := router.ExecutePeers(ctx, provider, raw)
	if err != nil {
		return toolErrorResult(err)
	}
	details := router.PeersDetails(result)
	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: router.PeersContent(result)}},
		StructuredContent: details,
	}, nil
}

// handleAvenorMessage routes the merged message tool through the shared router.
// The result object is both the tool's text output and its structured result.
func (s *Server) handleAvenorMessage(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	raw := toolArguments(req)
	provider, cleanup, err := s.messageSurfaceProvider(s.messageSupervisorPath(raw))
	if err != nil {
		return nil, err
	}
	defer cleanup()
	rt, err := router.NewRouter(provider)
	if err != nil {
		return nil, err
	}
	result, err := rt.Execute(ctx, raw)
	if err != nil {
		return toolErrorResult(err)
	}
	object := result.Output()
	text, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: string(text)}},
		StructuredContent: object,
	}, nil
}

// toolErrorResult renders one router tool error the way the two child surfaces
// do: the rendered sentence as content, the same object as structured content,
// and the error flag set so a client branching on it sees the failure.
func toolErrorResult(err error) (*mcp.CallToolResult, error) {
	var toolErr *router.ToolError
	if !errors.As(err, &toolErr) {
		return nil, err
	}
	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: toolErr.Message}},
		StructuredContent: toolErr.Output(),
		IsError:           true,
	}, nil
}

// toolArguments returns the raw call arguments, treating an omitted arguments
// object as the empty one so the router applies the peers default action.
func toolArguments(req *mcp.CallToolRequest) json.RawMessage {
	if req == nil || req.Params == nil || len(req.Params.Arguments) == 0 {
		return json.RawMessage("{}")
	}
	return req.Params.Arguments
}

// messageSupervisorPath binds the merged tools to the supervisor that owns the
// run they address. Neither tool takes a model-facing supervisor_id; the
// binding comes from the same registry the run-addressed tools read, so a run
// spawned on a non-default supervisor stays reachable through the provider.
func (s *Server) messageSupervisorPath(raw json.RawMessage) string {
	var probe struct {
		Target    string `json:"target"`
		SessionID string `json:"session_id"`
	}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &probe)
	}
	for _, key := range []string{probe.Target, probe.SessionID} {
		if key == "" || key == supervisorRunID || key == selfAlias {
			continue
		}
		if info := s.registry.LookupUnique(key); info != nil && info.SupervisorID != "" {
			return info.SupervisorID
		}
		if info := s.registry.LookupRuntime(key); info != nil && info.SupervisorID != "" {
			return info.SupervisorID
		}
	}
	return ""
}

// messageSurfaceProvider acquires the owner control client for the bound
// supervisor and binds it to the router. An empty binding means the default
// supervisor.
func (s *Server) messageSurfaceProvider(supervisorPath string) (*mcpMessageProvider, func(), error) {
	cl, cleanup, resolvedPath, err := s.getClientForSupervisorWithPath(supervisorPath)
	if err != nil {
		return nil, nil, err
	}
	provider, err := newMCPMessageProvider(s, cl, resolvedPath)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return provider, cleanup, nil
}
