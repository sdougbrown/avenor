package mcpserver

import (
	"context"
	"encoding/json"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// handleAvenorPeers is a thin wrapper over the one peers executor. This surface
// owns only its rendering: the list block, the rename triple, or last's text
// plus structured object.
func (s *Server) handleAvenorPeers(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	raw := toolArguments(req)
	provider, cleanup, err := s.messageSurfaceProvider(ctx, s.messageSupervisorPath(raw))
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
// The rendered sentence is the tool's text content and the result object is its
// structured content, matching the other surfaces that answer this tool.
func (s *Server) handleAvenorMessage(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	raw := toolArguments(req)
	provider, cleanup, err := s.messageSurfaceProvider(ctx, s.messageSupervisorPath(raw))
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
	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: result.Text}},
		StructuredContent: result.Output(),
	}, nil
}

// toolErrorResult renders one router tool error the way the two child surfaces
// do: the rendered sentence as content, the same object as structured content,
// and the error flag set so a client branching on it sees the failure. Any
// other error goes back to the JSON-RPC layer.
func toolErrorResult(err error) (*mcp.CallToolResult, error) {
	object, text, ok := router.ToolFailure(err)
	if !ok {
		return nil, err
	}
	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: text}},
		StructuredContent: object,
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
// spawned on a non-default supervisor stays reachable through the provider. A
// key that identifies more than one run binds nothing, so the call answers on
// the default supervisor rather than on whichever entry a map iteration
// reached first. The registry is in-process memory, so the binding is not
// restored with the server: after an MCP restart a run on a non-default
// supervisor is addressable only once a status call naming that supervisor_id
// has registered it again.
func (s *Server) messageSupervisorPath(raw json.RawMessage) string {
	for _, key := range router.SupervisorKeys(raw) {
		// The router substitutes its reserved self alias with the caller's run
		// id before a provider sees it, so IsReservedTarget keeps that alias,
		// the empty key and the supervisor's own run id from binding a
		// supervisor.
		if router.IsReservedTarget(key) {
			continue
		}
		if supervisorID := s.supervisorForPeerKey(key); supervisorID != "" {
			return supervisorID
		}
	}
	return ""
}

// supervisorForRunKey resolves a run id or label to the supervisor that
// registered it, and returns "" when the key is unknown or two supervisors both
// claim it. It is the ladder every surface that binds by run key shares.
func (s *Server) supervisorForRunKey(key string) string {
	if info := s.registry.LookupUnique(key); info != nil {
		return info.SupervisorID
	}
	return ""
}

// supervisorForPeerKey resolves a key the peers and message tools accept, which
// may name a runtime id as well as a run id or label. Runtime ids are minted per
// supervisor, so LookupRuntimeUnique only answers when exactly one supervisor
// owns the id and the caller falls back deterministically otherwise.
func (s *Server) supervisorForPeerKey(key string) string {
	if supervisorID := s.supervisorForRunKey(key); supervisorID != "" {
		return supervisorID
	}
	if info := s.registry.LookupRuntimeUnique(key); info != nil {
		return info.SupervisorID
	}
	return ""
}

// messageSurfaceProvider acquires the owner control client for the bound
// supervisor and binds it to the router. An empty binding means the default
// supervisor.
func (s *Server) messageSurfaceProvider(ctx context.Context, supervisorPath string) (*mcpMessageProvider, func(), error) {
	cl, cleanup, resolvedPath, err := s.getClientForSupervisorWithPath(supervisorPath)
	if err != nil {
		return nil, nil, err
	}
	provider, err := newMCPMessageProvider(ctx, s, cl, resolvedPath)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return provider, cleanup, nil
}
