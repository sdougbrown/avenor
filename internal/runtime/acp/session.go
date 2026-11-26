package acp

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"

	"github.com/sdougbrown/avenor/internal/events"
)

const ProbePrompt = "List the files in this directory and exit."

type Session struct {
	Client      *Client
	RunID       string
	BrokerToken string
	SessionID   string

	// chunks counts the agent_message_chunk updates dispatch has attributed to
	// this session; chunkBaseline is the count when the current prompt began.
	// PromptFailedPolicy implementations read the pair through StreamedContent.
	chunks        atomic.Uint64
	chunkBaseline atomic.Uint64
}

type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type MCPServer struct {
	Name    string   `json:"name"`
	Command string   `json:"command"`
	Args    []string `json:"args"`
	Env     []EnvVar `json:"env"`
}

// SessionOpenOptions is what a backend adds to session/new and session/resume.
// MCPServers is the ACP-standard server list and the only way a session gets
// MCP servers.
type SessionOpenOptions struct {
	MCPServers []MCPServer
}

func (c *Client) NewSession(ctx context.Context) (*Session, error) {
	return c.NewSessionWithOptions(ctx, SessionOpenOptions{})
}

func (c *Client) NewSessionWithOptions(ctx context.Context, opts SessionOpenOptions) (*Session, error) {
	result, err := c.request(ctx, "session/new", sessionOpenParams(c.dir, opts))
	if err != nil {
		return nil, err
	}

	var payload struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(result, &payload); err != nil {
		return nil, err
	}
	if payload.SessionID == "" {
		return nil, errors.New("session/new response missing sessionId")
	}

	session := &Session{Client: c, SessionID: payload.SessionID}
	c.registerSession(session)
	return session, nil
}

// registerSession records the session in the client's registry so dispatch
// can attribute session updates to it. Re-registration of a known ID is a
// no-op: the registry holds the first session for that ID, so a warm client
// resuming a session never orphans the counter an in-flight prompt reads.
func (c *Client) registerSession(session *Session) {
	if session.SessionID == "" {
		return
	}
	if _, ok := c.sessions.Load(session.SessionID); !ok {
		c.sessions.Store(session.SessionID, session)
	}
}

func (c *Client) LoadSession(ctx context.Context, sessionID string) (*Session, error) {
	_, err := c.request(ctx, "session/load", sessionOpenParams(c.dir, SessionOpenOptions{}, "sessionId", sessionID))
	if err != nil {
		return nil, err
	}
	session := &Session{Client: c, SessionID: sessionID}
	c.registerSession(session)
	return session, nil
}

func (c *Client) ResumeSession(ctx context.Context, sessionID string) (*Session, error) {
	return c.ResumeSessionWithOptions(ctx, sessionID, SessionOpenOptions{})
}

func (c *Client) ResumeSessionWithOptions(ctx context.Context, sessionID string, opts SessionOpenOptions) (*Session, error) {
	_, err := c.request(ctx, "session/resume", sessionOpenParams(c.dir, opts, "sessionId", sessionID))
	if err != nil {
		return nil, err
	}
	session := &Session{Client: c, SessionID: sessionID}
	c.registerSession(session)
	return session, nil
}

func (s *Session) Prompt(ctx context.Context, prompt string) (events.Event, error) {
	s.chunkBaseline.Store(s.chunks.Load())
	result, err := s.Client.request(ctx, "session/prompt", map[string]any{
		"sessionId": s.SessionID,
		"prompt": []map[string]any{
			{
				"type": "text",
				"text": prompt,
			},
		},
	})
	if err != nil {
		return events.Event{}, err
	}
	return mapPromptResponse(s.SessionID, result)
}

func (s *Session) Cancel() error {
	return s.Client.notify("session/cancel", map[string]any{
		"sessionId": s.SessionID,
	})
}

// StreamedContent reports whether the current prompt has streamed at least one
// agent message chunk. The count is taken when the client dispatches the
// update — the read loop's serial wire order — so the answer is complete the
// moment session/prompt returns: a chunk dispatched before the response is
// counted before the caller can observe the failure.
func (s *Session) StreamedContent() bool {
	return s.chunks.Load() > s.chunkBaseline.Load()
}

func (s *Session) Close(ctx context.Context) error {
	_, err := s.Client.request(ctx, "session/close", map[string]any{
		"sessionId": s.SessionID,
	})
	return err
}

func sessionOpenParams(dir string, opts SessionOpenOptions, extra ...any) map[string]any {
	params := map[string]any{
		"cwd":        dir,
		"mcpServers": []any{},
	}
	if len(opts.MCPServers) > 0 {
		servers := make([]MCPServer, len(opts.MCPServers))
		for i, srv := range opts.MCPServers {
			copied := MCPServer{
				Name:    srv.Name,
				Command: srv.Command,
				Args:    make([]string, len(srv.Args)),
				Env:     make([]EnvVar, len(srv.Env)),
			}
			copy(copied.Args, srv.Args)
			copy(copied.Env, srv.Env)
			servers[i] = copied
		}
		params["mcpServers"] = servers
	}
	for i := 0; i+1 < len(extra); i += 2 {
		key, ok := extra[i].(string)
		if !ok || key == "" {
			continue
		}
		params[key] = extra[i+1]
	}
	return params
}

func setSessionModeParams(sessionID string, modeID string) map[string]any {
	return map[string]any{
		"sessionId": sessionID,
		"modeId":    modeID,
	}
}

func setSessionConfigOptionParams(sessionID string, configID string, value string) map[string]any {
	return map[string]any{
		"sessionId": sessionID,
		"configId":  configID,
		"value":     value,
	}
}
