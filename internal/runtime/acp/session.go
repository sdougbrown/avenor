package acp

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/sdougbrown/avenor/internal/events"
)

const ProbePrompt = "List the files in this directory and exit."

type Session struct {
	Client    *Client
	SessionID string
}

func (c *Client) NewSession(ctx context.Context) (*Session, error) {
	result, err := c.request(ctx, "session/new", sessionOpenParams(c.dir))
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

	return &Session{Client: c, SessionID: payload.SessionID}, nil
}

func (c *Client) LoadSession(ctx context.Context, sessionID string) (*Session, error) {
	_, err := c.request(ctx, "session/load", sessionOpenParams(c.dir, "sessionId", sessionID))
	if err != nil {
		return nil, err
	}
	return &Session{Client: c, SessionID: sessionID}, nil
}

func (c *Client) ResumeSession(ctx context.Context, sessionID string) (*Session, error) {
	_, err := c.request(ctx, "session/resume", sessionOpenParams(c.dir, "sessionId", sessionID))
	if err != nil {
		return nil, err
	}
	return &Session{Client: c, SessionID: sessionID}, nil
}

func (s *Session) Prompt(ctx context.Context, prompt string) (events.Event, error) {
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

func (s *Session) Close(ctx context.Context) error {
	_, err := s.Client.request(ctx, "session/close", map[string]any{
		"sessionId": s.SessionID,
	})
	return err
}

func sessionOpenParams(dir string, extra ...any) map[string]any {
	params := map[string]any{
		"cwd":        dir,
		"mcpServers": []any{},
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

func setSessionModelParams(sessionID string, modelID string) map[string]any {
	return map[string]any{
		"sessionId": sessionID,
		"configId":   "model",
		"value":      modelID,
	}
}