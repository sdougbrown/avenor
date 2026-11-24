package acp

import (
	"context"
)

func (c *Client) SetSessionMode(ctx context.Context, sessionID string, modeID string) error {
	if modeID == "" {
		return nil
	}
	_, err := c.request(ctx, "session/set_mode", setSessionModeParams(sessionID, modeID))
	return err
}

func (c *Client) SetSessionModel(ctx context.Context, sessionID string, modelID string) error {
	return c.SetSessionConfigOption(ctx, sessionID, "model", modelID)
}

// SetSessionConfigOption applies one ACP session config option (for example
// "model" or "thought_level") to an open session. An empty value is a no-op;
// the backend keeps whatever the session already selected.
func (c *Client) SetSessionConfigOption(ctx context.Context, sessionID string, configID string, value string) error {
	if value == "" {
		return nil
	}
	_, err := c.request(ctx, "session/set_config_option", setSessionConfigOptionParams(sessionID, configID, value))
	return err
}
