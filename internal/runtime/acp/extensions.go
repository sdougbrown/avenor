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
	if modelID == "" {
		return nil
	}
	_, err := c.request(ctx, "session/set_config_option", setSessionModelParams(sessionID, modelID))
	return err
}