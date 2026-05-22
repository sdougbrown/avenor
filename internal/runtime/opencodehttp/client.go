package opencodehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
)

// Client is an HTTP client for opencode serve.
type Client struct {
	baseURL    string
	httpClient *http.Client
	username   string
	password   string
}

// NewClient creates a new Client.
func NewClient(opts ClientOptions) *Client {
	return &Client{
		baseURL:    strings.TrimRight(opts.BaseURL, "/"),
		httpClient: &http.Client{},
		username:   opts.Username,
		password:   opts.Password,
	}
}

// Health checks if the server is reachable.
func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/global/health", nil)
	if err != nil {
		return err
	}
	c.setAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check: %s", resp.Status)
	}
	return nil
}

// CreateSession creates a new session and returns its ID.
func (c *Client) CreateSession(ctx context.Context) (string, error) {
	body := bytes.NewReader([]byte(`{}`))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/session", body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("create session: %s", resp.Status)
	}
	var session struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		return "", err
	}
	return session.ID, nil
}

// GetSession fetches session metadata (used for resume).
func (c *Client) GetSession(ctx context.Context, sessionID string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.sessionURL(sessionID), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	c.setAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get session: %s", resp.Status)
	}
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

// SendMessage sends a prompt message to a session.
func (c *Client) SendMessage(ctx context.Context, sessionID string, payload map[string]any) (map[string]any, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.sessionURL(sessionID)+"/message", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return nil, fmt.Errorf("send message: %s: %s", resp.Status, string(body))
	}
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

// Abort cancels a running message on a session.
func (c *Client) Abort(ctx context.Context, sessionID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.sessionURL(sessionID)+"/abort", nil)
	if err != nil {
		return err
	}
	c.setAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("abort: %s", resp.Status)
	}
	return nil
}

// AnswerPermission responds to a permission request.
func (c *Client) AnswerPermission(ctx context.Context, sessionID, permissionID string, payload map[string]any) error {
	if reply, _ := payload["reply"].(string); reply != "" {
		err := c.postJSON(ctx, c.baseURL+"/permission/"+url.PathEscape(permissionID)+"/reply", map[string]any{"reply": reply})
		if err == nil {
			return nil
		}
		log.Printf("permission reply endpoint failed: %v; falling back to session endpoint", err)
	}
	// Defensive fallback for direct Client callers who don't set "response".
	// Provider.AnswerPermission always passes both "reply" and "response",
	// so this branch is only reachable for non-Provider callers.
	if _, ok := payload["response"]; !ok {
		if reply, _ := payload["reply"].(string); reply != "" {
			payload["response"] = reply
		}
	}
	return c.answerSessionPermission(ctx, sessionID, permissionID, payload)
}

func (c *Client) answerSessionPermission(ctx context.Context, sessionID, permissionID string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.sessionURL(sessionID)+"/permissions/"+url.PathEscape(permissionID), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("answer permission: %s", resp.Status)
	}
	return nil
}

func (c *Client) postJSON(ctx context.Context, url string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("post json: %s", resp.Status)
	}
	return nil
}

// StreamEvents connects to the SSE event stream.
func (c *Client) StreamEvents(ctx context.Context) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/event", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	c.setAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("stream events: %s", resp.Status)
	}
	return resp.Body, nil
}

func (c *Client) setAuth(req *http.Request) {
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}
}

func (c *Client) sessionURL(sessionID string) string {
	return c.baseURL + "/session/" + url.PathEscape(sessionID)
}
