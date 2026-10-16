// Package modelapi makes direct model calls over OpenAI-compatible
// chat-completions endpoints, outside any agent runtime. It is the shared
// foundation for harness-free judgment steps (today the permission
// direct-model decider; later gates and loop markers), so its interface
// stays generic: messages in, completion text out, no caller-specific
// vocabulary.
package modelapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultTimeout bounds one completion call when the caller does not supply
// an HTTP client. Judgment calls must be bounded so a hung endpoint leaves
// the caller's failure path (bounded retries, then a pending human gate)
// instead of blocking forever.
const DefaultTimeout = 30 * time.Second

// Message is one chat-completions message.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Client calls one model on one OpenAI-compatible endpoint.
type Client struct {
	baseURL string
	apiKey  string
	model   string
	http    *http.Client
}

// New builds a client for a model on an OpenAI-compatible endpoint. baseURL
// is the API root (with or without a trailing /v1); apiKey authenticates as
// a bearer token; a nil httpClient gets DefaultTimeout.
func New(baseURL, apiKey, model string, httpClient *http.Client) (*Client, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		return nil, fmt.Errorf("modelapi: invalid base_url scheme: %s (must be http:// or https://)", baseURL)
	}
	if model == "" {
		return nil, fmt.Errorf("modelapi: model is required")
	}
	if !strings.Contains(baseURL, "/v1") {
		baseURL = baseURL + "/v1"
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: DefaultTimeout}
	}
	return &Client{
		baseURL: baseURL + "/chat/completions",
		apiKey:  apiKey,
		model:   model,
		http:    httpClient,
	}, nil
}

type completionRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature float64   `json:"temperature"`
	MaxTokens   int       `json:"max_tokens,omitempty"`
	Stream      bool      `json:"stream"`
}

type completionResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
}

// Complete performs one non-streaming chat completion and returns the
// message text of the first choice. A non-2xx response, a transport error,
// or a response without a choice is an error the caller treats like any
// other failed judgment call.
func (c *Client) Complete(ctx context.Context, messages []Message, maxTokens int) (string, error) {
	body, err := json.Marshal(completionRequest{
		Model:       c.model,
		Messages:    messages,
		Temperature: 0,
		MaxTokens:   maxTokens,
		Stream:      false,
	})
	if err != nil {
		return "", fmt.Errorf("modelapi: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("modelapi: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// Transport errors embed the full request URL; a registry base_url
		// may carry credentials in its query string, so redact everything
		// after the path before the error reaches any log.
		if uerr := (*url.Error)(nil); errors.As(err, &uerr) {
			if u, perr := url.Parse(uerr.URL); perr == nil {
				u.RawQuery, u.User, uerr.URL = "", nil, u.String()
			}
		}
		return "", fmt.Errorf("modelapi: call %s: %w", c.model, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("modelapi: call %s: %s: %s", c.model, resp.Status, strings.TrimSpace(string(snippet)))
	}
	var parsed completionResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&parsed); err != nil {
		return "", fmt.Errorf("modelapi: call %s: decode response: %w", c.model, err)
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("modelapi: call %s: response has no choices", c.model)
	}
	return parsed.Choices[0].Message.Content, nil
}
