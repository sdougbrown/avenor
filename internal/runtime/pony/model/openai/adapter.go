package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/sdougbrown/avenor/internal/runtime/pony/model"
)

type Adapter struct {
	cfg          Config
	httpClient   *http.Client
	baseEndpoint string
}

func New(cfg Config) *Adapter {
	baseURL := cfg.BaseURL
	baseURL = strings.TrimRight(baseURL, "/")
	if !strings.Contains(baseURL, "/v1") {
		baseURL = baseURL + "/v1"
	}
	baseURL = baseURL + "/chat/completions"

	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	return &Adapter{
		cfg:          cfg,
		httpClient:   client,
		baseEndpoint: baseURL,
	}
}

func (a *Adapter) Name() string {
	return "openai"
}

func (a *Adapter) Stream(ctx context.Context, req model.Request) (<-chan model.Chunk, error) {
	ch := make(chan model.Chunk)

	go func() {
		defer close(ch)

		body, err := a.buildRequestBody(req)
		if err != nil {
			select {
			case ch <- model.Chunk{Type: model.ChunkTypeError, Err: err}:
			default:
			}
			return
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseEndpoint, body)
		if err != nil {
			select {
			case ch <- model.Chunk{Type: model.ChunkTypeError, Err: err}:
			default:
			}
			return
		}

		httpReq.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "text/event-stream")

		httpResp, err := a.httpClient.Do(httpReq)
		if err != nil {
			select {
			case ch <- model.Chunk{Type: model.ChunkTypeError, Err: err}:
			default:
			}
			return
		}
		defer httpResp.Body.Close()

		if httpResp.StatusCode != http.StatusOK {
			errMsg := fmt.Sprintf("HTTP %d", httpResp.StatusCode)
			bodyBytes, _ := io.ReadAll(httpResp.Body)
			if len(bodyBytes) > 0 {
				errMsg = string(bodyBytes)
			}
			select {
			case ch <- model.Chunk{Type: model.ChunkTypeError, Err: fmt.Errorf("%s", errMsg)}:
			default:
			}
			return
		}

		a.parseSSE(ctx, httpResp.Body, ch)
	}()

	return ch, nil
}

func (a *Adapter) buildRequestBody(req model.Request) (io.Reader, error) {
	openaiReq := openAIRequest{
		Model:    req.Model,
		Messages: make([]openAIMessage, len(req.Messages)),
		Stream:   true,
		Tools:    make([]openAITool, len(req.Tools)),
	}

	if req.MaxTokens > 0 {
		openaiReq.MaxTokens = req.MaxTokens
	}

	for i, m := range req.Messages {
		openaiReq.Messages[i] = openAIMessage{
			Role:       string(m.Role),
			Content:    m.Content,
			ToolCallID: m.ToolCallID,
			ToolCalls:  m.ToolCalls,
		}
	}

	for i, t := range req.Tools {
		openaiReq.Tools[i] = openAITool{
			Type: "function",
			Function: openAIFunction{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  t.InputSchema,
			},
		}
	}

	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(openaiReq)
	return buf, err
}

func (a *Adapter) parseSSE(ctx context.Context, reader io.Reader, ch chan<- model.Chunk) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}

		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}

		payload := strings.TrimPrefix(line, "data: ")
		payload = strings.TrimSpace(payload)

		if payload == "[DONE]" {
			return
		}

		var chunk openAIStreamChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			select {
			case ch <- model.Chunk{Type: model.ChunkTypeError, Err: fmt.Errorf("failed to parse SSE chunk: %w", err)}:
			default:
			}
			return
		}

		for _, choice := range chunk.Choices {
			c := payloadParser{
				ch: ch,
			}

			if usage := chunk.Usage; usage != nil {
				select {
				case ch <- model.Chunk{
					Type:  model.ChunkTypeUsage,
					Usage: &model.Usage{
						InputTokens:  usage.InputTokens,
						OutputTokens: usage.OutputTokens,
					},
				}:
				default:
				}
			}

			if choice.FinishReason != nil && *choice.FinishReason != "" {
				select {
				case ch <- model.Chunk{
					Type:   model.ChunkTypeFinish,
					Finish: &model.FinishReason{Reason: *choice.FinishReason},
				}:
				default:
				}
			}

			c.processDelta(choice.Delta)
		}
	}

	if err := scanner.Err(); err != nil {
		select {
		case ch <- model.Chunk{Type: model.ChunkTypeError, Err: fmt.Errorf("SSE scan error: %w", err)}:
		default:
		}
		return
	}
}

type payloadParser struct {
	ch chan<- model.Chunk
}

func (p *payloadParser) processDelta(delta openAIDelta) {
	if delta.Content != "" {
		select {
		case p.ch <- model.Chunk{Type: model.ChunkTypeText, Text: delta.Content}:
		default:
		}
	}

	for _, tc := range delta.ToolCalls {
		tcd := &model.ToolCallDelta{
			Index: tc.Index,
		}

		if tc.ID != "" {
			tcd.ID = tc.ID
		}

		if tc.Function.Name != "" {
			tcd.Name = tc.Function.Name
		}

		if tc.Function.Arguments != "" {
			tcd.Arguments = json.RawMessage(tc.Function.Arguments)
		}

		select {
		case p.ch <- model.Chunk{Type: model.ChunkTypeToolCallDelta, ToolCall: tcd}:
		default:
		}
	}
}

type openAIRequest struct {
	Model     string            `json:"model"`
	Messages  []openAIMessage   `json:"messages"`
	Tools     []openAITool      `json:"tools,omitempty"`
	Stream    bool              `json:"stream"`
	MaxTokens int               `json:"max_tokens,omitempty"`
}

type openAIMessage struct {
	Role       string                 `json:"role"`
	Content    string                 `json:"content"`
	ToolCallID string                 `json:"tool_call_id,omitempty"`
	ToolCalls  []model.ToolCall       `json:"tool_calls,omitempty"`
}

type openAITool struct {
	Type     string           `json:"type"`
	Function openAIFunction   `json:"function"`
}

type openAIFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type openAIStreamChunk struct {
	Choices []openAIChoice `json:"choices"`
	Usage   *struct {
		InputTokens  int `json:"input_tokens,omitempty"`
		OutputTokens int `json:"output_tokens,omitempty"`
	} `json:"usage,omitempty"`
}

type openAIChoice struct {
	Delta        openAIDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
	Index        int         `json:"index"`
}

type openAIDelta struct {
	Role      string         `json:"role,omitempty"`
	Content   string         `json:"content,omitempty"`
	ToolCalls []openAIToolCall `json:"tool_calls,omitempty"`
}

type openAIToolCall struct {
	Index    int             `json:"index"`
	ID       string          `json:"id,omitempty"`
	Type     string          `json:"type,omitempty"`
	Function openAIToolDelta `json:"function,omitempty"`
}

type openAIToolDelta struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}
