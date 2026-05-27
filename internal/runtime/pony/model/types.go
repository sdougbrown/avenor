package model

import "encoding/json"

// MessageRole distinguishes system, user, assistant, and tool messages.
type MessageRole string

const (
	RoleSystem    MessageRole = "system"
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleTool      MessageRole = "tool"
)

// Message is a single turn in the conversation history.
type Message struct {
	Role       MessageRole `json:"role"`
	Content    string      `json:"content"`
	ToolCallID string      `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall  `json:"tool_calls,omitempty"`
}

// ToolCall represents a tool invocation from the model.
type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// ToolDef is a JSON-schema tool definition sent to the model.
type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// ChunkType discriminates the kind of content in a stream chunk.
type ChunkType string

const (
	ChunkTypeText          ChunkType = "text"
	ChunkTypeReasoning     ChunkType = "reasoning"
	ChunkTypeToolCallDelta ChunkType = "tool_call_delta"
	ChunkTypeFinish        ChunkType = "finish"
	ChunkTypeError         ChunkType = "error"
	ChunkTypeUsage         ChunkType = "usage"
)

// ToolCallDelta is a partial or complete tool call from the stream.
type ToolCallDelta struct {
	ID        string          `json:"id,omitempty"`
Index     int             `json:"index"`
	Name      string          `json:"name,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// FinishReason is the terminal state of a generation.
type FinishReason struct {
	Reason string `json:"reason"`
}

// Usage reports token consumption when available.
type Usage struct {
	InputTokens  int `json:"input_tokens,omitempty"`
	OutputTokens int `json:"output_tokens,omitempty"`
}

// Request is the canonical request sent to the model adapter.
type Request struct {
	Model     string     `json:"model"`
	Messages  []Message  `json:"messages"`
	Tools     []ToolDef  `json:"tools,omitempty"`
	Stream    bool       `json:"stream"`
	MaxTokens int        `json:"max_tokens,omitempty"`
}

// Chunk is a single item from the adapter's stream channel.
type Chunk struct {
	Type             ChunkType       `json:"type"`
	Text             string          `json:"text,omitempty"`
	ReasoningContent string          `json:"reasoning_content,omitempty"`
	ToolCall         *ToolCallDelta  `json:"tool_call,omitempty"`
	Finish           *FinishReason   `json:"finish,omitempty"`
	Usage            *Usage          `json:"usage,omitempty"`
	Err              error           `json:"-"`
}
