package codexappserver

import "encoding/json"

// Transport layer

type rpcMessage struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// thread/start

type threadStartParams struct {
	CWD   string `json:"cwd,omitempty"`
	Model string `json:"model,omitempty"`
}

type threadStartResult struct {
	Thread struct {
		ID string `json:"id"`
	} `json:"thread"`
}

// thread/resume

type threadResumeParams struct {
	ThreadID string `json:"threadId"`
}

// turn/start

type turnStartParams struct {
	ThreadID string      `json:"threadId"`
	Input    []inputPart `json:"input"`
	Effort   string      `json:"effort,omitempty"`
}

type inputPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type turnStartResult struct {
	Turn struct {
		ID string `json:"id"`
	} `json:"turn"`
}

// turn/interrupt

type turnInterruptParams struct {
	ThreadID string `json:"threadId"`
	TurnID   string `json:"turnId"`
}

// turn/completed notification

type turnCompletedNotification struct {
	ThreadID string `json:"threadId"`
	Turn     struct {
		ID     string     `json:"id"`
		Status string     `json:"status"` // "completed" | "interrupted" | "failed"
		Error  *turnError `json:"error,omitempty"`
	} `json:"turn"`
}

type turnError struct {
	Message string `json:"message"`
}

// item notifications (minimal — only fields needed for routing)

type itemNotification struct {
	ThreadID string `json:"threadId"`
	TurnID   string `json:"turnId"`
}

// Approval server requests

type approvalRequestParams struct {
	ThreadID string `json:"threadId"`
	TurnID   string `json:"turnId,omitempty"`
	ItemID   string `json:"itemId,omitempty"`
	Command  string `json:"command,omitempty"`
	Path     string `json:"path,omitempty"`
	Message  string `json:"message,omitempty"`
	Summary  string `json:"summary,omitempty"`
}

type approvalDecision struct {
	Decision string `json:"decision"` // "accept" | "decline"
}
