package pi

import "encoding/json"

type piLine struct {
	Type     string          `json:"type"`
	ID       string          `json:"id,omitempty"`
	Message  string          `json:"message,omitempty"`
	Success  bool            `json:"success,omitempty"`
	Method   string          `json:"method,omitempty"`
	Title    string          `json:"title,omitempty"`
	Options  json.RawMessage `json:"options,omitempty"`
	Result   json.RawMessage `json:"result,omitempty"`
	Error    *rpcError       `json:"error,omitempty"`
	Messages json.RawMessage `json:"messages,omitempty"`
	State    json.RawMessage `json:"state,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
