package control

import (
	"encoding/json"
	"time"
)

type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	JSONRPC string     `json:"jsonrpc"`
	ID      any        `json:"id"`
	Result  any        `json:"result,omitempty"`
	Error   *RespError `json:"error,omitempty"`
}

type RespError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type Notification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// WorkflowHandler is the control-plane surface for workflow management. The
// Phase 2 workflow.Manager implements it directly.
type WorkflowHandler interface {
	WorkflowCreate(json.RawMessage) (any, error)
	WorkflowInstantiate(json.RawMessage) (any, error)
	WorkflowStatus(string) (any, error)
	WorkflowWait(string, time.Duration) (any, error)
	WorkflowInspect(string) (any, error)
	WorkflowEvents(string, int64, int) (any, error)
	WorkflowCommand(string, json.RawMessage) (any, error)
}

func success(id any, result any) Response {
	return Response{JSONRPC: "2.0", ID: id, Result: result}
}

func failure(id any, code int, message string, data any) Response {
	return Response{JSONRPC: "2.0", ID: id, Error: &RespError{Code: code, Message: message, Data: data}}
}
