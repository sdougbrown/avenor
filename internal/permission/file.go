package permission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

const (
	DefaultTimeout      = 10 * time.Minute
	DefaultPollInterval = 500 * time.Millisecond
)

// FileRequest is the JSON written to <base>.req.
//
// Example:
//
//	{
//	  "request_id": "17",
//	  "session_id": "ses_...",
//	  "tool": "bash",
//	  "question": "Run command?",
//	  "options": [{"optionId":"allow","kind":"allow"}],
//	  "payload": {"request_id":"17","tool":"bash","options":[...]}
//	}
//
// The operator or answer-jockey skill replies by writing <base>.req.response:
//
//	{"outcome":"selected","option_id":"allow","message":"optional note"}
type FileRequest struct {
	RequestID string         `json:"request_id"`
	SessionID string         `json:"session_id,omitempty"`
	Tool      string         `json:"tool,omitempty"`
	Question  string         `json:"question,omitempty"`
	Options   any            `json:"options,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
}

type FileHandler struct {
	BasePath     string
	Timeout      time.Duration
	PollInterval time.Duration
}

type Resolution struct {
	RequestID string
	OptionID  string
	Cancelled bool
}

func NewFileHandler(basePath string) *FileHandler {
	return &FileHandler{
		BasePath:     basePath,
		Timeout:      DefaultTimeout,
		PollInterval: DefaultPollInterval,
	}
}

func (h *FileHandler) Handle(ctx context.Context, provider runtime.Provider, event events.Event, emit func(events.Event) error) (Resolution, error) {
	if h == nil {
		return Resolution{}, errors.New("permission file handler is nil")
	}
	if h.BasePath == "" {
		return Resolution{}, errors.New("permission file handler path is required")
	}
	if provider == nil {
		return Resolution{}, errors.New("permission provider is required")
	}

	request := requestFromEvent(event)
	if request.RequestID == "" {
		return Resolution{}, errors.New("permission.request missing request_id")
	}

	requestPath := h.BasePath + ".req"
	responsePath := requestPath + ".response"
	_ = os.Remove(responsePath)

	data, err := json.MarshalIndent(request, "", "  ")
	if err != nil {
		return Resolution{}, err
	}
	data = append(data, '\n')
	if err := writeFileAtomic(requestPath, data, 0o600); err != nil {
		return Resolution{}, err
	}

	if emit != nil {
		if err := emit(normalizedPermissionEvent(request)); err != nil {
			return Resolution{}, err
		}
	}

	rawResponse, optionID, err := h.waitForResponse(ctx, responsePath)
	if err != nil {
		return Resolution{}, err
	}
	if rawResponse.Outcome == "cancelled" {
		return Resolution{RequestID: request.RequestID, OptionID: optionID, Cancelled: true}, nil
	}
	response, err := permissionResponseFromRequest(request.Options, rawResponse, optionID)
	if err != nil {
		return Resolution{}, err
	}
	if err := provider.AnswerPermission(ctx, event.SessionID, request.RequestID, response); err != nil {
		return Resolution{}, err
	}
	return Resolution{RequestID: request.RequestID, OptionID: optionID}, nil
}

func (h *FileHandler) waitForResponse(ctx context.Context, path string) (fileResponse, string, error) {
	timeout := h.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	interval := h.PollInterval
	if interval == 0 {
		interval = DefaultPollInterval
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		raw, err := readResponse(path)
		if err == nil {
			return raw, raw.OptionID, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fileResponse{}, "", err
		}

		select {
		case <-waitCtx.Done():
			return fileResponse{}, "", fmt.Errorf("wait for permission response: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

type fileResponse struct {
	Outcome  string `json:"outcome"`
	OptionID string `json:"option_id"`
	Message  string `json:"message,omitempty"`
}

func (r *fileResponse) UnmarshalJSON(data []byte) error {
	var wire struct {
		Outcome  string          `json:"outcome"`
		OptionID string          `json:"option_id"`
		Message  json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	message, err := runtime.DecodePermissionMessageJSON(wire.Message)
	if err != nil {
		return err
	}
	r.Outcome, r.OptionID, r.Message = wire.Outcome, wire.OptionID, message
	return nil
}

func readResponse(path string) (fileResponse, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return fileResponse{}, err
	}
	if len(data) == 0 {
		// File exists but is empty — likely a non-atomic writer mid-O_TRUNC.
		// Treat as not-yet-ready so waitForResponse keeps polling.
		return fileResponse{}, os.ErrNotExist
	}
	var raw fileResponse
	if err := json.Unmarshal(data, &raw); err != nil {
		return fileResponse{}, err
	}
	switch raw.Outcome {
	case "":
		raw.Outcome = "selected"
	case "selected", "cancelled":
	default:
		return fileResponse{}, fmt.Errorf("invalid permission response outcome %q", raw.Outcome)
	}
	return raw, nil
}

func permissionResponseFromRequest(options any, response fileResponse, optionID string) (runtime.PermissionResponse, error) {
	allow, requiresMessage, err := permissionOptionFromOptions(options, optionID)
	if err != nil {
		return runtime.PermissionResponse{}, err
	}
	if err := runtime.ValidatePermissionMessage(response.Message); err != nil {
		return runtime.PermissionResponse{}, err
	}
	if requiresMessage && response.Message == "" {
		return runtime.PermissionResponse{}, fmt.Errorf("permission option_id %q requires a message", optionID)
	}
	return runtime.PermissionResponse{
		Allow:    allow,
		OptionID: optionID,
		Message:  response.Message,
	}, nil
}

func permissionOptionFromOptions(options any, optionID string) (bool, bool, error) {
	list, ok := options.([]any)
	if !ok {
		return false, false, fmt.Errorf("permission request options must be an array, got %T", options)
	}
	for _, opt := range list {
		m, ok := opt.(map[string]any)
		if !ok {
			continue
		}
		oid, _ := m["optionId"].(string)
		if oid != optionID {
			continue
		}
		kind, _ := m["kind"].(string)
		requiresMessage, _ := m["requiresMessage"].(bool)
		switch NormalizeOptionKind(kind) {
		case "allow":
			return true, requiresMessage, nil
		case "reject":
			return false, requiresMessage, nil
		default:
			return false, false, fmt.Errorf("unsupported permission option kind %q for option_id %q", kind, optionID)
		}
	}
	return false, false, fmt.Errorf("unknown permission option_id %q", optionID)
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func requestFromEvent(event events.Event) FileRequest {
	fields := map[string]any{}
	for k, v := range event.Fields {
		fields[k] = v
	}
	requestID, _ := fields["request_id"].(string)
	tool, _ := fields["tool"].(string)
	question, _ := fields["question"].(string)
	if question == "" {
		question, _ = fields["message"].(string)
	}

	return FileRequest{
		RequestID: requestID,
		SessionID: event.SessionID,
		Tool:      tool,
		Question:  question,
		Options:   fields["options"],
		Payload:   fields,
	}
}

func normalizedPermissionEvent(request FileRequest) events.Event {
	fields := map[string]any{
		"request_id": request.RequestID,
	}
	if request.Tool != "" {
		fields["tool"] = request.Tool
	}
	if request.Question != "" {
		fields["question"] = request.Question
	}
	if request.Options != nil {
		fields["options"] = request.Options
	}
	return events.Event{
		Event:     "permission.request",
		SessionID: request.SessionID,
		Fields:    fields,
	}
}
