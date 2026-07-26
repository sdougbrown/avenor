package permission

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type fakeProvider struct {
	response  runtime.PermissionResponse
	requestID string
	sessionID string
}

func (f *fakeProvider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	return runtime.Session{}, nil
}

func (f *fakeProvider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	return runtime.Session{}, nil
}

func (f *fakeProvider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	return nil
}

func (f *fakeProvider) Cancel(ctx context.Context, sessionID string) error {
	return nil
}

func (f *fakeProvider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	return nil, nil
}

func (f *fakeProvider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	f.sessionID = sessionID
	f.requestID = requestID
	f.response = response
	return nil
}

func (f *fakeProvider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

func TestFileHandlerRoundTrip(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "permission")
	handler := NewFileHandler(base)
	handler.Timeout = 2 * time.Second
	handler.PollInterval = 10 * time.Millisecond

	provider := &fakeProvider{}
	event := events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "42",
			"tool":       "bash",
			"question":   "Run command?",
			"options": []any{
				map[string]any{"optionId": "allow_fh", "kind": "allow"},
			},
		},
	}

	emitted := make(chan events.Event, 1)
	done := make(chan error, 1)
	resolved := make(chan Resolution, 1)
	go func() {
		res, err := handler.Handle(context.Background(), provider, event, func(event events.Event) error {
			emitted <- event
			return nil
		})
		resolved <- res
		done <- err
	}()

	reqPath := base + ".req"
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(reqPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	data, err := os.ReadFile(reqPath)
	if err != nil {
		t.Fatalf("read request file: %v", err)
	}
	var request FileRequest
	if err := json.Unmarshal(data, &request); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if request.RequestID != "42" || request.SessionID != "ses_1" || request.Tool != "bash" || request.Question != "Run command?" {
		t.Fatalf("request = %+v", request)
	}

	response := []byte(`{"outcome":"selected","option_id":"allow_fh"}`)
	if err := os.WriteFile(reqPath+".response", response, 0o600); err != nil {
		t.Fatalf("write response: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Handle returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler")
	}

	if provider.sessionID != "ses_1" || provider.requestID != "42" {
		t.Fatalf("answered session=%q request=%q", provider.sessionID, provider.requestID)
	}
	if !provider.response.Allow {
		t.Fatalf("response.Allow = false, want true")
	}
	if provider.response.OptionID != "allow_fh" {
		t.Fatalf("response.OptionID = %q, want allow_fh", provider.response.OptionID)
	}
	select {
	case res := <-resolved:
		if res.RequestID != "42" || res.OptionID != "allow_fh" {
			t.Fatalf("resolution = %+v", res)
		}
	default:
		t.Fatal("missing resolution")
	}

	select {
	case event := <-emitted:
		if event.Event != "permission.request" || event.Fields["request_id"] != "42" {
			t.Fatalf("emitted = %+v", event)
		}
	default:
		t.Fatal("permission.request was not emitted")
	}
}

func TestFileHandlerCancelledOutcomeDoesNotAnswerPermission(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "permission")
	handler := NewFileHandler(base)
	handler.Timeout = 2 * time.Second
	handler.PollInterval = 10 * time.Millisecond

	provider := &fakeProvider{}
	event := events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "42",
			"options": []any{
				map[string]any{"optionId": "allow_fh", "kind": "allow"},
			},
		},
	}

	done := make(chan struct {
		res Resolution
		err error
	}, 1)
	go func() {
		res, err := handler.Handle(context.Background(), provider, event, nil)
		done <- struct {
			res Resolution
			err error
		}{res: res, err: err}
	}()

	reqPath := base + ".req"
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(reqPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(reqPath); err != nil {
		t.Fatalf("request file was not created before response: %v", err)
	}
	if err := os.WriteFile(reqPath+".response", []byte(`{"outcome":"cancelled","option_id":"allow_fh"}`), 0o600); err != nil {
		t.Fatalf("write response: %v", err)
	}

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Handle returned error: %v", got.err)
		}
		if !got.res.Cancelled {
			t.Fatalf("Resolution.Cancelled = false, want true: %+v", got.res)
		}
		if provider.requestID != "" {
			t.Fatalf("AnswerPermission was called for request %q", provider.requestID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler")
	}
}

func TestReadResponseMapping(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name       string
		json       string
		options    any
		wantAllow  bool
		wantOptID  string
		wantErrMsg string
	}{
		{
			name:      "custom allow id maps to Allow=true",
			json:      `{"outcome":"selected","option_id":"allow_fh"}`,
			options:   []any{map[string]any{"optionId": "allow_fh", "kind": "allow"}},
			wantAllow: true,
			wantOptID: "allow_fh",
		},
		{
			name:      "reject id maps to Allow=false",
			json:      `{"outcome":"selected","option_id":"deny_fh"}`,
			options:   []any{map[string]any{"optionId": "deny_fh", "kind": "reject"}},
			wantAllow: false,
			wantOptID: "deny_fh",
		},
		{
			name:       "required write-in rejects empty message",
			json:       `{"outcome":"selected","option_id":"other"}`,
			options:    []any{map[string]any{"optionId": "other", "kind": "allow", "requiresMessage": true}},
			wantErrMsg: `permission option_id "other" requires a message`,
		},
		{
			name:      "required write-in accepts message",
			json:      `{"outcome":"selected","option_id":"other","message":"typed"}`,
			options:   []any{map[string]any{"optionId": "other", "kind": "allow", "requiresMessage": true}},
			wantAllow: true,
			wantOptID: "other",
		},
		{
			name:       "unknown option id errors",
			json:       `{"outcome":"selected","option_id":"custom_deny"}`,
			options:    []any{map[string]any{"optionId": "allow_fh", "kind": "allow"}},
			wantErrMsg: `unknown permission option_id "custom_deny"`,
		},
		{
			name:      "cancelled outcome is accepted",
			json:      `{"outcome":"cancelled","option_id":"allow_fh"}`,
			options:   []any{map[string]any{"optionId": "allow_fh", "kind": "allow"}},
			wantAllow: true,
			wantOptID: "allow_fh",
		},
		{
			name:       "invalid outcome errors",
			json:       `{"outcome":"ignored","option_id":"allow_fh"}`,
			options:    []any{map[string]any{"optionId": "allow_fh", "kind": "allow"}},
			wantErrMsg: `invalid permission response outcome "ignored"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(dir, "test.resp")
			if err := os.WriteFile(path, []byte(tt.json), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			raw, err := readResponse(path)
			if err != nil {
				if tt.wantErrMsg == "" {
					t.Fatalf("readResponse: %v", err)
				}
				if err.Error() != tt.wantErrMsg {
					t.Fatalf("readResponse error = %q, want %q", err.Error(), tt.wantErrMsg)
				}
				return
			}
			resp, err := permissionResponseFromRequest(tt.options, raw, raw.OptionID)
			if err != nil {
				if tt.wantErrMsg == "" {
					t.Fatalf("permissionResponseFromRequest: %v", err)
				}
				if err.Error() != tt.wantErrMsg {
					t.Fatalf("permissionResponseFromRequest error = %q, want %q", err.Error(), tt.wantErrMsg)
				}
				return
			}
			if resp.Allow != tt.wantAllow {
				t.Errorf("Allow = %v, want %v", resp.Allow, tt.wantAllow)
			}
			if resp.OptionID != tt.wantOptID {
				t.Errorf("response.OptionID = %q, want %q", resp.OptionID, tt.wantOptID)
			}
			if raw.OptionID != tt.wantOptID {
				t.Errorf("optionID = %q, want %q", raw.OptionID, tt.wantOptID)
			}
		})
	}
}

func TestFileHandlerPreservesMessage(t *testing.T) {
	dir := t.TempDir()
	permBase := filepath.Join(dir, "perm")

	// Write a request file with options.
	req := map[string]any{
		"options": []any{
			map[string]any{"optionId": "allow", "kind": "allow"},
			map[string]any{"optionId": "deny", "kind": "reject"},
		},
	}
	reqData, _ := json.Marshal(req)
	if err := os.WriteFile(permBase+".req", reqData, 0o600); err != nil {
		t.Fatal(err)
	}

	// Write a response with a message.
	resp := fileResponse{Outcome: "selected", OptionID: "allow", Message: "approved with caveat"}
	respData, _ := json.Marshal(resp)
	if err := os.WriteFile(permBase+".req.response", respData, 0o600); err != nil {
		t.Fatal(err)
	}

	raw, err := readResponse(permBase + ".req.response")
	if err != nil {
		t.Fatalf("readResponse: %v", err)
	}

	pr, err := permissionResponseFromRequest(req["options"], raw, "allow")
	if err != nil {
		t.Fatalf("permissionResponseFromRequest: %v", err)
	}
	if pr.Message != "approved with caveat" {
		t.Errorf("Message = %q, want 'approved with caveat'", pr.Message)
	}
}

func TestFileHandlerRejectsOversizedMessage(t *testing.T) {
	dir := t.TempDir()
	permBase := filepath.Join(dir, "perm")

	req := map[string]any{
		"options": []any{
			map[string]any{"optionId": "allow", "kind": "allow"},
		},
	}
	reqData, _ := json.Marshal(req)
	if err := os.WriteFile(permBase+".req", reqData, 0o600); err != nil {
		t.Fatal(err)
	}

	longMsg := strings.Repeat("x", runtime.MaxPermissionMessageBytes+1)
	resp := fileResponse{Outcome: "selected", OptionID: "allow", Message: longMsg}
	respData, _ := json.Marshal(resp)
	if err := os.WriteFile(permBase+".req.response", respData, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := readResponse(permBase + ".req.response"); err == nil {
		t.Fatal("oversized message should be rejected during strict decode")
	}
}
