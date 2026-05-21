package acp

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestSelectPermissionOption(t *testing.T) {
	opts := func(pairs ...string) []any {
		out := make([]any, 0, len(pairs)/2)
		for i := 0; i+1 < len(pairs); i += 2 {
			out = append(out, map[string]any{"optionId": pairs[i], "kind": pairs[i+1]})
		}
		return out
	}

	tests := []struct {
		name    string
		options []any
		approve bool
		want    string
		wantErr string
	}{
		{
			name:    "approve picks allow-kind",
			options: opts("deny", "reject", "allow", "allow"),
			approve: true,
			want:    "allow",
		},
		{
			name:    "approve picks option with allow_once kind",
			options: opts("reject", "reject_once", "once", "allow_once"),
			approve: true,
			want:    "once",
		},
		{
			name:    "reject picks reject-kind",
			options: opts("allow", "allow", "deny", "reject"),
			approve: false,
			want:    "deny",
		},
		{
			name:    "reject picks deny_once kind",
			options: opts("allow", "allow", "x", "deny_once"),
			approve: false,
			want:    "x",
		},
		{
			name:    "reject picks deny_always kind",
			options: opts("allow", "allow", "y", "deny_always"),
			approve: false,
			want:    "y",
		},
		{
			name:    "reject picks reject-once kind",
			options: opts("always", "allow_always", "reject", "reject_once"),
			approve: false,
			want:    "reject",
		},
		{
			name:    "unknown kind returns error",
			options: opts("x1", "other"),
			approve: true,
			wantErr: `permission options missing kind "allow"`,
		},
		{
			name:    "empty options returns error",
			options: nil,
			approve: true,
			wantErr: `permission options missing kind "allow"`,
		},
		{
			name:    "reject missing returns error",
			options: opts("allow", "allow"),
			approve: false,
			wantErr: `permission options missing kind "reject"`,
		},
		{
			name:    "optionId empty string is an error",
			options: opts("", "allow"),
			approve: true,
			wantErr: `missing optionId`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectPermissionOption(tt.options, tt.approve)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("selectPermissionOption() error = nil, want error containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("selectPermissionOption() error = %v, want error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("selectPermissionOption() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("selectPermissionOption(approve=%v) = %q, want %q", tt.approve, got, tt.want)
			}
		})
	}
}

func TestAnswerPermissionMissingOptions(t *testing.T) {
	p := &Provider{}
	p.mu.Lock()
	p.client = &Client{}
	p.pendingOptions = map[string]map[string][]any{}
	p.mu.Unlock()

	err := p.AnswerPermission(context.Background(), "ses", "req_missing", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error for missing options")
	}
	if !strings.Contains(err.Error(), `resolve permission request "req_missing"`) {
		t.Fatalf("expected resolve error, got: %v", err)
	}
	if !strings.Contains(err.Error(), `permission options missing kind "allow"`) {
		t.Fatalf("expected missing allow option error, got: %v", err)
	}
}

func TestCachePermissionOptions(t *testing.T) {
	p := &Provider{}
	ev := events.Event{
		Event:     "permission.request",
		SessionID: "ses_cache",
		Fields: map[string]any{
			"request_id": "req_cache",
			"options": []any{
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	p.cachePermissionOptions(ev)

	p.mu.Lock()
	sessionOpts, ok := p.pendingOptions["ses_cache"]
	opts, okReq := sessionOpts["req_cache"]
	p.mu.Unlock()
	if !ok || !okReq {
		t.Fatal("options not cached")
	}
	if len(opts) != 1 {
		t.Fatalf("got %d options, want 1", len(opts))
	}
}

func TestCachePermissionOptionsNonRequestEvent(t *testing.T) {
	p := &Provider{}
	p.mu.Lock()
	p.pendingOptions = map[string]map[string][]any{
		"ses_existing": {
			"req_existing": {
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	p.mu.Unlock()
	ev := events.Event{
		Event:  "message",
		Fields: map[string]any{"request_id": "req_nope", "options": []any{map[string]any{"optionId": "deny", "kind": "reject"}}},
	}
	p.cachePermissionOptions(ev)

	p.mu.Lock()
	_, cachedNope := p.pendingOptions[""]["req_nope"]
	_, keptExisting := p.pendingOptions["ses_existing"]["req_existing"]
	gotLen := len(p.pendingOptions)
	p.mu.Unlock()
	if cachedNope {
		t.Fatal("non-request event should not cache options")
	}
	if !keptExisting || gotLen != 1 {
		t.Fatalf("non-request event changed cache: len=%d keptExisting=%v", gotLen, keptExisting)
	}
}

func TestCachePermissionOptionsSessionEndClearsCache(t *testing.T) {
	p := &Provider{}
	p.mu.Lock()
	p.pendingOptions = map[string]map[string][]any{
		"ses_pending": {
			"req_pending": {
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	p.mu.Unlock()
	p.cachePermissionOptions(events.Event{
		Event:     "session.end",
		SessionID: "ses_pending",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	})

	p.mu.Lock()
	gotLen := len(p.pendingOptions)
	p.mu.Unlock()
	if gotLen != 0 {
		t.Fatalf("session.end should clear pending options, got %d entries", gotLen)
	}
}

func TestCachePermissionOptionsSessionEndWithoutSessionClearsAll(t *testing.T) {
	p := &Provider{}
	p.mu.Lock()
	p.pendingOptions = map[string]map[string][]any{
		"ses_a": {
			"req_a": {
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
		"ses_b": {
			"req_b": {
				map[string]any{"optionId": "deny", "kind": "reject"},
			},
		},
	}
	p.mu.Unlock()
	p.cachePermissionOptions(events.Event{Event: "session.end"})

	p.mu.Lock()
	gotLen := len(p.pendingOptions)
	p.mu.Unlock()
	if gotLen != 0 {
		t.Fatalf("session.end without session id should clear all pending options, got %d entries", gotLen)
	}
}

func TestCachePermissionOptionsNoOptions(t *testing.T) {
	p := &Provider{}
	ev := events.Event{
		Event:     "permission.request",
		SessionID: "ses_noopts",
		Fields: map[string]any{
			"request_id": "req_noopts",
		},
	}
	p.cachePermissionOptions(ev)

	p.mu.Lock()
	_, ok := p.pendingOptions["ses_noopts"]["req_noopts"]
	p.mu.Unlock()
	if ok {
		t.Fatal("event without options should not cache entry")
	}
}

func TestCloseClearsPermissionOptions(t *testing.T) {
	p := &Provider{}
	p.mu.Lock()
	p.pendingOptions = map[string]map[string][]any{
		"ses_pending": {
			"req_pending": {
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	p.mu.Unlock()

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	p.mu.Lock()
	gotLen := len(p.pendingOptions)
	p.mu.Unlock()
	if gotLen != 0 {
		t.Fatalf("Close should clear pending options, got %d entries", gotLen)
	}
}

func TestAnswerPermissionKeepsCacheOnAnswerFailure(t *testing.T) {
	p := &Provider{}
	p.mu.Lock()
	p.client = &Client{}
	p.pendingOptions = map[string]map[string][]any{
		"ses": {
			"req": {
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	p.mu.Unlock()

	err := p.AnswerPermission(context.Background(), "ses", "req", runtime.PermissionResponse{Allow: true})
	if err == nil || !strings.Contains(err.Error(), `permission request "req" not found`) {
		t.Fatalf("AnswerPermission error = %v, want missing-request error", err)
	}

	p.mu.Lock()
	_, ok := p.pendingOptions["ses"]["req"]
	p.mu.Unlock()
	if !ok {
		t.Fatal("pending options should remain after answer failure")
	}
}

type recordingWriteCloser struct {
	bytes.Buffer
}

func (recordingWriteCloser) Close() error { return nil }

func TestAnswerPermissionRemovesCacheAfterSuccess(t *testing.T) {
	stdin := &recordingWriteCloser{}
	p := &Provider{}
	p.mu.Lock()
	p.client = &Client{
		stdin:       stdin,
		permissions: map[string]rpcEnvelope{"req": {ID: []byte("1")}},
	}
	p.pendingOptions = map[string]map[string][]any{
		"ses": {
			"req": {
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	p.mu.Unlock()

	if err := p.AnswerPermission(context.Background(), "ses", "req", runtime.PermissionResponse{Allow: true}); err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}

	p.mu.Lock()
	_, ok := p.pendingOptions["ses"]
	p.mu.Unlock()
	if ok {
		t.Fatal("pending options should be cleared after successful answer")
	}
}

func TestAnswerPermissionUsesExplicitOptionID(t *testing.T) {
	stdin := &recordingWriteCloser{}
	p := &Provider{}
	p.mu.Lock()
	p.client = &Client{
		stdin:       stdin,
		permissions: map[string]rpcEnvelope{"req": {ID: []byte("1")}},
	}
	p.pendingOptions = map[string]map[string][]any{
		"ses": {
			"req": {
				map[string]any{"optionId": "allow_first", "kind": "allow"},
				map[string]any{"optionId": "allow_second", "kind": "allow"},
			},
		},
	}
	p.mu.Unlock()

	err := p.AnswerPermission(context.Background(), "ses", "req", runtime.PermissionResponse{
		Allow:    true,
		OptionID: "allow_second",
	})
	if err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}
	if !strings.Contains(stdin.String(), `"optionId":"allow_second"`) {
		t.Fatalf("answer payload = %s, want explicit optionId allow_second", stdin.String())
	}
}

func TestAnswerPermissionRejectsExplicitOptionKindMismatch(t *testing.T) {
	p := &Provider{}
	p.mu.Lock()
	p.client = &Client{}
	p.pendingOptions = map[string]map[string][]any{
		"ses": {
			"req": {
				map[string]any{"optionId": "deny", "kind": "reject"},
			},
		},
	}
	p.mu.Unlock()

	err := p.AnswerPermission(context.Background(), "ses", "req", runtime.PermissionResponse{
		Allow:    true,
		OptionID: "deny",
	})
	if err == nil || !strings.Contains(err.Error(), `has kind "reject", want "allow"`) {
		t.Fatalf("AnswerPermission error = %v, want kind mismatch", err)
	}

	p.mu.Lock()
	_, ok := p.pendingOptions["ses"]["req"]
	p.mu.Unlock()
	if !ok {
		t.Fatal("pending options should remain after explicit option mismatch")
	}
}

func TestAnswerPermissionRemovesFallbackCacheAfterSuccess(t *testing.T) {
	stdin := &recordingWriteCloser{}
	p := &Provider{}
	p.mu.Lock()
	p.client = &Client{
		stdin:       stdin,
		permissions: map[string]rpcEnvelope{"req": {ID: []byte("1")}},
	}
	p.pendingOptions = map[string]map[string][]any{
		"": {
			"req": {
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	p.mu.Unlock()

	if err := p.AnswerPermission(context.Background(), "ses", "req", runtime.PermissionResponse{Allow: true}); err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}

	p.mu.Lock()
	_, ok := p.pendingOptions[""]
	p.mu.Unlock()
	if ok {
		t.Fatal("fallback pending options should be cleared after successful answer")
	}
}
