package acp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
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

type promptReplyingWriteCloser struct {
	t       *testing.T
	client  **Client
	prompts []string
}

func (w *promptReplyingWriteCloser) Write(p []byte) (int, error) {
	var msg rpcEnvelope
	if err := json.Unmarshal(bytes.TrimSpace(p), &msg); err != nil {
		w.t.Fatalf("decode request: %v", err)
	}
	var params struct {
		SessionID string `json:"sessionId"`
		Prompt    []struct {
			Text string `json:"text"`
		} `json:"prompt"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		w.t.Fatalf("decode params: %v", err)
	}
	if len(params.Prompt) > 0 {
		w.prompts = append(w.prompts, params.Prompt[0].Text)
	}
	client := *w.client
	client.pendingMu.Lock()
	ch := client.pending[string(msg.ID)]
	client.pendingMu.Unlock()
	if ch != nil {
		ch <- rpcEnvelope{ID: msg.ID, Result: json.RawMessage(`{"stopReason":"end_turn"}`)}
	}
	return len(p), nil
}

func (*promptReplyingWriteCloser) Close() error { return nil }

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

func TestCachePermissionOptionsSessionEndInjectsPendingMessages(t *testing.T) {
	var client *Client
	stdin := &promptReplyingWriteCloser{t: t, client: &client}
	client = &Client{
		stdin:   stdin,
		events:  make(chan events.Event, 2),
		done:    make(chan struct{}),
		pending: map[string]chan rpcEnvelope{},
	}
	p := &Provider{
		sessions: map[string]*Session{
			"ses": {Client: client, SessionID: "ses"},
		},
		pendingOptions: map[string]map[string][]any{
			"ses": {
				"req": {map[string]any{"optionId": "allow", "kind": "allow"}},
			},
		},
		pendingMessages: map[string][]string{"ses": {"<channel>hello</channel>"}},
		events:          make(chan events.Event, 2),
	}

	p.cachePermissionOptions(events.Event{Event: "session.end", SessionID: "ses"})

	if len(stdin.prompts) != 1 || stdin.prompts[0] != "<channel>hello</channel>" {
		t.Fatalf("injected prompts = %#v, want pending channel message", stdin.prompts)
	}
	p.pendingMu.Lock()
	if len(p.pendingMessages["ses"]) != 0 {
		p.pendingMu.Unlock()
		t.Fatalf("pending messages not cleared: %#v", p.pendingMessages["ses"])
	}
	p.pendingMu.Unlock()
	p.mu.Lock()
	if _, ok := p.pendingOptions["ses"]; ok {
		p.mu.Unlock()
		t.Fatal("pending permission options should be cleared after session end")
	}
	p.mu.Unlock()

	select {
	case evt := <-p.events:
		if evt.Event != "agent.message_chunk" || evt.SessionID != "ses" {
			t.Fatalf("published event = %#v, want agent.message_chunk for ses", evt)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for injected agent.message_chunk event")
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

func TestStartRunsClientEnvCleanupOnEnsureClientFailure(t *testing.T) {
	cleaned := false
	p := &Provider{
		backendID:       "test-acp",
		bin:             "definitely-not-a-real-binary",
		sessions:        map[string]*Session{},
		runIDs:          map[string]string{},
		pollContexts:    map[string]context.Context{},
		pollCancels:     map[string]context.CancelFunc{},
		pendingMessages: map[string][]string{},
		broker:          broker.New(""),
		buildClientEnv: func(opts runtime.StartOptions, runID, brokerToken, brokerAddr string) (map[string]string, func() error) {
			return map[string]string{"OPENCODE_CONFIG": "/tmp/unused.json"}, func() error {
				cleaned = true
				return nil
			}
		},
	}

	_, err := p.Start(context.Background(), runtime.StartOptions{Broker: p.broker})
	if err == nil {
		t.Fatal("expected Start to fail with invalid binary")
	}
	if !cleaned {
		t.Fatal("expected client env cleanup to run after ensureClient failure")
	}
}

func TestCloseRunsClientEnvCleanup(t *testing.T) {
	path := t.TempDir() + "/opencode.json"
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	p := &Provider{
		pollCancels:      map[string]context.CancelFunc{},
		pollContexts:     map[string]context.Context{},
		pendingMessages:  map[string][]string{},
		clientEnvCleanup: func() error { return os.Remove(path) },
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("config file still exists after Close cleanup: %v", err)
	}
}

// acpStub describes a minimal ACP stdio stub. It answers the first request
// (initialize) and then either a session/open result, an error, or nothing.
type acpStub struct {
	Mode  string // "exit", "silent", "fail-after-init", "hang-prompt" or "serve"
	Gate  string // when set, the stub waits for this file before reading stdin
	Ready string // when set, the stub touches this path once it is spawned
	// Observe, when set, makes the stub write the allowlisted launch
	// environment to this path before it answers any request.
	Observe string
	// Prompt, in "hang-prompt" mode, is touched when the stub receives a
	// session/prompt request; the stub never answers such a request.
	Prompt string
	// ReadyPerProcess appends the stub's pid to Ready so each stub instance
	// leaves a distinct marker.
	ReadyPerProcess bool
}

func writeACPStub(t *testing.T, stub acpStub) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "acp-stub.sh")
	var body strings.Builder
	body.WriteString("#!/bin/sh\n")
	if stub.Observe != "" {
		body.WriteString(observerBody(stub.Observe))
	}
	if stub.Ready != "" {
		if stub.ReadyPerProcess {
			fmt.Fprintf(&body, "touch %s\"$$\"\n", shellQuotePath(stub.Ready))
		} else {
			fmt.Fprintf(&body, "touch %s\n", shellQuotePath(stub.Ready))
		}
	}
	if stub.Gate != "" {
		fmt.Fprintf(&body, "while [ ! -f %s ]; do sleep 0.01; done\n", shellQuotePath(stub.Gate))
	}
	switch stub.Mode {
	case "exit":
		body.WriteString("exit 3\n")
	case "silent":
		body.WriteString("cat > /dev/null\n")
	case "hang-prompt":
		body.WriteString("n=0\n")
		body.WriteString("while IFS= read -r line; do\n")
		body.WriteString("  id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n")
		body.WriteString("  [ -n \"$id\" ] || continue\n")
		body.WriteString("  n=$((n+1))\n")
		body.WriteString("  if [ \"$n\" -eq 1 ]; then\n")
		body.WriteString("    printf '{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"protocolVersion\":1}}\\n' \"$id\"\n")
		body.WriteString("  elif [ \"$n\" -eq 2 ]; then\n")
		body.WriteString("    printf '{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"sessionId\":\"ses_stub\"}}\\n' \"$id\"\n")
		if stub.Prompt != "" {
			fmt.Fprintf(&body, "  else\n    touch %s\n", shellQuotePath(stub.Prompt))
		} else {
			body.WriteString("  else\n    :\n")
		}
		body.WriteString("  fi\ndone\n")
	default:
		body.WriteString("n=0\n")
		body.WriteString("while IFS= read -r line; do\n")
		body.WriteString("  id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n")
		body.WriteString("  [ -n \"$id\" ] || continue\n")
		body.WriteString("  n=$((n+1))\n")
		body.WriteString("  if [ \"$n\" -eq 1 ]; then\n")
		body.WriteString("    printf '{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"protocolVersion\":1}}\\n' \"$id\"\n")
		body.WriteString("  else\n")
		if stub.Mode == "fail-after-init" {
			body.WriteString("    printf '{\"jsonrpc\":\"2.0\",\"id\":%s,\"error\":{\"code\":-32000,\"message\":\"stub refused\"}}\\n' \"$id\"\n")
		} else {
			body.WriteString("    printf '{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"sessionId\":\"ses_stub\"}}\\n' \"$id\"\n")
		}
		body.WriteString("  fi\ndone\n")
	}
	if err := os.WriteFile(script, []byte(body.String()), 0o755); err != nil {
		t.Fatalf("write acp stub: %v", err)
	}
	return script
}

// writeCandidateConfig stands in for a config file a backend generated for one
// launch, with the same repeatable cleanup contract the backends use.
func writeCandidateConfig(t *testing.T) (string, func() error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "candidate.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write candidate config: %v", err)
	}
	return path, func() error {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
}

func newStubProvider(bin string, build func(runtime.StartOptions, string, string, string) (map[string]string, func() error)) *Provider {
	return &Provider{
		backendID:       "test-acp",
		bin:             bin,
		sessions:        map[string]*Session{},
		runIDs:          map[string]string{},
		pollContexts:    map[string]context.Context{},
		pollCancels:     map[string]context.CancelFunc{},
		pendingMessages: map[string][]string{},
		buildClientEnv:  build,
	}
}

func candidateEnv(path string) map[string]string {
	return map[string]string{"OPENCODE_CONFIG": path}
}

func providerClient(p *Provider) *Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client
}

func waitForPath(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestEnsureClientReuseKeepsOriginalConfig pins the ownership transfer: a live
// client keeps its own config and cleanup, while the candidate launch that was
// never retained cleans up only itself.
func TestEnsureClientReuseKeepsOriginalConfig(t *testing.T) {
	stub := writeACPStub(t, acpStub{Mode: "serve"})
	p := newStubProvider(stub, nil)
	p.client = &Client{}
	originalCleanupRan := false
	p.clientEnvCleanup = func() error {
		originalCleanupRan = true
		return nil
	}

	candidate, cleanup := writeCandidateConfig(t)
	created, err := p.ensureClient(context.Background(), runtime.StartOptions{}, candidateEnv(candidate), cleanup)
	if err != nil {
		t.Fatalf("ensureClient: %v", err)
	}
	if created {
		t.Fatal("candidate launch was retained despite a live client")
	}
	if _, err := os.Stat(candidate); !os.IsNotExist(err) {
		t.Fatalf("unused candidate config not cleaned up: %v", err)
	}
	if originalCleanupRan {
		t.Fatal("live client's config cleanup was replaced or run")
	}
	if p.clientEnvCleanup == nil {
		t.Fatal("live client lost ownership of its config")
	}
}

// TestEnsureClientDiscardsConfigWhenAuthenticationFails covers the cleanup of
// a launch abandoned after initialization but before a client is retained.
func TestEnsureClientDiscardsConfigWhenAuthenticationFails(t *testing.T) {
	stub := writeACPStub(t, acpStub{Mode: "fail-after-init"})
	p := newStubProvider(stub, nil)
	p.authenticateID = "stub-auth"

	candidate, cleanup := writeCandidateConfig(t)
	created, err := p.ensureClient(context.Background(), runtime.StartOptions{}, candidateEnv(candidate), cleanup)
	if err == nil {
		t.Fatal("expected authentication failure")
	}
	if created {
		t.Fatal("failed launch was retained as the provider client")
	}
	if _, statErr := os.Stat(candidate); !os.IsNotExist(statErr) {
		t.Fatalf("abandoned launch config not cleaned up: %v", statErr)
	}
	if client := providerClient(p); client != nil {
		t.Fatal("failed launch retained its client")
	}
}

// TestStartRetainsConfigForLiveClientAfterSessionOpenFailure keeps the clean
// split: a retained client still owns its config after a session-open error, and
// provider close is what removes it. The startup error is propagated unchanged.
func TestStartRetainsConfigForLiveClientAfterSessionOpenFailure(t *testing.T) {
	stub := writeACPStub(t, acpStub{Mode: "fail-after-init"})
	candidate, cleanup := writeCandidateConfig(t)
	p := newStubProvider(stub, func(runtime.StartOptions, string, string, string) (map[string]string, func() error) {
		return candidateEnv(candidate), cleanup
	})

	_, err := p.Start(context.Background(), runtime.StartOptions{})
	if err == nil {
		t.Fatal("expected session open failure")
	}
	if !strings.Contains(err.Error(), "stub refused") {
		t.Fatalf("session open error = %v, want the stub's refusal", err)
	}
	if _, statErr := os.Stat(candidate); statErr != nil {
		t.Fatalf("config removed while its client is still retained: %v", statErr)
	}

	_ = p.Close()
	if _, statErr := os.Stat(candidate); !os.IsNotExist(statErr) {
		t.Fatalf("provider close did not remove its retained config: %v", statErr)
	}
}

// TestStartDiscardsConfigWhenLaunchIsCancelledDuringInitialize covers a launch
// abandoned before any client is retained.
func TestStartDiscardsConfigWhenLaunchIsCancelledDuringInitialize(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	stub := writeACPStub(t, acpStub{Mode: "silent", Ready: ready})
	candidate, cleanup := writeCandidateConfig(t)
	p := newStubProvider(stub, func(runtime.StartOptions, string, string, string) (map[string]string, func() error) {
		return candidateEnv(candidate), cleanup
	})

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() {
		_, err := p.Start(ctx, runtime.StartOptions{})
		started <- err
	}()
	waitForPath(t, ready)
	cancel()

	select {
	case err := <-started:
		if err == nil {
			t.Fatal("expected cancelled startup to fail")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}
	if _, statErr := os.Stat(candidate); !os.IsNotExist(statErr) {
		t.Fatalf("cancelled launch config not cleaned up: %v", statErr)
	}
}

// TestStartRetainsConfigWhenSessionConfigurationFails covers the other retained
// case: the client and its config outlive a session-configuration error and are
// only released by close.
func TestStartRetainsConfigWhenSessionConfigurationFails(t *testing.T) {
	stub := writeACPStub(t, acpStub{Mode: "serve"})
	candidate, cleanup := writeCandidateConfig(t)
	p := newStubProvider(stub, func(runtime.StartOptions, string, string, string) (map[string]string, func() error) {
		return candidateEnv(candidate), cleanup
	})
	p.configureSession = func(context.Context, *Session, runtime.StartOptions) error {
		return errors.New("configure refused")
	}

	_, err := p.Start(context.Background(), runtime.StartOptions{})
	if err == nil {
		t.Fatal("expected session configuration failure")
	}
	if !strings.Contains(err.Error(), "configure refused") {
		t.Fatalf("session configuration error = %v, want the stub's refusal", err)
	}
	if providerClient(p) == nil {
		t.Fatal("session configuration failure dropped the client that owned the config")
	}
	if _, statErr := os.Stat(candidate); statErr != nil {
		t.Fatalf("config removed while its client is still retained: %v", statErr)
	}

	_ = p.Close()
	if _, statErr := os.Stat(candidate); !os.IsNotExist(statErr) {
		t.Fatalf("provider close did not remove its retained config: %v", statErr)
	}
}

// TestCancelledPromptKeepsLiveClientConfig pins that cancelling a prompt leaves
// the launch — client and config — in place until close.
func TestCancelledPromptKeepsLiveClientConfig(t *testing.T) {
	promptSeen := filepath.Join(t.TempDir(), "prompt-seen")
	stub := writeACPStub(t, acpStub{Mode: "hang-prompt", Prompt: promptSeen})
	candidate, cleanup := writeCandidateConfig(t)
	p := newStubProvider(stub, func(runtime.StartOptions, string, string, string) (map[string]string, func() error) {
		return candidateEnv(candidate), cleanup
	})

	session, err := p.Start(context.Background(), runtime.StartOptions{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	promptCtx, cancelPrompt := context.WithCancel(context.Background())
	prompted := make(chan error, 1)
	go func() { prompted <- p.Prompt(promptCtx, session.SessionID, "hello") }()
	waitForPath(t, promptSeen)

	if err := p.Cancel(context.Background(), session.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	cancelPrompt()

	select {
	case err := <-prompted:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("prompt error = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("prompt did not return after cancellation")
	}
	if providerClient(p) == nil {
		t.Fatal("cancelled prompt dropped the live client")
	}
	if _, statErr := os.Stat(candidate); statErr != nil {
		t.Fatalf("config removed while its client is still live: %v", statErr)
	}

	_ = p.Close()
	if _, statErr := os.Stat(candidate); !os.IsNotExist(statErr) {
		t.Fatalf("provider close did not remove the live client config: %v", statErr)
	}
}

// TestEnsureClientConcurrentInstallKeepsSingleConfigOwner releases two launches
// at the same time: exactly one becomes the provider client, the loser cleans
// only its own config, and close removes the retained winner's config.
func TestEnsureClientConcurrentInstallKeepsSingleConfigOwner(t *testing.T) {
	dir := t.TempDir()
	gate := filepath.Join(dir, "gate")
	readyPrefix := filepath.Join(dir, "ready")
	stub := writeACPStub(t, acpStub{Mode: "serve", Gate: gate, Ready: readyPrefix + ".", ReadyPerProcess: true})
	p := newStubProvider(stub, nil)

	type outcome struct {
		id      int
		path    string
		created bool
		err     error
	}
	results := make(chan outcome, 2)
	candidates := make([]string, 2)
	for i := 0; i < 2; i++ {
		candidate, cleanup := writeCandidateConfig(t)
		candidates[i] = candidate
		go func(id int, env map[string]string, cleanup func() error) {
			created, err := p.ensureClient(context.Background(), runtime.StartOptions{}, env, cleanup)
			results <- outcome{id: id, path: candidate, created: created, err: err}
		}(i, candidateEnv(candidate), cleanup)
	}

	waitForPathCount(t, readyPrefix, 2)
	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatalf("release stubs: %v", err)
	}

	createdIDs := map[int]bool{}
	for i := 0; i < 2; i++ {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("ensureClient %d: %v", result.id, result.err)
			}
			if result.created {
				createdIDs[result.id] = true
			}
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent ensureClient did not finish")
		}
	}
	if len(createdIDs) != 1 {
		t.Fatalf("retained launches = %v, want exactly one", createdIDs)
	}

	_ = p.Close()
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); !os.IsNotExist(err) {
			t.Fatalf("config %q leaked after close: %v", candidate, err)
		}
	}
}

func waitForPathCount(t *testing.T, prefix string, want int) {
	t.Helper()
	dir := filepath.Dir(prefix)
	base := filepath.Base(prefix)
	deadline := time.Now().Add(10 * time.Second)
	for {
		entries, err := os.ReadDir(dir)
		if err == nil {
			count := 0
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), base+".") {
					count++
				}
			}
			if count >= want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d %s markers", want, base)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
