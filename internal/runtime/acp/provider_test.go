package acp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/warmqueue"
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

// TestCloseClearsCachedState pins what a closed provider stops answering for:
// the sessions, runs and pending permissions it held before Close are gone
// rather than left to fail against a client Close already closed.
func TestCloseClearsCachedState(t *testing.T) {
	p := &Provider{}
	p.mu.Lock()
	p.pendingOptions = map[string]map[string][]any{
		"ses_pending": {
			"req_pending": {
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	p.sessions = map[string]*Session{"ses_pending": {}}
	p.runIDs = map[string]string{"ses_pending": "run_pending"}
	p.mu.Unlock()

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	p.mu.Lock()
	pending, sessions, runIDs := len(p.pendingOptions), len(p.sessions), len(p.runIDs)
	p.mu.Unlock()
	if pending != 0 || sessions != 0 || runIDs != 0 {
		t.Fatalf("Close left %d pending options, %d sessions and %d run ids", pending, sessions, runIDs)
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
	mu      sync.Mutex
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
		w.mu.Lock()
		w.prompts = append(w.prompts, params.Prompt[0].Text)
		w.mu.Unlock()
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

func (w *promptReplyingWriteCloser) snapshotPrompts() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.prompts)
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
	hub := newEventHub()
	p := &Provider{
		sessions: map[string]*Session{
			"ses": {Client: client, SessionID: "ses"},
		},
		pendingOptions: map[string]map[string][]any{
			"ses": {
				"req": {map[string]any{"optionId": "allow", "kind": "allow"}},
			},
		},
		Queue: warmqueue.NewQueue(),
		hub:   hub,
	}
	go p.pumpClientEvents(client, hub)
	defer func() {
		hub.requestStop()
		<-hub.pumpDone
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	evCh, err := p.Events(ctx, "ses")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	p.Queue.Enqueue("ses", "<channel>hello</channel>")
	p.cachePermissionOptions(events.Event{Event: "session.end", SessionID: "ses"})

	if left, _ := p.Queue.Take("ses"); len(left) != 0 {
		t.Fatalf("pending messages not cleared: %#v", left)
	}
	p.mu.Lock()
	if _, ok := p.pendingOptions["ses"]; ok {
		p.mu.Unlock()
		t.Fatal("pending permission options should be cleared after session end")
	}
	p.mu.Unlock()

	select {
	case evt := <-evCh:
		if evt.Event != "agent.message_chunk" || evt.SessionID != "ses" {
			t.Fatalf("published event = %#v, want agent.message_chunk for ses", evt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for injected agent.message_chunk event")
	}

	gotPrompts := stdin.snapshotPrompts()
	if len(gotPrompts) != 1 || gotPrompts[0] != "<channel>hello</channel>" {
		t.Fatalf("injected prompts = %#v, want pending channel message", gotPrompts)
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
		backendID:    "test-acp",
		bin:          "definitely-not-a-real-binary",
		sessions:     map[string]*Session{},
		runIDs:       map[string]string{},
		pollContexts: map[string]context.Context{},
		pollCancels:  map[string]context.CancelFunc{},
		Queue:        warmqueue.NewQueue(),
		broker:       broker.New(""),
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
		Queue:            warmqueue.NewQueue(),
		clientEnvCleanup: func() error { return os.Remove(path) },
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("config file still exists after Close cleanup: %v", err)
	}
}

// TestStartDiscardsConfigWhenLaunchIsCancelledDuringInitialize covers the
// cleanup of a launch abandoned before any client is retained.
func TestStartDiscardsConfigWhenLaunchIsCancelledDuringInitialize(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	bin := filepath.Join(dir, "silent.sh")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\ntouch "+shellQuotePath(ready)+"\ncat > /dev/null\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	config := filepath.Join(dir, "candidate.json")
	if err := os.WriteFile(config, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	p := &Provider{
		backendID: "test-acp",
		bin:       bin,
		sessions:  map[string]*Session{},
		buildClientEnv: func(runtime.StartOptions, string, string, string) (map[string]string, func() error) {
			return map[string]string{"OPENCODE_CONFIG": config}, func() error { return os.Remove(config) }
		},
	}

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
	if _, err := os.Stat(config); !os.IsNotExist(err) {
		t.Fatalf("cancelled launch config not cleaned up: %v", err)
	}
}

// errLaunchStopped stands in for a launch that fails on its own while Close is
// in progress.
var errLaunchStopped = errors.New("launch stopped")

// TestAbandonedLaunchStillEndsWhenCloseGaveUp covers a launch Close admitted but
// gave up waiting for: the launch must run under the lifetime Close already
// cancelled instead of building a lifetime of its own after Close returned.
func TestAbandonedLaunchStillEndsWhenCloseGaveUp(t *testing.T) {
	p := &Provider{backendID: "test-acp"}
	if err := p.beginLaunch(); err != nil {
		t.Fatalf("beginLaunch: %v", err)
	}
	// This launch never settles, so Close waits out its bound and returns first.
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	launchCtx, _, abandon := p.launchContext(context.Background())
	defer abandon()
	awaitSignal(t, launchCtx.Done(), "the abandoned launch context to end")
	p.settleLaunch()
}

// TestCloseDuringLaunchLeavesNoClient covers the launch-versus-Close race: a
// launch still in flight when Close starts keeps Close waiting, never installs
// a client, its config cleanup, or a session afterwards, and does not leave the
// child it started behind.
func TestCloseDuringLaunchLeavesNoClient(t *testing.T) {
	t.Run("close waits for a real launch", func(t *testing.T) {
		pidFile := filepath.Join(t.TempDir(), "child.pid")
		// The stub answers the handshake so the launch reaches configureSession,
		// and exits cleanly on SIGTERM the way a well-behaved backend does.
		p, config := newLaunchRaceProvider(t, `#!/bin/sh
trap 'exit 0' TERM
echo $$ > `+shellQuotePath(pidFile)+`
read -r line
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{}}'
read -r line
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{}}'
read -r line
printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"sessionId":"stub-session"}}'
cat > /dev/null
`)
		p.authenticateID = "stub-auth"
		// This gate has no context to cancel, so only the launch count can
		// release Close.
		entered, gate := make(chan struct{}), make(chan struct{})
		p.configureSession = func(context.Context, *Session, runtime.StartOptions) error {
			close(entered)
			awaitSignal(t, gate, "the configureSession gate")
			return nil
		}

		started := startInBackground(p)
		awaitSignal(t, entered, "configureSession")
		closed := make(chan error, 1)
		began := time.Now()
		go func() { closed <- p.Close() }()
		assertCloseBlocked(t, closed)
		close(gate)
		awaitClose(t, closed, began, 0, 10*time.Second)

		assertLaunchAbandoned(t, p, started, config, errProviderClosed)
		assertRecordedChildGone(t, pidFile)
	})

	t.Run("close waits for every launch", func(t *testing.T) {
		// exec keeps the child a single process that dies with its stdin, so no
		// orphan holds the client's pipe open.
		p, config := newLaunchRaceProvider(t, "#!/bin/sh\nexec cat > /dev/null\n")
		entered, childStarted := make(chan struct{}, 2), make(chan struct{})
		settled, wedged := make(chan struct{}), make(chan struct{})
		childPID := 0
		var calls atomic.Int32
		p.startClient = func(ctx context.Context, cfg ClientConfig) (*Client, error) {
			if calls.Add(1) == 1 {
				// The first launch fails on its own while Close is running, so its
				// failure is its own and not the provider's.
				entered <- struct{}{}
				awaitSignal(t, settled, "the first launch's release")
				return nil, errLaunchStopped
			}
			// The second launch starts a real child and waits for its context, so
			// only the provider lifetime can end it.
			client, err := NewClient(ctx, cfg)
			if err != nil {
				return nil, err
			}
			childPID = client.PID()
			close(childStarted)
			entered <- struct{}{}
			awaitSignal(t, wedged, "the wedged launch's release")
			return nil, ctx.Err()
		}

		// The first launch is admitted and in startClient before the second
		// begins, so the call order matches the order the launches are started.
		first := startInBackground(p)
		awaitSignal(t, entered, "the first launch")
		second := startInBackground(p)
		awaitSignal(t, entered, "the second launch")
		awaitSignal(t, childStarted, "the wedged child")

		closed := make(chan error, 1)
		began := time.Now()
		go func() { closed <- p.Close() }()
		assertCloseBlocked(t, closed)
		settled <- struct{}{}
		// The second launch is still in flight, so Close stays blocked.
		assertCloseBlocked(t, closed)
		awaitClose(t, closed, began, launchSettleTimeout, launchSettleTimeout+5*time.Second)
		assertPIDGone(t, childPID)
		// The launch Close gave up on is no longer counted, so it does not wait
		// the bound again.
		closed = make(chan error, 1)
		began = time.Now()
		go func() { closed <- p.Close() }()
		awaitClose(t, closed, began, 0, time.Second)

		close(wedged)
		assertLaunchAbandoned(t, p, first, config, errLaunchStopped)
		assertLaunchAbandoned(t, p, second, config, errProviderClosed)
	})

	t.Run("a client that starts after close is torn down", func(t *testing.T) {
		// The launch refuses the context derived from the provider lifetime, so
		// only the closing check at install can tear its client down.
		p, config := newLaunchRaceProvider(t, "#!/bin/sh\nexec cat > /dev/null\n")
		childStarted, release := make(chan struct{}), make(chan struct{})
		childPID := 0
		p.startClient = func(_ context.Context, cfg ClientConfig) (*Client, error) {
			client, err := NewClient(context.Background(), cfg)
			if err != nil {
				return nil, err
			}
			childPID = client.PID()
			close(childStarted)
			awaitSignal(t, release, "the install gate")
			return client, nil
		}

		started := startInBackground(p)
		awaitSignal(t, childStarted, "the started client")
		closed := make(chan error, 1)
		began := time.Now()
		go func() { closed <- p.Close() }()
		awaitClosing(t, p)
		close(release)
		awaitClose(t, closed, began, 0, 10*time.Second)

		assertLaunchAbandoned(t, p, started, config, errProviderClosed)
		assertPIDGone(t, childPID)
	})

	t.Run("close abandons a resume in flight", func(t *testing.T) {
		// The stub answers the handshake, notes the resume request and then
		// holds it open, so Close closes the client the resume is waiting on.
		dir := t.TempDir()
		pidFile := filepath.Join(dir, "child.pid")
		resumeReady := filepath.Join(dir, "resume")
		p, config := newLaunchRaceProvider(t, `#!/bin/sh
trap 'exit 0' TERM
echo $$ > `+shellQuotePath(pidFile)+`
read -r line
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{}}'
read -r line
touch `+shellQuotePath(resumeReady)+`
cat > /dev/null
`)

		resumed := make(chan error, 1)
		go func() {
			_, err := p.ResumeWithOptions(context.Background(), "stub-session", runtime.StartOptions{})
			resumed <- err
		}()
		waitForPath(t, resumeReady)

		closed := make(chan error, 1)
		began := time.Now()
		go func() { closed <- p.Close() }()
		awaitClose(t, closed, began, launchSettleTimeout, launchSettleTimeout+5*time.Second)

		assertLaunchAbandoned(t, p, resumed, config, errProviderClosed)
		assertRecordedChildGone(t, pidFile)
	})
}

// newLaunchRaceProvider builds a provider backed by the stub at script, whose
// launch generates a config cleanup removes, and returns that config's path.
func newLaunchRaceProvider(t *testing.T, script string) (*Provider, string) {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "stub.sh")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	config := filepath.Join(dir, "candidate.json")
	if err := os.WriteFile(config, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return &Provider{
		backendID: "test-acp",
		bin:       bin,
		sessions:  map[string]*Session{},
		buildClientEnv: func(runtime.StartOptions, string, string, string) (map[string]string, func() error) {
			return map[string]string{"OPENCODE_CONFIG": config}, func() error { return os.Remove(config) }
		},
	}, config
}

func startInBackground(p *Provider) <-chan error {
	started := make(chan error, 1)
	go func() {
		_, err := p.Start(context.Background(), runtime.StartOptions{})
		started <- err
	}()
	return started
}

// assertCloseBlocked fails when Close returns while a launch is still owed.
func assertCloseBlocked(t *testing.T, closed <-chan error) {
	t.Helper()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before the launch settled: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

// awaitClosing waits for Close to latch its closing flag, which it sets before
// it waits for the launches it owes.
func awaitClosing(t *testing.T, p *Provider) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		p.mu.Lock()
		closing := p.closing
		p.mu.Unlock()
		if closing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for Close to latch")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// awaitSignal fails the test when ch stays silent past a bound, so a stuck race
// reports instead of hanging the run.
func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// awaitClose waits for Close and pins how long it took: a lower bound for a wait
// that must still be running, an upper bound for the bounded launch wait.
func awaitClose(t *testing.T, closed <-chan error, began time.Time, min, max time.Duration) {
	t.Helper()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(max + 5*time.Second):
		t.Fatal("Close did not return")
	}
	if elapsed := time.Since(began); elapsed < min || elapsed > max {
		t.Fatalf("Close took %s, want between %s and %s", elapsed, min, max)
	}
}

// assertLaunchAbandoned proves an abandoned launch failed with wantErr without
// being relabelled as the provider's closure — or, when wantErr is that closure,
// that it was — installed nothing, and removed the generated config at config.
func assertLaunchAbandoned(t *testing.T, p *Provider, started <-chan error, config string, wantErr error) {
	t.Helper()
	select {
	case err := <-started:
		if !errors.Is(err, wantErr) {
			t.Fatalf("abandoned call error = %v, want %v", err, wantErr)
		}
		if wantErr != errProviderClosed && errors.Is(err, errProviderClosed) {
			t.Fatalf("abandoned call error = %v, want %v alone", err, wantErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("abandoned call did not return after Close")
	}
	p.mu.Lock()
	installed := p.client != nil || p.clientEnvCleanup != nil || len(p.sessions) != 0
	p.mu.Unlock()
	if installed {
		t.Fatal("launch installed a client, its cleanup or a session after Close")
	}
	if _, err := os.Stat(config); config != "" && !os.IsNotExist(err) {
		t.Fatalf("launch config not cleaned up: %v", err)
	}
}

// assertRecordedChildGone reads the pid the stub wrote and proves it exited.
func assertRecordedChildGone(t *testing.T, pidFile string) {
	t.Helper()
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read recorded child pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("parse recorded child pid %q: %v", data, err)
	}
	assertPIDGone(t, pid)
}

// assertPIDGone proves the process pid has exited.
func assertPIDGone(t *testing.T, pid int) {
	t.Helper()
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("child process %d outlived Close: %v", pid, err)
	}
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

// TestStartAfterCloseIsRefused pins the closing check in beginLaunch: a closed
// provider refuses to launch, so the missing binary is never reached.
func TestStartAfterCloseIsRefused(t *testing.T) {
	p := NewProvider(ProviderConfig{BackendID: "test", Bin: filepath.Join(t.TempDir(), "absent")}).(*Provider)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := p.Start(context.Background(), runtime.StartOptions{}); !errors.Is(err, errProviderClosed) {
		t.Fatalf("Start after Close = %v, want errProviderClosed", err)
	}
}

type fakePeerCall struct {
	Method string
	Params map[string]any
}

type fakeInMemoryPeer struct {
	mu          sync.Mutex
	client      *Client
	calls       []fakePeerCall
	onCall      func(msg rpcEnvelope, params map[string]any) *rpcEnvelope
	nextSession int
}

func newFakeInMemoryPeer(t *testing.T) *fakeInMemoryPeer {
	t.Helper()
	peer := &fakeInMemoryPeer{}
	client := &Client{
		dir:         t.TempDir(),
		events:      make(chan events.Event, 128),
		stopEvents:  make(chan struct{}),
		done:        make(chan struct{}),
		pending:     map[string]chan rpcEnvelope{},
		permissions: map[string]rpcEnvelope{},
		pid:         4242,
	}
	client.stdin = &rpcFuncWriteCloser{
		fn: func(msg rpcEnvelope) {
			var params map[string]any
			if len(msg.Params) > 0 {
				_ = json.Unmarshal(msg.Params, &params)
			}
			peer.mu.Lock()
			peer.calls = append(peer.calls, fakePeerCall{Method: msg.Method, Params: params})
			handler := peer.onCall
			peer.nextSession++
			seq := peer.nextSession
			peer.mu.Unlock()

			if len(msg.ID) == 0 {
				return
			}
			// Deliver RPC responses asynchronously so handlers that block (e.g.
			// waiting for a concurrent session/cancel) do not hold Client.sendMu.
			go func() {
				var reply rpcEnvelope
				if handler != nil {
					if custom := handler(msg, params); custom != nil {
						reply = *custom
					}
				}
				if len(reply.ID) == 0 && reply.Error == nil && len(reply.Result) == 0 {
					switch msg.Method {
					case "session/new":
						reply = rpcEnvelope{
							ID:     msg.ID,
							Result: mustRaw(map[string]any{"sessionId": "ses_" + strconv.Itoa(seq)}),
						}
					default:
						reply = rpcEnvelope{
							ID:     msg.ID,
							Result: json.RawMessage(`{"stopReason":"end_turn"}`),
						}
					}
				}
				client.pendingMu.Lock()
				ch := client.pending[string(msg.ID)]
				client.pendingMu.Unlock()
				if ch != nil {
					ch <- reply
				}
			}()
		},
	}
	peer.client = client
	return peer
}

func (f *fakeInMemoryPeer) snapshotCalls() []fakePeerCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func TestBrokerIdentityEnsureRunRefusalAndEmptyURL(t *testing.T) {
	t.Run("unstarted broker with empty URL installs no MCP creds or poller", func(t *testing.T) {
		unstartedBroker := broker.New("")
		peer := newFakeInMemoryPeer(t)

		var envRunID, envToken, envURL string
		var openRunID, openToken, openURL string
		p := NewProvider(ProviderConfig{
			BackendID: "test-acp",
			BuildClientEnv: func(_ runtime.StartOptions, runID, token, url string) (map[string]string, func() error) {
				envRunID, envToken, envURL = runID, token, url
				return nil, nil
			},
			BuildSessionOpenOptions: func(_ runtime.StartOptions, runID, token, url string) (SessionOpenOptions, error) {
				openRunID, openToken, openURL = runID, token, url
				return SessionOpenOptions{}, nil
			},
		}).(*Provider)
		p.startClient = func(context.Context, ClientConfig) (*Client, error) {
			return peer.client, nil
		}
		defer func() {
			close(peer.client.events)
			close(peer.client.done)
			_ = p.Close()
		}()

		ses, err := p.Start(context.Background(), runtime.StartOptions{
			Broker:    unstartedBroker,
			RuntimeID: "run_unstarted",
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if envRunID != "" || envToken != "" || envURL != "" {
			t.Fatalf("BuildClientEnv got (%q, %q, %q), want all empty", envRunID, envToken, envURL)
		}
		if openRunID != "" || openToken != "" || openURL != "" {
			t.Fatalf("BuildSessionOpenOptions got (%q, %q, %q), want all empty", openRunID, openToken, openURL)
		}
		p.mu.Lock()
		owned := p.sessions[ses.SessionID]
		pollCount := len(p.pollCancels)
		p.mu.Unlock()
		if owned.RunID != "" || owned.BrokerToken != "" {
			t.Fatalf("owned session has RunID=%q Token=%q, want empty", owned.RunID, owned.BrokerToken)
		}
		if pollCount != 0 {
			t.Fatalf("pollCancels count = %d, want 0", pollCount)
		}
	})

	t.Run("EnsureRun refusal on Start and ResumeWithOptions installs no MCP creds or poller", func(t *testing.T) {
		b := broker.New("")
		if err := b.Start(); err != nil {
			t.Fatalf("start broker: %v", err)
		}
		defer func() { _ = b.Stop() }()

		// Pre-register run_taken via the open /register route so EnsureRun refuses
		// with ErrRunNotOwned.
		regResp, err := http.Post(b.URL()+"/register", "application/json", strings.NewReader(`{"run_id":"run_taken","token":"foreign-secret-token"}`))
		if err != nil {
			t.Fatalf("POST /register: %v", err)
		}
		_ = regResp.Body.Close()
		if regResp.StatusCode != http.StatusOK {
			t.Fatalf("POST /register status = %d, want 200", regResp.StatusCode)
		}

		for _, mode := range []string{"start", "resume"} {
			t.Run(mode, func(t *testing.T) {
				peer := newFakeInMemoryPeer(t)
				var gotOpenRunID, gotOpenToken, gotOpenURL string
				p := NewProvider(ProviderConfig{
					BackendID: "test-acp",
					BuildSessionOpenOptions: func(_ runtime.StartOptions, runID, token, url string) (SessionOpenOptions, error) {
						gotOpenRunID, gotOpenToken, gotOpenURL = runID, token, url
						return SessionOpenOptions{}, nil
					},
				}).(*Provider)
				p.startClient = func(context.Context, ClientConfig) (*Client, error) {
					return peer.client, nil
				}
				defer func() {
					close(peer.client.events)
					close(peer.client.done)
					_ = p.Close()
				}()

				var (
					ses runtime.Session
					err error
				)
				if mode == "start" {
					ses, err = p.Start(context.Background(), runtime.StartOptions{Broker: b, RuntimeID: "run_taken"})
				} else {
					ses, err = p.ResumeWithOptions(context.Background(), "ses_refused", runtime.StartOptions{Broker: b, RuntimeID: "run_taken"})
				}
				if err != nil {
					t.Fatalf("%s failed: %v", mode, err)
				}
				if gotOpenRunID != "" || gotOpenToken != "" || gotOpenURL != "" {
					t.Fatalf("BuildSessionOpenOptions got (%q, %q, %q), want all empty on EnsureRun refusal", gotOpenRunID, gotOpenToken, gotOpenURL)
				}
				p.mu.Lock()
				owned := p.sessions[ses.SessionID]
				pollCount := len(p.pollCancels)
				p.mu.Unlock()
				if owned.RunID != "" || owned.BrokerToken != "" {
					t.Fatalf("owned session has RunID=%q Token=%q, want empty", owned.RunID, owned.BrokerToken)
				}
				if pollCount != 0 {
					t.Fatalf("pollCancels count = %d, want 0", pollCount)
				}
			})
		}
	})
}

func TestValidationAndErrorPrecedence(t *testing.T) {
	valErr := errors.New("AVENOR_AGY_TRANSPORT is no longer supported")
	var launchCalls atomic.Int32
	p := NewProvider(ProviderConfig{
		BackendID: "agy",
		ValidateStart: func(runtime.StartOptions) error {
			return valErr
		},
	}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		launchCalls.Add(1)
		return nil, errors.New("should not launch")
	}

	// 1. Empty sessionID on Resume/ResumeWithOptions takes precedence over ValidateStart.
	if _, err := p.Resume(context.Background(), ""); err == nil || err.Error() != "session id is required" {
		t.Fatalf("Resume empty sessionID = %v, want 'session id is required'", err)
	}
	if _, err := p.ResumeWithOptions(context.Background(), "", runtime.StartOptions{}); err == nil || err.Error() != "session id is required" {
		t.Fatalf("ResumeWithOptions empty sessionID = %v, want 'session id is required'", err)
	}

	// 2. ValidateStart runs on Start, Resume, and ResumeWithOptions before any process launch.
	if _, err := p.Start(context.Background(), runtime.StartOptions{}); !errors.Is(err, valErr) {
		t.Fatalf("Start err = %v, want %v", err, valErr)
	}
	if _, err := p.Resume(context.Background(), "ses_1"); !errors.Is(err, valErr) {
		t.Fatalf("Resume err = %v, want %v", err, valErr)
	}
	if _, err := p.ResumeWithOptions(context.Background(), "ses_1", runtime.StartOptions{}); !errors.Is(err, valErr) {
		t.Fatalf("ResumeWithOptions err = %v, want %v", err, valErr)
	}
	if launchCalls.Load() != 0 {
		t.Fatalf("launchCalls = %d, want 0", launchCalls.Load())
	}

	// 3. After Close, empty sessionID still wins on Resume, and errProviderClosed wins over ValidateStart.
	_ = p.Close()
	if _, err := p.Resume(context.Background(), ""); err == nil || err.Error() != "session id is required" {
		t.Fatalf("Resume empty sessionID after Close = %v, want 'session id is required'", err)
	}
	if _, err := p.Start(context.Background(), runtime.StartOptions{}); !errors.Is(err, errProviderClosed) {
		t.Fatalf("Start after Close = %v, want errProviderClosed", err)
	}
	if _, err := p.ResumeWithOptions(context.Background(), "ses_1", runtime.StartOptions{}); !errors.Is(err, errProviderClosed) {
		t.Fatalf("ResumeWithOptions after Close = %v, want errProviderClosed", err)
	}
}

func TestBuildClientArgsAndBuildSessionOpenOptionsErrors(t *testing.T) {
	t.Run("BuildClientArgs error fails before BuildClientEnv", func(t *testing.T) {
		argErr := errors.New("invalid client args")
		envCalled := false
		p := NewProvider(ProviderConfig{
			BackendID: "agy",
			BuildClientArgs: func(runtime.StartOptions, string) ([]string, error) {
				return nil, argErr
			},
			BuildClientEnv: func(runtime.StartOptions, string, string, string) (map[string]string, func() error) {
				envCalled = true
				return nil, nil
			},
		})
		if _, err := p.Start(context.Background(), runtime.StartOptions{}); !errors.Is(err, argErr) {
			t.Fatalf("Start err = %v, want %v", err, argErr)
		}
		if envCalled {
			t.Fatal("BuildClientEnv should not run when BuildClientArgs fails")
		}
	})

	t.Run("BuildSessionOpenOptions error fails Start and ResumeWithOptions before RPC or poller", func(t *testing.T) {
		openErr := errors.New("resolve executable failed")
		peer := newFakeInMemoryPeer(t)
		p := NewProvider(ProviderConfig{
			BackendID: "agy",
			BuildSessionOpenOptions: func(runtime.StartOptions, string, string, string) (SessionOpenOptions, error) {
				return SessionOpenOptions{}, openErr
			},
		}).(*Provider)
		p.startClient = func(context.Context, ClientConfig) (*Client, error) {
			return peer.client, nil
		}
		defer func() {
			close(peer.client.events)
			close(peer.client.done)
			_ = p.Close()
		}()

		if _, err := p.Start(context.Background(), runtime.StartOptions{}); !errors.Is(err, openErr) {
			t.Fatalf("Start err = %v, want %v", err, openErr)
		}
		if _, err := p.ResumeWithOptions(context.Background(), "ses_1", runtime.StartOptions{}); !errors.Is(err, openErr) {
			t.Fatalf("ResumeWithOptions err = %v, want %v", err, openErr)
		}
		if calls := peer.snapshotCalls(); len(calls) != 0 {
			t.Fatalf("peer calls = %#v, want none", calls)
		}
	})
}

func TestLaunchCompatibilityWarmAndConcurrentLoser(t *testing.T) {
	t.Run("warm reuse rejects Dir Agent and LaunchWrapper mismatches", func(t *testing.T) {
		dirA := t.TempDir()
		dirB := t.TempDir()
		peer := newFakeInMemoryPeer(t)

		var gotCfg ClientConfig
		p := NewProvider(ProviderConfig{
			BackendID: "agy",
			SetCmdDir: true,
			BuildClientArgs: func(opts runtime.StartOptions, absDir string) ([]string, error) {
				args := []string{"acp", "--stdio", "--add-dir", absDir}
				if opts.Agent != "" {
					args = append(args, "--agent", opts.Agent)
				}
				return args, nil
			},
		}).(*Provider)
		p.startClient = func(_ context.Context, cfg ClientConfig) (*Client, error) {
			gotCfg = cfg
			return peer.client, nil
		}
		defer func() {
			close(peer.client.events)
			close(peer.client.done)
			_ = p.Close()
		}()

		ses, err := p.Start(context.Background(), runtime.StartOptions{
			Dir:           dirA,
			Agent:         "jockey",
			LaunchWrapper: []string{"/bin/wrap", "--a"},
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if ses.Dir != dirA {
			t.Fatalf("ses.Dir = %q, want %q", ses.Dir, dirA)
		}
		if !gotCfg.SetCmdDir || gotCfg.Dir != dirA {
			t.Fatalf("ClientConfig Dir=%q SetCmdDir=%v, want %q true", gotCfg.Dir, gotCfg.SetCmdDir, dirA)
		}
		wantArgs := []string{"acp", "--stdio", "--add-dir", dirA, "--agent", "jockey"}
		if !slices.Equal(gotCfg.Args, wantArgs) {
			t.Fatalf("ClientConfig Args = %v, want %v", gotCfg.Args, wantArgs)
		}

		// Plain Resume with no per-call overrides succeeds and reports dirA.
		resumed, err := p.Resume(context.Background(), ses.SessionID)
		if err != nil {
			t.Fatalf("plain Resume: %v", err)
		}
		if resumed.Dir != dirA {
			t.Fatalf("resumed.Dir = %q, want %q", resumed.Dir, dirA)
		}

		// Mismatched Dir rejected.
		if _, err := p.ResumeWithOptions(context.Background(), ses.SessionID, runtime.StartOptions{Dir: dirB}); err == nil || !strings.Contains(err.Error(), "already running in dir") {
			t.Fatalf("expected dir mismatch error, got: %v", err)
		}
		// Mismatched Agent rejected.
		if _, err := p.ResumeWithOptions(context.Background(), ses.SessionID, runtime.StartOptions{Agent: "mule"}); err == nil || !strings.Contains(err.Error(), "already running with agent") {
			t.Fatalf("expected agent mismatch error, got: %v", err)
		}
		// Mismatched LaunchWrapper rejected.
		if _, err := p.ResumeWithOptions(context.Background(), ses.SessionID, runtime.StartOptions{LaunchWrapper: []string{"/bin/wrap", "--b"}}); err == nil || !strings.Contains(err.Error(), "already running with launch wrapper") {
			t.Fatalf("expected wrapper mismatch error, got: %v", err)
		}
	})

	t.Run("concurrent loser with incompatible launch spec cleans up and returns error", func(t *testing.T) {
		winnerPeer := newFakeInMemoryPeer(t)
		loserPeer := newFakeInMemoryPeer(t)
		close(loserPeer.client.events)
		close(loserPeer.client.done)

		bothEntered := make(chan struct{}, 2)
		releaseWinner := make(chan struct{})
		releaseLoser := make(chan struct{})
		var loserCleaned atomic.Bool

		p := NewProvider(ProviderConfig{
			BackendID: "agy",
			SetCmdDir: true,
			BuildClientArgs: func(opts runtime.StartOptions, absDir string) ([]string, error) {
				return []string{"acp", "--add-dir", absDir, "--agent", opts.Agent}, nil
			},
			BuildClientEnv: func(opts runtime.StartOptions, _, _, _ string) (map[string]string, func() error) {
				if opts.Agent == "loser-agent" {
					return map[string]string{"K": "loser"}, func() error {
						loserCleaned.Store(true)
						return nil
					}
				}
				return nil, nil
			},
		}).(*Provider)

		p.startClient = func(_ context.Context, cfg ClientConfig) (*Client, error) {
			bothEntered <- struct{}{}
			if slices.Contains(cfg.Args, "winner-agent") {
				<-releaseWinner
				return winnerPeer.client, nil
			}
			<-releaseLoser
			return loserPeer.client, nil
		}
		defer func() {
			close(winnerPeer.client.events)
			close(winnerPeer.client.done)
			_ = p.Close()
		}()

		winErrCh := make(chan error, 1)
		loseErrCh := make(chan error, 1)
		go func() {
			_, err := p.Start(context.Background(), runtime.StartOptions{Agent: "winner-agent"})
			winErrCh <- err
		}()
		go func() {
			_, err := p.Start(context.Background(), runtime.StartOptions{Agent: "loser-agent"})
			loseErrCh <- err
		}()

		<-bothEntered
		<-bothEntered
		close(releaseWinner)
		if err := <-winErrCh; err != nil {
			t.Fatalf("winner Start: %v", err)
		}
		close(releaseLoser)
		loseErr := <-loseErrCh
		if loseErr == nil || !strings.Contains(loseErr.Error(), "already running with agent") {
			t.Fatalf("loser Start err = %v, want agent mismatch error", loseErr)
		}
		if !loserCleaned.Load() {
			t.Fatal("expected loser env cleanup to run")
		}
	})
}

// TestCheckLaunchCompatGuardsDifferByMode pins the one difference between the
// warm and strict compatibility checks: the warm check only compares a field the
// launch set, while the strict check compares every field of a fully specified
// launch. Both modes still skip the dir comparison when the installed client
// recorded no dir.
func TestCheckLaunchCompatGuardsDifferByMode(t *testing.T) {
	installed := clientLaunchSpec{absDir: "/installed", agent: "jockey", wrapper: []string{"/bin/wrap", "--a"}}
	candidate := clientLaunchSpec{absDir: "/candidate", agent: "mule", wrapper: []string{"/bin/wrap", "--b"}}
	p := NewProvider(ProviderConfig{BackendID: "agy", SetCmdDir: true}).(*Provider)

	t.Run("warm skips fields the launch left unset", func(t *testing.T) {
		if err := p.checkWarmLaunchCompat(runtime.StartOptions{}, candidate, installed); err != nil {
			t.Fatalf("warm compat with no overrides = %v, want nil", err)
		}
	})

	cases := []struct {
		name string
		opts runtime.StartOptions
		want string
	}{
		{"dir", runtime.StartOptions{Dir: "/candidate"}, "already running in dir"},
		{"agent", runtime.StartOptions{Agent: "mule"}, "already running with agent"},
		{"wrapper", runtime.StartOptions{LaunchWrapper: []string{"/bin/wrap", "--b"}}, "already running with launch wrapper"},
	}
	for _, c := range cases {
		t.Run("warm compares the set "+c.name, func(t *testing.T) {
			if err := p.checkWarmLaunchCompat(c.opts, candidate, installed); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("warm compat err = %v, want %q", err, c.want)
			}
		})
	}

	t.Run("strict compares a field the launch left unset", func(t *testing.T) {
		// No opts.Dir: the warm check would skip the dir comparison, the strict
		// check still refuses the reuse.
		if err := p.checkConcurrentLaunchCompat(runtime.StartOptions{}, candidate, installed); err == nil || !strings.Contains(err.Error(), "already running in dir") {
			t.Fatalf("strict compat err = %v, want a dir mismatch", err)
		}
	})

	t.Run("both modes skip a dir the installed client never recorded", func(t *testing.T) {
		noDir := clientLaunchSpec{agent: "mule", wrapper: candidate.wrapper}
		if err := p.checkWarmLaunchCompat(runtime.StartOptions{Dir: "/candidate"}, candidate, noDir); err != nil {
			t.Fatalf("warm compat against an installed client with no dir = %v, want nil", err)
		}
		if err := p.checkConcurrentLaunchCompat(runtime.StartOptions{}, candidate, noDir); err != nil {
			t.Fatalf("strict compat against an installed client with no dir = %v, want nil", err)
		}
	})

	t.Run("backends that ignore dir and args skip both modes", func(t *testing.T) {
		q := NewProvider(ProviderConfig{BackendID: "gemini-acp"}).(*Provider)
		if err := q.checkWarmLaunchCompat(runtime.StartOptions{Dir: "/candidate"}, candidate, installed); err != nil {
			t.Fatalf("warm compat = %v, want nil for a backend with no per-run argv", err)
		}
		if err := q.checkConcurrentLaunchCompat(runtime.StartOptions{}, candidate, installed); err != nil {
			t.Fatalf("strict compat = %v, want nil for a backend with no per-run argv", err)
		}
	})
}

func TestColdWarmPlainAndOptionAwareResumeContract(t *testing.T) {
	b1 := broker.New("")
	if err := b1.Start(); err != nil {
		t.Fatalf("start b1: %v", err)
	}
	defer func() { _ = b1.Stop() }()

	b2 := broker.New("")
	if err := b2.Start(); err != nil {
		t.Fatalf("start b2: %v", err)
	}
	defer func() { _ = b2.Stop() }()

	peer := newFakeInMemoryPeer(t)
	var failConfigure atomic.Bool
	var envTokens, openTokens []string

	p := NewProvider(ProviderConfig{
		BackendID: "agy",
		BuildClientEnv: func(_ runtime.StartOptions, runID, token, url string) (map[string]string, func() error) {
			envTokens = append(envTokens, runID+":"+token+":"+url)
			return nil, nil
		},
		BuildSessionOpenOptions: func(_ runtime.StartOptions, runID, token, url string) (SessionOpenOptions, error) {
			openTokens = append(openTokens, runID+":"+token+":"+url)
			if runID == "" || token == "" || url == "" {
				return SessionOpenOptions{}, nil
			}
			return SessionOpenOptions{
				MCPServers: []MCPServer{
					{
						Name:    "avenor-channel-tools",
						Command: "/bin/avenor",
						Args:    []string{"channel-tools", "--run-id", runID, "--broker-url", url},
						Env:     []EnvVar{{Name: runtime.EnvBrokerToken, Value: token}},
					},
				},
			}, nil
		},
		ConfigureSession: func(_ context.Context, session *Session, opts runtime.StartOptions) error {
			if failConfigure.Load() {
				return errors.New("json-rpc session/set_config_option failed: Unknown model ID (-32602)")
			}
			return session.Client.SetSessionModel(context.Background(), session.SessionID, opts.Model)
		},
	}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		return peer.client, nil
	}
	defer func() {
		close(peer.client.events)
		close(peer.client.done)
		_ = p.Close()
	}()

	// 1. Cold ResumeWithOptions computes launchRun ONCE and passes identical identity to BuildClientEnv and BuildSessionOpenOptions.
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	resumed, err := p.ResumeWithOptions(ctx1, "ses_cold_opt", runtime.StartOptions{
		Broker:    b1,
		RuntimeID: "run_cold_1",
		Model:     "gemini-4-argon-high",
	})
	if err != nil {
		t.Fatalf("cold ResumeWithOptions: %v", err)
	}
	if resumed.SessionID != "ses_cold_opt" {
		t.Fatalf("SessionID = %q, want ses_cold_opt", resumed.SessionID)
	}
	if len(envTokens) != 1 || len(openTokens) != 1 || envTokens[0] != openTokens[0] {
		t.Fatalf("envTokens=%v openTokens=%v, want 1 matching entry each", envTokens, openTokens)
	}
	p.mu.Lock()
	owned := p.sessions["ses_cold_opt"]
	gen1 := p.pollGen["ses_cold_opt"]
	p.mu.Unlock()
	if owned == nil || owned.RunID != "run_cold_1" || owned.BrokerToken == "" {
		t.Fatalf("owned session = %#v, want RunID=run_cold_1 with token", owned)
	}
	if gen1 == 0 {
		t.Fatal("expected active poller generation for ses_cold_opt")
	}

	// 2. Warm Resume and warm ResumeWithOptions reuse owned identity and live poller.
	if _, err := p.Resume(context.Background(), "ses_cold_opt"); err != nil {
		t.Fatalf("warm plain Resume: %v", err)
	}
	p.mu.Lock()
	genAfterWarm := p.pollGen["ses_cold_opt"]
	ownedAfterWarm := p.sessions["ses_cold_opt"]
	p.mu.Unlock()
	if genAfterWarm != gen1 {
		t.Fatalf("warm Resume changed live poller gen from %d to %d", gen1, genAfterWarm)
	}
	if ownedAfterWarm.RunID != "run_cold_1" || ownedAfterWarm.BrokerToken != owned.BrokerToken {
		t.Fatalf("warm Resume lost owned session identity: %#v", ownedAfterWarm)
	}

	// 3. If the prior turn's context ends so the poller exits, a subsequent warm Resume restarts the poller cleanly.
	cancel1()
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		g := p.pollGen["ses_cold_opt"]
		p.mu.Unlock()
		if g == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for cancelled poller to exit")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := p.ResumeWithOptions(context.Background(), "ses_cold_opt", runtime.StartOptions{}); err != nil {
		t.Fatalf("warm ResumeWithOptions after poller exit: %v", err)
	}
	p.mu.Lock()
	genRestarted := p.pollGen["ses_cold_opt"]
	p.mu.Unlock()
	if genRestarted <= gen1 {
		t.Fatalf("expected restarted poller gen > %d, got %d", gen1, genRestarted)
	}

	// 4. Warm ResumeWithOptions rejects conflicting RuntimeID or Broker without modifying owned session.
	if _, err := p.ResumeWithOptions(context.Background(), "ses_cold_opt", runtime.StartOptions{RuntimeID: "run_other"}); err == nil || !strings.Contains(err.Error(), "already bound to run") {
		t.Fatalf("expected run mismatch error, got: %v", err)
	}
	if _, err := p.ResumeWithOptions(context.Background(), "ses_cold_opt", runtime.StartOptions{Broker: b2}); err == nil || !strings.Contains(err.Error(), "already bound to a different broker") {
		t.Fatalf("expected broker mismatch error, got: %v", err)
	}

	// 5. Failed ConfigureSession on WARM resume preserves existing p.sessions[sessionID] and poller without calling session/close.
	callsBeforeWarmFail := len(peer.snapshotCalls())
	failConfigure.Store(true)
	if _, err := p.ResumeWithOptions(context.Background(), "ses_cold_opt", runtime.StartOptions{Model: "bad-model"}); err == nil || !strings.Contains(err.Error(), "-32602") {
		t.Fatalf("expected -32602 configure error on warm resume, got: %v", err)
	}
	p.mu.Lock()
	stillOwned := p.sessions["ses_cold_opt"]
	stillGen := p.pollGen["ses_cold_opt"]
	p.mu.Unlock()
	if stillOwned == nil || stillOwned.RunID != "run_cold_1" || stillGen != genRestarted {
		t.Fatalf("failed warm resume disturbed owned session (%#v) or poller (%d vs %d)", stillOwned, stillGen, genRestarted)
	}
	for _, c := range peer.snapshotCalls()[callsBeforeWarmFail:] {
		if c.Method == "session/close" {
			t.Fatal("failed warm resume should not send session/close for an already-owned live session")
		}
	}

	// 6. Failed ConfigureSession on Start or COLD Resume sends best-effort session/close and installs no session or poller.
	callsBeforeColdFail := len(peer.snapshotCalls())
	if _, err := p.Start(context.Background(), runtime.StartOptions{Model: "bad-model"}); err == nil || !strings.Contains(err.Error(), "-32602") {
		t.Fatalf("expected -32602 configure error on Start, got: %v", err)
	}
	sawClose := false
	for _, c := range peer.snapshotCalls()[callsBeforeColdFail:] {
		if c.Method == "session/close" {
			sawClose = true
		}
	}
	if !sawClose {
		t.Fatal("expected failed Start configureSession to send best-effort session/close")
	}
	p.mu.Lock()
	totalSessions := len(p.sessions)
	p.mu.Unlock()
	if totalSessions != 1 {
		t.Fatalf("sessions len = %d, want 1 (only ses_cold_opt)", totalSessions)
	}
}

func TestEventHubOrderingFinalChunkBeforeSessionEndBeforeEOF(t *testing.T) {
	// Fake ACP server that writes a final agent_message_chunk, the session/prompt
	// response (with extended usage), and closes stdout (EOF) back-to-back.
	script := filepath.Join(t.TempDir(), "fast-eof-peer.sh")
	body := `#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"ses_order"}}\n' "$id"
      ;;
    *'"method":"session/prompt"'*)
      printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_order","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"DONE"}}}}\n'
      printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn","usage":{"inputTokens":11,"outputTokens":7,"totalTokens":18,"cachedReadTokens":2,"cachedWriteTokens":3,"thoughtTokens":5}}}\n' "$id"
      exit 0
      ;;
  esac
done
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write fake peer: %v", err)
	}

	for iter := 0; iter < 15; iter++ {
		p := NewProvider(ProviderConfig{
			BackendID:        "agy",
			Bin:              script,
			EmitSessionStart: true,
			Opts:             runtime.StartOptions{Model: "gemini-4-argon-high"},
		}).(*Provider)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		ses, err := p.Start(ctx, runtime.StartOptions{})
		if err != nil {
			cancel()
			t.Fatalf("iter %d Start: %v", iter, err)
		}

		// Subscribe AFTER Start to verify pre-subscribe backlog delivers session.start first.
		evCh, err := p.Events(ctx, ses.SessionID)
		if err != nil {
			cancel()
			t.Fatalf("iter %d Events: %v", iter, err)
		}

		if err := p.Prompt(ctx, ses.SessionID, "Say DONE"); err != nil {
			cancel()
			t.Fatalf("iter %d Prompt: %v", iter, err)
		}

		var got []events.Event
		for ev := range evCh {
			got = append(got, ev)
		}
		cancel()
		_ = p.Close()

		wantKinds := []string{"session.start", "agent.message_chunk", "session.end"}
		if len(got) != len(wantKinds) {
			t.Fatalf("iter %d got events %+v, want kinds %v", iter, got, wantKinds)
		}
		for i, want := range wantKinds {
			if got[i].Event != want {
				t.Fatalf("iter %d event[%d] = %q, want %q (all: %+v)", iter, i, got[i].Event, want, got)
			}
		}
		usage, ok := got[2].Fields["usage"].(map[string]any)
		if !ok {
			t.Fatalf("iter %d session.end missing usage map: %+v", iter, got[2])
		}
		if usage["cached_write_tokens"] != float64(3) || usage["thought_tokens"] != float64(5) {
			t.Fatalf("iter %d unexpected extended usage: %+v", iter, usage)
		}
	}
}

func TestEventHubFanOutBacklogDeepCloneStalledSubscriberAndLateSubscribe(t *testing.T) {
	peer := newFakeInMemoryPeer(t)
	p := NewProvider(ProviderConfig{
		BackendID:        "agy",
		EmitSessionStart: true,
		Opts:             runtime.StartOptions{Model: "gemini-4-argon-high"},
	}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		return peer.client, nil
	}

	// Pre-Start Events() must return "provider has not been started".
	if _, err := p.Events(context.Background(), "ses_1"); err == nil || !strings.Contains(err.Error(), "provider has not been started") {
		t.Fatalf("pre-Start Events() err = %v, want 'provider has not been started'", err)
	}

	ses, err := p.Start(context.Background(), runtime.StartOptions{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// First subscriber receives the pre-subscribe backlog (session.start).
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	sub1, err := p.Events(ctx1, ses.SessionID)
	if err != nil {
		t.Fatalf("sub1 Events: %v", err)
	}
	select {
	case ev := <-sub1:
		if ev.Event != "session.start" || ev.Fields["backend"] != "agy" || ev.Fields["model"] != "gemini-4-argon-high" {
			t.Fatalf("sub1 first event = %+v, want session.start", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for session.start backlog on sub1")
	}

	// Second subscriber is future-only (does not receive the already-drained backlog).
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	sub2, err := p.Events(ctx2, ses.SessionID)
	if err != nil {
		t.Fatalf("sub2 Events: %v", err)
	}
	select {
	case ev := <-sub2:
		t.Fatalf("sub2 received historical backlog event: %+v", ev)
	default:
	}

	// Third subscriber will be intentionally stalled (never reading) to verify
	// streaming chunk overflow drops without blocking sub1 and sub2.
	ctxStalled, cancelStalled := context.WithCancel(context.Background())
	defer cancelStalled()
	_, err = p.Events(ctxStalled, ses.SessionID)
	if err != nil {
		t.Fatalf("stalled sub Events: %v", err)
	}

	// Emit > subscriberBufferCap streaming chunks with nested maps/slices and
	// mutate them concurrently in sub1 and sub2 to prove deep-clone isolation.
	const totalChunks = subscriberBufferCap + 40
	var wg sync.WaitGroup
	wg.Add(2)
	consumeAndMutate := func(ch <-chan events.Event, tag string) {
		defer wg.Done()
		for i := 0; i < totalChunks; i++ {
			select {
			case ev := <-ch:
				if _, exists := ev.Fields["mutated_by"]; exists {
					t.Errorf("%s saw pre-mutated field on chunk %d: %+v", tag, i, ev.Fields)
					return
				}
				nested, ok := ev.Fields["content"].(map[string]any)
				if !ok || nested["text"] != "hello" {
					t.Errorf("%s saw corrupted nested content on chunk %d: %+v", tag, i, ev.Fields["content"])
					return
				}
				list, ok := ev.Fields["items"].([]any)
				if !ok || len(list) == 0 || list[0] != "a" {
					t.Errorf("%s saw corrupted nested slice on chunk %d: %+v", tag, i, ev.Fields["items"])
					return
				}
				ev.Fields["mutated_by"] = tag
				nested["text"] = tag
				list[0] = tag
			case <-time.After(5 * time.Second):
				t.Errorf("%s timed out at chunk %d", tag, i)
				return
			}
		}
	}
	go consumeAndMutate(sub1, "sub1")
	go consumeAndMutate(sub2, "sub2")

	for i := 0; i < totalChunks; i++ {
		peer.client.events <- events.Event{
			Event:     "agent.message_chunk",
			SessionID: ses.SessionID,
			Fields: map[string]any{
				"content": map[string]any{"type": "text", "text": "hello"},
				"items":   []any{"a", "b"},
			},
		}
	}
	wg.Wait()

	// Cancelling sub2's context unregisters and closes sub2 cleanly without affecting sub1.
	cancel2()
	select {
	case _, ok := <-sub2:
		if ok {
			t.Fatal("expected sub2 channel to close after ctx cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sub2 channel close after ctx cancel")
	}

	// Cancel stalled subscriber so critical event delivery doesn't wait on it,
	// then emit a critical permission.request and verify sub1 receives it and caches options ONCE.
	cancelStalled()
	peer.client.events <- events.Event{
		Event:     "permission.request",
		SessionID: ses.SessionID,
		Fields: map[string]any{
			"request_id": "req_hub_1",
			"options":    []any{map[string]any{"optionId": "allow_once", "kind": "allow_once"}},
		},
	}
	select {
	case ev := <-sub1:
		if ev.Event != "permission.request" {
			t.Fatalf("sub1 event = %+v, want permission.request", ev)
		}
		// Mutate the subscriber's copy of options and verify cached pendingOptions stayed isolated.
		if opts, ok := ev.Fields["options"].([]any); ok && len(opts) > 0 {
			if m, ok := opts[0].(map[string]any); ok {
				m["optionId"] = "mutated_by_sub1"
			}
		}
		p.mu.Lock()
		cached := p.pendingOptions[ses.SessionID]["req_hub_1"]
		p.mu.Unlock()
		if len(cached) != 1 {
			t.Fatalf("cached pendingOptions[req_hub_1] = %+v, want 1 option", cached)
		}
		if m, ok := cached[0].(map[string]any); !ok || m["optionId"] != "allow_once" {
			t.Fatalf("cached pendingOptions corrupted by subscriber mutation: %+v", cached)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for permission.request on sub1")
	}

	// Close client events (EOF) and verify sub1 closes and late subscribe returns an immediately-closed channel.
	close(peer.client.events)
	close(peer.client.done)
	select {
	case _, ok := <-sub1:
		if ok {
			t.Fatal("expected sub1 to close on client EOF")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sub1 close on client EOF")
	}

	lateCh, err := p.Events(context.Background(), ses.SessionID)
	if err != nil {
		t.Fatalf("late Events() after EOF: %v", err)
	}
	select {
	case _, ok := <-lateCh:
		if ok {
			t.Fatal("expected late subscriber channel to be immediately closed")
		}
	default:
		t.Fatal("late subscriber channel was not closed immediately")
	}

	// Repeated Close is safe and late Events() after Close also returns an immediately-closed channel.
	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	afterCloseCh, err := p.Events(context.Background(), ses.SessionID)
	if err != nil {
		t.Fatalf("Events() after Close: %v", err)
	}
	select {
	case _, ok := <-afterCloseCh:
		if ok {
			t.Fatal("expected subscriber channel after Close to be immediately closed")
		}
	default:
		t.Fatal("subscriber channel after Close was not closed immediately")
	}
}

func TestOverlappingPromptRejectedAndCancelIdempotent(t *testing.T) {
	peer := newFakeInMemoryPeer(t)
	promptEntered := make(chan struct{})
	releasePrompt := make(chan struct{})

	peer.onCall = func(msg rpcEnvelope, _ map[string]any) *rpcEnvelope {
		if msg.Method == "session/prompt" {
			select {
			case <-promptEntered:
			default:
				close(promptEntered)
			}
			<-releasePrompt
			return &rpcEnvelope{
				ID:     msg.ID,
				Result: json.RawMessage(`{"stopReason":"end_turn"}`),
			}
		}
		return nil
	}

	p := NewProvider(ProviderConfig{BackendID: "agy"}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		return peer.client, nil
	}
	defer func() {
		close(peer.client.events)
		close(peer.client.done)
		_ = p.Close()
	}()

	ses, err := p.Start(context.Background(), runtime.StartOptions{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- p.Prompt(context.Background(), ses.SessionID, "first turn")
	}()
	<-promptEntered

	// Overlapping Prompt on the same session must be rejected cleanly.
	if err := p.Prompt(context.Background(), ses.SessionID, "overlapping turn"); err == nil || !strings.Contains(err.Error(), "prompt already in flight") {
		t.Fatalf("overlapping Prompt err = %v, want 'prompt already in flight'", err)
	}

	// Cancel with an already-cancelled ctx returns ctx.Err() first.
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Cancel(cancelledCtx, ses.SessionID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Cancel with cancelled ctx = %v, want context.Canceled", err)
	}

	// Calling Cancel twice is idempotent.
	if err := p.Cancel(context.Background(), ses.SessionID); err != nil {
		t.Fatalf("first Cancel: %v", err)
	}
	if err := p.Cancel(context.Background(), ses.SessionID); err != nil {
		t.Fatalf("second Cancel: %v", err)
	}

	close(releasePrompt)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Prompt: %v", err)
	}

	// Follow-up sequential Prompt succeeds after the first completes.
	if err := p.Prompt(context.Background(), ses.SessionID, "second turn"); err != nil {
		t.Fatalf("second sequential Prompt: %v", err)
	}
}

func TestShutdownWithFloodedClientUpdatesDoesNotHang(t *testing.T) {
	peer := newFakeInMemoryPeer(t)
	peer.client.stopEvents = make(chan struct{})
	p := NewProvider(ProviderConfig{BackendID: "agy"}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		return peer.client, nil
	}

	if _, err := p.Start(context.Background(), runtime.StartOptions{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Simulate the client readLoop still emitting >128 updates while Close runs.
	floodDone := make(chan struct{})
	go func() {
		defer close(floodDone)
		for i := 0; i < 300; i++ {
			peer.client.dispatch(rpcEnvelope{
				JSONRPC: "2.0",
				Method:  "session/update",
				Params:  mustRaw(map[string]any{"sessionId": "ses_1", "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "x"}}}),
			})
		}
		close(peer.client.events)
		close(peer.client.done)
	}()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- p.Close()
	}()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Provider.Close hung while client was emitting updates")
	}
	select {
	case <-floodDone:
	case <-time.After(3 * time.Second):
		t.Fatal("client dispatch flood did not unblock on Close")
	}
}

func TestSlowSubscriberPreservesAllMessageChunksAndBacklogKeepsPermission(t *testing.T) {
	peer := newFakeInMemoryPeer(t)
	p := NewProvider(ProviderConfig{BackendID: "agy"}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		return peer.client, nil
	}

	ses, err := p.Start(context.Background(), runtime.StartOptions{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Emit > hubBacklogCap chunks BEFORE the first subscriber, then a permission.request.
	for i := 0; i < hubBacklogCap+32; i++ {
		peer.client.events <- events.Event{
			Event:     "agent.message_chunk",
			SessionID: ses.SessionID,
			Fields:    map[string]any{"index": i},
		}
	}
	peer.client.events <- events.Event{
		Event:     "permission.request",
		SessionID: ses.SessionID,
		Fields: map[string]any{
			"request_id": "req_backlog_1",
			"options":    []any{map[string]any{"optionId": "allow_once", "kind": "allow_once"}},
		},
	}

	// Synchronize on pumpClientEvents having processed the permission.request.
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		_, cached := p.pendingOptions[ses.SessionID]["req_backlog_1"]
		p.mu.Unlock()
		if cached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for permission.request in backlog")
		}
		time.Sleep(5 * time.Millisecond)
	}

	sub, err := p.Events(context.Background(), ses.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	// Now flood > subscriberBufferCap chunks WITHOUT reading yet, then close client (EOF)
	// and verify the slow subscriber receives the preserved permission.request AND every post-subscribe chunk.
	const burst = subscriberBufferCap + 75
	for i := 0; i < burst; i++ {
		peer.client.events <- events.Event{
			Event:     "agent.message_chunk",
			SessionID: ses.SessionID,
			Fields:    map[string]any{"burst": i},
		}
	}
	close(peer.client.events)
	close(peer.client.done)

	sawPerm := false
	gotBurst := 0
	for ev := range sub {
		if ev.Event == "permission.request" && ev.Fields["request_id"] == "req_backlog_1" {
			sawPerm = true
		}
		if ev.Event == "agent.message_chunk" {
			if idx, ok := ev.Fields["burst"].(int); ok {
				if idx != gotBurst {
					t.Fatalf("out-of-order or dropped burst chunk: got %d, want %d", idx, gotBurst)
				}
				gotBurst++
			}
		}
	}
	if !sawPerm {
		t.Fatal("pre-subscribe backlog dropped permission.request")
	}
	if gotBurst != burst {
		t.Fatalf("slow subscriber received %d/%d burst message chunks", gotBurst, burst)
	}
	_ = p.Close()
}

// TestBacklogCapHoldsWithoutDroppableEvents pins the backlog bound when every
// retained event is non-droppable. Lifecycle and permission events cannot be
// skipped the way chunks are, but the cap still has to hold: a provider whose
// sessions are never subscribed keeps its backlog until Close, and an unbounded
// one grows for the life of the process.
func TestBacklogCapHoldsWithoutDroppableEvents(t *testing.T) {
	h := newEventHub()
	total := hubBacklogCap + 16

	h.mu.Lock()
	for i := 0; i < total; i++ {
		h.appendBacklogLocked(events.Event{
			Event:     "permission.request",
			SessionID: "ses_cap",
			Fields:    map[string]any{"request_id": "req_" + strconv.Itoa(i)},
		})
	}
	got := len(h.backlog)
	oldest, _ := h.backlog[0].Fields["request_id"].(string)
	newest, _ := h.backlog[got-1].Fields["request_id"].(string)
	h.mu.Unlock()

	if got != hubBacklogCap {
		t.Fatalf("backlog len = %d after %d non-droppable events, want the cap %d", got, total, hubBacklogCap)
	}
	if want := "req_" + strconv.Itoa(total-1); newest != want {
		t.Fatalf("backlog newest = %q, want the last appended event %q", newest, want)
	}
	if want := "req_" + strconv.Itoa(total-hubBacklogCap); oldest != want {
		t.Fatalf("backlog oldest = %q, want %q: the cap keeps the newest window", oldest, want)
	}
}

func TestColdPlainResumeUsesConstructorBrokerAndOpenOptions(t *testing.T) {
	peer := newFakeInMemoryPeer(t)
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer func() { _ = b.Stop() }()

	var gotOpenRunID, gotOpenToken, gotOpenURL string
	var gotEnvRunID, gotEnvToken, gotEnvURL string
	p := NewProvider(ProviderConfig{
		BackendID: "agy",
		Opts: runtime.StartOptions{
			Broker:    b,
			RuntimeID: "rt_cold_plain",
			Label:     "plain-resume",
		},
		BuildSessionOpenOptions: func(_ runtime.StartOptions, runID, token, url string) (SessionOpenOptions, error) {
			gotOpenRunID, gotOpenToken, gotOpenURL = runID, token, url
			return SessionOpenOptions{
				MCPServers: []MCPServer{{Name: "avenor-channel-tools", Command: "/bin/true"}},
			}, nil
		},
		BuildClientEnv: func(_ runtime.StartOptions, runID, token, url string) (map[string]string, func() error) {
			gotEnvRunID, gotEnvToken, gotEnvURL = runID, token, url
			return map[string]string{"TEST_RUN": runID}, nil
		},
	}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		return peer.client, nil
	}
	defer func() {
		close(peer.client.events)
		close(peer.client.done)
		_ = p.Close()
	}()

	if _, err := p.Resume(context.Background(), "ses_cold_plain"); err != nil {
		t.Fatalf("cold plain Resume: %v", err)
	}
	if gotOpenRunID != "rt_cold_plain" || gotOpenToken == "" || gotOpenURL != b.URL() {
		t.Fatalf("BuildSessionOpenOptions got (%q, %q, %q), want runID=rt_cold_plain with token and broker URL", gotOpenRunID, gotOpenToken, gotOpenURL)
	}
	if gotEnvRunID != "rt_cold_plain" || gotEnvToken != gotOpenToken || gotEnvURL != b.URL() {
		t.Fatalf("BuildClientEnv got (%q, %q, %q), want matching broker creds", gotEnvRunID, gotEnvToken, gotEnvURL)
	}
	p.mu.Lock()
	gen := p.pollGen["ses_cold_plain"]
	p.mu.Unlock()
	if gen == 0 {
		t.Fatal("expected broker poller to start on cold plain Resume")
	}
}

func TestQueuedMessageInjectionSerializesWithPromptAndRequeuesOnError(t *testing.T) {
	peer := newFakeInMemoryPeer(t)
	var inFlight atomic.Int32
	var overlap atomic.Bool
	injectEntered := make(chan struct{})
	releaseInject := make(chan struct{})
	var callNum atomic.Int32

	peer.onCall = func(msg rpcEnvelope, _ map[string]any) *rpcEnvelope {
		if msg.Method != "session/prompt" {
			return nil
		}
		n := callNum.Add(1)
		if inFlight.Add(1) > 1 {
			overlap.Store(true)
		}
		defer inFlight.Add(-1)
		if n == 2 {
			close(injectEntered)
			<-releaseInject
			return &rpcEnvelope{
				ID:    msg.ID,
				Error: &rpcError{Code: -32000, Message: "transient failure"},
			}
		}
		return &rpcEnvelope{
			ID:     msg.ID,
			Result: json.RawMessage(`{"stopReason":"end_turn"}`),
		}
	}

	p := NewProvider(ProviderConfig{BackendID: "agy"}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		return peer.client, nil
	}
	defer func() {
		close(peer.client.events)
		close(peer.client.done)
		_ = p.Close()
	}()

	ses, err := p.Start(context.Background(), runtime.StartOptions{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Queue a message before ending turn 1 so session.end triggers injection.
	p.Queue.Enqueue(ses.SessionID, "queued-msg-1")
	if err := p.Prompt(context.Background(), ses.SessionID, "turn-1"); err != nil {
		t.Fatalf("turn-1 Prompt: %v", err)
	}

	<-injectEntered
	// While injection is running, an external Prompt must be rejected instead of overlapping.
	if err := p.Prompt(context.Background(), ses.SessionID, "overlapping-external"); err == nil || !strings.Contains(err.Error(), "prompt already in flight") {
		t.Fatalf("Prompt during injection err = %v, want 'prompt already in flight'", err)
	}
	close(releaseInject)

	// Wait for injection to finish and verify the failed message was re-enqueued.
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		reserved := p.injectReserved[ses.SessionID]
		active := p.activePrompts[ses.SessionID]
		p.mu.Unlock()
		if reserved == 0 && active == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for injection cleanup")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if overlap.Load() {
		t.Fatal("observed overlapping session/prompt calls between Prompt and injection")
	}
	requeued, done := p.Queue.Take(ses.SessionID)
	if done != nil {
		done()
	}
	if !slices.Equal(requeued, []string{"queued-msg-1"}) {
		t.Fatalf("requeued = %v, want [queued-msg-1]", requeued)
	}
}

func TestSubscribeCancelledContextAndCloseRaceFree(t *testing.T) {
	for i := 0; i < 20; i++ {
		peer := newFakeInMemoryPeer(t)
		p := NewProvider(ProviderConfig{BackendID: "agy"}).(*Provider)
		p.startClient = func(context.Context, ClientConfig) (*Client, error) {
			return peer.client, nil
		}
		ses, err := p.Start(context.Background(), runtime.StartOptions{})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, _ = p.Events(ctx, ses.SessionID)
		}()
		go func() {
			defer wg.Done()
			close(peer.client.events)
			close(peer.client.done)
			_ = p.Close()
		}()
		wg.Wait()
	}
}

func TestMultiSessionEventHubPreservesPerSessionBacklog(t *testing.T) {
	peer := newFakeInMemoryPeer(t)
	var nextSes atomic.Int32
	peer.onCall = func(msg rpcEnvelope, _ map[string]any) *rpcEnvelope {
		if msg.Method == "session/new" {
			n := nextSes.Add(1)
			return &rpcEnvelope{
				ID:     msg.ID,
				Result: mustRaw(map[string]any{"sessionId": "ses_" + strconv.Itoa(int(n))}),
			}
		}
		return nil
	}

	p := NewProvider(ProviderConfig{
		BackendID:        "agy",
		EmitSessionStart: true,
		Opts:             runtime.StartOptions{Model: "gemini-4-argon-high"},
	}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		return peer.client, nil
	}
	defer func() {
		close(peer.client.events)
		close(peer.client.done)
		_ = p.Close()
	}()

	ses1, err := p.Start(context.Background(), runtime.StartOptions{})
	if err != nil {
		t.Fatalf("Start ses1: %v", err)
	}
	sub1, err := p.Events(context.Background(), ses1.SessionID)
	if err != nil {
		t.Fatalf("Events ses1: %v", err)
	}
	select {
	case ev := <-sub1:
		if ev.Event != "session.start" || ev.SessionID != ses1.SessionID {
			t.Fatalf("sub1 first event = %+v, want session.start for %s", ev, ses1.SessionID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ses1 session.start")
	}

	// Starting a second session after ses1 has already subscribed must still
	// buffer ses2's session.start until ses2's first subscriber attaches.
	ses2, err := p.Start(context.Background(), runtime.StartOptions{})
	if err != nil {
		t.Fatalf("Start ses2: %v", err)
	}
	sub2, err := p.Events(context.Background(), ses2.SessionID)
	if err != nil {
		t.Fatalf("Events ses2: %v", err)
	}
	select {
	case ev := <-sub2:
		if ev.Event != "session.start" || ev.SessionID != ses2.SessionID {
			t.Fatalf("sub2 first event = %+v, want session.start for %s", ev, ses2.SessionID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ses2 session.start")
	}
}

// Thinking is scoped: a provider constructed with a default level applies it to
// a fresh start, an explicit resume overrides it for that call, and a resume
// that names no level sends none so a constructor default cannot leak into it.
func TestProviderThinkingScopeStartVsResume(t *testing.T) {
	peer := newFakeInMemoryPeer(t)
	var configured []runtime.StartOptions
	p := NewProvider(ProviderConfig{
		BackendID: "agy",
		Opts:      runtime.StartOptions{Thinking: "low"},
		ConfigureSession: func(_ context.Context, _ *Session, opts runtime.StartOptions) error {
			configured = append(configured, opts)
			return nil
		},
	}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		return peer.client, nil
	}
	defer func() {
		close(peer.client.events)
		close(peer.client.done)
		_ = p.Close()
	}()

	last := func() runtime.StartOptions {
		t.Helper()
		if len(configured) == 0 {
			t.Fatal("ConfigureSession was never called")
		}
		return configured[len(configured)-1]
	}
	ctx := context.Background()

	ses, err := p.Start(ctx, runtime.StartOptions{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := last().Thinking; got != "low" {
		t.Fatalf("fresh Start Thinking = %q, want the constructor default low", got)
	}

	if _, err := p.ResumeWithOptions(ctx, ses.SessionID, runtime.StartOptions{Thinking: "high"}); err != nil {
		t.Fatalf("ResumeWithOptions(thinking high): %v", err)
	}
	if got := last().Thinking; got != "high" {
		t.Fatalf("ResumeWithOptions Thinking = %q, want the explicit high", got)
	}

	if _, err := p.ResumeWithOptions(ctx, ses.SessionID, runtime.StartOptions{}); err != nil {
		t.Fatalf("ResumeWithOptions(empty): %v", err)
	}
	if got := last().Thinking; got != "" {
		t.Fatalf("ResumeWithOptions without thinking = %q, want empty", got)
	}

	if _, err := p.Resume(ctx, ses.SessionID); err != nil {
		t.Fatalf("plain Resume: %v", err)
	}
	if got := last().Thinking; got != "" {
		t.Fatalf("plain Resume Thinking = %q, want empty", got)
	}
}

// The optionAware flag is the gate, not the presence of callOpts: a resume that
// is not option-aware never applies per-call thinking even when its caller
// passes non-empty call options.
func TestProviderThinkingResumeIgnoresCallOptsWhenNotOptionAware(t *testing.T) {
	peer := newFakeInMemoryPeer(t)
	var configured []runtime.StartOptions
	p := NewProvider(ProviderConfig{
		BackendID: "agy",
		ConfigureSession: func(_ context.Context, _ *Session, opts runtime.StartOptions) error {
			configured = append(configured, opts)
			return nil
		},
	}).(*Provider)
	p.startClient = func(context.Context, ClientConfig) (*Client, error) {
		return peer.client, nil
	}
	defer func() {
		close(peer.client.events)
		close(peer.client.done)
		_ = p.Close()
	}()

	ctx := context.Background()
	ses, err := p.Start(ctx, runtime.StartOptions{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	callOpts := runtime.StartOptions{Thinking: "high"}
	if _, err := p.resume(ctx, ses.SessionID, callOpts, runtime.MergeStartOptions(p.opts, callOpts), false); err != nil {
		t.Fatalf("non-option-aware resume: %v", err)
	}
	if len(configured) == 0 {
		t.Fatal("ConfigureSession was never called")
	}
	if got := configured[len(configured)-1].Thinking; got != "" {
		t.Fatalf("non-option-aware resume Thinking = %q, want empty", got)
	}
}
