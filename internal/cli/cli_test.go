package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestEffectivePermissionHandlerAutoApproveSkipsDerivedSentinelHandler(t *testing.T) {
	sentinel := filepath.Join(t.TempDir(), "run.done")

	if got := effectivePermissionHandler(sentinel, "", true); got != "" {
		t.Fatalf("effectivePermissionHandler(autoApprove=true) = %q, want empty", got)
	}
	if got := effectivePermissionHandler(sentinel, "", false); got != "file:"+DerivePermBase(sentinel) {
		t.Fatalf("effectivePermissionHandler(autoApprove=false) = %q, want derived file handler", got)
	}
	if got := effectivePermissionHandler(sentinel, "file:/tmp/custom.perm", true); got != "file:/tmp/custom.perm" {
		t.Fatalf("effectivePermissionHandler(explicit, autoApprove=true) = %q, want explicit handler", got)
	}
}

type cliFakeProvider struct {
	answerSessionID string
	answerRequestID string
	answerResponse  runtime.PermissionResponse
	cancelSessionID string
}

type directConflictProvider struct {
	mu      sync.Mutex
	events  chan events.Event
	starts  int
	resumes int
	prompts int
	noStart bool
}

func (p *directConflictProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.starts++
	p.events = make(chan events.Event, 2)
	return runtime.Session{SessionID: "direct-provisional"}, nil
}

func (p *directConflictProvider) Resume(context.Context, string) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resumes++
	p.events = make(chan events.Event, 2)
	return runtime.Session{SessionID: "direct-retry-provisional"}, nil
}

func (p *directConflictProvider) Prompt(context.Context, string, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prompts++
	if !p.noStart {
		p.events <- events.Event{Event: "session.start", Fields: map[string]any{"conversation_id": "occupied"}}
	}
	p.events <- events.Event{Event: "session.end", Fields: map[string]any{"stop_reason": "end_turn"}}
	close(p.events)
	return nil
}

func (*directConflictProvider) Cancel(context.Context, string) error { return nil }
func (p *directConflictProvider) Events(context.Context, string) (<-chan events.Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.events, nil
}
func (*directConflictProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}
func (*directConflictProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

type permissionLifecycleProvider struct {
	answers chan string
}

func (p *permissionLifecycleProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	return runtime.Session{}, nil
}
func (p *permissionLifecycleProvider) Resume(context.Context, string) (runtime.Session, error) {
	return runtime.Session{}, nil
}
func (p *permissionLifecycleProvider) Prompt(context.Context, string, string) error { return nil }
func (p *permissionLifecycleProvider) Cancel(context.Context, string) error         { return nil }
func (p *permissionLifecycleProvider) Events(context.Context, string) (<-chan events.Event, error) {
	return nil, nil
}
func (p *permissionLifecycleProvider) AnswerPermission(_ context.Context, _ string, requestID string, _ runtime.PermissionResponse) error {
	p.answers <- requestID
	return nil
}
func (p *permissionLifecycleProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

type permissionLifecycleSink struct {
	responses chan string
	onRequest func()
}

type permissionRequestCaptureSink struct {
	mu           sync.Mutex
	request      events.Event
	claimState   control.PermissionResolverState
	onRequest    func()
	captured     bool
	requestCount int
}

func (s *permissionRequestCaptureSink) Write(event events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.Event == "permission.request" {
		s.requestCount++
		if !s.captured {
			s.request = event
			s.captured = true
			if s.onRequest != nil {
				s.onRequest()
			}
		}
	}
	return nil
}

func (s *permissionRequestCaptureSink) Close() error { return nil }

func (s *permissionLifecycleSink) Write(event events.Event) error {
	if event.Event == "permission.request" && s.onRequest != nil {
		s.onRequest()
	}
	if event.Event == "permission.response" {
		requestID, _ := event.Fields["request_id"].(string)
		s.responses <- requestID
	}
	return nil
}

func (s *permissionLifecycleSink) Close() error { return nil }

type signalEventSink struct {
	EventSink
	eventName string
	field     string
	value     string
	signal    chan struct{}
	once      sync.Once
}

func (s *signalEventSink) Write(event events.Event) error {
	if err := s.EventSink.Write(event); err != nil {
		return err
	}
	if event.Event == s.eventName {
		if value, _ := event.Fields[s.field].(string); value == s.value {
			s.once.Do(func() { close(s.signal) })
		}
	}
	return nil
}

type callbackEventSink struct {
	EventSink
	eventName string
	field     string
	value     string
	callback  func()
	once      sync.Once
}

func (s *callbackEventSink) Write(event events.Event) error {
	if event.Event == s.eventName {
		if value, _ := event.Fields[s.field].(string); value == s.value {
			s.once.Do(s.callback)
		}
	}
	return s.EventSink.Write(event)
}

func (f *cliFakeProvider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	return runtime.Session{}, nil
}

func (f *cliFakeProvider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	return runtime.Session{}, nil
}

func (f *cliFakeProvider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	return nil
}

func (f *cliFakeProvider) Cancel(ctx context.Context, sessionID string) error {
	f.cancelSessionID = sessionID
	return nil
}

func (f *cliFakeProvider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	return nil, nil
}

func (f *cliFakeProvider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	f.answerSessionID = sessionID
	f.answerRequestID = requestID
	f.answerResponse = response
	return nil
}

func (f *cliFakeProvider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

type scriptedSession struct {
	opts   runtime.StartOptions
	prompt string
	events chan events.Event
}

type scriptedProvider struct {
	mu       sync.Mutex
	nextID   int
	sessions map[string]*scriptedSession
}

func newScriptedProvider() *scriptedProvider {
	return &scriptedProvider{sessions: map[string]*scriptedSession{}}
}

func (p *scriptedProvider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextID++
	sessionID := fmt.Sprintf("ses_%d", p.nextID)
	p.sessions[sessionID] = &scriptedSession{
		opts:   opts,
		events: make(chan events.Event, 8),
	}
	return runtime.Session{SessionID: sessionID}, nil
}

func (p *scriptedProvider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.sessions[sessionID]; !ok {
		return runtime.Session{}, fmt.Errorf("unknown session %q", sessionID)
	}
	return runtime.Session{SessionID: sessionID}, nil
}

func (p *scriptedProvider) Prompt(ctx context.Context, sessionID string, prompt string) error {
	p.mu.Lock()
	session, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown session %q", sessionID)
	}
	session.prompt = prompt

	var reply string
	switch {
	case strings.Contains(prompt, "decide reviewers"):
		reply = "[team: skip | security]"
	case strings.Contains(prompt, "review style"):
		reply = "style findings"
	case strings.Contains(prompt, "summarize findings"):
		reply = "final report"
	default:
		reply = "ok"
	}

	session.events <- events.Event{
		Event:     "agent.message_chunk",
		SessionID: sessionID,
		Fields: map[string]any{
			"content": map[string]any{"text": reply},
		},
	}
	session.events <- events.Event{
		Event:     "session.end",
		SessionID: sessionID,
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(session.events)
	return nil
}

func (p *scriptedProvider) Cancel(ctx context.Context, sessionID string) error { return nil }

func (p *scriptedProvider) Events(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	session, ok := p.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("unknown session %q", sessionID)
	}
	return session.events, nil
}

func (p *scriptedProvider) AnswerPermission(ctx context.Context, sessionID string, requestID string, response runtime.PermissionResponse) error {
	return nil
}

func (p *scriptedProvider) Capabilities(ctx context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

func (p *scriptedProvider) snapshotSessions() []scriptedSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]scriptedSession, 0, len(p.sessions))
	for _, session := range p.sessions {
		out = append(out, scriptedSession{
			opts:   session.opts,
			prompt: session.prompt,
		})
	}
	return out
}

func waitForSessionForTest(
	ctx context.Context,
	provider runtime.Provider,
	writer EventSink,
	fileHandler *permission.FileHandler,
	controlServer *control.ControlServer,
	eventCh <-chan events.Event,
	promptDone <-chan error,
	interruptCh <-chan struct{},
	sessionID string,
	runID string,
	runLabel string,
	autoApprove bool,
	permClaimTimeout time.Duration,
	timeout <-chan time.Time,
	stderr io.Writer,
) sessionResult {
	return WaitForSession(ctx, provider, SessionWaitConfig{
		EventCh:                eventCh,
		PromptDone:             promptDone,
		InterruptCh:            interruptCh,
		SessionID:              sessionID,
		RunID:                  runID,
		RunLabel:               runLabel,
		AutoApprove:            autoApprove,
		PermissionClaimTimeout: permClaimTimeout,
		Timeout:                timeout,
	}, SessionWaitDeps{
		Writer:        writer,
		FileHandler:   fileHandler,
		ControlServer: controlServer,
		Stderr:        stderr,
	})
}

func shortControlSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "avenor-control-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "control.sock")
}

func waitForPendingPermissionForTest(t *testing.T, cs *control.ControlServer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if cs.HasPendingPermission() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for pending permission claim")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForControlClientForTest(t *testing.T, cs *control.ControlServer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cs.HasClients() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for control client")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWaitForSessionAutoApproveAnswersAllowKindAndOrdersEvents(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 2)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_1",
			"question":   "Allow write?",
			"options": []any{
				map[string]any{"optionId": "deny", "kind": "reject"},
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_1",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(eventCh)
	promptDone := make(chan error, 1)
	promptDone <- nil

	provider := &cliFakeProvider{}
	var stderr strings.Builder
	result := waitForSessionForTest(context.Background(), provider, writer, nil, nil, eventCh, promptDone, nil, "ses_1", "run_1", "review", true, DefaultPermissionClaimTimeout, nil, &stderr)
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatalf("close writer: %v", closeErr)
	}
	if result.ExitCode != 0 {
		t.Fatalf("waitForSessionForTest() = %d, want 0; stderr=%s", result.ExitCode, stderr.String())
	}
	if provider.answerSessionID != "ses_1" || provider.answerRequestID != "req_1" {
		t.Fatalf("answered session=%q request=%q", provider.answerSessionID, provider.answerRequestID)
	}
	if !provider.answerResponse.Allow {
		t.Fatal("answerResponse.Allow = false, want true")
	}
	if provider.answerResponse.OptionID != "allow" {
		t.Fatalf("answerResponse.OptionID = %q, want allow", provider.answerResponse.OptionID)
	}

	got := readEventLogForTest(t, eventsPath)
	requestIndex := -1
	responseAfterRequest := false
	for i, ev := range got {
		if ev.Event == "permission.request" {
			requestIndex = i
			if gotResolver := ev.Fields["resolver"]; gotResolver != "automatic" {
				t.Fatalf("permission.request resolver = %v, want automatic", gotResolver)
			}
		}
		// With the goroutine-based auto-answer, AnswerPermission runs concurrently
		// with event draining, so session.end may arrive before the permissionDone
		// case fires. The reliable audit signal is permission.response, not
		// agent.status working (which is suppressed when the session is already done).
		if requestIndex >= 0 && i > requestIndex && ev.Event == "permission.response" {
			responseAfterRequest = true
		}
	}
	if requestIndex < 0 {
		t.Fatalf("permission.request not written: %+v", got)
	}
	if !responseAfterRequest {
		t.Fatalf("permission.response not emitted after permission.request: %+v", got)
	}
	if got[0].Fields["run_label"] != "review" {
		t.Fatalf("run_label = %v, want review", got[0].Fields["run_label"])
	}
}

func TestEffectivePermissionResolverState(t *testing.T) {
	tests := []struct {
		name              string
		autoApprove       bool
		requiresUserInput bool
		hasControlClient  bool
		hasFileHandler    bool
		want              control.PermissionResolverState
	}{
		{name: "automatic", autoApprove: true, want: control.PermissionResolverAutomatic},
		{name: "write-in with control", autoApprove: true, requiresUserInput: true, hasControlClient: true, hasFileHandler: true, want: control.PermissionResolverReserved},
		{name: "write-in with file", autoApprove: true, requiresUserInput: true, hasFileHandler: true, want: control.PermissionResolverFile},
		{name: "file fallback", hasFileHandler: true, want: control.PermissionResolverFile},
		{name: "write-in with none", autoApprove: true, requiresUserInput: true, want: control.PermissionResolverNoResolver},
		{name: "none", want: control.PermissionResolverNoResolver},
		{name: "reserved no-auto", autoApprove: false, hasControlClient: true, requiresUserInput: false, want: control.PermissionResolverReserved},
		{name: "automatic takes precedence", autoApprove: true, hasControlClient: true, hasFileHandler: true, want: control.PermissionResolverAutomatic},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectivePermissionResolverState(tt.autoApprove, tt.requiresUserInput, tt.hasControlClient, tt.hasFileHandler); got != tt.want {
				t.Fatalf("effectivePermissionResolverState() = %v, want %v", got, tt.want)
			}
			if got := effectivePermissionResolverState(tt.autoApprove, tt.requiresUserInput, tt.hasControlClient, tt.hasFileHandler).String(); got != tt.want.String() {
				t.Fatalf("serialized resolver = %q, want %q", got, tt.want.String())
			}
		})
	}
}

func TestPermissionRequestEmitsEffectiveResolverAndClaimState(t *testing.T) {
	tests := []struct {
		name              string
		autoApprove       bool
		hasFileHandler    bool
		requiresUserInput bool
		want              control.PermissionResolverState
		connectControl    bool
	}{
		{name: "automatic", autoApprove: true, want: control.PermissionResolverAutomatic},
		{name: "reserved", want: control.PermissionResolverReserved, connectControl: true},
		{name: "file", hasFileHandler: true, want: control.PermissionResolverFile},
		{name: "none", want: control.PermissionResolverNoResolver},
		{name: "write-in none", autoApprove: true, requiresUserInput: true, want: control.PermissionResolverNoResolver},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := control.NewServer(control.NewState("run_1", "", 0))
			var conn net.Conn
			if tt.connectControl {
				socketPath := shortControlSocketPath(t)
				if err := cs.Start(socketPath); err != nil {
					t.Fatalf("start control server: %v", err)
				}
				defer cs.Stop()
				var err error
				conn, err = net.Dial("unix", socketPath)
				if err != nil {
					t.Fatalf("dial control server: %v", err)
				}
				defer conn.Close()
				waitForControlClientForTest(t, cs)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			eventCh := make(chan events.Event, 1)
			eventCh <- events.Event{
				Event:     "permission.request",
				SessionID: "ses_resolver",
				Fields: map[string]any{
					"request_id":          "req_resolver",
					"requires_user_input": tt.requiresUserInput,
					"options": []any{
						map[string]any{"optionId": "allow", "kind": "allow"},
					},
				},
			}
			close(eventCh)
			promptDone := make(chan error, 1)
			promptDone <- nil
			sink := &permissionRequestCaptureSink{}
			sink.onRequest = func() {
				sink.claimState = cs.PermissionResolverState("", "req_resolver")
				cancel()
			}
			fileHandler := (*permission.FileHandler)(nil)
			if tt.hasFileHandler {
				fileHandler = permission.NewFileHandler(filepath.Join(t.TempDir(), "permission"))
			}

			result := waitForSessionForTest(ctx, &cliFakeProvider{}, sink, fileHandler, cs, eventCh, promptDone, nil, "ses_resolver", "run_1", "", tt.autoApprove, time.Millisecond, nil, io.Discard)
			if result.ExitCode == 0 && tt.want == control.PermissionResolverReserved {
				t.Fatal("reserved permission unexpectedly completed")
			}
			if got := sink.request.Fields["resolver"]; got != tt.want.String() {
				t.Fatalf("permission.request resolver = %v, want %q", got, tt.want.String())
			}
			if got := sink.claimState; got != tt.want {
				t.Fatalf("claim state before resolution = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPermissionRequestResolverInjectionDoesNotMutateSourceEvent(t *testing.T) {
	source := events.Event{
		Event:     "permission.request",
		SessionID: "ses_source_event",
		Fields: map[string]any{
			"request_id": "req_source_event",
			"options": []any{
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	sourceBefore, err := json.Marshal(source)
	if err != nil {
		t.Fatalf("marshal source permission event: %v", err)
	}
	eventCh := make(chan events.Event, 2)
	eventCh <- source
	eventCh <- events.Event{Event: "session.end", SessionID: source.SessionID, Fields: map[string]any{"stop_reason": "end_turn"}}
	close(eventCh)
	promptDone := make(chan error, 1)
	promptDone <- nil
	sink := &permissionRequestCaptureSink{}

	result := waitForSessionForTest(context.Background(), &cliFakeProvider{}, sink, nil, nil, eventCh, promptDone, nil, source.SessionID, "run_source_event", "", true, DefaultPermissionClaimTimeout, nil, io.Discard)
	if result.ExitCode != 0 {
		t.Fatalf("WaitForSession exit code = %d, want 0", result.ExitCode)
	}
	sourceAfter, err := json.Marshal(source)
	if err != nil {
		t.Fatalf("marshal source permission event after wait: %v", err)
	}
	if string(sourceAfter) != string(sourceBefore) {
		t.Fatalf("source permission event mutated: got %s, want %s", sourceAfter, sourceBefore)
	}
	sink.mu.Lock()
	resolver := sink.request.Fields["resolver"]
	sink.mu.Unlock()
	if resolver != "automatic" {
		t.Fatalf("written permission.request resolver = %v, want automatic", resolver)
	}
}

func TestWaitForSessionFilePermissionEmitsOneEffectiveRequestAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "permission")
	handler := permission.NewFileHandler(base)
	handler.PollInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventCh := make(chan events.Event, 1)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_file_once",
		Fields: map[string]any{
			"request_id": "req_file_once",
			"options": []any{
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	close(eventCh)
	promptDone := make(chan error, 1)
	promptDone <- nil
	sink := &permissionRequestCaptureSink{onRequest: cancel}

	result := waitForSessionForTest(ctx, &cliFakeProvider{}, sink, handler, nil, eventCh, promptDone, nil, "ses_file_once", "run_1", "", false, time.Second, nil, io.Discard)
	if result.ExitCode != 130 {
		t.Fatalf("WaitForSession exit code = %d, want 130", result.ExitCode)
	}
	if sink.requestCount != 1 {
		t.Fatalf("permission.request count = %d, want 1", sink.requestCount)
	}
	if got := sink.request.Fields["resolver"]; got != "file" {
		t.Fatalf("permission.request resolver = %v, want file", got)
	}
	if _, err := os.Stat(base + ".req"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("permission request file still exists after synchronized cancellation: %v", err)
	}
}

func TestWaitForSessionSynchronizesFileCleanupForEveryTerminationSource(t *testing.T) {
	tests := []struct {
		name             string
		stopReason       string
		expectedExitCode int
		trigger          func(chan<- error, chan<- time.Time, chan<- time.Time, chan<- struct{})
	}{
		{
			name:             "prompt cancellation",
			stopReason:       "cancelled",
			expectedExitCode: 130,
			trigger: func(promptDone chan<- error, _ chan<- time.Time, _ chan<- time.Time, _ chan<- struct{}) {
				promptDone <- context.Canceled
			},
		},
		{
			name:             "progress timeout",
			stopReason:       "progress_timeout",
			expectedExitCode: 124,
			trigger: func(_ chan<- error, progressTimer chan<- time.Time, _ chan<- time.Time, _ chan<- struct{}) {
				progressTimer <- time.Time{}
			},
		},
		{
			name:             "configured timeout",
			stopReason:       "timeout",
			expectedExitCode: 124,
			trigger: func(_ chan<- error, _ chan<- time.Time, timeout chan<- time.Time, _ chan<- struct{}) {
				timeout <- time.Time{}
			},
		},
		{
			name:             "interrupt",
			stopReason:       "cancelled",
			expectedExitCode: 130,
			trigger: func(_ chan<- error, _ chan<- time.Time, _ chan<- time.Time, interruptCh chan<- struct{}) {
				interruptCh <- struct{}{}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			base := filepath.Join(dir, "permission")
			handler := permission.NewFileHandler(base)
			handler.PollInterval = time.Millisecond
			promptDone := make(chan error)
			progressTimer := make(chan time.Time, 1)
			timeout := make(chan time.Time, 1)
			interruptCh := make(chan struct{}, 1)
			eventCh := make(chan events.Event, 1)
			eventCh <- events.Event{Event: "permission.request", SessionID: "ses_cleanup", Fields: map[string]any{
				"request_id": "req_cleanup",
				"options":    []any{map[string]any{"optionId": "allow", "kind": "allow"}},
			}}
			sink := &permissionRequestCaptureSink{}
			requestWritten := make(chan struct{})
			var requestOnce sync.Once
			sink.onRequest = func() { requestOnce.Do(func() { close(requestWritten) }) }
			resultCh := make(chan sessionResult, 1)
			go func() {
				resultCh <- WaitForSession(context.Background(), &cliFakeProvider{}, SessionWaitConfig{
					EventCh:                eventCh,
					PromptDone:             promptDone,
					InterruptCh:            interruptCh,
					SessionID:              "ses_cleanup",
					RunID:                  "run_cleanup",
					PermissionClaimTimeout: time.Second,
					ProgressTimeout:        time.Second,
					Timeout:                timeout,
				}, SessionWaitDeps{
					Writer:         sink,
					FileHandler:    handler,
					ProgressTimerC: progressTimer,
					Stderr:         io.Discard,
				})
			}()
			select {
			case <-requestWritten:
			case <-time.After(time.Second):
				t.Fatal("permission.request was not written")
			}
			deadline := time.Now().Add(time.Second)
			for {
				if _, err := os.Stat(base + ".req"); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("file handler did not create .req before termination")
				}
				time.Sleep(time.Millisecond)
			}
			triggerDone := make(chan struct{})
			go func() {
				defer close(triggerDone)
				tt.trigger(promptDone, progressTimer, timeout, interruptCh)
			}()
			select {
			case result := <-resultCh:
				<-triggerDone
				if result.StopReason != tt.stopReason {
					t.Fatalf("StopReason = %q, want %q", result.StopReason, tt.stopReason)
				}
				if result.ExitCode != tt.expectedExitCode {
					t.Fatalf("ExitCode = %d, want %d", result.ExitCode, tt.expectedExitCode)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("WaitForSession did not synchronize and return")
			}
			if _, err := os.Stat(base + ".req"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf(".req remained after %s termination: %v", tt.name, err)
			}
		})
	}
}

func TestResolvePermissionAutoApproveDoesNotAnswerQuestion(t *testing.T) {
	provider := &cliFakeProvider{}
	event := events.Event{Event: "permission.request", Fields: map[string]any{
		"request_id":          "req_question",
		"requires_user_input": true,
		"options": []any{
			map[string]any{"optionId": "choice", "kind": "allow"},
		},
	}}
	result := resolvePermission(context.Background(), provider, nil, nil, event, "ses_1", "", "req_question", true, DefaultPermissionClaimTimeout)
	if provider.answerRequestID != "" {
		t.Fatalf("question was auto-answered: %q", provider.answerRequestID)
	}
	if result.err != nil {
		t.Fatalf("auto-approve result error = %v, want nil", result.err)
	}
	if result.source != "none" {
		t.Fatalf("result source = %q, want none", result.source)
	}
}

func readEventLogForTest(t *testing.T, path string) []events.Event {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	out := make([]events.Event, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		var ev events.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("decode event %q: %v", line, err)
		}
		out = append(out, ev)
	}
	return out
}

func TestRunCleansSentinelFilesBetweenRetries(t *testing.T) {
	oldRunAttempt := runAttempt
	oldRetryAfter := retryAfter
	t.Cleanup(func() {
		runAttempt = oldRunAttempt
		retryAfter = oldRetryAfter
	})

	dir := t.TempDir()
	promptPath := filepath.Join(dir, "prompt.txt")
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentinelPath := filepath.Join(dir, "run.done")
	permBase := filepath.Join(dir, "run.perm")
	if err := os.WriteFile(promptPath, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write prompt: %v", err)
	}

	attempts := 0
	runAttempt = func(
		ctx context.Context,
		cfg attemptConfig,
		deps attemptDeps,
	) attemptResult {
		attempts++
		switch attempts {
		case 1:
			if err := os.WriteFile(permBase+".req", []byte("stale request"), 0o600); err != nil {
				t.Fatalf("write stale req: %v", err)
			}
			if err := os.WriteFile(permBase+".req.response", []byte("stale response"), 0o600); err != nil {
				t.Fatalf("write stale response: %v", err)
			}
			return attemptResult{exitCode: 1, sessionID: "ses_1"}
		case 2:
			for _, path := range []string{permBase + ".req", permBase + ".req.response"} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("%s was not cleaned before retry", path)
				}
			}
			return attemptResult{exitCode: 0, sessionID: "ses_2"}
		default:
			t.Fatalf("unexpected attempt %d", attempts)
			return attemptResult{exitCode: 1}
		}
	}
	retryAfter = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}

	var stderr strings.Builder
	exitCode := run([]string{
		"--prompt-file", promptPath,
		"--on-event", eventsPath,
		"--sentinel-file", sentinelPath,
		"--max-retries", "1",
	}, func(key string) string {
		if key == "AVENOR_OPENCODE_URL" {
			return "http://localhost:8080"
		}
		return ""
	}, &stderr)
	if exitCode != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", exitCode, stderr.String())
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestRunSessionIDConflictDoesNotRetryProvisionalSession(t *testing.T) {
	oldRunAttempt, oldNewProvider, oldRetryAfter := runAttempt, newProvider, retryAfter
	t.Cleanup(func() {
		runAttempt, newProvider, retryAfter = oldRunAttempt, oldNewProvider, oldRetryAfter
	})

	conflictProvider := &directConflictProvider{}
	noStartProvider := &directConflictProvider{noStart: true}
	factoryCalls := 0
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return conflictProvider, nil
		}
		return noStartProvider, nil
	}
	runAttempt = func(ctx context.Context, cfg attemptConfig, deps attemptDeps) attemptResult {
		if _, exists := cfg.sessionBackends.identity("occupied"); !exists {
			owner := &cliSessionAttempt{}
			identity := cliSessionIdentity{backend: cfg.backend, agent: cfg.startOptions.Agent, model: cfg.startOptions.Model, agentProfile: cfg.startOptions.AgentProfile}
			if err := cfg.sessionBackends.claim("occupied", identity, owner, ""); err != nil {
				t.Fatal(err)
			}
			cfg.sessionBackends.finish("occupied", "occupied", owner)
		}
		return runSingleAttempt(ctx, cfg, deps)
	}
	retryAfter = func(time.Duration) <-chan time.Time {
		t.Fatal("fatal session identity conflict entered retry backoff")
		return nil
	}

	var stderr strings.Builder
	if code := run([]string{"--prompt", "hello", "--max-retries", "3"}, func(string) string { return "" }, &stderr); code != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", code, stderr.String())
	}
	conflictProvider.mu.Lock()
	starts, resumes, prompts := conflictProvider.starts, conflictProvider.resumes, conflictProvider.prompts
	conflictProvider.mu.Unlock()
	if factoryCalls != 1 || starts != 1 || resumes != 0 || prompts != 1 {
		t.Fatalf("factory=%d starts=%d resumes=%d prompts=%d, want one fatal attempt", factoryCalls, starts, resumes, prompts)
	}
	noStartProvider.mu.Lock()
	noStartPrompts := noStartProvider.prompts
	noStartProvider.mu.Unlock()
	if noStartPrompts != 0 {
		t.Fatal("retry succeeded without a session.start event")
	}
}

func TestRunLoopFileReturnsFailureOnLooprunnerError(t *testing.T) {
	dir := t.TempDir()
	loopPath := filepath.Join(dir, "loop.json")
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentinelPath := filepath.Join(dir, "run.done")
	if err := os.WriteFile(loopPath, []byte(`{"pre":[{"name":"broken","prompt":"{{"}]}`), 0o600); err != nil {
		t.Fatalf("write loop config: %v", err)
	}

	var stderr strings.Builder
	exitCode := run([]string{
		"--loop-file", loopPath,
		"--on-event", eventsPath,
		"--sentinel-file", sentinelPath,
		"--run-id", "run_1",
	}, func(key string) string {
		if key == "AVENOR_OPENCODE_URL" {
			return "http://localhost:8080"
		}
		return ""
	}, &stderr)
	if exitCode != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "avenor: loop run:") {
		t.Fatalf("stderr = %q, want loop run error", stderr.String())
	}
	sentinel, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	want := "FAILED\nSESSION=\nSTOP_REASON=error\nEXIT_CODE=1\nRUN=run_1\n"
	if string(sentinel) != want {
		t.Fatalf("sentinel = %q, want %q", string(sentinel), want)
	}
}

func TestRunLoopFileConfigErrorWritesErrorSentinel(t *testing.T) {
	dir := t.TempDir()
	loopPath := filepath.Join(dir, "loop.json")
	sentinelPath := filepath.Join(dir, "run.done")
	if err := os.WriteFile(loopPath, []byte(`{"pre":[{"prompt":"missing name"}]}`), 0o600); err != nil {
		t.Fatalf("write loop config: %v", err)
	}

	var stderr strings.Builder
	exitCode := run([]string{
		"--loop-file", loopPath,
		"--sentinel-file", sentinelPath,
		"--run-id", "run_1",
	}, func(key string) string {
		if key == "AVENOR_OPENCODE_URL" {
			return "http://localhost:8080"
		}
		return ""
	}, &stderr)
	if exitCode != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", exitCode, stderr.String())
	}
	sentinel, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	want := "FAILED\nSESSION=\nSTOP_REASON=error\nEXIT_CODE=1\nRUN=run_1\n"
	if string(sentinel) != want {
		t.Fatalf("sentinel = %q, want %q", string(sentinel), want)
	}
}

func TestRunRejectsLoopFileAndResume(t *testing.T) {
	var stderr strings.Builder
	exitCode := run([]string{
		"--loop-file", "/tmp/loop.json",
		"--resume", "ses_1",
	}, func(string) string { return "" }, &stderr)
	if exitCode != 1 {
		t.Fatalf("run() = %d, want 1", exitCode)
	}
	if !strings.Contains(stderr.String(), "--loop-file and --resume are mutually exclusive") {
		t.Fatalf("stderr = %q, want mutual exclusion error", stderr.String())
	}
}

func TestRunRejectsLoopFileAndTeamFile(t *testing.T) {
	var stderr strings.Builder
	exitCode := run([]string{
		"--loop-file", "/tmp/loop.json",
		"--team-file", "/tmp/team.json",
	}, func(string) string { return "" }, &stderr)
	if exitCode != 1 {
		t.Fatalf("run() = %d, want 1", exitCode)
	}
	if !strings.Contains(stderr.String(), "--loop-file and --team-file are mutually exclusive") {
		t.Fatalf("stderr = %q, want mutual exclusion error", stderr.String())
	}
}

func TestRunRejectsTeamFileAndResume(t *testing.T) {
	var stderr strings.Builder
	exitCode := run([]string{
		"--team-file", "/tmp/team.json",
		"--resume", "ses_1",
	}, func(string) string { return "" }, &stderr)
	if exitCode != 1 {
		t.Fatalf("run() = %d, want 1", exitCode)
	}
	if !strings.Contains(stderr.String(), "--team-file and --resume are mutually exclusive") {
		t.Fatalf("stderr = %q, want mutual exclusion error", stderr.String())
	}
}

func TestRunTeamFileConfigErrorWritesErrorSentinel(t *testing.T) {
	dir := t.TempDir()
	teamPath := filepath.Join(dir, "team.json")
	sentinelPath := filepath.Join(dir, "run.done")
	if err := os.WriteFile(teamPath, []byte(`{"team":[{"prompt":"missing name"}]}`), 0o600); err != nil {
		t.Fatalf("write team config: %v", err)
	}

	var stderr strings.Builder
	exitCode := run([]string{
		"--team-file", teamPath,
		"--sentinel-file", sentinelPath,
		"--run-id", "run_1",
	}, func(key string) string {
		if key == "AVENOR_OPENCODE_URL" {
			return "http://localhost:8080"
		}
		return ""
	}, &stderr)
	if exitCode != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", exitCode, stderr.String())
	}
	sentinel, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	want := "FAILED\nSESSION=\nSTOP_REASON=error\nEXIT_CODE=1\nRUN=run_1\n"
	if string(sentinel) != want {
		t.Fatalf("sentinel = %q, want %q", string(sentinel), want)
	}
}

func TestRunTeamFileFakeE2E(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	var backendsMu sync.Mutex
	var backends []string
	newProvider = func(startOpts runtime.StartOptions, backend string) (runtime.Provider, error) {
		backendsMu.Lock()
		backends = append(backends, backend)
		backendsMu.Unlock()
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	teamPath := filepath.Join(dir, "team.json")
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentinelPath := filepath.Join(dir, "run.done")
	config := `{
		"pre":[{"name":"decide","prompt":"decide reviewers"}],
		"team":[
			{"name":"security","prompt":"review security","conditional":true,"agent":"security-agent","model":"security-model"},
			{"name":"style","prompt":"review style","agent":"style-agent","model":"style-model"}
		],
		"post":[{"name":"report","prompt":"summarize findings"}]
	}`
	if err := os.WriteFile(teamPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write team config: %v", err)
	}

	var stderr strings.Builder
	exitCode := run([]string{
		"--backend", "gemini-acp",
		"--team-file", teamPath,
		"--on-event", eventsPath,
		"--sentinel-file", sentinelPath,
		"--run-id", "run_team_e2e",
		"--agent", "default-agent",
		"--model", "default-model",
	}, func(key string) string {
		return ""
	}, &stderr)
	if exitCode != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", exitCode, stderr.String())
	}

	sentinel, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	wantSentinel := "DONE\nSESSION=\nSTOP_REASON=end_turn\nRUN=run_team_e2e\n"
	if string(sentinel) != wantSentinel {
		t.Fatalf("sentinel = %q, want %q", string(sentinel), wantSentinel)
	}

	gotEvents := readEventLogForTest(t, eventsPath)
	var phasesStarted []string
	var teamStart, teamEnd bool
	for _, ev := range gotEvents {
		switch ev.Event {
		case "avenor.team.start":
			teamStart = true
		case "avenor.team.end":
			teamEnd = true
			if ev.Fields["exit_reason"] != "end_turn" {
				t.Fatalf("team.end exit_reason = %v, want end_turn", ev.Fields["exit_reason"])
			}
		case "avenor.phase.start":
			phasesStarted = append(phasesStarted, ev.Fields["phase"].(string))
		}
	}
	if !teamStart || !teamEnd {
		t.Fatalf("team lifecycle events missing: start=%v end=%v", teamStart, teamEnd)
	}
	if strings.Contains(strings.Join(phasesStarted, ","), "security") {
		t.Fatalf("security phase should have been skipped; phases=%v", phasesStarted)
	}
	wantPhases := []string{"decide", "style", "report"}
	if len(phasesStarted) != len(wantPhases) {
		t.Fatalf("phase starts = %v, want %v", phasesStarted, wantPhases)
	}
	for i, want := range wantPhases {
		if phasesStarted[i] != want {
			t.Fatalf("phase starts = %v, want %v", phasesStarted, wantPhases)
		}
	}

	sessions := provider.snapshotSessions()
	if len(sessions) != 3 {
		t.Fatalf("provider sessions = %d, want 3", len(sessions))
	}
	var sawPre, sawStyle, sawPost bool
	for _, session := range sessions {
		switch {
		case strings.Contains(session.prompt, "decide reviewers"):
			sawPre = true
			if session.opts.Agent != "default-agent" || session.opts.Model != "default-model" {
				t.Fatalf("pre opts = %+v, want default agent/model", session.opts)
			}
			if !strings.Contains(session.prompt, "<|team: skip | <name>|>") {
				t.Fatalf("pre prompt missing conditional injection: %q", session.prompt)
			}
		case strings.Contains(session.prompt, "review style"):
			sawStyle = true
			if session.opts.Agent != "style-agent" || session.opts.Model != "style-model" {
				t.Fatalf("style opts = %+v, want style overrides", session.opts)
			}
		case strings.Contains(session.prompt, "summarize findings"):
			sawPost = true
			if session.opts.Agent != "default-agent" || session.opts.Model != "default-model" {
				t.Fatalf("post opts = %+v, want default agent/model", session.opts)
			}
		}
	}
	if !sawPre || !sawStyle || !sawPost {
		t.Fatalf("did not observe all expected phases in provider snapshot: %+v", sessions)
	}

	backendsMu.Lock()
	defer backendsMu.Unlock()
	if len(backends) != 3 {
		t.Fatalf("provider backends = %v, want 3 gemini-acp entries", backends)
	}
	for _, backend := range backends {
		if backend != "gemini-acp" {
			t.Fatalf("provider backends = %v, want only gemini-acp", backends)
		}
	}
}

func TestRunTeamFileOpenCodeBackend(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	var backendsMu sync.Mutex
	var backends []string
	newProvider = func(startOpts runtime.StartOptions, backend string) (runtime.Provider, error) {
		backendsMu.Lock()
		backends = append(backends, backend)
		backendsMu.Unlock()
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	xdgDir := filepath.Join(dir, "xdg")
	opencodeDir := filepath.Join(xdgDir, "opencode")
	if err := os.MkdirAll(opencodeDir, 0o700); err != nil {
		t.Fatalf("mkdir opencode config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(opencodeDir, "opencode.json"), []byte(`{
		"agent": {
			"style-agent": {"model": "style-model"}
		}
	}`), 0o600); err != nil {
		t.Fatalf("write opencode config: %v", err)
	}
	teamPath := filepath.Join(dir, "team.json")
	eventsPath := filepath.Join(dir, "events.ndjson")
	sentinelPath := filepath.Join(dir, "run.done")
	config := `{
		"pre":[{"name":"decide","prompt":"decide reviewers"}],
		"team":[
			{"name":"style","prompt":"review style","agent":"style-agent"}
		],
		"post":[{"name":"report","prompt":"summarize findings"}]
	}`
	if err := os.WriteFile(teamPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write team config: %v", err)
	}

	var stderr strings.Builder
	exitCode := run([]string{
		"--backend", "opencode-acp",
		"--team-file", teamPath,
		"--on-event", eventsPath,
		"--sentinel-file", sentinelPath,
		"--run-id", "run_team_opencode",
		"--agent", "default-agent",
		"--model", "default-model",
	}, func(key string) string {
		if key == "XDG_CONFIG_HOME" {
			return xdgDir
		}
		return ""
	}, &stderr)
	if exitCode != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", exitCode, stderr.String())
	}

	sessions := provider.snapshotSessions()
	var sawStyle bool
	for _, session := range sessions {
		if strings.Contains(session.prompt, "review style") {
			sawStyle = true
			if session.opts.Agent != "style-agent" || session.opts.Model != "style-model" {
				t.Fatalf("style opts = %+v, want agent=style-agent model=style-model (resolved from opencode config)", session.opts)
			}
		}
	}
	if !sawStyle {
		t.Fatal("did not observe style phase in provider snapshot")
	}

	backendsMu.Lock()
	defer backendsMu.Unlock()
	for _, backend := range backends {
		if backend != "opencode-acp" {
			t.Fatalf("provider backends = %v, want only opencode-acp", backends)
		}
	}
}

func TestWriteRetryEventReturnsWriterError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	writer, err := NewEventWriter(path)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := writeRetryEvent(writer, "ses_1", "run_1", 2, 3); err == nil {
		t.Fatal("writeRetryEvent() error = nil, want error")
	}
}

func TestNewEventWriterNullPath(t *testing.T) {
	w, err := NewEventWriter("")
	if err != nil {
		t.Fatalf("NewEventWriter(\"\") error = %v", err)
	}
	defer w.Close()
	// Write should succeed and silently discard.
	if err := w.Write(makeEvent("agent.status", nil)); err != nil {
		t.Fatalf("Write() to null writer error = %v", err)
	}
}

func TestFanoutWriterWithoutControlStampsEventLogOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	baseWriter, err := NewEventWriter(path)
	if err != nil {
		t.Fatalf("NewEventWriter: %v", err)
	}
	writer := newFanoutWriter(baseWriter, nil, NewEventMetadata("run_1", "review", ""))

	for _, ev := range []events.Event{
		{Event: "agent.status", SessionID: "ses_1", Fields: map[string]any{"phase": "working", "custom": "keep"}},
		{Event: "tool.call", SessionID: "ses_1", Fields: map[string]any{"seq": int64(99), "ts": int64(123), "title": "build"}},
		{Event: "session.end", SessionID: "ses_1", Fields: map[string]any{"stop_reason": "end_turn"}},
	} {
		if err := writer.Write(ev); err != nil {
			t.Fatalf("Write(%s): %v", ev.Event, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := readEventLogForTest(t, path)
	if len(got) != 3 {
		t.Fatalf("event count = %d, want 3", len(got))
	}
	if got[0].Fields["run_id"] != "run_1" {
		t.Fatalf("run_id = %v, want run_1", got[0].Fields["run_id"])
	}
	if got[0].Fields["run_label"] != "review" {
		t.Fatalf("run_label = %v, want review", got[0].Fields["run_label"])
	}
	if got[0].Fields["custom"] != "keep" {
		t.Fatalf("custom = %v, want keep", got[0].Fields["custom"])
	}
	if seq := got[0].Fields["seq"]; seq != float64(1) {
		t.Fatalf("first seq = %v, want 1", seq)
	}
	if ts, ok := got[0].Fields["ts"].(float64); !ok || ts <= 0 {
		t.Fatalf("first ts = %v, want positive", got[0].Fields["ts"])
	}
	if seq := got[1].Fields["seq"]; seq != float64(99) {
		t.Fatalf("second seq = %v, want preserved 99", seq)
	}
	if ts := got[1].Fields["ts"]; ts != float64(123) {
		t.Fatalf("second ts = %v, want preserved 123", ts)
	}
	if seq := got[2].Fields["seq"]; seq != float64(100) {
		t.Fatalf("third seq = %v, want 100", seq)
	}
}

func TestFanoutWriterWithControlAndNoMetadataPreservesOutputAndBoundsStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	baseWriter, err := NewEventWriter(path)
	if err != nil {
		t.Fatalf("NewEventWriter: %v", err)
	}
	state := control.NewState("run_control", "review", 0)
	server := control.NewServer(state)
	writer := newFanoutWriter(baseWriter, server, nil)
	finalOutput := strings.Repeat("é", events.MaxFinalOutputRunes+10)

	if err := writer.Write(events.Event{
		Event:     "session.end",
		SessionID: "ses_control",
		Fields:    map[string]any{"stop_reason": "end_turn", "final_output": finalOutput},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	logged := readEventLogForTest(t, path)
	if len(logged) != 1 {
		t.Fatalf("event count = %d, want 1", len(logged))
	}
	if got := logged[0].Fields["run_id"]; got != "run_control" {
		t.Fatalf("run_id = %v, want run_control", got)
	}
	if _, ok := logged[0].Fields["seq"]; !ok {
		t.Fatal("expected seq to be stamped")
	}
	got, _ := logged[0].Fields["final_output"].(string)
	if got != finalOutput {
		t.Fatal("durable event log did not preserve the complete final_output")
	}
	preview := state.Snapshot().FinalOutput
	if count := len([]rune(preview)); count != events.MaxFinalOutputRunes {
		t.Fatalf("status final_output rune count = %d, want %d", count, events.MaxFinalOutputRunes)
	}
	if state.FinalOutput() != finalOutput {
		t.Fatal("control result storage did not preserve the complete final_output")
	}
}

func TestDiscoverServerSelection(t *testing.T) {
	env := func(key string) string {
		if key == "AVENOR_OPENCODE_URL" {
			return "http://env.example"
		}
		return ""
	}

	tests := []struct {
		name   string
		flag   string
		env    func(string) string
		source string
		mode   string
		url    string
	}{
		{
			name:   "flag wins",
			flag:   "http://flag.example",
			env:    env,
			source: "flag",
			mode:   "external",
			url:    "http://flag.example",
		},
		{
			name:   "env wins over fallback",
			env:    env,
			source: "env",
			mode:   "external",
			url:    "http://env.example",
		},
		{
			name:   "subprocess fallback",
			env:    func(string) string { return "" },
			source: "subprocess",
			mode:   "subprocess",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DiscoverServer(tt.flag, tt.env)
			if got.Source != tt.source || got.Mode != tt.mode || got.URL != tt.url {
				t.Fatalf("DiscoverServer() = %+v, want source=%s mode=%s url=%s", got, tt.source, tt.mode, tt.url)
			}
		})
	}
}

func TestOverrideStopReason(t *testing.T) {
	tests := []struct {
		name      string
		server    string
		cancelled bool
		timedOut  bool
		want      string
	}{
		{name: "normal", server: "end_turn", want: "end_turn"},
		{name: "cancelled", server: "end_turn", cancelled: true, want: "cancelled"},
		{name: "timeout", server: "end_turn", timedOut: true, want: "timeout"},
		{name: "timeout wins", server: "end_turn", cancelled: true, timedOut: true, want: "timeout"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := OverrideStopReason(tt.server, tt.cancelled, tt.timedOut); got != tt.want {
				t.Fatalf("OverrideStopReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

// errorFakeProvider is a variant of cliFakeProvider whose AnswerPermission
// returns a fixed error. Used to test the error path in the auto-answer goroutine.
type errorFakeProvider struct {
	cliFakeProvider
	answerErr error
}

func (f *errorFakeProvider) AnswerPermission(ctx context.Context, sessionID, requestID string, response runtime.PermissionResponse) error {
	return f.answerErr
}

// blockingFakeProvider is a variant of cliFakeProvider whose AnswerPermission
// blocks until unblockAnswer is closed. Used to test that the auto-answer
// goroutine does not wedge the event loop.
type blockingFakeProvider struct {
	cliFakeProvider
	unblockAnswer chan struct{}
	answered      chan struct{} // closed when AnswerPermission is called
	returned      chan struct{} // closed when AnswerPermission is about to return
}

func newBlockingFakeProvider() *blockingFakeProvider {
	return &blockingFakeProvider{
		unblockAnswer: make(chan struct{}),
		answered:      make(chan struct{}),
		returned:      make(chan struct{}),
	}
}

func (f *blockingFakeProvider) AnswerPermission(ctx context.Context, sessionID, requestID string, response runtime.PermissionResponse) error {
	f.cliFakeProvider.answerSessionID = sessionID
	f.cliFakeProvider.answerRequestID = requestID
	f.cliFakeProvider.answerResponse = response
	close(f.answered) // signal that we have been called
	<-f.unblockAnswer // block until the test unblocks us
	close(f.returned) // signal that we are about to return
	return nil
}

// cancelUnblocksPermissionProvider models providers whose AnswerPermission
// ignores its context but is released by the session-level Cancel operation.
type cancelUnblocksPermissionProvider struct {
	cliFakeProvider
	answerStarted  chan struct{}
	cancelStarted  chan struct{}
	answerReturned chan struct{}
	cancelOnce     sync.Once
}

func newCancelUnblocksPermissionProvider() *cancelUnblocksPermissionProvider {
	return &cancelUnblocksPermissionProvider{
		answerStarted:  make(chan struct{}),
		cancelStarted:  make(chan struct{}),
		answerReturned: make(chan struct{}),
	}
}

func (p *cancelUnblocksPermissionProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	close(p.answerStarted)
	<-p.cancelStarted
	close(p.answerReturned)
	return nil
}

func (p *cancelUnblocksPermissionProvider) Cancel(context.Context, string) error {
	p.cancelOnce.Do(func() { close(p.cancelStarted) })
	return nil
}

type nonCooperativePermissionProvider struct {
	cliFakeProvider
	answerStarted  chan struct{}
	answerRelease  chan struct{}
	answerReturned chan struct{}
	cancelStarted  chan struct{}
	cancelRelease  chan struct{}
	cancelReturned chan struct{}
	releaseOnce    sync.Once
}

func newNonCooperativePermissionProvider() *nonCooperativePermissionProvider {
	return &nonCooperativePermissionProvider{
		answerStarted:  make(chan struct{}),
		answerRelease:  make(chan struct{}),
		answerReturned: make(chan struct{}),
		cancelStarted:  make(chan struct{}),
		cancelRelease:  make(chan struct{}),
		cancelReturned: make(chan struct{}),
	}
}

func (p *nonCooperativePermissionProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	close(p.answerStarted)
	<-p.answerRelease
	close(p.answerReturned)
	return nil
}

func (p *nonCooperativePermissionProvider) Cancel(context.Context, string) error {
	close(p.cancelStarted)
	<-p.cancelRelease
	close(p.cancelReturned)
	return nil
}

func (p *nonCooperativePermissionProvider) release() {
	p.releaseOnce.Do(func() {
		close(p.answerRelease)
		close(p.cancelRelease)
	})
}

type coordinatedLifecycleProvider struct {
	cliFakeProvider
	answerStarted         chan struct{}
	answerRelease         chan struct{}
	answerReturned        chan struct{}
	cancelStarted         chan struct{}
	cancelRelease         chan struct{}
	cancelReturned        chan struct{}
	closeCalled           chan struct{}
	completionReady       chan struct{}
	completionRelease     chan struct{}
	answerStartOnce       sync.Once
	answerDoneOnce        sync.Once
	cancelStartOnce       sync.Once
	cancelDoneOnce        sync.Once
	closeOnce             sync.Once
	answerReleaseOnce     sync.Once
	cancelReleaseOnce     sync.Once
	completionReleaseOnce sync.Once
	mu                    sync.Mutex
	cancelCalls           int
	closeCalls            int
	answerInFlight        bool
	cancelInFlight        bool
	closeRaced            bool
}

func newCoordinatedLifecycleProvider() *coordinatedLifecycleProvider {
	return &coordinatedLifecycleProvider{
		answerStarted:  make(chan struct{}),
		answerRelease:  make(chan struct{}),
		answerReturned: make(chan struct{}),
		cancelStarted:  make(chan struct{}),
		cancelRelease:  make(chan struct{}),
		cancelReturned: make(chan struct{}),
		closeCalled:    make(chan struct{}),
	}
}

func (p *coordinatedLifecycleProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	p.mu.Lock()
	p.answerInFlight = true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.answerInFlight = false
		p.mu.Unlock()
		p.answerDoneOnce.Do(func() { close(p.answerReturned) })
	}()
	p.answerStartOnce.Do(func() { close(p.answerStarted) })
	<-p.answerRelease
	p.waitForConcurrentCompletion()
	return nil
}

func (p *coordinatedLifecycleProvider) Cancel(context.Context, string) error {
	p.mu.Lock()
	p.cancelCalls++
	p.cancelInFlight = true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.cancelInFlight = false
		p.mu.Unlock()
		p.cancelDoneOnce.Do(func() { close(p.cancelReturned) })
	}()
	p.cancelStartOnce.Do(func() { close(p.cancelStarted) })
	<-p.cancelRelease
	p.waitForConcurrentCompletion()
	return nil
}

func (p *coordinatedLifecycleProvider) Close() error {
	p.mu.Lock()
	p.closeCalls++
	if p.answerInFlight || p.cancelInFlight {
		p.closeRaced = true
	}
	p.mu.Unlock()
	p.closeOnce.Do(func() { close(p.closeCalled) })
	return nil
}

func (p *coordinatedLifecycleProvider) releaseAnswer() {
	p.answerReleaseOnce.Do(func() { close(p.answerRelease) })
}

func (p *coordinatedLifecycleProvider) releaseCancel() {
	p.cancelReleaseOnce.Do(func() { close(p.cancelRelease) })
}

func (p *coordinatedLifecycleProvider) release() {
	p.releaseAnswer()
	p.releaseCancel()
}

func (p *coordinatedLifecycleProvider) enableConcurrentCompletion() {
	p.completionReady = make(chan struct{}, 2)
	p.completionRelease = make(chan struct{})
}

func (p *coordinatedLifecycleProvider) waitForConcurrentCompletion() {
	if p.completionReady == nil {
		return
	}
	p.completionReady <- struct{}{}
	<-p.completionRelease
}

func (p *coordinatedLifecycleProvider) releaseConcurrentCompletion() {
	if p.completionRelease != nil {
		p.completionReleaseOnce.Do(func() { close(p.completionRelease) })
	}
}

func (p *coordinatedLifecycleProvider) lifecycleCounts() (int, int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cancelCalls, p.closeCalls, p.closeRaced
}

type blockingPromptLifecycleProvider struct {
	cliFakeProvider
	started     chan struct{}
	release     chan struct{}
	returned    chan struct{}
	closed      chan struct{}
	releaseOnce sync.Once
	closeOnce   sync.Once
	mu          sync.Mutex
	inFlight    bool
	closeCalls  int
	closeRaced  bool
}

func newBlockingPromptLifecycleProvider() *blockingPromptLifecycleProvider {
	return &blockingPromptLifecycleProvider{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
		closed:   make(chan struct{}),
	}
}

func (p *blockingPromptLifecycleProvider) Prompt(context.Context, string, string) error {
	p.mu.Lock()
	p.inFlight = true
	p.mu.Unlock()
	close(p.started)
	defer func() {
		p.mu.Lock()
		p.inFlight = false
		p.mu.Unlock()
		close(p.returned)
	}()
	<-p.release
	return nil
}

func (p *blockingPromptLifecycleProvider) Close() error {
	p.mu.Lock()
	p.closeCalls++
	if p.inFlight {
		p.closeRaced = true
	}
	p.mu.Unlock()
	p.closeOnce.Do(func() { close(p.closed) })
	return nil
}

func (p *blockingPromptLifecycleProvider) unblock() {
	p.releaseOnce.Do(func() { close(p.release) })
}

type multiSessionCancelProvider struct {
	cliFakeProvider
	started     chan string
	release     chan struct{}
	releaseOnce sync.Once
	mu          sync.Mutex
	sessionIDs  []string
}

func newMultiSessionCancelProvider() *multiSessionCancelProvider {
	return &multiSessionCancelProvider{
		started: make(chan string, 2),
		release: make(chan struct{}),
	}
}

func (p *multiSessionCancelProvider) Cancel(_ context.Context, sessionID string) error {
	p.mu.Lock()
	p.sessionIDs = append(p.sessionIDs, sessionID)
	p.mu.Unlock()
	p.started <- sessionID
	<-p.release
	return nil
}

func (p *multiSessionCancelProvider) unblock() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func (p *multiSessionCancelProvider) sessions() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.sessionIDs...)
}

type reusableLifecycleCancelProvider struct {
	cliFakeProvider
	started chan int
	gates   []chan struct{}
	mu      sync.Mutex
	calls   int
}

func newReusableLifecycleCancelProvider(callCount int) *reusableLifecycleCancelProvider {
	gates := make([]chan struct{}, callCount)
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	return &reusableLifecycleCancelProvider{
		started: make(chan int, callCount),
		gates:   gates,
	}
}

func (p *reusableLifecycleCancelProvider) Cancel(ctx context.Context, _ string) error {
	p.mu.Lock()
	p.calls++
	call := p.calls
	gate := p.gates[call-1]
	p.mu.Unlock()
	p.started <- call
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *reusableLifecycleCancelProvider) release(call int) {
	select {
	case <-p.gates[call-1]:
	default:
		close(p.gates[call-1])
	}
}

func (p *reusableLifecycleCancelProvider) releaseAll() {
	for call := range p.gates {
		p.release(call + 1)
	}
}

type waitExitMatrixProvider struct {
	cliFakeProvider
	mu                sync.Mutex
	cancels           int
	sessions          []string
	permissionRelease <-chan struct{}
	permissionErr     error
}

func (p *waitExitMatrixProvider) AnswerPermission(ctx context.Context, _, _ string, _ runtime.PermissionResponse) error {
	if p.permissionRelease == nil {
		return nil
	}
	select {
	case <-p.permissionRelease:
		return p.permissionErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *waitExitMatrixProvider) Cancel(_ context.Context, sessionID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancels++
	p.sessions = append(p.sessions, sessionID)
	return nil
}

func (p *waitExitMatrixProvider) cancelSnapshot() (int, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cancels, append([]string(nil), p.sessions...)
}

type waitExitMatrixSink struct {
	mu           sync.Mutex
	events       []events.Event
	onSessionEnd func()
}

func (s *waitExitMatrixSink) Write(event events.Event) error {
	s.mu.Lock()
	s.events = append(s.events, event)
	onSessionEnd := s.onSessionEnd
	s.mu.Unlock()
	if event.Event == "session.end" && onSessionEnd != nil {
		onSessionEnd()
	}
	return nil
}

func (*waitExitMatrixSink) Close() error { return nil }

func (s *waitExitMatrixSink) counts() (sessionEnds, errors int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.events {
		switch event.Event {
		case "session.end":
			sessionEnds++
		case "avenor.error":
			errors++
		}
	}
	return sessionEnds, errors
}

func (s *waitExitMatrixSink) terminalStopReasons() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var stopReasons []string
	for _, event := range s.events {
		if event.Event == "session.end" {
			stopReason, _ := event.Fields["stop_reason"].(string)
			stopReasons = append(stopReasons, stopReason)
		}
	}
	return stopReasons
}

type sequentialPermissionProvider struct {
	cliFakeProvider
	mu             sync.Mutex
	answerIDs      []string
	firstStarted   chan struct{}
	releaseFirst   chan struct{}
	secondAnswered chan struct{}
}

func newSequentialPermissionProvider() *sequentialPermissionProvider {
	return &sequentialPermissionProvider{
		firstStarted:   make(chan struct{}),
		releaseFirst:   make(chan struct{}),
		secondAnswered: make(chan struct{}),
	}
}

func (f *sequentialPermissionProvider) AnswerPermission(_ context.Context, _ string, requestID string, _ runtime.PermissionResponse) error {
	f.mu.Lock()
	f.answerIDs = append(f.answerIDs, requestID)
	answerNumber := len(f.answerIDs)
	f.mu.Unlock()

	switch answerNumber {
	case 1:
		close(f.firstStarted)
		<-f.releaseFirst
	case 2:
		close(f.secondAnswered)
	}
	return nil
}

func (f *sequentialPermissionProvider) answers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.answerIDs...)
}

func TestWaitForSessionExitMatrixStopsEveryNonCleanReturn(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(*SessionWaitConfig)
		wantExitCode    int
		wantStopReason  string
		wantCancels     int
		wantSessionEnds int
		wantErrors      int
	}{
		{
			name: "clean authoritative completion",
			setup: func(cfg *SessionWaitConfig) {
				eventCh := make(chan events.Event, 1)
				eventCh <- events.Event{Event: "session.end", SessionID: cfg.SessionID, Fields: map[string]any{"stop_reason": "end_turn"}}
				promptDone := make(chan error, 1)
				promptDone <- nil
				cfg.EventCh = eventCh
				cfg.PromptDone = promptDone
			},
			wantExitCode:    0,
			wantCancels:     0,
			wantSessionEnds: 1,
		},
		{
			name: "authoritative end without prompt return",
			setup: func(cfg *SessionWaitConfig) {
				eventCh := make(chan events.Event, 1)
				eventCh <- events.Event{Event: "session.end", SessionID: cfg.SessionID, Fields: map[string]any{"stop_reason": "end_turn"}}
				close(eventCh)
				cfg.EventCh = eventCh
			},
			wantExitCode:    0,
			wantCancels:     1,
			wantSessionEnds: 1,
		},
		{
			name: "event stream closes without end",
			setup: func(cfg *SessionWaitConfig) {
				eventCh := make(chan events.Event)
				close(eventCh)
				cfg.EventCh = eventCh
			},
			wantExitCode: 1,
			wantCancels:  1,
		},
		{
			name: "prompt failure",
			setup: func(cfg *SessionWaitConfig) {
				promptDone := make(chan error, 1)
				promptDone <- errors.New("prompt failed")
				cfg.PromptDone = promptDone
			},
			wantExitCode: 1,
			wantCancels:  1,
			wantErrors:   1,
		},
		{
			name: "authoritative identity conflict",
			setup: func(cfg *SessionWaitConfig) {
				eventCh := make(chan events.Event, 1)
				eventCh <- events.Event{Event: "session.start", SessionID: cfg.SessionID, Fields: map[string]any{"conversation_id": "occupied"}}
				cfg.EventCh = eventCh
				cfg.AcceptSessionID = func(string) bool { return false }
			},
			wantExitCode:   1,
			wantStopReason: runtime.SessionIDConflictStopReason,
			wantCancels:    1,
			wantErrors:     1,
		},
		{
			name: "synthetic timeout",
			setup: func(cfg *SessionWaitConfig) {
				timeout := make(chan time.Time, 1)
				timeout <- time.Now()
				cfg.Timeout = timeout
			},
			wantExitCode:    124,
			wantStopReason:  "timeout",
			wantCancels:     1,
			wantSessionEnds: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &waitExitMatrixProvider{}
			sink := &waitExitMatrixSink{}
			cfg := SessionWaitConfig{SessionID: "ses_exit_matrix", RunID: "run_exit_matrix"}
			tt.setup(&cfg)

			result := WaitForSession(context.Background(), provider, cfg, SessionWaitDeps{
				Writer:                sink,
				Stderr:                io.Discard,
				permissionJoinTimeout: 20 * time.Millisecond,
			})
			if result.ExitCode != tt.wantExitCode || result.StopReason != tt.wantStopReason {
				t.Fatalf("result = {exit:%d stop:%q}, want {%d %q}", result.ExitCode, result.StopReason, tt.wantExitCode, tt.wantStopReason)
			}
			cancelCount, sessions := provider.cancelSnapshot()
			if cancelCount != tt.wantCancels {
				t.Fatalf("provider Cancel calls = %d (%v), want %d", cancelCount, sessions, tt.wantCancels)
			}
			for _, sessionID := range sessions {
				if sessionID != cfg.SessionID {
					t.Fatalf("provider Cancel session = %q, want %q", sessionID, cfg.SessionID)
				}
			}
			sessionEnds, errorEvents := sink.counts()
			if sessionEnds != tt.wantSessionEnds || errorEvents != tt.wantErrors {
				t.Fatalf("audit counts = {session.end:%d errors:%d}, want {%d %d}", sessionEnds, errorEvents, tt.wantSessionEnds, tt.wantErrors)
			}
		})
	}
}

func TestWaitForSessionExitMatrixPreservesForwardedAuthoritativeEndDuringTeardown(t *testing.T) {
	tests := []struct {
		name              string
		permissionFailure bool
		trigger           func(context.CancelFunc, *SessionWaitConfig, *SessionWaitDeps, *waitExitMatrixProvider) func()
	}{
		{
			name: "interrupt",
			trigger: func(_ context.CancelFunc, cfg *SessionWaitConfig, _ *SessionWaitDeps, _ *waitExitMatrixProvider) func() {
				interruptCh := make(chan struct{})
				cfg.InterruptCh = interruptCh
				return func() { close(interruptCh) }
			},
		},
		{
			name: "parent context cancellation",
			trigger: func(cancel context.CancelFunc, _ *SessionWaitConfig, _ *SessionWaitDeps, _ *waitExitMatrixProvider) func() {
				return cancel
			},
		},
		{
			name: "prompt context cancellation",
			trigger: func(_ context.CancelFunc, cfg *SessionWaitConfig, _ *SessionWaitDeps, _ *waitExitMatrixProvider) func() {
				promptDone := make(chan error, 1)
				cfg.PromptDone = promptDone
				return func() { promptDone <- context.Canceled }
			},
		},
		{
			name: "prompt failure",
			trigger: func(_ context.CancelFunc, cfg *SessionWaitConfig, _ *SessionWaitDeps, _ *waitExitMatrixProvider) func() {
				promptDone := make(chan error, 1)
				cfg.PromptDone = promptDone
				return func() { promptDone <- errors.New("late prompt failure") }
			},
		},
		{
			name: "progress timeout",
			trigger: func(_ context.CancelFunc, _ *SessionWaitConfig, deps *SessionWaitDeps, _ *waitExitMatrixProvider) func() {
				progressCh := make(chan time.Time, 1)
				deps.ProgressTimerC = progressCh
				return func() { progressCh <- time.Now() }
			},
		},
		{
			name: "configured timeout",
			trigger: func(_ context.CancelFunc, cfg *SessionWaitConfig, _ *SessionWaitDeps, _ *waitExitMatrixProvider) func() {
				timeoutCh := make(chan time.Time, 1)
				cfg.Timeout = timeoutCh
				return func() { timeoutCh <- time.Now() }
			},
		},
		{
			name:              "permission failure",
			permissionFailure: true,
			trigger: func(_ context.CancelFunc, cfg *SessionWaitConfig, _ *SessionWaitDeps, provider *waitExitMatrixProvider) func() {
				release := make(chan struct{})
				provider.permissionRelease = release
				provider.permissionErr = errors.New("late permission failure")
				cfg.AutoApprove = true
				return func() { close(release) }
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			provider := &waitExitMatrixProvider{}
			eventCh := make(chan events.Event, 3)
			eventCh <- events.Event{
				Event:     "agent.message_chunk",
				SessionID: "ses_authoritative_teardown",
				Fields:    map[string]any{"content": map[string]any{"text": "authoritative reply"}},
			}
			if tt.permissionFailure {
				eventCh <- events.Event{
					Event:     "permission.request",
					SessionID: "ses_authoritative_teardown",
					Fields: map[string]any{
						"request_id": "req_authoritative_teardown",
						"options": []any{
							map[string]any{"optionId": "allow", "kind": "allow"},
						},
					},
				}
			}
			eventCh <- events.Event{
				Event:     "session.end",
				SessionID: "ses_authoritative_teardown",
				Fields: map[string]any{
					"stop_reason": "refusal",
					"usage":       map[string]any{"input_tokens": 7},
				},
			}

			cfg := SessionWaitConfig{
				EventCh:    eventCh,
				PromptDone: make(chan error),
				SessionID:  "ses_authoritative_teardown",
				RunID:      "run_authoritative_teardown",
			}
			deps := SessionWaitDeps{
				Stderr:                io.Discard,
				permissionJoinTimeout: 20 * time.Millisecond,
			}
			trigger := tt.trigger(cancel, &cfg, &deps, provider)
			sink := &waitExitMatrixSink{}
			var triggerOnce sync.Once
			sink.onSessionEnd = func() { triggerOnce.Do(trigger) }
			deps.Writer = sink

			result := WaitForSession(ctx, provider, cfg, deps)
			if result.ExitCode != 2 || result.StopReason != "" {
				t.Fatalf("result = {exit:%d stop:%q}, want authoritative {2 %q}", result.ExitCode, result.StopReason, "")
			}
			if result.Output != "authoritative reply" || result.FinalReply != "authoritative reply" {
				t.Fatalf("result reply = {output:%q final:%q}, want authoritative reply", result.Output, result.FinalReply)
			}
			if result.Usage == nil || result.Usage["input_tokens"] != 7 {
				t.Fatalf("result usage = %#v, want authoritative usage", result.Usage)
			}
			if got := sink.terminalStopReasons(); len(got) != 1 || got[0] != "refusal" {
				t.Fatalf("terminal stop reasons = %v, want exactly [refusal]", got)
			}
			if sessionEnds, errorEvents := sink.counts(); sessionEnds != 1 || errorEvents != 0 {
				t.Fatalf("audit counts = {session.end:%d errors:%d}, want {1 0}", sessionEnds, errorEvents)
			}
			if cancelCount, sessions := provider.cancelSnapshot(); cancelCount != 1 || len(sessions) != 1 || sessions[0] != cfg.SessionID {
				t.Fatalf("provider Cancel calls = %d (%v), want one for %q", cancelCount, sessions, cfg.SessionID)
			}
		})
	}
}

func TestWaitForSessionTerminationCancelsBeforeJoiningPermissionAnswer(t *testing.T) {
	tests := []struct {
		name       string
		trigger    string
		stopReason string
	}{
		{name: "interrupt", trigger: "interrupt", stopReason: "cancelled"},
		{name: "context", trigger: "context", stopReason: "cancelled"},
		{name: "prompt cancellation", trigger: "prompt", stopReason: "cancelled"},
		{name: "progress timeout", trigger: "progress", stopReason: "progress_timeout"},
		{name: "configured timeout", trigger: "timeout", stopReason: "timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := newCancelUnblocksPermissionProvider()
			writer, err := NewEventWriter("")
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			interruptCh := make(chan struct{})
			progressCh := make(chan time.Time, 1)
			timeoutCh := make(chan time.Time, 1)
			promptDone := make(chan error, 1)
			eventCh := make(chan events.Event, 1)
			eventCh <- events.Event{
				Event:     "permission.request",
				SessionID: "ses_cancel_answer",
				Fields: map[string]any{
					"request_id": "req_cancel_answer",
					"options": []any{
						map[string]any{"optionId": "allow", "kind": "allow"},
					},
				},
			}

			resultCh := make(chan sessionResult, 1)
			waitDone := make(chan struct{})
			t.Cleanup(func() {
				provider.cancelOnce.Do(func() { close(provider.cancelStarted) })
				cancel()
				select {
				case <-waitDone:
				case <-time.After(time.Second):
					t.Error("WaitForSession goroutine did not exit during cleanup")
				}
				if err := writer.Close(); err != nil {
					t.Errorf("close writer: %v", err)
				}
			})
			go func() {
				defer close(waitDone)
				resultCh <- WaitForSession(ctx, provider, SessionWaitConfig{
					EventCh:     eventCh,
					PromptDone:  promptDone,
					InterruptCh: interruptCh,
					SessionID:   "ses_cancel_answer",
					RunID:       "run_cancel_answer",
					AutoApprove: true,
					Timeout:     timeoutCh,
				}, SessionWaitDeps{
					Writer:         writer,
					Stderr:         io.Discard,
					ProgressTimerC: progressCh,
				})
			}()

			select {
			case <-provider.answerStarted:
			case <-time.After(time.Second):
				t.Fatal("AnswerPermission did not reach the forced blocking point")
			}
			switch tt.trigger {
			case "interrupt":
				close(interruptCh)
			case "context":
				cancel()
			case "prompt":
				promptDone <- context.Canceled
			case "progress":
				progressCh <- time.Now()
			case "timeout":
				timeoutCh <- time.Now()
			}

			select {
			case result := <-resultCh:
				if result.StopReason != tt.stopReason {
					t.Fatalf("StopReason = %q, want %q", result.StopReason, tt.stopReason)
				}
			case <-time.After(time.Second):
				t.Fatal("WaitForSession deadlocked joining AnswerPermission before provider.Cancel")
			}
			select {
			case <-provider.cancelStarted:
			default:
				t.Fatal("provider.Cancel was not called")
			}
			select {
			case <-provider.answerReturned:
			default:
				t.Fatal("WaitForSession returned before AnswerPermission joined")
			}
		})
	}
}

func TestProviderLifecycleDefersCloseAndSharesBlockedCancel(t *testing.T) {
	for _, answerFirst := range []bool{true, false} {
		name := "cancel returns first"
		if answerFirst {
			name = "answer returns first"
		}
		t.Run(name, func(t *testing.T) {
			provider := newCoordinatedLifecycleProvider()
			ctx, cancel := context.WithCancel(context.Background())
			lifecycle := NewProviderLifecycle(provider)
			turn := lifecycle.NewTurn()
			resolverFinished := make(chan struct{})
			writer, err := NewEventWriter("")
			if err != nil {
				t.Fatal(err)
			}

			interruptCh := make(chan struct{})
			eventCh := make(chan events.Event, 1)
			eventCh <- events.Event{
				Event:     "permission.request",
				SessionID: "ses_coordinated_lifecycle",
				Fields: map[string]any{
					"request_id": "req_coordinated_lifecycle",
					"options": []any{
						map[string]any{"optionId": "allow", "kind": "allow"},
					},
				},
			}
			resultCh := make(chan sessionResult, 1)
			waitDone := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				provider.release()
				select {
				case <-waitDone:
				case <-time.After(time.Second):
					t.Error("WaitForSession goroutine did not exit during cleanup")
				}
				closeDone := lifecycle.RequestClose()
				select {
				case <-closeDone:
				case <-time.After(time.Second):
					t.Error("provider lifecycle did not close during cleanup")
				}
				if err := writer.Close(); err != nil {
					t.Errorf("close writer: %v", err)
				}
			})
			go func() {
				defer close(waitDone)
				resultCh <- WaitForSession(ctx, provider, SessionWaitConfig{
					EventCh:     eventCh,
					PromptDone:  make(chan error),
					InterruptCh: interruptCh,
					SessionID:   "ses_coordinated_lifecycle",
					RunID:       "run_coordinated_lifecycle",
					AutoApprove: true,
				}, SessionWaitDeps{
					Writer:                writer,
					Stderr:                io.Discard,
					ProviderTurn:          turn,
					permissionJoinTimeout: 20 * time.Millisecond,
					permissionResultSent:  func() { close(resolverFinished) },
				})
			}()

			waitFor := func(label string, done <-chan struct{}) {
				t.Helper()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatalf("%s did not finish", label)
				}
			}
			assertPending := func(label string, done <-chan struct{}) {
				t.Helper()
				select {
				case <-done:
					t.Fatalf("%s finished before its provider call was released", label)
				default:
				}
			}

			waitFor("AnswerPermission start", provider.answerStarted)
			close(interruptCh)
			waitFor("Cancel start", provider.cancelStarted)
			select {
			case result := <-resultCh:
				if result.StopReason != "cancelled" {
					t.Fatalf("StopReason = %q, want cancelled", result.StopReason)
				}
			case <-time.After(time.Second):
				t.Fatal("WaitForSession did not return after the provider-call bounds")
			}

			// Model runSingleAttempt/stable teardown and a second cancellation
			// owner. Both must share the one still-live provider Cancel.
			lifecycle.RequestClose()
			secondTurn := lifecycle.NewTurn()
			secondCancelDone := secondTurn.RequestCancel("ses_coordinated_lifecycle", 20*time.Millisecond)
			assertPending("deferred Close", provider.closeCalled)
			assertPending("second cancel wait", secondCancelDone)
			if cancelCalls, _, _ := provider.lifecycleCounts(); cancelCalls != 1 {
				t.Fatalf("provider Cancel calls = %d, want one shared lifecycle", cancelCalls)
			}

			if answerFirst {
				provider.releaseAnswer()
				waitFor("AnswerPermission", provider.answerReturned)
				waitFor("permission resolver", resolverFinished)
				assertPending("deferred Close after answer", provider.closeCalled)
				assertPending("second cancel wait after answer", secondCancelDone)
				provider.releaseCancel()
				waitFor("Cancel", provider.cancelReturned)
				waitFor("second cancel wait", secondCancelDone)
			} else {
				provider.releaseCancel()
				waitFor("Cancel", provider.cancelReturned)
				waitFor("second cancel wait", secondCancelDone)
				assertPending("deferred Close after cancel", provider.closeCalled)
				assertPending("AnswerPermission after cancel", provider.answerReturned)
				provider.releaseAnswer()
				waitFor("AnswerPermission", provider.answerReturned)
				waitFor("permission resolver", resolverFinished)
			}
			waitFor("deferred Close", provider.closeCalled)
			if cancelCalls, closeCalls, raced := provider.lifecycleCounts(); cancelCalls != 1 || closeCalls != 1 || raced {
				t.Fatalf("provider lifecycle = {cancel calls:%d close calls:%d close raced:%v}, want {1 1 false}", cancelCalls, closeCalls, raced)
			}
		})
	}
}

func TestProviderLifecycleConcurrentCompletionStartsOneClose(t *testing.T) {
	provider := newCoordinatedLifecycleProvider()
	provider.enableConcurrentCompletion()
	t.Cleanup(func() {
		provider.release()
		provider.releaseConcurrentCompletion()
	})
	lifecycle := NewProviderLifecycle(provider)
	turn := lifecycle.NewTurn()
	answerDone := make(chan error, 1)
	go func() {
		answerDone <- turn.AnswerPermission(context.Background(), "ses_concurrent_close", "req_concurrent_close", runtime.PermissionResponse{})
	}()
	select {
	case <-provider.answerStarted:
	case <-time.After(time.Second):
		t.Fatal("AnswerPermission did not start")
	}

	cancelDone := turn.RequestCancel("ses_concurrent_close", time.Second)
	select {
	case <-provider.cancelStarted:
	case <-time.After(time.Second):
		t.Fatal("Cancel did not start")
	}
	closeDone := lifecycle.RequestClose()
	select {
	case <-closeDone:
		t.Fatal("provider closed while AnswerPermission and Cancel were live")
	default:
	}

	provider.release()
	for range 2 {
		select {
		case <-provider.completionReady:
		case <-time.After(time.Second):
			t.Fatal("provider calls did not reach the concurrent completion barrier")
		}
	}
	provider.releaseConcurrentCompletion()

	select {
	case err := <-answerDone:
		if err != nil {
			t.Fatalf("AnswerPermission: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AnswerPermission did not return")
	}
	select {
	case <-cancelDone:
		if err := turn.cancelError(); err != nil {
			t.Fatalf("Cancel: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Cancel did not return")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("provider did not close after concurrent completions")
	}
	_, closeCalls, raced := provider.lifecycleCounts()
	if closeCalls != 1 || raced {
		t.Fatalf("provider close lifecycle = {calls:%d raced:%v}, want {1 false}", closeCalls, raced)
	}
}

func TestProviderLifecycleDefersCloseForPromptAndRejectsLateCalls(t *testing.T) {
	provider := newBlockingPromptLifecycleProvider()
	t.Cleanup(provider.unblock)
	lifecycle := NewProviderLifecycle(provider)
	turn := lifecycle.NewTurn()
	promptDone := make(chan error, 1)
	go func() { promptDone <- turn.Prompt(context.Background(), "ses_prompt", "prompt") }()

	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("Prompt did not reach the forced blocking point")
	}
	const closeRequests = 8
	closeResults := make(chan (<-chan struct{}), closeRequests)
	for range closeRequests {
		go func() { closeResults <- lifecycle.RequestClose() }()
	}
	var closeDone <-chan struct{}
	for range closeRequests {
		var done <-chan struct{}
		select {
		case done = <-closeResults:
		case <-time.After(time.Second):
			t.Fatal("concurrent RequestClose call blocked")
		}
		if closeDone == nil {
			closeDone = done
		} else if done != closeDone {
			t.Fatal("concurrent RequestClose calls returned different completion channels")
		}
	}
	select {
	case <-closeDone:
		t.Fatal("provider closed while Prompt remained live")
	default:
	}
	lateTurn := lifecycle.NewTurn()
	if err := lateTurn.Prompt(context.Background(), "ses_prompt", "late"); !errors.Is(err, errProviderLifecycleClosing) {
		t.Fatalf("late Prompt error = %v, want provider lifecycle closing", err)
	}
	if err := lateTurn.AnswerPermission(context.Background(), "ses_prompt", "req_late", runtime.PermissionResponse{}); !errors.Is(err, errProviderLifecycleClosing) {
		t.Fatalf("late AnswerPermission error = %v, want provider lifecycle closing", err)
	}
	lateCancelDone := lateTurn.RequestCancel("ses_prompt", 20*time.Millisecond)
	select {
	case <-lateCancelDone:
		if err := lateTurn.cancelError(); !errors.Is(err, errProviderLifecycleClosing) {
			t.Fatalf("late Cancel error = %v, want provider lifecycle closing", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late Cancel rejection did not complete")
	}

	provider.unblock()
	select {
	case err := <-promptDone:
		if err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Prompt did not return after release")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("provider did not close after Prompt returned")
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.closeCalls != 1 || provider.closeRaced {
		t.Fatalf("provider close lifecycle = {calls:%d raced:%v}, want {1 false}", provider.closeCalls, provider.closeRaced)
	}
}

func TestProviderLifecycleDoesNotShareCancelAcrossSessions(t *testing.T) {
	provider := newMultiSessionCancelProvider()
	t.Cleanup(provider.unblock)
	lifecycle := NewProviderLifecycle(provider)
	firstDone := lifecycle.NewTurn().RequestCancel("ses_first", time.Second)
	secondDone := lifecycle.NewTurn().RequestCancel("ses_second", time.Second)

	seen := map[string]bool{}
	for range 2 {
		select {
		case sessionID := <-provider.started:
			seen[sessionID] = true
		case <-time.After(time.Second):
			t.Fatal("distinct-session Cancel did not start")
		}
	}
	if !seen["ses_first"] || !seen["ses_second"] {
		t.Fatalf("provider Cancel sessions = %v, want both sessions", provider.sessions())
	}
	provider.unblock()
	for name, done := range map[string]<-chan struct{}{"first": firstDone, "second": secondDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s Cancel did not return after release", name)
		}
	}
	if got := provider.sessions(); len(got) != 2 {
		t.Fatalf("provider Cancel calls = %v, want two distinct calls", got)
	}
}

func TestProviderLifecycleCleansCompletedCancelAndAllowsSessionReuse(t *testing.T) {
	provider := newReusableLifecycleCancelProvider(2)
	t.Cleanup(provider.releaseAll)
	lifecycle := NewProviderLifecycle(provider)

	firstTurn := lifecycle.NewTurn()
	firstDone := firstTurn.RequestCancel("ses_reused_cancel", time.Second)
	select {
	case call := <-provider.started:
		if call != 1 {
			t.Fatalf("first started call = %d, want 1", call)
		}
	case <-time.After(time.Second):
		t.Fatal("first Cancel did not start")
	}
	sharedTurn := lifecycle.NewTurn()
	sharedDone := sharedTurn.RequestCancel("ses_reused_cancel", time.Second)
	if sharedDone != firstDone || sharedTurn.cancelCall != firstTurn.cancelCall {
		t.Fatal("live same-session Cancel did not share the exact call")
	}
	lifecycle.mu.Lock()
	activeBefore := len(lifecycle.activeCancels)
	mappedBefore := lifecycle.activeCancels["ses_reused_cancel"]
	lifecycle.mu.Unlock()
	if activeBefore != 1 || mappedBefore != firstTurn.cancelCall {
		t.Fatalf("active cancel before completion = {%d %p}, want {1 %p}", activeBefore, mappedBefore, firstTurn.cancelCall)
	}

	provider.release(1)
	for name, done := range map[string]<-chan struct{}{"first": firstDone, "shared": sharedDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s Cancel did not finish", name)
		}
	}
	lifecycle.mu.Lock()
	activeAfterFirst := len(lifecycle.activeCancels)
	lifecycle.mu.Unlock()
	if activeAfterFirst != 0 {
		t.Fatalf("active cancels after first completion = %d, want 0", activeAfterFirst)
	}

	laterTurn := lifecycle.NewTurn()
	laterDone := laterTurn.RequestCancel("ses_reused_cancel", time.Second)
	select {
	case call := <-provider.started:
		if call != 2 {
			t.Fatalf("later started call = %d, want 2", call)
		}
	case <-time.After(time.Second):
		t.Fatal("later same-session Cancel was incorrectly deduplicated")
	}
	if laterTurn.cancelCall == firstTurn.cancelCall {
		t.Fatal("later same-session Cancel reused a completed call")
	}
	provider.release(2)
	select {
	case <-laterDone:
	case <-time.After(time.Second):
		t.Fatal("later Cancel did not finish")
	}
	lifecycle.mu.Lock()
	activeAfterSecond := len(lifecycle.activeCancels)
	lifecycle.mu.Unlock()
	if activeAfterSecond != 0 {
		t.Fatalf("active cancels after second completion = %d, want 0", activeAfterSecond)
	}
}

func TestWaitForSessionTerminationBoundsNonCooperativeProviderCalls(t *testing.T) {
	provider := newNonCooperativePermissionProvider()
	resolverFinished := make(chan struct{})
	writer, err := NewEventWriter("")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	waitDone := make(chan struct{})
	t.Cleanup(func() {
		provider.release()
		cancel()
		select {
		case <-provider.answerReturned:
		case <-time.After(time.Second):
			t.Error("AnswerPermission goroutine did not exit after cleanup release")
		}
		select {
		case <-provider.cancelReturned:
		case <-time.After(time.Second):
			t.Error("Cancel goroutine did not exit after cleanup release")
		}
		select {
		case <-resolverFinished:
		case <-time.After(time.Second):
			t.Error("permission resolver goroutine did not exit after cleanup release")
		}
		select {
		case <-waitDone:
		case <-time.After(time.Second):
			t.Error("WaitForSession goroutine did not exit during cleanup")
		}
		if err := writer.Close(); err != nil {
			t.Errorf("close writer: %v", err)
		}
	})

	interruptCh := make(chan struct{})
	eventCh := make(chan events.Event, 1)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_stubborn_answer",
		Fields: map[string]any{
			"request_id": "req_stubborn_answer",
			"options": []any{
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	resultCh := make(chan sessionResult, 1)
	go func() {
		defer close(waitDone)
		resultCh <- WaitForSession(ctx, provider, SessionWaitConfig{
			EventCh:     eventCh,
			PromptDone:  make(chan error),
			InterruptCh: interruptCh,
			SessionID:   "ses_stubborn_answer",
			RunID:       "run_stubborn_answer",
			AutoApprove: true,
		}, SessionWaitDeps{
			Writer:                writer,
			Stderr:                io.Discard,
			permissionJoinTimeout: 20 * time.Millisecond,
			permissionResultSent:  func() { close(resolverFinished) },
		})
	}()

	select {
	case <-provider.answerStarted:
	case <-time.After(time.Second):
		t.Fatal("AnswerPermission did not reach the forced blocking point")
	}
	close(interruptCh)
	select {
	case <-provider.cancelStarted:
	case <-time.After(time.Second):
		t.Fatal("provider.Cancel did not reach the forced blocking point")
	}
	select {
	case result := <-resultCh:
		if result.StopReason != "cancelled" {
			t.Fatalf("StopReason = %q, want cancelled", result.StopReason)
		}
	case <-time.After(time.Second):
		t.Fatal("non-cooperative provider calls made termination unbounded")
	}
	select {
	case <-provider.answerReturned:
		t.Fatal("AnswerPermission unexpectedly returned before test cleanup")
	default:
	}
	select {
	case <-provider.cancelReturned:
		t.Fatal("Cancel unexpectedly returned before test cleanup")
	default:
	}
}

// TestAutoAnswerGoroutineDoesNotBlockEventLoop verifies that a slow
// AnswerPermission call does not prevent subsequent events from draining.
// It injects a blocking provider, sends a permission.request followed by
// session.end, and asserts that session.end is processed before the
// AnswerPermission goroutine returns.
func TestAutoAnswerGoroutineDoesNotBlockEventLoop(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 3)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_1",
			"options": []any{
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}

	provider := newBlockingFakeProvider()
	promptDone := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	var result sessionResult
	go func() {
		defer wg.Done()
		result = waitForSessionForTest(context.Background(), provider, writer, nil, nil, eventCh, promptDone, nil, "ses_1", "run_1", "", true, DefaultPermissionClaimTimeout, nil, io.Discard)
	}()

	// Wait until AnswerPermission has been called (goroutine is now blocked inside it).
	select {
	case <-provider.answered:
	case <-time.After(5 * time.Second):
		t.Fatal("AnswerPermission was not called within 5 seconds")
	}

	// Now push session.end — this must drain even while AnswerPermission is blocked.
	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_1",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	promptDone <- nil
	close(eventCh)

	// Confirm the loop is not wedged by verifying it reads the session.end.
	// The goroutine cannot unblock (close(provider.unblockAnswer) below) until
	// AFTER sessionEndSeen fires, so there is no race: the goroutine is held
	// blocked in AnswerPermission for the entire duration of the poll.
	sessionEndSeen := make(chan struct{})
	go func() {
		// Poll the event log briefly. Each iteration is 50ms; 50 iterations = 2.5s.
		for i := 0; i < 50; i++ {
			time.Sleep(50 * time.Millisecond)
			data, _ := os.ReadFile(eventsPath)
			if strings.Contains(string(data), "session.end") {
				close(sessionEndSeen)
				return
			}
		}
	}()

	select {
	case <-sessionEndSeen:
		// Good — session.end was written while AnswerPermission was still blocked.
	case <-time.After(5 * time.Second):
		t.Fatal("session.end was not written within 5 seconds; event loop appears blocked")
	}

	// Unblock the goroutine and wait for the loop to finish.
	close(provider.unblockAnswer)
	wg.Wait()
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("WaitForSession() = %d, want 0", result.ExitCode)
	}
}

// TestAutoAnswerMissingRequestIDEmitsErrorNotWorking verifies that a
// permission.request without a request_id does NOT transition the agent to
// "working" and DOES emit an avenor.error event.
func TestAutoAnswerMissingRequestIDEmitsErrorNotWorking(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 3)
	// permission.request without request_id
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		// Nil fields exercise request-event enrichment before serialization.
		Fields: nil,
	}
	// session.end so the loop can terminate
	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_1",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(eventCh)

	promptDone := make(chan error, 1)
	promptDone <- nil

	provider := &cliFakeProvider{}
	var stderr strings.Builder
	result := waitForSessionForTest(context.Background(), provider, writer, nil, nil, eventCh, promptDone, nil, "ses_1", "run_1", "", true, DefaultPermissionClaimTimeout, nil, &stderr)
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("waitForSessionForTest() = %d, want 0", result.ExitCode)
	}
	// AnswerPermission must NOT have been called.
	if provider.answerRequestID != "" {
		t.Fatalf("AnswerPermission was called with requestID=%q, want no call", provider.answerRequestID)
	}

	got := readEventLogForTest(t, eventsPath)
	var hasError, hasWorking, hasRequest bool
	for _, ev := range got {
		if ev.Event == "permission.request" {
			hasRequest = true
			if gotResolver := ev.Fields["resolver"]; gotResolver != "automatic" {
				t.Fatalf("permission.request resolver = %v, want automatic", gotResolver)
			}
		}
		if ev.Event == "avenor.error" {
			msg, _ := ev.Fields["message"].(string)
			if strings.Contains(msg, "missing request_id") {
				hasError = true
			}
		}
		if ev.Event == "agent.status" && ev.Fields["phase"] == "working" {
			hasWorking = true
		}
	}
	if !hasRequest {
		t.Fatalf("permission.request was not emitted; events: %+v", got)
	}
	if !hasError {
		t.Fatalf("expected avenor.error with 'missing request_id'; events: %+v", got)
	}
	if hasWorking {
		t.Fatalf("agent.status working emitted despite missing request_id; events: %+v", got)
	}
}

// TestAutoAnswerEmitsPermissionResponse verifies that a successful auto-answer
// emits a permission.response event with the correct fields.
func TestAutoAnswerEmitsPermissionResponse(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 3)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_42",
			"options": []any{
				map[string]any{"optionId": "deny_it", "kind": "reject"},
				map[string]any{"optionId": "allow_it", "kind": "allow"},
			},
		},
	}
	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_1",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(eventCh)

	promptDone := make(chan error, 1)
	promptDone <- nil

	provider := &cliFakeProvider{}
	var stderr strings.Builder
	result := waitForSessionForTest(context.Background(), provider, writer, nil, nil, eventCh, promptDone, nil, "ses_1", "run_1", "mylabel", true, DefaultPermissionClaimTimeout, nil, &stderr)
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("waitForSessionForTest() = %d, want 0; stderr=%s", result.ExitCode, stderr.String())
	}

	got := readEventLogForTest(t, eventsPath)
	var resp *events.Event
	for i := range got {
		if got[i].Event == "permission.response" {
			resp = &got[i]
			break
		}
	}
	if resp == nil {
		t.Fatalf("permission.response not found in events: %+v", got)
	}
	if resp.Fields["request_id"] != "req_42" {
		t.Errorf("request_id = %v, want req_42", resp.Fields["request_id"])
	}
	if resp.Fields["option_id"] != "allow_it" {
		t.Errorf("option_id = %v, want allow_it", resp.Fields["option_id"])
	}
	if resp.Fields["kind"] != "allow" {
		t.Errorf("kind = %v, want allow", resp.Fields["kind"])
	}
	if resp.Fields["source"] != "avenor" {
		t.Errorf("source = %v, want avenor", resp.Fields["source"])
	}
	if resp.Fields["run_id"] != "run_1" {
		t.Errorf("run_id = %v, want run_1", resp.Fields["run_id"])
	}
	if resp.Fields["run_label"] != "mylabel" {
		t.Errorf("run_label = %v, want mylabel", resp.Fields["run_label"])
	}
	if ts, ok := resp.Fields["ts"].(float64); !ok || ts <= 0 {
		t.Errorf("ts missing or non-positive: %v", resp.Fields["ts"])
	}
}

// TestAutoAnswerAnswerPermissionErrorEmitsError verifies that when
// AnswerPermission returns an error before an authoritative terminal, the loop
// emits an avenor.error event and exits with code 1.
func TestAutoAnswerAnswerPermissionErrorEmitsError(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 1)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_err",
			"options": []any{
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}
	close(eventCh)

	promptDone := make(chan error, 1)
	promptDone <- nil

	provider := &errorFakeProvider{answerErr: fmt.Errorf("backend unavailable")}
	var stderr strings.Builder
	result := waitForSessionForTest(context.Background(), provider, writer, nil, nil, eventCh, promptDone, nil, "ses_1", "run_1", "", true, DefaultPermissionClaimTimeout, nil, &stderr)
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatalf("close writer: %v", closeErr)
	}
	if result.ExitCode != 1 {
		t.Fatalf("waitForSessionForTest() = %d, want 1 (error exit)", result.ExitCode)
	}

	got := readEventLogForTest(t, eventsPath)
	var hasError bool
	for _, ev := range got {
		if ev.Event == "avenor.error" {
			msg, _ := ev.Fields["message"].(string)
			if strings.Contains(msg, "backend unavailable") {
				hasError = true
			}
		}
	}
	if !hasError {
		t.Fatalf("expected avenor.error with 'backend unavailable'; events: %+v", got)
	}
}

// TestAutoAnswerWorkingStatusEmittedOnPermissionResume verifies that when
// AnswerPermission completes before session.end, the event loop emits
// agent.status working to signal the agent has resumed.
// The test uses file-polling to confirm the working event is written before
// sending session.end, ensuring the permissionDone case fires first.
func TestAutoAnswerWorkingStatusEmittedOnPermissionResume(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	provider := newBlockingFakeProvider()
	eventCh := make(chan events.Event, 4)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_resume",
			"options": []any{
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}

	promptDone := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	var result sessionResult
	go func() {
		defer wg.Done()
		result = waitForSessionForTest(context.Background(), provider, writer, nil, nil, eventCh, promptDone, nil, "ses_1", "run_1", "", true, DefaultPermissionClaimTimeout, nil, io.Discard)
	}()

	// Wait until AnswerPermission is called, then unblock it so the
	// permissionDone case can fire and emit agent.status working.
	select {
	case <-provider.answered:
	case <-time.After(5 * time.Second):
		t.Fatal("AnswerPermission not called within 5 seconds")
	}
	close(provider.unblockAnswer)

	// Poll for agent.status working in the event log. This confirms the
	// permissionDone case has fired and written the status before we send
	// session.end (which would advance the tracker to phaseDone and suppress
	// the working emission).
	workingSeen := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(25 * time.Millisecond)
			data, _ := os.ReadFile(eventsPath)
			if strings.Contains(string(data), `"working"`) {
				close(workingSeen)
				return
			}
		}
	}()
	select {
	case <-workingSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("agent.status working not written within 5 seconds")
	}

	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_1",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	promptDone <- nil
	close(eventCh)

	wg.Wait()
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatalf("close writer: %v", closeErr)
	}
	if result.ExitCode != 0 {
		t.Fatalf("WaitForSession() = %d, want 0", result.ExitCode)
	}

	got := readEventLogForTest(t, eventsPath)
	// Find permission.request index, then verify agent.status working follows it.
	requestIdx := -1
	var workingAfterRequest bool
	for i, ev := range got {
		if ev.Event == "permission.request" {
			requestIdx = i
		}
		if requestIdx >= 0 && ev.Event == "agent.status" && ev.Fields["phase"] == "working" {
			workingAfterRequest = true
		}
	}
	if requestIdx < 0 {
		t.Fatalf("permission.request not written: %+v", got)
	}
	if !workingAfterRequest {
		t.Fatalf("agent.status working not emitted after permission resolved; events: %+v", got)
	}
}

// TestControlPermissionResolution verifies that a control socket owner can
// answer a permission.request via answer_permission, which flows through
// resolvePermission and calls AnswerPermission on the provider.
func TestControlPermissionResolution(t *testing.T) {
	state := control.NewState("run_1", "mylabel", 0)
	cs := control.NewServer(state)
	socketPath := filepath.Join(t.TempDir(), "cs.sock")
	if err := cs.Start(socketPath); err != nil {
		t.Fatalf("start control: %v", err)
	}
	defer cs.Stop()

	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 2)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_ctrl",
		Fields: map[string]any{
			"request_id": "req_ctrl",
			"question":   "Allow?",
			"options": []any{
				map[string]any{"optionId": "deny", "kind": "reject"},
				map[string]any{"optionId": "allow_x", "kind": "allow", "requiresMessage": true},
			},
		},
	}
	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_ctrl",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(eventCh)
	promptDone := make(chan error, 1)
	promptDone <- nil

	provider := &cliFakeProvider{}

	// Connect to control socket BEFORE starting waitForSession so
	// the answer arrives within the claim window.
	deadline := time.Now().Add(5 * time.Second)
	var ctrlConn net.Conn
	for {
		ctrlConn, err = net.DialTimeout("unix", socketPath, 2*time.Second)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial control socket: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer ctrlConn.Close()
	waitForControlClientForTest(t, cs)

	var wg sync.WaitGroup
	wg.Add(1)
	var result sessionResult
	go func() {
		defer wg.Done()
		result = waitForSessionForTest(context.Background(), provider, writer, nil, cs, eventCh, promptDone, nil, "ses_ctrl", "run_1", "mylabel", false, 5*time.Second, nil, io.Discard)
	}()

	waitForPendingPermissionForTest(t, cs)

	// A missing required write-in is rejected without consuming the claim.
	emptyParams, _ := json.Marshal(control.PermissionAnswer{RequestID: "req_ctrl", OptionID: "allow_x"})
	emptyRequest := control.Request{JSONRPC: "2.0", ID: 0, Method: "answer_permission", Params: emptyParams}
	emptyBytes, _ := json.Marshal(emptyRequest)
	if _, err := ctrlConn.Write(append(emptyBytes, '\n')); err != nil {
		t.Fatalf("write empty answer_permission: %v", err)
	}
	_ = ctrlConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(ctrlConn)
	if !scanner.Scan() {
		t.Fatal("no response to empty answer_permission")
	}
	var emptyResponse control.Response
	if err := json.Unmarshal(scanner.Bytes(), &emptyResponse); err != nil || emptyResponse.Error == nil {
		t.Fatalf("empty write-in response = %#v, %v", emptyResponse, err)
	}

	// A valid write-in can still answer the same pending claim.
	params, _ := json.Marshal(control.PermissionAnswer{RequestID: "req_ctrl", OptionID: "allow_x", Message: "typed response"})
	req := control.Request{JSONRPC: "2.0", ID: 1, Method: "answer_permission", Params: params}
	b, _ := json.Marshal(req)
	b = append(b, '\n')
	if _, err := ctrlConn.Write(b); err != nil {
		t.Fatalf("write answer_permission: %v", err)
	}

	_ = ctrlConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if !scanner.Scan() {
		t.Fatal("no response to answer_permission")
	}
	var resp control.Response
	if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("answer_permission error: %+v", resp.Error)
	}

	wg.Wait()
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("WaitForSession() = %d, want 0", result.ExitCode)
	}
	if provider.answerRequestID != "req_ctrl" {
		t.Fatalf("answerRequestID = %q, want req_ctrl", provider.answerRequestID)
	}
	if !provider.answerResponse.Allow || provider.answerResponse.Message != "typed response" {
		t.Fatalf("answerResponse = %#v", provider.answerResponse)
	}
	for _, event := range readEventLogForTest(t, eventsPath) {
		if _, leaked := event.Fields["message"]; leaked {
			t.Fatalf("permission message leaked to event log: %#v", event)
		}
	}
}

// TestControlPermissionNonLiteralOptionIDMapsByKind verifies that the control
// answer path maps the optionID to Allow by looking up the option kind from
// the permission.request options, even when the option ID is not the literal
// string "allow". It tests both the allow path (kind "allow" → Allow=true)
// and the reject path (kind "reject" → Allow=false).
func TestControlPermissionNonLiteralOptionIDMapsByKind(t *testing.T) {
	run := func(t *testing.T, optionID string, expectAllow bool) {
		t.Helper()

		dir := t.TempDir()
		eventsPath := filepath.Join(dir, "events.ndjson")

		sockDir, err := os.MkdirTemp("", "av-pnl-*")
		if err != nil {
			t.Fatalf("mkdirtemp: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(sockDir) })

		state := control.NewState("run_map", "label", 0)
		cs := control.NewServer(state)
		socketPath := filepath.Join(sockDir, "m.sock")
		if err := cs.Start(socketPath); err != nil {
			t.Fatalf("start control: %v", err)
		}
		defer cs.Stop()

		writer, err := NewEventWriter(eventsPath)
		if err != nil {
			t.Fatalf("newEventWriter: %v", err)
		}

		eventCh := make(chan events.Event, 2)
		eventCh <- events.Event{
			Event:     "permission.request",
			SessionID: "ses_map",
			Fields: map[string]any{
				"request_id": "req_map",
				"options": []any{
					map[string]any{"optionId": "nope_please", "kind": "reject"},
					map[string]any{"optionId": "yes_please", "kind": "allow"},
				},
			},
		}
		eventCh <- events.Event{
			Event:     "session.end",
			SessionID: "ses_map",
			Fields:    map[string]any{"stop_reason": "end_turn"},
		}
		close(eventCh)
		promptDone := make(chan error, 1)
		promptDone <- nil

		provider := &cliFakeProvider{}

		deadline := time.Now().Add(5 * time.Second)
		var ctrlConn net.Conn
		for {
			ctrlConn, err = net.DialTimeout("unix", socketPath, 2*time.Second)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("dial control socket: %v", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		defer ctrlConn.Close()
		waitForControlClientForTest(t, cs)

		var wg sync.WaitGroup
		wg.Add(1)
		var result sessionResult
		go func() {
			defer wg.Done()
			result = waitForSessionForTest(context.Background(), provider, writer, nil, cs, eventCh, promptDone, nil, "ses_map", "run_map", "", false, 5*time.Second, nil, io.Discard)
		}()

		waitForPendingPermissionForTest(t, cs)

		params, _ := json.Marshal(control.PermissionAnswer{RequestID: "req_map", OptionID: optionID})
		req := control.Request{JSONRPC: "2.0", ID: 1, Method: "answer_permission", Params: params}
		b, _ := json.Marshal(req)
		b = append(b, '\n')
		if _, err := ctrlConn.Write(b); err != nil {
			t.Fatalf("write answer_permission: %v", err)
		}

		_ = ctrlConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		scanner := bufio.NewScanner(ctrlConn)
		if !scanner.Scan() {
			t.Fatal("no response to answer_permission")
		}
		var resp control.Response
		if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp.Error != nil {
			t.Fatalf("answer_permission error: %+v", resp.Error)
		}

		wg.Wait()
		if err := writer.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}
		if result.ExitCode != 0 {
			t.Fatalf("WaitForSession() = %d, want 0", result.ExitCode)
		}
		if provider.answerResponse.Allow != expectAllow {
			t.Fatalf("answerResponse.Allow = %v, want %v (optionID=%q)", provider.answerResponse.Allow, expectAllow, optionID)
		}
		if provider.answerResponse.OptionID != optionID {
			t.Fatalf("answerResponse.OptionID = %q, want %q", provider.answerResponse.OptionID, optionID)
		}
	}

	t.Run("allow_by_kind", func(t *testing.T) {
		run(t, "yes_please", true)
	})
	t.Run("reject_by_kind", func(t *testing.T) {
		run(t, "nope_please", false)
	})
}

func TestControlPermissionRejectsUnknownOptionID(t *testing.T) {
	sockDir, err := os.MkdirTemp("", "av-bad-*")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })

	state := control.NewState("run_bad", "label", 0)
	cs := control.NewServer(state)
	socketPath := filepath.Join(sockDir, "cs.sock")
	if err := cs.Start(socketPath); err != nil {
		t.Fatalf("start control: %v", err)
	}
	defer cs.Stop()

	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 1)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_bad",
		Fields: map[string]any{
			"request_id": "req_bad",
			"options": []any{
				map[string]any{"optionId": "allow_x", "kind": "allow"},
				map[string]any{"optionId": "deny_x", "kind": "reject"},
			},
		},
	}
	close(eventCh)
	promptDone := make(chan error, 1)
	promptDone <- nil

	provider := &cliFakeProvider{}
	deadline := time.Now().Add(5 * time.Second)
	var ctrlConn net.Conn
	for {
		ctrlConn, err = net.DialTimeout("unix", socketPath, 2*time.Second)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial control socket: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer ctrlConn.Close()
	waitForControlClientForTest(t, cs)

	var wg sync.WaitGroup
	wg.Add(1)
	var result sessionResult
	go func() {
		defer wg.Done()
		result = waitForSessionForTest(context.Background(), provider, writer, nil, cs, eventCh, promptDone, nil, "ses_bad", "run_bad", "", false, 5*time.Second, nil, io.Discard)
	}()
	waitForPendingPermissionForTest(t, cs)

	params, _ := json.Marshal(control.PermissionAnswer{RequestID: "req_bad", OptionID: "missing"})
	req := control.Request{JSONRPC: "2.0", ID: 1, Method: "answer_permission", Params: params}
	b, _ := json.Marshal(req)
	b = append(b, '\n')
	if _, err := ctrlConn.Write(b); err != nil {
		t.Fatalf("write answer_permission: %v", err)
	}

	wg.Wait()
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("WaitForSession() = %d, want 1", result.ExitCode)
	}
	if provider.answerRequestID != "" {
		t.Fatalf("AnswerPermission was called for request %q", provider.answerRequestID)
	}
}

func TestControlPermissionRejectsUnsupportedKind(t *testing.T) {
	sockDir, err := os.MkdirTemp("", "av-bad-kind-*")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })

	state := control.NewState("run_bad_kind", "label", 0)
	cs := control.NewServer(state)
	socketPath := filepath.Join(sockDir, "cs.sock")
	if err := cs.Start(socketPath); err != nil {
		t.Fatalf("start control: %v", err)
	}
	defer cs.Stop()

	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 1)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_bad_kind",
		Fields: map[string]any{
			"request_id": "req_bad_kind",
			"options": []any{
				map[string]any{"optionId": "weird", "kind": "maybe"},
			},
		},
	}
	close(eventCh)
	promptDone := make(chan error, 1)
	promptDone <- nil

	provider := &cliFakeProvider{}
	deadline := time.Now().Add(5 * time.Second)
	var ctrlConn net.Conn
	for {
		ctrlConn, err = net.DialTimeout("unix", socketPath, 2*time.Second)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial control socket: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer ctrlConn.Close()
	waitForControlClientForTest(t, cs)

	var wg sync.WaitGroup
	wg.Add(1)
	var result sessionResult
	go func() {
		defer wg.Done()
		result = waitForSessionForTest(context.Background(), provider, writer, nil, cs, eventCh, promptDone, nil, "ses_bad_kind", "run_bad_kind", "", false, 5*time.Second, nil, io.Discard)
	}()
	waitForPendingPermissionForTest(t, cs)

	params, _ := json.Marshal(control.PermissionAnswer{RequestID: "req_bad_kind", OptionID: "weird"})
	req := control.Request{JSONRPC: "2.0", ID: 1, Method: "answer_permission", Params: params}
	b, _ := json.Marshal(req)
	b = append(b, '\n')
	if _, err := ctrlConn.Write(b); err != nil {
		t.Fatalf("write answer_permission: %v", err)
	}

	wg.Wait()
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("WaitForSession() = %d, want 1", result.ExitCode)
	}
	if provider.answerRequestID != "" {
		t.Fatalf("AnswerPermission was called for request %q", provider.answerRequestID)
	}
}

// TestAutoAnswerSecondRequestWhilePendingEmitsErrorAndExits verifies that
// receiving a second permission.request while one is already in-flight causes
// the loop to emit an avenor.error and exit with code 1.
func TestAutoAnswerSecondRequestWhilePendingEmitsErrorAndExits(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	provider := newBlockingFakeProvider()
	eventCh := make(chan events.Event, 4)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_first",
			"options": []any{
				map[string]any{"optionId": "allow", "kind": "allow"},
			},
		},
	}

	promptDone := make(chan error, 1)
	resultCh := make(chan sessionResult, 1)
	overlapSeen := make(chan struct{})
	sink := &signalEventSink{
		EventSink: writer,
		eventName: "avenor.error",
		field:     "message",
		value:     "another permission request is already pending",
		signal:    overlapSeen,
	}
	go func() {
		resultCh <- waitForSessionForTest(context.Background(), provider, sink, nil, nil, eventCh, promptDone, nil, "ses_1", "run_1", "", true, DefaultPermissionClaimTimeout, nil, io.Discard)
	}()
	var unblockOnce sync.Once
	unblockProvider := func() { unblockOnce.Do(func() { close(provider.unblockAnswer) }) }
	t.Cleanup(unblockProvider)

	// Wait until the first AnswerPermission call is in-flight (blocking).
	select {
	case <-provider.answered:
	case <-time.After(5 * time.Second):
		t.Fatal("AnswerPermission not called within 5 seconds")
	}

	// Send a second permission.request while the first is still pending.
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_first",
			"options": []any{
				map[string]any{"optionId": "allow", "kind": "reject"},
			},
		},
	}
	// Keep the first resolver active until the overlap error is emitted. The
	// deferred resolver join cannot complete until the provider is released.
	select {
	case <-overlapSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForSession did not reject overlapping request")
	}
	unblockProvider()

	var result sessionResult
	select {
	case result = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForSession did not finish after rejecting overlapping request")
	}
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatalf("close writer: %v", closeErr)
	}
	if result.ExitCode != 1 {
		t.Fatalf("WaitForSession() = %d, want 1 (duplicate permission guard)", result.ExitCode)
	}

	got := readEventLogForTest(t, eventsPath)
	var hasError bool
	requestCount := 0
	for _, ev := range got {
		if ev.Event == "permission.request" {
			requestCount++
			options, _ := ev.Fields["options"].([]any)
			if len(options) > 0 {
				option, _ := options[0].(map[string]any)
				if kind, _ := option["kind"].(string); kind != "allow" {
					t.Fatalf("published overlapping request semantics: %+v", ev)
				}
			}
		}
		if ev.Event == "avenor.error" {
			msg, _ := ev.Fields["message"].(string)
			if strings.Contains(msg, "already pending") {
				hasError = true
			}
		}
	}
	if !hasError {
		t.Fatalf("expected avenor.error about 'already pending'; events: %+v", got)
	}
	if requestCount != 1 {
		t.Fatalf("permission.request count = %d, want only the first; events: %+v", requestCount, got)
	}
}

func TestPermissionRequestConsumesCompletedResolverBeforeReusedID(t *testing.T) {
	eventsPath := filepath.Join(t.TempDir(), "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	provider := newSequentialPermissionProvider()
	completionSent := make(chan struct{})
	gateTimedOut := make(chan struct{}, 1)
	var completionOnce sync.Once
	sink := &callbackEventSink{
		EventSink: writer,
		eventName: "agent.status",
		field:     "label",
		value:     "second request",
		callback: func() {
			close(provider.releaseFirst)
			select {
			case <-completionSent:
			case <-time.After(5 * time.Second):
				gateTimedOut <- struct{}{}
			}
		},
	}
	controlServer := control.NewServer(control.NewState("run_1", "", 0))
	eventCh := make(chan events.Event, 4)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_reused",
			"question":   "first request",
			"options": []any{
				map[string]any{"optionId": "allow_first", "kind": "allow"},
			},
		},
	}
	promptDone := make(chan error, 1)
	resultCh := make(chan sessionResult, 1)
	go func() {
		resultCh <- WaitForSession(context.Background(), provider, SessionWaitConfig{
			EventCh:              eventCh,
			PromptDone:           promptDone,
			SessionID:            "ses_1",
			RunID:                "run_1",
			PermissionClaimScope: "rt_1",
			AutoApprove:          true,
		}, SessionWaitDeps{
			Writer:        sink,
			ControlServer: controlServer,
			Stderr:        io.Discard,
			permissionResultSent: func() {
				completionOnce.Do(func() { close(completionSent) })
			},
		})
	}()

	select {
	case <-provider.firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first permission resolution did not start")
	}

	// The second waiting status is emitted after the event loop selects request
	// B but before it checks permissionDone. Its sink callback releases A and
	// waits until A's buffered completion result is definitely ready, forcing
	// the regression window without scheduler timing.
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_reused",
			"question":   "second request",
			"options": []any{
				map[string]any{"optionId": "allow_second", "kind": "allow"},
			},
		},
	}

	select {
	case <-provider.secondAnswered:
	case result := <-resultCh:
		_ = writer.Close()
		t.Fatalf("WaitForSession exited before resolving the second request: %+v", result)
	case <-time.After(5 * time.Second):
		t.Fatal("second permission resolution did not complete")
	}
	select {
	case <-gateTimedOut:
		t.Fatal("timed out waiting for the first completion result")
	default:
	}

	eventCh <- events.Event{Event: "session.end", SessionID: "ses_1", Fields: map[string]any{"stop_reason": "end_turn"}}
	promptDone <- nil
	close(eventCh)

	var result sessionResult
	select {
	case result = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForSession did not finish")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("WaitForSession() = %d, want 0", result.ExitCode)
	}
	if got := provider.answers(); len(got) != 2 || got[0] != "req_reused" || got[1] != "req_reused" {
		t.Fatalf("provider answers = %v, want [req_reused req_reused]", got)
	}

	var lifecycle []string
	for _, event := range readEventLogForTest(t, eventsPath) {
		switch event.Event {
		case "permission.request":
			requestID, _ := event.Fields["request_id"].(string)
			options, _ := event.Fields["options"].([]any)
			option, _ := options[0].(map[string]any)
			optionID, _ := option["optionId"].(string)
			lifecycle = append(lifecycle, event.Event+":"+requestID+":"+optionID)
		case "permission.response":
			requestID, _ := event.Fields["request_id"].(string)
			optionID, _ := event.Fields["option_id"].(string)
			lifecycle = append(lifecycle, event.Event+":"+requestID+":"+optionID)
		case "avenor.error":
			message, _ := event.Fields["message"].(string)
			if strings.Contains(message, "already pending") {
				t.Fatalf("completed resolver was reported as overlapping: %+v", event)
			}
		}
	}
	want := []string{
		"permission.request:req_reused:allow_first",
		"permission.response:req_reused:allow_first",
		"permission.request:req_reused:allow_second",
		"permission.response:req_reused:allow_second",
	}
	if fmt.Sprint(lifecycle) != fmt.Sprint(want) {
		t.Fatalf("permission lifecycle = %v, want %v", lifecycle, want)
	}
	if state := controlServer.PermissionResolverState("rt_1", "req_reused"); state != control.PermissionResolverResolved {
		t.Fatalf("resolver state = %v, want resolved", state)
	}
}

func TestPermissionRequestRejectsExistingDirectDeliveryBeforePublishing(t *testing.T) {
	eventsPath := filepath.Join(t.TempDir(), "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	cs := control.NewServer(control.NewState("run_1", "", 0))
	if !cs.PreparePermissionClaim("rt_1", "req_reused", control.PermissionResolverNoResolver, nil) {
		t.Fatal("PreparePermissionClaim returned false")
	}
	if got := cs.DeliverPendingPermission("rt_1", "req_reused", "allow", ""); got != control.PermissionAnswerNoResolver {
		t.Fatalf("initial delivery = %v, want no-resolver", got)
	}

	provider := newBlockingFakeProvider()
	go provider.AnswerPermission(context.Background(), "ses_1", "req_reused", runtime.PermissionResponse{
		Allow: true, OptionID: "allow",
	})
	select {
	case <-provider.answered:
	case <-time.After(time.Second):
		t.Fatal("existing provider delivery did not start")
	}
	var releaseOnce sync.Once
	releaseProvider := func() { releaseOnce.Do(func() { close(provider.unblockAnswer) }) }
	t.Cleanup(releaseProvider)

	eventCh := make(chan events.Event, 1)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_reused",
			"options": []any{
				map[string]any{"optionId": "different", "kind": "reject"},
			},
		},
	}
	close(eventCh)

	resultCh := make(chan sessionResult, 1)
	go func() {
		resultCh <- WaitForSession(context.Background(), provider, SessionWaitConfig{
			EventCh:              eventCh,
			PromptDone:           make(chan error),
			SessionID:            "ses_1",
			RunID:                "run_1",
			PermissionClaimScope: "rt_1",
			AutoApprove:          true,
		}, SessionWaitDeps{Writer: writer, ControlServer: cs, Stderr: io.Discard})
	}()
	var result sessionResult
	select {
	case result = <-resultCh:
	case <-time.After(time.Second):
		t.Fatal("WaitForSession did not reject reused request")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	releaseProvider()

	if result.ExitCode != 1 {
		t.Fatalf("WaitForSession() = %d, want 1", result.ExitCode)
	}
	if state := cs.PermissionResolverState("rt_1", "req_reused"); state != control.PermissionResolverDirectDelivery {
		t.Fatalf("resolver state = %v, want direct-delivery", state)
	}
	logged := readEventLogForTest(t, eventsPath)
	var requestCount, overlapErrors int
	for _, event := range logged {
		if event.Event == "permission.request" {
			requestCount++
		}
		if event.Event == "avenor.error" {
			message, _ := event.Fields["message"].(string)
			if strings.Contains(message, "already pending") {
				overlapErrors++
			}
		}
	}
	if requestCount != 0 {
		t.Fatalf("published %d rejected permission.request events", requestCount)
	}
	if overlapErrors != 1 {
		t.Fatalf("overlap errors = %d, want 1; events: %+v", overlapErrors, logged)
	}
}

func TestPermissionRequestRejectsExistingNoResolverBeforePublishing(t *testing.T) {
	eventsPath := filepath.Join(t.TempDir(), "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}
	cs := control.NewServer(control.NewState("run_1", "", 0))
	if !cs.PreparePermissionClaim("rt_1", "req_reused", control.PermissionResolverNoResolver, nil) {
		t.Fatal("PreparePermissionClaim returned false")
	}

	eventCh := make(chan events.Event, 1)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "req_reused",
			"options": []any{
				map[string]any{"optionId": "replacement", "kind": "allow"},
			},
		},
	}
	close(eventCh)
	result := WaitForSession(context.Background(), &cliFakeProvider{}, SessionWaitConfig{
		EventCh:              eventCh,
		PromptDone:           make(chan error),
		SessionID:            "ses_1",
		RunID:                "run_1",
		PermissionClaimScope: "rt_1",
		AutoApprove:          true,
	}, SessionWaitDeps{Writer: writer, ControlServer: cs, Stderr: io.Discard})
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	if result.ExitCode != 1 {
		t.Fatalf("WaitForSession() = %d, want 1", result.ExitCode)
	}
	if state := cs.PermissionResolverState("rt_1", "req_reused"); state != control.PermissionResolverNoResolver {
		t.Fatalf("resolver state = %v, want no-resolver", state)
	}
	for _, event := range readEventLogForTest(t, eventsPath) {
		if event.Event == "permission.request" {
			t.Fatalf("rejected request was published: %+v", event)
		}
	}
	if got := cs.DeliverPendingPermission("rt_1", "req_reused", "original", ""); got != control.PermissionAnswerNoResolver {
		t.Fatalf("original claim delivery = %v, want no-resolver", got)
	}
}

// TestControlPermissionClaimDisconnectFallsThrough verifies that when a
// client is connected at the HasClients() gate but never sends
// answer_permission, resolvePermission falls through to the file-handler (or
// no-resolver) when the client disconnects, rather than blocking indefinitely.
func TestControlPermissionClaimDisconnectFallsThrough(t *testing.T) {
	// Use /tmp to keep socket path under the 104-byte macOS limit.
	sockDir, err := os.MkdirTemp("", "av-pct-*")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })

	state := control.NewState("run_timeout", "label", 0)
	cs := control.NewServer(state)
	socketPath := filepath.Join(sockDir, "t.sock")
	if err := cs.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer cs.Stop()

	// Connect a client so HasClients() returns true — but never answer.
	silentConn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial silent client: %v", err)
	}
	t.Cleanup(func() { silentConn.Close() })

	waitForControlClientForTest(t, cs)

	event := events.Event{
		Event:     "permission.request",
		SessionID: "ses_timeout",
		Fields: map[string]any{
			"request_id": "req_timeout",
			"options": []any{
				map[string]any{"optionId": "deny", "kind": "reject"},
				map[string]any{"optionId": "allow_t", "kind": "allow"},
			},
		},
	}

	provider := &cliFakeProvider{}

	// claimTimeout = 0 means no wall-clock timer; fallback happens only on
	// client disconnect.
	resultCh := make(chan permissionResult, 1)
	go func() {
		resultCh <- resolvePermission(context.Background(), provider, nil, cs, event, "ses_timeout", "", "req_timeout", false, 0)
	}()

	// Wait for the claim to be registered before disconnecting.
	waitForPendingPermissionForTest(t, cs)

	// Disconnect the silent client — this should trigger the fallback.
	silentConn.Close()

	var res permissionResult
	select {
	case res = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("resolvePermission did not return within 5 seconds after client disconnect")
	}

	// No file-handler provided, so result should be the no-resolver sentinel.
	if res.source != "none" {
		t.Fatalf("result source = %q, want \"none\"", res.source)
	}
	if res.err != nil {
		t.Fatalf("unexpected error: %v", res.err)
	}
	// The claim should have been cleaned up.
	if cs.HasPendingPermission() {
		t.Fatal("permission claim still registered after disconnect")
	}
	// Control path fell through without answering — AnswerPermission must NOT have been called.
	if provider.answerRequestID != "" {
		t.Fatalf("provider.AnswerPermission was called with requestID=%q, want no call (control path should not answer on disconnect)", provider.answerRequestID)
	}
}

func TestScopedControlPermissionClaimCompletesAndReleasesForNextRequest(t *testing.T) {
	cs := control.NewServer(control.NewState("run_1", "", 0))
	socketPath := shortControlSocketPath(t)
	if err := cs.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer cs.Stop()
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	for !cs.HasClients() {
		if time.Now().After(deadline) {
			t.Fatal("control client was not registered")
		}
		time.Sleep(time.Millisecond)
	}
	provider := &permissionLifecycleProvider{answers: make(chan string, 2)}
	sink := &permissionLifecycleSink{responses: make(chan string, 2)}
	eventCh := make(chan events.Event)
	promptDone := make(chan error, 1)
	promptDone <- nil
	resultCh := make(chan sessionResult, 1)

	go func() {
		resultCh <- WaitForSession(context.Background(), provider, SessionWaitConfig{
			EventCh:                eventCh,
			PromptDone:             promptDone,
			SessionID:              "ses_1",
			RunID:                  "run_1",
			PermissionClaimScope:   "rt_1",
			PermissionClaimTimeout: time.Second,
		}, SessionWaitDeps{
			Writer:        sink,
			ControlServer: cs,
			Stderr:        io.Discard,
		})
	}()

	request := events.Event{
		Event:     "permission.request",
		SessionID: "ses_1",
		Fields: map[string]any{
			"request_id": "0",
			"options": []any{
				map[string]any{"optionId": "always", "kind": "allow_always"},
			},
		},
	}
	for i := 0; i < 2; i++ {
		eventCh <- request
		waitForPendingPermissionForTest(t, cs)
		if !cs.AnswerPendingPermission("rt_1", "0", "always", "") {
			t.Fatalf("answer %d was not delivered", i+1)
		}
		select {
		case got := <-provider.answers:
			if got != "0" {
				t.Fatalf("provider request %d = %q", i+1, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("provider was not called for request %d", i+1)
		}
		select {
		case got := <-sink.responses:
			if got != "0" {
				t.Fatalf("permission.response %d request = %q", i+1, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("permission.response was not emitted for request %d", i+1)
		}
	}

	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_1",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(eventCh)
	select {
	case result := <-resultCh:
		if result.ExitCode != 0 {
			t.Fatalf("WaitForSession exit code = %d", result.ExitCode)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForSession did not complete")
	}
}

func TestPermissionClaimIsReservedBeforeSynchronousRequestSink(t *testing.T) {
	cs := control.NewServer(control.NewState("run_1", "", 0))
	socketPath := shortControlSocketPath(t)
	if err := cs.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer cs.Stop()
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer conn.Close()
	clientDeadline := time.Now().Add(time.Second)
	for !cs.HasClients() {
		if time.Now().After(clientDeadline) {
			t.Fatal("control client was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	provider := &permissionLifecycleProvider{answers: make(chan string, 1)}
	sink := &permissionLifecycleSink{responses: make(chan string, 1)}
	sink.onRequest = func() {
		if !cs.AnswerPendingPermission("rt_sync", "0", "always", "") {
			t.Error("synchronous answer was not queued on the reserved claim")
		}
	}
	eventCh := make(chan events.Event, 2)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_sync",
		Fields: map[string]any{
			"request_id": "0",
			"options": []any{
				map[string]any{"optionId": "always", "kind": "allow_always"},
			},
		},
	}
	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_sync",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(eventCh)
	promptDone := make(chan error, 1)
	promptDone <- nil

	result := WaitForSession(context.Background(), provider, SessionWaitConfig{
		EventCh:                eventCh,
		PromptDone:             promptDone,
		SessionID:              "ses_sync",
		RunID:                  "run_1",
		PermissionClaimScope:   "rt_sync",
		PermissionClaimTimeout: time.Second,
	}, SessionWaitDeps{Writer: sink, ControlServer: cs, Stderr: io.Discard})
	if result.ExitCode != 0 {
		t.Fatalf("WaitForSession exit code = %d", result.ExitCode)
	}
	select {
	case got := <-provider.answers:
		if got != "0" {
			t.Fatalf("provider request = %q", got)
		}
	default:
		t.Fatal("provider did not receive the synchronously queued answer")
	}
	select {
	case <-sink.responses:
	default:
		t.Fatal("permission.response was not emitted")
	}
	if got := cs.PermissionResolverState("rt_sync", "0"); got != control.PermissionResolverResolved {
		t.Fatalf("completed claim state = %v, want resolved", got)
	}
}

func TestLateControlAnswerIsBlockedWhileFileResolverOwnsRequest(t *testing.T) {
	cs := control.NewServer(control.NewState("run_1", "", 0))
	socketPath := shortControlSocketPath(t)
	if err := cs.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer cs.Stop()
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer conn.Close()
	clientDeadline := time.Now().Add(time.Second)
	for !cs.HasClients() {
		if time.Now().After(clientDeadline) {
			t.Fatal("control client was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider := &cliFakeProvider{}
	handler := permission.NewFileHandler(filepath.Join(t.TempDir(), "permission"))
	handler.PollInterval = time.Millisecond
	resultCh := make(chan permissionResult, 1)
	event := events.Event{
		Event:     "permission.request",
		SessionID: "ses_file",
		Fields: map[string]any{
			"request_id": "0",
			"options": []any{
				map[string]any{"optionId": "always", "kind": "allow_always"},
			},
		},
	}
	cs.PreparePermissionClaim("rt_file", "0", control.PermissionResolverReserved, nil)
	go func() {
		resultCh <- resolvePermission(ctx, provider, handler, cs, event, "ses_file", "rt_file", "0", false, time.Millisecond)
	}()

	deadline := time.Now().Add(time.Second)
	for cs.PermissionResolverState("rt_file", "0") != control.PermissionResolverFile {
		if time.Now().After(deadline) {
			t.Fatal("resolver did not transition to file ownership")
		}
		time.Sleep(time.Millisecond)
	}
	if got := cs.DeliverPendingPermission("rt_file", "0", "always", ""); got != control.PermissionAnswerResolverOwned {
		t.Fatalf("late answer delivery = %v, want resolver-owned", got)
	}
	if provider.answerRequestID != "" {
		t.Fatalf("provider was called directly for %q while file resolver was active", provider.answerRequestID)
	}
	cancel()
	select {
	case <-resultCh:
	case <-time.After(time.Second):
		t.Fatal("file resolver did not stop after cancellation")
	}
	if got := cs.PermissionResolverState("rt_file", "0"); got != control.PermissionResolverUnknown {
		t.Fatalf("cancelled file claim state = %v, want removed", got)
	}
}

func TestFailedAutomaticPermissionRemovesClaim(t *testing.T) {
	cs := control.NewServer(control.NewState("run_1", "", 0))
	cs.PreparePermissionClaim("rt_auto", "0", control.PermissionResolverAutomatic, nil)
	provider := &errorFakeProvider{answerErr: errors.New("answer failed")}
	event := events.Event{
		Event:     "permission.request",
		SessionID: "ses_auto",
		Fields: map[string]any{
			"request_id": "0",
			"options": []any{
				map[string]any{"optionId": "always", "kind": "allow_always"},
			},
		},
	}
	result := resolvePermission(context.Background(), provider, nil, cs, event, "ses_auto", "rt_auto", "0", true, time.Second)
	if result.err == nil {
		t.Fatal("automatic permission unexpectedly succeeded")
	}
	if got := cs.PermissionResolverState("rt_auto", "0"); got != control.PermissionResolverUnknown {
		t.Fatalf("failed automatic claim state = %v, want removed", got)
	}
}

// TestControlPermissionClaimDisconnectFallsToFileHandler verifies that when
// the client disconnects and a file-handler is configured, resolvePermission
// routes to the file-handler rather than returning source "none".
func TestControlPermissionClaimDisconnectFallsToFileHandler(t *testing.T) {
	// Use /tmp to keep socket path under the 104-byte macOS limit.
	sockDir, err := os.MkdirTemp("", "av-pcf-*")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })

	state := control.NewState("run_fh", "label", 0)
	cs := control.NewServer(state)
	socketPath := filepath.Join(sockDir, "f.sock")
	if err := cs.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer cs.Stop()

	// Silent client keeps HasClients() true.
	silentConn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial silent client: %v", err)
	}
	t.Cleanup(func() { silentConn.Close() })

	waitForControlClientForTest(t, cs)

	dir := t.TempDir()
	base := filepath.Join(dir, "perm")
	fh := permission.NewFileHandler(base)
	fh.Timeout = 5 * time.Second
	fh.PollInterval = 20 * time.Millisecond

	// The file handler writes <base>.req then removes any pre-existing
	// <base>.req.response before polling for a new one. Write the response
	// only after the .req file appears — this eliminates the race where
	// os.Remove in the file handler deletes a response written too early
	// (which would cause the handler to wait the full fh.Timeout).
	reqPath := base + ".req"
	respPath := base + ".req.response"
	go func() {
		// Poll for the request file with a generous deadline.
		pollDeadline := time.Now().Add(5 * time.Second)
		for {
			if _, statErr := os.Stat(reqPath); statErr == nil {
				break
			}
			if time.Now().After(pollDeadline) {
				return // test will fail via fh.Timeout; no need to panic here
			}
			time.Sleep(10 * time.Millisecond)
		}
		_ = os.WriteFile(respPath, []byte(`{"outcome":"selected","option_id":"allow_fh"}`+"\n"), 0o600)
	}()

	event := events.Event{
		Event:     "permission.request",
		SessionID: "ses_fh",
		Fields: map[string]any{
			"request_id": "req_fh",
			"options": []any{
				map[string]any{"optionId": "deny", "kind": "reject"},
				map[string]any{"optionId": "allow_fh", "kind": "allow"},
			},
		},
	}

	provider := &cliFakeProvider{}

	// claimTimeout = 0 means no wall-clock timer; fallback happens only on
	// client disconnect.
	resultCh := make(chan permissionResult, 1)
	go func() {
		resultCh <- resolvePermission(context.Background(), provider, fh, cs, event, "ses_fh", "", "req_fh", false, 0)
	}()

	// Wait for the claim to be registered before disconnecting.
	waitForPendingPermissionForTest(t, cs)

	// Disconnect the silent client — this should trigger the fallback to the
	// file handler.
	silentConn.Close()

	var res permissionResult
	select {
	case res = <-resultCh:
	case <-time.After(10 * time.Second):
		t.Fatal("resolvePermission did not return within 10 seconds after client disconnect")
	}

	if res.err != nil {
		t.Fatalf("unexpected error: %v", res.err)
	}
	if res.source != "file" {
		t.Fatalf("result source = %q, want \"file\"", res.source)
	}
	if res.optionID != "allow_fh" {
		t.Fatalf("optionID = %q, want \"allow_fh\"", res.optionID)
	}
	// File handler called provider.AnswerPermission — verify the fields.
	if provider.answerRequestID != "req_fh" {
		t.Fatalf("provider.answerRequestID = %q, want \"req_fh\"", provider.answerRequestID)
	}
	if !provider.answerResponse.Allow {
		t.Fatal("provider.answerResponse.Allow = false, want true")
	}
	if provider.answerResponse.OptionID != "allow_fh" {
		t.Fatalf("provider.answerResponse.OptionID = %q, want allow_fh", provider.answerResponse.OptionID)
	}
}

func TestFilePermissionCancelledOutcomeDoesNotError(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "perm")
	fh := permission.NewFileHandler(base)
	fh.Timeout = 5 * time.Second
	fh.PollInterval = 20 * time.Millisecond

	reqPath := base + ".req"
	respPath := base + ".req.response"
	go func() {
		pollDeadline := time.Now().Add(5 * time.Second)
		for {
			if _, statErr := os.Stat(reqPath); statErr == nil {
				break
			}
			if time.Now().After(pollDeadline) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		_ = os.WriteFile(respPath, []byte(`{"outcome":"cancelled","option_id":"allow_fh"}`+"\n"), 0o600)
	}()

	event := events.Event{
		Event:     "permission.request",
		SessionID: "ses_fh",
		Fields: map[string]any{
			"request_id": "req_fh",
			"options": []any{
				map[string]any{"optionId": "allow_fh", "kind": "allow"},
			},
		},
	}

	provider := &cliFakeProvider{}
	res := resolvePermission(context.Background(), provider, fh, nil, event, "ses_fh", "", "req_fh", false, DefaultPermissionClaimTimeout)

	if res.err != nil {
		t.Fatalf("unexpected error: %v", res.err)
	}
	if !res.cancelled {
		t.Fatalf("permissionResult.cancelled = false, want true: %+v", res)
	}
	if provider.answerRequestID != "" {
		t.Fatalf("AnswerPermission was called for request %q", provider.answerRequestID)
	}
}

func TestWaitForSessionCancelledFilePermissionCancelsRun(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	base := filepath.Join(dir, "perm")
	fh := permission.NewFileHandler(base)
	fh.Timeout = 5 * time.Second
	fh.PollInterval = 20 * time.Millisecond

	reqPath := base + ".req"
	respPath := base + ".req.response"
	go func() {
		pollDeadline := time.Now().Add(5 * time.Second)
		for {
			if _, statErr := os.Stat(reqPath); statErr == nil {
				break
			}
			if time.Now().After(pollDeadline) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		_ = os.WriteFile(respPath, []byte(`{"outcome":"cancelled","option_id":"allow_fh"}`+"\n"), 0o600)
	}()

	eventCh := make(chan events.Event, 1)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_cancel_file",
		Fields: map[string]any{
			"request_id": "req_cancel_file",
			"options": []any{
				map[string]any{"optionId": "allow_fh", "kind": "allow"},
			},
		},
	}
	close(eventCh)
	promptDone := make(chan error)

	provider := &cliFakeProvider{}
	result := waitForSessionForTest(context.Background(), provider, writer, fh, nil, eventCh, promptDone, nil, "ses_cancel_file", "run_cancel_file", "", false, DefaultPermissionClaimTimeout, nil, io.Discard)
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	if result.ExitCode != 130 {
		t.Fatalf("WaitForSession() = %d, want 130", result.ExitCode)
	}
	if provider.cancelSessionID != "ses_cancel_file" {
		t.Fatalf("Cancel session = %q, want ses_cancel_file", provider.cancelSessionID)
	}
	got := readEventLogForTest(t, eventsPath)
	last := got[len(got)-1]
	if last.Event != "session.end" || last.Fields["stop_reason"] != "cancelled" {
		t.Fatalf("last event = %+v, want cancelled session.end", last)
	}
}

func TestWaitForSessionClosedEventChannelWaitsForPermissionThenErrors(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "perm")
	fh := permission.NewFileHandler(base)
	fh.Timeout = 5 * time.Second
	fh.PollInterval = 20 * time.Millisecond

	reqPath := base + ".req"
	respPath := base + ".req.response"
	go func() {
		pollDeadline := time.Now().Add(5 * time.Second)
		for {
			if _, statErr := os.Stat(reqPath); statErr == nil {
				break
			}
			if time.Now().After(pollDeadline) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		_ = os.WriteFile(respPath, []byte(`{"outcome":"selected","option_id":"allow_fh"}`+"\n"), 0o600)
	}()

	eventCh := make(chan events.Event, 1)
	eventCh <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_closed",
		Fields: map[string]any{
			"request_id": "req_closed",
			"options": []any{
				map[string]any{"optionId": "allow_fh", "kind": "allow"},
			},
		},
	}
	close(eventCh)
	promptDone := make(chan error)

	writer, err := NewEventWriter("")
	if err != nil {
		t.Fatalf("new event writer: %v", err)
	}
	defer writer.Close()

	provider := &cliFakeProvider{}
	result := waitForSessionForTest(context.Background(), provider, writer, fh, nil, eventCh, promptDone, nil, "ses_closed", "run_closed", "", false, DefaultPermissionClaimTimeout, nil, io.Discard)

	if result.ExitCode != 1 {
		t.Fatalf("WaitForSession() = %d, want 1", result.ExitCode)
	}
	if provider.answerRequestID != "req_closed" {
		t.Fatalf("AnswerPermission request = %q, want req_closed", provider.answerRequestID)
	}
}

// TestControlPermissionClaimContextCancelReleasesClaim verifies that when the
// context is cancelled while resolvePermission is waiting on the control-plane
// claim channel, the claim is cleaned up (HasPendingPermission returns false)
// and the result carries the context error.
func TestControlPermissionClaimContextCancelReleasesClaim(t *testing.T) {
	// claimTimeout = 0 means no wall-clock timer; only context cancellation
	// or client disconnect can break the wait. We test ctx cancellation here.
	claimTimeout := time.Duration(0)

	// Use /tmp to keep socket path under the 104-byte macOS limit.
	sockDir, err := os.MkdirTemp("", "av-pcx-*")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })

	state := control.NewState("run_cancel", "label", 0)
	cs := control.NewServer(state)
	socketPath := filepath.Join(sockDir, "x.sock")
	if err := cs.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer cs.Stop()

	// Silent client — never sends answer_permission, just keeps HasClients() true.
	silentConn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial silent client: %v", err)
	}
	defer silentConn.Close()

	waitForControlClientForTest(t, cs)

	event := events.Event{
		Event:     "permission.request",
		SessionID: "ses_cancel",
		Fields: map[string]any{
			"request_id": "req_cancel",
			"options": []any{
				map[string]any{"optionId": "deny", "kind": "reject"},
				map[string]any{"optionId": "allow_c", "kind": "allow"},
			},
		},
	}

	provider := &cliFakeProvider{}

	ctx, cancel := context.WithCancel(context.Background())

	resultCh := make(chan permissionResult, 1)
	go func() {
		resultCh <- resolvePermission(ctx, provider, nil, cs, event, "ses_cancel", "", "req_cancel", false, claimTimeout)
	}()

	// Wait for the claim to be registered before cancelling.
	waitForPendingPermissionForTest(t, cs)

	cancel()

	var res permissionResult
	select {
	case res = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("resolvePermission did not return within 5 seconds after context cancel")
	}

	// The result must carry the context error.
	if res.err == nil {
		t.Fatal("expected non-nil error from context cancellation, got nil")
	}
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("res.err = %v, want context.Canceled", res.err)
	}
	// The claim must have been released.
	if cs.HasPendingPermission() {
		t.Fatal("HasPendingPermission() = true after context cancel; claim was not cleaned up")
	}
	// AnswerPermission must NOT have been called (ctx was cancelled before any answer).
	if provider.answerRequestID != "" {
		t.Fatalf("provider.AnswerPermission called with requestID=%q, want no call", provider.answerRequestID)
	}
}

// TestControlPermissionClaimExplicitTimeoutFallsThrough verifies that when an
// explicit --permission-claim-timeout > 0 is set, the wall-clock timer still
// fires and causes the resolver to fall through to the file-handler or
// no-resolver path, even if the client remains connected.
func TestControlPermissionClaimExplicitTimeoutFallsThrough(t *testing.T) {
	// Shorten the claim window so the test completes quickly.
	claimTimeout := 100 * time.Millisecond

	// Use /tmp to keep socket path under the 104-byte macOS limit.
	sockDir, err := os.MkdirTemp("", "av-pet-*")
	if err != nil {
		t.Fatalf("mkdirtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })

	state := control.NewState("run_et", "label", 0)
	cs := control.NewServer(state)
	socketPath := filepath.Join(sockDir, "et.sock")
	if err := cs.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer cs.Stop()

	// Connect a client so HasClients() returns true — but never answer.
	silentConn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial silent client: %v", err)
	}
	defer silentConn.Close()

	waitForControlClientForTest(t, cs)

	event := events.Event{
		Event:     "permission.request",
		SessionID: "ses_et",
		Fields: map[string]any{
			"request_id": "req_et",
			"options": []any{
				map[string]any{"optionId": "deny", "kind": "reject"},
				map[string]any{"optionId": "allow_et", "kind": "allow"},
			},
		},
	}

	provider := &cliFakeProvider{}

	// With an explicit timeout > 0, the timer fires even though the client
	// stays connected.
	start := time.Now()
	res := resolvePermission(context.Background(), provider, nil, cs, event, "ses_et", "", "req_et", false, claimTimeout)
	elapsed := time.Since(start)

	// Lower bound: must have waited at least the claim timeout (80ms gives 20ms slack).
	if elapsed < 50*time.Millisecond {
		t.Fatalf("resolvePermission returned too quickly (%v); expected to wait ~%v for the claim timer", elapsed, claimTimeout)
	}
	// Upper bound: should not have blocked indefinitely.
	if elapsed > 5*time.Second {
		t.Fatalf("resolvePermission blocked for %v, want ~%v", elapsed, claimTimeout)
	}
	// No file-handler provided, so result should be the no-resolver sentinel.
	if res.source != "none" {
		t.Fatalf("result source = %q, want \"none\"", res.source)
	}
	if res.err != nil {
		t.Fatalf("unexpected error: %v", res.err)
	}
}

func TestWaitForSessionChunkedLoopMarker(t *testing.T) {
	tests := []struct {
		name      string
		chunks    []string
		wantDir   string
		wantLabel string
	}{
		{
			name:      "loop marker split across chunks",
			chunks:    []string{"[lo", "op: exit | tests green]"},
			wantDir:   "exit",
			wantLabel: "tests green",
		},
		{
			name:      "loop marker in single chunk",
			chunks:    []string{"[loop: continue | single]"},
			wantDir:   "continue",
			wantLabel: "single",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventCh := make(chan events.Event, len(tt.chunks)+2)
			for _, c := range tt.chunks {
				eventCh <- events.Event{
					Event:     "agent.message_chunk",
					SessionID: "ses_chunk",
					Fields: map[string]any{
						"content": map[string]any{"text": c},
					},
				}
			}
			eventCh <- events.Event{
				Event:     "session.end",
				SessionID: "ses_chunk",
				Fields:    map[string]any{"stop_reason": "end_turn"},
			}
			close(eventCh)

			promptDone := make(chan error, 1)
			promptDone <- nil

			dir := t.TempDir()
			eventsPath := filepath.Join(dir, "events.ndjson")
			writer, err := NewEventWriter(eventsPath)
			if err != nil {
				t.Fatalf("newEventWriter: %v", err)
			}

			provider := &cliFakeProvider{}
			var stderr strings.Builder
			result := waitForSessionForTest(
				context.Background(),
				provider,
				writer, nil, nil,
				eventCh, promptDone, nil,
				"ses_chunk", "run_chunk", "",
				true, DefaultPermissionClaimTimeout,
				nil, &stderr,
			)
			if closeErr := writer.Close(); closeErr != nil {
				t.Fatalf("close writer: %v", closeErr)
			}

			if result.LoopDirective != tt.wantDir {
				t.Errorf("LoopDirective = %q, want %q", result.LoopDirective, tt.wantDir)
			}
			if result.LoopLabel != tt.wantLabel {
				t.Errorf("LoopLabel = %q, want %q", result.LoopLabel, tt.wantLabel)
			}
		})
	}
}

func TestRunBackendOpenCodeACP(t *testing.T) {
	oldRunAttempt := runAttempt
	oldRetryAfter := retryAfter
	t.Cleanup(func() {
		runAttempt = oldRunAttempt
		retryAfter = oldRetryAfter
	})

	var gotBackend string
	runAttempt = func(
		ctx context.Context,
		cfg attemptConfig,
		deps attemptDeps,
	) attemptResult {
		gotBackend = cfg.backend
		return attemptResult{exitCode: 0}
	}
	retryAfter = func(time.Duration) <-chan time.Time { return make(chan time.Time) }

	dir := t.TempDir()
	promptPath := filepath.Join(dir, "prompt.txt")
	if err := os.WriteFile(promptPath, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write prompt: %v", err)
	}

	var stderr strings.Builder
	code := run([]string{
		"--prompt-file", promptPath,
		"--backend", "opencode-acp",
	}, func(string) string { return "" }, &stderr)
	if code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	if gotBackend != "opencode-acp" {
		t.Fatalf("runAttempt backend = %q, want %q", gotBackend, "opencode-acp")
	}
}

func TestRunDefaultBackendACP(t *testing.T) {
	oldRunAttempt := runAttempt
	oldRetryAfter := retryAfter
	t.Cleanup(func() {
		runAttempt = oldRunAttempt
		retryAfter = oldRetryAfter
	})

	var gotBackend string
	var gotServerURL string
	runAttempt = func(
		ctx context.Context,
		cfg attemptConfig,
		deps attemptDeps,
	) attemptResult {
		gotBackend = cfg.backend
		gotServerURL = cfg.startOptions.ServerURL
		return attemptResult{exitCode: 0}
	}
	retryAfter = func(time.Duration) <-chan time.Time { return make(chan time.Time) }

	var stderr strings.Builder
	code := run([]string{
		"--prompt", "hello",
	}, func(key string) string {
		if key == "AVENOR_OPENCODE_URL" {
			return "http://env.example"
		}
		return ""
	}, &stderr)
	if code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	if gotBackend != "opencode-acp" {
		t.Fatalf("runAttempt backend = %q, want %q", gotBackend, "opencode-acp")
	}
	// DiscoverServer always resolves AVENOR_OPENCODE_URL into ServerURL,
	// even for backends that don't use it (ACP rejects it in ensureClient).
	if gotServerURL == "" {
		t.Fatal("startOptions.ServerURL should be resolved from env")
	}
}

func TestRunBackendOpenCodeHTTPWithServerURL(t *testing.T) {
	oldRunAttempt := runAttempt
	oldRetryAfter := retryAfter
	t.Cleanup(func() {
		runAttempt = oldRunAttempt
		retryAfter = oldRetryAfter
	})

	var gotBackend string
	var gotServerURL string
	runAttempt = func(
		ctx context.Context,
		cfg attemptConfig,
		deps attemptDeps,
	) attemptResult {
		gotBackend = cfg.backend
		gotServerURL = cfg.startOptions.ServerURL
		return attemptResult{exitCode: 0}
	}
	retryAfter = func(time.Duration) <-chan time.Time { return make(chan time.Time) }

	dir := t.TempDir()
	promptPath := filepath.Join(dir, "prompt.txt")
	if err := os.WriteFile(promptPath, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write prompt: %v", err)
	}

	var stderr strings.Builder
	code := run([]string{
		"--prompt-file", promptPath,
		"--backend", "opencode-http",
		"--server-url", "http://localhost:8080",
	}, func(string) string { return "" }, &stderr)
	if code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	if gotBackend != "opencode-http" {
		t.Fatalf("runAttempt backend = %q, want %q", gotBackend, "opencode-http")
	}
	if gotServerURL != "http://localhost:8080" {
		t.Fatalf("startOptions.ServerURL = %q, want flag URL", gotServerURL)
	}
}

func TestRunBackendOpenCodeHTTPWithEnvServerURL(t *testing.T) {
	oldRunAttempt := runAttempt
	oldRetryAfter := retryAfter
	t.Cleanup(func() {
		runAttempt = oldRunAttempt
		retryAfter = oldRetryAfter
	})

	var gotBackend string
	var gotServerURL string
	runAttempt = func(
		ctx context.Context,
		cfg attemptConfig,
		deps attemptDeps,
	) attemptResult {
		gotBackend = cfg.backend
		gotServerURL = cfg.startOptions.ServerURL
		return attemptResult{exitCode: 0}
	}
	retryAfter = func(time.Duration) <-chan time.Time { return make(chan time.Time) }

	dir := t.TempDir()
	promptPath := filepath.Join(dir, "prompt.txt")
	if err := os.WriteFile(promptPath, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write prompt: %v", err)
	}

	env := func(key string) string {
		if key == "AVENOR_OPENCODE_URL" {
			return "http://env.example"
		}
		return ""
	}
	var stderr strings.Builder
	code := run([]string{
		"--prompt-file", promptPath,
		"--backend", "opencode-http",
	}, env, &stderr)
	if code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	if gotBackend != "opencode-http" {
		t.Fatalf("runAttempt backend = %q, want %q", gotBackend, "opencode-http")
	}
	if gotServerURL != "http://env.example" {
		t.Fatalf("startOptions.ServerURL = %q, want env URL", gotServerURL)
	}
}

func TestRunUnknownBackend(t *testing.T) {
	var stderr strings.Builder
	code := run([]string{"--backend", "bogus"}, nil, &stderr)
	if code != 1 {
		t.Fatalf("run() = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), `avenor: unknown backend "bogus"`) {
		t.Fatalf("stderr = %q, want unknown backend message", stderr.String())
	}
}

func TestRunOpenCodeHTTPWithoutServerURL(t *testing.T) {
	var stderr strings.Builder
	code := run([]string{"--backend", "opencode-http"}, nil, &stderr)
	if code != 1 {
		t.Fatalf("run() = %d, want 1", code)
	}
	if stderr.String() != "avenor: --server-url is required for backend opencode-http\n" {
		t.Fatalf("stderr = %q, want exact --server-url message", stderr.String())
	}
}

func TestRunDefaultBackendWithoutServerURL(t *testing.T) {
	oldRunAttempt := runAttempt
	oldRetryAfter := retryAfter
	t.Cleanup(func() {
		runAttempt = oldRunAttempt
		retryAfter = oldRetryAfter
	})

	var gotBackend string
	runAttempt = func(
		ctx context.Context,
		cfg attemptConfig,
		deps attemptDeps,
	) attemptResult {
		gotBackend = cfg.backend
		return attemptResult{exitCode: 0}
	}
	retryAfter = func(time.Duration) <-chan time.Time { return make(chan time.Time) }

	var stderr strings.Builder
	code := run([]string{"--prompt", "hello"}, func(string) string { return "" }, &stderr)
	if code != 0 {
		t.Fatalf("run() = %d, want 0 (ACP provider does not require --server-url); stderr=%s", code, stderr.String())
	}
	if gotBackend != "opencode-acp" {
		t.Fatalf("runAttempt backend = %q, want %q", gotBackend, "opencode-acp")
	}
}

func TestRunCodexAppServerBackend(t *testing.T) {
	oldRunAttempt := runAttempt
	oldRetryAfter := retryAfter
	t.Cleanup(func() {
		runAttempt = oldRunAttempt
		retryAfter = oldRetryAfter
	})

	var gotBackend string
	runAttempt = func(
		ctx context.Context,
		cfg attemptConfig,
		deps attemptDeps,
	) attemptResult {
		gotBackend = cfg.backend
		return attemptResult{exitCode: 0}
	}
	retryAfter = func(time.Duration) <-chan time.Time { return make(chan time.Time) }

	var stderr strings.Builder
	code := run([]string{"--backend", "codex-app-server", "--prompt", "hello"}, func(string) string { return "" }, &stderr)
	if code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	if gotBackend != "codex-app-server" {
		t.Fatalf("runAttempt backend = %q, want %q", gotBackend, "codex-app-server")
	}
}

func TestWaitForSessionChunkedStatusMarker(t *testing.T) {
	tests := []struct {
		name      string
		chunks    []string
		wantPhase string
		wantLabel string
		wantOK    bool
	}{
		{
			name:      "status marker split across chunks",
			chunks:    []string{"[sta", "tus: working | running tests]"},
			wantPhase: "working",
			wantLabel: "running tests",
			wantOK:    true,
		},
		{
			name:      "status marker in single chunk",
			chunks:    []string{"[status: thinking | analysing]"},
			wantPhase: "thinking",
			wantLabel: "analysing",
			wantOK:    true,
		},
		{
			name:      "angle status marker split across chunks",
			chunks:    []string{"<|sta", "tus: working | running tests|>"},
			wantPhase: "working",
			wantLabel: "running tests",
			wantOK:    true,
		},
		{
			name:      "angle status marker in single chunk",
			chunks:    []string{"<|status: thinking | analysing|>"},
			wantPhase: "thinking",
			wantLabel: "analysing",
			wantOK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			eventsPath := filepath.Join(dir, "events.ndjson")
			writer, err := NewEventWriter(eventsPath)
			if err != nil {
				t.Fatalf("newEventWriter: %v", err)
			}

			eventCh := make(chan events.Event, len(tt.chunks)+2)
			for _, c := range tt.chunks {
				eventCh <- events.Event{
					Event:     "agent.message_chunk",
					SessionID: "ses_chunk",
					Fields: map[string]any{
						"content": map[string]any{"text": c},
					},
				}
			}
			eventCh <- events.Event{
				Event:     "session.end",
				SessionID: "ses_chunk",
				Fields:    map[string]any{"stop_reason": "end_turn"},
			}
			close(eventCh)

			promptDone := make(chan error, 1)
			promptDone <- nil

			provider := &cliFakeProvider{}
			var stderr strings.Builder
			result := waitForSessionForTest(
				context.Background(), provider,
				writer, nil, nil,
				eventCh, promptDone, nil,
				"ses_chunk", "run_chunk", "",
				true, DefaultPermissionClaimTimeout,
				nil, &stderr,
			)
			if closeErr := writer.Close(); closeErr != nil {
				t.Fatalf("close writer: %v", closeErr)
			}
			if result.ExitCode != 0 {
				t.Fatalf("result.ExitCode = %d, want 0", result.ExitCode)
			}

			got := readEventLogForTest(t, eventsPath)
			var foundStatus bool
			for _, ev := range got {
				if ev.Event == "agent.status" {
					ph, _ := ev.Fields["phase"].(string)
					lbl, _ := ev.Fields["label"].(string)
					if ph == tt.wantPhase && lbl == tt.wantLabel {
						foundStatus = true
					}
				}
			}
			if !foundStatus {
				t.Errorf("status phase=%q label=%q not found in events: %+v", tt.wantPhase, tt.wantLabel, got)
			}
		})
	}
}

func TestWaitForSessionChunkedStatusMarkerDedup(t *testing.T) {
	// Verifies that sending the same status marker across multiple chunk
	// events results in only ONE agent.status emission.
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 5)
	eventCh <- events.Event{
		Event:     "agent.message_chunk",
		SessionID: "ses_dedup",
		Fields: map[string]any{
			"content": map[string]any{"text": "[sta"},
		},
	}
	eventCh <- events.Event{
		Event:     "agent.message_chunk",
		SessionID: "ses_dedup",
		Fields: map[string]any{
			"content": map[string]any{"text": "tus: working | first]"},
		},
	}
	// Append more text so status markers keep getting scanned, then
	// repeat the same marker text — chunkBuf dedup should suppress it.
	eventCh <- events.Event{
		Event:     "agent.message_chunk",
		SessionID: "ses_dedup",
		Fields: map[string]any{
			"content": map[string]any{"text": " more text [status: working | first] trailing"},
		},
	}
	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_dedup",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(eventCh)

	promptDone := make(chan error, 1)
	promptDone <- nil

	provider := &cliFakeProvider{}
	var stderr strings.Builder
	result := waitForSessionForTest(
		context.Background(), provider,
		writer, nil, nil,
		eventCh, promptDone, nil,
		"ses_dedup", "run_dedup", "",
		true, DefaultPermissionClaimTimeout,
		nil, &stderr,
	)
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatalf("close writer: %v", closeErr)
	}
	if result.ExitCode != 0 {
		t.Fatalf("result.ExitCode = %d, want 0", result.ExitCode)
	}

	got := readEventLogForTest(t, eventsPath)
	workingCount := 0
	for _, ev := range got {
		if ev.Event == "agent.status" {
			if ph, _ := ev.Fields["phase"].(string); ph == "working" {
				workingCount++
			}
		}
	}
	if workingCount != 1 {
		t.Fatalf("expected 1 agent.status working event, got %d: %+v", workingCount, got)
	}
}

// TestCancelAndEndIncludesBufferedUsage verifies that the timeout path includes
// usage accumulated from prior events in the synthetic session.end event.
func TestCancelAndEndIncludesBufferedUsage(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 2)
	eventCh <- events.Event{
		Event:     "agent.message_chunk",
		SessionID: "ses_usage",
		Fields: map[string]any{
			"usage": map[string]any{
				"input_tokens":  float64(5),
				"output_tokens": float64(2),
				"total_tokens":  float64(7),
			},
		},
	}

	timeoutCh := make(chan time.Time, 1)

	provider := &cliFakeProvider{}
	var wg sync.WaitGroup
	wg.Add(1)
	var result sessionResult
	go func() {
		defer wg.Done()
		result = waitForSessionForTest(context.Background(), provider, writer, nil, nil, eventCh, nil, nil, "ses_usage", "run_usage", "", true, DefaultPermissionClaimTimeout, timeoutCh, io.Discard)
	}()

	usageWritten := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(25 * time.Millisecond)
			data, _ := os.ReadFile(eventsPath)
			if strings.Contains(string(data), `"input_tokens":5`) {
				close(usageWritten)
				return
			}
		}
	}()
	select {
	case <-usageWritten:
	case <-time.After(5 * time.Second):
		t.Fatal("usage event not written within 5 seconds")
	}

	timeoutCh <- time.Now()

	wg.Wait()
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if result.ExitCode != 124 {
		t.Fatalf("WaitForSession().ExitCode = %d, want 124 (timeout)", result.ExitCode)
	}
	if result.StopReason != "timeout" {
		t.Fatalf("WaitForSession().StopReason = %q, want %q", result.StopReason, "timeout")
	}

	got := readEventLogForTest(t, eventsPath)
	last := got[len(got)-1]
	if last.Event != "session.end" {
		t.Fatalf("last event = %+v, want session.end", last)
	}
	if last.Fields["stop_reason"] != "timeout" {
		t.Fatalf("stop_reason = %v, want timeout", last.Fields["stop_reason"])
	}
	if result.Usage == nil {
		t.Fatalf("sessionResult.Usage is nil, buffered usage was not propagated")
	}
	if result.Usage["input_tokens"] != float64(5) {
		t.Errorf("Usage.input_tokens = %v, want 5", result.Usage["input_tokens"])
	}
	if result.Usage["output_tokens"] != float64(2) {
		t.Errorf("Usage.output_tokens = %v, want 2", result.Usage["output_tokens"])
	}
	if result.Usage["total_tokens"] != float64(7) {
		t.Errorf("Usage.total_tokens = %v, want 7", result.Usage["total_tokens"])
	}
	usage, ok := last.Fields["usage"].(map[string]any)
	if !ok {
		t.Fatalf("usage missing from synthetic session.end event")
	}
	if usage["input_tokens"] != float64(5) {
		t.Errorf("event usage.input_tokens = %v, want 5", usage["input_tokens"])
	}
	if usage["output_tokens"] != float64(2) {
		t.Errorf("event usage.output_tokens = %v, want 2", usage["output_tokens"])
	}
	if usage["total_tokens"] != float64(7) {
		t.Errorf("event usage.total_tokens = %v, want 7", usage["total_tokens"])
	}
}

// TestCancelAndEndNilUsage verifies that a timeout before any usage-bearing
// event emits session.end without a usage key.
func TestCancelAndEndNilUsage(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("newEventWriter: %v", err)
	}

	eventCh := make(chan events.Event, 1)
	timeoutCh := make(chan time.Time, 1)

	provider := &cliFakeProvider{}
	var wg sync.WaitGroup
	wg.Add(1)
	var result sessionResult
	go func() {
		defer wg.Done()
		result = waitForSessionForTest(context.Background(), provider, writer, nil, nil, eventCh, nil, nil, "ses_nousage", "run_nousage", "", true, DefaultPermissionClaimTimeout, timeoutCh, io.Discard)
	}()

	// No usage-bearing event — fire timeout immediately.
	timeoutCh <- time.Now()

	wg.Wait()
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	if result.ExitCode != 124 {
		t.Fatalf("WaitForSession().ExitCode = %d, want 124 (timeout)", result.ExitCode)
	}
	if result.Usage != nil {
		t.Fatalf("sessionResult.Usage should be nil when no usage-bearing event fired, got %+v", result.Usage)
	}

	got := readEventLogForTest(t, eventsPath)
	last := got[len(got)-1]
	if last.Event != "session.end" {
		t.Fatalf("last event = %+v, want session.end", last)
	}
	if _, exists := last.Fields["usage"]; exists {
		t.Fatal("session.end should not contain usage field when none was buffered")
	}
}

func TestWaitForSessionAccumulatesAgentMessageOutput(t *testing.T) {
	eventCh := make(chan events.Event, 3)
	eventCh <- events.Event{
		Event:     "agent.message_chunk",
		SessionID: "ses_output",
		Fields: map[string]any{
			"content": map[string]any{"text": "[team: skip | security]"},
		},
	}
	eventCh <- events.Event{
		Event:     "agent.thought_chunk",
		SessionID: "ses_output",
		Fields: map[string]any{
			"content": map[string]any{"text": "internal reasoning"},
		},
	}
	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_output",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(eventCh)

	promptDone := make(chan error, 1)
	promptDone <- nil

	writer, err := NewEventWriter("")
	if err != nil {
		t.Fatalf("NewEventWriter(\"\") error = %v", err)
	}
	defer writer.Close()

	result := waitForSessionForTest(context.Background(), &cliFakeProvider{}, writer, nil, nil, eventCh, promptDone, nil, "ses_output", "run_output", "", true, DefaultPermissionClaimTimeout, nil, io.Discard)
	if result.Output != "[team: skip | security]" {
		t.Fatalf("Output = %q, want assistant output only", result.Output)
	}
}

func TestWaitForSessionAccumulatesDeltaAgentMessageOutput(t *testing.T) {
	eventCh := make(chan events.Event, 3)
	eventCh <- events.Event{
		Event:     "agent.message_chunk",
		SessionID: "ses_output",
		Fields: map[string]any{
			"delta": "[team: skip | security-reviewer]",
		},
	}
	eventCh <- events.Event{
		Event:     "agent.thought_chunk",
		SessionID: "ses_output",
		Fields: map[string]any{
			"delta": "internal reasoning",
		},
	}
	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "ses_output",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(eventCh)

	promptDone := make(chan error, 1)
	promptDone <- nil

	writer, err := NewEventWriter("")
	if err != nil {
		t.Fatalf("NewEventWriter(\"\") error = %v", err)
	}
	defer writer.Close()

	result := waitForSessionForTest(context.Background(), &cliFakeProvider{}, writer, nil, nil, eventCh, promptDone, nil, "ses_output", "run_output", "", true, DefaultPermissionClaimTimeout, nil, io.Discard)
	if result.Output != "[team: skip | security-reviewer]" {
		t.Fatalf("Output = %q, want delta assistant output only", result.Output)
	}
}

func TestWaitForSessionFinalReply(t *testing.T) {
	runFinalReplyTest := func(t *testing.T, evts []events.Event, wantOutput, wantFinalReply string) {
		t.Helper()
		eventCh := make(chan events.Event, len(evts)+1)
		for _, ev := range evts {
			eventCh <- ev
		}
		eventCh <- events.Event{
			Event:     "session.end",
			SessionID: "ses_output",
			Fields:    map[string]any{"stop_reason": "end_turn"},
		}
		close(eventCh)

		promptDone := make(chan error, 1)
		promptDone <- nil

		writer, err := NewEventWriter("")
		if err != nil {
			t.Fatalf("NewEventWriter(\"\") error = %v", err)
		}
		defer writer.Close()

		result := waitForSessionForTest(context.Background(), &cliFakeProvider{}, writer, nil, nil, eventCh, promptDone, nil, "ses_output", "run_output", "", true, DefaultPermissionClaimTimeout, nil, io.Discard)
		if result.Output != wantOutput {
			t.Fatalf("Output = %q, want %q", result.Output, wantOutput)
		}
		if result.FinalReply != wantFinalReply {
			t.Fatalf("FinalReply = %q, want %q", result.FinalReply, wantFinalReply)
		}
	}

	msg := func(delta string) events.Event {
		return events.Event{Event: "agent.message_chunk", SessionID: "ses_output", Fields: map[string]any{"delta": delta}}
	}
	toolCall := events.Event{Event: "tool.call", SessionID: "ses_output", Fields: map[string]any{"kind": "shell", "title": "shell"}}

	t.Run("single turn with tool call then final text", func(t *testing.T) {
		runFinalReplyTest(t, []events.Event{
			msg("Let me check."),
			toolCall,
			msg("Here are findings:"),
			msg(" No issues."),
		}, "Let me check.Here are findings: No issues.", "Here are findings: No issues.")
	})

	t.Run("zero tool calls — FinalReply equals full Output", func(t *testing.T) {
		runFinalReplyTest(t, []events.Event{
			msg("Summary only."),
		}, "Summary only.", "Summary only.")
	})

	t.Run("multiple tool calls — only last block captured", func(t *testing.T) {
		runFinalReplyTest(t, []events.Event{
			msg("Checking files."),
			toolCall,
			msg("Checking tests."),
			toolCall,
			msg("Final answer."),
		}, "Checking files.Checking tests.Final answer.", "Final answer.")
	})

	t.Run("tool call at end with no subsequent text — FinalReply empty", func(t *testing.T) {
		runFinalReplyTest(t, []events.Event{
			msg("Initial thought."),
			toolCall,
		}, "Initial thought.", "")
	})
}

func runSessionEventLogTest(t *testing.T, run func(EventSink) sessionResult) (sessionResult, []events.Event) {
	t.Helper()
	eventsPath := filepath.Join(t.TempDir(), "events.ndjson")
	writer, err := NewEventWriter(eventsPath)
	if err != nil {
		t.Fatalf("NewEventWriter: %v", err)
	}
	result := run(writer)
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return result, readEventLogForTest(t, eventsPath)
}

func TestWaitForSessionSessionEndIncludesFinalOutput(t *testing.T) {
	msg := func(delta string) events.Event {
		return events.Event{Event: "agent.message_chunk", SessionID: "ses_output", Fields: map[string]any{"delta": delta}}
	}
	thought := events.Event{Event: "agent.thought_chunk", SessionID: "ses_output", Fields: map[string]any{"delta": "private reasoning"}}
	toolCall := events.Event{Event: "tool.call", SessionID: "ses_output", Fields: map[string]any{"kind": "shell", "title": "shell"}}

	tests := []struct {
		name            string
		events          []events.Event
		wantOutput      string
		wantFinalReply  string
		wantFinalOutput string
	}{
		{
			name: "complete message stream across tool calls",
			events: []events.Event{
				msg("Before α. "),
				toolCall,
				msg("During tools. "),
				thought,
				toolCall,
				msg("Final answer."),
			},
			wantOutput:      "Before α. During tools. Final answer.",
			wantFinalReply:  "Final answer.",
			wantFinalOutput: "Before α. During tools. Final answer.",
		},
		{
			name: "trailing tool call still synthesizes full output",
			events: []events.Event{
				msg("Before the tool."),
				toolCall,
			},
			wantOutput:      "Before the tool.",
			wantFinalReply:  "",
			wantFinalOutput: "Before the tool.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, logged := runSessionEventLogTest(t, func(writer EventSink) sessionResult {
				eventCh := make(chan events.Event, len(tt.events)+1)
				for _, event := range tt.events {
					eventCh <- event
				}
				eventCh <- events.Event{Event: "session.end", SessionID: "ses_output", Fields: map[string]any{"stop_reason": "end_turn"}}
				close(eventCh)
				promptDone := make(chan error, 1)
				promptDone <- nil

				return waitForSessionForTest(context.Background(), &cliFakeProvider{}, writer, nil, nil, eventCh, promptDone, nil, "ses_output", "run_output", "", true, DefaultPermissionClaimTimeout, nil, io.Discard)
			})
			if result.Output != tt.wantOutput {
				t.Fatalf("Output = %q, want %q", result.Output, tt.wantOutput)
			}
			if result.FinalReply != tt.wantFinalReply {
				t.Fatalf("FinalReply = %q, want %q", result.FinalReply, tt.wantFinalReply)
			}

			last := logged[len(logged)-1]
			got, ok := last.Fields["final_output"].(string)
			if !ok || got != tt.wantFinalOutput {
				t.Fatalf("final_output = %q (present=%t), want %q", got, ok, tt.wantFinalOutput)
			}
		})
	}

	t.Run("no message chunks", func(t *testing.T) {
		result, logged := runSessionEventLogTest(t, func(writer EventSink) sessionResult {
			eventCh := make(chan events.Event, 1)
			eventCh <- events.Event{Event: "session.end", SessionID: "ses_output_empty", Fields: map[string]any{"stop_reason": "end_turn"}}
			close(eventCh)
			promptDone := make(chan error, 1)
			promptDone <- nil

			return waitForSessionForTest(context.Background(), &cliFakeProvider{}, writer, nil, nil, eventCh, promptDone, nil, "ses_output_empty", "run_output_empty", "", true, DefaultPermissionClaimTimeout, nil, io.Discard)
		})
		if result.Output != "" {
			t.Fatalf("Output = %q, want empty", result.Output)
		}
		if result.FinalReply != "" {
			t.Fatalf("FinalReply = %q, want empty", result.FinalReply)
		}

		last := logged[len(logged)-1]
		if last.Event != "session.end" {
			t.Fatalf("last event = %q, want session.end", last.Event)
		}
		if _, ok := last.Fields["final_output"]; ok {
			t.Fatal("final_output is present, want it absent")
		}
	})
}

func TestWaitForSessionSessionEndPreservesBackendFinalOutput(t *testing.T) {
	for _, tt := range []struct {
		name           string
		value          any
		wantOutput     string
		wantFinalReply string
	}{
		{name: "non-empty", value: "backend final", wantOutput: "local message", wantFinalReply: "local message"},
		{name: "explicit empty", value: "", wantOutput: "local message", wantFinalReply: "local message"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, logged := runSessionEventLogTest(t, func(writer EventSink) sessionResult {
				eventCh := make(chan events.Event, 2)
				eventCh <- events.Event{Event: "agent.message_chunk", SessionID: "ses_backend_final", Fields: map[string]any{"delta": "local message"}}
				eventCh <- events.Event{Event: "session.end", SessionID: "ses_backend_final", Fields: map[string]any{
					"stop_reason":  "end_turn",
					"final_output": tt.value,
				}}
				close(eventCh)
				promptDone := make(chan error, 1)
				promptDone <- nil

				return waitForSessionForTest(context.Background(), &cliFakeProvider{}, writer, nil, nil, eventCh, promptDone, nil, "ses_backend_final", "run_backend_final", "", true, DefaultPermissionClaimTimeout, nil, io.Discard)
			})
			if result.Output != tt.wantOutput {
				t.Fatalf("Output = %q, want %q", result.Output, tt.wantOutput)
			}
			if result.FinalReply != tt.wantFinalReply {
				t.Fatalf("FinalReply = %q, want %q", result.FinalReply, tt.wantFinalReply)
			}

			last := logged[len(logged)-1]
			got, ok := last.Fields["final_output"]
			if !ok || got != tt.value {
				t.Fatalf("final_output = %#v (present=%t), want %#v", got, ok, tt.value)
			}
		})
	}
}

func TestWaitForSessionCancelAndEndUsesFullReply(t *testing.T) {
	msg := func(delta string) events.Event {
		return events.Event{Event: "agent.message_chunk", SessionID: "ses_cancel", Fields: map[string]any{"delta": delta}}
	}
	toolCall := events.Event{Event: "tool.call", SessionID: "ses_cancel", Fields: map[string]any{"kind": "shell", "title": "shell"}}

	tests := []struct {
		name            string
		stopReason      string
		wantOutput      string
		wantFinalReply  string
		wantFinalOutput string
		cancel          func(context.CancelFunc, chan<- time.Time)
	}{
		{
			name:            "context cancellation",
			stopReason:      "cancelled",
			wantOutput:      "Before the tool. After the tool.",
			wantFinalReply:  "After the tool.",
			wantFinalOutput: "Before the tool. After the tool.",
			cancel: func(cancel context.CancelFunc, _ chan<- time.Time) {
				cancel()
			},
		},
		{
			name:            "explicit timeout",
			stopReason:      "timeout",
			wantOutput:      "Before the tool. After the tool.",
			wantFinalReply:  "After the tool.",
			wantFinalOutput: "Before the tool. After the tool.",
			cancel: func(_ context.CancelFunc, timeout chan<- time.Time) {
				timeout <- time.Now()
			},
		},
		{
			name:            "trailing tool call",
			stopReason:      "cancelled",
			wantOutput:      "Before the tool. ",
			wantFinalReply:  "",
			wantFinalOutput: "Before the tool. ",
			cancel: func(cancel context.CancelFunc, _ chan<- time.Time) {
				cancel()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, logged := runSessionEventLogTest(t, func(writer EventSink) sessionResult {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				timeoutCh := make(chan time.Time, 1)
				eventCh := make(chan events.Event)
				eventsToSend := []events.Event{msg("Before the tool. "), toolCall}
				if tt.wantFinalReply != "" {
					eventsToSend = append(eventsToSend, msg("After the tool."))
				}
				sent := make(chan struct{})
				go func() {
					for _, event := range eventsToSend {
						eventCh <- event
					}
					close(sent)
				}()

				resultCh := make(chan sessionResult, 1)
				go func() {
					resultCh <- WaitForSession(ctx, &cliFakeProvider{}, SessionWaitConfig{
						EventCh:   eventCh,
						SessionID: "ses_cancel",
						RunID:     "run_cancel",
						Timeout:   timeoutCh,
					}, SessionWaitDeps{Writer: writer, Stderr: io.Discard})
				}()

				select {
				case <-sent:
				case <-time.After(2 * time.Second):
					t.Fatal("message events were not consumed")
				}
				tt.cancel(cancel, timeoutCh)

				select {
				case result := <-resultCh:
					return result
				case <-time.After(2 * time.Second):
					t.Fatal("WaitForSession did not return")
					return sessionResult{}
				}
			})
			if result.StopReason != tt.stopReason {
				t.Fatalf("StopReason = %q, want %q", result.StopReason, tt.stopReason)
			}
			if result.FinalReply != tt.wantFinalReply {
				t.Fatalf("FinalReply = %q, want %q", result.FinalReply, tt.wantFinalReply)
			}
			if result.Output != tt.wantOutput {
				t.Fatalf("Output = %q, want %q", result.Output, tt.wantOutput)
			}

			last := logged[len(logged)-1]
			if last.Event != "session.end" {
				t.Fatalf("last event = %q, want session.end", last.Event)
			}
			got, ok := last.Fields["final_output"].(string)
			if !ok || got != tt.wantFinalOutput {
				t.Fatalf("final_output = %q (present=%t), want %q", got, ok, tt.wantFinalOutput)
			}
		})
	}

	t.Run("progress timeout uses complete output and last block reply", func(t *testing.T) {
		const fullOutput = "Before the tool. After the tool."
		const lastBlockReply = "After the tool."

		result, logged := runSessionEventLogTest(t, func(writer EventSink) sessionResult {
			progressTimerC := make(chan time.Time, 1)
			messageWritten := make(chan struct{})
			eventCh := make(chan events.Event)
			signalWriter := &signalEventSink{
				EventSink: writer,
				eventName: "agent.message_chunk",
				field:     "delta",
				value:     lastBlockReply,
				signal:    messageWritten,
			}
			resultCh := make(chan sessionResult, 1)
			go func() {
				resultCh <- WaitForSession(context.Background(), &cliFakeProvider{}, SessionWaitConfig{
					EventCh:   eventCh,
					SessionID: "ses_cancel",
					RunID:     "run_cancel",
				}, SessionWaitDeps{
					Writer:         signalWriter,
					Stderr:         io.Discard,
					ProgressTimerC: progressTimerC,
				})
			}()

			eventCh <- msg("Before the tool. ")
			eventCh <- toolCall
			eventCh <- msg(lastBlockReply)
			<-messageWritten
			progressTimerC <- time.Time{}
			return <-resultCh
		})
		if result.StopReason != "progress_timeout" {
			t.Fatalf("StopReason = %q, want progress_timeout", result.StopReason)
		}
		if result.FinalReply != lastBlockReply {
			t.Fatalf("FinalReply = %q, want %q", result.FinalReply, lastBlockReply)
		}
		if result.Output != fullOutput {
			t.Fatalf("Output = %q, want %q", result.Output, fullOutput)
		}

		last := logged[len(logged)-1]
		if last.Event != "session.end" {
			t.Fatalf("last event = %q, want session.end", last.Event)
		}
		if last.Fields["stop_reason"] != "progress_timeout" {
			t.Fatalf("stop_reason = %v, want progress_timeout", last.Fields["stop_reason"])
		}
		got, ok := last.Fields["final_output"].(string)
		if !ok || got != fullOutput {
			t.Fatalf("final_output = %q (present=%t), want %q", got, ok, fullOutput)
		}
	})
}

func TestWaitForSessionAdoptsExternalConversationID(t *testing.T) {
	eventCh := make(chan events.Event, 2)
	eventCh <- events.Event{
		Event:     "session.start",
		SessionID: "external-conversation",
		Fields: map[string]any{
			"conversation_id": "external-conversation",
		},
	}
	eventCh <- events.Event{
		Event:     "session.end",
		SessionID: "external-conversation",
		Fields: map[string]any{
			"stop_reason": "end_turn",
		},
	}
	promptDone := make(chan error, 1)
	promptDone <- nil

	adopted := ""
	result := WaitForSession(context.Background(), &cliFakeProvider{}, SessionWaitConfig{
		EventCh:    eventCh,
		PromptDone: promptDone,
		SessionID:  "agy-pending-local",
		AdoptSessionID: func(sessionID string) {
			adopted = sessionID
		},
	}, SessionWaitDeps{
		Writer: &permissionLifecycleSink{responses: make(chan string, 1)},
		Stderr: io.Discard,
	})

	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	if adopted != "external-conversation" {
		t.Fatalf("adopted session ID = %q", adopted)
	}
}

func TestWaitForSessionFailsWhenAuthoritativeConversationIDIsRejected(t *testing.T) {
	eventCh := make(chan events.Event, 2)
	eventCh <- events.Event{Event: "session.start", SessionID: "shared", Fields: map[string]any{"conversation_id": "shared"}}
	// A later successful terminal event must not turn the rejected collision into
	// a successful attempt.
	eventCh <- events.Event{Event: "session.end", SessionID: "shared", Fields: map[string]any{"stop_reason": "end_turn"}}
	promptDone := make(chan error, 1)
	promptDone <- nil

	adopted := false
	result := WaitForSession(context.Background(), &cliFakeProvider{}, SessionWaitConfig{
		EventCh: eventCh, PromptDone: promptDone, SessionID: "pending",
		AcceptSessionID: func(string) bool { return false },
		AdoptSessionID:  func(string) { adopted = true },
	}, SessionWaitDeps{Writer: &permissionLifecycleSink{responses: make(chan string, 1)}, Stderr: io.Discard})

	if result.ExitCode != 1 || result.StopReason != "session_id_conflict" {
		t.Fatalf("result = %+v, want fatal session identity conflict", result)
	}
	if adopted {
		t.Fatal("rejected authoritative ID reached adoption callback")
	}
}
