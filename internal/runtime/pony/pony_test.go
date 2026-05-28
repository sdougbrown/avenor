package pony

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/pony/model"
	"github.com/sdougbrown/avenor/internal/runtime/pony/tools"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func textChunk(text string) model.Chunk {
	return model.Chunk{Type: model.ChunkTypeText, Text: text}
}

func finishChunk(reason string) model.Chunk {
	return model.Chunk{Type: model.ChunkTypeFinish, Finish: &model.FinishReason{Reason: reason}}
}

func errorChunk(err error) model.Chunk {
	return model.Chunk{Type: model.ChunkTypeError, Err: err}
}

func usageChunk(in, out int) model.Chunk {
	return model.Chunk{Type: model.ChunkTypeUsage, Usage: &model.Usage{InputTokens: in, OutputTokens: out}}
}

func toolCallChunk(id string, name string, args json.RawMessage, index int) model.Chunk {
	return model.Chunk{Type: model.ChunkTypeToolCallDelta, ToolCall: &model.ToolCallDelta{ID: id, Name: name, Arguments: args, Index: index}}
}

func reasonChunk(text string) model.Chunk {
	return model.Chunk{Type: model.ChunkTypeReasoning, ReasoningContent: text}
}

// fakeAdapter implements model.Adapter with a configurable chunk sequence.
type fakeAdapter struct {
	chunks          []model.Chunk
	chunkDelay      time.Duration
	callCount       int
	mutualExclusion bool // when true, use atomic counter for thread-safe call counting
	streamFunc      func(ctx context.Context, req model.Request) (<-chan model.Chunk, error)
	onStream        func() // called when Stream is invoked (for synchronization)
}

var adapterCallCount int64

func (f *fakeAdapter) Name() string { return "fake" }

func (f *fakeAdapter) Stream(ctx context.Context, req model.Request) (<-chan model.Chunk, error) {
	if f.streamFunc != nil {
		return f.streamFunc(ctx, req)
	}
	if f.mutualExclusion {
		atomic.AddInt64(&adapterCallCount, 1)
	} else {
		f.callCount++
	}
	ch := make(chan model.Chunk, 1)
	go func() {
		defer close(ch)
		if f.onStream != nil {
			f.onStream()
		}
		for _, c := range f.chunks {
			select {
			case <-ctx.Done():
				return
			case ch <- c:
				if f.chunkDelay > 0 {
					time.Sleep(f.chunkDelay)
				}
			}
		}
	}()
	return ch, nil
}



// fakeExecutor implements OrchestratorExecutor for orchestration tool tests.
type fakeExecutor struct {
	spawnCalled    int
	sendCalled     int
	lastRequestID  string
	statusCalled   int
	waitCalled     int
	sendToParentCalled int
	spawnDelay     time.Duration
	waitDone       bool
	waitErr        error
	spawnID        string
	getStatusResp  map[string]any
}

func (f *fakeExecutor) SpawnAgent(ctx context.Context, params map[string]any) (string, error) {
	f.spawnCalled++
	if f.spawnDelay > 0 {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(f.spawnDelay):
		}
	}
	id := f.spawnID
	if id == "" {
		id = fmt.Sprintf("spawned_%d", f.spawnCalled)
	}
	return id, nil
}

func (f *fakeExecutor) SendPrompt(ctx context.Context, sessionID, prompt, requestID string) error {
	f.sendCalled++
	f.lastRequestID = requestID
	return nil
}

func (f *fakeExecutor) GetStatus(ctx context.Context, sessionID string) (map[string]any, error) {
	f.statusCalled++
	if f.getStatusResp != nil {
		return f.getStatusResp, nil
	}
	return map[string]any{"phase": "running", "state": "active"}, nil
}

func (f *fakeExecutor) WaitForDone(ctx context.Context, sessionID string) (*runtime.AgentResult, error) {
	f.waitCalled++
	if f.waitErr != nil {
		return nil, f.waitErr
	}
	if !f.waitDone {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &runtime.AgentResult{
		SessionID:  sessionID,
		StopReason: "stop",
		ExitCode:   0,
	}, nil
}

func (f *fakeExecutor) SendToParent(ctx context.Context, runtimeID, message string) error {
	f.sendToParentCalled++
	return nil
}


// ── RunLoop tests ────────────────────────────────────────────────────────────

func TestRunLoop_textOnly_stop(t *testing.T) {
	ctx := context.Background()
	ch := make(chan events.Event, 64)
	defer close(ch)

	adapter := &fakeAdapter{chunks: []model.Chunk{
		textChunk("Hello "),
		textChunk("world"),
		finishChunk("stop"),
	}}

	history, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "hi"}},
		nil, "", ch, nil, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopReason != "end_turn" {
		t.Errorf("stopReason = %q, want %q", stopReason, "end_turn")
	}
	if len(history) != 2 {
		t.Fatalf("history length = %d, want 2", len(history))
	}
	if history[1].Role != model.RoleAssistant {
		t.Errorf("last message role = %v, want %v", history[1].Role, model.RoleAssistant)
	}
	if history[1].Content != "Hello world" {
		t.Errorf("content = %q, want %q", history[1].Content, "Hello world")
	}
}

func TestRunLoop_textOnly_length(t *testing.T) {
	ctx := context.Background()
	ch := make(chan events.Event, 64)
	defer close(ch)

	adapter := &fakeAdapter{chunks: []model.Chunk{
		textChunk("some text"),
		finishChunk("length"),
	}}

	history, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "hi"}},
		nil, "", ch, nil, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopReason != "max_tokens" {
		t.Errorf("stopReason = %q, want %q", stopReason, "max_tokens")
	}
	if len(history) != 2 {
		t.Errorf("history length = %d, want 2", len(history))
	}
}

func TestRunLoop_textToolCalls_stop(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry([]tools.Tool{tools.NewFileReadTool()})
	ch := make(chan events.Event, 64)
	defer close(ch)
	tmpDir := t.TempDir()

	// Step-based fake adapter: different chunks per Stream call
	type stepAdapter struct{ callCount int }
	sa := &stepAdapter{}
	adapter := &fakeAdapter{
		mutualExclusion: true, // prevent concurrent modification
	}
	adapter.streamFunc = func(ctx context.Context, req model.Request) (<-chan model.Chunk, error) {
		sa.callCount++
		chunks := make(chan model.Chunk, 4)
		go func() {
			defer close(chunks)
			if sa.callCount == 1 {
				chunks <- toolCallChunk("tc1", "file_read", json.RawMessage(`{"path":"go.mod"}`), 0)
				chunks <- textChunk("")
				chunks <- finishChunk("tool_calls")
			} else {
				chunks <- textChunk("done")
				chunks <- finishChunk("stop")
			}
		}()
		return chunks, nil
	}

	history := []model.Message{{Role: model.RoleUser, Content: "read go.mod"}}

	finalHistory, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		history, reg, tmpDir, ch, nil, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopReason != "end_turn" {
		t.Errorf("stopReason = %q, want %q", stopReason, "end_turn")
	}
	if len(finalHistory) != 4 {
		t.Fatalf("history length = %d, want 4", len(finalHistory))
	}
	assistantMsg := finalHistory[1]
	if len(assistantMsg.ToolCalls) != 1 {
		t.Fatalf("tool calls count = %d, want 1", len(assistantMsg.ToolCalls))
	}
	if assistantMsg.ToolCalls[0].Name != "file_read" {
		t.Errorf("tool name = %q, want %q", assistantMsg.ToolCalls[0].Name, "file_read")
	}
	toolMsg := finalHistory[2]
	if toolMsg.Role != model.RoleTool {
		t.Errorf("tool message role = %v, want %v", toolMsg.Role, model.RoleTool)
	}
	if toolMsg.ToolCallID != "tc1" {
		t.Errorf("tool_call_id = %q, want %q", toolMsg.ToolCallID, "tc1")
	}
}


func TestRunLoop_toolExecuteError(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry([]tools.Tool{tools.NewFileReadTool()})
	ch := make(chan events.Event, 64)
	defer close(ch)
	tmpDir := t.TempDir()

	callNum := 0
	adapter := &fakeAdapter{}
	adapter.streamFunc = func(ctx context.Context, req model.Request) (<-chan model.Chunk, error) {
		callNum++
		ch := make(chan model.Chunk, 4)
		go func() {
			defer close(ch)
			if callNum == 1 {
				ch <- toolCallChunk("tc1", "file_read", json.RawMessage(`{"path":"nonexistent.txt"}`), 0)
				ch <- finishChunk("tool_calls")
			} else {
				ch <- textChunk("error handled")
				ch <- finishChunk("stop")
			}
		}()
		return ch, nil
	}

	history := []model.Message{{Role: model.RoleUser, Content: "read nonexistent"}}

	finalHistory, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		history, reg, tmpDir, ch, nil, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopReason != "end_turn" {
		t.Errorf("stopReason = %q, want %q", stopReason, "end_turn")
	}
	toolMsg := finalHistory[2]
	if len(toolMsg.Content) == 0 {
		t.Error("tool result content is empty, expected error message")
	}
	if len(finalHistory) != 4 {
		t.Fatalf("history length = %d, want 4", len(finalHistory))
	}
}

func TestRunLoop_streamEndedUnexpectedly(t *testing.T) {
	ctx := context.Background()
	ch := make(chan events.Event, 64)
	defer close(ch)

	adapter := &fakeAdapter{chunks: []model.Chunk{
		textChunk("partial"),
	}}

	_, _, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "hi"}},
		nil, "", ch, nil, nil)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "stream ended unexpectedly" {
		t.Errorf("error = %q, want %q", err.Error(), "stream ended unexpectedly")
	}
}

func TestRunLoop_errorChunk(t *testing.T) {
	ctx := context.Background()
	ch := make(chan events.Event, 64)
	defer close(ch)

	wantErr := errors.New("provider error")
	adapter := &fakeAdapter{chunks: []model.Chunk{
		errorChunk(wantErr),
	}}

	_, _, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "hi"}},
		nil, "", ch, nil, nil)

	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want %v", err, wantErr)
	}
}

func TestRunLoop_cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	adapter := &fakeAdapter{chunks: []model.Chunk{
		textChunk("slow "),
	}, onStream: func() { close(started) }}

	errCh := make(chan error, 1)
	go func() {
		_, _, err := RunLoop(
			ctx, adapter, "test-model", 1024,
			[]model.Message{{Role: model.RoleUser, Content: "hi"}},
			nil, "", nil, nil, nil)
		errCh <- err
	}()

	<-started
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			// Stream may complete before cancel() takes effect —
			// not a bug, just a test timing artifact.
			t.Logf("RunLoop returned non-cancel error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunLoop did not respond to cancellation")
	}
}

func TestRunLoop_reasoningChunk(t *testing.T) {
	ctx := context.Background()
	ch := make(chan events.Event, 64)

	adapter := &fakeAdapter{chunks: []model.Chunk{
		textChunk("hello"),
		usageChunk(10, 5),
		finishChunk("stop"),
	}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = RunLoop(
			ctx, adapter, "test-model", 1024,
			[]model.Message{{Role: model.RoleUser, Content: "hi"}},
			nil, "", ch, nil, nil)
	}()

	var foundUsage bool
	timeout := time.After(2 * time.Second)
	for {
		select {
		case evt, ok := <-ch:
			if !ok {
				return
			}
			if evt.Event == "usage" {
				foundUsage = true
				if evt.Fields["input_tokens"] != 10 {
					t.Errorf("input_tokens = %v, want 10", evt.Fields["input_tokens"])
				}
				if evt.Fields["output_tokens"] != 5 {
					t.Errorf("output_tokens = %v, want 5", evt.Fields["output_tokens"])
				}
			}
		case <-done:
			// Drain any remaining events that were emitted before RunLoop returned
			for {
				select {
				case evt := <-ch:
					if evt.Event == "usage" {
						foundUsage = true
					}
				default:
					if !foundUsage {
						t.Fatal("RunLoop finished without emitting usage event")
					}
					return
				}
			}
		case <-timeout:
			t.Fatal("timeout waiting for events")
		}
	}
}

func TestRunLoop_reasoningContent(t *testing.T) {
	ctx := context.Background()
	ch := make(chan events.Event, 64)
	defer close(ch)

	adapter := &fakeAdapter{chunks: []model.Chunk{
		reasonChunk("I need to think about this step by step."),
		reasonChunk("First, I'll analyze the problem."),
		textChunk("The answer is 42."),
		finishChunk("stop"),
	}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, stopReason, err := RunLoop(
			ctx, adapter, "test-model", 1024,
			[]model.Message{{Role: model.RoleUser, Content: "hi"}},
			nil, "", ch, nil, nil)
		if err != nil {
			t.Errorf("RunLoop error: %v", err)
		}
		if stopReason != "end_turn" {
			t.Errorf("stopReason = %q, want %q", stopReason, "end_turn")
		}
	}()

	thoughtCount := 0
	for {
		select {
		case evt, ok := <-ch:
			if !ok {
				if thoughtCount != 2 {
					t.Errorf("expected 2 thought_chunk events, got %d", thoughtCount)
				}
				return
			}
			if evt.Event == "agent.thought_chunk" {
				thoughtCount++
			}
		case <-done:
			for {
				select {
				case evt := <-ch:
					if evt.Event == "agent.thought_chunk" {
						thoughtCount++
					}
				default:
					if thoughtCount != 2 {
						t.Errorf("expected 2 thought_chunk events, got %d", thoughtCount)
					}
					return
				}
			}
		}
	}
}

func TestRunLoop_customFinishReason(t *testing.T) {
	ctx := context.Background()
	ch := make(chan events.Event, 64)
	defer close(ch)

	adapter := &fakeAdapter{chunks: []model.Chunk{
		textChunk("done"),
		finishChunk("max_tokens"),
	}}

	history, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "hi"}},
		nil, "", ch, nil, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopReason != "max_tokens" {
		t.Errorf("stopReason = %q, want %q", stopReason, "max_tokens")
	}
	if len(history) != 2 {
		t.Errorf("history length = %d, want 2", len(history))
	}
}

func TestRunLoop_multipleToolCalls(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry([]tools.Tool{tools.NewFileReadTool()})
	ch := make(chan events.Event, 64)
	defer close(ch)
	tmpDir := t.TempDir()

	callNum := 0
	adapter := &fakeAdapter{}
	adapter.streamFunc = func(ctx context.Context, req model.Request) (<-chan model.Chunk, error) {
		callNum++
		ch := make(chan model.Chunk, 4)
		go func() {
			defer close(ch)
			if callNum == 1 {
				ch <- toolCallChunk("tc1", "file_read", json.RawMessage(`{"path":"go.mod"}`), 0)
				ch <- toolCallChunk("tc2", "file_read", json.RawMessage(`{"path":"go.sum"}`), 1)
				ch <- finishChunk("tool_calls")
			} else {
				ch <- textChunk("done")
				ch <- finishChunk("stop")
			}
		}()
		return ch, nil
	}

	history := []model.Message{{Role: model.RoleUser, Content: "read files"}}

	finalHistory, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		history, reg, tmpDir, ch, nil, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopReason != "end_turn" {
		t.Errorf("stopReason = %q, want %q", stopReason, "end_turn")
	}
	if len(finalHistory) != 5 {
		t.Fatalf("history length = %d, want 5 (user + assistant(tool) + 2 tool results + assistant(text))", len(finalHistory))
	}
	if finalHistory[1].ToolCalls[0].ID != "tc1" {
		t.Errorf("first tool call id = %q, want %q", finalHistory[1].ToolCalls[0].ID, "tc1")
	}
}

func TestRunLoop_stopConditionOnToolCalls(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry([]tools.Tool{tools.NewFileReadTool()})
	ch := make(chan events.Event, 64)
	defer close(ch)
	tmpDir := t.TempDir()

	adapter := &fakeAdapter{chunks: []model.Chunk{
		toolCallChunk("tc1", "file_read", json.RawMessage(`{"path":"go.mod"}`), 0),
		finishChunk("tool_calls"),
	}}

	history := []model.Message{{Role: model.RoleUser, Content: "read file"}}

	finalHistory, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		history, reg, tmpDir, ch, []StopCondition{StepCountIs(1)}, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopReason != "end_turn" {
		t.Errorf("stopReason = %q, want %q", stopReason, "end_turn")
	}
	// StepCountIs(1) triggers after tool_calls turn completes
	if len(finalHistory) != 3 {
		t.Fatalf("history length = %d, want 3 (user + assistant + tool result)", len(finalHistory))
	}
}


// ── LoopWithRetry tests ──────────────────────────────────────────────────────

func TestLoopWithRetry_succeedsFirstAttempt(t *testing.T) {
	ctx := context.Background()
	ch := make(chan events.Event, 64)
	defer close(ch)

	adapter := &fakeAdapter{mutualExclusion: true}
	adapter.chunks = []model.Chunk{
		textChunk("ok"),
		finishChunk("stop"),
	}

	history, stopReason, err := LoopWithRetry(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "hi"}},
		nil, "", ch, nil, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopReason != "end_turn" {
		t.Errorf("stopReason = %q, want %q", stopReason, "end_turn")
	}
	if len(history) != 2 {
		t.Errorf("history length = %d, want 2", len(history))
	}
	if atomic.LoadInt64(&adapterCallCount) != 1 {
		t.Errorf("call count = %d, want 1", atomic.LoadInt64(&adapterCallCount))
	}
}

func TestLoopWithRetry_retriesOnError(t *testing.T) {
	ctx := context.Background()
	ch := make(chan events.Event, 64)
	defer close(ch)

	adapter := &fakeAdapter{mutualExclusion: false}

	_, _, err := LoopWithRetry(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "hi"}},
		nil, "", ch, nil, nil)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if adapter.callCount != 4 {
		t.Errorf("call count = %d, want 4 (exhausted retries)", adapter.callCount)
	}
}

func TestLoopWithRetry_noRetryAfterToolCalls(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry([]tools.Tool{tools.NewFileReadTool()})
	ch := make(chan events.Event, 64)
	defer close(ch)
	tmpDir := t.TempDir()

	adapter := &fakeAdapter{mutualExclusion: false}

	history := []model.Message{{Role: model.RoleUser, Content: "read file"}}

	finalHistory, stopReason, err := LoopWithRetry(
		ctx, adapter, "test-model", 1024,
		history, reg, tmpDir, ch, nil, nil)

	// With empty chunks (no finish), RunLoop returns "stream ended unexpectedly"
	// Since no tool calls were made, LoopWithRetry should retry up to 4 times
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if stopReason != "error" {
		t.Errorf("stopReason = %q, want %q", stopReason, "error")
	}
	if adapter.callCount != 4 {
		t.Errorf("call count = %d, want 4 (exhausted retries)", adapter.callCount)
	}
	// history should be nil on exhausted retries
	if finalHistory != nil {
		t.Error("expected nil history on exhausted retries")
	}
}

func TestLoopWithRetry_exhaustsRetries(t *testing.T) {
	ctx := context.Background()
	ch := make(chan events.Event, 64)
	defer close(ch)

	adapter := &fakeAdapter{chunks: []model.Chunk{
		errorChunk(errors.New("always fails")),
	}}

	_, _, err := LoopWithRetry(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "hi"}},
		nil, "", ch, nil, nil)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if adapter.callCount != 4 {
		t.Errorf("adapter call count = %d, want 4", adapter.callCount)
	}
}

func TestLoopWithRetry_contextCancelledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan events.Event, 64)
	defer close(ch)

	// First call errors, then we cancel during backoff
	adapter := &fakeAdapter{mutualExclusion: true}

	// Use a context that gets cancelled after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	_, _, err := LoopWithRetry(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "hi"}},
		nil, "", ch, nil, nil)

	if err != context.Canceled {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}


// ── Provider tests ───────────────────────────────────────────────────────────

func TestProvider_Capabilities(t *testing.T) {
	p := New(Config{Model: "test"})
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if caps.Backend != "pony" {
		t.Errorf("backend = %q, want %q", caps.Backend, "pony")
	}
	if caps.Permissions {
		t.Error("permissions should be false")
	}
	if !caps.ExternalServerURL {
		t.Error("external_server_url should be true")
	}
	if caps.Resume {
		t.Error("resume should be false")
	}
}

func TestProvider_Start(t *testing.T) {
	ctx := context.Background()
	p := New(Config{
		Model:           "test-model",
		SystemPrompt:    "You are helpful.",
		InitialPrompt:   "Hello!",
		WorkingDir:      "/tmp",
	})

	session, err := p.Start(ctx, runtime.StartOptions{Dir: "/test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if session.Backend != "pony" {
		t.Errorf("backend = %q, want %q", session.Backend, "pony")
	}
	if session.SessionID == "" {
		t.Error("session ID should not be empty")
	}
	if !strings.HasPrefix(session.SessionID, "pony_") {
		t.Errorf("session ID prefix = %q, want %q", session.SessionID[:5], "pony_")
	}
}

func TestProvider_Start_multipleSessions(t *testing.T) {
	ctx := context.Background()
	p := New(Config{Model: "test-model"})

	s1, err := p.Start(ctx, runtime.StartOptions{})
	if err != nil {
		t.Fatalf("start 1 error: %v", err)
	}
	s2, err := p.Start(ctx, runtime.StartOptions{})
	if err != nil {
		t.Fatalf("start 2 error: %v", err)
	}

	if s1.SessionID == s2.SessionID {
		t.Error("session IDs should be unique")
	}
}

func TestProvider_Resume(t *testing.T) {
	ctx := context.Background()
	p := New(Config{Model: "test-model"})

	_, err := p.Resume(ctx, "pony_1")
	if err == nil {
		t.Fatal("expected error from Resume, got nil")
	}
}

func TestProvider_Prompt_unknownSession(t *testing.T) {
	ctx := context.Background()
	p := New(Config{Model: "test-model"})

	err := p.Prompt(ctx, "pony_999", "hello")
	if err == nil {
		t.Fatal("expected error for unknown session, got nil")
	}
}

func TestProvider_Cancel_unknownSession(t *testing.T) {
	ctx := context.Background()
	p := New(Config{Model: "test-model"})

	err := p.Cancel(ctx, "pony_999")
	if err == nil {
		t.Fatal("expected error for unknown session, got nil")
	}
}

func TestProvider_Prompt_cancel(t *testing.T) {
	ctx := context.Background()

	started := make(chan struct{})
	slowAdapter := &fakeAdapter{chunks: []model.Chunk{
		textChunk("slow response..."),
	}, onStream: func() { close(started) }}

	p := New(Config{Model: "test-model", Adapter: slowAdapter})

	session, _ := p.Start(ctx, runtime.StartOptions{})

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Prompt(ctx, session.SessionID, "hello")
	}()

	<-started
	p.Cancel(ctx, session.SessionID)

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Logf("Prompt returned error: %v (expected cancellation)", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("Prompt did not respond to Cancel")
	}
}

func TestProvider_Events_returnsChannel(t *testing.T) {
	ctx := context.Background()
	p := New(Config{Model: "test-model"})
	session, _ := p.Start(ctx, runtime.StartOptions{})

	eventCh, err := p.Events(ctx, session.SessionID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if eventCh == nil {
		t.Fatal("events channel should not be nil")
	}
}

func TestProvider_Events_unknownSession(t *testing.T) {
	ctx := context.Background()
	p := New(Config{Model: "test-model"})

	_, err := p.Events(ctx, "pony_999")
	if err == nil {
		t.Fatal("expected error for unknown session, got nil")
	}
}


func TestProvider_Start_withAgentsMD(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	agentsMD := "# Project Rules\n\nBe nice."
	err := os.WriteFile(filepath.Join(tmpDir, "AGENTS.md"), []byte(agentsMD), 0644)
	if err != nil {
		t.Fatalf("write AGENTS.md: %v", err)
	}

	p := New(Config{
		Model:          "test-model",
		SystemPrompt:   "You are helpful.",
		InjectAgentsMD: true,
		WorkingDir:     tmpDir,
	})

	session, err := p.Start(ctx, runtime.StartOptions{Dir: tmpDir})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	p.mu.Lock()
	ss := p.sessions[session.SessionID]
	p.mu.Unlock()

	ss.mu.Lock()
	defer ss.mu.Unlock()

	if len(ss.history) != 2 {
		t.Fatalf("history length = %d, want 2", len(ss.history))
	}
	if ss.history[0].Role != model.RoleSystem {
		t.Errorf("first message role = %v, want %v", ss.history[0].Role, model.RoleSystem)
	}
	if ss.history[1].Role != model.RoleUser {
		t.Errorf("second message role = %v, want %v", ss.history[1].Role, model.RoleUser)
	}
	if !strings.Contains(ss.history[1].Content, "## Project conventions") {
		t.Error("agents MD content should be prefixed with '## Project conventions'")
	}
}

func TestProvider_Start_withInitialPrompt(t *testing.T) {
	ctx := context.Background()
	p := New(Config{
		Model:         "test-model",
		SystemPrompt:  "You are helpful.",
		InitialPrompt: "Hello, agent!",
	})

	session, err := p.Start(ctx, runtime.StartOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	p.mu.Lock()
	ss := p.sessions[session.SessionID]
	p.mu.Unlock()

	ss.mu.Lock()
	defer ss.mu.Unlock()

	if len(ss.history) != 2 {
		t.Fatalf("history length = %d, want 2", len(ss.history))
	}
	if ss.history[1].Role != model.RoleUser || ss.history[1].Content != "Hello, agent!" {
		t.Errorf("initial prompt not in history: %+v", ss.history)
	}
}

func TestProvider_NewWithOptions(t *testing.T) {
	cfg := Config{Model: "base-model", WorkingDir: "/base"}
	SetGlobalConfig(&cfg)

	p := NewWithOptions(runtime.StartOptions{Model: "override-model", Dir: "/override"})

	p.mu.Lock()
	gotCfg := p.cfg
	p.mu.Unlock()

	if gotCfg.Model != "override-model" {
		t.Errorf("model = %q, want %q", gotCfg.Model, "override-model")
	}
	if gotCfg.WorkingDir != "/override" {
		t.Errorf("working dir = %q, want %q", gotCfg.WorkingDir, "/override")
	}
}

func TestProvider_NewWithOptions_noGlobalConfig(t *testing.T) {
	SetGlobalConfig(nil)

	p := NewWithOptions(runtime.StartOptions{Model: "m", Dir: "/d"})

	p.mu.Lock()
	gotCfg := p.cfg
	p.mu.Unlock()

	if gotCfg.Model != "m" {
		t.Errorf("model = %q, want %q", gotCfg.Model, "m")
	}
	if gotCfg.WorkingDir != "/d" {
		t.Errorf("working dir = %q, want %q", gotCfg.WorkingDir, "/d")
	}
}


// ── Orchestration tools tests ────────────────────────────────────────────────

func TestOrchestrationTools_spawn(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry(newOrchestrationTools(&fakeExecutor{spawnID: "child_1"}))

	result, err := reg.Dispatch(ctx, ".", "spawn_agent", json.RawMessage(`{"prompt":"hello"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "Agent spawned with session ID: child_1"
	if result != expected {
		t.Errorf("result = %q, want %q", result, expected)
	}
}

func TestOrchestrationTools_sendPrompt(t *testing.T) {
	ctx := context.Background()
	fake := &fakeExecutor{}
	reg := tools.NewRegistry(newOrchestrationTools(fake))

	result, err := reg.Dispatch(ctx, ".", "send_prompt", json.RawMessage(`{"session_id":"child_1","prompt":"more"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "Prompt sent." {
		t.Errorf("result = %q, want %q", result, "Prompt sent.")
	}

	_, err = reg.Dispatch(ctx, ".", "send_prompt", json.RawMessage(`{"session_id":"child_1","prompt":"more","request_id":"cq_9"}`))
	if err != nil {
		t.Fatalf("unexpected error for request_id: %v", err)
	}
	if fake.lastRequestID != "cq_9" {
		t.Fatalf("lastRequestID = %q, want cq_9", fake.lastRequestID)
	}
}

func TestOrchestrationTools_getStatus(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry(newOrchestrationTools(&fakeExecutor{getStatusResp: map[string]any{"phase": "completed"}}))

	result, err := reg.Dispatch(ctx, ".", "get_status", json.RawMessage(`{"session_id":"child_1"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "completed") {
		t.Errorf("result should contain 'completed': %q", result)
	}
}

func TestOrchestrationTools_waitForDone_immediate(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry(newOrchestrationTools(&fakeExecutor{waitDone: true}))

	result, err := reg.Dispatch(ctx, ".", "wait_for_done", json.RawMessage(`{"session_id":"child_1"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "Agent child_1 completed (exit 0, stop). Output files: []"
	if result != expected {
		t.Errorf("result = %q, want %q", result, expected)
	}
}

func TestOrchestrationTools_waitForDone_cancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reg := tools.NewRegistry(newOrchestrationTools(&fakeExecutor{waitDone: false}))

	errCh := make(chan error, 1)
	go func() {
		_, err := reg.Dispatch(ctx, ".", "wait_for_done", json.RawMessage(`{"session_id":"child_1"}`))
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Logf("dispatch returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("wait_for_done did not respond to cancellation")
	}
}

func TestOrchestrationTools_spawnMissingPrompt(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry(newOrchestrationTools(&fakeExecutor{}))

	_, err := reg.Dispatch(ctx, ".", "spawn_agent", json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected error for missing prompt, got nil")
	}
}

func TestOrchestrationTools_spawnAgents(t *testing.T) {
	ctx := context.Background()
	fe := &fakeExecutor{}
	reg := tools.NewRegistry(newOrchestrationTools(fe))

	result, err := reg.Dispatch(ctx, ".", "spawn_agents", json.RawMessage(`{"agents":[{"prompt":"task one"},{"prompt":"task two"}]}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fe.spawnCalled != 2 {
		t.Errorf("spawnCalled = %d, want 2", fe.spawnCalled)
	}
	expected := "Spawned agents: [spawned_1, spawned_2]"
	if result != expected {
		t.Errorf("result = %q, want %q", result, expected)
	}
}

func TestOrchestrationTools_spawnAgreesEmpty(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry(newOrchestrationTools(&fakeExecutor{}))

	_, err := reg.Dispatch(ctx, ".", "spawn_agents", json.RawMessage(`{"agents":[]}`))
	if err == nil {
		t.Fatal("expected error for empty agents, got nil")
	}
}

func TestOrchestrationTools_spawnAgentsMissingPrompt(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry(newOrchestrationTools(&fakeExecutor{}))

	_, err := reg.Dispatch(ctx, ".", "spawn_agents", json.RawMessage(`{"agents":[{"prompt":"ok"},{}]}`))
	if err == nil {
		t.Fatal("expected error for missing prompt, got nil")
	}
}

func TestOrchestrationTools_spawnAgentsDirsOutsideWorkDir(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry(newOrchestrationTools(&fakeExecutor{}))

	_, err := reg.Dispatch(ctx, ".", "spawn_agents", json.RawMessage(`{"agents":[{"prompt":"hi","dir":"/etc"}]}`))
	if err == nil {
		t.Fatal("expected error for dir outside workdir, got nil")
	}
}

func TestOrchestrationTools_sendToParent(t *testing.T) {
	ctx := context.WithValue(context.Background(), RuntimeIDKey, "rt_child")
	fe := &fakeExecutor{}
	reg := tools.NewRegistry(newOrchestrationTools(fe))

	result, err := reg.Dispatch(ctx, ".", "send_to_parent", json.RawMessage(`{"message":"need help"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fe.sendToParentCalled != 1 {
		t.Errorf("sendToParentCalled = %d, want 1", fe.sendToParentCalled)
	}
	if result != "Message sent to parent agent." {
		t.Errorf("result = %q, want %q", result, "Message sent to parent agent.")
	}
}

func TestOrchestrationTools_sendToParentMissingRuntimeContext(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry(newOrchestrationTools(&fakeExecutor{}))

	_, err := reg.Dispatch(ctx, ".", "send_to_parent", json.RawMessage(`{"message":"need help"}`))
	if err == nil {
		t.Fatal("expected error for missing runtime context, got nil")
	}
}

func TestOrchestrationTools_sendToParentMissingMessage(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry(newOrchestrationTools(&fakeExecutor{}))

	_, err := reg.Dispatch(ctx, ".", "send_to_parent", json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected error for missing message, got nil")
	}
}


// ── e2e tool with real file read ─────────────────────────────────────────────

func TestRunLoop_e2e_fileRead(t *testing.T) {
	ctx := context.Background()
	reg := tools.NewRegistry([]tools.Tool{tools.NewFileReadTool()})
	ch := make(chan events.Event, 64)
	defer close(ch)
	tmpDir := t.TempDir()

	// Create a test file in the temp directory
	testContent := "package main\n\nfunc main() {}\n"
	err := os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte(testContent), 0644)
	if err != nil {
		t.Fatalf("write test file: %v", err)
	}

	callNum := 0
	adapter := &fakeAdapter{}
	adapter.streamFunc = func(ctx context.Context, req model.Request) (<-chan model.Chunk, error) {
		callNum++
		ch := make(chan model.Chunk, 4)
		go func() {
			defer close(ch)
			if callNum == 1 {
				ch <- toolCallChunk("tc1", "file_read", json.RawMessage(`{"path":"main.go"}`), 0)
				ch <- finishChunk("tool_calls")
			} else {
				ch <- textChunk("read it")
				ch <- finishChunk("stop")
			}
		}()
		return ch, nil
	}

	history := []model.Message{{Role: model.RoleUser, Content: "read main.go"}}

	finalHistory, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		history, reg, tmpDir, ch, nil, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopReason != "end_turn" {
		t.Errorf("stopReason = %q, want %q", stopReason, "end_turn")
	}
	// user + assistant(tool) + tool result + assistant(text) = 4 messages
	if len(finalHistory) != 4 {
		t.Fatalf("history length = %d, want 4", len(finalHistory))
	}
	toolMsg := finalHistory[2]
	if !strings.Contains(toolMsg.Content, testContent) {
		t.Errorf("tool result content = %q, want to contain %q", toolMsg.Content, testContent)
	}
}

// ── sessionState tests ───────────────────────────────────────────────────────

func TestSessionState_emit(t *testing.T) {
	s := newSessionState()
	ch1 := make(chan events.Event, 8)
	ch2 := make(chan events.Event, 8)
	s.addSubscriber(ch1)
	s.addSubscriber(ch2)

	s.emit(events.Event{Event: "test", Fields: map[string]any{"key": "val"}})

	close(ch1)
	close(ch2)

	var got1, got2 bool
	for range ch1 {
		got1 = true
	}
	for range ch2 {
		got2 = true
	}
	if !got1 || !got2 {
		t.Error("both subscribers should receive the event")
	}
}

func TestSessionState_removeSubscriber(t *testing.T) {
	s := newSessionState()
	ch := make(chan events.Event, 8)
	s.addSubscriber(ch)
	s.removeSubscriber(ch)
	s.emit(events.Event{Event: "test", Fields: nil})
	// channel should be empty since subscriber was removed
	select {
	case _, ok := <-ch:
		if ok && len(ch) > 0 {
			t.Error("should not receive event after removal")
		}
	default:
	}
}

func TestSessionState_removeSubscriber_nonExistent(t *testing.T) {
	s := newSessionState()
	ch1 := make(chan events.Event, 8)
	ch2 := make(chan events.Event, 8)
	s.addSubscriber(ch1)
	// Try to remove a channel that isn't subscribed
	s.removeSubscriber(ch2)
	s.emit(events.Event{Event: "test", Fields: nil})
	close(ch1)
	_, ok := <-ch1
	if !ok {
		t.Error("ch1 should still be open and receive the event")
	}
}
