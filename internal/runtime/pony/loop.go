package pony

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/pony/model"
	"github.com/sdougbrown/avenor/internal/runtime/pony/tools"
)

// StopCondition is a function that returns true when the loop should stop.
// Called after each turn (including tool execution).
type StopCondition func(state LoopRunState) bool

// StepCountIs returns a stop condition that triggers after n steps (loop turns).
func StepCountIs(n int) StopCondition {
	return func(state LoopRunState) bool {
		return state.StepCount >= n
	}
}

// LoopRunState tracks the state across loop iterations for stop conditions.
type LoopRunState struct {
	StepCount   int
	CalledTools []string // tool names called so far
}

// ApprovalChecker is called before a tool executes. It blocks until the
// permission is granted or denied. Returns true if approved, false if denied.
type ApprovalChecker func(ctx context.Context, toolName string, eventCh chan<- events.Event) (bool, error)

// defaultCompactionThreshold is the fallback soft limit for conversation history
// size before request-level compaction kicks in. At 80 KB and ~4 chars/token,
// this is ~20K tokens. Per-turn compaction (compactOldToolResults) handles the
// steady state; this safety net catches unusually large single turns that slip
// through. Override per-model via SetCompactionThreshold from the profile Context.
const defaultCompactionThreshold = 80 << 10 // 80 KB

// compactionThreshold is the active limit for compactForRequest. Uses atomic
// for safe concurrent access between config-setup and loop execution.
var compactionThreshold atomic.Int64

func init() {
	compactionThreshold.Store(defaultCompactionThreshold)
}

// SetCompactionThreshold overrides the byte threshold for compactForRequest.
// Values <= 0 reset to the default (80 KB). Called from New() which derives
// the byte value from the profile's Context field (tokens × bytesPerToken).
func SetCompactionThreshold(bytes int) {
	if bytes <= 0 {
		compactionThreshold.Store(defaultCompactionThreshold)
		return
	}
	compactionThreshold.Store(int64(bytes))
}

// oldResultMinBytes is the minimum tool result size to compact per-turn.
// Results below this (grep/glob/small shell outputs) are preserved indefinitely
// since they're cheap context the model may reference across turns. Large
// results (file reads of patches/source) are the ones that cause overflow.
const oldResultMinBytes = 2 << 10 // 2 KB

// compactOldToolResults compacts large tool result messages from turns the model
// has already consumed. After the assistant produces a new response, tool results
// from prior turns that exceed oldResultMinBytes are summarized in-place.
// Current-turn results (after the most recent assistant message) and small
// results are left intact.
//
// Returns the number of results compacted and total bytes removed.
func compactOldToolResults(history []model.Message) (compacted int, bytesSaved int) {
	lastAssistant := -1
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == model.RoleAssistant {
			lastAssistant = i
			break
		}
	}
	if lastAssistant < 0 {
		return 0, 0
	}
	for i := 0; i < lastAssistant; i++ {
		if history[i].Role == model.RoleTool && len(history[i].Content) > oldResultMinBytes {
			oldLen := len(history[i].Content)
			history[i].Content = fmt.Sprintf("[old tool result: was %d bytes]", oldLen)
			compacted++
			bytesSaved += oldLen
		}
	}
	return compacted, bytesSaved
}

// compactOldAndEmit calls compactOldToolResults and emits an avenor.compaction
// event if any results were compacted.
func compactOldAndEmit(history []model.Message, eventCh chan<- events.Event) {
	compacted, saved := compactOldToolResults(history)
	if compacted > 0 {
		emit(eventCh, "avenor.compaction", map[string]any{
			"kind":        "per_turn",
			"compacted":   compacted,
			"bytes_saved": saved,
		})
	}
}

// compactedHistory holds a compacted copy of history and stats about what was removed.
type compactedHistory struct {
	messages   []model.Message
	compacted  int // number of tool results compacted
	bytesSaved int // total bytes removed from those results
}

// compactForRequest creates a copy of history suitable for the model request.
// If total byte size exceeds the threshold, it compacts the oldest tool results
// to bring it under the limit. Returns a copy (never mutates the original) and
// compaction stats. This is a safety net for unusually large single turns;
// steady-state compaction is handled by compactOldToolResults.
func compactForRequest(history []model.Message, thresholdBytes int) compactedHistory {
	if thresholdBytes <= 0 {
		return compactedHistory{messages: history}
	}
	total := 0
	for _, msg := range history {
		total += len(msg.Content)
	}
	if total <= thresholdBytes {
		return compactedHistory{messages: history}
	}

	// Protect the most recent messages up to half the threshold.
	// Tool results before that window get compacted.
	protected := thresholdBytes / 2
	remaining := protected
	cutoff := len(history)
	for i := len(history) - 1; i >= 0 && remaining > 0; i-- {
		remaining -= len(history[i].Content)
		cutoff = i
	}

	messages := make([]model.Message, len(history))
	var compacted int
	var bytesSaved int
	for i, msg := range history {
		if i < cutoff && msg.Role == model.RoleTool && len(msg.Content) > 0 {
			messages[i] = model.Message{
				Role:       msg.Role,
				ToolCallID: msg.ToolCallID,
				Content:    fmt.Sprintf("[tool result too large: was %d bytes]", len(msg.Content)),
			}
			compacted++
			bytesSaved += len(msg.Content)
		} else {
			messages[i] = msg
		}
	}
	return compactedHistory{messages: messages, compacted: compacted, bytesSaved: bytesSaved}
}

// RunLoop drives the multi-turn loop until a stop condition or terminal condition.
//
// It sends the history + tool defs to the adapter each turn, processes streamed
// chunks, accumulates tool calls, dispatches them through the registry, and
// appends results to history.
//
// If approval is non-nil, it is called before each tool to check for permission.
// Events are emitted on eventCh as the loop runs for subscribers.
//
// Returns the final conversation history (including all assistant and tool turns)
// and the final stop reason.
func RunLoop(
	ctx context.Context,
	adapter model.Adapter,
	modelName string,
	maxTokens int,
	history []model.Message,
	reg *tools.Registry,
	workingDir string,
	eventCh chan<- events.Event,
	stopConditions []StopCondition,
	approval ApprovalChecker,
) ([]model.Message, string, error) {

	toolDefs := buildToolDefs(reg)

turnLoop:
	for turn := 0; ; turn++ {
		// Copy with threshold safety net for unusually large single turns.
		ch := compactForRequest(history, int(compactionThreshold.Load()))
		if ch.compacted > 0 {
			emit(eventCh, "avenor.compaction", map[string]any{
				"kind":        "threshold",
				"compacted":   ch.compacted,
				"bytes_saved": ch.bytesSaved,
			})
		}

		req := model.Request{
			Model:     modelName,
			Messages:  ch.messages,
			Tools:     toolDefs,
			Stream:    true,
			MaxTokens: maxTokens,
		}

		chunkCh, err := adapter.Stream(ctx, req)
		if err != nil {
			return history, "error", err
		}

		var assistantContent strings.Builder
		accumulators := make(map[int]*accumulatedToolCall)

		for chunk := range chunkCh {
			select {
			case <-ctx.Done():
				msg := buildAssistantMsg(assistantContent.String(), nil)
				history = append(history, msg)
				compactOldAndEmit(history, eventCh)
				return history, "error", ctx.Err()
			default:
			}

			switch chunk.Type {
			case model.ChunkTypeText:
				assistantContent.WriteString(chunk.Text)
				emit(eventCh, "agent.message_chunk", map[string]any{"delta": chunk.Text})

			case model.ChunkTypeReasoning:
				emit(eventCh, "agent.thought_chunk", map[string]any{"delta": chunk.ReasoningContent})

			case model.ChunkTypeToolCallDelta:
				tc := chunk.ToolCall
				if tc == nil {
					continue
				}
				acc, ok := accumulators[tc.Index]
				if !ok {
					acc = &accumulatedToolCall{index: tc.Index}
					accumulators[tc.Index] = acc
				}
				if tc.ID != "" {
					acc.id = tc.ID
				}
				if tc.Name != "" {
					acc.name = tc.Name
				}
				if len(tc.Arguments) > 0 {
					acc.arguments.WriteString(string(tc.Arguments))
				}

			case model.ChunkTypeFinish:
				finish := chunk.Finish
				switch finish.Reason {
				case "stop":
					msg := buildAssistantMsg(assistantContent.String(), accumulators)
					history = append(history, msg)
					compactOldAndEmit(history, eventCh)
					emit(eventCh, "session.end", map[string]any{"stop_reason": "end_turn"})
					return history, "end_turn", nil

				case "tool_calls":
					msg := buildAssistantMsg(assistantContent.String(), accumulators)
					history = append(history, msg)
					compactOldAndEmit(history, eventCh)

					// Sort by index for deterministic execution order
					indices := make([]int, 0, len(accumulators))
					for idx := range accumulators {
						indices = append(indices, idx)
					}
					sort.Ints(indices)

					for _, idx := range indices {
						acc := accumulators[idx]
						result, err := executeToolCall(ctx, reg, workingDir, acc, eventCh, approval)
						toolResult := model.Message{
							Role:       model.RoleTool,
							ToolCallID: acc.id,
							Content:    result,
						}
						if err != nil {
							toolResult.Content = fmt.Sprintf("Error: %v", err)
						}
						history = append(history, toolResult)
					}

					state := LoopRunState{
						StepCount:   turn + 1,
						CalledTools: collectToolNames(accumulators),
					}
					if shouldStop(state, stopConditions) {
						emit(eventCh, "session.end", map[string]any{"stop_reason": "end_turn"})
						return history, "end_turn", nil
					}

					continue turnLoop

				case "length":
					msg := buildAssistantMsg(assistantContent.String(), nil)
					history = append(history, msg)
					compactOldAndEmit(history, eventCh)
					emit(eventCh, "session.end", map[string]any{"stop_reason": "max_tokens"})
					return history, "max_tokens", nil

				default:
					msg := buildAssistantMsg(assistantContent.String(), nil)
					history = append(history, msg)
					compactOldAndEmit(history, eventCh)
					emit(eventCh, "session.end", map[string]any{"stop_reason": finish.Reason})
					return history, finish.Reason, nil
				}

			case model.ChunkTypeUsage:
				if chunk.Usage != nil {
					emit(eventCh, "usage", map[string]any{
						"input_tokens":  chunk.Usage.InputTokens,
						"output_tokens": chunk.Usage.OutputTokens,
					})
				}

			case model.ChunkTypeError:
				return history, "error", chunk.Err
			}
		}

		// Stream ended without a finish reason — preserve partial content
		if assistantContent.Len() > 0 || len(accumulators) > 0 {
			msg := buildAssistantMsg(assistantContent.String(), accumulators)
			history = append(history, msg)
			compactOldAndEmit(history, eventCh)
		}
		return history, "error", fmt.Errorf("stream ended unexpectedly")
	}
}

type accumulatedToolCall struct {
	index     int
	id        string
	name      string
	arguments strings.Builder
}

func buildAssistantMsg(text string, accs map[int]*accumulatedToolCall) model.Message {
	msg := model.Message{Role: model.RoleAssistant, Content: text}
	if len(accs) > 0 {
		indices := make([]int, 0, len(accs))
		for idx := range accs {
			indices = append(indices, idx)
		}
		sort.Ints(indices)

		for _, idx := range indices {
			acc := accs[idx]
			toolCall := model.ToolCall{
				ID:        acc.id,
				Name:      acc.name,
				Arguments: json.RawMessage(acc.arguments.String()),
			}
			msg.ToolCalls = append(msg.ToolCalls, toolCall)
		}
	}
	return msg
}

func executeToolCall(ctx context.Context, reg *tools.Registry, workingDir string, acc *accumulatedToolCall, eventCh chan<- events.Event, approval ApprovalChecker) (string, error) {
	emit(eventCh, "tool.call", map[string]any{
		"kind":   acc.name,
		"id":     acc.id,
		"title":  acc.name,
		"status": "called",
	})

	// Check approval before dispatching
	if approval != nil {
		approved, err := approval(ctx, acc.name, eventCh)
		if err != nil {
			emit(eventCh, "tool.result", map[string]any{
				"kind":     acc.name,
				"content":  fmt.Sprintf("Error: %v", err),
				"is_error": true,
			})
			return "", err
		}
		if !approved {
			emit(eventCh, "tool.result", map[string]any{
				"kind":     acc.name,
				"content":  fmt.Sprintf("Error: tool %q was denied", acc.name),
				"is_error": true,
			})
			return "", fmt.Errorf("tool %q was denied", acc.name)
		}
	}

	args := json.RawMessage(acc.arguments.String())

	// Emit structured finding events for report_finding tool
	if acc.name == tools.ReportFindingToolName {
		if findingFields, err := tools.FindingResult(args); err == nil {
			findingFields["tool_call_id"] = acc.id
			emit(eventCh, "finding.report", findingFields)
		} else {
			emit(eventCh, "finding.report", map[string]any{
				"tool_call_id": acc.id,
				"is_error":     true,
				"message":      fmt.Sprintf("parse finding result: %v", err),
			})
		}
	}

	result, err := reg.Dispatch(ctx, workingDir, acc.name, args)
	if err != nil {
		emit(eventCh, "tool.result", map[string]any{
			"kind":     acc.name,
			"content":  fmt.Sprintf("Error: %v", err),
			"is_error": true,
		})
		return "", err
	}

	emit(eventCh, "tool.result", map[string]any{
		"kind":    acc.name,
		"content": result,
	})
	return result, nil
}

func shouldStop(state LoopRunState, conditions []StopCondition) bool {
	for _, cond := range conditions {
		if cond(state) {
			return true
		}
	}
	return false
}

func collectToolNames(accs map[int]*accumulatedToolCall) []string {
	names := make([]string, 0, len(accs))
	seen := make(map[string]bool, len(accs))
	for _, acc := range accs {
		if !seen[acc.name] {
			names = append(names, acc.name)
			seen[acc.name] = true
		}
	}
	return names
}

func emit(ch chan<- events.Event, eventType string, fields map[string]any) {
	if ch == nil {
		return
	}
	evt := events.Event{Event: eventType, Fields: fields}
	select {
	case ch <- evt:
	default:
	}
}

func buildToolDefs(reg *tools.Registry) []model.ToolDef {
	if reg == nil {
		return nil
	}
	tools := reg.AllTools()
	defs := make([]model.ToolDef, 0, len(tools))
	for _, t := range tools {
		defs = append(defs, model.ToolDef{
			Name:        t.Name(),
			Description: t.Description(),
			InputSchema: t.Schema(),
		})
	}
	return defs
}

// LoopWithRetry wraps RunLoop with exponential backoff retry.
// Retries on error only if no tool calls were dispatched (retrying after tool
// execution is unsafe because side effects would replay).
// Max 4 attempts total. Backoff: 1s, 2s, 4s.
func LoopWithRetry(
	ctx context.Context,
	adapter model.Adapter,
	modelName string,
	maxTokens int,
	history []model.Message,
	reg *tools.Registry,
	workingDir string,
	eventCh chan<- events.Event,
	stopConditions []StopCondition,
	approval ApprovalChecker,
) ([]model.Message, string, error) {

	const maxAttempts = 4

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := time.Duration(1<<uint(attempt-1)) * time.Second
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, "error", ctx.Err()
			}
			emit(eventCh, "avenor.retry", map[string]any{
				"attempt":     attempt + 1,
				"max_retries": maxAttempts - 1,
			})
		}

		history, stopReason, err := RunLoop(ctx, adapter, modelName, maxTokens, history, reg, workingDir, eventCh, stopConditions, approval)
		if err == nil {
			return history, stopReason, nil
		}

		lastErr = err

		if hasToolCalls(history) {
			return history, "error", err
		}

		if ctx.Err() != nil {
			return history, "error", err
		}
	}

	return nil, "error", lastErr
}

func hasToolCalls(history []model.Message) bool {
	for _, msg := range history {
		if msg.Role == model.RoleTool {
			return true
		}
		if len(msg.ToolCalls) > 0 {
			return true
		}
	}
	return false
}
