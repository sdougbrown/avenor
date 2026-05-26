package digest

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestAccumulatorProcessChunks(t *testing.T) {
	acc := NewAccumulator()

	// Same session + same type should accumulate without emitting.
	lines, err := acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","content":{"text":"He","type":"text"}}`), false)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("Process() returned %d lines, want 0 (accumulating)", len(lines))
	}

	lines, err = acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","content":{"text":"llo","type":"text"}}`), false)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("Process() returned %d lines, want 0 (still accumulating)", len(lines))
	}
}

func TestAccumulatorFlushesOnTypeChange(t *testing.T) {
	acc := NewAccumulator()

	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","content":{"text":"thinking...","type":"text"}}`), false)
	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","content":{"text":" more thinking","type":"text"}}`), false)

	// Different event type triggers flush.
	lines, err := acc.Process([]byte(`{"event":"agent.message_chunk","session_id":"ses_1","content":{"text":"Hello world","type":"text"}}`), false)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 1 {
		t.Fatalf("Process() returned %d lines, want 1 (flush thought_chunks)", len(lines))
	}
	want := "EVENT agent.thought_chunk ses_1 thinking... more thinking"
	if lines[0] != want {
		t.Fatalf("flushed line = %q, want %q", lines[0], want)
	}

	// After flush, the new event type starts accumulating.
	rem := acc.FlushRemaining(false)
	if len(rem) != 1 {
		t.Fatalf("FlushRemaining() returned %d lines, want 1", len(rem))
	}
	wantRem := "EVENT agent.message_chunk ses_1 Hello world"
	if rem[0] != wantRem {
		t.Fatalf("FlushRemaining line = %q, want %q", rem[0], wantRem)
	}
}

func TestAccumulatorFlushesOnSessionChange(t *testing.T) {
	acc := NewAccumulator()

	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_a","content":{"text":"session A","type":"text"}}`), false)

	// Different session triggers flush.
	lines, err := acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_b","content":{"text":"session B","type":"text"}}`), false)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 1 {
		t.Fatalf("Process() returned %d lines, want 1 (flush ses_a)", len(lines))
	}
	want := "EVENT agent.thought_chunk ses_a session A"
	if lines[0] != want {
		t.Fatalf("flushed line = %q, want %q", lines[0], want)
	}
}

func TestAccumulatorFlushesOnNonChunk(t *testing.T) {
	acc := NewAccumulator()

	// Buffer a thought_chunk.
	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","content":{"text":"thinking","type":"text"}}`), false)

	// Different type triggers flush of the thought_chunk and starts buffering message.
	lines, _ := acc.Process([]byte(`{"event":"agent.message_chunk","session_id":"ses_1","content":{"text":"msg","type":"text"}}`), false)
	if len(lines) != 1 || lines[0] != "EVENT agent.thought_chunk ses_1 thinking" {
		t.Fatalf("type-change flush: got %v", lines)
	}

	// Non-chunk event flushes the message_chunk buffer AND emits the non-chunk event.
	lines, err := acc.Process([]byte(`{"event":"tool.call","session_id":"ses_1","kind":"read","title":"foo.go"}`), false)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("Process() returned %d lines, want 2 (1 flushed message + 1 tool)", len(lines))
	}
	if lines[0] != "EVENT agent.message_chunk ses_1 msg" {
		t.Fatalf("line[0] = %q", lines[0])
	}
	if lines[1] != "EVENT tool.call ses_1 read:foo.go" {
		t.Fatalf("line[1] = %q", lines[1])
	}
}

func TestAccumulatorFlushesOnSessionAndTypeChange(t *testing.T) {
	acc := NewAccumulator()

	// Buffer thought chunks for ses_a.
	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_a","content":{"text":"A thinking","type":"text"}}`), false)

	// Different type same session flushes thought, starts buffering message.
	lines, _ := acc.Process([]byte(`{"event":"agent.message_chunk","session_id":"ses_a","content":{"text":"A msg","type":"text"}}`), false)
	if len(lines) != 1 {
		t.Fatalf("first type change: got %d lines, want 1", len(lines))
	}
	if lines[0] != "EVENT agent.thought_chunk ses_a A thinking" {
		t.Fatalf("got %q", lines[0])
	}

	// Different session triggers flush of ses_a's message buffer.
	lines, _ = acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_b","content":{"text":"B thinking","type":"text"}}`), false)
	if len(lines) != 1 {
		t.Fatalf("session change: got %d lines, want 1", len(lines))
	}
	if lines[0] != "EVENT agent.message_chunk ses_a A msg" {
		t.Fatalf("got %q", lines[0])
	}

	// Remaining has ses_b's thought.
	rem := acc.FlushRemaining(false)
	if len(rem) != 1 || rem[0] != "EVENT agent.thought_chunk ses_b B thinking" {
		t.Fatalf("FlushRemaining: %v", rem)
	}
}

func TestAccumulatorWithClassify(t *testing.T) {
	acc := NewAccumulator()

	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","content":{"text":"[finding] possible issue","type":"text"}}`), true)

	// Flush with classification.
	lines, err := acc.Process([]byte(`{"event":"session.end","session_id":"ses_1","stop_reason":"end_turn"}`), true)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if !strings.HasPrefix(lines[0], "FINDING EVENT") {
		t.Fatalf("line[0] = %q, want FINDING prefix", lines[0])
	}
	if !strings.Contains(lines[0], "possible issue") {
		t.Fatalf("line[0] = %q, missing accumulated text", lines[0])
	}
	if !strings.HasPrefix(lines[1], "MILESTONE EVENT") {
		t.Fatalf("line[1] = %q, want MILESTONE prefix", lines[1])
	}
}

func TestAccumulatorEmptyChunksIgnored(t *testing.T) {
	acc := NewAccumulator()

	// SSE format with empty delta.
	lines, err := acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","delta":""}`), false)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("got %d lines, want 0 (empty text ignored)", len(lines))
	}

	// Now a real chunk.
	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","content":{"text":"real","type":"text"}}`), false)
	rem := acc.FlushRemaining(false)
	if len(rem) != 1 || rem[0] != "EVENT agent.thought_chunk ses_1 real" {
		t.Fatalf("FlushRemaining: %v", rem)
	}
}

func TestAccumulatorFlushRemainingEmpty(t *testing.T) {
	acc := NewAccumulator()
	lines := acc.FlushRemaining(false)
	if len(lines) != 0 {
		t.Fatalf("FlushRemaining on empty accumulator returned %d lines", len(lines))
	}
}

func TestAccumulatorNonEventJSONIgnored(t *testing.T) {
	acc := NewAccumulator()

	lines, err := acc.Process([]byte(`{"no_event_field":"here"}`), false)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("got %d lines, want 0 (non-event JSON ignored)", len(lines))
	}
}

func TestAccumulatorEmptyLineIgnored(t *testing.T) {
	acc := NewAccumulator()

	lines, err := acc.Process([]byte(""), false)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("got %d lines, want 0", len(lines))
	}

	lines, err = acc.Process([]byte("   \n"), false)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("got %d lines, want 0", len(lines))
	}
}

func TestAccumulatorTwoSessionsInterleaved(t *testing.T) {
	acc := NewAccumulator()

	// Buffer ses_a thought.
	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_a","content":{"text":"A1","type":"text"}}`), false)

	// Different session triggers flush of ses_a, then buffers ses_b.
	lines, _ := acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_b","content":{"text":"B1","type":"text"}}`), false)
	if len(lines) != 1 || lines[0] != "EVENT agent.thought_chunk ses_a A1" {
		t.Fatalf("first flush (ses_a): got %v", lines)
	}

	// Back to ses_a — different session triggers flush of ses_b, then buffers ses_a.
	lines, _ = acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_a","content":{"text":"A2","type":"text"}}`), false)
	if len(lines) != 1 || lines[0] != "EVENT agent.thought_chunk ses_b B1" {
		t.Fatalf("second flush (ses_b): got %v", lines)
	}

	rem := acc.FlushRemaining(false)
	if len(rem) != 1 || rem[0] != "EVENT agent.thought_chunk ses_a A2" {
		t.Fatalf("FlushRemaining: %v", rem)
	}
}

func TestAccumulatorSSEDeltaFormat(t *testing.T) {
	acc := NewAccumulator()

	// SSE format uses "delta" instead of "content.text".
	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","delta":"th","message_id":"m1","part_id":"p1"}`), false)
	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","delta":"inking","message_id":"m1","part_id":"p1"}`), false)

	rem := acc.FlushRemaining(false)
	if len(rem) != 1 || rem[0] != "EVENT agent.thought_chunk ses_1 thinking" {
		t.Fatalf("FlushRemaining: %v", rem)
	}
}

func TestAccumulatorUserMessageChunk(t *testing.T) {
	acc := NewAccumulator()

	acc.Process([]byte(`{"event":"user.message_chunk","session_id":"ses_1","content":{"text":"User","type":"text"}}`), false)
	acc.Process([]byte(`{"event":"user.message_chunk","session_id":"ses_1","content":{"text":" input","type":"text"}}`), false)

	// Flush by changing event type.
	lines, _ := acc.Process([]byte(`{"event":"agent.message_chunk","session_id":"ses_1","content":{"text":"response","type":"text"}}`), false)
	if len(lines) != 1 || lines[0] != "EVENT user.message_chunk ses_1 User input" {
		t.Fatalf("user.message_chunk flush: got %v", lines)
	}
}

func TestAccumulatorWhitespaceCollapsing(t *testing.T) {
	acc := NewAccumulator()

	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","content":{"text":"line1\nline2","type":"text"}}`), false)
	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","content":{"text":"\tindented","type":"text"}}`), false)
	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_1","content":{"text":"\rreturn","type":"text"}}`), false)

	rem := acc.FlushRemaining(false)
	if len(rem) != 1 || rem[0] != "EVENT agent.thought_chunk ses_1 line1 line2 indented return" {
		t.Fatalf("whitespace collapsing: got %v", rem)
	}
}

func TestAccumulatorMultipleBuffersFlushedByNonChunk(t *testing.T) {
	acc := NewAccumulator()

	// Build up thought_chunk and message_chunk for ses_a simultaneously.
	acc.Process([]byte(`{"event":"agent.thought_chunk","session_id":"ses_a","content":{"text":"thinking","type":"text"}}`), false)
	// Type change: thought flushed, message starts buffering.
	acc.Process([]byte(`{"event":"agent.message_chunk","session_id":"ses_a","content":{"text":"speaking","type":"text"}}`), false)

	// Both buffers exist now (thought flushed above, message still buffered).
	// Add another chunk with same key (message_chunk for ses_a) — stays buffered.
	acc.Process([]byte(`{"event":"agent.message_chunk","session_id":"ses_a","content":{"text":" more","type":"text"}}`), false)

	// Non-chunk event flushes the remaining message buffer.
	lines, err := acc.Process([]byte(`{"event":"tool.call","session_id":"ses_a","kind":"read","title":"foo.go"}`), false)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2 (1 flushed message + 1 tool)", len(lines))
	}
	if lines[0] != "EVENT agent.message_chunk ses_a speaking more" {
		t.Fatalf("line[0] = %q", lines[0])
	}
	if lines[1] != "EVENT tool.call ses_a read:foo.go" {
		t.Fatalf("line[1] = %q", lines[1])
	}
}

func TestAccumulatorEmptySessionID(t *testing.T) {
	acc := NewAccumulator()

	// Two sessions with empty session_id should NOT collide (they get key "|agent.thought_chunk").
	// In practice this is fine — events with no session_id are rare and the Accumulator
	// doesn't claim to disentangle sessions with missing IDs.
	acc.Process([]byte(`{"event":"agent.thought_chunk","content":{"text":"A","type":"text"}}`), false)

	// Same key flushes nothing, just appends.
	acc.Process([]byte(`{"event":"agent.thought_chunk","content":{"text":"B","type":"text"}}`), false)

	rem := acc.FlushRemaining(false)
	if len(rem) != 1 || rem[0] != "EVENT agent.thought_chunk  AB" {
		t.Fatalf("empty session_id: got %v", rem)
	}
}

func TestAccumulatorMalformedJSONWrapsParseError(t *testing.T) {
	acc := NewAccumulator()
	_, err := acc.Process([]byte(`{broken`), false)
	if err == nil {
		t.Fatal("Process() error = nil, want malformed JSON error")
	}
	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Fatalf("Process() error %v is not a *json.SyntaxError (type: %T)", err, err)
	}
}
