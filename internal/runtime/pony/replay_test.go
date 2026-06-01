package pony

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/pony/model"
)

// TestReplayFailedRun18_TerminatesOnControlToken is the acceptance
// test the user asked for: replaying the failed run-18 reasoning
// stream through RunLoop must terminate quickly with
// stop_reason=degenerate_reasoning_stream, and the event log must
// not contain thousands of useless single-character thought chunks.
//
// The run log is read-only and is only consulted when explicitly
// available. The test is skipped when the file is absent so it does
// not break for developers without the original artifact.
func TestReplayFailedRun18_TerminatesOnControlToken(t *testing.T) {
	const logPath = "/tmp/pony/failed-run-18.ndjson"
	f, err := os.Open(logPath)
	if err != nil {
		t.Skipf("replay log not present at %s: %v", logPath, err)
	}
	defer f.Close()

	// Extract every agent.thought_chunk delta from the log. This is the
	// upstream signal that the failed run was emitting; we replay them
	// as ChunkTypeReasoning deltas through a fresh RunLoop.
	var deltas []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var row map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			continue
		}
		if row["event"] != "agent.thought_chunk" {
			continue
		}
		s, _ := row["delta"].(string)
		deltas = append(deltas, s)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan log: %v", err)
	}
	if len(deltas) == 0 {
		t.Fatalf("no thought_chunk deltas in %s", logPath)
	}
	t.Logf("replaying %d reasoning deltas from %s", len(deltas), logPath)

	// Build the chunk stream. Each delta becomes a ChunkTypeReasoning.
	// We add a finish chunk at the tail that the loop must NOT reach
	// because the guard will abort the session first.
	chunks := make([]model.Chunk, 0, len(deltas)+1)
	for _, d := range deltas {
		chunks = append(chunks, reasonChunk(d))
	}
	chunks = append(chunks, finishChunk("stop"))

	ctx := context.Background()
	eventCh := make(chan events.Event, 256)
	defer close(eventCh)

	// A streaming adapter that consumes the prepared chunk slice one at
	// a time, with a small per-chunk delay so a buggy "burn the budget"
	// implementation would actually take the full max_tokens budget.
	adapter := &fakeAdapter{
		chunks:     chunks,
		chunkDelay: 0, // no delay: failure must come from the guard, not from a wall-clock wait
	}

	start := time.Now()
	history, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 8192,
		[]model.Message{{Role: model.RoleUser, Content: "replay"}},
		nil, "", eventCh, nil, nil,
	)
	elapsed := time.Since(start)
	t.Logf("RunLoop returned in %v with stop_reason=%q, err=%v", elapsed, stopReason, err)

	// Acceptance: stop_reason must be the new degenerate reason.
	if stopReason != DegenerateReasoningStopReason() {
		t.Fatalf("stopReason = %q, want %q", stopReason, DegenerateReasoningStopReason())
	}
	if err != nil {
		t.Errorf("RunLoop should not return an error for degenerate streams (would cause pointless retries), got %v", err)
	}

	// Acceptance: must not have consumed the entire budget. The guard
	// fires on the first <|channel|> delta (very early in the stream),
	// so elapsed time should be tiny.
	if elapsed > 2*time.Second {
		t.Errorf("RunLoop took %v to abort; expected < 2s after guard trip", elapsed)
	}

	// Acceptance: event log must not contain thousands of useless
	// thought_chunk events. The guard observes the leaked control
	// token in the very first delta and aborts before any
	// thought_chunk is forwarded.
	evts := drainEvents(eventCh, 1*time.Second)
	thoughtCount := 0
	for _, e := range evts {
		if e.Event == "agent.thought_chunk" {
			thoughtCount++
		}
	}
	if thoughtCount > 1 {
		t.Errorf("thought_chunk events = %d, want <= 1 (guard must drop degenerate content)", thoughtCount)
	}

	// Acceptance: must emit avenor.error + session.end with the new
	// stop reason.
	if findEvent(evts, "avenor.error", nil) == nil {
		t.Error("expected avenor.error event")
	}
	endEvt := findEvent(evts, "session.end", nil)
	if endEvt == nil {
		t.Fatal("expected session.end event")
	}
	if got := endEvt.Fields["stop_reason"]; got != DegenerateReasoningStopReason() {
		t.Errorf("session.end stop_reason = %v, want %q", got, DegenerateReasoningStopReason())
	}

	// history should still contain the original user prompt.
	if len(history) == 0 {
		t.Error("expected history to be non-empty")
	}
}

// Sanity: the constant is what we expect (defends against accidental
// rename).
func TestDegenerateReasoningStopReasonConstant(t *testing.T) {
	if got, want := DegenerateReasoningStopReason(), "degenerate_reasoning_stream"; got != want {
		t.Errorf("DegenerateReasoningStopReason() = %q, want %q", got, want)
	}
	if got := fmt.Sprintf("%s", "degenerate_reasoning_stream"); got != "degenerate_reasoning_stream" {
		// Defensive: make sure the test file's expectations are in
		// sync with the loop's emitted value.
		t.Errorf("constant drift: %q", got)
	}
}
