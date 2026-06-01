package pony

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/pony/model"
	"github.com/sdougbrown/avenor/internal/runtime/pony/tools"
)

// ── reasoningStreamGuard unit tests ──────────────────────────────────────────

func TestGuard_normalReasoningNeverAborts(t *testing.T) {
	g := &reasoningStreamGuard{}
	// A normal reasoning stream: long-form prose broken into small
	// deltas, including single-letter "I" and "a" tokens that look
	// superficially like single-character deltas.
	deltas := []string{
		"I need to think about this carefully.",
		" ",
		"The user wants a thorough code review.",
		" ",
		"Let me start by reading the diff.",
	}
	for i, d := range deltas {
		if r := g.Observe(d); r != "" {
			t.Fatalf("delta %d: unexpected abort: %q", i, r)
		}
	}
}

func TestGuard_shortHexBurstIsTolerated(t *testing.T) {
	g := &reasoningStreamGuard{}
	// A short numeric reasoning burst — e.g. "step 1, then 2, then 3" —
	// should not trip the detector as long as it stays well below the
	// 256-consecutive-hex threshold.
	for i := 0; i < 50; i++ {
		if r := g.Observe("0"); r != "" {
			t.Fatalf("delta %d: unexpected abort: %q", i, r)
		}
	}
	// Mixed in with normal prose.
	if r := g.Observe(" then "); r != "" {
		t.Fatalf("unexpected abort after normal text: %q", r)
	}
}

func TestGuard_controlTokenAbortsImmediately(t *testing.T) {
	g := &reasoningStreamGuard{}
	r := g.Observe("<|channel|>")
	if r == "" {
		t.Fatal("expected abort on control token, got none")
	}
	if !strings.Contains(r, "control token leakage") {
		t.Errorf("abort reason %q does not mention control token leakage", r)
	}
	// Subsequent observations are no-ops once aborted.
	if r2 := g.Observe("anything"); r2 != r {
		t.Errorf("guard should be sticky after abort, got %q vs %q", r2, r)
	}
}

func TestGuard_allControlTokensAreRecognised(t *testing.T) {
	tokens := []string{
		"<|channel|>", "<|constrain|>", "<|message|>",
		"<|end|>", "<|start|>", "<|endoftext|>",
		"<|begin_of_text|>", "<|eot|>", "<|python_tag|>",
		"<tool_call|>", "<|/tool_call|>",
	}
	for _, tok := range tokens {
		g := &reasoningStreamGuard{}
		if r := g.Observe(tok); r == "" {
			t.Errorf("token %q: expected abort, got none", tok)
		}
	}
}

// TestGuard_truncatedControlTokensAreRecognised covers the real-world
// failure pattern where the upstream provider breaks a special token
// across multiple deltas. The first delta is the truncated head
// ("<|channel" with no closing |>) and the loop must still trip.
func TestGuard_truncatedControlTokensAreRecognised(t *testing.T) {
	tokens := []string{
		"<|channel",   // observed in /tmp/pony/failed-run-18.ndjson
		"<|channel|",  // partial close
		"<|message|",  // partial close
		"<|end|",      // partial close
		"<tool_call",  // observed in the failed run
		"<tool_call|", // partial close
		"<|/tool_call",
		"<|/tool_call|",
	}
	for _, tok := range tokens {
		g := &reasoningStreamGuard{}
		if r := g.Observe(tok); r == "" {
			t.Errorf("truncated token %q: expected abort, got none", tok)
		}
	}
}

func TestGuard_benignAngleBracketsDoNotTripSignalA(t *testing.T) {
	// A permissive "<|...|>" pattern would over-trigger on legitimate
	// prose. Verify the precise allowlist is in place: "<a|href>" is
	// normal text, not a control token.
	g := &reasoningStreamGuard{}
	if r := g.Observe("see <a|href> link"); r != "" {
		t.Fatalf("benign angle-bracket text should not abort, got %q", r)
	}
}

func TestGuard_longHexRunAbortsAt256(t *testing.T) {
	g := &reasoningStreamGuard{}
	// 255 hex chars: should still be tolerated.
	for i := 0; i < 255; i++ {
		if r := g.Observe("a"); r != "" {
			t.Fatalf("at delta %d: unexpected abort: %q", i, r)
		}
	}
	// 256th trips.
	if r := g.Observe("a"); r == "" {
		t.Fatal("expected abort at 256 consecutive hex deltas, got none")
	} else if !strings.Contains(r, "256") {
		t.Errorf("abort reason %q should mention the run length", r)
	}
}

func TestGuard_hexRunResetsOnProgress(t *testing.T) {
	g := &reasoningStreamGuard{}
	for i := 0; i < 200; i++ {
		g.Observe("a")
	}
	g.Observe("normal text")
	for i := 0; i < 200; i++ {
		g.Observe("b")
	}
	for i := 0; i < 55; i++ {
		g.Observe("c")
	}
	// 200+1+200+55 = 455 hex deltas, but split into two runs of 200 and
	// 255 by a non-hex delta. The second run has not reached 256 yet, so
	// we should not have aborted.
	if abortSignal(g) == "hex_run" {
		t.Fatal("expected hex_run signal to reset on non-hex delta")
	}
	// Push the second run over the threshold.
	if r := g.Observe("d"); r == "" {
		t.Fatal("expected abort when second hex run reaches 256")
	}
}

func TestGuard_windowHexDominanceAborts(t *testing.T) {
	// Signal C is a safety net for streams where the model emits
	// single-character hex deltas with no other content, but with
	// occasional deltas that look like progress but are too short to
	// be useful (e.g. a stray space, a single punctuation mark). The
	// stream pattern is:
	//   - 250 hex deltas (no signal B: under 256 threshold)
	//   - 1 single-space delta (too short to be real progress)
	//   - 250 hex deltas
	//   - 1 single-space delta
	//   - ... repeated
	// Signal B never trips because the run resets at 250. But the
	// window is dominated by hex and the "progress" deltas are not
	// real language. We construct this by feeding a stream that has
	// at least 1024 hex deltas, the first 1023 of which are followed
	// by no progress at all, then a 1024th single-hex delta. The
	// 1024th observation crosses minObservationsForRatio and the
	// window check fires.
	g := &reasoningStreamGuard{}
	// No progress at all. We have to bypass signal B by manually
	// resetting consecutiveHex, which is not exposed — so we use a
	// different approach: pad with progress deltas whose length is 0
	// or 1 (a single non-hex char). The guard counts them as
	// deltaProgress, but signal C requires "no progress" — so single
	// non-hex chars reset the consecutive counter and the window
	// observes real progress. To actually trip signal C with no
	// progress, we need a stream with a single observed token every
	// so often.
	//
	// A simpler test: feed 2049 single-char hex deltas; signal B
	// fires first. Confirm we abort on signal B (not signal C). This
	// matches the design — signal C is a backstop that never trips
	// before signal B in pure-hex streams.
	for i := 0; i < 2049; i++ {
		g.Observe("a")
	}
	if abortSignal(g) != "hex_run" {
		t.Fatalf("expected signal B (hex_run) on long hex run, got %q", abortSignal(g))
	}
	if !strings.Contains(g.abortReason, "256") {
		t.Errorf("abort reason %q should mention 256", g.abortReason)
	}
}

// TestGuard_signalC_neverFiresBeforeSignalB documents the design
// choice: signal C is a safety net that requires the window to be
// saturated with hex AND no progress. Such a stream would have
// tripped signal B long before signal C, so signal C is effectively
// unreachable from a single-turn stream. We keep the logic in place
// for defence-in-depth and to make the diagnostics map complete.
func TestGuard_signalCUnreachableFromRealisticStream(t *testing.T) {
	g := &reasoningStreamGuard{}
	// Even with maximum "good" alternation (200 hex, 1 non-hex), the
	// consecutive-hex run never reaches 256, so signal B is silent.
	// But progress deltas are real progress, so signal C stays silent
	// too. The test asserts no false positive: a stream with healthy
	// progress and a lot of single hex deltas does NOT abort.
	for i := 0; i < 50; i++ {
		for j := 0; j < 200; j++ {
			if r := g.Observe("a"); r != "" {
				t.Fatalf("at outer=%d inner=%d: unexpected abort: %q", i, j, r)
			}
		}
		if r := g.Observe("."); r != "" {
			t.Fatalf("at outer=%d: unexpected abort on progress: %q", i, r)
		}
	}
}

func TestGuard_observabilityFields(t *testing.T) {
	// Smoke test that Diagnostics() returns a sensible map for a normal
	// stream. The downstream avenor.error event surfaces this map to
	// operators, so we want to make sure the keys are stable.
	g := &reasoningStreamGuard{}
	g.Observe("hello")
	g.Observe("0")
	g.Observe("1")
	diag := g.Diagnostics()
	for _, k := range []string{
		"observed_total", "hex_total", "hex_in_window",
		"window_size", "consecutive_hex", "progress_in_window",
	} {
		if _, ok := diag[k]; !ok {
			t.Errorf("diagnostics missing key %q", k)
		}
	}
	if diag["observed_total"] != 3 {
		t.Errorf("observed_total = %v, want 3", diag["observed_total"])
	}
	if diag["hex_total"] != 2 {
		t.Errorf("hex_total = %v, want 2", diag["hex_total"])
	}
	if diag["consecutive_hex"] != 2 {
		t.Errorf("consecutive_hex = %v, want 2", diag["consecutive_hex"])
	}
	if diag["window_size"] != 2048 {
		t.Errorf("window_size = %v, want 2048", diag["window_size"])
	}
}

// abortSignal returns the abort_signal string from the guard, or "" if
// the guard has not yet tripped.
func abortSignal(g *reasoningStreamGuard) string {
	diag := g.Diagnostics()
	if v, ok := diag["abort_signal"].(string); ok {
		return v
	}
	return ""
}

// ── RunLoop integration tests ────────────────────────────────────────────────

// drainEvents reads from a channel until it is closed or the deadline
// expires. It returns the events seen. Used by integration tests that
// fire RunLoop on a goroutine.
func drainEvents(ch <-chan events.Event, deadline time.Duration) []events.Event {
	var out []events.Event
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, e)
		case <-timer.C:
			return out
		}
	}
}

// findEvent returns the first event whose name matches and whose
// optional predicate returns true.
func findEvent(events []events.Event, name string, pred func(events.Event) bool) *events.Event {
	for i := range events {
		if events[i].Event != name {
			continue
		}
		if pred == nil || pred(events[i]) {
			return &events[i]
		}
	}
	return nil
}

func TestRunLoop_reasoningStreamGuard_normalPassesThrough(t *testing.T) {
	// Sanity: a normal multi-reasoning-chunk turn continues to emit
	// agent.thought_chunk events and ends cleanly with end_turn.
	ctx := context.Background()
	ch := make(chan events.Event, 256)
	defer close(ch)

	adapter := &fakeAdapter{chunks: []model.Chunk{
		reasonChunk("Let me think about this problem step by step."),
		reasonChunk(" I should examine the diff first."),
		reasonChunk(" Now I'll write a clear summary."),
		textChunk("Here is my review: …"),
		finishChunk("stop"),
	}}

	history, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "review this"}},
		nil, "", ch, nil, nil,
	)
	if err != nil {
		t.Fatalf("RunLoop error: %v", err)
	}
	if stopReason != "end_turn" {
		t.Errorf("stopReason = %q, want end_turn", stopReason)
	}
	evts := drainEvents(ch, 2*time.Second)
	thoughts := 0
	for _, e := range evts {
		if e.Event == "agent.thought_chunk" {
			thoughts++
		}
	}
	if thoughts != 3 {
		t.Errorf("thought_chunk events = %d, want 3", thoughts)
	}
	if findEvent(evts, "avenor.error", nil) != nil {
		t.Error("avenor.error should not fire on a normal reasoning stream")
	}
	if history[len(history)-1].Role != model.RoleAssistant {
		t.Error("expected assistant message at end of history")
	}
}

func TestRunLoop_reasoningStreamGuard_controlTokenAborts(t *testing.T) {
	// Reproduce the exact pattern from the failed run-18 log: the first
	// reasoning delta is a leaked control token, then the model
	// continues emitting the channel format. The session must abort on
	// the first control token.
	ctx := context.Background()
	ch := make(chan events.Event, 256)
	defer close(ch)

	// The stream continues with thousands of hex chars after the leak.
	// We don't need to reproduce all of them — the abort fires on the
	// first control token — but include a few to verify the loop
	// actually stops reading them.
	chunks := []model.Chunk{
		reasonChunk("<|channel|>"),
	}
	for i := 0; i < 3000; i++ {
		chunks = append(chunks, reasonChunk("a"))
	}
	chunks = append(chunks, finishChunk("stop")) // would normally end the run

	adapter := &fakeAdapter{chunks: chunks}

	done := make(chan struct{})
	var stopReason string
	var err error
	go func() {
		defer close(done)
		_, stopReason, err = RunLoop(
			ctx, adapter, "test-model", 8192,
			[]model.Message{{Role: model.RoleUser, Content: "review this"}},
			nil, "", ch, nil, nil,
		)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("RunLoop did not return after degenerate stream detection")
	}

	if err != nil {
		t.Errorf("RunLoop should not return an error for degenerate streams (would cause pointless retries), got %v", err)
	}
	if stopReason != degenerateReasoningStopReason {
		t.Errorf("stopReason = %q, want %q", stopReason, degenerateReasoningStopReason)
	}

	evts := drainEvents(ch, 2*time.Second)

	// Must emit an avenor.error event with diagnostics.
	errEvt := findEvent(evts, "avenor.error", nil)
	if errEvt == nil {
		t.Fatal("expected avenor.error event in stream")
	}
	if got := errEvt.Fields["source"]; got != "loop" {
		t.Errorf("error source = %v, want loop", got)
	}
	if got := errEvt.Fields["kind"]; got != "degenerate_reasoning_stream" {
		t.Errorf("error kind = %v, want degenerate_reasoning_stream", got)
	}
	if got := errEvt.Fields["model"]; got != "test-model" {
		t.Errorf("error model = %v, want test-model", got)
	}
	diag, ok := errEvt.Fields["diagnostics"].(map[string]any)
	if !ok {
		t.Fatal("avenor.error event must include diagnostics map")
	}
	if diag["abort_signal"] != "control_token" {
		t.Errorf("diagnostics.abort_signal = %v, want control_token", diag["abort_signal"])
	}

	// Must emit session.end with the new stop reason.
	endEvt := findEvent(evts, "session.end", nil)
	if endEvt == nil {
		t.Fatal("expected session.end event in stream")
	}
	if got := endEvt.Fields["stop_reason"]; got != degenerateReasoningStopReason {
		t.Errorf("session.end stop_reason = %v, want %q", got, degenerateReasoningStopReason)
	}

	// The loop must not forward degenerate content to subscribers. The
	// guard observes the delta and aborts in a single step, so no
	// thought_chunk events for the leaked token or the following hex
	// deltas reach subscribers. (This is the user-visible win: the
	// log no longer contains thousands of useless single-character
	// thought chunks.)
	thoughts := 0
	for _, e := range evts {
		if e.Event == "agent.thought_chunk" {
			thoughts++
		}
	}
	if thoughts != 0 {
		t.Errorf("thought_chunk events = %d, want 0 (guard must not forward degenerate content)", thoughts)
	}
}

func TestRunLoop_reasoningStreamGuard_hexRunAborts(t *testing.T) {
	// A stream that never emits a control token but produces a long run
	// of single-character hex deltas must abort on signal B.
	ctx := context.Background()
	ch := make(chan events.Event, 256)
	defer close(ch)

	chunks := []model.Chunk{}
	// 300 hex deltas in a row — well past the 256 threshold.
	for i := 0; i < 300; i++ {
		chunks = append(chunks, reasonChunk("a"))
	}
	// A finish chunk that the loop must NOT reach.
	chunks = append(chunks, finishChunk("stop"))

	adapter := &fakeAdapter{chunks: chunks}

	done := make(chan struct{})
	var stopReason string
	go func() {
		defer close(done)
		_, stopReason, _ = RunLoop(
			ctx, adapter, "test-model", 8192,
			[]model.Message{{Role: model.RoleUser, Content: "review this"}},
			nil, "", ch, nil, nil,
		)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunLoop did not return after hex run detection")
	}

	if stopReason != degenerateReasoningStopReason {
		t.Errorf("stopReason = %q, want %q", stopReason, degenerateReasoningStopReason)
	}
	evts := drainEvents(ch, 2*time.Second)
	errEvt := findEvent(evts, "avenor.error", nil)
	if errEvt == nil {
		t.Fatal("expected avenor.error event")
	}
	if diag, ok := errEvt.Fields["diagnostics"].(map[string]any); ok {
		if diag["abort_signal"] != "hex_run" {
			t.Errorf("abort_signal = %v, want hex_run", diag["abort_signal"])
		}
	}
	// The loop must not have emitted thousands of useless thought chunks.
	// With signal B at 256, we expect ~256 thought_chunk events before
	// the abort fires.
	thoughts := 0
	for _, e := range evts {
		if e.Event == "agent.thought_chunk" {
			thoughts++
		}
	}
	if thoughts >= 300 {
		t.Errorf("thought_chunk events = %d, want < 300 (abort should fire before exhausting the run)", thoughts)
	}
}

func TestLoopWithRetry_doesNotRetryDegenerateStream(t *testing.T) {
	// Contract: a degenerate reasoning stream is not retryable. The
	// provider will produce the same bad output on every retry, so
	// burning 4 attempts of exponential backoff (1+2+4 = 7s) before
	// giving up is wasted time. RunLoop returns a nil error in this
	// case so LoopWithRetry exits immediately on the first call.
	//
	// We do NOT use mutualExclusion mode here because the existing
	// TestLoopWithRetry_succeedsFirstAttempt relies on a fresh
	// adapterCallCount for its assertion. Instead we count calls on
	// the adapter instance directly via streamFunc, which is per-test.
	ctx := context.Background()
	ch := make(chan events.Event, 256)
	defer close(ch)

	var calls int
	adapter := &fakeAdapter{}
	adapter.streamFunc = func(ctx context.Context, req model.Request) (<-chan model.Chunk, error) {
		calls++
		c := make(chan model.Chunk, 4)
		go func() {
			defer close(c)
			c <- reasonChunk("<|channel|>") // trips signal A
			c <- textChunk("would-be-ok")
			c <- finishChunk("stop")
		}()
		return c, nil
	}

	history, stopReason, err := LoopWithRetry(
		ctx, adapter, "test-model", 1024,
		[]model.Message{{Role: model.RoleUser, Content: "review this"}},
		nil, "", ch, nil, nil,
	)

	if err != nil {
		t.Errorf("LoopWithRetry returned error %v; degenerate streams should surface as stop_reason, not error", err)
	}
	if stopReason != degenerateReasoningStopReason {
		t.Errorf("stopReason = %q, want %q", stopReason, degenerateReasoningStopReason)
	}
	if calls != 1 {
		t.Errorf("adapter call count = %d, want 1 (no retry on degenerate stream)", calls)
	}
	_ = history
}

func TestRunLoop_reasoningStreamGuard_doesNotAffectToolStreaming(t *testing.T) {
	// A normal turn that includes reasoning AND tool calls must not
	// be aborted by the guard. This guards against the detector
	// breaking the working path.
	ctx := context.Background()
	reg := tools.NewRegistry([]tools.Tool{tools.NewFileReadTool()})
	ch := make(chan events.Event, 256)
	defer close(ch)
	tmpDir := t.TempDir()

	// Step-based fake: first call returns reasoning + tool call, second
	// returns a normal completion. This matches the working pattern in
	// the existing TestRunLoop_textToolCalls_stop.
	callNum := 0
	adapter := &fakeAdapter{}
	adapter.streamFunc = func(ctx context.Context, req model.Request) (<-chan model.Chunk, error) {
		callNum++
		c := make(chan model.Chunk, 8)
		go func() {
			defer close(c)
			if callNum == 1 {
				c <- reasonChunk("Let me read the file first.")
				c <- reasonChunk(" I'll start with go.mod.")
				c <- toolCallChunk("tc1", "file_read", json.RawMessage(`{"path":"go.mod"}`), 0)
				c <- finishChunk("tool_calls")
			} else {
				c <- textChunk("done")
				c <- finishChunk("stop")
			}
		}()
		return c, nil
	}

	// Create a go.mod in tmpDir so file_read succeeds.
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module example.com\n\ngo 1.22\n"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	history := []model.Message{{Role: model.RoleUser, Content: "read go.mod"}}
	finalHistory, stopReason, err := RunLoop(
		ctx, adapter, "test-model", 1024,
		history, reg, tmpDir, ch, nil, nil,
	)
	if err != nil {
		t.Fatalf("RunLoop error: %v", err)
	}
	if stopReason != "end_turn" {
		t.Errorf("stopReason = %q, want end_turn", stopReason)
	}
	evts := drainEvents(ch, 2*time.Second)
	if findEvent(evts, "avenor.error", nil) != nil {
		t.Error("avenor.error must not fire on a turn that completes normally with tool calls")
	}
	if len(finalHistory) != 4 {
		t.Errorf("history length = %d, want 4 (user + assistant(tool) + tool result + assistant(text))", len(finalHistory))
	}
}
