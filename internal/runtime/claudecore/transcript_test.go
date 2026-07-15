package claudecore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

func TestEncodeProjectPath(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"/Users/x/Code/foo", "-Users-x-Code-foo"},
		{"/Users/x/.botfiles", "-Users-x--botfiles"},
		{"/Users/x/Code/.wt-thing", "-Users-x-Code--wt-thing"},
		{"/path/with_underscore", "-path-with-underscore"},
		{"/Users/x/Code/afresh-app-api", "-Users-x-Code-afresh-app-api"},
	}
	for _, c := range cases {
		if got := encodeProjectPath(c.in); got != c.want {
			t.Errorf("encodeProjectPath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestTranscriptReaderMissingFile(t *testing.T) {
	r := NewTranscriptReader(filepath.Join(t.TempDir(), "missing.jsonl"))
	recs, _, err := r.Tick()
	if err != nil {
		t.Fatalf("Tick on missing file: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 records, got %d", len(recs))
	}
}

func TestTranscriptReaderIncremental(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	r := NewTranscriptReader(path)

	initial := `{"type":"user","timestamp":"t1"}
{"type":"ai-title","aiTitle":"hello world"}
`
	if err := os.WriteFile(path, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}

	recs, _, err := r.Tick()
	if err != nil {
		t.Fatalf("first Tick: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	if recs[1].AITitle != "hello world" {
		t.Errorf("aiTitle = %q, want %q", recs[1].AITitle, "hello world")
	}

	// Idempotent on unchanged file.
	recs, _, err = r.Tick()
	if err != nil {
		t.Fatalf("idempotent Tick: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 new records, got %d", len(recs))
	}

	// Appended records are returned on the next tick.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"type":"assistant","timestamp":"t3"}` + "\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	recs, _, err = r.Tick()
	if err != nil {
		t.Fatalf("Tick after append: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 new record, got %d", len(recs))
	}
	if recs[0].Type != "assistant" {
		t.Errorf("type = %q, want %q", recs[0].Type, "assistant")
	}
}

func TestTranscriptReaderSkipsMalformedLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	content := `{"type":"user"}
this is not json
{"type":"assistant"}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	r := NewTranscriptReader(path)
	recs, _, err := r.Tick()
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records (malformed skipped), got %d", len(recs))
	}
	if recs[0].Type != "user" || recs[1].Type != "assistant" {
		t.Fatalf("returned records = %+v, want [user, assistant]", recs)
	}
	// Offset must be past the malformed line.
	recs, _, err = r.Tick()
	if err != nil {
		t.Fatalf("idempotent Tick: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 records on idempotent tick, got %d", len(recs))
	}
}

func TestTranscriptReaderTruncationResetsOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	r := NewTranscriptReader(path)

	// Write a long initial transcript so the offset advances well past any
	// later truncation.
	first := `{"type":"user","timestamp":"long-enough-to-push-offset-past-replacement"}` + "\n"
	if err := os.WriteFile(path, []byte(first), 0o644); err != nil {
		t.Fatal(err)
	}
	if recs, _, err := r.Tick(); err != nil || len(recs) != 1 {
		t.Fatalf("first Tick: recs=%d err=%v", len(recs), err)
	}

	// Truncate and rewrite with a much shorter record. Without truncation
	// handling the reader's offset would stay above EOF and silently return
	// nothing forever.
	if err := os.WriteFile(path, []byte(`{"type":"assistant"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	recs, _, err := r.Tick()
	if err != nil {
		t.Fatalf("Tick after truncate: %v", err)
	}
	if len(recs) != 1 || recs[0].Type != "assistant" {
		t.Fatalf("recs after truncate = %+v, want [assistant]", recs)
	}
}

func TestTranscriptReaderLeavesPartialLineForNextTick(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	r := NewTranscriptReader(path)

	// Write a complete record followed by a partial (un-terminated) record.
	first := `{"type":"user"}` + "\n"
	partial := `{"type":"ass`
	if err := os.WriteFile(path, []byte(first+partial), 0o644); err != nil {
		t.Fatal(err)
	}

	recs, _, err := r.Tick()
	if err != nil {
		t.Fatalf("first Tick: %v", err)
	}
	if len(recs) != 1 || recs[0].Type != "user" {
		t.Fatalf("first Tick recs = %+v, want [user]", recs)
	}

	// Now complete the partial record with the rest of the JSON + newline.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`istant"}` + "\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	recs, _, err = r.Tick()
	if err != nil {
		t.Fatalf("second Tick: %v", err)
	}
	if len(recs) != 1 || recs[0].Type != "assistant" {
		t.Fatalf("second Tick recs = %+v, want [assistant]", recs)
	}
}

func TestUnmarshalRecordExtractsContentEvents(t *testing.T) {
	rec, err := unmarshalRecord([]byte(`{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"hello"},{"type":"thinking","thinking":"hmm"},{"type":"tool_use","id":"tool-1","name":"bash","input":{"cmd":"ls"}},{"type":"tool_result","tool_use_id":"tool-1","content":"ok"}],"stop_reason":"end_turn"}}`))
	if err != nil {
		t.Fatalf("unmarshalRecord: %v", err)
	}
	if rec.StopReason != "end_turn" {
		t.Fatalf("StopReason = %q, want end_turn", rec.StopReason)
	}
	if rec.Text != "hello" {
		t.Fatalf("Text = %q, want hello", rec.Text)
	}
	if len(rec.ContentEvents) != 4 {
		t.Fatalf("len(ContentEvents) = %d, want 4", len(rec.ContentEvents))
	}
	if rec.ContentEvents[0].Event != "agent.message_chunk" || rec.ContentEvents[1].Event != "agent.thought_chunk" || rec.ContentEvents[2].Event != "tool.call" || rec.ContentEvents[3].Event != "tool.call_update" {
		t.Fatalf("content events = %+v", rec.ContentEvents)
	}
}

func TestSessionScanTranscriptTickEmitsIncrementalContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	r := NewTranscriptReader(path)
	sess := NewSession(context.Background(), SessionOptions{SessionID: "ses_1", Transcript: r, EventsBuf: 16})
	sess.Prompted = true

	first := `{"type":"user","message":{"role":"user","content":"hello"}}` + "\n" + `{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"hi there"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(first), 0o644); err != nil {
		t.Fatal(err)
	}
	sess.ScanTranscriptTick()
	got := drainSessionEvents(sess.Events)
	if len(got) != 3 {
		t.Fatalf("first tick events = %+v, want 3 events", got)
	}
	if got[0].Event != "user.message_chunk" || got[1].Event != "agent.message_chunk" || got[2].Event != "agent.status" {
		t.Fatalf("first tick order = %+v", got)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn"}}` + "\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	sess.ScanTranscriptTick()
	got = drainSessionEvents(sess.Events)
	if len(got) != 2 {
		t.Fatalf("second tick events = %+v, want 2 events", got)
	}
	if got[0].Event != "agent.message_chunk" || got[1].Event != "session.end" {
		t.Fatalf("second tick order = %+v", got)
	}
}

func drainSessionEvents(ch <-chan events.Event) []events.Event {
	var out []events.Event
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}
