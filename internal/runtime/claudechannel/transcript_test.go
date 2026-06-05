package claudechannel

import (
	"os"
	"path/filepath"
	"testing"
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
	r := newTranscriptReader(filepath.Join(t.TempDir(), "missing.jsonl"))
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
	r := newTranscriptReader(path)

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
	r := newTranscriptReader(path)
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
	r := newTranscriptReader(path)

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
	r := newTranscriptReader(path)

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
