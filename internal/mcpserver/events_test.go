package mcpserver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

func TestReadEventsBasic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.log")
	content := `{"event":"start","type":"lifecycle"}
{"event":"prompt","type":"turn","text":"hello"}
{"event":"done","type":"lifecycle"}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	events, _, err := readEvents(path, nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
}

func TestReadEventsBoundsTerminalPreviewAndMarksIt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-bounded.log")
	complete := strings.Repeat("é", events.MaxFinalOutputRunes+10)
	line, err := json.Marshal(map[string]any{"event": "session.end", "final_output": complete})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(line, '\n'), 0644); err != nil {
		t.Fatal(err)
	}

	read, _, err := readEvents(path, nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	preview, _ := read[0]["final_output"].(string)
	if len([]rune(preview)) != events.MaxFinalOutputRunes {
		t.Fatalf("preview rune count = %d, want %d", len([]rune(preview)), events.MaxFinalOutputRunes)
	}
	if read[0]["final_output_truncated"] != true {
		t.Fatalf("final_output_truncated = %v, want true", read[0]["final_output_truncated"])
	}
}

func TestReadFinalOutputReadsBeyondScannerLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-large.log")
	complete := strings.Repeat("é", 40_000) // 80 KiB UTF-8, beyond Scanner's 64 KiB default.
	line, err := json.Marshal(map[string]any{"event": "session.end", "final_output": complete})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(line, '\n'), 0644); err != nil {
		t.Fatal(err)
	}

	output, found, err := readFinalOutput(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || output != complete {
		t.Fatalf("readFinalOutput found=%v length=%d, want complete length=%d", found, len(output), len(complete))
	}
}

func TestReadEventsFilterByType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-filter.log")
	content := `{"event":"start","type":"lifecycle"}
{"event":"prompt","type":"turn"}
{"event":"done","type":"lifecycle"}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	events, _, err := readEvents(path, []string{"turn"}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event after filtering, got %d", len(events))
	}
	if events[0]["event"] != "prompt" {
		t.Errorf("expected prompt, got %v", events[0]["event"])
	}
}

func TestReadEventsFilterByEventField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-event.log")
	content := `{"event":"start","type":"lifecycle"}
{"event":"prompt","type":"turn"}
{"event":"done","type":"lifecycle"}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	events, _, err := readEvents(path, []string{"lifecycle"}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 lifecycle events, got %d", len(events))
	}
}

func TestReadEventsWithLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-limit.log")
	var lines string
	for i := 0; i < 100; i++ {
		lines += fmt.Sprintf(`{"event":"tick","n":%d}`+"\n", i)
	}
	if err := os.WriteFile(path, []byte(lines), 0644); err != nil {
		t.Fatal(err)
	}

	events, _, err := readEvents(path, nil, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 10 {
		t.Fatalf("expected 10 events, got %d", len(events))
	}
	last, _ := events[9]["n"].(float64)
	if last != 99 {
		t.Errorf("expected last event n=99, got %v", last)
	}
}

func TestReadEventsFileNotFound(t *testing.T) {
	events, _, err := readEvents("/nonexistent/events.log", nil, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	if events != nil {
		t.Fatalf("expected nil events for missing file, got %v", events)
	}
}

func TestReadEventsMalformedLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-malformed.log")
	content := `{"event":"good"}
not json
{"event":"also good"}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	events, _, err := readEvents(path, nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 valid events skipping malformed, got %d", len(events))
	}
}

func TestReadEventsEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-empty.log")
	if err := os.WriteFile(path, []byte(""), 0644); err != nil {
		t.Fatal(err)
	}

	events, _, err := readEvents(path, nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events from empty file, got %d", len(events))
	}
}

func TestReadEventsFilterWithLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-filter-limit.log")
	var lines string
	for i := 0; i < 10; i++ {
		lines += fmt.Sprintf(`{"event":"start","type":"lifecycle","n":%d}`+"\n", i*2)
		lines += fmt.Sprintf(`{"event":"tick","type":"turn","n":%d}`+"\n", i*2+1)
	}
	if err := os.WriteFile(path, []byte(lines), 0644); err != nil {
		t.Fatal(err)
	}

	events, _, err := readEvents(path, []string{"lifecycle"}, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
	for _, e := range events {
		typ, _ := e["type"].(string)
		if typ != "lifecycle" {
			t.Errorf("expected all lifecycle events, got type %v", typ)
		}
	}
	firstN, _ := events[0]["n"].(float64)
	lastN, _ := events[4]["n"].(float64)
	if firstN != 10 || lastN != 18 {
		t.Errorf("expected LAST 5 lifecycle events (n=10 to 18), got n=%v to n=%v", firstN, lastN)
	}
}

func TestReadEventsMultiTypeFilter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-multi.log")
	content := `{"event":"start","type":"lifecycle"}
{"event":"prompt","type":"turn"}
{"event":"error","type":"turn"}
{"event":"done","type":"lifecycle"}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	events, _, err := readEvents(path, []string{"lifecycle", "turn"}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("expected 4 events matching lifecycle or turn, got %d", len(events))
	}
}

func TestReadEventsAfterSeqZeroReturnsFromStart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-cursor0.log")
	var lines string
	for i := 1; i <= 5; i++ {
		lines += fmt.Sprintf(`{"event":"tick","seq":%d}`+"\n", i)
	}
	if err := os.WriteFile(path, []byte(lines), 0644); err != nil {
		t.Fatal(err)
	}

	cursor := int64(0)
	events, latestSeq, err := readEvents(path, nil, 0, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 {
		t.Fatalf("expected 5 events from cursor 0, got %d", len(events))
	}
	first, _ := events[0]["seq"].(float64)
	if first != 1 {
		t.Errorf("expected first event seq=1 (oldest-first), got %v", first)
	}
	last, _ := events[4]["seq"].(float64)
	if last != 5 {
		t.Errorf("expected last event seq=5, got %v", last)
	}
	if latestSeq != 5 {
		t.Errorf("expected latest_seq=5, got %d", latestSeq)
	}
}

func TestReadEventsCursorPagesOldestFirst(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-page.log")
	var lines string
	for i := 1; i <= 10; i++ {
		lines += fmt.Sprintf(`{"event":"tick","seq":%d}`+"\n", i)
	}
	if err := os.WriteFile(path, []byte(lines), 0644); err != nil {
		t.Fatal(err)
	}

	cursor := int64(3)
	events, latestSeq, err := readEvents(path, nil, 4, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}
	want := []float64{4, 5, 6, 7}
	for i, e := range events {
		seq, _ := e["seq"].(float64)
		if seq != want[i] {
			t.Errorf("event %d seq = %v, want %v (oldest-first page)", i, seq, want[i])
		}
	}
	if latestSeq != 7 {
		t.Errorf("expected latest_seq=7, got %d", latestSeq)
	}
}

func TestReadEventsEmptyPageReturnsCursor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-beyond.log")
	var lines string
	for i := 1; i <= 5; i++ {
		lines += fmt.Sprintf(`{"event":"tick","seq":%d}`+"\n", i)
	}
	if err := os.WriteFile(path, []byte(lines), 0644); err != nil {
		t.Fatal(err)
	}

	cursor := int64(99)
	events, latestSeq, err := readEvents(path, nil, 10, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events beyond the end, got %d", len(events))
	}
	if latestSeq != 99 {
		t.Errorf("expected latest_seq=99 (the cursor), got %d", latestSeq)
	}
}

func TestReadEventsCursorDropsEventsWithoutSeq(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-noseq.log")
	content := `{"event":"a","seq":1}
{"event":"b"}
{"event":"c","seq":2}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cursor := int64(0)
	events, latestSeq, err := readEvents(path, nil, 0, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 seq-carrying events with cursor, got %d", len(events))
	}
	if latestSeq != 2 {
		t.Errorf("expected latest_seq=2 with cursor, got %d", latestSeq)
	}

	all, latestAll, err := readEvents(path, nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 events without cursor (seq-less included), got %d", len(all))
	}
	if latestAll != 2 {
		t.Errorf("expected latest_seq=2 without cursor, got %d", latestAll)
	}
}

func TestReadEventsRepeatedPagingCoversLogExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-resume.log")
	var lines string
	for i := 1; i <= 10; i++ {
		lines += fmt.Sprintf(`{"event":"tick","seq":%d}`+"\n", i)
	}
	if err := os.WriteFile(path, []byte(lines), 0644); err != nil {
		t.Fatal(err)
	}

	seen := map[int64]int{}
	cursor := int64(0)
	for page := 0; page < 100; page++ {
		events, latestSeq, err := readEvents(path, nil, 3, &cursor)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) == 0 {
			if latestSeq != cursor {
				t.Fatalf("empty page latest_seq = %d, want cursor %d", latestSeq, cursor)
			}
			break
		}
		for _, e := range events {
			seq, _ := e["seq"].(float64)
			seen[int64(seq)]++
		}
		if latestSeq <= cursor {
			t.Fatalf("latest_seq %d did not advance past cursor %d", latestSeq, cursor)
		}
		cursor = latestSeq
	}
	if len(seen) != 10 {
		t.Fatalf("expected 10 distinct seqs, got %d", len(seen))
	}
	for i := int64(1); i <= 10; i++ {
		if seen[i] != 1 {
			t.Errorf("seq %d seen %d times, want exactly once", i, seen[i])
		}
	}
}

func TestReadEventsMissingFileWithCursor(t *testing.T) {
	cursor := int64(42)
	events, latestSeq, err := readEvents("/nonexistent/events.log", nil, 50, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("expected empty events for missing file with cursor, got %v", events)
	}
	if latestSeq != 42 {
		t.Errorf("expected latest_seq=42 (the cursor) for missing file, got %d", latestSeq)
	}

	events, latestSeq, err = readEvents("/nonexistent/events.log", nil, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	if events != nil {
		t.Fatalf("expected nil events for missing file, got %v", events)
	}
	if latestSeq != 0 {
		t.Errorf("expected latest_seq=0 for missing file, got %d", latestSeq)
	}
}

// Without a cursor, latest_seq is the highest seq anywhere in the file
// (fileMaxSeq), not the max of the filtered page.
func TestReadEventsNoCursorLatestSeqIsFileMax(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-filemax.log")
	// The "turn" event (seq 5) has a higher seq than the "lifecycle" events.
	content := `{"event":"start","type":"lifecycle","seq":1}
{"event":"prompt","type":"turn","seq":5}
{"event":"done","type":"lifecycle","seq":3}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	// Filter to lifecycle only; the excluded turn event (seq 5) is the file max.
	events, latestSeq, err := readEvents(path, []string{"lifecycle"}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 lifecycle events, got %d", len(events))
	}
	if latestSeq != 5 {
		t.Errorf("expected latest_seq=5 (the file max), got %d", latestSeq)
	}
}

func TestReadEventsEmptyFilteredPageNoCursorReturnsFileMax(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-emptyfilter.log")
	content := `{"event":"start","type":"lifecycle","seq":1}
{"event":"prompt","type":"turn","seq":5}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	// Filter to a type that matches nothing; the page is empty.
	events, latestSeq, err := readEvents(path, []string{"nope"}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
	if latestSeq != 5 {
		t.Errorf("expected latest_seq=5 (the file max) for an empty filtered page, got %d", latestSeq)
	}
}

func TestReadEventsSeqlessFileNoCursorReturnsZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events-seqless.log")
	content := `{"event":"a"}
{"event":"b"}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	_, latestSeq, err := readEvents(path, nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if latestSeq != 0 {
		t.Errorf("expected latest_seq=0 for a seq-less file, got %d", latestSeq)
	}
}
