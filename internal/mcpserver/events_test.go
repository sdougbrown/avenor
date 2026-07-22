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

	events, err := readEvents(path, nil, 0)
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

	read, err := readEvents(path, nil, 0)
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

	events, err := readEvents(path, []string{"turn"}, 0)
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

	events, err := readEvents(path, []string{"lifecycle"}, 0)
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

	events, err := readEvents(path, nil, 10)
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
	events, err := readEvents("/nonexistent/events.log", nil, 50)
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

	events, err := readEvents(path, nil, 0)
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

	events, err := readEvents(path, nil, 0)
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

	events, err := readEvents(path, []string{"lifecycle"}, 5)
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

	events, err := readEvents(path, []string{"lifecycle", "turn"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("expected 4 events matching lifecycle or turn, got %d", len(events))
	}
}
