package claudecore

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

// Claude Code writes JSONL transcripts under ~/.claude/projects/<encoded>/
// where <encoded> is the absolute project dir with anything outside the
// [A-Za-z0-9-] character class collapsed to "-". This mapping is lossy
// (e.g. ".foo" and "/foo" both encode to "-foo") but Claude treats it as
// an opaque directory identifier; we only need to match its output.
var projectPathEncode = regexp.MustCompile(`[^A-Za-z0-9-]`)

func encodeProjectPath(dir string) string {
	return projectPathEncode.ReplaceAllString(dir, "-")
}

func TranscriptPath(home, dir, sessionID string) string {
	return filepath.Join(home, ".claude", "projects", encodeProjectPath(dir), sessionID+".jsonl")
}

// TranscriptRecord is the minimal subset of JSONL fields used for status and
// best-effort content extraction.
type TranscriptRecord struct {
	Type          string         `json:"type"`
	AITitle       string         `json:"aiTitle,omitempty"`
	Timestamp     string         `json:"timestamp,omitempty"`
	StopReason    string         `json:"stop_reason,omitempty"`
	Text          string         `json:"text,omitempty"`
	ContentEvents []events.Event `json:"-"`
}

// unmarshalRecord decodes a JSONL line into a TranscriptRecord, lifting
// stop_reason and best-effort text/tool events from the nested message object.
func unmarshalRecord(raw []byte) (TranscriptRecord, error) {
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawMap); err != nil {
		return TranscriptRecord{}, err
	}
	rec := TranscriptRecord{}
	if t, ok := rawMap["type"]; ok {
		_ = json.Unmarshal(t, &rec.Type)
	}
	if t, ok := rawMap["aiTitle"]; ok {
		_ = json.Unmarshal(t, &rec.AITitle)
	}
	if t, ok := rawMap["timestamp"]; ok {
		_ = json.Unmarshal(t, &rec.Timestamp)
	}
	if msg, ok := rawMap["message"]; ok {
		var m struct {
			Role       string          `json:"role,omitempty"`
			Content    json.RawMessage `json:"content,omitempty"`
			StopReason string          `json:"stop_reason,omitempty"`
		}
		if err := json.Unmarshal(msg, &m); err == nil {
			if m.StopReason != "" {
				rec.StopReason = m.StopReason
			}
			rec.ContentEvents, rec.Text = transcriptContentEvents(rec.Type, m.Role, m.Content)
		}
	}
	if rec.StopReason == "" {
		if sr, ok := rawMap["stop_reason"]; ok {
			_ = json.Unmarshal(sr, &rec.StopReason)
		}
	}
	return rec, nil
}

func transcriptContentEvents(recordType, role string, raw json.RawMessage) ([]events.Event, string) {
	kind := role
	if kind == "" {
		kind = recordType
	}
	kind = strings.ToLower(kind)
	if len(raw) == 0 {
		return nil, ""
	}

	var text string
	var simple string
	if err := json.Unmarshal(raw, &simple); err == nil {
		if ev := transcriptTextEvent(kind, simple); ev != nil {
			return []events.Event{*ev}, simple
		}
		return nil, simple
	}

	var blocks []any
	if err := json.Unmarshal(raw, &blocks); err == nil {
		out := make([]events.Event, 0, len(blocks))
		var builder strings.Builder
		for _, block := range blocks {
			m, ok := block.(map[string]any)
			if !ok {
				continue
			}
			blockType, _ := m["type"].(string)
			switch blockType {
			case "text":
				chunk, _ := m["text"].(string)
				if chunk == "" {
					continue
				}
				if ev := transcriptTextEvent(kind, chunk); ev != nil {
					out = append(out, *ev)
					builder.WriteString(chunk)
				}
			case "thinking", "reasoning":
				chunk, _ := m["thinking"].(string)
				if chunk == "" {
					chunk, _ = m["text"].(string)
				}
				if chunk == "" || kind != "assistant" {
					continue
				}
				out = append(out, events.Event{Event: "agent.thought_chunk", Fields: map[string]any{"delta": chunk}})
			case "tool_use":
				if kind != "assistant" {
					continue
				}
				fields := map[string]any{"kind": "tool", "status": "running"}
				if id, _ := m["id"].(string); id != "" {
					fields["tool_use_id"] = id
				}
				if name, _ := m["name"].(string); name != "" {
					fields["title"] = name
				}
				if input := m["input"]; input != nil {
					fields["input"] = input
				}
				out = append(out, events.Event{Event: "tool.call", Fields: fields})
			case "tool_result":
				fields := map[string]any{"kind": "tool", "status": "completed"}
				if id, _ := m["tool_use_id"].(string); id != "" {
					fields["tool_use_id"] = id
				}
				if chunk := extractTranscriptText(m["content"]); chunk != "" {
					fields["delta"] = chunk
				}
				out = append(out, events.Event{Event: "tool.call_update", Fields: fields})
			}
		}
		return out, builder.String()
	}

	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err == nil {
		if chunk := extractTranscriptText(obj); chunk != "" {
			if ev := transcriptTextEvent(kind, chunk); ev != nil {
				return []events.Event{*ev}, chunk
			}
			text = chunk
		}
	}
	return nil, text
}

func transcriptTextEvent(kind, text string) *events.Event {
	if text == "" {
		return nil
	}
	switch kind {
	case "assistant":
		ev := events.Event{Event: "agent.message_chunk", Fields: map[string]any{"delta": text}}
		return &ev
	case "user":
		ev := events.Event{Event: "user.message_chunk", Fields: map[string]any{"delta": text}}
		return &ev
	default:
		return nil
	}
}

func extractTranscriptText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		if text, _ := x["text"].(string); text != "" {
			return text
		}
		if content := extractTranscriptText(x["content"]); content != "" {
			return content
		}
	case []any:
		parts := make([]string, 0, len(x))
		for _, item := range x {
			if text := extractTranscriptText(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "")
	}
	return ""
}

// TranscriptReader incrementally reads new records from a JSONL transcript.
// Successive Tick calls return only records appended since the last call.
type TranscriptReader struct {
	path   string
	offset int64
}

func NewTranscriptReader(path string) *TranscriptReader {
	return &TranscriptReader{path: path}
}

// Tick reads records appended since the previous call. It returns the
// records, the file's mtime (zero if missing), and any read error. A
// missing file is not an error — callers tick early and often, and the
// transcript only appears once Claude writes its first record.
//
// Offset only advances past newline-terminated lines so a half-written
// final record is left for the next tick to consume in full.
func (r *TranscriptReader) Tick() ([]TranscriptRecord, time.Time, error) {
	f, err := os.Open(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, time.Time{}, nil
		}
		return nil, time.Time{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, time.Time{}, err
	}
	if info.Size() < r.offset {
		r.offset = 0
	}
	if _, err := f.Seek(r.offset, io.SeekStart); err != nil {
		return nil, info.ModTime(), err
	}
	reader := bufio.NewReaderSize(f, 1024*1024)
	var records []TranscriptRecord
	pos := r.offset
	for {
		line, readErr := reader.ReadString('\n')
		if readErr == nil {
			rec, jsonErr := unmarshalRecord([]byte(line[:len(line)-1]))
			if jsonErr == nil {
				records = append(records, rec)
			}
			pos += int64(len(line))
			continue
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		r.offset = pos
		return records, info.ModTime(), readErr
	}
	r.offset = pos
	return records, info.ModTime(), nil
}
