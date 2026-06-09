package claudecore

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"
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

// TranscriptRecord is the minimal subset of JSONL fields used for status
// detection. Records are heterogeneous; unknown fields are ignored.
type TranscriptRecord struct {
	Type       string `json:"type"`
	AITitle    string `json:"aiTitle,omitempty"`
	Timestamp  string `json:"timestamp,omitempty"`
	StopReason string `json:"stop_reason,omitempty"`
}

// unmarshalRecord decodes a JSONL line into a TranscriptRecord, lifting
// stop_reason from the nested message object on assistant records.
func unmarshalRecord(raw []byte) (TranscriptRecord, error) {
	// Parse the full object into a raw map first so we can extract both
	// top-level and nested fields in a single pass.
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawMap); err != nil {
		return TranscriptRecord{}, err
	}
	rec := TranscriptRecord{}
	if t, ok := rawMap["type"]; ok {
		json.Unmarshal(t, &rec.Type)
	}
	if t, ok := rawMap["aiTitle"]; ok {
		json.Unmarshal(t, &rec.AITitle)
	}
	if t, ok := rawMap["timestamp"]; ok {
		json.Unmarshal(t, &rec.Timestamp)
	}
	// stop_reason lives inside message.stop_reason on assistant records;
	// also check top-level as a fallback for records that flatten the field.
	if rec.Type == "assistant" {
		if msg, ok := rawMap["message"]; ok {
			var m struct {
				StopReason string `json:"stop_reason,omitempty"`
			}
			if err := json.Unmarshal(msg, &m); err == nil && m.StopReason != "" {
				rec.StopReason = m.StopReason
			}
		}
	}
	if rec.StopReason == "" {
		if sr, ok := rawMap["stop_reason"]; ok {
			json.Unmarshal(sr, &rec.StopReason)
		}
	}
	return rec, nil
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
		// File was truncated/rotated — re-read from the start.
		r.offset = 0
	}
	if _, err := f.Seek(r.offset, io.SeekStart); err != nil {
		return nil, info.ModTime(), err
	}
	// Claude's records can carry large tool outputs inline; use a generous buffer.
	reader := bufio.NewReaderSize(f, 1024*1024)
	var records []TranscriptRecord
	pos := r.offset
	for {
		line, readErr := reader.ReadString('\n')
		if readErr == nil {
			// Complete \n-terminated record.
			rec, jsonErr := unmarshalRecord([]byte(line[:len(line)-1]))
			if jsonErr == nil {
				records = append(records, rec)
			}
			// Malformed lines are skipped but still consumed.
			pos += int64(len(line))
			continue
		}
		if errors.Is(readErr, io.EOF) {
			// Any leftover bytes are a partial line we leave for next tick.
			break
		}
		r.offset = pos
		return records, info.ModTime(), readErr
	}
	r.offset = pos
	return records, info.ModTime(), nil
}
