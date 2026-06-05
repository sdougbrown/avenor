package claudechannel

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

func transcriptPath(home, dir, sessionID string) string {
	return filepath.Join(home, ".claude", "projects", encodeProjectPath(dir), sessionID+".jsonl")
}

// transcriptRecord is the minimal subset of JSONL fields used for status
// detection. Records are heterogeneous; unknown fields are ignored.
type transcriptRecord struct {
	Type      string `json:"type"`
	AITitle   string `json:"aiTitle,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

// transcriptReader incrementally reads new records from a JSONL transcript.
// Successive Tick calls return only records appended since the last call.
type transcriptReader struct {
	path   string
	offset int64
}

func newTranscriptReader(path string) *transcriptReader {
	return &transcriptReader{path: path}
}

// Tick reads records appended since the previous call. It returns the
// records, the file's mtime (zero if missing), and any read error. A
// missing file is not an error — callers tick early and often, and the
// transcript only appears once Claude writes its first record.
//
// Offset only advances past newline-terminated lines so a half-written
// final record is left for the next tick to consume in full.
func (r *transcriptReader) Tick() ([]transcriptRecord, time.Time, error) {
	info, err := os.Stat(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, time.Time{}, nil
		}
		return nil, time.Time{}, err
	}
	if info.Size() < r.offset {
		// File was truncated/rotated — re-read from the start.
		r.offset = 0
	}
	if info.Size() == r.offset {
		return nil, info.ModTime(), nil
	}
	f, err := os.Open(r.path)
	if err != nil {
		return nil, info.ModTime(), err
	}
	defer f.Close()
	if _, err := f.Seek(r.offset, io.SeekStart); err != nil {
		return nil, info.ModTime(), err
	}
	// Claude's records can carry large tool outputs inline; use a generous buffer.
	reader := bufio.NewReaderSize(f, 1024*1024)
	var records []transcriptRecord
	pos := r.offset
	for {
		line, readErr := reader.ReadString('\n')
		if readErr == nil {
			// Complete \n-terminated record.
			var rec transcriptRecord
			if jsonErr := json.Unmarshal([]byte(line[:len(line)-1]), &rec); jsonErr == nil {
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
