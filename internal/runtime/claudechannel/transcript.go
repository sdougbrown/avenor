package claudechannel

import (
	"bufio"
	"encoding/json"
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
func (r *transcriptReader) Tick() ([]transcriptRecord, time.Time, error) {
	info, err := os.Stat(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, time.Time{}, nil
		}
		return nil, time.Time{}, err
	}
	if info.Size() <= r.offset {
		return nil, info.ModTime(), nil
	}
	f, err := os.Open(r.path)
	if err != nil {
		return nil, info.ModTime(), err
	}
	defer f.Close()
	if _, err := f.Seek(r.offset, 0); err != nil {
		return nil, info.ModTime(), err
	}
	scanner := bufio.NewScanner(f)
	// Claude's records can carry large tool outputs inline.
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	var records []transcriptRecord
	for scanner.Scan() {
		var rec transcriptRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			// Skip a single malformed line rather than aborting the tick;
			// the next tick will retry from the new offset.
			continue
		}
		records = append(records, rec)
	}
	end, _ := f.Seek(0, 2)
	r.offset = end
	return records, info.ModTime(), scanner.Err()
}
