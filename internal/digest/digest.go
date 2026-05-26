package digest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"
)

const (
	defaultFormat       = "plain"
	defaultPollInterval = 250 * time.Millisecond
	maxExcerptRunes     = 120

	// cursorRewriteInterval is how many events are processed between periodic
	// cursor rewrites in follow mode, so a crash loses at most this many events
	// of progress.
	cursorRewriteInterval = 10
)

var lineBreakWhitespace = regexp.MustCompile(`[\r\n\t]+`)

type Options struct {
	Follow       bool
	PollInterval time.Duration
	Format       string
	// Classify, when true, prefixes each plain-format line with a tag:
	// MILESTONE, FINDING, or ACTIVITY. For JSON format, a top-level
	// "classify" field is injected into each emitted object.
	Classify bool
	// Accumulate, when true in plain format, buffers consecutive chunk
	// events (thought/message chunks) with the same session ID and event
	// type, flushing them as a single human-readable line when the event
	// type or session changes. Ignored in json format.
	Accumulate bool

	// CursorPath, when non-empty, causes Stream to atomically rewrite the
	// cursor file after processing. The caller is responsible for seeking the
	// underlying reader to the offset recorded in the cursor file before
	// calling Stream; CursorStartOffset must be set to that same offset so
	// Stream can compute the absolute file position when writing the cursor.
	//
	// Design note: we keep Stream's signature as io.Reader (rather than
	// *os.File) so tests can pass bytes.Buffer or strings.Reader without
	// touching the filesystem. The caller in watch.go handles the actual
	// os.File seek; we track bytes consumed here via a counting wrapper.
	CursorPath        string
	CursorStartOffset int64
}

// countingReader wraps an io.Reader and counts total bytes read.
type countingReader struct {
	r     io.Reader
	total int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.total += int64(n)
	return n, err
}

// writeCursor atomically rewrites the cursor file at path with offset.
// It writes to path+".tmp" then renames into place.
func writeCursor(path string, offset int64) error {
	tmp := path + ".tmp"
	content := fmt.Sprintf("%d\n", offset)
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		// Clean up the temp file if it was partially written.
		_ = os.Remove(tmp)
		return fmt.Errorf("write cursor tmp %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename cursor %s -> %s: %w", tmp, path, err)
	}
	return nil
}

// DigestLine parses a single raw JSON line and returns the formatted plain-text
// line, the parsed event map, and any error.
//
// When the JSON has no "event" field the line should be skipped: the returned
// string is empty and event is nil (no error).
// Callers that only need the string can ignore the map; callers that also need
// classification (e.g. streamLine) can pass the returned map directly to
// Classify without re-parsing the bytes.
func DigestLine(raw []byte) (line string, event map[string]any, err error) {
	if err = json.Unmarshal(bytes.TrimSpace(raw), &event); err != nil {
		return "", nil, err
	}

	name := stringField(event, "event")
	if name == "" {
		return "", nil, nil
	}

	sessionID := stringField(event, "session_id")
	if sessionID == "" {
		sessionID = stringField(event, "sessionID")
	}

	return fmt.Sprintf("EVENT %s %s %s", name, sessionID, clean(excerpt(event, name))), event, nil
}

func Stream(in io.Reader, out io.Writer, opts Options) error {
	format := opts.Format
	if format == "" {
		format = defaultFormat
	}
	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}

	// Wrap the reader with a byte counter so we can track our position in the
	// file and update the cursor without needing a seekable *os.File here.
	cr := &countingReader{r: in}
	reader := bufio.NewReader(cr)

	// currentOffset returns the absolute file position (start offset + bytes
	// consumed so far, minus any buffered-but-not-yet-processed bytes in the
	// bufio layer). bufio.Reader may have read ahead; subtract the buffered
	// amount to get the position of the last fully-delivered byte.
	currentOffset := func() int64 {
		buffered := int64(reader.Buffered())
		return opts.CursorStartOffset + cr.total - buffered
	}

	writeCurrentCursor := func() error {
		if opts.CursorPath == "" {
			return nil
		}
		return writeCursor(opts.CursorPath, currentOffset())
	}

	var acc *Accumulator
	useAccumulator := opts.Accumulate && format == "plain"
	if opts.Accumulate && format != "plain" {
		fmt.Fprintf(os.Stderr, "avenor watch: --accumulate is only supported with --format plain; ignoring\n")
	}
	if useAccumulator {
		acc = NewAccumulator()
	}

	lineNumber := 0
	eventsSinceCursorWrite := 0
	for {
		raw, err := reader.ReadBytes('\n')
		if len(raw) > 0 {
			lineNumber++
			if useAccumulator {
				lines, processErr := acc.Process(raw, opts.Classify)
				if processErr != nil {
					logMalformed(lineNumber, processErr)
				} else {
					for _, line := range lines {
						if _, writeErr := fmt.Fprintln(out, line); writeErr != nil {
							return writeErr
						}
					}
				}
			} else {
				if streamErr := streamLine(raw, lineNumber, out, format, opts.Classify); streamErr != nil {
					return streamErr
				}
			}
			eventsSinceCursorWrite++
			// In follow mode, rewrite the cursor periodically so a crash only
			// loses at most cursorRewriteInterval events of progress.
			if opts.Follow && opts.CursorPath != "" && eventsSinceCursorWrite >= cursorRewriteInterval {
				if cursorErr := writeCurrentCursor(); cursorErr != nil {
					return cursorErr
				}
				eventsSinceCursorWrite = 0
			}
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			if opts.Follow {
				time.Sleep(pollInterval)
				continue
			}
			// Oneshot mode: flush accumulated chunks, then write cursor on clean EOF.
			if acc != nil {
				for _, line := range acc.FlushRemaining(opts.Classify) {
					if _, writeErr := fmt.Fprintln(out, line); writeErr != nil {
						return writeErr
					}
				}
			}
			return writeCurrentCursor()
		}
		if errors.Is(err, os.ErrClosed) {
			// Graceful shutdown (file closed by signal handler): flush accumulated
			// chunks, then write cursor.
			if acc != nil {
				for _, line := range acc.FlushRemaining(opts.Classify) {
					if _, writeErr := fmt.Fprintln(out, line); writeErr != nil {
						return writeErr
					}
				}
			}
			return writeCurrentCursor()
		}
		return err
	}
}

func streamLine(raw []byte, lineNumber int, out io.Writer, format string, classify bool) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	if format == "json" {
		if err := validateJSON(raw); err != nil {
			logMalformed(lineNumber, err)
			return nil
		}
		if classify {
			return writeJSONWithClassify(raw, out)
		}
		_, err := out.Write(raw)
		return err
	}

	line, event, err := DigestLine(raw)
	if err != nil {
		logMalformed(lineNumber, err)
		return nil
	}
	if line == "" {
		return nil
	}
	if classify {
		tag := Classify(event)
		_, err = fmt.Fprintln(out, tag+" "+line)
		return err
	}
	_, err = fmt.Fprintln(out, line)
	return err
}

// writeJSONWithClassify parses raw, injects a top-level "classify" field, then
// writes the modified object followed by a newline. The original fields are
// preserved; only "classify" is added (or overwritten if already present).
func writeJSONWithClassify(raw []byte, out io.Writer) error {
	var event map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(raw), &event); err != nil {
		// Should not happen — caller already validated; fall back to raw.
		_, writeErr := out.Write(raw)
		return writeErr
	}
	event["classify"] = Classify(event)
	encoded, err := json.Marshal(event)
	if err != nil {
		// Encoding failure is unexpected; fall back to raw bytes.
		_, writeErr := out.Write(raw)
		return writeErr
	}
	encoded = append(encoded, '\n')
	_, err = out.Write(encoded)
	return err
}

func validateJSON(raw []byte) error {
	var value any
	return json.Unmarshal(bytes.TrimSpace(raw), &value)
}

func logMalformed(lineNumber int, err error) {
	fmt.Fprintf(os.Stderr, "avenor watch: malformed event at line %d: %v\n", lineNumber, err)
}

// chunkText extracts the human-readable text from a chunk event (message or
// thought), handling both ACP format (content.text) and SSE format (delta).
func chunkText(event map[string]any) string {
	if text := contentText(event["content"]); text != "" {
		return text
	}
	return stringField(event, "delta")
}

func excerpt(event map[string]any, name string) string {
	switch name {
	case "agent.message_chunk", "agent.thought_chunk", "user.message_chunk":
		return chunkText(event)
	case "agent.status":
		phase := stringField(event, "phase")
		if label := stringField(event, "label"); label != "" {
			return phase + " | " + label
		}
		return phase
	case "tool.call", "tool.call_update":
		return toolSummary(event)
	case "permission.request":
		if question := stringField(event, "question"); question != "" {
			return question
		}
		return stringField(event, "tool")
	case "session.plan":
		if label := stringField(event, "label"); label != "" {
			return label
		}
		return stringField(event, "title")
	case "session.end":
		return "stop_reason=" + stringField(event, "stop_reason")
	case "avenor.loop.start":
		return fmt.Sprintf("loop start, max_iterations=%v", event["max_iterations"])
	case "avenor.phase.start":
		return fmt.Sprintf("phase: %s (iter %v)", stringField(event, "phase"), event["iteration"])
	case "avenor.phase.end":
		return fmt.Sprintf("phase: %s → %s", stringField(event, "phase"), stringField(event, "stop_reason"))
	case "avenor.loop.end":
		return fmt.Sprintf("loop end: %s", stringField(event, "exit_reason"))
	case "avenor.retry":
		if event["attempt"] == nil {
			return "retrying"
		}
		return fmt.Sprintf("attempt=%v/%v", event["attempt"], event["max_retries"])
	case "avenor.error":
		if msg := stringField(event, "message"); msg != "" {
			return stringField(event, "source") + ": " + msg
		}
		return stringField(event, "source")
	default:
		return ""
	}
}

func contentText(value any) string {
	content, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	return stringValue(content["text"])
}

func toolSummary(event map[string]any) string {
	kind := stringField(event, "kind")
	title := stringField(event, "title")
	status := stringField(event, "status")

	base := ""
	switch {
	case kind != "" && title != "":
		base = kind + ":" + title
	case kind != "":
		base = kind
	case title != "":
		base = title
	}
	if status != "" {
		return base + " [" + status + "]"
	}
	return base
}

func stringField(event map[string]any, key string) string {
	return stringValue(event[key])
}

func stringValue(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return text
}

func clean(text string) string {
	text = lineBreakWhitespace.ReplaceAllString(text, " ")
	runes := []rune(text)
	if len(runes) > maxExcerptRunes {
		text = string(runes[:maxExcerptRunes])
	}
	return strings.TrimRightFunc(text, unicode.IsSpace)
}
