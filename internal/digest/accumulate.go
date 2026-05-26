package digest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Accumulator buffers consecutive chunk events (agent.message_chunk,
// agent.thought_chunk, user.message_chunk) with the same session ID and event
// type, then flushes them as a single human-readable line when the event type
// or session changes.
type Accumulator struct {
	buffers map[string]*chunkBuf // key: "sessionID|eventType"
	order   []string             // insertion order for deterministic flushing
}

type chunkBuf struct {
	sessionID string
	eventType string
	text      strings.Builder
}

func NewAccumulator() *Accumulator {
	return &Accumulator{
		buffers: make(map[string]*chunkBuf),
	}
}

// Process ingests a raw event line. It returns zero or more formatted lines
// that the caller should write to output. For chunk events that match the
// current buffer key, nothing is returned (the text is accumulated). When a
// chunk event triggers a flush (different key), the flushed lines are returned
// but the current event's text is buffered for next time. For non-chunk events,
// all accumulated buffers are flushed plus the current event is formatted.
//
// Empty lines and events with no "event" field are silently ignored.
func (a *Accumulator) Process(raw []byte, classify bool) ([]string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, nil
	}

	var event map[string]any
	if err := json.Unmarshal(trimmed, &event); err != nil {
		return nil, err
	}

	name := stringField(event, "event")
	if name == "" {
		return nil, nil
	}

	// Non-chunk events: flush all accumulated buffers, then format and return
	// the current event as a normal line.
	if !isChunkEvent(name) {
		var lines []string
		lines = append(lines, a.flushAll(classify)...)
		line := digestEventLine(event, name)
		if line != "" {
			if classify {
				tag := Classify(event)
				line = tag + " " + line
			}
			lines = append(lines, line)
		}
		return lines, nil
	}

	// Chunk event: accumulate or flush-and-restart.
	sessionID := extractSessionID(event)
	text := chunkText(event)
	if text == "" {
		return nil, nil
	}

	key := sessionID + "|" + name

	// If we have buffers for a different key, flush them first.
	if len(a.buffers) > 0 {
		if _, exists := a.buffers[key]; !exists {
			lines := a.flushAll(classify)
			a.addChunk(key, sessionID, name, text)
			return lines, nil
		}
	}

	a.addChunk(key, sessionID, name, text)
	return nil, nil
}

// FlushRemaining returns formatted lines for any buffered chunks that haven't
// been flushed yet. Call this at EOF to avoid losing trailing accumulated text.
func (a *Accumulator) FlushRemaining(classify bool) []string {
	return a.flushAll(classify)
}

func (a *Accumulator) addChunk(key, sessionID, eventType, text string) {
	buf, exists := a.buffers[key]
	if !exists {
		buf = &chunkBuf{sessionID: sessionID, eventType: eventType}
		a.buffers[key] = buf
		a.order = append(a.order, key)
	}
	buf.text.WriteString(text)
}

func (a *Accumulator) flushAll(classify bool) []string {
	if len(a.buffers) == 0 {
		return nil
	}
	lines := make([]string, 0, len(a.order))
	for _, key := range a.order {
		buf := a.buffers[key]
		if buf == nil || buf.text.Len() == 0 {
			continue
		}
		line := formatAccumulatedLine(buf)
		if classify {
			synthetic := map[string]any{
				"event":      buf.eventType,
				"session_id": buf.sessionID,
				"content":    map[string]any{"text": buf.text.String()},
			}
			tag := Classify(synthetic)
			line = tag + " " + line
		}
		lines = append(lines, line)
	}
	// Clear after flushing.
	a.buffers = make(map[string]*chunkBuf)
	a.order = a.order[:0]
	return lines
}

func isChunkEvent(name string) bool {
	return name == "agent.thought_chunk" || name == "agent.message_chunk" || name == "user.message_chunk"
}

func extractSessionID(event map[string]any) string {
	sid := stringField(event, "session_id")
	if sid == "" {
		sid = stringField(event, "sessionID")
	}
	return sid
}

// digestEventLine formats a single non-chunk event the same way DigestLine
// does, reusing the already-parsed event map.
func digestEventLine(event map[string]any, name string) string {
	sessionID := extractSessionID(event)
	return fmt.Sprintf("EVENT %s %s %s", name, sessionID, clean(excerpt(event, name)))
}

// formatAccumulatedLine formats a buffered chunk as a single EVENT line.
// The accumulated text is not truncated (unlike per-chunk excerpts).
func formatAccumulatedLine(buf *chunkBuf) string {
	text := lineBreakWhitespace.ReplaceAllString(buf.text.String(), " ")
	text = strings.TrimSpace(text)
	return fmt.Sprintf("EVENT %s %s %s", buf.eventType, buf.sessionID, text)
}
