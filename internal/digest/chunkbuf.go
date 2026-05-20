package digest

import "strings"

// maxChunkBuf is the maximum number of bytes to retain across chunk
// boundaries.  It must be large enough to fit any plausible marker plus
// surrounding text from the stream.  The longest realistic marker is well
// under 200 characters.
const maxChunkBuf = 512

// ChunkBuffer accumulates text from streaming chunks and provides a
// sliding window for marker extraction.  It prevents unbounded growth
// and deduplicates status markers so that the same status is not
// re-emitted on every subsequent chunk.
type ChunkBuffer struct {
	buf           strings.Builder
	maxLen        int
	lastStatusKey string
}

// NewChunkBuffer creates a buffer with the default maximum size.
func NewChunkBuffer() *ChunkBuffer {
	return NewChunkBufferLen(maxChunkBuf)
}

// NewChunkBufferLen creates a buffer with a custom maximum size.
func NewChunkBufferLen(maxLen int) *ChunkBuffer {
	if maxLen < 64 {
		maxLen = 64
	}
	return &ChunkBuffer{maxLen: maxLen}
}

// Append adds text to the buffer and trims the oldest bytes when the
// total exceeds maxLen.
func (cb *ChunkBuffer) Append(text string) {
	cb.buf.WriteString(text)
	if cb.buf.Len() > cb.maxLen {
		tail := cb.buf.String()[cb.buf.Len()-cb.maxLen:]
		cb.buf.Reset()
		cb.buf.WriteString(tail)
		cb.lastStatusKey = ""
	}
}

// Text returns the accumulated buffer contents. Used in unit tests for assertions.
func (cb *ChunkBuffer) Text() string {
	return cb.buf.String()
}

// ScanLoopMarker runs ExtractLoopMarker over the accumulated buffer.
// Note: unlike ScanStatusMarker, this does NOT deduplicate. In the
// WaitForSession loop the severity guard (abort > exit > continue)
// prevents incorrect behaviour even if a loop marker is re-found while
// still in the buffer.
func (cb *ChunkBuffer) ScanLoopMarker() (directive, label string, ok bool) {
	return ExtractLoopMarker(cb.buf.String())
}

// ScanStatusMarker runs ExtractStatusMarker over the accumulated buffer.
// It returns ok=false for a status marker that was already returned by
// a previous call, preventing duplicate emissions when the same text
// remains in the buffer across multiple chunks.
func (cb *ChunkBuffer) ScanStatusMarker() (phase, label string, ok bool) {
	phase, label, ok = ExtractStatusMarker(cb.buf.String())
	if !ok {
		return "", "", false
	}
	key := phase + "|" + label
	if key == cb.lastStatusKey {
		return "", "", false
	}
	cb.lastStatusKey = key
	return phase, label, ok
}
