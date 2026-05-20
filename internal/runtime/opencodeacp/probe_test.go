package opencodeacp

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

func TestProbeTranscriptVocabulary(t *testing.T) {
	path := filepath.Join("testdata", "probe-transcript.ndjson")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer file.Close()

	// Kinds observed in the live `avenor probe --dir /tmp` capture against
	// opencode 1.14.41. user.message_chunk and session.plan are reachable
	// per plan §3 but did not surface in this trivial probe. The replay
	// suite locks the kinds we did observe; the parser still accepts the
	// other documented kinds when they appear (see TestMapRawSessionUpdate).
	expected := map[string]bool{
		"agent.message_chunk": false,
		"agent.thought_chunk": false,
		"tool.call":           false,
		"tool.call_update":    false,
		"session.end":         false,
	}

	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		event, err := acp.ParseTranscriptEvent(scanner.Bytes())
		if err != nil {
			t.Fatalf("line %d: %v", lineNumber, err)
		}
		if _, ok := expected[event.Event]; !ok {
			t.Fatalf("line %d: unexpected event %q", lineNumber, event.Event)
		}
		expected[event.Event] = true
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan fixture: %v", err)
	}
	if lineNumber == 0 {
		t.Fatal("fixture is empty")
	}

	for event, seen := range expected {
		if !seen {
			t.Errorf("fixture missing %s", event)
		}
	}
}

func TestMapRawSessionUpdate(t *testing.T) {
	line := []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello"},"messageId":"m1"}}}`)
	event, err := acp.ParseTranscriptEvent(line)
	if err != nil {
		t.Fatalf("parse raw update: %v", err)
	}
	if event.Event != "agent.message_chunk" {
		t.Fatalf("event = %q, want agent.message_chunk", event.Event)
	}
	if event.SessionID != "s1" {
		t.Fatalf("session id = %q, want s1", event.SessionID)
	}
	if event.Fields["session_update"] != "agent_message_chunk" {
		t.Fatalf("session_update = %v, want agent_message_chunk", event.Fields["session_update"])
	}
}
