// Package tooltest holds the helpers the agent-facing tool server tests share:
// reading a JSON-RPC response line from a stdio server and waiting for an
// agent message to reach a broker run.
package tooltest

import (
	"bufio"
	"io"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// WaitForAgentMessage waits for the first agent message queued for runID and
// returns a copy of it.
func WaitForAgentMessage(t *testing.T, b *broker.Broker, runID string) broker.ControlMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if st := b.GetRun(runID); st != nil {
			st.Mu.Lock()
			for _, msg := range st.ControlQueue {
				if msg.Type == "agent_message" {
					queued := *msg
					st.Mu.Unlock()
					return queued
				}
			}
			st.Mu.Unlock()
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no agent message reached %s", runID)
	return broker.ControlMessage{}
}

// ReadResponse reads one JSON-RPC response line from a running server.
func ReadResponse(t *testing.T, stdout io.Reader) []byte {
	t.Helper()
	lines := make(chan []byte, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadBytes('\n')
		if err == nil {
			lines <- line
		}
		close(lines)
	}()
	select {
	case line := <-lines:
		return line
	case <-time.After(5 * time.Second):
		t.Fatal("no JSON-RPC response")
		return nil
	}
}
