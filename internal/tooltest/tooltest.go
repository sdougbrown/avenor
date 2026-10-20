// Package tooltest holds the helper the agent-facing tool server tests share:
// reading a JSON-RPC response line from a stdio server.
package tooltest

import (
	"bufio"
	"io"
	"testing"
	"time"
)

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
