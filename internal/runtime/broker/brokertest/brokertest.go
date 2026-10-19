// Package brokertest holds the helper the broker's ask tests share: waiting
// for an agent message to reach a run's control queue.
package brokertest

import (
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
