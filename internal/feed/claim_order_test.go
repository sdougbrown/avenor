package feed

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// claimCheckingWriter verifies the claim exists when the feed writes a line.
type claimCheckingWriter struct {
	dir    string
	result chan error
}

// Write observes the record at the output boundary.
func (w claimCheckingWriter) Write(p []byte) (int, error) {
	var line struct {
		ID string `json:"id"`
	}
	err := json.Unmarshal(p, &line)
	if err == nil {
		_, err = os.Stat(filepath.Join(w.dir, line.ID+".json"))
	}
	select {
	case w.result <- err:
	default:
	}
	return len(p), nil
}

// TestAskRecordPrecedesOutput observes the handoff at the writer boundary.
func TestAskRecordPrecedesOutput(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	token, _, err := b.EnsureRun("receiver", "receiver")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.EnsureRun("sender", "sender"); err != nil {
		t.Fatal(err)
	}
	if err := b.SendTo("sender", "receiver", "agent_message", json.RawMessage(`{"id":"ask","message":"question","expects_reply":true}`), "ask"); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "asks")
	result := make(chan error, 1)
	cancel, done := startFeed(t, Broker{RunID: "receiver", Token: token, BrokerURL: b.URL(), LockDir: t.TempDir(), AsksDir: dir, Out: claimCheckingWriter{dir: dir, result: result}, RequestTimeout: time.Second})
	select {
	case err := <-result:
		if err != nil {
			cancel()
			awaitExit(t, done)
			t.Fatalf("claim missing at write: %v", err)
		}
	case <-time.After(3 * time.Second):
		cancel()
		awaitExit(t, done)
		t.Fatal("no output")
	}
	cancel()
	awaitExit(t, done)
}
