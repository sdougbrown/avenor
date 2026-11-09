package main

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
)

// A one-shot `avenor run --control-socket` server owns a single run. Awaiting
// that socket must resolve the run from its list entry and follow it to the
// terminal snapshot.
func TestAwaitAttachesToOneShotControlSocket(t *testing.T) {
	state := control.NewState("run_oneshot", "one-shot", 0)
	owner, err := control.GenerateOwnerCapability()
	if err != nil {
		t.Fatal(err)
	}
	server := control.NewServer(state, owner)
	socket := filepath.Join(t.TempDir(), "control.sock")
	if err := server.Start(socket); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	server.PublishEvent(events.Event{Event: "session.start", SessionID: "ses_1"})

	exit := make(chan int, 1)
	var stdout, stderr bytes.Buffer
	go func() {
		exit <- awaitRunDeclared([]string{"--socket", socket, "--until", "done", "--print-output", "one-shot"}, &stdout, &stderr)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for !server.HasClients() {
		if time.Now().After(deadline) {
			t.Fatal("await never attached to the control socket")
		}
		time.Sleep(10 * time.Millisecond)
	}

	server.PublishEvent(events.Event{Event: "session.end", SessionID: "ses_1", Fields: map[string]any{
		"stop_reason":  "end_turn",
		"exit_code":    0,
		"final_output": "ok",
	}})

	select {
	case code := <-exit:
		if code != 0 {
			t.Fatalf("await exit = %d, want 0; stderr: %s", code, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("await did not return after the run settled")
	}
	if got := stdout.String(); !bytes.Contains([]byte(got), []byte("ok")) {
		t.Fatalf("await output = %q, want the terminal reply", got)
	}
}
