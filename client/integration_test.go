package client_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
)

// TestSubscribeRuntimeWithoutExplicitSubscribeDeliversEvents pins issue #262:
// SubscribeRuntime must send the control server's subscribe method itself, so
// the connection lands in the server's subscriber set and receives published
// events without the caller having to call subscribe first.
func TestSubscribeRuntimeWithoutExplicitSubscribeDeliversEvents(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "ctrl.sock")

	srv := control.NewServer(control.NewState("run_test", "", 0))
	if err := srv.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer srv.Stop()

	c, err := client.Dial(socketPath)
	if err != nil {
		t.Fatalf("dial control socket: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	eventCh := c.SubscribeRuntime(ctx, "rt_1")

	srv.PublishEvent(events.Event{
		Event: "session.end",
		Fields: map[string]any{
			"runtime_id":  "rt_1",
			"stop_reason": "end_turn",
		},
	})

	select {
	case ev := <-eventCh:
		if ev.Event != "session.end" {
			t.Fatalf("event = %q, want session.end", ev.Event)
		}
		if ev.RuntimeID != "rt_1" {
			t.Fatalf("runtime_id = %q, want rt_1", ev.RuntimeID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session.end; subscribe was likely never sent")
	}
}
