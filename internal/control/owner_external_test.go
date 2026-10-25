package control_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/control/controltest"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/token"
)

func externalSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(os.TempDir(), "avc-ext-"+time.Now().Format("150405.000000000")+".sock")
}

func TestSocketStartIdentityTokenIsOneShot(t *testing.T) {
	path := externalSocketPath(t)
	t.Setenv(token.EnvControlIdentityToken, "child-token")

	s, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	if got := os.Getenv(token.EnvControlIdentityToken); got != "" {
		t.Fatalf("identity token still in environment: %q", got)
	}
	cl, err := client.Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.Close()
	if token, err := cl.Identity(); err != nil || token != "child-token" {
		t.Fatalf("identity = %q, %v; want child-token, nil", token, err)
	}
}

func TestResultOverControlSocketReadsReplyBeyondScannerLimit(t *testing.T) {
	state := control.NewState("run_1", "demo", 0)
	s, _ := controltest.NewServer(state)
	path := externalSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	complete := strings.Repeat("é", 40_000) // 80 KiB UTF-8, beyond Scanner's 64 KiB default.
	s.PublishEvent(events.Event{Event: "session.end", Fields: map[string]any{"final_output": complete}})

	c, err := client.Dial(path)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	result, err := c.Result("")
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got, _ := result["final_output"].(string); got != complete {
		t.Fatalf("final_output byte length = %d, want %d", len(got), len(complete))
	}
}
