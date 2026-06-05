//go:build linux || darwin || freebsd
// +build linux darwin freebsd

package terminal

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// ProbeResult records the outcome of a single PTY probe variant.
type ProbeResult struct {
	Name             string
	Err              error
	ProcessStarted   bool
	FirstCapture     string
	Alive10s         bool
	DevChannelPrompt bool
	MCPFound         bool
	PromptSubmitted  bool
	ChannelReg       bool
}

// RunProbeMatrix runs the probe matrix against the PTY launcher.
// Returns results for each variant and a summary.
func RunProbeMatrix(t *testing.T, command string) map[string]ProbeResult {
	// The command must be something that starts and stays alive for at least 10s.
	if command == "" {
		command = "sh -c 'echo ready && sleep 60'"
	}

	variants := []struct {
		name string
		env  []string
		size [2]int
	}{
		{"A-xterm", []string{"TERM=xterm-256color"}, [2]int{220, 50}},
		{"B-tmux", []string{"TERM=tmux-256color"}, [2]int{220, 50}},
		{"C-screen", []string{"TERM=screen-256color"}, [2]int{220, 50}},
		{"D-small", []string{"TERM=tmux-256color"}, [2]int{120, 30}},
		{"E-bracketed", []string{"TERM=tmux-256color", "BRACKETED_PASTE=1"}, [2]int{220, 50}},
	}

	results := make(map[string]ProbeResult)
	ctx := context.Background()

	for _, v := range variants {
		res := ProbeResult{Name: v.name}

		launcher := PTYLauncher{Env: append(os.Environ(), v.env...)}
		opts := StartOptions{
			Name:    "probe-" + v.name,
			Dir:     "",
			Cols:    v.size[0],
			Rows:    v.size[1],
			Command: command,
		}

		sess, err := launcher.Start(ctx, opts)
		if err != nil {
			res.Err = fmt.Errorf("start: %w", err)
			results[v.name] = res
			continue
		}
		res.ProcessStarted = true
		cleanup := func() { _ = sess.Kill(ctx) }

		// Give it a moment to initialize.
		time.Sleep(500 * time.Millisecond)

		// Capture screen.
		capture, err := sess.Capture(ctx)
		if err != nil {
			res.Err = fmt.Errorf("capture: %w", err)
			cleanup()
			results[v.name] = res
			continue
		}
		res.FirstCapture = capture

		// Check if alive after 10s.
		alive10s := false
		select {
		case <-time.After(10 * time.Second):
			alive10s = sess.Alive(ctx)
		case <-ctx.Done():
		}
		res.Alive10s = alive10s

		// Check for development channel prompt.
		if strings.Contains(capture, "Loading development channels") {
			res.DevChannelPrompt = true
		}

		// Check for MCP found.
		if strings.Contains(capture, "New MCP server found in this project") {
			res.MCPFound = true
		}

		// Try submitting a prompt.
		if err := sess.PasteAndEnter(ctx, "hello"); err == nil {
			res.PromptSubmitted = true
		}

		cleanup()
		results[v.name] = res
	}

	return results
}
