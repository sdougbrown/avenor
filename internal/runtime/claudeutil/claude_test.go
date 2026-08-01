package claudeutil

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestBuildArgsPreservesBackendSpecificOrdering(t *testing.T) {
	opts := runtime.StartOptions{Agent: "agent", Label: "label", Model: "model", Thinking: "high"}
	tests := []struct {
		name       string
		serverName string
		opts       runtime.StartOptions
		want       []string
	}{
		{
			name: "normal with options",
			opts: opts,
			want: []string{"--session-id", "session", "--permission-mode", "default", "--agent", "agent", "--name", "label", "--model", "model", "--effort", "high"},
		},
		{
			name:       "channel with options",
			serverName: "server",
			opts:       opts,
			want:       []string{"--dangerously-load-development-channels", "server:server", "--session-id", "session", "--agent", "agent", "--name", "label", "--model", "model", "--effort", "high", "--permission-mode", "default"},
		},
		{
			name: "normal without options",
			want: []string{"--session-id", "session", "--permission-mode", "default"},
		},
		{
			name:       "channel without options",
			serverName: "server",
			want:       []string{"--dangerously-load-development-channels", "server:server", "--session-id", "session", "--permission-mode", "default"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BuildArgs("session", tt.serverName, tt.opts); !slices.Equal(got, tt.want) {
				t.Fatalf("args = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCheckEffortCapability(t *testing.T) {
	original := helpOutput
	t.Cleanup(func() { helpOutput = original })

	calls := 0
	helpOutput = func(context.Context) ([]byte, error) {
		calls++
		return []byte("usage: claude"), nil
	}
	if err := CheckEffortCapability(context.Background(), "claude", ""); err != nil {
		t.Fatalf("empty effort error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("help calls for empty effort = %d", calls)
	}

	err := CheckEffortCapability(context.Background(), "claude", "high")
	if err == nil || !strings.Contains(err.Error(), "claude") || !strings.Contains(err.Error(), "thinking") {
		t.Fatalf("unsupported capability error = %v", err)
	}

	helpOutput = func(context.Context) ([]byte, error) { return nil, errors.New("help failed") }
	if err := CheckEffortCapability(context.Background(), "claude", "high"); err == nil || !strings.Contains(err.Error(), "thinking") {
		t.Fatalf("failed help error = %v", err)
	}

	helpOutput = func(context.Context) ([]byte, error) { return []byte("--effort <level>"), nil }
	if err := CheckEffortCapability(context.Background(), "claude-channel", "max"); err != nil {
		t.Fatalf("supported capability error = %v", err)
	}
}
