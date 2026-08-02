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
			want: []string{"--session-id", "session", "--agent", "agent", "--name", "label", "--model", "model", "--effort", "high", "--permission-mode", "default"},
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
	tests := []struct {
		name      string
		backend   string
		effort    string
		output    []byte
		helpErr   error
		wantCalls int
		wantErr   bool
	}{
		{
			name:      "empty effort skips probe",
			backend:   "claude",
			output:    []byte("usage: claude"),
			wantCalls: 0,
		},
		{
			name:      "unsupported output",
			backend:   "claude",
			effort:    "high",
			output:    []byte("usage: claude"),
			wantCalls: 1,
			wantErr:   true,
		},
		{
			name:      "failed help",
			backend:   "claude",
			effort:    "high",
			helpErr:   errors.New("help failed"),
			wantCalls: 1,
			wantErr:   true,
		},
		{
			name:      "supported output",
			backend:   "claude-channel",
			effort:    "max",
			output:    []byte("--effort <level>"),
			wantCalls: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			calls := 0
			err := checkEffortCapability(context.Background(), tt.backend, tt.effort, func(context.Context) ([]byte, error) {
				calls++
				return tt.output, tt.helpErr
			})
			if calls != tt.wantCalls {
				t.Fatalf("help calls = %d, want %d", calls, tt.wantCalls)
			}
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), tt.backend) || !strings.Contains(err.Error(), "thinking") {
					t.Fatalf("capability error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("capability error = %v", err)
			}
		})
	}
}
