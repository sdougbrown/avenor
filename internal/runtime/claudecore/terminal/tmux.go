package terminal

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// TmuxLauncher implements Launcher by spawning tmux sessions.
type TmuxLauncher struct{}

func (TmuxLauncher) Start(ctx context.Context, opts StartOptions) (Session, error) {
	if opts.Cols == 0 {
		opts.Cols = 220
	}
	if opts.Rows == 0 {
		opts.Rows = 50
	}

	shellCmd := opts.Command
	if shellCmd == "" {
		return nil, fmt.Errorf("tmux launcher: Command is required")
	}

	args := []string{
		"new-session", "-d",
		"-s", opts.Name,
		"-c", opts.Dir,
		"-x", strconv.Itoa(opts.Cols),
		"-y", strconv.Itoa(opts.Rows),
		shellCmd,
	}

	if _, err := exec.CommandContext(ctx, "tmux", args...).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("tmux new-session: %w", err)
	}

	// Wait briefly for the pane PID to appear.
	var pid int
	deadline := time.After(2 * time.Second)
	for {
		pidOut, err := exec.CommandContext(ctx, "tmux", "list-panes", "-t", opts.Name, "-F", "#{pane_pid}").Output()
		if err == nil && len(bytes.TrimSpace(pidOut)) > 0 {
			if p, err := strconv.Atoi(strings.TrimSpace(string(pidOut))); err == nil {
				pid = p
				break
			}
		}
		select {
		case <-deadline:
			return &TmuxSession{name: opts.Name, pid: pid}, nil
		case <-time.After(50 * time.Millisecond):
		}
	}

	return &TmuxSession{name: opts.Name, pid: pid}, nil
}

// TmuxSession is a terminal.Session backed by a tmux session.
type TmuxSession struct {
	name string
	pid  int
}

func (s *TmuxSession) Kind() string { return "tmux" }

func (s *TmuxSession) Name() string { return s.name }

func (s *TmuxSession) PID() int { return s.pid }

func (s *TmuxSession) Capture(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "tmux", "capture-pane", "-t", s.name, "-p").Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func (s *TmuxSession) PasteAndEnter(ctx context.Context, text string) error {
	bufferName := "avenor-prompt-" + s.name
	cmd := exec.CommandContext(ctx, "tmux", "load-buffer", "-b", bufferName, "-")
	cmd.Stdin = strings.NewReader(text)
	if err := cmd.Run(); err != nil {
		return err
	}
	if err := exec.CommandContext(ctx, "tmux", "paste-buffer", "-t", s.name, "-b", bufferName).Run(); err != nil {
		return err
	}
	return exec.CommandContext(ctx, "tmux", "send-keys", "-t", s.name, string(KeyEnter)).Run()
}

func (s *TmuxSession) SendKeys(ctx context.Context, keys ...Key) error {
	args := []string{"send-keys", "-t", s.name}
	for _, k := range keys {
		args = append(args, string(k))
	}
	return exec.CommandContext(ctx, "tmux", args...).Run()
}

func (s *TmuxSession) Alive(ctx context.Context) bool {
	return exec.CommandContext(ctx, "tmux", "has-session", "-t", s.name).Run() == nil
}

func (s *TmuxSession) Kill(ctx context.Context) error {
	return exec.CommandContext(ctx, "tmux", "kill-session", "-t", s.name).Run()
}

var _ Session = (*TmuxSession)(nil)
