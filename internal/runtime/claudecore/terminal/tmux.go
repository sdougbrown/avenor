package terminal

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TmuxLauncher implements Launcher by spawning tmux sessions.
type TmuxLauncher struct{}

// tmuxNewSessionArgs renders the new-session argv. It prefixes the command with
// UnsetParentClaudeEnvPrefix and the launch identity exports rather than setting
// the child's environment. A pane inherits the tmux server's environment, not
// avenor's, so the command itself is the only place a leaked variable can be
// removed and a new value can be added.
//
// What the launch provisions for the child (envFile's contents) is sourced from
// a mode-0600 file instead of exported here: this argv is world-readable through
// ps and /proc/<pid>/cmdline, so a credential in it is a credential handed to
// every local process.
func tmuxNewSessionArgs(opts StartOptions, envFile string) []string {
	return []string{
		"new-session", "-d",
		"-s", opts.Name,
		"-c", opts.Dir,
		"-x", strconv.Itoa(opts.Cols),
		"-y", strconv.Itoa(opts.Rows),
		UnsetParentClaudeEnvPrefix() + identityEnvPrefix(opts.Provenance, envFile) + opts.Command,
	}
}

// writeLaunchEnvFile writes this launch's environment to a file only this user
// can read: a fresh mode-0700 directory under TMPDIR, holding one mode-0600
// file. It reports "" when there is nothing to provision, so a launch with no
// caller environment writes no file at all.
func writeLaunchEnvFile(env []string) (string, error) {
	source := launchEnvSource(env)
	if source == "" {
		return "", nil
	}
	dir, err := os.MkdirTemp("", "avenor-launch-env-")
	if err != nil {
		return "", fmt.Errorf("launch env dir: %w", err)
	}
	path := filepath.Join(dir, "env")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("launch env file: %w", err)
	}
	return path, nil
}

func (TmuxLauncher) Start(ctx context.Context, opts StartOptions) (Session, error) {
	if opts.Cols == 0 {
		opts.Cols = 220
	}
	if opts.Rows == 0 {
		opts.Rows = 50
	}

	if opts.Command == "" {
		return nil, fmt.Errorf("tmux launcher: Command is required")
	}

	envFile, err := writeLaunchEnvFile(opts.Env)
	if err != nil {
		return nil, err
	}
	// The file lives exactly as long as the pane: every path out of Start either
	// hands it to the session that will remove it on teardown, or removes it here.
	session := &TmuxSession{name: opts.Name, envFile: envFile}
	if _, err := exec.CommandContext(ctx, "tmux", tmuxNewSessionArgs(opts, envFile)...).CombinedOutput(); err != nil {
		session.removeEnvFile()
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
			session.pid = pid
			return session, nil
		case <-time.After(50 * time.Millisecond):
		}
	}

	session.pid = pid
	return session, nil
}

// TmuxSession is a terminal.Session backed by a tmux session.
type TmuxSession struct {
	name string
	pid  int
	// envFile is the sourced environment this launch created, removed with the
	// session so a launch's credentials do not outlive it.
	envFile string
	// envOnce keeps the removal single whichever teardown path runs first.
	envOnce sync.Once
}

func (s *TmuxSession) removeEnvFile() {
	s.envOnce.Do(func() {
		if s.envFile == "" {
			return
		}
		_ = os.RemoveAll(filepath.Dir(s.envFile))
	})
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

// PasteAndEnter pastes the sanitised text as a bracketed paste, then sends
// Enter. The buffer carries its own markers. paste-buffer -p adds them only
// once the pane has enabled bracketed paste, and an unframed multi-line prompt
// is submitted line by line. See bracketedPaste.
//
// tmux 3.7 and later pass a pasted buffer through vis(3), which turns the
// markers into literal text; -S pastes the bytes unchanged. Older tmux has no
// -S and pastes bytes unchanged. When tmux rejects the flag, the paste is
// retried without it.
func (s *TmuxSession) PasteAndEnter(ctx context.Context, text string) error {
	bufferName := "avenor-prompt-" + s.name
	cmd := exec.CommandContext(ctx, "tmux", "load-buffer", "-b", bufferName, "-")
	cmd.Stdin = bytes.NewReader(bracketedPaste(text))
	if err := cmd.Run(); err != nil {
		return err
	}
	if out, err := exec.CommandContext(ctx, "tmux", "paste-buffer", "-S", "-t", s.name, "-b", bufferName).CombinedOutput(); err != nil {
		if !strings.Contains(string(out), "unknown flag") && !strings.Contains(string(out), "usage:") {
			return fmt.Errorf("tmux paste-buffer: %w: %s", err, strings.TrimSpace(string(out)))
		}
		if err := exec.CommandContext(ctx, "tmux", "paste-buffer", "-t", s.name, "-b", bufferName).Run(); err != nil {
			return err
		}
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
	err := exec.CommandContext(ctx, "tmux", "kill-session", "-t", s.name).Run()
	s.removeEnvFile()
	return err
}

// Wait polls tmux session ownership rather than terminal contents. tmux owns
// its pane child, so there is no child process for this package to reap. The
// pane is gone when it returns, so the launch's environment file goes with it.
func (s *TmuxSession) Wait(ctx context.Context) error {
	defer s.removeEnvFile()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !s.Alive(ctx) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

var _ Session = (*TmuxSession)(nil)
