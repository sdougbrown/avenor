package mcpserver

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/client"
)

var execCommand = exec.Command

var startupTimeout = 10 * time.Second

type supervisorLifecycle struct {
	socketPath string
	cmd        *exec.Cmd
	client     ControlClient
	exited     <-chan error
}

func startSupervisor(socketPath string, idleTimeout time.Duration) (*supervisorLifecycle, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("get home directory: %w", err)
	}

	socketsDir := filepath.Join(home, ".avenor", "sockets")
	if err := os.MkdirAll(socketsDir, 0700); err != nil {
		return nil, fmt.Errorf("create sockets directory: %w", err)
	}

	if socketPath == "" {
		socketPath = filepath.Join(socketsDir, fmt.Sprintf("avenor-mcp-%d.sock", os.Getpid()))
	}

	if _, statErr := os.Stat(socketPath); statErr == nil {
		cl, err := client.Dial(socketPath)
		if err == nil {
			_, statusErr := cl.Status("")
			if statusErr == nil {
				return &supervisorLifecycle{
					socketPath: socketPath,
					client:     cl,
				}, nil
			}
			cl.Close()
		}
		if err := os.Remove(socketPath); err != nil {
			return nil, fmt.Errorf("remove stale socket: %w", err)
		}
	}

	exePath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("get executable path: %w", err)
	}

	args := []string{"stable", "--control-socket", socketPath}
	if idleTimeout > 0 {
		args = append(args, "--idle-timeout", idleTimeout.String())
	}

	cmd := execCommand(exePath, args...)
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start supervisor: %w", err)
	}

	exited := make(chan error, 1)
	go func() {
		exited <- cmd.Wait()
	}()

	var cl *client.Client
	backoff := 50 * time.Millisecond
	deadline := time.After(startupTimeout)

	for {
		select {
		case <-exited:
			os.Remove(socketPath)
			return nil, fmt.Errorf("supervisor exited during startup")
		case <-deadline:
			cmd.Process.Kill()
			<-exited
			os.Remove(socketPath)
			return nil, fmt.Errorf("supervisor startup timed out after %s", startupTimeout)
		default:
		}

		var dialErr error
		cl, dialErr = client.Dial(socketPath)
		if dialErr == nil {
			_, statusErr := cl.Status("")
			if statusErr == nil {
				return &supervisorLifecycle{
					socketPath: socketPath,
					cmd:        cmd,
					client:     cl,
					exited:     exited,
				}, nil
			}
			cl.Close()
			cl = nil
		}

		time.Sleep(backoff)
		backoff *= 2
		if backoff > 500*time.Millisecond {
			backoff = 500 * time.Millisecond
		}
	}
}

func (l *supervisorLifecycle) Shutdown() error {
	return l.ShutdownWithMode("graceful")
}

func (l *supervisorLifecycle) ShutdownWithMode(mode string) error {
	var firstErr error

	if l.client != nil {
		// Best-effort graceful shutdown; if the process already exited
		// the socket write will fail, which is fine.
		_ = l.client.Shutdown(mode)
		l.client.Close()
		l.client = nil
	}

	if l.cmd != nil && l.cmd.Process != nil {
		select {
		case <-l.exited:
		case <-time.After(3 * time.Second):
			if pgid, err := syscall.Getpgid(l.cmd.Process.Pid); err == nil {
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			} else {
				_ = l.cmd.Process.Kill()
			}
			<-l.exited
		}
	}

	if l.socketPath != "" {
		if err := os.Remove(l.socketPath); err != nil && !os.IsNotExist(err) {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}
