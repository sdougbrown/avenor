package mcpserver

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/client"
)

var execCommand = exec.Command

var startupTimeout = 10 * time.Second

// afterSupervisorReady is a no-op production hook used to coordinate the
// post-readiness socket-replacement test before its first identity probe.
var afterSupervisorReady = func() {}

const controlIdentityTokenEnv = "AVENOR_CONTROL_IDENTITY_TOKEN"

func newControlIdentityToken() (string, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate control identity token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

type supervisorLifecycle struct {
	mu         sync.Mutex
	socketPath string
	cmd        *exec.Cmd
	client     ControlClient
	exited     <-chan error
	waited     bool

	// shared means this lifecycle attached to an already-running supervisor.
	// Closing an attached MCP server may not shut down or unlink the
	// supervisor owned by another process; an explicit shutdown tool still
	// sends the shutdown RPC.
	shared bool
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

	// The lock covers the health check/startup sequence, not the lifetime of
	// the supervisor. A waiter must be able to acquire it after startup and
	// attach to the winner instead of blocking until the winner shuts down.
	lockPath := socketPath + ".start.lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open supervisor startup lock: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("lock supervisor startup: %w", err)
	}
	defer func() {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
	}()

	if cl, ok := dialHealthySupervisor(socketPath); ok {
		return &supervisorLifecycle{
			socketPath: socketPath,
			client:     cl,
			shared:     true,
		}, nil
	}

	// Stale-path removal belongs to ControlServer.Start, which serializes it
	// with bind and Stop under the per-socket control lock. The startup lock
	// only elects an MCP lifecycle owner and must not mutate the socket path.

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

	// A healthy socket alone does not prove this child owns it: an independent
	// stable process can bind between the preflight check above and cmd.Start.
	// Have ControlServer.Start acknowledge its successful bind over an inherited
	// pipe before accepting the child as the owner.
	readyReader, readyWriter, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create supervisor readiness pipe: %w", err)
	}
	defer readyReader.Close()
	identityToken, err := newControlIdentityToken()
	if err != nil {
		_ = readyWriter.Close()
		return nil, err
	}
	readyFD := 3 + len(cmd.ExtraFiles)
	cmd.ExtraFiles = append(cmd.ExtraFiles, readyWriter)
	env := cmd.Env
	if env == nil {
		env = os.Environ()
	}
	const readyEnv = "AVENOR_CONTROL_READY_FD="
	const identityEnv = controlIdentityTokenEnv + "="
	cmd.Env = make([]string, 0, len(env)+2)
	for _, entry := range env {
		if !strings.HasPrefix(entry, readyEnv) && !strings.HasPrefix(entry, identityEnv) {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env,
		fmt.Sprintf("%s%d", readyEnv, readyFD),
		identityEnv+identityToken,
	)

	if err := cmd.Start(); err != nil {
		_ = readyWriter.Close()
		return nil, fmt.Errorf("start supervisor: %w", err)
	}
	_ = readyWriter.Close()

	exited := make(chan error, 1)
	go func() {
		exited <- cmd.Wait()
	}()
	ready := make(chan error, 1)
	go func() {
		var signal [1]byte
		_, err := readyReader.Read(signal[:])
		ready <- err
	}()

	backoff := 50 * time.Millisecond
	deadline := time.After(startupTimeout)
	readyCh := ready
	childReady := false

	for {
		select {
		case readyErr := <-readyCh:
			readyCh = nil
			childReady = readyErr == nil
			if childReady {
				afterSupervisorReady()
			}
		case <-exited:
			// A competing starter may have won the bind. Re-check before
			// declaring failure, and never unlink or shut down its socket.
			if cl, ok := dialHealthySupervisor(socketPath); ok {
				return &supervisorLifecycle{
					socketPath: socketPath,
					client:     cl,
					shared:     true,
				}, nil
			}
			return nil, fmt.Errorf("supervisor exited during startup")
		case <-deadline:
			_ = cmd.Process.Kill()
			<-exited
			return nil, fmt.Errorf("supervisor startup timed out after %s", startupTimeout)
		default:
		}

		if childReady {
			cl, dialErr := client.Dial(socketPath)
			if dialErr == nil {
				_, statusErr := cl.Status("")
				if statusErr == nil {
					identity, identityErr := cl.Identity()
					if identityErr == nil && identity == identityToken {
						// Readiness proves the child bound the path once. The identity
						// probe proves this particular connection still reaches it.
						select {
						case <-exited:
							_ = cl.Close()
							if winner, ok := dialHealthySupervisor(socketPath); ok {
								return &supervisorLifecycle{socketPath: socketPath, client: winner, shared: true}, nil
							}
							return nil, fmt.Errorf("supervisor exited during startup")
						default:
							return &supervisorLifecycle{
								socketPath: socketPath,
								cmd:        cmd,
								client:     cl,
								exited:     exited,
							}, nil
						}
					}
					if identityErr == nil {
						// A replacement won the path after readiness. Retire the child
						// we spawned before attaching; it may still be running with its
						// original socket unlinked and must not become an orphan.
						if cmd.Process != nil {
							_ = cmd.Process.Kill()
							<-exited
						}
						return &supervisorLifecycle{socketPath: socketPath, client: cl, shared: true}, nil
					}
				}
				_ = cl.Close()
			}
		}

		time.Sleep(backoff)
		backoff *= 2
		if backoff > 500*time.Millisecond {
			backoff = 500 * time.Millisecond
		}
	}
}

func dialHealthySupervisor(socketPath string) (*client.Client, bool) {
	cl, err := client.Dial(socketPath)
	if err != nil {
		return nil, false
	}
	if _, err := cl.Status(""); err != nil {
		_ = cl.Close()
		return nil, false
	}
	return cl, true
}

func (l *supervisorLifecycle) Shutdown() error {
	return l.ShutdownWithMode("graceful")
}

// Close releases the MCP server's connection. An attached/shared
// lifecycle must not shut down the supervisor that it does not own.
func (l *supervisorLifecycle) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.shared {
		if l.client != nil {
			err := l.client.Close()
			l.client = nil
			return err
		}
		return nil
	}
	return l.shutdownWithMode("graceful")
}

func (l *supervisorLifecycle) ShutdownWithMode(mode string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.shutdownWithMode(mode)
}

func (l *supervisorLifecycle) shutdownWithMode(mode string) error {
	var firstErr error

	if l.client != nil {
		if err := l.client.Shutdown(mode); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := l.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		l.client = nil
	}

	if l.cmd != nil && l.cmd.Process != nil && l.exited != nil && !l.waited {
		select {
		case <-l.exited:
			l.waited = true
		case <-time.After(3 * time.Second):
			if pgid, err := syscall.Getpgid(l.cmd.Process.Pid); err == nil {
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			} else {
				_ = l.cmd.Process.Kill()
			}
			<-l.exited
			l.waited = true
		}
	}

	// The child ControlServer.Stop removes its socket while its listener is
	// still live. Never unlink here: after the child exits a replacement can
	// bind the same path and reuse its inode before a parent-side Stat.

	return firstErr
}
