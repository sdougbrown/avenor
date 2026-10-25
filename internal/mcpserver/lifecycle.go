package mcpserver

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/token"
)

var execCommand = exec.Command

var startupTimeout = 10 * time.Second

// afterSupervisorReady is a no-op production hook used to coordinate the
// post-readiness socket-replacement test before its first identity probe.
var afterSupervisorReady = func() {}

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

func startSupervisor(socketPath string, idleTimeout time.Duration, mcpLaunchWrapperPolicyPath string) (*supervisorLifecycle, error) {
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

	if cl, state, err := attachSupervisor(socketPath); state == supervisorLive {
		return &supervisorLifecycle{
			socketPath: socketPath,
			client:     cl,
			shared:     true,
		}, nil
	} else if state != supervisorDead {
		// Nothing proves the path is free, so a replacement is never started for
		// it: the running server keeps its socket, record and owner access.
		return nil, fmt.Errorf("attach supervisor %s: %w", socketPath, err)
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
	// The policy path is forwarded only when this process autostarts the
	// supervisor; attaching to a healthy supervisor above never modifies it.
	if mcpLaunchWrapperPolicyPath != "" {
		args = append(args, "--mcp-launch-wrapper-policy", mcpLaunchWrapperPolicyPath)
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
	identityToken, err := token.NewHexToken()
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
	const identityEnv = token.EnvControlIdentityToken + "="
	// Credential and ownership plumbing must not leak into the nested
	// supervisor: a parent-link token in the environment with no
	// --broker-parent-url would make the nested broker serve parent-link
	// routes under the operator's shared credential.
	strippedEnvs := []string{
		readyEnv,
		identityEnv,
		token.EnvBrokerParentToken + "=",
		runtime.EnvTestHooks + "=",
		runtime.EnvTestOwnerPublishDelay + "=",
	}
	cmd.Env = make([]string, 0, len(env)+2)
	for _, entry := range env {
		skip := false
		for _, prefix := range strippedEnvs {
			if strings.HasPrefix(entry, prefix) {
				skip = true
				break
			}
		}
		if !skip {
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
			return attachWinner(socketPath)
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
						// probe proves this particular connection still reaches it, and
						// the owner capability is loaded and presented on that same
						// connection so this attempt can never authenticate as a winner.
						authErr := authenticateOwnChild(cl, socketPath, cmd.Process.Pid)
						if authErr == nil {
							select {
							case <-exited:
								_ = cl.Close()
								return attachWinner(socketPath)
							default:
								return &supervisorLifecycle{
									socketPath: socketPath,
									cmd:        cmd,
									client:     cl,
									exited:     exited,
								}, nil
							}
						}
						_ = cl.Close()
						if !isOwnerRecordError(authErr) {
							// The identity-matched connection refused this child, so no
							// capability of this attempt's was presented. Retire the child
							// we spawned and attach the owner that holds the path through
							// its own record; a genuine fault in this child fails the same
							// way once it is settled.
							if cmd.Process != nil {
								_ = cmd.Process.Kill()
								<-exited
							}
							if isOwnerCapabilityRejection(authErr) {
								if winner, err := attachWinner(socketPath); err == nil {
									return winner, nil
								}
							}
							return nil, fmt.Errorf("authenticate owned supervisor %s: %w", socketPath, authErr)
						}
						// The record published on the path is not this child's yet. That
						// is the ordinary replacement window, so retry until the deadline.
					} else if identityErr == nil {
						// A replacement won the path after readiness. Retire the child
						// we spawned before attaching; it may still be running with its
						// original socket unlinked and must not become an orphan.
						if cmd.Process != nil {
							_ = cmd.Process.Kill()
							<-exited
						}
						_ = cl.Close()
						return attachWinner(socketPath)
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

// supervisorProbeTimeout bounds the credential-free dial that decides whether
// a listener holds the control socket path.
const supervisorProbeTimeout = 250 * time.Millisecond

// supervisorState is the launcher's replacement decision for an existing
// control socket path.
type supervisorState int

const (
	// supervisorLive means a listener holds the path and accepted the owner
	// capability published beside it.
	supervisorLive supervisorState = iota
	// supervisorDead means no listener holds the path, so a replacement may be
	// attempted.
	supervisorDead
	// supervisorAuthRejected means a listener answers but this process cannot
	// authenticate as its owner.
	supervisorAuthRejected
	// supervisorUncertain means the probe proved neither liveness nor absence.
	supervisorUncertain
)

// supervisorProbeDial is the credential-free dial the replacement decision is
// based on; tests replace it to inject a probe outcome.
var supervisorProbeDial = func(socketPath string) (net.Conn, error) {
	return net.DialTimeout("unix", socketPath, supervisorProbeTimeout)
}

// attachSupervisor classifies an existing control socket and, when it is live,
// attaches to it with its published owner capability.
func attachSupervisor(socketPath string) (*client.Client, supervisorState, error) {
	conn, err := supervisorProbeDial(socketPath)
	if err != nil {
		return nil, classifyProbeError(err), err
	}
	_ = conn.Close()
	cl, err := client.AttachOwner(socketPath)
	if err != nil {
		return nil, classifyAttachError(err), err
	}
	return cl, supervisorLive, nil
}

// attachWinner returns the shared lifecycle of the supervisor that holds
// socketPath after this attempt's own child lost it.
func attachWinner(socketPath string) (*supervisorLifecycle, error) {
	cl, state, err := attachSupervisor(socketPath)
	if state == supervisorLive {
		return &supervisorLifecycle{socketPath: socketPath, client: cl, shared: true}, nil
	}
	if state != supervisorDead {
		return nil, fmt.Errorf("attach competing supervisor %s: %w", socketPath, err)
	}
	return nil, fmt.Errorf("supervisor exited during startup")
}

// classifyProbeError decides the replacement decision from a credential-free
// dial: a bound unix listener never refuses, so ECONNREFUSED proves that no
// listener holds the path. Anything else is uncertain and never authorizes a
// replacement.
func classifyProbeError(err error) supervisorState {
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, fs.ErrNotExist) {
		return supervisorDead
	}
	return supervisorUncertain
}

// classifyAttachError separates a live socket this process may not authenticate
// to from one that vanished between the probe and the attach.
func classifyAttachError(err error) supervisorState {
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, fs.ErrNotExist) {
		return supervisorDead
	}
	if isOwnerCapabilityRejection(err) || isOwnerRecordError(err) {
		return supervisorAuthRejected
	}
	return supervisorUncertain
}

// isOwnerCapabilityRejection reports a peer that refused, did not support or
// timed out the presented capability, or was not a same-uid process.
func isOwnerCapabilityRejection(err error) bool {
	var foreign *client.ForeignPeerError
	return errors.Is(err, client.ErrOwnerUnauthenticated) ||
		errors.Is(err, client.ErrUnsupportedOwnerAuth) ||
		errors.Is(err, client.ErrOwnerAuthTimeout) ||
		errors.As(err, &foreign)
}

// isOwnerRecordError reports a record that is absent or does not describe the
// live socket. During an owned start that record belongs to a predecessor and
// is retryable; for an existing socket it is terminal.
func isOwnerRecordError(err error) bool {
	return errors.Is(err, control.ErrOwnerRecordMissing) || errors.Is(err, control.ErrStaleOwnerRecord)
}

// authenticateOwnChild authenticates the connection that proved this attempt's
// identity token with the record that child published before the token became
// observable. A record that does not describe the live socket, or that names
// another owner, is reported as a retryable error, never as a rejection.
func authenticateOwnChild(cl *client.Client, socketPath string, childPID int) error {
	record, err := control.LoadOwnerRecord(socketPath)
	if err != nil {
		return err
	}
	if err := client.ValidateOwnerRecord(socketPath, record); err != nil {
		return err
	}
	if record.OwnerPID != childPID {
		// A winner rebound the path between the identity probe and this attach.
		// Presenting its capability would hand the winner's secret to this
		// launcher's own child.
		return fmt.Errorf("%w: %s is owned by pid %d, not this launcher's child %d", control.ErrStaleOwnerRecord, socketPath, record.OwnerPID, childPID)
	}
	return cl.AuthenticateOwner(control.OwnerCapability{ID: record.CapabilityID, Secret: record.Secret})
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
