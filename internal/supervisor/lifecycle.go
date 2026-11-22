// Package supervisor starts, attaches to and stops the long-lived Avenor
// supervisor process that owns one control socket. It is the launcher half of
// the control plane: an MCP server autostarts a supervisor for itself, and a
// conversation's inbox feed autostarts the one that serves that conversation.
// Every caller decides when a supervisor may be started; a caller that only
// wants an existing one calls Attach and fails when there is none.
package supervisor

import (
	"crypto/sha256"
	"encoding/hex"
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

// SocketDir returns the sockets root a conversation's own supervisor socket is
// derived under: $AVENOR_HOME/sockets when AVENOR_HOME names the Avenor home,
// otherwise ~/.avenor/sockets. It is the root the documented supervisor_id
// containment rule describes. It never creates or resolves the directory.
//
// The default per-process supervisor an MCP server autostarts keeps its own
// ~/.avenor/sockets path, which is not this rule: that socket's location is part
// of the MCP server's existing behaviour.
func SocketDir() (string, error) {
	home := os.Getenv("AVENOR_HOME")
	if home == "" {
		var err error
		home, err = os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve the home directory for the sockets root: %w", err)
		}
		home = filepath.Join(home, ".avenor")
	}
	return filepath.Join(home, "sockets"), nil
}

// SocketDirs returns the roots a supervisor socket is published under, in the
// order a scan reads them: the default ~/.avenor/sockets root and the sockets
// root the conversation rule names, which is the same directory unless
// $AVENOR_HOME moves it. A reader that discovers supervisors scans both, so a
// supervisor started before that root moved stays visible.
func SocketDirs() ([]string, error) {
	dirs := make([]string, 0, 2)
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve the home directory for the sockets root: %w", err)
	}
	defaultDir := filepath.Join(home, ".avenor", "sockets")
	dirs = append(dirs, defaultDir)
	conversationDir, err := SocketDir()
	if err != nil {
		return nil, err
	}
	if conversationDir != defaultDir {
		dirs = append(dirs, conversationDir)
	}
	return dirs, nil
}

// ConversationSocket returns the control socket of the supervisor that serves
// one native host conversation. The feed that binds the conversation and every
// command that reads its binding derive the same path, so a session reaches one
// supervisor without configuring a socket.
//
// The name is a hash of the pair: a native session id is up to 200 bytes and is
// not required to be ASCII, so it cannot name a unix socket inside the pathname
// limit itself. The hash is computed over length-unambiguous material and a
// collision only means two conversations share one supervisor, which its
// per-conversation bindings already support.
func ConversationSocket(host, sessionID string) (string, error) {
	if host == "" || sessionID == "" {
		return "", fmt.Errorf("a conversation supervisor socket needs a native host and a session id")
	}
	root, err := SocketDir()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(host + "\x00" + sessionID))
	return filepath.Join(root, "avenor-conv-"+hex.EncodeToString(sum[:10])+".sock"), nil
}

// Client is the control connection a lifecycle holds: the part of the control
// surface a launcher itself uses — shut the supervisor down and release the
// connection. The caller keeps the full surface it needs.
//
// The owner capability is same-uid trust over a local socket, so this interface
// is not a privilege boundary against another process of the same user.
type Client interface {
	Shutdown(mode string) error
	Close() error
}

// Options are the launch settings a caller supplies when it starts a
// supervisor. The zero value is the production configuration.
type Options struct {
	// ShutdownTimeout overrides the supervisor's --shutdown-timeout, so a
	// caller whose own parent kills it after a grace period gets a shutdown
	// that finishes inside it. Zero keeps the supervisor's default.
	ShutdownTimeout time.Duration
	// ShutdownOnParentExit has the kernel signal the child when the process
	// that started it dies, whatever the reason: a launcher that owns the
	// supervisor's lifetime cannot observe its own SIGKILL, and this is the one
	// route left that still stops the supervisor. The signal is the one a
	// supervisor shuts down gracefully on. It is a no-op where the platform has
	// no parent-death signal.
	ShutdownOnParentExit bool
}

// The launcher seams. Production uses these values; a test substitutes the
// binary under test, an interleaving hook, a probe outcome or a shorter startup
// bound instead of starting the real thing.
var (
	// ExecCommand builds the supervisor child command.
	ExecCommand = exec.Command
	// AfterReady runs once the child has proved it bound its socket, before the
	// first identity probe.
	AfterReady = func() {}
	// ProbeDial classifies an existing control socket before a replacement is
	// attempted: a bounded credential-free dial unless a test injects an outcome.
	ProbeDial = func(socketPath string) (net.Conn, error) {
		return net.DialTimeout("unix", socketPath, probeTimeout)
	}
	// StartupTimeout is how long a started supervisor has to prove it bound its
	// socket and published its identity.
	StartupTimeout = 10 * time.Second
)

// probeTimeout bounds the credential-free dial that decides whether a listener
// holds a control socket path.
const probeTimeout = 250 * time.Millisecond

// Lifecycle is one launcher's hold on a supervisor: the child it started, or
// the shared supervisor it attached to.
type Lifecycle struct {
	mu sync.Mutex
	// SocketPath is the control socket the supervisor serves.
	SocketPath string
	// Cmd is the child this launcher started, nil when it attached.
	Cmd *exec.Cmd
	// Client is this launcher's owner connection.
	Client Client
	// Exited receives the child's exit once it ends.
	Exited <-chan error
	// Waited reports that the child's exit has been collected.
	Waited bool

	// Shared means this lifecycle attached to an already-running supervisor.
	// Closing a lifecycle that did not start its supervisor may not shut down
	// or unlink a supervisor another process owns; an explicit shutdown call
	// still sends the shutdown RPC.
	Shared bool
}

// Start starts the supervisor that serves socketPath, or attaches to the one
// already there, and returns the lifecycle. An empty socketPath names the
// default per-process supervisor socket under the sockets root.
func Start(socketPath string, idleTimeout time.Duration, mcpLaunchWrapperPolicyPath string, opts Options) (*Lifecycle, error) {
	// The launcher owns the socket's directory. A default socket needs the
	// home root; a named one needs only its own parent, and must not require a
	// home directory it will never write to.
	if socketPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("get home directory: %w", err)
		}
		socketPath = filepath.Join(home, ".avenor", "sockets", fmt.Sprintf("avenor-mcp-%d.sock", os.Getpid()))
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0700); err != nil {
		return nil, fmt.Errorf("create sockets directory: %w", err)
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

	if cl, state, err := Attach(socketPath, ProbeDial); state == Live {
		return &Lifecycle{
			SocketPath: socketPath,
			Client:     cl,
			Shared:     true,
		}, nil
	} else if state != Dead {
		// Nothing proves the path is free, so a replacement is never started for
		// it: the running server keeps its socket, record and owner access.
		return nil, fmt.Errorf("attach supervisor %s: %w", socketPath, err)
	}

	// Stale-path removal belongs to ControlServer.Start, which serializes it
	// with bind and Stop under the per-socket control lock. The startup lock
	// only elects one launcher among competing starters and must not mutate the
	// socket path.

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
	if opts.ShutdownTimeout > 0 {
		args = append(args, "--shutdown-timeout", opts.ShutdownTimeout.String())
	}

	cmd := ExecCommand(exePath, args...)
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if opts.ShutdownOnParentExit {
		shutdownOnParentExit(cmd.SysProcAttr)
	}

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
	parentURL := ""
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, token.EnvBrokerParentURL+"="); ok {
			parentURL = value
		}
	}
	// The strip list is sourced from runtime's single broker/test name list so
	// a name added there is stripped here without a second hand-maintained
	// copy. The parent URL is deliberately forwarded so the nested supervisor
	// joins the same hub, and the parent credential rides along only with it:
	// a lone credential would instead enable this child's parent routes under
	// the operator's shared secret.
	strippedEnvs := []string{readyEnv, identityEnv}
	for _, name := range runtime.BrokerEnvNames() {
		if name == token.EnvBrokerParentURL {
			continue
		}
		if name == token.EnvBrokerParentToken && strings.TrimSpace(parentURL) != "" {
			continue
		}
		strippedEnvs = append(strippedEnvs, name+"=")
	}
	// A provisioned session's peer credential names the run its own plugin
	// joined, not this supervisor's. Stripping it here keeps the autostarted
	// supervisor from carrying it, and from handing it to a child it builds by
	// hand; the harness children it launches are scrubbed by LaunchEnv.
	for _, name := range runtime.PeerEnvNames() {
		strippedEnvs = append(strippedEnvs, name+"=")
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
	deadline := time.After(StartupTimeout)
	readyCh := ready
	childReady := false

	for {
		select {
		case readyErr := <-readyCh:
			readyCh = nil
			childReady = readyErr == nil
			if childReady {
				AfterReady()
			}
		case <-exited:
			// A competing starter may have won the bind. Re-check before
			// declaring failure, and never unlink or shut down its socket.
			return attachWinner(socketPath)
		case <-deadline:
			_ = cmd.Process.Kill()
			<-exited
			return nil, fmt.Errorf("supervisor startup timed out after %s", StartupTimeout)
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
						authErr := AuthenticateOwnChild(cl, socketPath, cmd.Process.Pid)
						if authErr == nil {
							select {
							case <-exited:
								_ = cl.Close()
								return attachWinner(socketPath)
							default:
								return &Lifecycle{
									SocketPath: socketPath,
									Cmd:        cmd,
									Client:     cl,
									Exited:     exited,
								}, nil
							}
						}
						_ = cl.Close()
						if !IsOwnerRecordError(authErr) {
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

// State is the launcher's replacement decision for an existing control socket
// path.
type State int

const (
	// Live means a listener holds the path and accepted the owner capability
	// published beside it.
	Live State = iota
	// Dead means no listener holds the path, so a replacement may be
	// attempted.
	Dead
	// AuthRejected means a listener answers but this process cannot
	// authenticate as its owner.
	AuthRejected
	// Uncertain means the probe proved neither liveness nor absence.
	Uncertain
)

// Attach classifies an existing control socket and, when it is live, attaches
// to it with its published owner capability. It never starts a supervisor, so a
// caller that must not autostart has one call to make. A nil probeDial uses the
// bounded credential-free dial the replacement decision is based on.
func Attach(socketPath string, probeDial func(socketPath string) (net.Conn, error)) (Client, State, error) {
	if probeDial == nil {
		probeDial = ProbeDial
	}
	conn, err := probeDial(socketPath)
	if err != nil {
		return nil, classifyProbeError(err), err
	}
	_ = conn.Close()
	cl, err := client.AttachOwner(socketPath)
	if err != nil {
		return nil, classifyAttachError(err), err
	}
	return cl, Live, nil
}

// attachWinner returns the shared lifecycle of the supervisor that holds
// socketPath after this attempt's own child lost it.
func attachWinner(socketPath string) (*Lifecycle, error) {
	cl, state, err := Attach(socketPath, ProbeDial)
	if state == Live {
		return &Lifecycle{SocketPath: socketPath, Client: cl, Shared: true}, nil
	}
	if state != Dead {
		return nil, fmt.Errorf("attach competing supervisor %s: %w", socketPath, err)
	}
	return nil, fmt.Errorf("supervisor exited during startup")
}

// classifyProbeError decides the replacement decision from a credential-free
// dial: a bound unix listener never refuses, so ECONNREFUSED proves that no
// listener holds the path. Anything else is uncertain and never authorizes a
// replacement.
func classifyProbeError(err error) State {
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, fs.ErrNotExist) {
		return Dead
	}
	return Uncertain
}

// classifyAttachError separates a live socket this process may not authenticate
// to from one that vanished between the probe and the attach.
func classifyAttachError(err error) State {
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, fs.ErrNotExist) {
		return Dead
	}
	if isOwnerCapabilityRejection(err) || IsOwnerRecordError(err) {
		return AuthRejected
	}
	return Uncertain
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
func IsOwnerRecordError(err error) bool {
	return errors.Is(err, control.ErrOwnerRecordMissing) || errors.Is(err, control.ErrStaleOwnerRecord)
}

// AuthenticateOwnChild authenticates the connection that proved this attempt's
// identity token with the record that child published before the token became
// observable. A record that does not describe the live socket, or that names
// another owner, is reported as a retryable error, never as a rejection.
func AuthenticateOwnChild(cl *client.Client, socketPath string, childPID int) error {
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

func (l *Lifecycle) Shutdown() error {
	return l.ShutdownWithMode("graceful")
}

// Close releases the launcher's connection and, when this lifecycle started the
// supervisor, shuts it down. A shared lifecycle must not shut down a supervisor
// another process owns.
func (l *Lifecycle) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Shared {
		if l.Client != nil {
			err := l.Client.Close()
			l.Client = nil
			return err
		}
		return nil
	}
	return l.shutdownWithMode("graceful")
}

func (l *Lifecycle) ShutdownWithMode(mode string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.shutdownWithMode(mode)
}

func (l *Lifecycle) shutdownWithMode(mode string) error {
	var firstErr error

	if l.Client != nil {
		if err := l.Client.Shutdown(mode); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := l.Client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		l.Client = nil
	}

	if l.Cmd != nil && l.Cmd.Process != nil && l.Exited != nil && !l.Waited {
		select {
		case <-l.Exited:
			l.Waited = true
		case <-time.After(3 * time.Second):
			if pgid, err := syscall.Getpgid(l.Cmd.Process.Pid); err == nil {
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			} else {
				_ = l.Cmd.Process.Kill()
			}
			<-l.Exited
			l.Waited = true
		}
	}

	// The child ControlServer.Stop removes its socket while its listener is
	// still live. Never unlink here: after the child exits a replacement can
	// bind the same path and reuse its inode before a parent-side Stat.

	return firstErr
}
