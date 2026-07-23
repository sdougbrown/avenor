package agy

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

const ptyRPCCloseTimeout = 5 * time.Second

// ptyRPCHost owns one interactive agy process and its validated typed RPC
// client. The terminal is deliberately not exposed as a protocol surface: it
// exists only to give agy a TUI-compatible lifetime.
type ptyRPCHost struct {
	terminal terminal.Session
	rpc      *rpcHost
	cancel   context.CancelFunc

	conversationID string
	processDone    <-chan struct{}

	mu       sync.Mutex
	closing  bool
	closed   chan struct{}
	closeErr error

	// coordinator is reserved for the Stage 12 recovery owner when Stage 18
	// selects this host. Keeping the close seam here ensures the host remains
	// the sole lifetime owner rather than leaking a future stream.
	coordinator interface{ Close() error }
}

type ptyRPCHostFactory func(context.Context, runtime.StartOptions, string, string) (*ptyRPCHost, error)

var (
	discoverPTYRPCHost = discoverRPCHost
	startPTYCascade    = func(ctx context.Context, host *rpcHost) (*agyv115.StartCascadeResponse, error) {
		return host.startCascade(ctx)
	}
	validatePTYSession = func(ctx context.Context, host *rpcHost, id string) error { return host.validateSession(ctx, id) }
)

func defaultPTYRPCHostFactory(ctx context.Context, opts runtime.StartOptions, resumeID, version string) (*ptyRPCHost, error) {
	return startPTYRPCHost(ctx, terminal.PTYLauncher{}, opts, resumeID, version, rpcDiscoveryOptions{})
}

// startPTYRPCHost starts interactive agy, discovers only its exact-PID
// loopback RPC endpoint, and binds a conversation before handing ownership to
// the caller. resumeID selects resume mode; an empty ID creates a cascade.
func startPTYRPCHost(ctx context.Context, launcher terminal.Launcher, opts runtime.StartOptions, resumeID, version string, discoveryOpts rpcDiscoveryOptions) (*ptyRPCHost, error) {
	if launcher == nil {
		return nil, errors.New("agy RPC host startup failed")
	}
	if version != "1.1.5" {
		return nil, errors.New("agy RPC host requires supported version 1.1.5")
	}

	lifetimeCtx, cancel := context.WithCancel(context.Background())
	session, err := launcher.Start(lifetimeCtx, terminal.StartOptions{
		Name:    "agy-rpc",
		Dir:     opts.Dir,
		Cols:    220,
		Rows:    50,
		Command: interactiveAgyCommand(resumeID, opts.Agent),
	})
	if err != nil {
		cancel()
		return nil, errors.New("agy RPC host startup failed")
	}

	host := &ptyRPCHost{terminal: session, cancel: cancel, closed: make(chan struct{})}
	pid := session.PID()
	if pid <= 0 {
		_ = host.Close(context.Background())
		return nil, errors.New("agy RPC host did not report a process PID")
	}

	processDone := make(chan struct{})
	host.processDone = processDone
	go func() {
		_ = session.Wait(context.Background())
		close(processDone)
	}()

	var rpc *rpcHost
	err = awaitPTYRPCStartup(ctx, processDone, func(startCtx context.Context) error {
		var discoverErr error
		rpc, discoverErr = discoverPTYRPCHostWithRetry(startCtx, pid, version, discoveryOpts)
		return discoverErr
	})
	if err != nil {
		_ = host.Close(context.Background())
		return nil, err
	}
	host.rpc = rpc

	var conversationID string
	if resumeID == "" {
		response, startErr := awaitPTYCascadeStart(ctx, processDone, rpc)
		if startErr != nil {
			_ = host.Close(context.Background())
			return nil, startErr
		}
		conversationID = response.GetCascadeId()
		if conversationID == "" {
			_ = host.Close(context.Background())
			return nil, errors.New("agy RPC host did not create a conversation")
		}
	} else {
		conversationID = resumeID
	}

	if err := awaitPTYRPCStartup(ctx, processDone, func(startCtx context.Context) error {
		return validatePTYSession(startCtx, rpc, conversationID)
	}); err != nil {
		_ = host.Close(context.Background())
		return nil, err
	}
	host.conversationID = conversationID
	return host, nil
}

// discoverPTYRPCHostWithRetry gives interactive agy a bounded opportunity to
// bind its loopback listeners. Every attempt still delegates exact-PID and TLS
// validation to discoverRPCHost; no endpoint is cached or inferred here.
func discoverPTYRPCHostWithRetry(ctx context.Context, pid int, version string, options rpcDiscoveryOptions) (*rpcHost, error) {
	deadline := time.NewTimer(rpcDiscoveryTimeout)
	defer deadline.Stop()
	for {
		host, err := discoverPTYRPCHost(ctx, pid, version, options)
		if err == nil {
			return host, nil
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-deadline.C:
			timer.Stop()
			return nil, errors.New("agy RPC host discovery timed out")
		case <-timer.C:
		}
	}
}

func awaitPTYCascadeStart(ctx context.Context, processDone <-chan struct{}, host *rpcHost) (*agyv115.StartCascadeResponse, error) {
	var response *agyv115.StartCascadeResponse
	err := awaitPTYRPCStartup(ctx, processDone, func(startCtx context.Context) error {
		var startErr error
		response, startErr = startPTYCascade(startCtx, host)
		return startErr
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// awaitPTYRPCStartup makes caller cancellation and process death observable at
// every startup boundary. The action must honor its context; production RPC
// discovery/calls do, so its goroutine is joined before this function returns.
func awaitPTYRPCStartup(ctx context.Context, processDone <-chan struct{}, action func(context.Context) error) error {
	actionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- action(actionCtx) }()

	select {
	case err := <-result:
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			return errors.New("agy RPC host startup failed")
		}
		select {
		case <-processDone:
			return errors.New("agy RPC host exited during startup")
		default:
			return nil
		}
	case <-processDone:
		cancel()
		<-result
		return errors.New("agy RPC host exited during startup")
	case <-ctx.Done():
		cancel()
		<-result
		return ctx.Err()
	}
}

// RPC returns the validated typed RPC host. It deliberately exposes no
// terminal operations or untyped protobuf transport.
func (h *ptyRPCHost) RPC() *rpcHost { return h.rpc }

// ConversationID returns the validated external agy conversation identity.
func (h *ptyRPCHost) ConversationID() string { return h.conversationID }

// Close is safe to call concurrently. It closes RPC/coordinator ownership,
// cancels and kills the PTY, then synchronously waits for reaping.
func (h *ptyRPCHost) Close(ctx context.Context) error {
	cleanupCtx, cancel := boundedPTYRPCCloseContext(ctx)
	defer cancel()

	h.mu.Lock()
	if h.closing {
		done := h.closed
		h.mu.Unlock()
		select {
		case <-done:
			h.mu.Lock()
			err := h.closeErr
			h.mu.Unlock()
			return err
		case <-cleanupCtx.Done():
			return cleanupCtx.Err()
		}
	}
	h.closing = true
	h.mu.Unlock()

	var firstErr error
	if h.coordinator != nil {
		if err := h.coordinator.Close(); err != nil {
			firstErr = err
		}
	}
	if h.rpc != nil {
		h.rpc.close()
	}
	h.cancel()
	if err := h.terminal.Kill(cleanupCtx); err != nil && firstErr == nil {
		firstErr = err
	}
	if h.processDone != nil {
		select {
		case <-h.processDone:
		case <-cleanupCtx.Done():
			if firstErr == nil {
				firstErr = cleanupCtx.Err()
			}
		}
	} else if err := h.terminal.Wait(cleanupCtx); err != nil && firstErr == nil {
		firstErr = err
	}

	h.mu.Lock()
	h.closeErr = firstErr
	close(h.closed)
	h.mu.Unlock()
	return firstErr
}

func boundedPTYRPCCloseContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := ptyRPCCloseTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}
	return context.WithTimeout(context.Background(), timeout)
}

func interactiveAgyCommand(resumeID, agent string) string {
	command := "exec agy"
	if resumeID != "" {
		command += " --conversation " + posixShellQuote(resumeID)
	}
	if agent != "" {
		command += " --agent " + posixShellQuote(agent)
	}
	return command
}

// posixShellQuote produces one POSIX-shell word. Single quotes are represented
// by adjacent quoted fragments, so newlines and shell metacharacters remain
// literal data rather than syntax.
func posixShellQuote(value string) string {
	return "'" + replaceSingleQuotes(value) + "'"
}

func replaceSingleQuotes(value string) string {
	return strings.ReplaceAll(value, "'", "'\"'\"'")
}
