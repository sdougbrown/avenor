package agy

import (
	"context"
	"errors"
	"strconv"
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

	mu            sync.Mutex
	retryMu       sync.Mutex
	closing       bool
	turnCancel    context.CancelFunc
	turnDone      chan struct{}
	turn          *rpcTurn
	turnStarting  bool
	pendingCancel bool
	closed        chan struct{}
	closeErr      error

	models   *agyv115.FetchAvailableModelsResponse
	mapper   *trajectoryMapper
	turnGate chan struct{}

	// coordinator is the active Stage 12 recovery owner for the current turn.
	coordinator  *trajectoryRecoveryCoordinator
	interactions *interactionBridge
}

type ptyRPCHostFactory func(context.Context, runtime.StartOptions, string, string) (*ptyRPCHost, error)

func supportedRPCVersion(version string) bool {
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" || len(part) > 1 && part[0] == '0' {
			return false
		}
		if _, err := strconv.ParseUint(part, 10, 64); err != nil {
			return false
		}
	}
	return parts[0] == "1"
}

var (
	discoverPTYRPCHost = discoverRPCHost
	startPTYCascade    = func(ctx context.Context, host *rpcHost) (*agyv115.StartCascadeResponse, error) {
		return host.startCascade(ctx)
	}
	validatePTYSession     = func(ctx context.Context, host *rpcHost, id string) error { return host.validateSession(ctx, id) }
	awaitModelAvailability = func(ctx context.Context, host *rpcHost) (*agyv115.FetchAvailableModelsResponse, error) {
		return host.client.waitForAvailableModels(ctx)
	}
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
	if !supportedRPCVersion(version) {
		return nil, errors.New("agy RPC host requires compatible major version 1")
	}

	lifetimeCtx, cancel := context.WithCancel(context.Background())
	session, err := launcher.Start(lifetimeCtx, terminal.StartOptions{
		Name:    "agy-rpc",
		Dir:     opts.Dir,
		Cols:    220,
		Rows:    50,
		Command: interactiveAgyCommand(resumeID, opts.Agent, opts.Dir),
	})
	if err != nil {
		cancel()
		return nil, errors.New("agy RPC host startup failed")
	}

	turnGate := make(chan struct{}, 1)
	turnGate <- struct{}{}
	host := &ptyRPCHost{terminal: session, cancel: cancel, closed: make(chan struct{}), turnGate: turnGate}
	pid := session.PID()
	if pid <= 0 {
		return failPTYRPCHostStartup(host, errors.New("agy RPC host did not report a process PID"))
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
		return failPTYRPCHostStartup(host, err)
	}
	host.rpc = rpc

	var conversationID string
	if resumeID == "" {
		response, startErr := awaitPTYCascadeStart(ctx, processDone, rpc)
		if startErr != nil {
			return failPTYRPCHostStartup(host, startErr)
		}
		conversationID = response.GetCascadeId()
		if conversationID == "" {
			return failPTYRPCHostStartup(host, errors.New("agy RPC host did not create a conversation"))
		}
	} else {
		conversationID = resumeID
	}

	if err := awaitPTYRPCStartup(ctx, processDone, func(startCtx context.Context) error {
		return validatePTYSession(startCtx, rpc, conversationID)
	}); err != nil {
		return failPTYRPCHostStartup(host, err)
	}
	host.conversationID = conversationID

	var modelsResp *agyv115.FetchAvailableModelsResponse
	if err := awaitPTYRPCStartup(ctx, processDone, func(startCtx context.Context) error {
		var waitErr error
		modelsResp, waitErr = awaitModelAvailability(startCtx, rpc)
		return waitErr
	}); err != nil {
		return failPTYRPCHostStartup(host, err)
	}
	host.models = modelsResp
	// AgentStateUpdate.conversation_id is a distinct wire identity. Pin it from
	// the first stream update rather than equating it with the cascade ID.
	host.mapper = newTrajectoryMapper(conversationID, "", "")
	host.installInteractionBridge()
	return host, nil
}

func failPTYRPCHostStartup(host *ptyRPCHost, cause error) (*ptyRPCHost, error) {
	if host != nil {
		if err := host.Close(context.Background()); err != nil {
			if retryErr := host.Close(context.Background()); retryErr != nil {
				return host, errors.New("agy RPC host startup cleanup failed")
			}
		}
	}
	return nil, cause
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

func (h *ptyRPCHost) installInteractionBridge() {
	if h == nil || h.rpc == nil || h.rpc.client == nil || h.mapper == nil {
		return
	}
	bridge := newInteractionBridge(h.conversationID, h.conversationID,
		func(ctx context.Context, offset uint32) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
			return h.rpc.client.getCascadeTrajectorySteps(ctx, h.conversationID, offset, agyv115.SnapshotTrajectoryVerbosity_CLIENT_TRAJECTORY_VERBOSITY_FULL, agyv115.StreamTrajectoryVerbosity_CLIENT_TRAJECTORY_VERBOSITY_FULL)
		},
		func(ctx context.Context, interaction *agyv115.CascadeUserInteraction) error {
			_, err := h.rpc.client.handleCascadeUserInteraction(ctx, h.conversationID, interaction)
			return err
		},
	)
	h.interactions = bridge
	h.mapper.SetInteractionObserver(bridge.Observe, bridge.Evict)
}

// AnswerPermission resolves only the current local request owned by this host.
func (h *ptyRPCHost) AnswerPermission(ctx context.Context, requestID string, response runtime.PermissionResponse) error {
	if h == nil {
		return errInteractionRejected
	}
	h.mu.Lock()
	closing, bridge := h.closing, h.interactions
	h.mu.Unlock()
	if closing || bridge == nil {
		return errInteractionRejected
	}
	return bridge.Answer(ctx, requestID, response)
}

// ConversationID returns the validated external agy conversation identity.
func (h *ptyRPCHost) ConversationID() string { return h.conversationID }

// Close is safe to call concurrently. It first cancels any in-flight
// interaction answer and clears the bridge, then closes the recovery
// coordinator, the typed RPC client, the PTY, and finally waits for reaping.
// Clearing interactions first ensures a blocked pre-answer snapshot or Handle
// RPC exits before RPC teardown, and no Handle mutation is sent against a
// closed transport.
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
			if err != nil {
				return h.retryCloseCleanup(ctx)
			}
			return nil
		case <-cleanupCtx.Done():
			return cleanupCtx.Err()
		}
	}
	h.closing = true
	if h.turnCancel != nil {
		h.turnCancel()
	}
	turnDone := h.turnDone
	coordinator := h.coordinator
	interactions := h.interactions
	rpc := h.rpc
	h.mu.Unlock()

	var firstErr error
	// Clear and cancel any in-flight interaction answer before tearing RPC
	// down. This is the gate that proves Close during a blocked pre-answer
	// fetch yields zero Handle mutations and no goroutine leak.
	if interactions != nil {
		if err := interactions.CancelAndWait(cleanupCtx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if coordinator != nil {
		if err := coordinator.Close(); err != nil {
			firstErr = err
		}
		if err := coordinator.Wait(cleanupCtx); err != nil && !errors.Is(err, errTrajectoryRecoveryClosed) && !errors.Is(err, context.Canceled) && firstErr == nil {
			firstErr = err
		}
	}
	if turnDone != nil {
		select {
		case <-turnDone:
		case <-cleanupCtx.Done():
			if firstErr == nil {
				firstErr = cleanupCtx.Err()
			}
		}
	}
	if rpc != nil {
		rpc.close()
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

func (h *ptyRPCHost) retryCloseCleanup(ctx context.Context) error {
	h.retryMu.Lock()
	defer h.retryMu.Unlock()
	h.mu.Lock()
	if h.closeErr == nil {
		h.mu.Unlock()
		return nil
	}
	processDone := h.processDone
	interactions := h.interactions
	coordinator := h.coordinator
	turnDone := h.turnDone
	rpc := h.rpc
	h.mu.Unlock()

	cleanupCtx, cancel := boundedPTYRPCCloseContext(ctx)
	defer cancel()
	if interactions != nil {
		if err := interactions.CancelAndWait(cleanupCtx); err != nil {
			return err
		}
	}
	if coordinator != nil {
		_ = coordinator.Close()
		if err := coordinator.Wait(cleanupCtx); err != nil && !errors.Is(err, errTrajectoryRecoveryClosed) && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	if turnDone != nil {
		select {
		case <-turnDone:
		case <-cleanupCtx.Done():
			return cleanupCtx.Err()
		}
	}
	if rpc != nil {
		rpc.close()
	}
	if h.cancel != nil {
		h.cancel()
	}
	if h.terminal != nil {
		_ = h.terminal.Kill(cleanupCtx)
	}
	if processDone != nil {
		select {
		case <-processDone:
		case <-cleanupCtx.Done():
			return cleanupCtx.Err()
		}
	} else if h.terminal != nil {
		if err := h.terminal.Wait(cleanupCtx); err != nil && cleanupCtx.Err() != nil {
			return cleanupCtx.Err()
		}
	}
	h.mu.Lock()
	h.closeErr = nil
	h.mu.Unlock()
	return nil
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

func interactiveAgyCommand(resumeID, agent, dir string) string {
	command := "exec agy"
	if resumeID != "" {
		command += " --conversation " + posixShellQuote(resumeID)
	}
	if agent != "" {
		command += " --agent " + posixShellQuote(agent)
	}
	// agy's interactive tool workspace is not derived from the process cwd, and
	// it resolves a literal "." after changing directories. Let the launch shell
	// expand its trusted cwd without embedding the user path in this command.
	if dir != "" {
		command += " --add-dir \"$PWD\""
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
