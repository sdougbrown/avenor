package agy

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

const (
	maxRPCModelSlugBytes = 256
	ptyRPCCancelTimeout  = 5 * time.Second
)

// ptyRPCGraceTimeout is the bounded grace window the host gives the exact
// hosted process to exit on its own after Interrupt before escalating to
// Kill. Production defaults match shutdownGrace used by other agy lifecycle
// paths and may be overridden through the test-only seams ptyRPCGraceOverride
// and ptyRPCCancelOverride.
const ptyRPCGraceTimeout = 2 * time.Second

// ptyRPCCancelOverride and ptyRPCGraceOverride allow tests to compress the
// production timeouts without changing the constants above.
var (
	ptyRPCCancelOverride func() time.Duration
	ptyRPCGraceOverride  func() time.Duration
)

func rpcCancelTimeout() time.Duration {
	if ptyRPCCancelOverride != nil {
		return ptyRPCCancelOverride()
	}
	return ptyRPCCancelTimeout
}

func rpcGraceTimeout() time.Duration {
	if ptyRPCGraceOverride != nil {
		return ptyRPCGraceOverride()
	}
	return ptyRPCGraceTimeout
}

var (
	errRPCTurnActive       = errors.New("agy RPC turn is already active")
	errRPCHostClosed       = errors.New("agy RPC host is closed")
	errRPCTurnCancelled    = errors.New("agy RPC turn was cancelled")
	errRPCCancelCleanup    = errors.New("agy RPC cancellation cleanup failed")
	errRPCModelUnavailable = errors.New("requested agy model is unavailable")
	errRPCModelRequired    = errors.New("agy RPC requires an explicit model")
)

// rpcTurn owns only per-turn synchronization. The mapper remains exclusively
// owned by the recovery goroutine; cancellation suppresses its later events
// rather than mutating it concurrently.
type rpcTurn struct {
	cancel     context.CancelFunc
	done       chan struct{}
	cancelDone chan struct{}
	onEvent    func(events.Event)
	sessionID  string

	mu                     sync.Mutex
	cancelStarted          bool
	cancelled              bool
	remoteTerminalObserved bool
	terminalEmitted        bool
	cancelResult           error
	sendIssued             bool

	// sendMu serializes the SendUserCascadeMessage mutation against markCancel.
	// The send path holds it across the RPC call. The cancel path holds it
	// across markCancel. Whichever party arrives first wins; the loser observes
	// the new state under the gate. No send may start after cancel marks.
	sendMu sync.Mutex
}

func (t *rpcTurn) emit(event events.Event) {
	t.mu.Lock()
	if event.Event == "session.end" {
		t.remoteTerminalObserved = true
	}
	if t.cancelled {
		t.mu.Unlock()
		return
	}
	if event.Event == "session.end" {
		t.terminalEmitted = true
	}
	onEvent := t.onEvent
	t.mu.Unlock()
	if onEvent != nil {
		onEvent(event)
	}
}

// markCancelled wins only before the normal terminal is published. The cancel
// path holds sendMu while calling it, so a concurrent send either observed
// cancelled before issuing its mutation or is blocked until cancel has marked.
func (t *rpcTurn) markCancelled() bool {
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.terminalEmitted {
		return false
	}
	t.cancelled = true
	return true
}

func (t *rpcTurn) isCancelled() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cancelled
}

func (t *rpcTurn) observedRemoteTerminal() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.remoteTerminalObserved
}

func (t *rpcTurn) emitCancelled() {
	t.mu.Lock()
	if !t.cancelled || t.terminalEmitted {
		t.mu.Unlock()
		return
	}
	t.terminalEmitted = true
	onEvent := t.onEvent
	t.mu.Unlock()
	if onEvent != nil {
		onEvent(events.Event{Event: "session.end", SessionID: t.sessionID, Fields: map[string]any{"stop_reason": "cancelled", "final_output": ""}})
	}
}

// recordCancelResult stores the result of the first cancel completion so
// concurrent and duplicate callers observe the same outcome.
func (t *rpcTurn) recordCancelResult(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cancelResult = err
}

// cancelOutcome returns the recorded cancel result. Caller must hold t.mu
// (the lock is non-reentrant and the function reads the same protected field).
func (t *rpcTurn) cancelOutcome() error {
	return t.cancelResult
}

// resolveRPCModel maps an exact CLI slug to the typed enum advertised by agy.
// Empty slugs are rejected because agy 1.1.5 completed an unspecified-model
// probe without producing an assistant reply.
func resolveRPCModel(models *agyv115.FetchAvailableModelsResponse, slug string) (agyv115.Model, error) {
	if slug == "" {
		return agyv115.Model_MODEL_UNSPECIFIED, errRPCModelRequired
	}
	if len(slug) > maxRPCModelSlugBytes || models == nil {
		return agyv115.Model_MODEL_UNSPECIFIED, errRPCModelUnavailable
	}
	details, ok := models.GetModels()[slug]
	if !ok || details == nil || details.GetModel() == agyv115.Model_MODEL_UNSPECIFIED {
		return agyv115.Model_MODEL_UNSPECIFIED, errRPCModelUnavailable
	}
	return details.GetModel(), nil
}

// sendHeld runs the supplied mutation while the per-turn gate is held. The
// mutation sees a non-cancelled turn because the holder already checked.
func (t *rpcTurn) sendHeld(call func() error) error {
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	t.mu.Lock()
	if t.cancelled {
		t.mu.Unlock()
		return errRPCTurnCancelled
	}
	t.sendIssued = true
	t.mu.Unlock()
	return call()
}

func (t *rpcTurn) wasSendIssued() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sendIssued
}

// RunTurn executes one RPC turn without using the PTY as a protocol surface.
// It resolves the requested model before opening a stream, installs the
// snapshot/reconnect coordinator, waits for stream readiness, sends exactly
// once, and returns only after the mapper emits its terminal event.
func (h *ptyRPCHost) RunTurn(ctx context.Context, prompt, modelSlug string, onEvent func(events.Event)) error {
	if h == nil || h.turnGate == nil {
		return errRPCHostClosed
	}
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		return errRPCHostClosed
	}
	select {
	case <-h.turnGate:
		h.turnStarting = true
		h.mu.Unlock()
	default:
		h.mu.Unlock()
		return errRPCTurnActive
	}

	releaseGate := true
	defer func() {
		if releaseGate {
			h.turnGate <- struct{}{}
		}
	}()

	turnCtx, cancel := context.WithCancel(ctx)
	h.mu.Lock()
	if h.closing || h.rpc == nil || h.rpc.client == nil || h.mapper == nil {
		h.turnStarting = false
		h.mu.Unlock()
		cancel()
		return errRPCHostClosed
	}
	if h.pendingCancel {
		h.pendingCancel = false
		h.turnStarting = false
		h.mu.Unlock()
		cancel()
		return errRPCTurnCancelled
	}
	model, err := resolveRPCModel(h.models, modelSlug)
	if err != nil {
		h.turnStarting = false
		h.mu.Unlock()
		cancel()
		return err
	}
	processDone := h.processDone
	select {
	case <-processDone:
		h.turnStarting = false
		h.mu.Unlock()
		cancel()
		return errors.New("agy RPC host exited before turn")
	default:
	}
	turnDone := make(chan struct{})
	turn := &rpcTurn{cancel: cancel, done: turnDone, cancelDone: make(chan struct{}), onEvent: onEvent, sessionID: h.mapper.sessionID}
	coordinator := newTrajectoryRecoveryCoordinator(h.rpc.client, h.mapper, trajectoryRecoveryOptions{
		cascadeID:         h.conversationID,
		conversationID:    h.conversationID,
		snapshotVerbosity: agyv115.SnapshotTrajectoryVerbosity_CLIENT_TRAJECTORY_VERBOSITY_FULL,
		streamVerbosity:   agyv115.StreamTrajectoryVerbosity_CLIENT_TRAJECTORY_VERBOSITY_FULL,
		onEvent:           turn.emit,
		holdAfterReady:    true,
	})
	h.coordinator = coordinator
	h.turnCancel = cancel
	h.turnDone = turnDone
	h.turn = turn
	h.turnStarting = false
	h.mu.Unlock()

	monitorStop := make(chan struct{})
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		select {
		case <-processDone:
			cancel()
		case <-turnCtx.Done():
		case <-monitorStop:
		}
	}()

	go func() { _ = coordinator.Run(turnCtx) }()
	defer func() {
		if h.interactions != nil {
			waitCtx, waitCancel := context.WithTimeout(context.Background(), rpcCancelTimeout())
			if turn.observedRemoteTerminal() && !turn.isCancelled() {
				// A normal completion update may precede the successful Handle
				// response. Retire identity without cancelling that response.
				h.interactions.Evict()
				if err := h.interactions.Wait(waitCtx); err != nil {
					h.interactions.Clear()
				}
			} else {
				_ = h.interactions.CancelAndWait(waitCtx)
			}
			waitCancel()
		}
		_ = coordinator.Close()
		_ = coordinator.Wait(context.Background())
		cancel()
		close(monitorStop)
		<-monitorDone
		if turn.isCancelled() {
			turn.emitCancelled()
		}
		h.mu.Lock()
		if h.coordinator == coordinator {
			h.coordinator = nil
			h.turnCancel = nil
			h.turnDone = nil
			h.turn = nil
		}
		h.mu.Unlock()
		h.turnGate <- struct{}{}
		releaseGate = false
		close(turnDone)
	}()

	if err := coordinator.WaitReady(turnCtx); err != nil {
		return h.turnError(ctx, processDone, turn, err)
	}
	if turn.isCancelled() {
		return errRPCTurnCancelled
	}
	if err := turnCtx.Err(); err != nil {
		return h.turnError(ctx, processDone, turn, err)
	}
	select {
	case <-processDone:
		return errors.New("agy RPC host exited before send")
	default:
	}
	h.mapper.BeginTurn()
	if turn.isCancelled() {
		return errRPCTurnCancelled
	}
	if err := turn.sendHeld(func() error {
		_, err := h.rpc.client.sendUserCascadeMessage(turnCtx, h.conversationID, prompt, model)
		return err
	}); err != nil {
		return h.turnError(ctx, processDone, turn, err)
	}
	coordinator.ReleaseAfterReady()
	if err := coordinator.Wait(turnCtx); err != nil {
		return h.turnError(ctx, processDone, turn, err)
	}
	if turn.isCancelled() {
		return errRPCTurnCancelled
	}
	return nil
}

func (h *ptyRPCHost) turnError(caller context.Context, processDone <-chan struct{}, turn *rpcTurn, err error) error {
	if turn != nil && turn.isCancelled() {
		return errRPCTurnCancelled
	}
	if callerErr := caller.Err(); callerErr != nil {
		return callerErr
	}
	select {
	case <-processDone:
		return errors.New("agy RPC host exited during turn")
	default:
	}
	if errors.Is(err, errTrajectoryRecoveryClosed) || errors.Is(err, context.Canceled) {
		return errRPCHostClosed
	}
	return errors.New("agy RPC turn failed")
}

// ArmCancellation fences a provider cancellation that can race RunTurn entry.
// If no turn is installed yet, the next in-progress start consumes the marker
// before opening a stream or sending a mutation.
func (h *ptyRPCHost) ArmCancellation() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if !h.closing && h.turn == nil {
		h.pendingCancel = true
	}
	h.mu.Unlock()
}

// DisarmCancellation clears an arm that was not consumed by the prompt that
// owned it. Provider invokes this while ending that prompt's running state.
func (h *ptyRPCHost) DisarmCancellation() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.pendingCancel = false
	h.mu.Unlock()
}

// Cancel stops the active typed-RPC turn. A successful RPC cancellation keeps
// the PTY alive for another/resumed turn; only an observed, terminal-stream
// convergence keeps the host reusable. Protocol errors, deadlines, or a stream
// that never converges fall back to interrupting the exact hosted process.
//
// Concurrent and duplicate callers serialize on turn.cancelOnce. Exactly one
// goroutine performs the cascade mutation and fallback, and every waiter
// observes the same recorded result.
func (h *ptyRPCHost) Cancel(ctx context.Context) error {
	if h == nil {
		return errRPCHostClosed
	}
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		return errRPCHostClosed
	}
	turn := h.turn
	bridge := h.interactions
	rpc := h.rpc
	cascadeID := h.conversationID
	if turn == nil {
		if h.turnStarting {
			h.pendingCancel = true
		}
		h.mu.Unlock()
		return nil
	}
	if rpc == nil || rpc.client == nil {
		h.mu.Unlock()
		return errRPCHostClosed
	}
	turn.mu.Lock()
	if turn.cancelStarted {
		done := turn.cancelDone
		turn.mu.Unlock()
		h.mu.Unlock()
		if err := waitRPCTurnCancel(ctx, done); err != nil {
			return err
		}
		turn.mu.Lock()
		result := turn.cancelOutcome()
		turn.mu.Unlock()
		return result
	}
	turn.cancelStarted = true
	turn.mu.Unlock()
	h.mu.Unlock()
	defer close(turn.cancelDone)

	result := h.runCancel(turn, bridge, rpc, cascadeID)
	turn.recordCancelResult(result)
	return result
}

// runCancel executes the cancel pipeline on exactly one goroutine. The gate
// ordering guarantees either SendUserCascadeMessage completed before this
// function marks, or this function marks before the send runs.
func (h *ptyRPCHost) runCancel(turn *rpcTurn, bridge *interactionBridge, rpc *rpcHost, cascadeID string) error {
	// Mark under the same gate as SendUserCascadeMessage. If send already owns
	// the gate it completes first; otherwise it observes cancellation and sends
	// nothing. Only then drain interaction work before the cancel mutation.
	if !turn.markCancelled() {
		return nil
	}
	joinCtx, joinCancel := context.WithTimeout(context.Background(), rpcCancelTimeout())
	if bridge != nil {
		if err := bridge.CancelAndWait(joinCtx); err != nil {
			joinCancel()
			return h.fallbackCancel(turn)
		}
	}
	joinCancel()

	cancelCtx, cancelRPC := context.WithTimeout(context.Background(), rpcCancelTimeout())
	_, err := rpc.client.cancelCascadeSteps(cancelCtx, cascadeID, nil)
	cancelRPC()
	if err != nil {
		return h.fallbackCancel(turn)
	}
	if !turn.wasSendIssued() {
		turn.cancel()
		if !waitRPCTurnDone(turn, rpcCancelTimeout()) {
			return h.fallbackCancel(turn)
		}
		return nil
	}
	if !waitRPCTurnTerminal(turn, rpcCancelTimeout()) {
		return h.fallbackCancel(turn)
	}

	turn.cancel()
	if !waitRPCTurnDone(turn, rpcCancelTimeout()) {
		return h.fallbackCancel(turn)
	}
	return nil
}

// waitRPCTurnTerminal returns true only when the mapper has emitted a terminal
// session.end before the bounded deadline. The local turn state reflects the
// observed terminal via terminalEmitted.
func waitRPCTurnTerminal(turn *rpcTurn, timeout time.Duration) bool {
	if turn == nil {
		return true
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		turn.mu.Lock()
		terminal := turn.remoteTerminalObserved
		turn.mu.Unlock()
		if terminal {
			return true
		}
		select {
		case <-turn.done:
			turn.mu.Lock()
			terminal = turn.remoteTerminalObserved
			turn.mu.Unlock()
			return terminal
		case <-deadline.C:
			return false
		}
	}
}

func waitRPCTurnCancel(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitRPCTurnDone(turn *rpcTurn, timeout time.Duration) bool {
	if turn == nil {
		return true
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	select {
	case <-turn.done:
		return true
	case <-deadline.C:
		turn.cancel()
	}
	second := time.NewTimer(timeout)
	defer second.Stop()
	select {
	case <-turn.done:
		return true
	case <-second.C:
		return false
	}
}

// fallbackCancel permanently terminates the one process this host owns. It
// deliberately interrupts the exact hosted process, gives it a bounded grace
// to exit on its own, escalates to Kill only when needed, and reaps the
// process synchronously before cancelling the lifetime context. Cleanup
// failures are collected sensibly: the post-Interrupt or post-Kill exit status
// is expected, but failure to kill or reap by the bounded deadline is a
// terminal close error.
//
// Concurrent Close calls observe the same single teardown through the
// closing flag and joined waiters.
func (h *ptyRPCHost) fallbackCancel(turn *rpcTurn) error {
	h.mu.Lock()
	if h.closing {
		closed := h.closed
		h.mu.Unlock()
		turn.cancel()
		joined := waitRPCTurnDone(turn, rpcCancelTimeout())
		waitCtx, waitCancel := context.WithTimeout(context.Background(), rpcCancelTimeout())
		defer waitCancel()
		select {
		case <-closed:
			h.mu.Lock()
			err := h.closeErr
			h.mu.Unlock()
			if !joined && err == nil {
				return errRPCCancelCleanup
			}
			return err
		case <-waitCtx.Done():
			return errRPCCancelCleanup
		}
	}
	h.closing = true
	terminalSession := h.terminal
	processDone := h.processDone
	rpc := h.rpc
	interactions := h.interactions
	h.mu.Unlock()

	turn.cancel()
	cleanupFailed := false
	processCleanupFailed := false
	if interactions != nil {
		clearCtx, clearCancel := context.WithTimeout(context.Background(), rpcCancelTimeout())
		if err := interactions.CancelAndWait(clearCtx); err != nil {
			cleanupFailed = true
		}
		clearCancel()
	}
	if rpc != nil {
		rpc.close()
	}

	if terminalSession == nil {
		processCleanupFailed = true
	} else {
		interrupted := false
		if interrupter, ok := terminalSession.(terminal.InterruptSession); ok {
			interrupted = interrupter.Interrupt(context.Background()) == nil
		}
		exited := false
		if interrupted {
			graceTimer := time.NewTimer(rpcGraceTimeout())
			select {
			case <-processDone:
				exited = true
				graceTimer.Stop()
			case <-graceTimer.C:
			}
		}
		if !exited {
			killCtx, killCancel := context.WithTimeout(context.Background(), rpcCancelTimeout())
			_ = terminalSession.Kill(killCtx)
			killCancel()
		}
		reapCtx, reapCancel := context.WithTimeout(context.Background(), rpcCancelTimeout())
		if err := terminalSession.Wait(reapCtx); err != nil && !isExpectedProcessExit(err) {
			processCleanupFailed = true
		}
		reapCancel()
	}

	// CommandContext lifetime cancellation comes only after graceful/forced
	// process cleanup, so it cannot preempt the exact-process Interrupt path.
	if h.cancel != nil {
		h.cancel()
	}
	if processCleanupFailed && terminalSession != nil {
		finalCtx, finalCancel := context.WithTimeout(context.Background(), rpcCancelTimeout())
		if err := terminalSession.Wait(finalCtx); err == nil || isExpectedProcessExit(err) {
			processCleanupFailed = false
		}
		finalCancel()
	}
	if !waitRPCTurnDone(turn, rpcCancelTimeout()) {
		cleanupFailed = true
	}

	var result error
	if cleanupFailed || processCleanupFailed {
		result = errRPCCancelCleanup
	}
	h.mu.Lock()
	if h.closeErr == nil {
		h.closeErr = result
	}
	close(h.closed)
	h.mu.Unlock()
	return result
}

// isExpectedProcessExit classifies the Wait result that follows a successful
// Interrupt or Kill: an os.ErrProcessDone or a clean exit is expected.
func isExpectedProcessExit(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrProcessDone) {
		return true
	}
	// ExitError typically carries the killed-by-signal exit code from a
	// graceful teardown; that is expected after Interrupt.
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return true
	}
	return false
}
