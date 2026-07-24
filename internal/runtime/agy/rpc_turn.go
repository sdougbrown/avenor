package agy

import (
	"context"
	"errors"

	"github.com/sdougbrown/avenor/internal/events"
	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
)

const maxRPCModelSlugBytes = 256

var (
	errRPCTurnActive       = errors.New("agy RPC turn is already active")
	errRPCHostClosed       = errors.New("agy RPC host is closed")
	errRPCModelUnavailable = errors.New("requested agy model is unavailable")
	errRPCModelRequired    = errors.New("agy RPC requires an explicit model")
)

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

// RunTurn executes one RPC turn without using the PTY as a protocol surface.
// It resolves the requested model before opening a stream, installs the
// snapshot/reconnect coordinator, waits for stream readiness, sends exactly
// once, and returns only after the mapper emits its terminal event.
func (h *ptyRPCHost) RunTurn(ctx context.Context, prompt, modelSlug string, onEvent func(events.Event)) error {
	if h == nil || h.turnGate == nil {
		return errRPCHostClosed
	}
	select {
	case <-h.turnGate:
		defer func() { h.turnGate <- struct{}{} }()
	default:
		return errRPCTurnActive
	}

	turnCtx, cancel := context.WithCancel(ctx)
	h.mu.Lock()
	if h.closing || h.rpc == nil || h.rpc.client == nil || h.mapper == nil {
		h.mu.Unlock()
		cancel()
		return errRPCHostClosed
	}
	model, err := resolveRPCModel(h.models, modelSlug)
	if err != nil {
		h.mu.Unlock()
		cancel()
		return err
	}
	processDone := h.processDone
	select {
	case <-processDone:
		h.mu.Unlock()
		cancel()
		return errors.New("agy RPC host exited before turn")
	default:
	}
	coordinator := newTrajectoryRecoveryCoordinator(h.rpc.client, h.mapper, trajectoryRecoveryOptions{
		cascadeID:         h.conversationID,
		conversationID:    h.conversationID,
		snapshotVerbosity: agyv115.SnapshotTrajectoryVerbosity_CLIENT_TRAJECTORY_VERBOSITY_FULL,
		streamVerbosity:   agyv115.StreamTrajectoryVerbosity_CLIENT_TRAJECTORY_VERBOSITY_FULL,
		onEvent:           onEvent,
		holdAfterReady:    true,
	})
	turnDone := make(chan struct{})
	h.coordinator = coordinator
	h.turnCancel = cancel
	h.turnDone = turnDone
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
		_ = coordinator.Close()
		_ = coordinator.Wait(context.Background())
		cancel()
		close(monitorStop)
		<-monitorDone
		h.mu.Lock()
		if h.coordinator == coordinator {
			h.coordinator = nil
			h.turnCancel = nil
			h.turnDone = nil
		}
		h.mu.Unlock()
		close(turnDone)
	}()

	if err := coordinator.WaitReady(turnCtx); err != nil {
		return h.turnError(ctx, processDone, err)
	}
	if err := turnCtx.Err(); err != nil {
		return h.turnError(ctx, processDone, err)
	}
	select {
	case <-processDone:
		return errors.New("agy RPC host exited before send")
	default:
	}
	h.mapper.BeginTurn()
	if _, err := h.rpc.client.sendUserCascadeMessage(turnCtx, h.conversationID, prompt, model); err != nil {
		return h.turnError(ctx, processDone, err)
	}
	coordinator.ReleaseAfterReady()
	if err := coordinator.Wait(turnCtx); err != nil {
		return h.turnError(ctx, processDone, err)
	}
	return nil
}

func (h *ptyRPCHost) turnError(caller context.Context, processDone <-chan struct{}, err error) error {
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
