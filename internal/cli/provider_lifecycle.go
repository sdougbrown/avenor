package cli

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

var errProviderLifecycleClosing = errors.New("provider lifecycle is closing")

// ProviderLifecycle coordinates untrusted provider calls with provider close.
// Close is requested synchronously when no calls remain and otherwise deferred
// until every admitted Prompt, AnswerPermission, and Cancel call has returned.
type ProviderLifecycle struct {
	provider runtime.Provider

	mu            sync.Mutex
	liveCalls     int
	closing       bool
	closeStarted  bool
	closeDone     chan struct{}
	activeCancels map[string]*providerCancelCall
}

type providerCancelCall struct {
	done chan struct{}
	err  error
}

// ProviderTurn gives one session wait an idempotent Cancel operation while all
// turns on the provider share live-call and close coordination.
type ProviderTurn struct {
	lifecycle *ProviderLifecycle

	cancelOnce sync.Once
	cancelCall *providerCancelCall
}

func NewProviderLifecycle(provider runtime.Provider) *ProviderLifecycle {
	return &ProviderLifecycle{
		provider:      provider,
		closeDone:     make(chan struct{}),
		activeCancels: make(map[string]*providerCancelCall),
	}
}

func (l *ProviderLifecycle) NewTurn() *ProviderTurn {
	return &ProviderTurn{lifecycle: l}
}

func (l *ProviderLifecycle) beginCall() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing {
		return false
	}
	l.liveCalls++
	return true
}

func (l *ProviderLifecycle) finishCall() {
	l.mu.Lock()
	l.liveCalls--
	closeNow := l.startCloseIfDrainedLocked()
	l.mu.Unlock()
	if closeNow {
		// A deferred close must not delay the provider call that made teardown
		// safe. This also contains a third-party Close implementation that blocks.
		go l.closeProvider()
	}
}

// startCloseIfDrainedLocked records the one deferred close after a tracked
// call leaves the lifecycle. l.mu must be held.
func (l *ProviderLifecycle) startCloseIfDrainedLocked() bool {
	if l.closing && l.liveCalls == 0 && !l.closeStarted {
		l.closeStarted = true
		return true
	}
	return false
}

func (l *ProviderLifecycle) closeProvider() {
	if closer, ok := l.provider.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	close(l.closeDone)
}

// RequestClose rejects new tracked calls and closes the provider when all calls
// already admitted to this lifecycle have returned. It never waits for a live
// third-party call.
func (l *ProviderLifecycle) RequestClose() <-chan struct{} {
	l.mu.Lock()
	l.closing = true
	closeNow := l.liveCalls == 0 && !l.closeStarted
	if closeNow {
		l.closeStarted = true
	}
	l.mu.Unlock()
	if closeNow {
		l.closeProvider()
	}
	return l.closeDone
}

// Prompt admits one provider prompt call into close coordination.
func (t *ProviderTurn) Prompt(ctx context.Context, sessionID, prompt string) error {
	if t == nil || t.lifecycle == nil || !t.lifecycle.beginCall() {
		return errProviderLifecycleClosing
	}
	defer t.lifecycle.finishCall()
	return t.lifecycle.provider.Prompt(ctx, sessionID, prompt)
}

// AnswerPermission admits one provider answer call into close coordination.
func (t *ProviderTurn) AnswerPermission(ctx context.Context, sessionID, requestID string, response runtime.PermissionResponse) error {
	if t == nil || t.lifecycle == nil || !t.lifecycle.beginCall() {
		return errProviderLifecycleClosing
	}
	defer t.lifecycle.finishCall()
	return t.lifecycle.provider.AnswerPermission(ctx, sessionID, requestID, response)
}

// RequestCancel starts at most one Cancel for this turn. If another turn's
// Cancel is still live, both turns share that exact call instead of issuing a
// concurrent duplicate against the same provider.
func (t *ProviderTurn) RequestCancel(sessionID string, timeout time.Duration) <-chan struct{} {
	if t == nil || t.lifecycle == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	t.cancelOnce.Do(func() {
		l := t.lifecycle
		l.mu.Lock()
		if call := l.activeCancels[sessionID]; call != nil {
			select {
			case <-call.done:
			default:
				t.cancelCall = call
				l.mu.Unlock()
				return
			}
		}
		call := &providerCancelCall{done: make(chan struct{})}
		t.cancelCall = call
		if l.closing {
			call.err = errProviderLifecycleClosing
			close(call.done)
			l.mu.Unlock()
			return
		}
		l.liveCalls++
		l.activeCancels[sessionID] = call
		l.mu.Unlock()

		go func() {
			cancelCtx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			err := l.provider.Cancel(cancelCtx, sessionID)

			l.mu.Lock()
			call.err = err
			close(call.done)
			if l.activeCancels[sessionID] == call {
				delete(l.activeCancels, sessionID)
			}
			l.liveCalls--
			closeNow := l.startCloseIfDrainedLocked()
			l.mu.Unlock()
			if closeNow {
				go l.closeProvider()
			}
		}()
	})
	return t.cancelCall.done
}

func (t *ProviderTurn) cancelError() error {
	if t == nil || t.cancelCall == nil {
		return nil
	}
	return t.cancelCall.err
}

// permissionTurnProvider routes permission answers through a tracked turn while
// preserving the Provider interface expected by file and control resolvers.
type permissionTurnProvider struct {
	runtime.Provider
	turn *ProviderTurn
}

func (p permissionTurnProvider) AnswerPermission(ctx context.Context, sessionID, requestID string, response runtime.PermissionResponse) error {
	return p.turn.AnswerPermission(ctx, sessionID, requestID, response)
}
