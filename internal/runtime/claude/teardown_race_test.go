package claude

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

// TestRunSessionTeardownNoSendOnClosedChannel reproduces the panic
//
//	panic: send on closed channel
//	claudecore.(*Session).Emit ... claude.(*Provider).Prompt.func1
//
// runSession used to `defer close(s.Events)`, but s.Events has several
// concurrent senders (the async Prompt goroutine, the session.start emitter,
// Cancel/forceCancel). Closing a channel that other live goroutines still send
// to is unsafe: an in-flight Emit racing the close panics. The fix is to never
// close s.Events and signal shutdown via context cancellation instead.
//
// This drives runSession to teardown (via a dead terminal → sessionGone) while
// several goroutines hammer Emit, and asserts none of them panic. The context
// stays live across the close, which is exactly the window the old code
// panicked in. Run with -race for maximum sensitivity.
func TestRunSessionTeardownNoSendOnClosedChannel(t *testing.T) {
	const iterations = 10
	for i := 0; i < iterations; i++ {
		p := &Provider{
			sessions: make(map[string]*claudecore.Session),
			launcher: terminal.PTYLauncher{},
		}
		s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
			SessionID: "ses-teardown",
			EventsBuf: 0, // unbuffered: emitters park in the send, racing the close
		})
		term := terminal.NewFakeSession("test-term", 1, "ready")
		term.SetAlive(false) // sessionGone fires on the first poll → teardown
		s.Term = term
		p.sessions[s.SessionID] = s

		var panicked atomic.Bool
		var wg sync.WaitGroup

		// Concurrent emitters stand in for the async Prompt goroutine and
		// Cancel emits that race runSession's teardown.
		for e := 0; e < 6; e++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						panicked.Store(true)
					}
				}()
				for {
					select {
					case <-s.Ctx.Done():
						return
					default:
					}
					s.Emit(events.Event{Event: "agent.status", SessionID: s.SessionID})
				}
			}()
		}

		// Drain so the unbuffered emitters keep making progress (and keep racing
		// the close) rather than all parking on a full/idle channel.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-s.Events:
				case <-s.Ctx.Done():
					return
				}
			}
		}()

		p.runSession(s.Ctx, s) // returns after sessionGone; runs teardown defers

		wg.Wait()
		if panicked.Load() {
			t.Fatalf("iteration %d: send on closed channel during runSession teardown", i)
		}
	}
}

// TestEventsDeliversSessionEndThenClosesOnTeardown pins the consumer contract
// that WaitForSession depends on: the channel returned by Events() must deliver
// the terminal session.end event (carrying a stop_reason) and then close. If it
// closes without a stop_reason, WaitForSession reports ExitCode 1 — the "looks
// like it failed even though the turn completed" symptom. If it never closes,
// WaitForSession hangs until its own ctx is cancelled.
//
// Because s.Events is no longer closed on teardown, this guards the Events()
// reader's drain-then-close-on-session-end path.
func TestEventsDeliversSessionEndThenClosesOnTeardown(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*claudecore.Session),
		launcher: terminal.PTYLauncher{},
	}
	s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-contract",
		EventsBuf: 8,
	})
	term := terminal.NewFakeSession("test-term", 1, "ready")
	term.SetAlive(false)
	s.Term = term
	p.sessions[s.SessionID] = s

	// Subscribe the way the supervisor does, before the session starts running.
	ch, err := p.Events(context.Background(), s.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	go p.runSession(s.Ctx, s)

	var gotEnd bool
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				if !gotEnd {
					t.Fatal("events channel closed without delivering session.end (WaitForSession would read ExitCode 1)")
				}
				return // closed after session.end — correct
			}
			if ev.Event == "session.end" {
				if ev.Fields["stop_reason"] == "" {
					t.Fatalf("session.end missing stop_reason: %+v", ev.Fields)
				}
				gotEnd = true
			}
		case <-deadline:
			t.Fatal("timeout: events channel neither delivered session.end nor closed")
		}
	}
}
