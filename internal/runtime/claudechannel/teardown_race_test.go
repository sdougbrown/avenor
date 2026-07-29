package claudechannel

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

// TestRunSessionTeardownNoSendOnClosedChannel is the claude-channel analogue of
// the claude backend's teardown race: runSession used to `defer close(s.Events)`
// while the async Prompt goroutine and the session.start emitter were still
// sending, panicking with "send on closed channel". The fix is to never close
// s.Events and signal shutdown via context cancellation. Run with -race.
func TestRunSessionTeardownNoSendOnClosedChannel(t *testing.T) {
	const iterations = 10
	for i := 0; i < iterations; i++ {
		p := &Provider{
			sessions: make(map[string]*session),
			broker:   broker.New(""),
			launcher: terminal.PTYLauncher{},
		}
		core := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
			SessionID: "ses-teardown",
			RunID:     "run-teardown",
			EventsBuf: 0, // unbuffered: emitters park in the send, racing the close
		})
		term := terminal.NewFakeSession("test-term", 1, "ready")
		term.SetAlive(false) // sessionGone fires on the first poll → teardown
		s := &session{Session: core}
		s.Term = term
		p.sessions[s.SessionID] = s

		var panicked atomic.Bool
		var wg sync.WaitGroup

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

		p.runSession(s.Ctx, s)

		wg.Wait()
		if panicked.Load() {
			t.Fatalf("iteration %d: send on closed channel during runSession teardown", i)
		}
	}
}

// TestEventsDeliversSessionEndThenClosesOnTeardown pins the same consumer
// contract WaitForSession depends on: the Events() channel must deliver
// session.end (with a stop_reason) and then close when the session ends.
func TestEventsDeliversSessionEndThenClosesOnTeardown(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*session),
		broker:   broker.New(""),
		launcher: terminal.PTYLauncher{},
	}
	core := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-contract",
		RunID:     "run-contract",
		EventsBuf: 8,
	})
	term := terminal.NewFakeSession("test-term", 1, "ready")
	term.SetAlive(false)
	s := &session{Session: core}
	s.Term = term
	p.sessions[s.SessionID] = s

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
				return
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

// TestPollBrokerEventsDeliversViaRunSession exercises the pollBrokerEvents path
// that the other teardown race tests don't cover. pollBrokerEvents uses raw
// s.Events <- sends (not s.Emit) that run in the runSession goroutine via the
// pollTick branch. This test pushes broker reports into a run, starts
// runSession with a live terminal so pollTick fires before sessionGone, and
// verifies the report events are delivered through Events(). It then kills the
// terminal to trigger sessionGone teardown and verifies session.end arrives
// and the channel closes — exercising the full runSession lifecycle including
// the raw-send path.
func TestPollBrokerEventsDeliversViaRunSession(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*session),
		broker:   broker.New(""),
		launcher: terminal.PTYLauncher{},
	}
	core := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-poll",
		RunID:     "run-poll",
		EventsBuf: 64,
	})
	term := terminal.NewFakeSession("test-term", 1, "ready")
	term.SetAlive(true) // keep alive so pollTick fires before sessionGone
	s := &session{Session: core}
	s.Term = term
	p.sessions[s.SessionID] = s

	// Register the run in the broker and push reports so pollBrokerEvents has
	// data to send on the first pollTick.
	if _, err := p.broker.CreateRun("run-poll"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	st := p.broker.GetRun("run-poll")
	if st == nil {
		t.Fatal("GetRun returned nil")
	}
	st.Lock()
	st.RegisteredAt = time.Now()
	st.Reports = append(st.Reports, broker.Report{
		RunID:   "run-poll",
		State:   "working",
		Payload: json.RawMessage(`{"step":1}`),
	})
	st.Reports = append(st.Reports, broker.Report{
		RunID:   "run-poll",
		State:   "thinking",
		Payload: json.RawMessage(`{"step":2}`),
	})
	st.Unlock()

	// Subscribe before runSession starts, the way the supervisor does.
	ch, err := p.Events(context.Background(), s.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	go p.runSession(s.Ctx, s)

	var gotChannelReady bool
	var reportCount int
	var gotEnd bool
	var triggeredTeardown bool
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				if !gotEnd {
					t.Fatal("events channel closed without delivering session.end")
				}
				return
			}
			switch ev.Event {
			case "agent.channel_ready":
				gotChannelReady = true
			case "agent.report":
				reportCount++
			case "session.end":
				if ev.Fields["stop_reason"] == "" {
					t.Fatalf("session.end missing stop_reason: %+v", ev.Fields)
				}
				gotEnd = true
			}
			// Once we've seen the broker events, trigger teardown.
			if gotChannelReady && reportCount >= 2 && !triggeredTeardown {
				term.SetAlive(false)
				triggeredTeardown = true
			}
		case <-deadline:
			t.Fatalf("timeout: gotChannelReady=%v reportCount=%d gotEnd=%v", gotChannelReady, reportCount, gotEnd)
		}
	}
}
