package poo

import (
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

// newBareClient is a client with its tables in place and no process behind it:
// the parts deliver and subscribe touch do not need one.
func newBareClient() *client {
	return &client{
		pending:     map[string]chan rpcReply{},
		subs:        map[string][]chan events.Event{},
		held:        map[string][]events.Event{},
		holds:       map[string]int{},
		submissions: map[string][]events.Event{},
		stderr:      newRollingBuffer(stderrLines),
	}
}

func namedEvent(name string) events.Event {
	return events.Event{Event: name, SessionID: "ses_1"}
}

// TestSubscribeKeepsHeldEventsAheadOfNewerOnes proves the events held before a
// subscriber attached are published before any newer one. A subscriber that
// attaches while the session is still streaming sees every event in order,
// because the flush runs under the client's lock and a concurrent deliver
// therefore waits for it.
func TestSubscribeKeepsHeldEventsAheadOfNewerOnes(t *testing.T) {
	const (
		held     = 4000
		after    = 500
		total    = held + after
		attempts = 20
	)
	for attempt := 0; attempt < attempts; attempt++ {
		c := newBareClient()
		ch := make(chan events.Event, total)
		before := make(chan struct{})
		goAway := make(chan struct{})
		delivered := make(chan struct{})
		go func() {
			defer close(delivered)
			for i := 0; i < held; i++ {
				c.deliver(events.Event{Event: strconv.Itoa(i), SessionID: "ses_1"})
			}
			close(before)
			<-goAway
			for i := held; i < total; i++ {
				c.deliver(events.Event{Event: strconv.Itoa(i), SessionID: "ses_1"})
			}
		}()
		<-before

		subscribed := make(chan struct{})
		go func() {
			defer close(subscribed)
			c.subscribe("ses_1", ch)
		}()
		for {
			c.mu.Lock()
			registered := len(c.subs["ses_1"]) == 1
			c.mu.Unlock()
			if registered {
				break
			}
			runtime.Gosched()
		}
		// Let the stream run again while the flush is still publishing.
		close(goAway)
		<-subscribed
		<-delivered

		for i := 0; i < total; i++ {
			select {
			case ev := <-ch:
				if want := strconv.Itoa(i); ev.Event != want {
					t.Fatalf("attempt %d: event %d = %q, want %q: a newer event overtook a held one", attempt, i, ev.Event, want)
				}
			default:
				t.Fatalf("attempt %d: only %d of %d events arrived", attempt, i, total)
			}
		}
		if extra := len(ch); extra != 0 {
			t.Fatalf("attempt %d: %d events arrived twice", attempt, extra)
		}
	}
}

// TestSubscribeLogsHeldEventDrops proves the held flush publishes through the
// same path as a live event, so a full subscriber buffer reports the drop
// instead of losing the event silently.
func TestSubscribeLogsHeldEventDrops(t *testing.T) {
	c := newBareClient()
	ch := make(chan events.Event, 1)
	c.deliver(namedEvent("first"))
	c.deliver(namedEvent("second"))
	c.subscribe("ses_1", ch)

	if ev := <-ch; ev.Event != "first" {
		t.Fatalf("the buffered event = %q, want the held one", ev.Event)
	}
	if logged := c.stderr.String(); !strings.Contains(logged, `dropped event "second"`) {
		t.Fatalf("stderr = %q, want the dropped event named", logged)
	}
}

// TestReleaseKeepsTheSubmittedEventsAheadOfTheHeldOnes proves a release that
// ends a pause orders the submitted records before everything the backend
// emitted while the pause held the session, whether or not a subscriber has
// attached: with one attached both are published now, and without one both
// stay buffered for the subscriber that attaches next.
func TestReleaseKeepsTheSubmittedEventsAheadOfTheHeldOnes(t *testing.T) {
	cases := []struct {
		name     string
		attached bool
	}{
		{name: "no subscriber yet", attached: false},
		{name: "a subscriber attached", attached: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newBareClient()
			var ch chan events.Event
			if tc.attached {
				ch = make(chan events.Event, 2)
				c.subscribe("ses_1", ch)
			}
			release := c.pauseEvents("ses_1")
			c.deliver(namedEvent("held"))
			submitted := events.Event{Event: "agent.prompt_submitted", SessionID: "ses_1"}
			release(&submitted)
			if !tc.attached {
				ch = make(chan events.Event, 2)
				c.subscribe("ses_1", ch)
			}

			if got := len(ch); got != 2 {
				t.Fatalf("%d events arrived for the subscriber, want the submitted one and the held one", got)
			}
			if ev := <-ch; ev.Event != "agent.prompt_submitted" {
				t.Fatalf("first event = %q, want the submitted one first", ev.Event)
			}
			if ev := <-ch; ev.Event != "held" {
				t.Fatalf("second event = %q, want the held one", ev.Event)
			}
		})
	}
}

func TestParkedPermissionEventsSurviveSubscriptionGap(t *testing.T) {
	c := newBareClient()
	c.subscribed = map[string]bool{}
	first := make(chan events.Event, 4)
	c.subscribe("ses_1", first)
	c.unsubscribe("ses_1", first)
	for _, name := range []string{"agent.message", "permission.request", "avenor.permission.answered", "session.end"} {
		c.deliver(namedEvent(name))
	}
	next := make(chan events.Event, 4)
	c.subscribe("ses_1", next)
	if len(next) != 2 || (<-next).Event != "permission.request" || (<-next).Event != "avenor.permission.answered" {
		t.Fatal("parked permission events were not replayed in order")
	}
}

// TestHeldEventsKeepOnlyTheNewest proves the hold buffer a session queues
// through is bounded: past heldEventLimit the oldest event is dropped, so a
// session whose subscriber never attaches cannot grow the buffer without limit
// and the events it does keep stay the most recent ones.
func TestHeldEventsKeepOnlyTheNewest(t *testing.T) {
	const extra = 5
	total := heldEventLimit + extra

	c := newBareClient()
	for i := 0; i < total; i++ {
		c.deliver(events.Event{Event: strconv.Itoa(i), SessionID: "ses_1"})
	}

	ch := make(chan events.Event, heldEventLimit)
	c.subscribe("ses_1", ch)
	if len(ch) != heldEventLimit {
		t.Fatalf("%d events were delivered, want the %d limit", len(ch), heldEventLimit)
	}
	for i := extra; i < total; i++ {
		if got := (<-ch).Event; got != strconv.Itoa(i) {
			t.Fatalf("event %d = %q, want %q", i, got, strconv.Itoa(i))
		}
	}
}
