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
