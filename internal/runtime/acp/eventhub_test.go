package acp

import (
	"strconv"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

// TestBacklogCapHoldsWithoutDroppableEvents pins the backlog bound when every
// retained event is non-droppable. Lifecycle and permission events cannot be
// skipped the way chunks are, but the cap still has to hold: a provider whose
// sessions are never subscribed keeps its backlog until Close, and an unbounded
// one grows for the life of the process.

func TestBacklogCapHoldsWithoutDroppableEvents(t *testing.T) {
	h := newEventHub()
	total := hubBacklogCap + 16

	h.mu.Lock()
	for i := 0; i < total; i++ {
		h.appendBacklogLocked(events.Event{
			Event:     "permission.request",
			SessionID: "ses_cap",
			Fields:    map[string]any{"request_id": "req_" + strconv.Itoa(i)},
		})
	}
	got := len(h.backlog)
	oldest, _ := h.backlog[0].Fields["request_id"].(string)
	newest, _ := h.backlog[got-1].Fields["request_id"].(string)
	h.mu.Unlock()

	if got != hubBacklogCap {
		t.Fatalf("backlog len = %d after %d non-droppable events, want the cap %d", got, total, hubBacklogCap)
	}
	if want := "req_" + strconv.Itoa(total-1); newest != want {
		t.Fatalf("backlog newest = %q, want the last appended event %q", newest, want)
	}
	if want := "req_" + strconv.Itoa(total-hubBacklogCap); oldest != want {
		t.Fatalf("backlog oldest = %q, want %q: the cap keeps the newest window", oldest, want)
	}
}
