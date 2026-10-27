package claudecore

import (
	"context"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

func TestEmitAfterTheSessionEndsStillReachesTheBuffer(t *testing.T) {
	// A terminal session.end is often emitted as the session ends, and the
	// Events readers drain the buffer on shutdown to deliver it. A send that
	// fits must not lose a coin toss against the cancelled context.
	const n = 32
	s := NewSession(context.Background(), SessionOptions{SessionID: "ses-emit", EventsBuf: n})
	s.CancelFn()

	for i := 0; i < n; i++ {
		s.Emit(events.Event{Event: "session.end", SessionID: s.SessionID})
	}

	if got := len(s.Events); got != n {
		t.Fatalf("%d of %d events reached the buffer after the session ended", got, n)
	}
}
