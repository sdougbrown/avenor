package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// stalledLastSeam answers a last-turn read the way a broker that never replies
// does: it holds the read until the caller's context ends.
type stalledLastSeam struct{ messaging.Seam }

func (stalledLastSeam) Last(ctx context.Context, _ string) (messaging.PeerLast, error) {
	<-ctx.Done()
	return messaging.PeerLast{}, fmt.Errorf("%w: %v", messaging.ErrRemoteReadFailed, ctx.Err())
}

// TestLastMessageRouteBoundsAReadThatLeavesTheBroker pins that /last_message
// answers a read another broker never answers as a failed read within the
// route's own deadline, instead of holding the caller open for the request
// timeout of every hop the read crosses.
func TestLastMessageRouteBoundsAReadThatLeavesTheBroker(t *testing.T) {
	b := New("")
	b.lastReadTimeout = 50 * time.Millisecond
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	token, err := b.CreateRun("reader", "reader")
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	b.SetMessenger(stalledLastSeam{Seam: b.LocalMessenger()})

	req, err := http.NewRequest(http.MethodGet, b.URL()+"/last_message?target=held-elsewhere", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(messaging.HeaderRunID, "reader")
	req.Header.Set(messaging.HeaderRunToken, token)
	client := &http.Client{Timeout: 3 * time.Second}

	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("the route held the read open past the client's timeout: %v", err)
	}
	defer resp.Body.Close()
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("the route took %v to give up, want about the route's own deadline", elapsed)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode the response: %v", err)
	}
	if resp.StatusCode != http.StatusBadGateway || body["error"] != messaging.ErrRemoteReadFailed.Error() {
		t.Fatalf("/last_message = (%d, %v), want 502 with the shared failed-read sentence", resp.StatusCode, body)
	}
}
