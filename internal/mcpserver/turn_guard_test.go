package mcpserver

import (
	"context"
	"errors"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

type guardedLastClient struct {
	*fakeMessageControlClient
	expected string
}

func (c *guardedLastClient) SessionLastTurnContext(_ context.Context, _, expected string) (messaging.PeerLast, error) {
	c.expected = expected
	return messaging.PeerLast{}, errors.New("turn changed")
}

func TestMessageProviderPinsOperationsToResolvedTurn(t *testing.T) {
	cl := &guardedLastClient{fakeMessageControlClient: &fakeMessageControlClient{fakeClient: &fakeClient{}}}
	cl.sendFunc = func(_ context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
		if req.ExpectedSentinelFile != "/turn-a" {
			t.Errorf("sentinel = %q", req.ExpectedSentinelFile)
		}
		return messaging.SendResult{}, errors.New("turn changed")
	}
	p := newTestMessageProvider(t, cl)
	p.resolvedRun = &RunInfo{RunID: "a", RuntimeID: "rt", SentinelPath: "/turn-a"}
	if _, err := p.Send(context.Background(), messaging.SendRequest{To: messaging.Identity{RunID: "rt"}}); err == nil {
		t.Fatal("send accepted changed turn")
	}
	if _, err := p.Last(context.Background(), "rt"); err == nil || cl.expected != "/turn-a" {
		t.Fatalf("last: expected=%q err=%v", cl.expected, err)
	}
}
