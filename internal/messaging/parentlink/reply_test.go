package parentlink

import (
	"errors"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// replyRequest reports one reply the test link sends up its relay route.
func replyRequest(messageID string) messaging.ReplyRequest {
	return messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		ToRunID:   "run-x",
		MessageID: messageID,
		Body:      `{"message":"pong"}`,
	}
}

// TestLinkReplyCompletesTheRelayedEdgeAndConsumesTheRoute pins the reply's
// read of a relayed ask: the reply reaches the parent as a reply to the
// relayed ask, the route is consumed, and a retried reply finds no route
// instead of posting a second reply.
func TestLinkReplyCompletesTheRelayedEdgeAndConsumesTheRoute(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"replied"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")
	if err := link.RecordRelay(messaging.RelayRoute{
		MessageID:      "msg-1",
		AskerRunID:     "run-x",
		EdgeGeneration: "gen-a",
	}); err != nil {
		t.Fatalf("RecordRelay: %v", err)
	}

	outcome, err := link.Reply(ctx, replyRequest("msg-1"))
	if err != nil || outcome != messaging.OutcomeReplied {
		t.Fatalf("Reply = (%q, %v), want the reply completed", outcome, err)
	}
	forwarded := stub.lastRequest(t, pathSend)
	if forwarded.Body["reply_to"] != "msg-1" || forwarded.Body["to_run_id"] != "run-x" {
		t.Fatalf("forwarded reply = %v, want a reply to the relayed ask", forwarded.Body)
	}

	again, err := link.Reply(ctx, replyRequest("msg-1"))
	if again != messaging.OutcomeEdgeAbsent || err != nil {
		t.Fatalf("second Reply = (%q, %v), want the edge absent", again, err)
	}
	if got := stub.countPath(pathSend); got != 1 {
		t.Fatalf("the parent saw %d reply posts, want the consumed route to end there", got)
	}
}

// TestLinkReplyReportsAnUnrecordedAskWithoutPosting pins the reply's guard:
// a reply to an ask no relay brought here has no route to follow, so it is
// refused without reaching the parent.
func TestLinkReplyReportsAnUnrecordedAskWithoutPosting(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		}
		return http.StatusOK, `{}`
	})
	link := newTestLink(t, stub, "gen-a")

	outcome, err := link.Reply(testContext(t), replyRequest("msg-unknown"))
	if outcome != messaging.OutcomeEdgeAbsent || err != nil {
		t.Fatalf("Reply = (%q, %v), want the edge absent", outcome, err)
	}
	if got := stub.countPath(pathSend); got != 0 {
		t.Fatalf("the parent saw %d reply posts, want none", got)
	}
}

func TestLinkReplyReportsTheProtocolErrorWhenTheParentAnswersWithoutAReplyVerdict(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "no verdict at all",
			body: `{"message_id":"msg-1"}`,
		},
		{
			name: "an outcome a reply never reports",
			body: `{"outcome":"sent","message_id":"msg-1"}`,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := newStubBroker(t, func(req stubRequest) (int, string) {
				switch req.Path {
				case pathRegister:
					return http.StatusOK, `{"generation":"gen-a"}`
				case pathSend:
					return http.StatusOK, testCase.body
				}
				return http.StatusOK, `{}`
			})
			ctx := testContext(t)
			link := newTestLink(t, stub, "gen-a")
			if err := link.RecordRelay(messaging.RelayRoute{
				MessageID:      "msg-1",
				AskerRunID:     "run-x",
				EdgeGeneration: "gen-a",
			}); err != nil {
				t.Fatalf("RecordRelay: %v", err)
			}

			outcome, err := link.Reply(ctx, replyRequest("msg-1"))
			if outcome != messaging.OutcomePossiblyPending {
				t.Fatalf("Reply outcome = %q, want %q", outcome, messaging.OutcomePossiblyPending)
			}
			if !errors.Is(err, ErrUnsupportedParentProtocol) {
				t.Fatalf("Reply error = %v, want ErrUnsupportedParentProtocol", err)
			}
			if got := stub.countPath(pathSend); got != 1 {
				t.Fatalf("the parent saw %d reply posts, want the reply to reach it once", got)
			}
		})
	}
}
