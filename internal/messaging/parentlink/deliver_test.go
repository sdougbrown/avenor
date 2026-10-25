package parentlink

import (
	"encoding/json"
	"errors"
	"github.com/sdougbrown/avenor/internal/messaging"
	"net/http"
	"testing"
)

// TestLinkTellForwardsTheKindWithoutEdgeFields pins the tell forward: the
// parent receives the message under the kind it was given, and carries no
// field an ask edge owns, because a tell admits no edge.
func TestLinkTellForwardsTheKindWithoutEdgeFields(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	payload, err := json.Marshal(map[string]string{"note": "hello"})
	if err != nil {
		t.Fatalf("marshal the payload: %v", err)
	}
	if err := link.Tell(ctx, messaging.TellRequest{
		FromRunID: "run-a",
		ToRunID:   "run-b",
		Kind:      "status_update",
		Payload:   payload,
	}); err != nil {
		t.Fatalf("Tell: %v", err)
	}
	forwarded := stub.lastRequest(t, pathSend)
	if forwarded.Body["type"] != "status_update" {
		t.Fatalf("forwarded kind = %v, want the delivered kind", forwarded.Body["type"])
	}
	for _, field := range []string{"expects_reply", "message_id"} {
		if _, carried := forwarded.Body[field]; carried {
			t.Fatalf("forwarded tell carries %q, which only an ask edge owns", field)
		}
	}
}

// TestLinkTellReportsAnUnreachableParent pins the tell's failure: a
// delivery the parent cannot receive is an error, not a silent success.
func TestLinkTellReportsAnUnreachableParent(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{"generation":"gen-a"}`
	})
	closed := stub.URL()
	stub.server.Close()

	link, err := NewLink(Options{
		ParentURL:  closed,
		Endpoint:   "http://child-one",
		ChildID:    "child-one",
		Runs:       testRuns,
		Generation: func() string { return "gen-a" },
	})
	if err != nil {
		t.Fatalf("NewLink: %v", err)
	}
	t.Cleanup(func() { _ = link.Close() })

	err = link.Tell(testContext(t), messaging.TellRequest{
		FromRunID: "run-a",
		ToRunID:   "run-b",
		Kind:      "status_update",
		Payload:   json.RawMessage(`{}`),
	})
	if !errors.Is(err, ErrParentUnavailable) {
		t.Fatalf("Tell = %v, want ErrParentUnavailable", err)
	}
}
