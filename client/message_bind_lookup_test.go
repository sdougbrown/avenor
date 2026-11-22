package client_test

import (
	"context"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// TestMessageBindLookupReadsWithoutMinting pins the client side of the read a
// registered command makes: the request carries the configured host and native
// session, the result crosses whole, and a supervisor refusal arrives as the
// typed code the caller reports instead of binding.
func TestMessageBindLookupReadsWithoutMinting(t *testing.T) {
	h := &bindHandler{}
	h.setLookup(messaging.MessageBindResult{
		BrokerURL:  "https://broker.test",
		RunID:      "rt_bound",
		RunToken:   "tok_bound",
		Generation: "g1",
	}, nil)
	c := bindServer(t, h)

	got, err := c.MessageBindLookupContext(context.Background(), messaging.MessageBindRequest{
		Host:      "poo",
		SessionID: "sess_bound",
	})
	if err != nil {
		t.Fatalf("message bind lookup: %v", err)
	}
	if req := h.observedLookup(); req.Host != "poo" || req.SessionID != "sess_bound" {
		t.Fatalf("handler saw request %+v, want host poo session sess_bound", req)
	}
	want := messaging.MessageBindResult{
		BrokerURL:  "https://broker.test",
		RunID:      "rt_bound",
		RunToken:   "tok_bound",
		Generation: "g1",
	}
	if got != want {
		t.Fatalf("lookup result = %+v, want %+v", got, want)
	}
	if h.req != (messaging.MessageBindRequest{}) {
		t.Fatalf("the read bound the conversation: %+v", h.req)
	}
}

// TestMessageBindLookupCarriesTheRefusal pins the refusal path: a conversation
// with no live binding arrives as the typed `refused` code and its message, so a
// caller reports the missing attachment rather than retrying into a mint.
func TestMessageBindLookupCarriesTheRefusal(t *testing.T) {
	h := &bindHandler{}
	h.setLookup(messaging.MessageBindResult{}, &router.ToolError{Code: "refused", Message: `no live conversation binding for host "poo" session "sess_1" on this supervisor; the conversation's inbox feed must bind it first`})
	c := bindServer(t, h)

	_, err := c.MessageBindLookupContext(context.Background(), messaging.MessageBindRequest{
		Host:      "poo",
		SessionID: "sess_1",
	})
	if err == nil {
		t.Fatal("lookup = nil error, want the supervisor's refusal")
	}
	if code := bindErrorCode(t, err); code != "refused" {
		t.Fatalf("error.data.code = %q, want refused", code)
	}
}
