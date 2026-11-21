package stable

import (
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestMessageBindLookupAnswersTheLiveBinding pins the read a registered command
// makes: once the feed has bound the conversation, the lookup answers the same
// run and credential the bind did, and reports it as an existing binding rather
// than a new one.
func TestMessageBindLookupAnswersTheLiveBinding(t *testing.T) {
	t.Parallel()
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "lookup-live")})
	bound, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "poo", SessionID: "ses_live"})
	if err != nil {
		t.Fatalf("message_bind: %v", err)
	}

	got, err := sup.MessageBindLookup(bindCaller(), messaging.MessageBindRequest{Host: "poo", SessionID: "ses_live"})
	if err != nil {
		t.Fatalf("message_bind_lookup: %v", err)
	}
	if got.RunID != bound.RunID || got.RunToken != bound.RunToken || got.BrokerURL != bound.BrokerURL {
		t.Fatalf("lookup = %+v, want the bind's run %q, token and broker URL", got, bound.RunID)
	}
	if got.Created {
		t.Fatal("the lookup reports the binding as created; only a bind mints")
	}
}

// TestMessageBindLookupNeverBinds pins the refusal that keeps a lookup from
// standing in for the feed: a conversation nothing bound on this supervisor is
// refused, and the refusal mints nothing — the bind that follows is the first
// one and reports created.
func TestMessageBindLookupNeverBinds(t *testing.T) {
	t.Parallel()
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "lookup-unbound")})

	_, err := sup.MessageBindLookup(bindCaller(), messaging.MessageBindRequest{Host: "poo", SessionID: "ses_unbound"})
	if err == nil || !strings.Contains(err.Error(), "no live conversation binding") {
		t.Fatalf("lookup error = %v, want the missing-binding refusal", err)
	}

	first, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "poo", SessionID: "ses_unbound"})
	if err != nil {
		t.Fatalf("message_bind: %v", err)
	}
	if !first.Created {
		t.Fatal("the bind after a refused lookup reused a run; the lookup minted one")
	}
}

// TestMessageBindLookupRefusesAStaleBinding pins the stale case: a bound run the
// broker reports retired is refused on every read, and the read never reclaims
// the binding or mints a replacement behind the inbox. The refusal names the
// conversation and what to do about it.
func TestMessageBindLookupRefusesAStaleBinding(t *testing.T) {
	t.Parallel()
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "lookup-stale")})
	bound, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "poo", SessionID: "ses_stale"})
	if err != nil {
		t.Fatalf("message_bind: %v", err)
	}
	retireBoundPeer(t, sup, bound)

	for attempt := 0; attempt < 2; attempt++ {
		_, err := sup.MessageBindLookup(bindCaller(), messaging.MessageBindRequest{Host: "poo", SessionID: "ses_stale"})
		if err == nil || !strings.Contains(err.Error(), "bind it again") {
			t.Fatalf("lookup attempt %d error = %v, want the stale-binding refusal", attempt+1, err)
		}
		if token, ok := sup.broker.OwnedReachableToken(bound.RunID); ok {
			t.Fatalf("lookup attempt %d left a live record for the retired run %s (token %q)", attempt+1, bound.RunID, token)
		}
	}
}

// TestMessageBindLookupRequiresTheOwnerConnection pins the defense in depth the
// bind already carries: a broker-authenticated caller is refused, because the
// read hands out a conversation's credential.
func TestMessageBindLookupRequiresTheOwnerConnection(t *testing.T) {
	t.Parallel()
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "lookup-owner")})
	_, err := sup.MessageBindLookup(messaging.Caller{Kind: messaging.CallerBroker, RunID: "r1"}, messaging.MessageBindRequest{Host: "poo", SessionID: "ses_1"})
	if err == nil || !strings.Contains(err.Error(), "owner connection") {
		t.Fatalf("lookup error = %v, want the owner-connection refusal", err)
	}
}
