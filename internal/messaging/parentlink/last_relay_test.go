package parentlink

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// getLast reads /last_message on one broker as the named run.
func getLast(t *testing.T, addr, runID, token string, query url.Values) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/last_message?"+query.Encode(), nil)
	if err != nil {
		t.Fatalf("build last_message request: %v", err)
	}
	req.Header.Set(messaging.HeaderRunID, runID)
	req.Header.Set(messaging.HeaderRunToken, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get /last_message: %v", err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

// TestHubReadsTheLastTurnOfARunItsChildOwns pins the relay: the hub holds only an
// adopted record of a child's run, so the turn is read from the child and
// addressed by the key the caller used.
func TestHubReadsTheLastTurnOfARunItsChildOwns(t *testing.T) {
	parent, _, second := crossBranch(t)
	if err := second.broker.SetLastAgentMessage("target", "built the thing", "end_turn", false); err != nil {
		t.Fatalf("record the child run's turn: %v", err)
	}

	last, err := parent.link.Last(context.Background(), second.key("target"))
	if err != nil {
		t.Fatalf("hub Last(child run) = %v, want the child's turn", err)
	}
	if last.RunID != second.key("target") || last.Label != "target" || !last.HasRecord ||
		last.Text != "built the thing" || last.StopReason != "end_turn" || last.Truncated || last.At == 0 {
		t.Fatalf("hub Last(child run) = %+v, want the child's turn under the key the caller used", last)
	}
}

// TestHubReadsAChildRunWithNoTurnAsNoRecord pins that a child run that has not
// finished a turn is a known run with nothing recorded, not an unavailable one.
func TestHubReadsAChildRunWithNoTurnAsNoRecord(t *testing.T) {
	parent, _, second := crossBranch(t)

	last, err := parent.link.Last(context.Background(), second.key("target"))
	if err != nil {
		t.Fatalf("hub Last(child run with no turn) = %v, want no error", err)
	}
	if last.HasRecord || last.RunID != second.key("target") {
		t.Fatalf("hub Last(child run with no turn) = %+v, want has_record=false under the key", last)
	}
}

// TestRelayedReadAsksTheChildForItsOwnRunByCanonicalID pins the request the hub
// sends: the run's canonical ID in the child, authenticated as that run with the
// credential the child registered for it.
func TestRelayedReadAsksTheChildForItsOwnRunByCanonicalID(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{"run_id":"run-b","label":"bravo","has_record":true,"final_output":"done","stop_reason":"end_turn","at":42}`
	})
	registry := NewRegistry()
	relay := newTestRelay(t, registry, "child-one", stub.URL(), "gen-a", "run-b")
	key := ParentRunKey("child-one", "run-b")

	last, err := relay.Last(testContext(t), key)
	if err != nil {
		t.Fatalf("relay.Last = %v, want the child's turn", err)
	}
	if last.RunID != key || last.Label != "bravo" || last.Text != "done" || last.At != 42 {
		t.Fatalf("relay.Last = %+v, want the child's turn under the key", last)
	}
	req := stub.lastRequest(t, pathLast)
	if req.Query.Get("target") != "run-b" {
		t.Fatalf("relayed query = %v, want the canonical run", req.Query)
	}
	if req.Header.Get(messaging.HeaderRunID) != "run-b" || req.Header.Get(messaging.HeaderRunToken) != "tok-b" {
		t.Fatalf("relayed credentials = %v, want the run's own credential", req.Header)
	}
}

// TestDescendantCallersAreNotRelayedAnotherBranchsRead pins the visibility rule
// the peer export follows, on the hub's real route: a run a child owns is read by
// callers outside every child branch, and refused for a caller that is itself a
// child's run, whether it asks the hub directly or through its own broker.
func TestDescendantCallersAreNotRelayedAnotherBranchsRead(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	first := startBranch(t, parent.url(), "asker")
	second := startBranch(t, parent.url(), "target")
	awaitRegistered(t, parent, first.key("asker"), second.key("target"))
	if err := second.broker.SetLastAgentMessage("target", "private to its branch", "end_turn", false); err != nil {
		t.Fatalf("record the child run's turn: %v", err)
	}
	query := url.Values{"target": {second.key("target")}}

	status, body := getLast(t, parent.broker.Addr(), "orchestrator", parent.token(t, "orchestrator"), query)
	if status != http.StatusOK || body["final_output"] != "private to its branch" {
		t.Fatalf("hub run's read of a child run = (%d, %v), want the child's turn", status, body)
	}

	status, body = getLast(t, parent.broker.Addr(), first.key("asker"), first.token(t, "asker"), query)
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("sibling's direct read at the hub = (%d, %v), want 404 target-unavailable", status, body)
	}

	status, body = getLast(t, first.broker.Addr(), "asker", first.token(t, "asker"), query)
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("sibling's read through its own broker = (%d, %v), want 404 target-unavailable", status, body)
	}
}

// TestUnreachableChildIsAFailedReadNotAnUnknownRun pins the failure class: a
// child that cannot be reached is a failed read at every layer, and the hub's
// route reports the shared sentence without naming the child.
func TestUnreachableChildIsAFailedReadNotAnUnknownRun(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	child := startBranch(t, parent.url(), "target")
	awaitRegistered(t, parent, child.key("target"))
	if err := child.broker.Stop(); err != nil {
		t.Fatalf("stop the child broker: %v", err)
	}

	_, err := parent.link.Last(context.Background(), child.key("target"))
	if !errors.Is(err, messaging.ErrRemoteReadFailed) || errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("hub Last with the child gone = %v, want ErrRemoteReadFailed and not target-unavailable", err)
	}

	status, body := getLast(t, parent.broker.Addr(), "orchestrator", parent.token(t, "orchestrator"),
		url.Values{"target": {child.key("target")}})
	if status != http.StatusBadGateway || body["error"] != messaging.ErrRemoteReadFailed.Error() {
		t.Fatalf("hub /last_message with the child gone = (%d, %v), want 502 with the shared sentence", status, body)
	}
	if !parent.link.Registry().Owns(child.key("target")) {
		t.Fatal("a failed read retired the child's claim")
	}
}

// TestRelayedReadLeavesTheRegistryUntouched pins that a read has no side effect
// on routing: a child that refuses the run, or its credential, is reported as an
// unknown run while the claim stays for the write relay to retire.
func TestRelayedReadLeavesTheRegistryUntouched(t *testing.T) {
	cases := map[string]struct {
		status int
		body   string
	}{
		"unknown run":   {http.StatusNotFound, `{"code":"target-unavailable","error":"run not found: run-b"}`},
		"bad token":     {http.StatusUnauthorized, `unauthorized`},
		"expired peer":  {http.StatusGone, `{"code":"peer-expired","error":"peer identity is no longer active"}`},
		"unknown route": {http.StatusNotFound, ``},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			stub := newStubBroker(t, func(stubRequest) (int, string) { return c.status, c.body })
			registry := NewRegistry()
			relay := newTestRelay(t, registry, "child-one", stub.URL(), "gen-a", "run-b")
			key := ParentRunKey("child-one", "run-b")

			if _, err := relay.Last(testContext(t), key); !errors.Is(err, messaging.ErrTargetUnavailable) {
				t.Fatalf("relay.Last = %v, want ErrTargetUnavailable", err)
			}
			if _, ok := registry.Hop(key); !ok {
				t.Fatal("a read retired the claim")
			}
		})
	}
}

// TestRelayedReadRefusesAClaimWithNoCredentialBeforeAnyHTTP pins that a
// registered run whose claim carries no credential reads as unknown and the
// relay sends nothing to the child.
func TestRelayedReadRefusesAClaimWithNoCredentialBeforeAnyHTTP(t *testing.T) {
	stub := newStubBroker(t, func(stubRequest) (int, string) { return http.StatusOK, `{}` })
	registry := NewRegistry()
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "child-one",
		Endpoint:   stub.URL(),
		Generation: "gen-a",
		Runs:       []messaging.RunClaim{{RunID: "run-b", Token: ""}},
	}); err != nil {
		t.Fatalf("register the tokenless claim: %v", err)
	}
	relay, err := newRelay(Options{Generation: func() string { return "gen-a" }}, registry)
	if err != nil {
		t.Fatalf("newRelay: %v", err)
	}
	t.Cleanup(func() { _ = relay.Close() })

	_, err = relay.Last(testContext(t), ParentRunKey("child-one", "run-b"))
	if !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("relay.Last = %v, want ErrTargetUnavailable", err)
	}
	if got := stub.countPath(pathLast); got != 0 {
		t.Fatalf("the child saw %d reads, want the credential guard to fire before any HTTP call", got)
	}
}

// TestRelayedReadKeepsAnUnreadableAnswerOutOfTheRunClasses pins that a child
// whose answer is not a last-turn record is neither an unknown run nor a run
// with no turn: it reports the protocol failure, which the edges refuse as a
// failed read.
func TestRelayedReadKeepsAnUnreadableAnswerOutOfTheRunClasses(t *testing.T) {
	for name, body := range map[string]string{"empty body": ``, "non-JSON body": `not a record`} {
		t.Run(name, func(t *testing.T) {
			stub := newStubBroker(t, func(stubRequest) (int, string) { return http.StatusOK, body })
			registry := NewRegistry()
			relay := newTestRelay(t, registry, "child-one", stub.URL(), "gen-a", "run-b")

			_, err := relay.Last(testContext(t), ParentRunKey("child-one", "run-b"))
			if !errors.Is(err, ErrUnsupportedParentProtocol) || errors.Is(err, messaging.ErrTargetUnavailable) {
				t.Fatalf("relay.Last = %v, want ErrUnsupportedParentProtocol and not target-unavailable", err)
			}
		})
	}
}

// TestRelayedReadReportsAServerErrorAsAFailedRead pins that a child that answers
// with a server error is a failed read, not an unknown run.
func TestRelayedReadReportsAServerErrorAsAFailedRead(t *testing.T) {
	stub := newStubBroker(t, func(stubRequest) (int, string) { return http.StatusInternalServerError, `{}` })
	registry := NewRegistry()
	relay := newTestRelay(t, registry, "child-one", stub.URL(), "gen-a", "run-b")

	_, err := relay.Last(testContext(t), ParentRunKey("child-one", "run-b"))
	if !errors.Is(err, messaging.ErrRemoteReadFailed) || errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("relay.Last = %v, want ErrRemoteReadFailed and not target-unavailable", err)
	}
}
