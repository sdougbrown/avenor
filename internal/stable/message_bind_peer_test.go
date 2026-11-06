package stable

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	runtimebroker "github.com/sdougbrown/avenor/internal/runtime/broker"
)

// registerBoundPeer registers a bound run as a peer the way the bound host's
// client does: the credentials message_bind returned, posted to the
// registration route with the peer flag. A minted bind that is not a leased
// peer record is refused here as a registration mismatch.
func registerBoundPeer(t *testing.T, sup *Supervisor, res messaging.MessageBindResult) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"run_id": res.RunID, "token": res.RunToken, "peer": true})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+sup.broker.Addr()+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var answer map[string]any
	if json.Unmarshal(data, &answer) != nil {
		t.Fatalf("peer registration body: %s", data)
	}
	if resp.StatusCode != http.StatusOK || answer["peer"] != true {
		t.Fatalf("bound run peer registration = %d %v", resp.StatusCode, answer)
	}
	return answer
}

// TestMessageBindMintsALeasedPeerRun pins the leased-bind contract: a minting
// host's bound run is a broker peer record the host can register under the
// credentials message_bind returned, so the broker's own lease and self-retire
// semantics — not a record that lives forever — back the conversation.
func TestMessageBindMintsALeasedPeerRun(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	res, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_peer"})
	if err != nil {
		t.Fatalf("message bind: %v", err)
	}
	answer := registerBoundPeer(t, sup, res)
	if answer["run_id"] != res.RunID || answer["token"] != res.RunToken {
		t.Fatalf("peer registration = %v, want the bound credentials echoed", answer)
	}

	repeat, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_peer"})
	if err != nil {
		t.Fatalf("repeated bind: %v", err)
	}
	if repeat.RunID != res.RunID || repeat.RunToken != res.RunToken || repeat.Created {
		t.Fatalf("repeated bind = %+v, want the bound run reused without minting", repeat)
	}
}

// retireBoundPeer retires a bound run the way the bound host's client does:
// the run's own credentials posted to the broker's production retire route.
func retireBoundPeer(t *testing.T, sup *Supervisor, res messaging.MessageBindResult) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"run_id": res.RunID, "token": res.RunToken})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+sup.broker.Addr()+"/retire", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bound run self-retire = %d", resp.StatusCode)
	}
}

// TestMessageBindRebindsVisiblyOnARetiredBind pins the reclamation contract: a
// bind whose run retired — self-retired or lease-lapsed — is re-answered with a
// fresh, visibly minted identity, never the old credential, and the retired
// record keeps its tombstone and its credential until the broker's own
// tombstone window reaps it.
func TestMessageBindRebindsVisiblyOnARetiredBind(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	res, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_dead"})
	if err != nil {
		t.Fatalf("message bind: %v", err)
	}
	if err := sup.broker.PushControl(res.RunID, runtimebroker.ControlMessage{ID: "m1", Type: "agent_message", RunID: res.RunID}); err != nil {
		t.Fatalf("queue for the bound run: %v", err)
	}
	retireBoundPeer(t, sup, res)

	rebound, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_dead"})
	if err != nil {
		t.Fatalf("rebind after retire: %v", err)
	}
	if !rebound.Created {
		t.Fatal("a rebind after retire must mint visibly, not fail closed")
	}
	if rebound.RunID == res.RunID || rebound.RunToken == res.RunToken {
		t.Fatalf("rebind = %+v, want a fresh identity beside %+v", rebound, res)
	}
	if stale := sup.broker.GetRun(res.RunID); stale == nil || stale.Token != res.RunToken {
		t.Fatal("the rebind must not revive or retarget the retired record")
	}
	if fresh := sup.broker.GetRun(rebound.RunID); fresh == nil || len(fresh.ControlQueue) != 0 {
		t.Fatal("the fresh bind must carry no replayed queue from the retired record")
	}

	repeat, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_dead"})
	if err != nil {
		t.Fatalf("repeated rebind: %v", err)
	}
	if repeat.Created || repeat.RunID != rebound.RunID || repeat.RunToken != rebound.RunToken {
		t.Fatalf("repeated rebind = %+v, want %+v reused without minting", repeat, rebound)
	}

	staleBody, err := json.Marshal(map[string]any{"run_id": res.RunID, "token": res.RunToken, "peer": true})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+sup.broker.Addr()+"/register", "application/json", bytes.NewReader(staleBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("the retired bind's stale credential must stay refused after the rebind")
	}
}

// TestMessageBindReclaimsAfterReapWithoutAdoptingAnAttackerClaimedRecord pins
// the substitution guard on the reclamation path: once the broker's tombstone
// window has reaped a bound record, the open /register route can claim the
// freed id under a token the claimant chose, and a rebind must treat that
// unowned record as unavailable — reclaiming onto a fresh identity — instead
// of answering the bind with the attacker's credential.
func TestMessageBindReclaimsAfterReapWithoutAdoptingAnAttackerClaimedRecord(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	res, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_swap"})
	if err != nil {
		t.Fatalf("message bind: %v", err)
	}
	retireBoundPeer(t, sup, res)
	if !sup.broker.DeleteRunIfToken(res.RunID, res.RunToken) {
		t.Fatal("the production retirement path must remove the retired bound record")
	}

	const attackerToken = "attacker-chosen-token"
	claim, err := json.Marshal(map[string]any{"run_id": res.RunID, "token": attackerToken})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+sup.broker.Addr()+"/register", "application/json", bytes.NewReader(claim))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the freed id's open registration = %d, want the attacker's claim accepted", resp.StatusCode)
	}

	rebound, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_swap"})
	if err != nil {
		t.Fatalf("rebind under an attacker-claimed id: %v", err)
	}
	if !rebound.Created || rebound.RunID == res.RunID {
		t.Fatalf("rebind under an attacker-claimed id = %+v, want a fresh minted identity beside %s", rebound, res.RunID)
	}
	if rebound.RunToken == attackerToken || rebound.RunToken == res.RunToken {
		t.Fatal("the rebind must never answer with the attacker's or the retired record's credential")
	}
	if stale := sup.broker.GetRun(res.RunID); stale == nil || stale.Token != attackerToken {
		t.Fatal("the attacker's record must survive the rebind untouched, never adopted or rotated")
	}
	if err := sup.broker.PushControl(rebound.RunID, runtimebroker.ControlMessage{ID: "m2", Type: "agent_message", RunID: rebound.RunID}); err != nil {
		t.Fatalf("queue for the rebound run: %v", err)
	}
	registerBoundPeer(t, sup, rebound)

	repeat, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_swap"})
	if err != nil {
		t.Fatalf("repeated rebind: %v", err)
	}
	if repeat.Created || repeat.RunID != rebound.RunID || repeat.RunToken != rebound.RunToken {
		t.Fatalf("repeated rebind = %+v, want %+v reused without minting", repeat, rebound)
	}
}

