package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/sdougbrown/avenor/internal/messaging"
	"io"
	"net/http"
	"runtime"
	"testing"
	"time"
)

func peerPost(t *testing.T, b *Broker, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(b.URL()+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if json.Unmarshal(data, &result) != nil {
		result = map[string]any{"error": string(data)}
	}
	return resp.StatusCode, result
}

// peerGet reads a GET route as creds, the way a broker client authenticates
// one: the credential travels in headers, never in the URL.
func peerGet(t *testing.T, b *Broker, path string, creds map[string]any) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, b.URL()+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(messaging.HeaderRunID, creds["run_id"].(string))
	req.Header.Set(messaging.HeaderRunToken, creds["token"].(string))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if json.Unmarshal(data, &result) != nil {
		result = map[string]any{"error": string(data)}
	}
	return resp.StatusCode, result
}

func peerBroker(t *testing.T) *Broker {
	t.Helper()
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	return b
}

func TestPeerRetirementFencesIdentityAndHidesIt(t *testing.T) {
	b := peerBroker(t)
	credentials := map[string]any{"run_id": "pi-peer", "token": "secret", "peer": true}
	for range 2 {
		if code, answer := peerPost(t, b, "/register", credentials); code != 200 || answer["peer"] != true {
			t.Fatalf("register = %d %v", code, answer)
		}
	}
	if code, _ := peerPost(t, b, "/retire", credentials); code != 200 {
		t.Fatalf("retire = %d", code)
	}
	// The record outlives the identity: a tombstone is what stops a delayed
	// registration from reviving the inbox, while every reader that routes to
	// the run answers as if it were gone.
	if b.GetRun("pi-peer") == nil {
		t.Fatal("retirement dropped the peer record")
	}
	for _, row := range b.sessions() {
		if row.RunID == "pi-peer" {
			t.Fatal("retired peer listed")
		}
	}
	target := map[string]any{"run_id": "reader", "token": ensureRun(t, b, "reader", "reader")}
	if code, result := peerGet(t, b, "/last_message?target=pi-peer", target); code != http.StatusGone || result["code"] != "peer-retired" {
		t.Fatalf("retired last_message = %d %v", code, result)
	}
	if code, result := peerPost(t, b, "/register", credentials); code != 409 || result["code"] != "peer-retired" {
		t.Fatalf("revive = %d %v", code, result)
	}
	if code, _ := peerPost(t, b, "/poll-control", credentials); code != 410 {
		t.Fatalf("retired poll = %d", code)
	}
}

func TestPeerRetireCannotRemoveProvisionedRun(t *testing.T) {
	b := peerBroker(t)
	token := ensureRun(t, b, "child", "child")
	if code, _ := peerPost(t, b, "/retire", map[string]any{"run_id": "child", "token": token}); code != 403 {
		t.Fatalf("child retire = %d", code)
	}
	if b.GetRun("child") == nil {
		t.Fatal("provisioned child removed")
	}
	if code, _ := peerPost(t, b, "/register", map[string]any{"run_id": "child", "token": token, "peer": true}); code != 409 {
		t.Fatalf("adopt provisioned = %d", code)
	}
}

func TestPeerLeaseExpiryCannotBeRenewedByRegistration(t *testing.T) {
	b := peerBroker(t)
	credentials := map[string]any{"run_id": "pi-peer", "token": "secret", "peer": true}
	if code, _ := peerPost(t, b, "/register", credentials); code != 200 {
		t.Fatal(code)
	}
	st := b.GetRun("pi-peer")
	st.Mu.Lock()
	st.peerDeadline = time.Now().Add(-time.Second)
	st.Mu.Unlock()
	if code, result := peerPost(t, b, "/heartbeat", credentials); code != 410 || result["code"] != "peer-expired" {
		t.Fatalf("expired heartbeat = %d %v", code, result)
	}
	if code, _ := peerPost(t, b, "/register", credentials); code != 409 {
		t.Fatalf("revive expired = %d", code)
	}
	for _, row := range b.sessions() {
		if row.RunID == "pi-peer" {
			t.Fatal("expired peer listed")
		}
	}
}

func TestPeerExpiryReleasesPendingAsks(t *testing.T) {
	b := peerBroker(t)
	creds := map[string]any{"run_id": "peer", "token": "secret", "peer": true}
	if code, _ := peerPost(t, b, "/register", creds); code != 200 {
		t.Fatal(code)
	}
	ensureRun(t, b, "sender", "sender")
	now := time.Now().UTC()
	id, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	gen := b.Store().Generation()
	if _, err = b.LocalMessenger().Send(context.Background(), messaging.SendRequest{
		From: messaging.Identity{RunID: "sender", Sender: "sender"}, To: messaging.Identity{RunID: "peer", Sender: "peer"}, MessageID: id, Generation: gen, Body: `{"message":"question"}`,
	}); err != nil {
		t.Fatal(err)
	}
	st := b.GetRun("peer")
	st.Mu.Lock()
	st.peerDeadline = now.Add(-time.Second)
	st.Mu.Unlock()
	b.expirePeers(now)
	result, err := b.LocalMessenger().Wait(context.Background(), messaging.WaitRequest{Sender: messaging.Identity{RunID: "sender"}, MessageID: id, Generation: gen, Timeout: time.Second})
	if err != nil || result.Outcome != messaging.OutcomeLost {
		t.Fatalf("wait after expiry = %+v %v", result, err)
	}
	if err := b.SendTo("sender", "peer", "agent_message", json.RawMessage(`{"message":"late"}`), "late"); err == nil {
		t.Fatal("send to expired peer succeeded")
	}
}

// A peer holding its activity lock is serving a request, so it is alive. Expiry
// must not wait for it: one wedged request would otherwise hold every other
// peer's tombstone back and leave lapsed peers routable.
func TestPeerExpiryDoesNotWaitOnABusyPeer(t *testing.T) {
	b := peerBroker(t)
	lapsed := time.Now().Add(-time.Second)
	for _, runID := range []string{"busy", "idle"} {
		if code, _ := peerPost(t, b, "/register", map[string]any{"run_id": runID, "token": "secret-" + runID, "peer": true}); code != http.StatusOK {
			t.Fatal(code)
		}
		st, _, ok := b.runStateFor(runID)
		if !ok {
			t.Fatalf("%s is not registered", runID)
		}
		st.Mu.Lock()
		st.peerDeadline = lapsed
		st.Mu.Unlock()
	}

	busy, _, _ := b.runStateFor("busy")
	busy.peerActivity.RLock()
	defer busy.peerActivity.RUnlock()
	done := make(chan struct{})
	go func() { b.expirePeers(time.Now()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("expiry waited on a peer serving a request")
	}

	idle, _, _ := b.runStateFor("idle")
	idle.Mu.Lock()
	idleRetired := idle.peerRetired
	idle.Mu.Unlock()
	if !idleRetired {
		t.Fatal("a busy peer held back another peer's tombstone")
	}
	busy.Mu.Lock()
	busyRetired := busy.peerRetired
	busy.Mu.Unlock()
	if busyRetired {
		t.Fatal("retired a peer that was serving a request")
	}
}

func TestPeerPollingRenewsLeaseAndRegistrationCannotDowngrade(t *testing.T) {
	b := peerBroker(t)
	creds := map[string]any{"run_id": "peer", "token": "secret", "peer": true}
	if code, _ := peerPost(t, b, "/register", creds); code != 200 {
		t.Fatal(code)
	}
	st := b.GetRun("peer")
	st.Mu.Lock()
	st.peerDeadline = time.Now().Add(time.Second)
	st.Mu.Unlock()
	if err := b.PushControl("peer", ControlMessage{ID: "ready", Type: "agent_message"}); err != nil {
		t.Fatal(err)
	}
	if code, _ := peerPost(t, b, "/poll-control", creds); code != 200 {
		t.Fatal(code)
	}
	st.Mu.Lock()
	deadline := st.peerDeadline
	st.Mu.Unlock()
	if time.Until(deadline) < peerLease/2 {
		t.Fatal("poll did not renew lease")
	}
	if code, _ := peerPost(t, b, "/register", map[string]any{"run_id": "peer", "token": "secret"}); code != 409 {
		t.Fatalf("downgrade = %d", code)
	}
}

func TestPeerDuplicatePollRefusedBeforeDrain(t *testing.T) {
	b := New("")
	b.pollTimeout = time.Minute
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	creds := map[string]any{"run_id": "peer", "token": "secret", "peer": true}
	if code, _ := peerPost(t, b, "/register", creds); code != 200 {
		t.Fatal(code)
	}
	first := make(chan int, 1)
	raw, _ := json.Marshal(creds)
	req, err := http.NewRequest(http.MethodPost, b.URL()+"/poll-control", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			first <- 0
			return
		}
		defer resp.Body.Close()
		var messages []ControlMessage
		if json.NewDecoder(resp.Body).Decode(&messages) != nil || len(messages) != 1 || messages[0].ID != "sentinel" {
			first <- 0
			return
		}
		first <- resp.StatusCode
	}()
	st := b.GetRun("peer")
	for {
		st.Mu.Lock()
		polling := st.peerPolling
		st.Mu.Unlock()
		if polling {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("first poll was not admitted")
		default:
			runtime.Gosched()
		}
	}
	if code, result := peerPost(t, b, "/poll-control", creds); code != 409 || result["code"] != "duplicate-consumer" {
		t.Fatalf("duplicate = %d %v", code, result)
	}
	if err := b.PushControl("peer", ControlMessage{ID: "sentinel", Type: "agent_message"}); err != nil {
		t.Fatal(err)
	}
	if code := <-first; code != 200 {
		t.Fatalf("first poll = %d", code)
	}
}

func TestPeerRetirementCannotBeBypassedWithCredentialAlias(t *testing.T) {
	b := peerBroker(t)
	peer := map[string]any{"run_id": "peer", "token": "shared-secret", "peer": true}
	if code, _ := peerPost(t, b, "/register", peer); code != http.StatusOK {
		t.Fatal(code)
	}
	if code, _ := peerPost(t, b, "/retire", peer); code != http.StatusOK {
		t.Fatal(code)
	}
	alias := map[string]any{"run_id": "alias", "token": "shared-secret"}
	if code, _ := peerPost(t, b, "/register", alias); code != http.StatusOK {
		t.Fatal(code)
	}
	ensureRun(t, b, "target", "target")
	alias["from_run_id"] = "peer"
	alias["to_run_id"] = "target"
	alias["type"] = "agent_message"
	alias["payload"] = map[string]any{"id": "alias-message", "message": "should not arrive"}
	code, result := peerPost(t, b, "/send", alias)
	if code != http.StatusForbidden || result["code"] != "sender-mismatch" {
		t.Fatalf("aliased send = %d %v", code, result)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 0 {
		t.Fatalf("target received %d forged messages", queued)
	}
}

func TestPeerPublishesOnlyOwnLastTurn(t *testing.T) {
	b := peerBroker(t)
	peer := map[string]any{"run_id": "peer", "token": "secret", "peer": true}
	if code, _ := peerPost(t, b, "/register", peer); code != http.StatusOK {
		t.Fatal(code)
	}
	ensureRun(t, b, "victim", "victim")
	body := map[string]any{"run_id": "peer", "token": "secret", "final_output": "finished work", "sequence": 1, "stop_reason": "end_turn", "truncated": true}
	if code, result := peerPost(t, b, "/last_message", body); code != http.StatusOK || result["ok"] != true {
		t.Fatalf("record = %d %v", code, result)
	}
	req, err := http.NewRequest(http.MethodGet, b.URL()+"/last_message?target=peer", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Avenor-Run-Id", "peer")
	req.Header.Set("X-Avenor-Run-Token", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var record map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&record); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || record["final_output"] != "finished work" || record["has_record"] != true || record["final_output_truncated"] != true || record["stop_reason"] != "end_turn" {
		t.Fatalf("record read = %d %v", resp.StatusCode, record)
	}
	body["target"] = "victim"
	if code, _ := peerPost(t, b, "/last_message", body); code != http.StatusBadRequest {
		t.Fatalf("cross-target record = %d", code)
	}
	if _, exists := b.LastAgentMessage("victim"); exists {
		t.Fatal("peer wrote another run's record")
	}
	if code, _ := peerPost(t, b, "/retire", peer); code != http.StatusOK {
		t.Fatal(code)
	}
	delete(body, "target")
	if code, _ := peerPost(t, b, "/last_message", body); code != http.StatusGone {
		t.Fatalf("retired write = %d", code)
	}
}

func TestPeerLastTurnSequenceFencesDelayedOlderRequests(t *testing.T) {
	b := peerBroker(t)
	peer := map[string]any{"run_id": "peer", "token": "secret", "peer": true}
	if code, _ := peerPost(t, b, "/register", peer); code != http.StatusOK {
		t.Fatal(code)
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.URL()+"/last_message", reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	olderDone := make(chan int, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			olderDone <- 0
			return
		}
		defer resp.Body.Close()
		olderDone <- resp.StatusCode
	}()
	if _, err := writer.Write([]byte(`{"run_id":"peer","token":"secret","sequence":1,`)); err != nil {
		t.Fatal(err)
	}
	newer := map[string]any{"run_id": "peer", "token": "secret", "sequence": 2, "final_output": "newer", "stop_reason": "end_turn"}
	if code, _ := peerPost(t, b, "/last_message", newer); code != http.StatusOK {
		t.Fatalf("newer = %d", code)
	}
	if _, err := writer.Write([]byte(`"final_output":"older","stop_reason":"end_turn"}`)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if code := <-olderDone; code != http.StatusOK {
		t.Fatalf("delayed older = %d", code)
	}
	newer["final_output"] = "different duplicate body"
	if code, _ := peerPost(t, b, "/last_message", newer); code != http.StatusOK {
		t.Fatalf("retry = %d", code)
	}
	last, ok := b.LastAgentMessage("peer")
	if !ok || last.Text != "newer" {
		t.Fatalf("last after delayed/retried writes = %+v, %v", last, ok)
	}
}

func TestPeerLastTurnRequiresPositiveIntegerSequence(t *testing.T) {
	b := peerBroker(t)
	if code, _ := peerPost(t, b, "/register", map[string]any{"run_id": "peer", "token": "secret", "peer": true}); code != http.StatusOK {
		t.Fatal(code)
	}
	for _, sequence := range []any{nil, 0, -1, 1.5, "1", uint64(9007199254740992)} {
		body := map[string]any{"run_id": "peer", "token": "secret", "final_output": "invalid"}
		if sequence != nil {
			body["sequence"] = sequence
		}
		if code, _ := peerPost(t, b, "/last_message", body); code != http.StatusBadRequest {
			t.Fatalf("sequence %v = %d", sequence, code)
		}
	}
}
