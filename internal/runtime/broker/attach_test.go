package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

func attachPost(t *testing.T, addr, runID, credential string) (int, map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"run_id": runID, "token": credential})
	if err != nil {
		t.Fatalf("encode attach body: %v", err)
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/attach", addr), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /attach: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read attach answer: %v", err)
	}
	var answer map[string]any
	if err := json.Unmarshal(raw, &answer); err != nil {
		// A refusal is plain text; keep it readable for the assertion.
		answer = map[string]any{"error": string(raw)}
	}
	return resp.StatusCode, answer
}

// ensureRun registers a run in process, failing the test rather than the
// launch: every case here provisions through the broker's own seam.
func ensureRun(t *testing.T, b *Broker, runID, label string) string {
	t.Helper()
	token, _, err := b.EnsureRun(runID, label)
	if err != nil {
		t.Fatalf("EnsureRun(%q): %v", runID, err)
	}
	return token
}

// attachTables reports how many grant entries the broker holds. Boundedness is
// the one grant property with no HTTP surface — a table that grows by a dead
// entry per relaunch answers every request correctly — so the boundedness case
// is the only one that reads the tables.
func attachTables(b *Broker) (pending, spent int) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.attachCreds) + len(b.attachByRun), len(b.attachConsumed)
}

// A run takes one inbound consumer. The launch's attach credential is spent by
// the first session that presents it, so a second session holding the same
// environment — a nested harness spawned from the provisioned session's own
// shell — is refused instead of polling the queue beside it.
func TestAttachSpendsTheLaunchCredentialOnce(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	runToken := ensureRun(t, b, "rt_1", "child-a")

	credential, ok := b.MintAttach("rt_1")
	if !ok {
		t.Fatal("MintAttach reported no grant for a run the broker holds")
	}
	if credential == "" || credential == runToken {
		t.Fatalf("attach credential = %q, want a credential of its own", credential)
	}

	status, answer := attachPost(t, b.Addr(), "rt_1", credential)
	if status != http.StatusOK {
		t.Fatalf("attach = (%d, %v), want 200", status, answer)
	}
	if answer["token"] != runToken || answer["run_id"] != "rt_1" {
		t.Fatalf("attach answer = %v, want the run's own credential", answer)
	}

	status, answer = attachPost(t, b.Addr(), "rt_1", credential)
	if status != http.StatusConflict {
		t.Fatalf("second attach = (%d, %v), want 409", status, answer)
	}
	if answer["code"] != "attach-consumed" {
		t.Fatalf("refusal = %v, want code attach-consumed", answer)
	}
	if reason, _ := answer["error"].(string); reason == "" {
		t.Fatalf("refusal = %v, want the reason a session logs", answer)
	}

	// The run itself keeps working: the refusal is about the second consumer,
	// not about the run's own credential.
	if token, ok := b.runToken("rt_1"); !ok || token != runToken {
		t.Fatalf("run credential = (%q, %v), want it unchanged", token, ok)
	}
}

// A resume or a retry is a fresh launch, and a fresh launch gets a fresh
// credential: minting rotates the pending grant, so the previous launch's
// unspent credential cannot join and the new one can.
func TestMintAttachRotatesTheLaunchCredential(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	ensureRun(t, b, "rt_1", "child-a")

	first, ok := b.MintAttach("rt_1")
	if !ok {
		t.Fatal("first MintAttach reported no grant")
	}
	second, ok := b.MintAttach("rt_1")
	if !ok {
		t.Fatal("second MintAttach reported no grant")
	}
	if first == second {
		t.Fatal("the rotated credential is the one it replaced")
	}
	if status, answer := attachPost(t, b.Addr(), "rt_1", first); status != http.StatusUnauthorized {
		t.Fatalf("attach with the replaced credential = (%d, %v), want 401", status, answer)
	}
	if status, answer := attachPost(t, b.Addr(), "rt_1", second); status != http.StatusOK {
		t.Fatalf("attach with the current credential = (%d, %v), want 200", status, answer)
	}
}

// The credential names the run it was minted for: a session cannot spend one
// run's grant on another, and an unknown credential is refused outright.
func TestAttachRefusesForeignAndUnknownCredentials(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	ensureRun(t, b, "rt_1", "child-a")
	ensureRun(t, b, "rt_2", "child-b")
	credential, ok := b.MintAttach("rt_1")
	if !ok {
		t.Fatal("MintAttach reported no grant")
	}

	if status, answer := attachPost(t, b.Addr(), "rt_2", credential); status != http.StatusUnauthorized {
		t.Fatalf("attach with another run's credential = (%d, %v), want 401", status, answer)
	}
	if status, answer := attachPost(t, b.Addr(), "rt_1", "not-a-credential"); status != http.StatusUnauthorized {
		t.Fatalf("attach with an unknown credential = (%d, %v), want 401", status, answer)
	}
	// Neither refusal spent the grant: the launch it belongs to can still join.
	if status, answer := attachPost(t, b.Addr(), "rt_1", credential); status != http.StatusOK {
		t.Fatalf("attach with the genuine credential = (%d, %v), want 200", status, answer)
	}
}

// A removed run takes its grants with it: a credential left behind must not
// outlive the record it would have joined. Both kinds are observable, because a
// grant that outlived its record is answered from the grant and not from the
// record: a forgotten one is 401, a leaked one is 404 (never spent) or 409
// (already spent).
func TestDeleteRunDropsItsAttachCredential(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	ensureRun(t, b, "rt_1", "child-a")
	spent, ok := b.MintAttach("rt_1")
	if !ok {
		t.Fatal("MintAttach reported no grant")
	}
	if status, answer := attachPost(t, b.Addr(), "rt_1", spent); status != http.StatusOK {
		t.Fatalf("attach = (%d, %v), want 200", status, answer)
	}
	pending, ok := b.MintAttach("rt_1")
	if !ok {
		t.Fatal("MintAttach reported no grant for the relaunch")
	}

	b.DeleteRun("rt_1")

	if status, answer := attachPost(t, b.Addr(), "rt_1", pending); status != http.StatusUnauthorized {
		t.Fatalf("attach with the pending grant of a deleted run = (%d, %v), want 401", status, answer)
	}
	if status, answer := attachPost(t, b.Addr(), "rt_1", spent); status != http.StatusUnauthorized {
		t.Fatalf("attach with the spent grant of a deleted run = (%d, %v), want 401", status, answer)
	}
}

// Retiring a run by credential — the parent link's path, which removes a record
// a relay could not reach — forgets the run's grants exactly as DeleteRun does,
// and the same two answers prove it.
func TestDeleteRunIfTokenDropsItsAttachCredential(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	runToken := ensureRun(t, b, "rt_1", "child-a")
	spent, ok := b.MintAttach("rt_1")
	if !ok {
		t.Fatal("MintAttach reported no grant")
	}
	if status, answer := attachPost(t, b.Addr(), "rt_1", spent); status != http.StatusOK {
		t.Fatalf("attach = (%d, %v), want 200", status, answer)
	}
	pending, ok := b.MintAttach("rt_1")
	if !ok {
		t.Fatal("MintAttach reported no grant for the relaunch")
	}

	if b.DeleteRunIfToken("rt_1", "token-from-a-replacement") {
		t.Fatal("DeleteRunIfToken retired a record a different credential owns")
	}
	if !b.DeleteRunIfToken("rt_1", runToken) {
		t.Fatal("DeleteRunIfToken did not retire the run its credential owns")
	}

	if status, answer := attachPost(t, b.Addr(), "rt_1", pending); status != http.StatusUnauthorized {
		t.Fatalf("attach with the pending grant of a retired run = (%d, %v), want 401", status, answer)
	}
	if status, answer := attachPost(t, b.Addr(), "rt_1", spent); status != http.StatusUnauthorized {
		t.Fatalf("attach with the spent grant of a retired run = (%d, %v), want 401", status, answer)
	}
}

// A relaunch replaces the grant it never spent and forgets the one its
// predecessor spent, so the grant tables stay bounded by the runs the broker
// holds instead of growing with every resume or retry.
func TestAttachGrantsStayBoundedAcrossRelaunches(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	ensureRun(t, b, "rt_1", "child-a")

	var last string
	const relaunches = 32
	for i := 0; i < relaunches; i++ {
		credential, ok := b.MintAttach("rt_1")
		if !ok {
			t.Fatalf("relaunch %d: MintAttach reported no grant", i)
		}
		if status, answer := attachPost(t, b.Addr(), "rt_1", credential); status != http.StatusOK {
			t.Fatalf("relaunch %d: attach = (%d, %v), want 200", i, status, answer)
		}
		last = credential
	}

	if pending, spent := attachTables(b); pending > 1 || spent > 1 {
		t.Fatalf("grant tables after %d relaunches = (pending=%d, spent=%d), want at most one of each", relaunches, pending, spent)
	}
	// The bounded history still answers for the launch that spent it, and the
	// grants of long-dead launches are refused rather than joined.
	if status, answer := attachPost(t, b.Addr(), "rt_1", last); status != http.StatusConflict {
		t.Fatalf("replay of the spent credential = (%d, %v), want 409", status, answer)
	}
	if status, answer := attachPost(t, b.Addr(), "rt_1", "a-credential-from-a-dead-launch"); status != http.StatusUnauthorized {
		t.Fatalf("attach with a dead launch's credential = (%d, %v), want 401", status, answer)
	}
}

// Minting requires a run to grant: a launch for a run the broker does not hold
// would hand its child a credential nothing could spend.
func TestMintAttachReportsNoGrantForAnUnknownRun(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	if credential, ok := b.MintAttach("rt_missing"); ok || credential != "" {
		t.Fatalf("MintAttach = (%q, %v), want no grant", credential, ok)
	}
}
