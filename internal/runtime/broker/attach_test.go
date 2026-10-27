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
	runToken, _ := b.EnsureRun("rt_1", "child-a")

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
	b.EnsureRun("rt_1", "child-a")

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
	b.EnsureRun("rt_1", "child-a")
	b.EnsureRun("rt_2", "child-b")
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
// outlive the record it would have joined.
func TestDeleteRunDropsItsAttachCredential(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	b.EnsureRun("rt_1", "child-a")
	credential, ok := b.MintAttach("rt_1")
	if !ok {
		t.Fatal("MintAttach reported no grant")
	}

	b.DeleteRun("rt_1")

	b.mu.RLock()
	_, still := b.attachCreds[credential]
	_, pending := b.attachByRun["rt_1"]
	b.mu.RUnlock()
	if still || pending {
		t.Fatalf("deleted run left a grant behind (creds=%v, pending=%v)", still, pending)
	}
	if status, _ := attachPost(t, b.Addr(), "rt_1", credential); status != http.StatusUnauthorized {
		t.Fatalf("attach after the run was deleted = %d, want 401", status)
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
