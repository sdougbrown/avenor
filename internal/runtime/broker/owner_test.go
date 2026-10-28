package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
)

// registerOverRoute posts to the broker's open /register route, the way a
// hand-started session joins, and returns the credential that route answered
// with.
func registerOverRoute(t *testing.T, addr, runID, token string) string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"run_id": runID, "token": token})
	if err != nil {
		t.Fatalf("encode register body: %v", err)
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/register", addr), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /register: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var answer map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
		t.Fatalf("decode register answer: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register = %d, want 200", resp.StatusCode)
	}
	return answer["token"]
}

// A record the open /register route created holds a credential its author chose,
// so provisioning refuses to adopt it. This is the substitution the ownership
// rule exists to stop: without it, a local process could squat a run ID before
// the supervisor provisions that child, and the supervisor would mint an attach
// grant for the squatted record — handing the legitimate child the credential
// the squatter already holds.
func TestEnsureRunRefusesARecordTheOpenRouteMinted(t *testing.T) {
	b := New("")
	b.SetIdentity("supervisor-under-test")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	squatted := registerOverRoute(t, b.Addr(), "rt_child", "a-token-the-registrant-chose")

	token, created, err := b.EnsureRun("rt_child", "child-a")
	if !errors.Is(err, ErrRunNotOwned) {
		t.Fatalf("EnsureRun on a registered record = (%q, %v, %v), want ErrRunNotOwned", token, created, err)
	}
	if token != "" || created {
		t.Fatalf("EnsureRun adopted the registrant's credential (token=%q, created=%v)", token, created)
	}

	// The provisioning seam mints nothing for it either, and MintAttach is the
	// second gate: no grant means no /attach answer, so the registrant's token
	// never reaches a launched child.
	if credential, ok := b.MintAttach("rt_child"); ok || credential != "" {
		t.Fatalf("MintAttach = (%q, %v) for a registered record, want no grant", credential, ok)
	}
	if status, answer := attachPost(t, b.Addr(), "rt_child", squatted); status != http.StatusUnauthorized {
		t.Fatalf("attach with the registrant's credential = (%d, %v), want 401", status, answer)
	}
}

// Ownership is the supervisor's, not the process's: a successor that keeps the
// record and comes back under the same identity adopts it, while a broker with
// any other identity refuses it. Run records live in memory today, so the
// hand-off below is what a broker that persisted them would hold after a
// restart.
func TestEnsureRunAdoptsItsOwnRecordAcrossRestarts(t *testing.T) {
	predecessor := New("")
	predecessor.SetIdentity("supervisor-stable-identity")
	if err := predecessor.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = predecessor.Stop() })

	provisioned := ensureRun(t, predecessor, "rt_child", "child-a")

	successor := New("")
	successor.SetIdentity("supervisor-stable-identity")
	if err := successor.Start(); err != nil {
		t.Fatalf("start successor broker: %v", err)
	}
	t.Cleanup(func() { _ = successor.Stop() })
	predecessor.mu.RLock()
	record := predecessor.runs["rt_child"]
	predecessor.mu.RUnlock()
	successor.mu.Lock()
	successor.runs["rt_child"] = record
	successor.mu.Unlock()

	token, created, err := successor.EnsureRun("rt_child", "child-a")
	if err != nil || created {
		t.Fatalf("EnsureRun after restart = (%q, %v, %v), want the adopted record", token, created, err)
	}
	if token != provisioned {
		t.Fatalf("adopted token = %q, want the predecessor's %q", token, provisioned)
	}
	if credential, ok := successor.MintAttach("rt_child"); !ok || credential == "" {
		t.Fatal("MintAttach reported no grant for a record this identity minted")
	}

	// A different supervisor identity is a different writer: the same record is
	// refused, so adoption follows the identity rather than whatever process
	// last held the records.
	other := New("")
	other.SetIdentity("some-other-supervisor")
	if err := other.Start(); err != nil {
		t.Fatalf("start other broker: %v", err)
	}
	t.Cleanup(func() { _ = other.Stop() })
	other.mu.Lock()
	other.runs["rt_child"] = record
	other.mu.Unlock()
	if _, _, err := other.EnsureRun("rt_child", "child-a"); !errors.Is(err, ErrRunNotOwned) {
		t.Fatalf("EnsureRun under another identity = %v, want ErrRunNotOwned", err)
	}
}

// A record the broker minted itself needs no configured identity to be
// adoptable: the process-scoped identity it falls back to is its own, so an
// unconfigured broker provisions and relaunches normally.
func TestUnconfiguredBrokerAdoptsItsOwnRecords(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	first := ensureRun(t, b, "rt_child", "child-a")
	second, created, err := b.EnsureRun("rt_child", "child-a")
	if err != nil || created || second != first {
		t.Fatalf("EnsureRun = (%q, %v, %v), want the token it minted (%q)", second, created, err, first)
	}
}
