package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestConcurrentClaimRefreshDoesNotRaceAuthentication pins that the credential a
// descendant run authenticates with is read under the same lock a replacement
// branch claim writes it with. Two refresh shapes run against authenticating
// callers: a replacement branch rotating the credential, and the periodic
// re-registration a live branch publishes, which rewrites the credential record
// with the value it already holds. Every read the callers make overlaps a write,
// so the race detector reports any reader that drops the broker mutex between the
// lookup and the comparison.
func TestConcurrentClaimRefreshDoesNotRaceAuthentication(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	if _, err := b.CreateRun("asker", "asker"); err != nil {
		t.Fatalf("create the asking run: %v", err)
	}
	if _, err := b.CreateRun("target", "target"); err != nil {
		t.Fatalf("create the target run: %v", err)
	}
	// The credential the run authenticates with before any rotation, so the
	// replaced-credential check at the end names one the rotation loop is
	// guaranteed to have replaced rather than one a starved loop may never
	// have moved past.
	original, ok := b.RunToken("asker")
	if !ok {
		t.Fatal("the asking run has no credential")
	}

	// A replacement claim rotates the credential, so a request whose credential
	// did not move while it was in flight must authenticate.
	var rotating rotationLog
	rotatingStop := make(chan struct{})
	var rotatingDone sync.WaitGroup
	rotatingDone.Add(1)
	go func() {
		defer rotatingDone.Done()
		for round := 0; ; round++ {
			select {
			case <-rotatingStop:
				return
			default:
			}
			rotating.write(func() {
				b.ownRun(messaging.ClaimedRun{
					RunClaim: messaging.RunClaim{RunID: "asker", Label: "asker", Token: fmt.Sprintf("rotation-%d", round)},
					Key:      "asker",
				})
			})
		}
	}()

	var callers sync.WaitGroup
	for caller := 0; caller < 4; caller++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			for round := 0; round < 100; round++ {
				token, ok := b.RunToken("asker")
				if !ok {
					t.Errorf("the run disappeared while its credential was refreshed")
					return
				}
				started, done := rotating.snapshot()
				status, err := authenticatedGet(b.Addr(), "asker", token)
				if err != nil {
					t.Errorf("authenticate: %v", err)
					return
				}
				settledStarted, settledDone := rotating.snapshot()
				if started == settledStarted && done == settledDone && status != http.StatusOK {
					// A write that incremented started before the first snapshot can
					// increment done after the second, which reads as no overlap while
					// the credential switched mid-request. The record then no longer
					// carries the credential the request used, so retry once with the
					// credential the record carries now; a refusal under a credential
					// the record still carries is a real failure.
					current, ok := b.RunToken("asker")
					if !ok {
						t.Errorf("the run disappeared while its credential was refreshed")
						return
					}
					if current != token {
						status, err = authenticatedGet(b.Addr(), "asker", current)
						if err != nil {
							t.Errorf("authenticate with the refreshed credential: %v", err)
							return
						}
					}
					if status != http.StatusOK {
						t.Errorf("authentication with the current credential %q = %d, want 200", token, status)
						return
					}
				}
			}
		}()
	}
	callers.Wait()
	close(rotatingStop)
	rotatingDone.Wait()

	// The periodic re-registration of a live branch rewrites the run's credential
	// record with the credential it already holds, while the run keeps serving.
	// Both the route and the send handler authenticate against that record.
	steady, ok := b.RunToken("asker")
	if !ok {
		t.Fatal("the refreshed run lost its record")
	}
	steadyStop := make(chan struct{})
	var steadyDone sync.WaitGroup
	steadyDone.Add(1)
	go func() {
		defer steadyDone.Done()
		for {
			select {
			case <-steadyStop:
				return
			default:
			}
			b.ownRun(messaging.ClaimedRun{
				RunClaim: messaging.RunClaim{RunID: "asker", Label: "asker", Token: steady},
				Key:      "asker",
			})
		}
	}()
	for caller := 0; ; caller++ {
		if caller == 50 {
			break
		}
		if status, err := authenticatedGet(b.Addr(), "asker", steady); err != nil || status != http.StatusOK {
			t.Fatalf("authentication during a re-registration = (%d, %v), want 200", status, err)
		}
		if status, err := authenticatedSend(b.Addr(), "asker", steady, "target"); err != nil || status != http.StatusOK {
			t.Fatalf("send during a re-registration = (%d, %v), want 200", status, err)
		}
	}
	close(steadyStop)
	steadyDone.Wait()

	if current, ok := b.RunToken("asker"); !ok || current != steady {
		t.Fatalf("credential after re-registration = (%q, %v), want %q", current, ok, steady)
	}
	if status, err := authenticatedGet(b.Addr(), "asker", original); err != nil || status != http.StatusUnauthorized {
		t.Fatalf("authentication with the replaced credential = (%d, %v), want 401", status, err)
	}
}

// TestDeleteRunIfTokenKeepsARecordItsCredentialLeftBehind pins the mismatch
// branch of the record retirement: a retirement that arrives after the record's
// credential rotated — a run that restarted inside a live branch while the relay
// refusal was in flight — deletes nothing, because a dropped record makes the
// run unclaimable and the healthy record belongs to its owner.
func TestDeleteRunIfTokenKeepsARecordItsCredentialLeftBehind(t *testing.T) {
	b := New("")
	if _, err := b.CreateRun("asker", "asker"); err != nil {
		t.Fatalf("create the run: %v", err)
	}
	stale, ok := b.RunToken("asker")
	if !ok || stale == "" {
		t.Fatalf("run credential = (%q, %v), want a credential to retire against", stale, ok)
	}

	// The run restarts inside the live branch and presents a fresh credential.
	b.ownRun(messaging.ClaimedRun{RunClaim: messaging.RunClaim{RunID: "asker", Label: "asker", Token: "token-new"}, Key: "asker"})
	if b.DeleteRunIfToken("asker", stale) {
		t.Fatal("a retirement carrying the rotated-away credential deleted the record")
	}
	if token, ok := b.RunToken("asker"); !ok || token != "token-new" {
		t.Fatalf("run credential = (%q, %v), want the restarted run's record kept", token, ok)
	}

	// The matching credential still retires the record, so the two halves of
	// the rule stay paired.
	if !b.DeleteRunIfToken("asker", "token-new") {
		t.Fatal("a retirement carrying the record's credential did not delete the record")
	}
	if _, ok := b.RunToken("asker"); ok {
		t.Fatal("the retirement left the record behind")
	}
}

// rotationLog reports whether any credential write overlapped a caller's
// request. A writer brackets its write with the counters, so a caller that saw
// the same pair on both sides of a request cannot have shared the record with a
// write.
type rotationLog struct {
	started atomic.Int64
	done    atomic.Int64
}

func (l *rotationLog) write(refresh func()) {
	l.started.Add(1)
	refresh()
	l.done.Add(1)
}

func (l *rotationLog) snapshot() (started, done int64) {
	return l.started.Load(), l.done.Load()
}

// authenticatedGet reads /sessions as runID with token, the way a peer broker
// does.
func authenticatedGet(addr, runID, token string) (int, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/sessions", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set(messaging.HeaderRunID, runID)
	req.Header.Set(messaging.HeaderRunToken, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = resp.Body.Read(make([]byte, 1<<10))
	return resp.StatusCode, nil
}

// authenticatedSend posts one fire-and-forget message as runID with token,
// which authenticates twice: once on the route and once in the send handler.
func authenticatedSend(addr, fromRunID, token, toRunID string) (int, error) {
	raw, err := json.Marshal(map[string]any{
		"run_id":      fromRunID,
		"token":       token,
		"from_run_id": fromRunID,
		"to_run_id":   toRunID,
		"type":        "agent_message",
		"payload":     map[string]any{"message": "ping"},
	})
	if err != nil {
		return 0, err
	}
	resp, err := http.Post("http://"+addr+"/send", "application/json", bytes.NewReader(raw))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = resp.Body.Read(make([]byte, 1<<10))
	return resp.StatusCode, nil
}
