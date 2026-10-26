package broker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/parentlink"
)

// parentLinkPause delegates every call to the real parent-link seam and, when
// armed, holds the caller after Register or Deregister has applied — the window
// where the registry no longer owns a retired run but the broker has not
// retired the adopted record yet.
type parentLinkPause struct {
	messaging.ParentLink
	mu                sync.Mutex
	registerPause     chan struct{}
	registerRelease   chan struct{}
	deregisterPause   chan struct{}
	deregisterRelease chan struct{}
}

func (p *parentLinkPause) armRegister() (paused <-chan struct{}, release chan<- struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pausedCh := make(chan struct{})
	releaseCh := make(chan struct{})
	p.registerPause = pausedCh
	p.registerRelease = releaseCh
	return pausedCh, releaseCh
}

func (p *parentLinkPause) armDeregister() (paused <-chan struct{}, release chan<- struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pausedCh := make(chan struct{})
	releaseCh := make(chan struct{})
	p.deregisterPause = pausedCh
	p.deregisterRelease = releaseCh
	return pausedCh, releaseCh
}

func (p *parentLinkPause) take(pause, release *chan struct{}) (chan struct{}, chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pauseCh, releaseCh := *pause, *release
	*pause, *release = nil, nil
	return pauseCh, releaseCh
}

func (p *parentLinkPause) Register(reg messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
	claim, err := p.ParentLink.Register(reg)
	pauseCh, releaseCh := p.take(&p.registerPause, &p.registerRelease)
	if pauseCh != nil {
		close(pauseCh)
		<-releaseCh
	}
	return claim, err
}

func (p *parentLinkPause) Deregister(childID string, reg messaging.DescendantRegistration) (map[string]string, error) {
	runs, err := p.ParentLink.Deregister(childID, reg)
	pauseCh, releaseCh := p.take(&p.deregisterPause, &p.deregisterRelease)
	if pauseCh != nil {
		close(pauseCh)
		<-releaseCh
	}
	return runs, err
}

// startParentBroker starts a broker that serves the parent-link routes with a
// real parent-mode messenger: a real descendant registry behind the seam.
func startParentBroker(t *testing.T) (*Broker, *parentlink.Messenger) {
	t.Helper()
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start parent broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	link, err := parentlink.New(parentlink.Options{
		Generation: b.Store().Generation,
	}, b.LocalMessenger())
	if err != nil {
		t.Fatalf("build parent messenger: %v", err)
	}
	b.SetMessenger(link)
	b.SetParentLink(link)
	return b, link
}

func postParentRegister(t *testing.T, addr string, body map[string]any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal registration: %v", err)
	}
	resp, err := http.Post("http://"+addr+"/parent/register", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("post /parent/register: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func awaitRecordGone(t *testing.T, b *Broker, registry *parentlink.Registry, runID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for b.GetRun(runID) != nil || registry.Owns(runID) {
		if time.Now().After(deadline) {
			t.Fatalf("run %q was never retired: record=%v owned=%v", runID, b.GetRun(runID) != nil, registry.Owns(runID))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestParentRunClaimsExcludeARetiredDescendantRecord pins the coherence of
// claim provenance: in the window after the registry applied a registration
// but before the broker retired the adopted record, the retired descendant's
// record must never publish as one of this broker's own runs — the
// descendant's credential must not travel upward as if it were local.
func TestParentRunClaimsExcludeARetiredDescendantRecord(t *testing.T) {
	b, link := startParentBroker(t)
	pause := &parentLinkPause{ParentLink: link}
	b.SetParentLink(pause)

	rootToken, err := b.CreateRun("root-run", "root-run")
	if err != nil {
		t.Fatalf("create the broker's own run: %v", err)
	}

	resp := postParentRegister(t, b.Addr(), map[string]any{
		"child_id":   "child-a",
		"endpoint":   "http://child-a",
		"generation": "gen-a",
		"runs":       []messaging.RunClaim{{RunID: "run-x", Label: "run-x", Token: "tok-x"}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register child-a = %d, want 200", resp.StatusCode)
	}
	awaitAdoptedRecord(t, b, link.Registry(), "run-x")

	// The next registration retires run-x. The pause holds the handler after
	// the registry removed its ownership but before the record is retired.
	paused, release := pause.armRegister()
	applyDone := make(chan int, 1)
	go func() {
		applyDone <- postParentRegister(t, b.Addr(), map[string]any{
			"child_id":   "child-a",
			"endpoint":   "http://child-a",
			"generation": "gen-a",
			"runs":       []messaging.RunClaim{},
		}).StatusCode
	}()
	<-paused

	claims := b.ParentRunClaims()
	if len(claims) != 1 {
		t.Fatalf("claims in the retirement window = %+v, want only the broker's own run", claims)
	}
	if claims[0].RunID != "root-run" || claims[0].Token != rootToken {
		t.Fatalf("claim = %+v, want root-run with its own credential", claims[0])
	}

	close(release)
	if code := <-applyDone; code != http.StatusOK {
		t.Fatalf("the retirement registration answered %d, want 200", code)
	}
	awaitRecordGone(t, b, link.Registry(), "run-x")

	// The same window exists when a child deregisters its whole claim.
	resp = postParentRegister(t, b.Addr(), map[string]any{
		"child_id":   "child-a",
		"endpoint":   "http://child-a",
		"generation": "gen-a",
		"runs":       []messaging.RunClaim{{RunID: "run-x", Label: "run-x", Token: "tok-x"}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("re-register child-a = %d, want 200", resp.StatusCode)
	}
	awaitAdoptedRecord(t, b, link.Registry(), "run-x")

	paused, release = pause.armDeregister()
	deregisterDone := make(chan int, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, "http://"+b.Addr()+"/parent/deregister",
			bytes.NewReader([]byte(`{"child_id":"child-a","endpoint":"http://child-a","generation":"gen-a","runs":[{"run_id":"run-x","label":"run-x","token":"tok-x"}]}`)))
		if err != nil {
			deregisterDone <- -1
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			deregisterDone <- -1
			return
		}
		defer resp.Body.Close()
		deregisterDone <- resp.StatusCode
	}()
	<-paused

	claims = b.ParentRunClaims()
	if len(claims) != 1 || claims[0].RunID != "root-run" || claims[0].Token != rootToken {
		t.Fatalf("claims in the deregistration window = %+v, want only root-run with its own credential", claims)
	}

	close(release)
	if code := <-deregisterDone; code != http.StatusOK {
		t.Fatalf("the deregistration answered %d, want 200", code)
	}
	awaitRecordGone(t, b, link.Registry(), "run-x")
}

// awaitAdoptedRecord waits until the registry adopted runID and the
// broker holds its adopted record.
func awaitAdoptedRecord(t *testing.T, b *Broker, registry *parentlink.Registry, runID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !registry.Owns(runID) || b.GetRun(runID) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("run %q was never adopted: owned=%v record=%v", runID, registry.Owns(runID), b.GetRun(runID) != nil)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
