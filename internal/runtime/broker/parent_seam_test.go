package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// parentLinkStub is a parent-link seam that records nothing and refuses every
// method a caller is not allowed to reach: a broker that calls the link outside
// its own registration routes fails loudly instead of quietly doing I/O under
// the ask store's lock.
type parentLinkStub struct {
	owned        string
	register     func(messaging.DescendantRegistration) (messaging.DescendantClaim, error)
	deregister   func(childID string, reg messaging.DescendantRegistration) (map[string]string, error)
	recordRelay  func(messaging.RelayRoute) error
	recorded     []messaging.RelayRoute
	deregistered bool
}

func (p *parentLinkStub) Owns(runID string) bool { return runID == p.owned }

func (p *parentLinkStub) Key(childID, runID string) string { return runID + "@" + childID }

func (p *parentLinkStub) Register(reg messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
	if p.register != nil {
		return p.register(reg)
	}
	panic("parentlink: registration is not part of an admission")
}

func (p *parentLinkStub) Deregister(childID string, reg messaging.DescendantRegistration) (map[string]string, error) {
	if p.deregister != nil {
		return p.deregister(childID, reg)
	}
	p.deregistered = true
	return nil, nil
}

func (p *parentLinkStub) RecordRelay(route messaging.RelayRoute) error {
	p.recorded = append(p.recorded, route)
	if p.recordRelay != nil {
		return p.recordRelay(route)
	}
	return nil
}

// TestAdmissionForADescendantRunTouchesNoLink pins the admission boundary: a
// publication for a run a child broker owns is accepted without a local queue
// and without entering the link, so the relay that reaches the child always runs
// after the store has finished admitting.
func TestAdmissionForADescendantRunTouchesNoLink(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	link := &parentLinkStub{owned: "remote"}
	b.SetParentLink(link)

	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("mint message id: %v", err)
	}
	res, err := b.LocalMessenger().Send(context.Background(), messaging.SendRequest{
		From:       messaging.Identity{RunID: "local", Sender: "local"},
		To:         messaging.Identity{RunID: "remote", Sender: "remote"},
		MessageID:  msgID,
		Generation: b.Store().Generation(),
		Body:       `{"message":"cross-branch"}`,
	}, false)
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("admission for a descendant = (%+v, %v), want sent", res, err)
	}
	if b.GetRun("remote") != nil {
		t.Fatal("the descendant run got a local queue record")
	}
	if edges := b.Store().ActiveCount(); edges != 1 {
		t.Fatalf("store holds %d ask edges, want the one the cross-branch ask owns", edges)
	}
	if len(link.recorded) != 0 {
		t.Fatalf("admission recorded %d relay routes, want none", len(link.recorded))
	}
}

// TestRelayRecordsTheReplyRouteBeforeExposingTheAsk pins the ordering rule: a
// relayed ask whose reply route cannot be recorded is never queued, so a consumer
// can never answer an ask whose route is unbound.
func TestRelayRecordsTheReplyRouteBeforeExposingTheAsk(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	token, err := b.CreateRun("branch-run", "branch-run")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	link := &parentLinkStub{recordRelay: func(messaging.RelayRoute) error {
		return messaging.ErrTransportClosed
	}}
	b.SetParentLink(link)

	status, body := postAuthBroker(t, b.Addr(), "/relay", "branch-run", token, map[string]any{
		"from_run_id":     "asker",
		"to_run_id":       "branch-run",
		"type":            "agent_message",
		"payload":         map[string]any{"message": "relayed"},
		"message_id":      "av1.abc.def",
		"generation":      b.Store().Generation(),
		"edge_generation": "edge-gen",
		"expects_reply":   true,
	})
	if status != http.StatusConflict {
		t.Fatalf("relay with an unrecordable route = (%d, %v), want 409", status, body)
	}
	if queued := countAgentMessages(t, b, "branch-run"); queued != 0 {
		t.Fatalf("run queue = %d messages, want the ask withheld until its route is bound", queued)
	}

	// With the route accepted, the same relay is exposed exactly once and the
	// route carries the asker and the generation the ancestor admitted it under.
	link.recordRelay = nil
	link.recorded = nil
	status, body = postAuthBroker(t, b.Addr(), "/relay", "branch-run", token, map[string]any{
		"from_run_id":     "asker",
		"to_run_id":       "branch-run",
		"type":            "agent_message",
		"payload":         map[string]any{"message": "relayed"},
		"message_id":      "av1.abc.def",
		"generation":      b.Store().Generation(),
		"edge_generation": "edge-gen",
		"expects_reply":   true,
	})
	if status != http.StatusOK {
		t.Fatalf("relay = (%d, %v), want 200", status, body)
	}
	if queued := countAgentMessages(t, b, "branch-run"); queued != 1 {
		t.Fatalf("run queue = %d messages, want the relayed ask", queued)
	}
	if len(link.recorded) != 1 || link.recorded[0].AskerRunID != "asker" ||
		link.recorded[0].EdgeGeneration != "edge-gen" {
		t.Fatalf("recorded relay routes = %+v, want the asker and edge generation", link.recorded)
	}
	if edges := b.Store().ActiveCount(); edges != 0 {
		t.Fatalf("a relayed ask admitted %d local edges, want 0", edges)
	}
}

// TestRelayRefusesAReplacementGeneration pins that a relay names the generation
// the branch pinned when it registered, so a relay prepared for a replaced broker
// is refused instead of landing in the replacement.
func TestRelayRefusesAReplacementGeneration(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	token, err := b.CreateRun("branch-run", "branch-run")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	b.SetParentLink(&parentLinkStub{})

	status, body := postAuthBroker(t, b.Addr(), "/relay", "branch-run", token, map[string]any{
		"from_run_id":   "asker",
		"to_run_id":     "branch-run",
		"type":          "agent_message",
		"payload":       map[string]any{"message": "stale"},
		"message_id":    "av1.abc.def",
		"generation":    "a-generation-from-before-the-restart",
		"expects_reply": true,
	})
	if status != http.StatusConflict || body["code"] != "generation-mismatch" {
		t.Fatalf("relay for a replacement generation = (%d, %v), want 409 generation-mismatch", status, body)
	}
	if queued := countAgentMessages(t, b, "branch-run"); queued != 0 {
		t.Fatalf("run queue = %d messages, want the refused relay withheld", queued)
	}
}

// postParentLink posts a registration body to /parent/register with an
// optional Authorization header and returns the status and decoded object.
func postParentLink(t *testing.T, addr, body, authHeader string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/parent/register", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post /parent/register: %v", err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

// TestParentRegisterRequiresTheParentLinkToken pins the parent-route bearer
// credential: with a token set, a registration without the header or with a
// wrong Bearer is refused, and the correct Bearer is accepted; with no token
// set, the loopback caller is accepted, the same trust the local routes run on.
func TestParentRegisterRequiresTheParentLinkToken(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"child_id":   "child-1",
		"endpoint":   "http://child:1234",
		"generation": "gen-1",
		"runs": []map[string]any{
			{"run_id": "remote-run", "label": "remote", "token": "child-token"},
		},
	})
	if err != nil {
		t.Fatalf("marshal registration: %v", err)
	}

	startWithLink := func(t *testing.T, token string) *Broker {
		t.Helper()
		b := New("")
		if err := b.Start(); err != nil {
			t.Fatalf("start broker: %v", err)
		}
		t.Cleanup(func() { _ = b.Stop() })
		b.SetParentLink(&parentLinkStub{
			register: func(messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
				return messaging.DescendantClaim{
					Runs:    []messaging.ClaimedRun{{RunClaim: messaging.RunClaim{RunID: "remote-run", Label: "remote", Token: "child-token"}, Key: "remote-run"}},
					Adopted: []string{"remote-run"},
				}, nil
			},
		})
		if token != "" {
			b.SetParentLinkToken(token)
		}
		return b
	}

	t.Run("no token set accepts the loopback caller", func(t *testing.T) {
		b := startWithLink(t, "")
		status, body := postParentLink(t, b.Addr(), string(raw), "")
		if status != http.StatusOK {
			t.Fatalf("registration with no token set = (%d, %v), want 200", status, body)
		}
		if b.GetRun("remote-run") == nil {
			t.Fatal("the adopted run has no record")
		}
	})

	t.Run("no Authorization header is refused", func(t *testing.T) {
		b := startWithLink(t, "parent-token")
		status, body := postParentLink(t, b.Addr(), string(raw), "")
		if status != http.StatusUnauthorized {
			t.Fatalf("registration with no header = (%d, %v), want 401", status, body)
		}
		if b.GetRun("remote-run") != nil {
			t.Fatal("a refused registration adopted the run")
		}
	})

	t.Run("a wrong Bearer token is refused", func(t *testing.T) {
		b := startWithLink(t, "parent-token")
		status, body := postParentLink(t, b.Addr(), string(raw), "Bearer wrong-token")
		if status != http.StatusUnauthorized {
			t.Fatalf("registration with a wrong token = (%d, %v), want 401", status, body)
		}
		if b.GetRun("remote-run") != nil {
			t.Fatal("a refused registration adopted the run")
		}
	})

	t.Run("the correct Bearer token is accepted", func(t *testing.T) {
		b := startWithLink(t, "parent-token")
		status, body := postParentLink(t, b.Addr(), string(raw), "Bearer parent-token")
		if status != http.StatusOK {
			t.Fatalf("registration with the correct token = (%d, %v), want 200", status, body)
		}
		if b.GetRun("remote-run") == nil {
			t.Fatal("the adopted run has no record")
		}
	})
}

// TestParentRegisterRefusesARunIDCarryingTheNamespacingSeparator pins the
// handler's refusal of a run ID the run-key namespacing reserves: a claimed
// run ID carrying "@" reads as a branch-namespaced key, so the registration
// is a 400 and nothing is adopted.
func TestParentRegisterRefusesARunIDCarryingTheNamespacingSeparator(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"child_id":   "child-1",
		"endpoint":   "http://child:1234",
		"generation": "gen-1",
		"runs": []map[string]any{
			{"run_id": messaging.SupervisorRunID + "@victim", "label": "forged", "token": "child-token"},
		},
	})
	if err != nil {
		t.Fatalf("marshal registration: %v", err)
	}

	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	b.SetParentLink(&parentLinkStub{
		register: func(messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
			t.Fatal("the handler passed a namespaced run id to the link")
			return messaging.DescendantClaim{}, nil
		},
	})

	status, body := postParentLink(t, b.Addr(), string(raw), "")
	if status != http.StatusBadRequest || body["code"] != "invalid-request" {
		t.Fatalf("registration with a namespaced run id = (%d, %v), want 400 invalid-request", status, body)
	}
	if b.GetRun(messaging.SupervisorRunID+"@victim") != nil {
		t.Fatal("the refused registration adopted the forged run")
	}
}

// TestConcurrentRegistrationsLeaveTheRegistryAndRecordsAgreeing pins the
// registration critical section: the guard, the register and the apply of one
// registration run as one sequence, so a second registration cannot run its
// register inside the first's window. The first registration is held inside
// its register call — the middle of the critical section — and the second
// request is only then handed to its handler, so the second's lock acquisition
// is ordered after the first's window opened. The second's register is then
// proven not to run until the window closes: without the guard its register
// and apply land in the gap and the two credentials diverge.
func TestConcurrentRegistrationsLeaveTheRegistryAndRecordsAgreeing(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	key := "rt_x@child-1"
	var mu sync.Mutex
	first := true
	// firstHeld is closed by the first registration's register call as it
	// blocks inside the critical section; secondEntered is closed if the
	// second registration's register call ever runs.
	firstHeld := make(chan struct{})
	secondEntered := make(chan struct{})
	release := make(chan struct{})
	link := &parentLinkStub{}
	link.register = func(reg messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
		claim := messaging.DescendantClaim{}
		for _, run := range reg.Runs {
			runKey := run.RunID + "@" + reg.ChildID
			claim.Runs = append(claim.Runs, messaging.ClaimedRun{RunClaim: run, Key: runKey})
			claim.Adopted = append(claim.Adopted, runKey)
		}
		mu.Lock()
		isFirst := first
		first = false
		if len(reg.Runs) > 0 {
			// The stub takes the claimed key so the second registration's guard
			// reads the first's record as the link's own and is accepted.
			link.owned = reg.Runs[0].RunID + "@" + reg.ChildID
		}
		mu.Unlock()
		if isFirst {
			close(firstHeld)
			<-release
		} else {
			close(secondEntered)
		}
		return claim, nil
	}
	b.SetParentLink(link)

	body := func(token string) string {
		raw, err := json.Marshal(map[string]any{
			"child_id":   "child-1",
			"endpoint":   "http://child:1234",
			"generation": "gen-1",
			"runs":       []map[string]any{{"run_id": "rt_x", "label": "claim", "token": token}},
		})
		if err != nil {
			t.Fatalf("marshal registration: %v", err)
		}
		return string(raw)
	}

	firstStatus := make(chan int, 1)
	go func() {
		resp, err := http.Post("http://"+b.Addr()+"/parent/register", "application/json", bytes.NewReader([]byte(body("token-1"))))
		if err != nil {
			firstStatus <- -1
			return
		}
		defer resp.Body.Close()
		firstStatus <- resp.StatusCode
	}()
	<-firstHeld

	// The second request's body is a pipe the test writes only after the first
	// register call holds the critical section: the handler is left reading the
	// registration until then, so its lock acquisition — and, without the
	// guard, its register — is ordered after the first's window opened.
	pr, pw := io.Pipe()
	secondStatus := make(chan int, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, "http://"+b.Addr()+"/parent/register", pr)
		if err != nil {
			secondStatus <- -1
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			secondStatus <- -1
			return
		}
		defer resp.Body.Close()
		secondStatus <- resp.StatusCode
	}()
	if _, err := pw.Write([]byte(body("token-2"))); err != nil {
		t.Fatalf("write the gated registration body: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close the gated registration body: %v", err)
	}

	// While the first registration holds the critical section the second's
	// register must not run. The assertion is bounded because a blocked
	// handler produces no signal to wait on: a guard-less window lets the
	// second handler straight through to its register, well inside the bound.
	select {
	case <-secondEntered:
		close(release)
		<-firstStatus
		<-secondStatus
		t.Fatal("the second registration's register ran inside the first's critical section")
	case <-time.After(500 * time.Millisecond):
	}

	close(release)
	if status := <-firstStatus; status != http.StatusOK {
		t.Fatalf("first registration = %d, want 200", status)
	}
	if status := <-secondStatus; status != http.StatusOK {
		t.Fatalf("second registration = %d, want 200", status)
	}
	// The record's credential is settled by whichever apply created it, and a
	// newly adopted claim does not replace it — so under the critical section
	// it is the first registration's token. A second register that ran inside
	// the first's window would apply first and leave the record carrying its
	// token-2 instead.
	if got, ok := b.runToken(key); !ok || got != "token-1" {
		t.Fatalf("run record credential = (%q, %v), want the first registration's token-1", got, ok)
	}
}

// TestConcurrentDeregisterCannotOrphanARegisteredRun pins the deregistration
// half of the registration critical section: a deregistration's retirement and
// record deletes run under the same mutex as a registration's guard, register
// and apply, so a close cannot retire the link's ownership between one
// registration's register and its apply. The registration the close raced is a
// same-view retry — the one registration whose view a closing branch can still
// match — and a retirement landing in its window would leave the apply
// recreating a record the link no longer owns, a state whose next claim is
// refused for good.
func TestConcurrentDeregisterCannotOrphanARegisteredRun(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	key := "rt_x@child-1"
	var mu sync.Mutex
	generation := ""
	token := ""
	calls := 0
	entered := make(chan struct{})
	release := make(chan struct{})
	stub := &parentLinkStub{}
	stub.register = func(reg messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
		claim := messaging.DescendantClaim{}
		mu.Lock()
		held := stub.owned
		mu.Unlock()
		for _, run := range reg.Runs {
			runKey := run.RunID + "@" + reg.ChildID
			claim.Runs = append(claim.Runs, messaging.ClaimedRun{RunClaim: run, Key: runKey})
			if runKey != held {
				claim.Adopted = append(claim.Adopted, runKey)
			}
		}
		mu.Lock()
		calls++
		block := calls == 2
		if len(reg.Runs) > 0 {
			stub.owned = reg.Runs[0].RunID + "@" + reg.ChildID
			generation = reg.Generation
			token = reg.Runs[0].Token
		}
		mu.Unlock()
		if block {
			close(entered)
			<-release
		}
		return claim, nil
	}
	stub.deregister = func(childID string, reg messaging.DescendantRegistration) (map[string]string, error) {
		mu.Lock()
		defer mu.Unlock()
		if reg.Generation != generation {
			return nil, messaging.ErrGenerationMismatch
		}
		retired := stub.owned
		stub.owned = ""
		return map[string]string{retired: token}, nil
	}
	b.SetParentLink(stub)

	body := func(payloadGeneration, payloadToken string) string {
		raw, err := json.Marshal(map[string]any{
			"child_id":   "child-1",
			"endpoint":   "http://child:1234",
			"generation": payloadGeneration,
			"runs":       []map[string]any{{"run_id": "rt_x", "label": "claim", "token": payloadToken}},
		})
		if err != nil {
			t.Fatalf("marshal registration: %v", err)
		}
		return string(raw)
	}
	post := func(payload string) int {
		t.Helper()
		resp, err := http.Post("http://"+b.Addr()+"/parent/register", "application/json", bytes.NewReader([]byte(payload)))
		if err != nil {
			t.Fatalf("post registration: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if status := post(body("gen-1", "token-1")); status != http.StatusOK {
		t.Fatalf("first registration = %d, want 200", status)
	}

	// The retry of the same registration is held between its register and its
	// apply — the window a concurrent close must not retire in.
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		resp, err := http.Post("http://"+b.Addr()+"/parent/register", "application/json", bytes.NewReader([]byte(body("gen-1", "token-1"))))
		if err == nil {
			resp.Body.Close()
		}
	}()
	<-entered

	deregisterDone := make(chan struct{})
	go func() {
		defer close(deregisterDone)
		resp, err := http.Post("http://"+b.Addr()+"/parent/deregister", "application/json", bytes.NewReader([]byte(body("gen-1", "token-1"))))
		if err == nil {
			resp.Body.Close()
		}
	}()
	// While the retry holds the critical section the close cannot proceed, so
	// the record the retry would recreate survives until the gate releases:
	// one check pins the no-orphan window without a spin.
	if b.GetRun(key) == nil {
		t.Fatal("the record vanished while the retry held the critical section: the close slipped inside the window")
	}
	close(release)
	<-secondDone
	<-deregisterDone

	// The branch recovers: the next registration of the key succeeds and leaves
	// the record carrying the new claim's credential under an owned claim.
	if status := post(body("gen-2", "token-2")); status != http.StatusOK {
		t.Fatalf("re-registration = %d, want 200: the recreated record refused the branch's claim for good", status)
	}
	mu.Lock()
	defer mu.Unlock()
	if stub.owned != key {
		t.Fatalf("claim owner = %q, want %q", stub.owned, key)
	}
	if got, ok := b.runToken(key); !ok || got != "token-2" {
		t.Fatalf("run record credential = (%q, %v), want the re-registration's", got, ok)
	}
}

// TestParentRegisterRefusesAnOversizeBody pins the request-body cap the parent
// routes share with withAuth: a registration whose body exceeds the cap is
// refused before the registry is consulted, and no claim is adopted.
func TestParentRegisterRefusesAnOversizeBody(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	b.SetParentLink(&parentLinkStub{})

	claim := `{"run_id":"remote-run","label":"remote","token":"t"},`
	oversize := `{"child_id":"child-1","endpoint":"http://child:1234","generation":"gen-1","runs":[` +
		strings.Repeat(claim, 40000) + `]}`
	status, _ := postParentLink(t, b.Addr(), oversize, "")
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize registration = %d, want 413", status)
	}
	if b.GetRun("remote-run") != nil {
		t.Fatal("a refused registration adopted the run")
	}
}

// TestRelayRefusesATargetTheCallerDidNotAuthenticate pins the relay's target
// authentication: a relay whose authenticated run does not match the body's
// to_run_id is refused before the target's queue is touched, while a relay
// that names the run it authenticated as still lands in that run's queue.
func TestRelayRefusesATargetTheCallerDidNotAuthenticate(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	token, err := b.CreateRun("branch-run", "branch-run")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := b.CreateRun("other-run", "other-run"); err != nil {
		t.Fatalf("create other run: %v", err)
	}
	b.SetParentLink(&parentLinkStub{})

	// A relay authenticated as one run cannot target another: the guard
	// refuses before the target's queue is touched.
	status, body := postAuthBroker(t, b.Addr(), "/relay", "branch-run", token, map[string]any{
		"from_run_id": "asker",
		"to_run_id":   "other-run",
		"type":        "agent_message",
		"payload":     map[string]any{"message": "hijacked"},
		"message_id":  "av1.abc.def",
		"generation":  b.Store().Generation(),
	})
	if status != http.StatusForbidden || body["code"] != "not-ask-target" {
		t.Fatalf("relay to another run's target = (%d, %v), want 403 not-ask-target", status, body)
	}
	if queued := countAgentMessages(t, b, "other-run"); queued != 0 {
		t.Fatalf("target queue = %d messages, want the refused relay withheld", queued)
	}

	// A relay that names the run it authenticated as still lands in that
	// run's queue, so the guard is not a blanket refusal.
	status, body = postAuthBroker(t, b.Addr(), "/relay", "branch-run", token, map[string]any{
		"from_run_id": "asker",
		"to_run_id":   "branch-run",
		"type":        "agent_message",
		"payload":     map[string]any{"message": "relayed"},
		"message_id":  "av1.ghi.jkl",
		"generation":  b.Store().Generation(),
	})
	if status != http.StatusOK {
		t.Fatalf("relay to the authenticated run = (%d, %v), want 200", status, body)
	}
	if queued := countAgentMessages(t, b, "branch-run"); queued != 1 {
		t.Fatalf("run queue = %d messages, want the relayed ask", queued)
	}
}
