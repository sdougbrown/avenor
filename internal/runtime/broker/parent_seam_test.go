package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// criticalSectionWindow bounds the negative assertions: the interval a racing
// request is given to prove it cannot enter the link while a sibling holds the
// registration critical section. The request's arrival at the handler is
// proven first, so a window that elapses describes contention, not absence.
const criticalSectionWindow = 200 * time.Millisecond

// parentLinkStub is a parent-link seam that records nothing and refuses every
// method a caller is not allowed to reach: a broker that calls the link outside
// its own registration routes fails loudly instead of quietly doing I/O under
// the ask store's lock.
type parentLinkStub struct {
	owned        string
	register     func(messaging.DescendantRegistration) (messaging.DescendantClaim, error)
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
	b.InstallParentLink(link, "", nil)

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
	})
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
	b.InstallParentLink(link, "", nil)

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

// TestRelayWithoutASenderNamesTheAskAsTheBranchResolvesIt pins handleRelay's
// from_sender fallback: a relay that omits the name — the mixed-version case —
// is still delivered with the sender name the receiving branch resolves for the
// asking run, not an empty Sender.
func TestRelayWithoutASenderNamesTheAskAsTheBranchResolvesIt(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	if _, err := b.CreateRun("asker", ""); err != nil {
		t.Fatalf("create asker: %v", err)
	}
	if err := b.SetHarnessName("asker", "asker-name"); err != nil {
		t.Fatalf("name the asker: %v", err)
	}
	token, err := b.CreateRun("branch-run", "branch-run")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	b.InstallParentLink(&parentLinkStub{}, "", nil)

	status, body := postAuthBroker(t, b.Addr(), "/relay", "branch-run", token, map[string]any{
		"from_run_id": "asker",
		"to_run_id":   "branch-run",
		"type":        "agent_message",
		"payload":     map[string]any{"message": "relayed"},
		"message_id":  "av1.abc.def",
		"generation":  b.Store().Generation(),
	})
	if status != http.StatusOK {
		t.Fatalf("relay without a sender = (%d, %v), want 200", status, body)
	}
	st := b.GetRun("branch-run")
	st.Mu.Lock()
	var sender string
	for _, msg := range st.ControlQueue {
		if msg.ID == "av1.abc.def" {
			sender = msg.Sender
		}
	}
	st.Mu.Unlock()
	if sender != "asker-name" {
		t.Fatalf("relayed ask sender = %q, want the name the branch resolves for the asker", sender)
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
	b.InstallParentLink(&parentLinkStub{}, "", nil)

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

// parentRegistryStub is the registry half of a parent link for the
// registration concurrency regressions: it holds the one accepted
// registration's view under a mutex, models the registry's view-matched
// deregistration, and can pause one choreographed registration after it has
// recorded its registry view but before its claim returns for record
// application — the last point where the registry and the run records can
// still disagree.
type parentRegistryStub struct {
	viewMu     sync.Mutex
	owned      string
	generation string
	token      string
	endpoint   string

	registers   int
	pauseAt     int           // the 1-indexed Register call to pause; 0 pauses none
	entered     chan struct{} // closed when the paused call reaches its gate
	release     chan struct{} // closed to let the paused call return its claim
	releaseOnce sync.Once

	// secondEntered closes when the Register call after the paused one enters,
	// and deregisterEntered closes the first time the deregister callback
	// runs: the negative assertions watch these while the paused call holds
	// the critical section.
	secondEntered     chan struct{}
	deregisterEntered chan struct{}
	deregisterOnce    sync.Once
}

func (s *parentRegistryStub) Owns(runID string) bool {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	return s.owned != "" && runID == s.owned
}

func (s *parentRegistryStub) Key(childID, runID string) string { return runID + "@" + childID }

func (s *parentRegistryStub) Register(reg messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
	claim := messaging.DescendantClaim{}
	s.viewMu.Lock()
	s.registers++
	call := s.registers
	for _, run := range reg.Runs {
		claim.Runs = append(claim.Runs, messaging.ClaimedRun{RunClaim: run, Key: run.RunID + "@" + reg.ChildID})
	}
	// Record the registry view before the claim returns: from here the
	// registry resolves this registration's endpoint, generation and
	// credential for every later hop.
	if len(reg.Runs) > 0 {
		s.owned = reg.Runs[0].RunID + "@" + reg.ChildID
		s.generation = reg.Generation
		s.token = reg.Runs[0].Token
		s.endpoint = reg.Endpoint
	}
	pause := call == s.pauseAt
	if s.pauseAt > 0 && call == s.pauseAt+1 {
		close(s.secondEntered)
	}
	s.viewMu.Unlock()
	if pause {
		close(s.entered)
		<-s.release
	}
	return claim, nil
}

func (s *parentRegistryStub) Deregister(childID string, reg messaging.DescendantRegistration) (map[string]string, error) {
	s.deregisterOnce.Do(func() {
		if s.deregisterEntered != nil {
			close(s.deregisterEntered)
		}
	})
	// Model the registry's view matching: the endpoint and generation must
	// resolve to the registration the parent holds, and the run it still holds
	// must appear in the closing branch's view with the credential it was
	// registered under.
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	if s.owned == "" || reg.Generation != s.generation || reg.Endpoint != s.endpoint {
		return nil, messaging.ErrGenerationMismatch
	}
	for _, run := range reg.Runs {
		if run.RunID+"@"+childID == s.owned {
			if run.Token != s.token {
				return nil, messaging.ErrGenerationMismatch
			}
			retired, token := s.owned, s.token
			s.owned, s.generation, s.token, s.endpoint = "", "", "", ""
			return map[string]string{retired: token}, nil
		}
	}
	return nil, messaging.ErrGenerationMismatch
}

// releasePaused lets a paused registration return its claim; the tests also
// call it deferred so a failed assertion cannot leak the paused goroutine.
func (s *parentRegistryStub) releasePaused() { s.releaseOnce.Do(func() { close(s.release) }) }

// accepted reports the registry's current view.
func (s *parentRegistryStub) accepted() (owned, generation, token string) {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	return s.owned, s.generation, s.token
}

func (s *parentRegistryStub) RecordRelay(messaging.RelayRoute) error {
	panic("parentlink: relay is not part of a registration")
}

// parentRouteResult carries one parent-route POST's outcome so the racing
// requests' goroutines can report without touching the testing state.
type parentRouteResult struct {
	status int
	body   map[string]any
	err    error
}

// postParentRoute posts one parent-route body with an Authorization header.
// Each call uses its own client with a timeout, so a request the critical
// section holds cannot hang the test past its window.
func postParentRoute(addr, path, authHeader string, body map[string]any) parentRouteResult {
	raw, err := json.Marshal(body)
	if err != nil {
		return parentRouteResult{err: err}
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+path, bytes.NewReader(raw))
	if err != nil {
		return parentRouteResult{err: err}
	}
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return parentRouteResult{err: err}
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return parentRouteResult{status: resp.StatusCode, body: decoded}
}

// registrationBody builds one child registration's body.
func registrationBody(endpoint, generation, token string) map[string]any {
	return map[string]any{
		"child_id":   "child-1",
		"endpoint":   endpoint,
		"generation": generation,
		"runs":       []map[string]any{{"run_id": "rt_x", "label": "claim", "token": token}},
	}
}

// startParentRouteHarness serves the broker's parent routes from a test-owned
// server that reports each request's arrival at the route, so the concurrency
// regressions can prove a racing request reached its handler before asserting
// what it cannot do while a sibling holds the registration critical section.
// The wiring mirrors Start's parent routes; the handlers carry their own
// checks, so the harness adds only the arrival report.
func startParentRouteHarness(t *testing.T, b *Broker) (string, <-chan string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	routes := http.NewServeMux()
	routes.HandleFunc("/parent/register", b.withMethod("POST", b.handleParentRegister))
	routes.HandleFunc("/parent/deregister", b.withMethod("POST", b.handleParentDeregister))
	arrivals := make(chan string, 8)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrivals <- r.URL.Path
		routes.ServeHTTP(w, r)
	})}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return l.Addr().String(), arrivals
}

// waitArrival proves a request reached the harness route, so a following
// negative assertion describes a request that arrived rather than one the
// client never delivered.
func waitArrival(t *testing.T, arrivals <-chan string, path string) {
	t.Helper()
	select {
	case got := <-arrivals:
		if got != path {
			t.Fatalf("a request for %s arrived, want one for %s", got, path)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no request reached %s", path)
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
// wrong Bearer is refused, and the correct Bearer is accepted; a broker with
// no configured credential serves no parent routes at all, which
// TestParentRoutesRefuseWithoutTheLinkCredential pins on the parentlink side.
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
		b.InstallParentLink(&parentLinkStub{
			register: func(messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
				return messaging.DescendantClaim{
					Runs: []messaging.ClaimedRun{{RunClaim: messaging.RunClaim{RunID: "remote-run", Label: "remote", Token: "child-token"}, Key: "remote-run"}},
				}, nil
			},
		}, token, nil)
		return b
	}

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
	b.InstallParentLink(&parentLinkStub{}, "", nil)

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

// TestConcurrentRegistrationsLeaveTheRegistryAndRecordsAgreeing pins the
// registration critical section: one registration's guard, its register and
// its record application run as one sequence, so a second registration cannot
// record its own registry view and land its claim's records between the
// first's view and its apply. The first registration pauses after it has
// recorded its registry view and before its claim returns — the last point
// where the registry and the run records can still disagree — and the second
// registration, proven to have reached its handler, cannot enter the link
// until the first has been released.
func TestConcurrentRegistrationsLeaveTheRegistryAndRecordsAgreeing(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	const key = "rt_x@child-1"
	stub := &parentRegistryStub{
		pauseAt:       1,
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
		secondEntered: make(chan struct{}),
	}
	t.Cleanup(stub.releasePaused)
	b.InstallParentLink(stub, "parent-token", nil)
	addr, arrivals := startParentRouteHarness(t, b)

	post := func(token string) <-chan parentRouteResult {
		done := make(chan parentRouteResult, 1)
		go func() {
			done <- postParentRoute(addr, "/parent/register", "Bearer parent-token", registrationBody("http://child:1234", "gen-1", token))
		}()
		return done
	}

	// The first registration pauses inside its register call, holding the
	// critical section with its registry view recorded.
	firstDone := post("token-1")
	waitArrival(t, arrivals, "/parent/register")
	<-stub.entered

	// The second registration, claiming the same run under a different
	// credential, must have reached its handler before the negative assertion
	// below can mean anything.
	secondDone := post("token-2")
	waitArrival(t, arrivals, "/parent/register")

	// While the first registration holds the critical section, the second's
	// register callback cannot enter the link.
	select {
	case <-stub.secondEntered:
		t.Fatal("the second registration entered the link while the first held the critical section")
	case <-time.After(criticalSectionWindow):
	}

	// Released, both registrations run to completion and both are served.
	stub.releasePaused()
	first := <-firstDone
	second := <-secondDone
	if first.err != nil || second.err != nil {
		t.Fatalf("registrations = (%v, %v), want two responses", first.err, second.err)
	}
	if first.status != http.StatusOK || second.status != http.StatusOK {
		t.Fatalf("registrations = (%d, %d), want both 200", first.status, second.status)
	}

	// The registry and the run record agree on the credential the final claim
	// carried: the second registration's apply followed the first's.
	owned, _, registryToken := stub.accepted()
	if owned != key {
		t.Fatalf("registry owner = %q, want %q", owned, key)
	}
	if token, ok := b.RunToken(key); !ok || token != registryToken {
		t.Fatalf("run record credential = (%q, %v), want the registry's %q", token, ok, registryToken)
	}
}

// TestConcurrentDeregisterCannotOrphanARegisteredRun pins the deregistration
// half of the registration critical section: a close carrying the branch's
// last accepted view cannot retire the registry between a same-view
// re-registration's recorded view and its record application. The
// re-registration pauses after its view is recorded and before its claim
// returns; the close, proven to have reached its handler, cannot enter the
// link until the paused registration has applied. With the critical section
// the retirement deletes the freshly applied record and the branch can
// register again; without it the retirement lands first and the delayed apply
// recreates a record the registry no longer owns.
func TestConcurrentDeregisterCannotOrphanARegisteredRun(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	const key = "rt_x@child-1"
	stub := &parentRegistryStub{
		pauseAt:           2,
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
		secondEntered:     make(chan struct{}),
		deregisterEntered: make(chan struct{}),
	}
	t.Cleanup(stub.releasePaused)
	b.InstallParentLink(stub, "parent-token", nil)
	addr, arrivals := startParentRouteHarness(t, b)
	view := registrationBody("http://child:1234", "gen-1", "token-1")

	// The registration the close will race settles first.
	first := postParentRoute(addr, "/parent/register", "Bearer parent-token", view)
	if first.err != nil || first.status != http.StatusOK {
		t.Fatalf("the registration the close will race = (%d, %v), want 200", first.status, first.err)
	}

	// A same-view retry of that registration pauses between its recorded
	// registry view and its claim's record application — the only window a
	// close carrying the branch's last accepted view could still retire in.
	retryDone := make(chan parentRouteResult, 1)
	go func() {
		retryDone <- postParentRoute(addr, "/parent/register", "Bearer parent-token", view)
	}()
	waitArrival(t, arrivals, "/parent/register")
	waitArrival(t, arrivals, "/parent/register")
	<-stub.entered

	// The close carries the exact accepted endpoint, generation, run and
	// credential view, and reaches its handler before the negative assertion
	// below can mean anything.
	closeDone := make(chan parentRouteResult, 1)
	go func() {
		closeDone <- postParentRoute(addr, "/parent/deregister", "Bearer parent-token", view)
	}()
	waitArrival(t, arrivals, "/parent/deregister")

	// While the re-registration holds the critical section, the close's
	// deregister callback cannot enter the link.
	select {
	case <-stub.deregisterEntered:
		t.Fatal("the deregistration entered the link while the re-registration held the critical section")
	case <-time.After(criticalSectionWindow):
	}

	// Released, the re-registration applies before the close retires anything.
	stub.releasePaused()
	retry := <-retryDone
	closed := <-closeDone
	if retry.err != nil || retry.status != http.StatusOK {
		t.Fatalf("same-view re-registration = (%d, %v), want 200", retry.status, retry.err)
	}
	if closed.err != nil || closed.status != http.StatusOK {
		t.Fatalf("deregistration = (%d, %v), want 200", closed.status, closed.err)
	}

	// Nothing is orphaned: the retirement took the registry's claim and the
	// freshly applied record went with it.
	if owned, _, _ := stub.accepted(); owned != "" {
		t.Fatalf("registry owner after the close = %q, want none", owned)
	}
	if b.GetRun(key) != nil {
		t.Fatal("the close left an orphan record the registry no longer owns")
	}

	// The branch recovers: a fresh registration succeeds and the registry and
	// the record agree on its credential.
	fresh := postParentRoute(addr, "/parent/register", "Bearer parent-token", registrationBody("http://child:1234", "gen-2", "token-2"))
	if fresh.err != nil || fresh.status != http.StatusOK {
		t.Fatalf("re-registration after the close = (%d, %v), want 200", fresh.status, fresh.err)
	}
	owned, generation, token := stub.accepted()
	if owned != key || generation != "gen-2" || token != "token-2" {
		t.Fatalf("registry view after re-registration = (%q, %q, %q), want the fresh claim", owned, generation, token)
	}
	if got, ok := b.RunToken(key); !ok || got != token {
		t.Fatalf("run record credential = (%q, %v), want the registry's %q", got, ok, token)
	}
}
