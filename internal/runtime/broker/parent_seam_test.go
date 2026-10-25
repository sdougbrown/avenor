package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/parentlink"
)

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

func (p *parentLinkStub) Register(reg messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
	if p.register != nil {
		return p.register(reg)
	}
	panic("parentlink: registration is not part of an admission")
}

func (p *parentLinkStub) Deregister(childID string) ([]string, error) {
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

// TestParentRegisterRefusesARunTheBrokerOwns pins the ownership guard on
// /parent/register: a child that claims a run this broker already owns — its
// own supervisor run, in any real nested topology — is refused, and the owned
// run keeps its owner.
func TestParentRegisterRefusesARunTheBrokerOwns(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	if _, err := b.CreateRun("web", "web"); err != nil {
		t.Fatalf("create the broker's own run: %v", err)
	}

	link, err := parentlink.New(parentlink.Options{
		Endpoint:   "http://child:1234",
		Generation: func() string { return b.Store().Generation() },
		Runs:       func() []messaging.RunClaim { return []messaging.RunClaim{} },
	}, b.LocalMessenger())
	if err != nil {
		t.Fatalf("build parent link: %v", err)
	}
	t.Cleanup(func() { _ = link.Close() })
	b.SetParentLink(link)

	raw, err := json.Marshal(map[string]any{
		"child_id":   "child-1",
		"endpoint":   "http://child:1234",
		"generation": "gen-1",
		"runs": []map[string]any{
			{"run_id": "web", "label": "web", "token": "child-token"},
		},
	})
	if err != nil {
		t.Fatalf("marshal registration: %v", err)
	}
	status, _ := postParentLink(t, b.Addr(), string(raw), "")
	if status != http.StatusConflict {
		t.Fatalf("register claiming a run the broker owns = %d, want 409", status)
	}
	if _, ok := b.RunToken("web"); !ok {
		t.Fatal("the owned run lost its credential to the refused claim")
	}
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
					Changed: true,
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
// registration route's refusal of a run ID the run-key namespacing reserves:
// a claimed run ID carrying "@" reads as a branch-namespaced key, so the
// registry refuses the claim and the route reports it. The handler re-checks
// nothing: the registry's refusal is the one enforcement point.
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
	registry := parentlink.NewRegistry()
	b.SetParentLink(&parentLinkStub{
		register: func(reg messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
			return registry.Register(reg)
		},
	})

	status, body := postParentLink(t, b.Addr(), string(raw), "")
	if status != http.StatusConflict || body["code"] != "invalid-request" {
		t.Fatalf("registration with a namespaced run id = (%d, %v), want 409 invalid-request", status, body)
	}
	if b.GetRun(messaging.SupervisorRunID+"@victim") != nil {
		t.Fatal("the refused registration adopted the forged run")
	}
	if registry.Owns(messaging.SupervisorRunID + "@victim") {
		t.Fatal("the refused claim stayed in the registry")
	}
}

// TestParentRegisterFiresTheRunObserverOnlyWhenTheSetChanged pins the handler's
// observer rule: a claim that adopted or retired a run notifies the run
// observer, and a refresh that re-registers the runs the child already claimed
// leaves the observer alone — an idle branch's periodic re-registration must
// not re-publish the parent's peer set.
func TestParentRegisterFiresTheRunObserverOnlyWhenTheSetChanged(t *testing.T) {
	raw := func(runID string) []byte {
		body, err := json.Marshal(map[string]any{
			"child_id":   "child-1",
			"endpoint":   "http://child:1234",
			"generation": "gen-1",
			"runs":       []map[string]any{{"run_id": runID, "label": "remote", "token": "child-token"}},
		})
		if err != nil {
			t.Fatalf("marshal registration: %v", err)
		}
		return body
	}
	start := func(t *testing.T, changed bool) (*Broker, *int) {
		t.Helper()
		b := New("")
		if err := b.Start(); err != nil {
			t.Fatalf("start broker: %v", err)
		}
		t.Cleanup(func() { _ = b.Stop() })
		fired := 0
		b.SetRunObserver(func() { fired++ })
		b.SetParentLink(&parentLinkStub{
			register: func(messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
				return messaging.DescendantClaim{
					Runs:    []messaging.ClaimedRun{{RunClaim: messaging.RunClaim{RunID: "remote-run", Label: "remote", Token: "child-token"}, Key: "remote-run"}},
					Changed: changed,
				}, nil
			},
		})
		return b, &fired
	}

	t.Run("a claim that adopted a run fires the observer", func(t *testing.T) {
		b, fired := start(t, true)
		status, _ := postParentLink(t, b.Addr(), string(raw("remote-run")), "")
		if status != http.StatusOK {
			t.Fatalf("registration = %d, want 200", status)
		}
		if *fired == 0 {
			t.Fatal("an adopting registration did not notify the run observer")
		}
	})

	t.Run("a refresh of the same claim does not fire the observer", func(t *testing.T) {
		b, fired := start(t, false)
		status, _ := postParentLink(t, b.Addr(), string(raw("remote-run")), "")
		if status != http.StatusOK {
			t.Fatalf("registration = %d, want 200", status)
		}
		if *fired != 0 {
			t.Fatalf("an unchanged re-registration notified the run observer %d times", *fired)
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
