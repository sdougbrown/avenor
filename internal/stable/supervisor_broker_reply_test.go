package stable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker/brokertest"
)

// newConstructedSupervisor builds a supervisor with the broker its constructor
// wires, and pins that the supervisor's own broker run exists before any
// caller asks for it: sub-agents target it as their parent.
func newConstructedSupervisor(t *testing.T) *Supervisor {
	t.Helper()
	budget := filepath.Join(t.TempDir(), "tree-budget")
	root, err := admission.CreateRoot(budget, 1)
	if err != nil {
		t.Fatalf("create tree budget: %v", err)
	}
	t.Cleanup(func() { _ = root.Close() })
	sup := NewSupervisor(Config{MaxRuntimes: 1, TreeBudgetFile: budget})
	t.Cleanup(func() {
		sup.closeTreeBudget()
		_ = sup.broker.Stop()
	})
	if sup.broker.Addr() == "" {
		t.Fatal("NewSupervisor did not start the broker")
	}
	if sup.broker.GetRun("supervisor") == nil {
		t.Fatal("NewSupervisor did not register the supervisor's broker run")
	}
	return sup
}

// brokerAsker is the registered broker run that asks the supervisor a question
// and waits for the answer. A generation marks an ask admitted under a store
// generation, which only the Go ask client pins; a spawned pi child sends none.
type brokerAsker struct {
	runID      string
	token      string
	generation string
}

// sendAsk posts one ask from a registered run to toRunID, with the envelope and
// payload a spawned pi child builds, and pins that the broker admitted it.
func sendAsk(t *testing.T, sup *Supervisor, asker brokerAsker, toRunID, msgID, question string) {
	t.Helper()
	payload, err := messaging.AskBody(msgID, asker.runID, toRunID, question, "agent")
	if err != nil {
		t.Fatalf("AskBody: %v", err)
	}
	ask := map[string]any{
		"run_id":      asker.runID,
		"token":       asker.token,
		"from_run_id": asker.runID,
		"to_run_id":   toRunID,
		"type":        "agent_message",
		"payload":     json.RawMessage(payload),
	}
	if asker.generation != "" {
		ask["generation"] = asker.generation
	}
	body, err := json.Marshal(ask)
	if err != nil {
		t.Fatalf("marshal ask: %v", err)
	}
	status, sent := brokerPost(t, sup, "send", body)
	if status != http.StatusOK || sent["queued"] != true {
		t.Fatalf("send = (%d, %v), want the ask queued", status, sent)
	}
}

// waitReply posts the asker's long-poll for the answer. It reports the
// transport error instead of failing the test, so a goroutine may park on it.
func waitReply(sup *Supervisor, asker brokerAsker, msgID string, timeoutMS int64, trace *httptrace.ClientTrace) (int, map[string]any, error) {
	wait := map[string]any{
		"run_id":      asker.runID,
		"token":       asker.token,
		"waiting_for": msgID,
	}
	if timeoutMS > 0 {
		wait["timeout_ms"] = timeoutMS
	}
	if asker.generation != "" {
		wait["generation"] = asker.generation
	}
	body, err := json.Marshal(wait)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal wait_reply: %w", err)
	}
	return brokerPOST(sup, "wait_reply", body, trace)
}

// replyThenWait answers the ask before any wait runs: the store keeps a replied
// edge until a wait consumes it, so the later wait still reads the answer.
func replyThenWait(t *testing.T, sup *Supervisor, asker brokerAsker, msgID, answer string) (int, map[string]any) {
	t.Helper()
	if err := sup.BrokerReply(asker.runID, msgID, answer); err != nil {
		t.Fatalf("BrokerReply: %v", err)
	}
	status, body, err := waitReply(sup, asker, msgID, 0, nil)
	if err != nil {
		t.Fatalf("wait_reply: %v", err)
	}
	return status, body
}

// waitThenReply sends the wait first and answers once the request is on the
// wire, so the reply reaches the broker after the wait rather than before it.
func waitThenReply(t *testing.T, sup *Supervisor, asker brokerAsker, msgID, answer string) (int, map[string]any) {
	t.Helper()
	type waitOutcome struct {
		status int
		body   map[string]any
		err    error
	}
	sent := make(chan struct{})
	var once sync.Once
	waited := make(chan waitOutcome, 1)
	go func() {
		status, body, err := waitReply(sup, asker, msgID, 0, &httptrace.ClientTrace{
			WroteRequest: func(httptrace.WroteRequestInfo) { once.Do(func() { close(sent) }) },
		})
		waited <- waitOutcome{status: status, body: body, err: err}
	}()
	select {
	case <-sent:
	case got := <-waited:
		t.Fatalf("wait_reply finished before the reply was sent: %v", got.err)
	}
	if err := sup.BrokerReply(asker.runID, msgID, answer); err != nil {
		t.Fatalf("BrokerReply: %v", err)
	}
	got := <-waited
	if got.err != nil {
		t.Fatalf("wait_reply: %v", got.err)
	}
	return got.status, got.body
}

// TestSupervisorBrokerReplyAnswersVersionedAsk pins the supervisor's reply
// sender against a versioned ask: BrokerReply posts no generation, yet it must
// answer the edge admitted in the broker store's current generation, whichever
// side reaches the broker first.
func TestSupervisorBrokerReplyAnswersVersionedAsk(t *testing.T) {
	for _, order := range []struct {
		name  string
		drive func(t *testing.T, sup *Supervisor, asker brokerAsker, msgID, answer string) (int, map[string]any)
	}{
		{name: "reply before wait_reply", drive: replyThenWait},
		{name: "wait_reply parked before reply", drive: waitThenReply},
	} {
		t.Run(order.name, func(t *testing.T) {
			sup := newConstructedSupervisor(t)
			askerToken, err := sup.broker.CreateRun("asker", "")
			if err != nil {
				t.Fatalf("create asker: %v", err)
			}
			now := time.Now().UTC()
			msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
			if err != nil {
				t.Fatalf("NewMsgID: %v", err)
			}
			asker := brokerAsker{runID: "asker", token: askerToken, generation: sup.broker.Store().Generation()}

			sendAsk(t, sup, asker, "supervisor", msgID, "status?")
			status, body := order.drive(t, sup, asker, msgID, "the answer")

			if status != http.StatusOK {
				t.Fatalf("wait_reply = (%d, %v), want the supervisor's answer", status, body)
			}
			if body["from_run_id"] != "supervisor" {
				t.Fatalf("wait_reply from_run_id = %v, want supervisor", body["from_run_id"])
			}
			if body["message_id"] != msgID {
				t.Fatalf("wait_reply message_id = %v, want %s", body["message_id"], msgID)
			}
		})
	}
}

// TestNewSupervisorAsksOverItsConstructedTransport pins the wiring the
// constructor owns: the supervisor asks in process, a child answers over the
// broker's HTTP router, and the reply resolves the edge the in-process
// transport admitted.
func TestNewSupervisorAsksOverItsConstructedTransport(t *testing.T) {
	sup := newConstructedSupervisor(t)
	if sup.messaging == nil {
		t.Fatal("NewSupervisor wired no messaging transport")
	}
	targetToken, err := sup.broker.CreateRun("target", "target")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	type askOutcome struct {
		res any
		err error
	}
	asked := make(chan askOutcome, 1)
	go func() {
		res, err := sup.BrokerAsk(context.Background(), "target", "status?", "agent")
		asked <- askOutcome{res: res, err: err}
	}()
	msgID := brokertest.WaitForAgentMessage(t, sup.broker, "target").ID

	reply := fmt.Sprintf(`{"run_id":"target","token":%q,"from_run_id":"target","to_run_id":"supervisor","type":"agent_message","payload":{"id":"reply-1","from":"target","from_run_id":"target","to_run_id":"supervisor","message":"all good","reply_to":%q}}`, targetToken, msgID)
	if status, body := brokerPost(t, sup, "send", []byte(reply)); status != http.StatusOK {
		t.Fatalf("reply send = (%d, %v)", status, body)
	}

	select {
	case out := <-asked:
		if out.err != nil {
			t.Fatalf("BrokerAsk: %v", out.err)
		}
		result, ok := out.res.(map[string]any)
		if !ok {
			t.Fatalf("BrokerAsk result has type %T, want map[string]any", out.res)
		}
		if result["message_id"] != msgID || result["from_run_id"] != "target" {
			t.Fatalf("BrokerAsk result = %v, want the target's answer to %s", result, msgID)
		}
		if payload, _ := result["payload"].(json.RawMessage); !strings.Contains(string(payload), "all good") {
			t.Fatalf("BrokerAsk payload = %s, want the reply text", payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the supervisor's in-process ask never saw its reply")
	}
}

// TestChildAskToSupervisorRunIsQueuedAndDrained pins the child→supervisor leg
// over the broker endpoints a spawned pi child drives: an unversioned ask from
// the child's registered broker run to the supervisor's own run is queued for
// the supervisor, drained by BrokerReceive, and answered to the child's waiting
// message id.
func TestChildAskToSupervisorRunIsQueuedAndDrained(t *testing.T) {
	sup := newConstructedSupervisor(t)
	child := &childRuntime{id: "rt_child", label: "worker"}
	childToken := sup.ensureChildBrokerRun(child)
	if childToken == "" {
		t.Fatal("child broker run was not registered")
	}
	child.mu.Lock()
	childCredential := child.brokerToken
	child.mu.Unlock()
	if childCredential != childToken {
		t.Fatalf("child broker token = %q, want the token registration returned %q", childCredential, childToken)
	}

	const msgID = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
	asker := brokerAsker{runID: child.id, token: childToken}
	sendAsk(t, sup, asker, "supervisor", msgID, "status?")

	drained, err := sup.BrokerReceive()
	if err != nil {
		t.Fatalf("BrokerReceive: %v", err)
	}
	asks, ok := drained.([]map[string]any)
	if !ok || len(asks) != 1 {
		t.Fatalf("drained = %#v, want the child's ask", drained)
	}
	if asks[0]["from_run_id"] != "rt_child" || asks[0]["message_id"] != msgID || asks[0]["message"] != "status?" || asks[0]["role"] != "agent" {
		t.Fatalf("drained ask = %v, want rt_child's status? ask from an agent", asks[0])
	}

	status, body := replyThenWait(t, sup, asker, msgID, "all good")
	if status != http.StatusOK || body["from_run_id"] != "supervisor" {
		t.Fatalf("child wait_reply = (%d, %v), want the supervisor's answer", status, body)
	}
}

// TestAskToEndedChildRunIsQueuedAndTimesOut pins what an ended direct child
// leaves behind: its broker run stays registered, so an ask to its rt_N is
// admitted and queued with a 200, and the asker waits out its own bounded
// timeout instead of being told the target is gone.
func TestAskToEndedChildRunIsQueuedAndTimesOut(t *testing.T) {
	sup := newConstructedSupervisor(t)
	child := &childRuntime{id: "rt_child", label: "worker"}
	sup.controlMu.Lock()
	sup.runtimes[child.id] = child
	sup.controlMu.Unlock()
	if sup.ensureChildBrokerRun(child) == "" {
		t.Fatal("child broker run was not registered")
	}
	// The runtime ends and is reaped; runChild's teardown removes no broker run.
	sup.controlMu.Lock()
	delete(sup.runtimes, child.id)
	sup.controlMu.Unlock()
	if sup.broker.GetRun(child.id) == nil {
		t.Fatal("the child's broker run went away with its runtime")
	}

	askerToken, err := sup.broker.CreateRun("asker", "")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	// A sibling asks with the same random, unversioned message id a pi child
	// mints, so no store generation is pinned on the ask.
	const msgID = "1f6c1a1e-3a1f-4a1e-9d0f-2b3c4d5e6f70"
	asker := brokerAsker{runID: "asker", token: askerToken}
	sendAsk(t, sup, asker, child.id, msgID, "status?")

	status, body, err := waitReply(sup, asker, msgID, 100, nil)
	if err != nil {
		t.Fatalf("wait_reply: %v", err)
	}
	if status != http.StatusGatewayTimeout {
		t.Fatalf("wait_reply = (%d, %v), want the bounded wait to time out", status, body)
	}
	if body["outcome"] != string(messaging.OutcomeTimedOut) {
		t.Fatalf("wait_reply outcome = %v, want %s", body["outcome"], messaging.OutcomeTimedOut)
	}
}

// brokerPost posts a body to a broker endpoint and fails the test on a
// transport error, so only the test goroutine itself uses it.
func brokerPost(t *testing.T, sup *Supervisor, path string, body []byte) (int, map[string]any) {
	t.Helper()
	status, result, err := brokerPOST(sup, path, body, nil)
	if err != nil {
		t.Fatalf("POST /%s: %v", path, err)
	}
	return status, result
}

// brokerPOST posts a broker endpoint body through the supervisor's own broker
// HTTP client under a bounded context, so a never-completing wait fails the
// caller instead of hanging it.
func brokerPOST(sup *Supervisor, path string, body []byte, trace *httptrace.ClientTrace) (int, map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if trace != nil {
		ctx = httptrace.WithClientTrace(ctx, trace)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/%s", sup.broker.Addr(), path), bytes.NewReader(body))
	if err != nil {
		return 0, nil, fmt.Errorf("new /%s request: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := sup.brokerHTTPClientDo(req)
	if err != nil {
		return 0, nil, fmt.Errorf("POST /%s: %w", path, err)
	}
	defer resp.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return resp.StatusCode, nil, fmt.Errorf("decode /%s: %w", path, err)
	}
	return resp.StatusCode, result, nil
}
