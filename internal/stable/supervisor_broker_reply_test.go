package stable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
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
	if _, err := admission.CreateRoot(budget, 1); err != nil {
		t.Fatalf("create tree budget: %v", err)
	}
	sup := NewSupervisor(Config{MaxRuntimes: 1, TreeBudgetFile: budget})
	t.Cleanup(func() {
		sup.closeTreeBudget()
		_ = sup.broker.Stop()
	})
	if sup.broker.GetRun("supervisor") == nil {
		t.Fatal("NewSupervisor did not register the supervisor's broker run")
	}
	return sup
}

// brokerAsker is the registered broker run that asks the supervisor a question
// and waits for the answer.
type brokerAsker struct {
	runID      string
	token      string
	generation string
}

// askSupervisor drives one ask and reply over the broker's HTTP endpoints as a
// registered run: it posts the ask to the supervisor's own run, lets onSent
// inspect the queued ask, parks the reply long-poll, answers it as the
// supervisor, and returns the /wait_reply body.
func askSupervisor(t *testing.T, sup *Supervisor, asker brokerAsker, msgID, question, answer string, onSent func()) map[string]any {
	t.Helper()
	payload := fmt.Sprintf(`{"id":%q,"from":%q,"from_run_id":%q,"to_run_id":"supervisor","message":%q,"expects_reply":true}`,
		msgID, asker.runID, asker.runID, question)
	gen := ""
	if asker.generation != "" {
		gen = fmt.Sprintf(`,"generation":%q`, asker.generation)
	}
	ask := fmt.Sprintf(`{"run_id":%q,"token":%q,"from_run_id":%q,"to_run_id":"supervisor","type":"agent_message","payload":%s%s}`,
		asker.runID, asker.token, asker.runID, payload, gen)
	if status, body := brokerPost(t, sup.broker.Addr(), "send", ask); status != http.StatusOK {
		t.Fatalf("send = (%d, %v)", status, body)
	}
	if onSent != nil {
		onSent()
	}

	wait := fmt.Sprintf(`{"run_id":%q,"token":%q,"waiting_for":%q%s}`, asker.runID, asker.token, msgID, gen)
	type waitOutcome struct {
		status int
		body   map[string]any
		err    error
	}
	waited := make(chan waitOutcome, 1)
	go func() {
		status, body, err := brokerPostWithin(context.Background(), sup.broker.Addr(), "wait_reply", wait)
		waited <- waitOutcome{status: status, body: body, err: err}
	}()
	// Let the long-poll park so the reply wakes it instead of finding an
	// already-completed edge.
	time.Sleep(100 * time.Millisecond)
	if err := sup.BrokerReply(asker.runID, msgID, answer); err != nil {
		t.Fatalf("BrokerReply: %v", err)
	}

	var got waitOutcome
	select {
	case got = <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("the parked wait_reply never saw the supervisor's answer")
	}
	if got.err != nil {
		t.Fatalf("wait_reply: %v", got.err)
	}
	if got.status != http.StatusOK {
		t.Fatalf("wait_reply = (%d, %v), want the supervisor's answer", got.status, got.body)
	}
	if got.body["message_id"] != msgID {
		t.Fatalf("wait_reply message_id = %v, want %s", got.body["message_id"], msgID)
	}
	if payload := fmt.Sprint(got.body["payload"]); !strings.Contains(payload, answer) {
		t.Fatalf("wait_reply = %v, want a payload carrying the answer text", got.body)
	}
	return got.body
}

// TestSupervisorBrokerReplyAnswersVersionedAsk pins the supervisor's reply
// sender against a versioned ask: BrokerReply posts no generation, yet it must
// answer the edge admitted in the broker store's current generation.
func TestSupervisorBrokerReplyAnswersVersionedAsk(t *testing.T) {
	sup := newConstructedSupervisor(t)
	askerToken, err := sup.broker.CreateRun("asker", "")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	gen := sup.broker.Store().Generation()
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}

	body := askSupervisor(t, sup, brokerAsker{runID: "asker", token: askerToken, generation: gen}, msgID, "status?", "the answer", nil)
	if body["from_run_id"] != "supervisor" {
		t.Fatalf("wait_reply from_run_id = %v, want supervisor", body["from_run_id"])
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
	if status, body := brokerPost(t, sup.broker.Addr(), "send", reply); status != http.StatusOK {
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
	inspect := func() {
		t.Helper()
		drained, err := sup.BrokerReceive()
		if err != nil {
			t.Fatalf("BrokerReceive: %v", err)
		}
		asks, ok := drained.([]map[string]any)
		if !ok || len(asks) != 1 {
			t.Fatalf("drained = %#v, want the child's ask", drained)
		}
		if asks[0]["from_run_id"] != "rt_child" || asks[0]["message_id"] != msgID || asks[0]["message"] != "status?" {
			t.Fatalf("drained ask = %v, want rt_child's status? ask", asks[0])
		}
	}
	body := askSupervisor(t, sup, brokerAsker{runID: "rt_child", token: childToken}, msgID, "status?", "all good", inspect)
	if body["from_run_id"] != "supervisor" {
		t.Fatalf("child wait_reply = %v, want the supervisor's answer", body)
	}
}

// brokerPost posts a body to a broker endpoint under a bounded context, so a
// never-completing wait fails the test instead of hanging it.
func brokerPost(t *testing.T, addr, path, body string) (int, map[string]any) {
	t.Helper()
	status, result, err := brokerPostWithin(context.Background(), addr, path, body)
	if err != nil {
		t.Fatalf("POST /%s: %v", path, err)
	}
	return status, result
}

// brokerPostWithin is brokerPost for callers that cannot fail the test, such
// as a goroutine parked on the long-poll.
func brokerPostWithin(parent context.Context, addr, path, body string) (int, map[string]any, error) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/%s", addr, path), bytes.NewReader([]byte(body)))
	if err != nil {
		return 0, nil, fmt.Errorf("new /%s request: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
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
