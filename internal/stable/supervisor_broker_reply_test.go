package stable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/broker/brokertest"
)

// TestSupervisorBrokerReplyAnswersVersionedAsk pins the supervisor's reply
// sender against a versioned ask: BrokerReply posts no generation, yet it must
// answer the edge admitted in the broker store's current generation.
func TestSupervisorBrokerReplyAnswersVersionedAsk(t *testing.T) {
	sup := &Supervisor{broker: broker.New("")}
	if err := sup.broker.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = sup.broker.Stop() })
	addr := sup.broker.Addr()
	if runID, _ := sup.registerBrokerRun(); runID == "" {
		t.Fatal("supervisor run was not registered with the broker")
	}

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

	payload := fmt.Sprintf(`{"id":%q,"from":"asker","from_run_id":"asker","to_run_id":"supervisor","message":"status?","expects_reply":true}`, msgID)
	ask := fmt.Sprintf(`{"run_id":"asker","token":%q,"from_run_id":"asker","to_run_id":"supervisor","type":"agent_message","payload":%s,"generation":%q}`,
		askerToken, payload, gen)
	if status, body := brokerPost(t, addr, "send", ask); status != http.StatusOK {
		t.Fatalf("send = (%d, %v)", status, body)
	}

	if err := sup.BrokerReply("asker", msgID, "the answer"); err != nil {
		t.Fatalf("BrokerReply: %v", err)
	}

	wait := fmt.Sprintf(`{"run_id":"asker","token":%q,"waiting_for":%q,"generation":%q}`, askerToken, msgID, gen)
	status, body := brokerPost(t, addr, "wait_reply", wait)
	if status != http.StatusOK || body["from_run_id"] != "supervisor" {
		t.Fatalf("wait_reply = (%d, %v), want the supervisor's answer", status, body)
	}
}

// brokerPost posts a body to a broker endpoint under a bounded context, so a
// never-completing wait fails the test instead of hanging it.
func brokerPost(t *testing.T, addr, path, body string) (int, map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/%s", addr, path), bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("new /%s request: %v", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /%s: %v", path, err)
	}
	defer resp.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode /%s: %v", path, err)
	}
	return resp.StatusCode, result
}

// TestNewSupervisorAsksOverItsConstructedTransport pins the wiring the
// constructor owns: the supervisor asks in process, a child answers over the
// broker's HTTP router, and the reply resolves the edge the in-process
// transport admitted.
func TestNewSupervisorAsksOverItsConstructedTransport(t *testing.T) {
	sup := NewSupervisor(Config{MaxRuntimes: 1})
	t.Cleanup(func() { _ = sup.broker.Stop() })
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
// over the same broker HTTP path a spawned pi child uses: an unversioned ask
// from the child's registered broker run to the supervisor's own run is queued
// for the supervisor, drained by BrokerReceive, and answered to the child's
// waiting message id.
func TestChildAskToSupervisorRunIsQueuedAndDrained(t *testing.T) {
	sup := NewSupervisor(Config{MaxRuntimes: 1})
	t.Cleanup(func() { _ = sup.broker.Stop() })
	if runID, _ := sup.registerBrokerRun(); runID != "supervisor" {
		t.Fatalf("supervisor broker run = %q, want supervisor", runID)
	}
	child := &childRuntime{id: "rt_child", label: "worker"}
	childToken := sup.ensureChildBrokerRun(child)
	if childToken == "" {
		t.Fatal("child broker run was not registered")
	}

	const msgID = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
	payload := fmt.Sprintf(`{"id":%q,"from":"rt_child","from_run_id":"rt_child","to_run_id":"supervisor","message":"status?","role":"agent","expects_reply":true}`, msgID)
	ask := fmt.Sprintf(`{"run_id":"rt_child","token":%q,"from_run_id":"rt_child","to_run_id":"supervisor","type":"agent_message","payload":%s}`, childToken, payload)
	if status, body := brokerPost(t, sup.broker.Addr(), "send", ask); status != http.StatusOK {
		t.Fatalf("child send = (%d, %v)", status, body)
	}

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

	if err := sup.BrokerReply("rt_child", msgID, "all good"); err != nil {
		t.Fatalf("BrokerReply: %v", err)
	}
	wait := fmt.Sprintf(`{"run_id":"rt_child","token":%q,"waiting_for":%q}`, childToken, msgID)
	status, body := brokerPost(t, sup.broker.Addr(), "wait_reply", wait)
	if status != http.StatusOK || body["from_run_id"] != "supervisor" {
		t.Fatalf("child wait_reply = (%d, %v), want the supervisor's answer", status, body)
	}
}
