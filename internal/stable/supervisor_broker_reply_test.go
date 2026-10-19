package stable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
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
