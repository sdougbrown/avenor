package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestBrokerHealth(t *testing.T) {
	b := New("test-token")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	resp, err := http.Get(fmt.Sprintf("http://%s/health", b.Addr()))
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health: %d", resp.StatusCode)
	}
}

func TestBrokerRegister(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	body := bytes.NewReader([]byte(`{"run_id":"run_1"}`))
	resp, err := http.Post(fmt.Sprintf("http://%s/register", b.Addr()), "application/json", body)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register: %d", resp.StatusCode)
	}
	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["run_id"] != "run_1" {
		t.Fatalf("run_id mismatch: %v", result)
	}
	if result["token"] == "" {
		t.Fatal("token missing")
	}

	// duplicate must fail
	body2 := bytes.NewReader([]byte(`{"run_id":"run_1"}`))
	resp2, err := http.Post(fmt.Sprintf("http://%s/register", b.Addr()), "application/json", body2)
	if err != nil {
		t.Fatalf("duplicate register: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate: expected 409, got %d", resp2.StatusCode)
	}
}

func TestBrokerRegisterExistingRunWithMatchingToken(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("run_existing")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"run_existing","token":"%s"}`, token)))
	resp, err := http.Post(fmt.Sprintf("http://%s/register", b.Addr()), "application/json", body)
	if err != nil {
		t.Fatalf("register existing: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register existing: expected 200, got %d", resp.StatusCode)
	}
	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["token"] != token {
		t.Fatalf("token = %q, want %q", result["token"], token)
	}
}

func TestBrokerPushControlAndPoll(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	// register
	regBody := bytes.NewReader([]byte(`{"run_id":"run_2"}`))
	resp, err := http.Post(fmt.Sprintf("http://%s/register", b.Addr()), "application/json", regBody)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	var regResult map[string]string
	json.NewDecoder(resp.Body).Decode(&regResult)
	resp.Body.Close()
	token := regResult["token"]

	// push control (via direct PushControl, bypasses auth for test)
	b.PushControl("run_2", ControlMessage{ID: "ctrl_1", Type: "continue", RunID: "run_2", Payload: []byte(`{"msg":"hello"}`)})

	// poll control
	pollBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"run_2","token":"%s"}`, token)))
	resp, err = http.Post(fmt.Sprintf("http://%s/poll-control", b.Addr()), "application/json", pollBody)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("poll: %d", resp.StatusCode)
	}
	var msgs []ControlMessage
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].ID != "ctrl_1" {
		t.Fatalf("unexpected id: %s", msgs[0].ID)
	}
}

func TestBrokerPollDrainsStaleNotifyAfterQueuedMessage(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("run_poll")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := b.PushControl("run_poll", ControlMessage{ID: "ctrl_1", Type: "continue", RunID: "run_poll"}); err != nil {
		t.Fatalf("push control: %v", err)
	}

	poll := func(ctx context.Context) (*http.Response, error) {
		body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"run_poll","token":"%s"}`, token)))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/poll-control", b.Addr()), body)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		return http.DefaultClient.Do(req)
	}

	resp, err := poll(context.Background())
	if err != nil {
		t.Fatalf("first poll: %v", err)
	}
	var msgs []ControlMessage
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode first poll: %v", err)
	}
	resp.Body.Close()
	if len(msgs) != 1 {
		t.Fatalf("first poll got %d messages, want 1", len(msgs))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	resp, err = poll(ctx)
	if err == nil {
		resp.Body.Close()
		t.Fatal("second empty poll returned before request context timed out")
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("second empty poll returned too quickly after %s", elapsed)
	}
}

func TestBrokerHeartbeat(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	regBody := bytes.NewReader([]byte(`{"run_id":"run_3"}`))
	resp, err := http.Post(fmt.Sprintf("http://%s/register", b.Addr()), "application/json", regBody)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	var regResult map[string]string
	json.NewDecoder(resp.Body).Decode(&regResult)
	resp.Body.Close()
	token := regResult["token"]

	// heartbeat
	hbBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"run_3","token":"%s"}`, token)))
	resp, err = http.Post(fmt.Sprintf("http://%s/heartbeat", b.Addr()), "application/json", hbBody)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// verify LastSeen was updated
	st := b.GetRun("run_3")
	if st == nil {
		t.Fatal("run not found")
	}
	if st.LastSeen.IsZero() {
		t.Fatal("LastSeen should be updated after heartbeat")
	}
}

func TestBrokerReports(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	regBody := bytes.NewReader([]byte(`{"run_id":"run_4"}`))
	resp, err := http.Post(fmt.Sprintf("http://%s/register", b.Addr()), "application/json", regBody)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	var regResult map[string]string
	json.NewDecoder(resp.Body).Decode(&regResult)
	resp.Body.Close()
	token := regResult["token"]

	// report
	repBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"run_4","token":"%s","state":"working","payload":{"summary":"ok"}}`, token)))
	resp, err = http.Post(fmt.Sprintf("http://%s/report", b.Addr()), "application/json", repBody)
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("report: %d", resp.StatusCode)
	}
	resp.Body.Close()

	st := b.GetRun("run_4")
	if st == nil {
		t.Fatal("run not found")
	}
	if len(st.Reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(st.Reports))
	}
	if st.Reports[0].State != "working" {
		t.Fatalf("unexpected state: %s", st.Reports[0].State)
	}
	if st.LastSeen.IsZero() {
		t.Fatal("LastSeen should be updated after report")
	}

	// finish
	finBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"run_4","token":"%s","status":"done","summary":"finished"}`, token)))
	resp, err = http.Post(fmt.Sprintf("http://%s/finish", b.Addr()), "application/json", finBody)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("finish: %d", resp.StatusCode)
	}
	resp.Body.Close()

	if len(st.Finishes) != 1 {
		t.Fatalf("expected 1 finish, got %d", len(st.Finishes))
	}
	if st.Finishes[0].Status != "done" {
		t.Fatalf("unexpected status: %s", st.Finishes[0].Status)
	}
}

func TestBrokerAuthRejection(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	regBody := bytes.NewReader([]byte(`{"run_id":"run_5"}`))
	resp, err := http.Post(fmt.Sprintf("http://%s/register", b.Addr()), "application/json", regBody)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	resp.Body.Close()

	// missing run_id
	badBody := bytes.NewReader([]byte(`{"token":"x"}`))
	resp, err = http.Post(fmt.Sprintf("http://%s/report", b.Addr()), "application/json", badBody)
	if err != nil {
		t.Fatalf("bad auth: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// wrong token
	wrongBody := bytes.NewReader([]byte(`{"run_id":"run_5","token":"wrong"}`))
	resp, err = http.Post(fmt.Sprintf("http://%s/report", b.Addr()), "application/json", wrongBody)
	if err != nil {
		t.Fatalf("wrong token: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token: expected 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestBrokerTimeout(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	regBody := bytes.NewReader([]byte(`{"run_id":"run_6"}`))
	resp, err := http.Post(fmt.Sprintf("http://%s/register", b.Addr()), "application/json", regBody)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	var regResult map[string]string
	json.NewDecoder(resp.Body).Decode(&regResult)
	resp.Body.Close()
	token := regResult["token"]

	// poll with no messages should not hang
	pollBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"run_6","token":"%s"}`, token)))
	resp, err = http.Post(fmt.Sprintf("http://%s/poll-control", b.Addr()), "application/json", pollBody)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("poll: %d", resp.StatusCode)
	}
	var msgs []ControlMessage
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(msgs))
	}
}

func TestBrokerDeleteRun(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	if _, err := b.CreateRun("run_to_delete"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if st := b.GetRun("run_to_delete"); st == nil {
		t.Fatal("GetRun should return state before delete")
	}

	b.DeleteRun("run_to_delete")

	if st := b.GetRun("run_to_delete"); st != nil {
		t.Fatal("GetRun should return nil after delete")
	}

	// DeleteRun on nonexistent run should not panic.
	b.DeleteRun("nonexistent")
}

func TestEnvelopeConversion(t *testing.T) {
	payload := json.RawMessage(`{"key":"val"}`)

	t.Run("Report", func(t *testing.T) {
		r := Report{RunID: "r1", State: "working", Payload: payload}
		e := r.ToEnvelope()
		if e.FromRunID != "r1" {
			t.Errorf("FromRunID = %q, want r1", e.FromRunID)
		}
		if e.Kind != "working" {
			t.Errorf("Kind = %q, want working", e.Kind)
		}
		if string(e.Payload) != `{"key":"val"}` {
			t.Errorf("Payload = %s", e.Payload)
		}
	})

	t.Run("Finish", func(t *testing.T) {
		f := Finish{RunID: "r2", Status: "done", Summary: "ok", Payload: payload}
		e := f.ToEnvelope()
		if e.Kind != "done" {
			t.Errorf("Kind = %q, want done", e.Kind)
		}
	})

	t.Run("FinishNoStatus", func(t *testing.T) {
		f := Finish{RunID: "r3"}
		e := f.ToEnvelope()
		if e.Kind != "done" {
			t.Errorf("Kind = %q, want done", e.Kind)
		}
	})

	t.Run("Reply", func(t *testing.T) {
		r := Reply{RunID: "r4", To: "controller", Payload: payload}
		e := r.ToEnvelope()
		if e.To != "controller" {
			t.Errorf("To = %q, want controller", e.To)
		}
	})

	t.Run("ControlMessage", func(t *testing.T) {
		now := time.Now()
		m := ControlMessage{ID: "ctrl_1", Type: "continue", RunID: "r5", Payload: payload, CreatedAt: now}
		e := m.ToEnvelope()
		if e.ToRunID != "r5" {
			t.Errorf("ToRunID = %q, want r5", e.ToRunID)
		}
		if e.Kind != "continue" {
			t.Errorf("Kind = %q, want continue", e.Kind)
		}
		if e.CorrelationID != "ctrl_1" {
			t.Errorf("CorrelationID = %q, want ctrl_1", e.CorrelationID)
		}
		if !e.CreatedAt.Equal(now) {
			t.Errorf("CreatedAt = %v, want %v", e.CreatedAt, now)
		}
	})
}

func TestBrokerSend(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("run_send")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	payload := json.RawMessage(`{"msg":"hello"}`)
	if err := b.Send("run_send", "continue", payload, "corr_1"); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Verify the control message was enqueued by polling.
	pollBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"run_send","token":"%s"}`, token)))
	resp, err := http.Post(fmt.Sprintf("http://%s/poll-control", b.Addr()), "application/json", pollBody)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("poll: %d", resp.StatusCode)
	}
	var msgs []ControlMessage
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].ID != "corr_1" {
		t.Errorf("ID = %q, want corr_1", msgs[0].ID)
	}
	if msgs[0].Type != "continue" {
		t.Errorf("Type = %q, want continue", msgs[0].Type)
	}
}

func TestBrokerSendNonexistentRun(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	err := b.Send("nonexistent", "continue", nil, "")
	if err == nil {
		t.Fatal("expected error for nonexistent run")
	}
}

func TestBrokerIngestReport(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("run_ingest_report")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	_ = token

	payload := json.RawMessage(`{"summary":"progress"}`)
	if err := b.Ingest("run_ingest_report", "thinking", payload); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	st := b.GetRun("run_ingest_report")
	if st == nil {
		t.Fatal("run not found")
	}
	if len(st.Reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(st.Reports))
	}
	if st.Reports[0].State != "thinking" {
		t.Errorf("State = %q, want thinking", st.Reports[0].State)
	}
}

func TestBrokerIngestFinish(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("run_ingest_finish")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	_ = token

	payload := json.RawMessage(`{"result":"ok"}`)
	if err := b.Ingest("run_ingest_finish", "done", payload); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	st := b.GetRun("run_ingest_finish")
	if st == nil {
		t.Fatal("run not found")
	}
	if len(st.Finishes) != 1 {
		t.Fatalf("expected 1 finish, got %d", len(st.Finishes))
	}
	if st.Finishes[0].Status != "done" {
		t.Errorf("Status = %q, want done", st.Finishes[0].Status)
	}
}

func TestBrokerIngestNonexistentRun(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	err := b.Ingest("nonexistent", "working", nil)
	if err == nil {
		t.Fatal("expected error for nonexistent run")
	}
}

func TestBrokerSendTo(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	// Create two runs
	senderToken, err := b.CreateRun("sender")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("target")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	// Send via HTTP POST /send
	sendBody := bytes.NewReader([]byte(`{
		"run_id": "sender",
		"token": "` + senderToken + `",
		"from_run_id": "sender",
		"to_run_id": "target",
		"type": "agent_message",
		"payload": {"message": "hello from sender"}
	}`))
	resp, err := http.Post(fmt.Sprintf("http://%s/send", b.Addr()), "application/json", sendBody)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("send: %d", resp.StatusCode)
	}

	// Poll the target to verify the message was enqueued with FromRunID
	pollBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"target","token":"%s"}`, targetToken)))
	resp2, err := http.Post(fmt.Sprintf("http://%s/poll-control", b.Addr()), "application/json", pollBody)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("poll: %d", resp2.StatusCode)
	}
	var msgs []ControlMessage
	if err := json.NewDecoder(resp2.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].FromRunID != "sender" {
		t.Errorf("FromRunID = %q, want sender", msgs[0].FromRunID)
	}
	if msgs[0].Type != "agent_message" {
		t.Errorf("Type = %q, want agent_message", msgs[0].Type)
	}
}

func TestBrokerSendToUnknownTarget(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("sender")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}

	sendBody := bytes.NewReader([]byte(`{
		"run_id": "sender",
		"token": "` + token + `",
		"from_run_id": "sender",
		"to_run_id": "nonexistent",
		"type": "agent_message",
		"payload": {"message": "hello"}
	}`))
	resp, err := http.Post(fmt.Sprintf("http://%s/send", b.Addr()), "application/json", sendBody)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestBrokerSendAuthFailure(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	_, err := b.CreateRun("sender")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	_, err = b.CreateRun("target")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	// Wrong token for from_run_id
	sendBody := bytes.NewReader([]byte(`{
		"run_id": "sender",
		"token": "wrong_token",
		"from_run_id": "sender",
		"to_run_id": "target",
		"type": "agent_message",
		"payload": {"message": "hello"}
	}`))
	resp, err := http.Post(fmt.Sprintf("http://%s/send", b.Addr()), "application/json", sendBody)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestBrokerSendAutoCorrelationID(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("sender_auto")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	b.DeleteRun("target_auto")
	targetToken, err := b.CreateRun("target_auto")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	// Send without correlation ID
	sendBody := bytes.NewReader([]byte(`{
		"run_id": "sender_auto",
		"token": "` + senderToken + `",
		"from_run_id": "sender_auto",
		"to_run_id": "target_auto",
		"type": "agent_message",
		"payload": {"message": "auto corr"}
	}`))
	resp, err := http.Post(fmt.Sprintf("http://%s/send", b.Addr()), "application/json", sendBody)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	resp.Body.Close()

	// Poll to verify correlation ID was auto-generated
	pollBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"target_auto","token":"%s"}`, targetToken)))
	resp2, err := http.Post(fmt.Sprintf("http://%s/poll-control", b.Addr()), "application/json", pollBody)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("poll: %d", resp2.StatusCode)
	}
	var msgs []ControlMessage
	if err := json.NewDecoder(resp2.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].ID == "" {
		t.Error("expected auto-generated correlation ID")
	}
}

func TestPollAgentMessagesUsesAuthenticatedFromRunID(t *testing.T) {
	b := New("")
	st := &RunState{}
	st.ControlQueue = []*ControlMessage{{
		Type:      "agent_message",
		FromRunID: "trusted_sender",
		Payload:   json.RawMessage(`{"from_run_id":"spoofed_sender","message":"hello","role":"supervisor"}`),
	}}
	b.mu.Lock()
	b.runs["target"] = st
	b.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan string, 1)
	go b.PollAgentMessages(ctx, "target", func(wrapped string) {
		got <- wrapped
		cancel()
	})

	select {
	case wrapped := <-got:
		if !strings.Contains(wrapped, `from_run_id="trusted_sender"`) {
			t.Fatalf("wrapped message missing authenticated sender: %s", wrapped)
		}
		if strings.Contains(wrapped, `from_run_id="spoofed_sender"`) {
			t.Fatalf("wrapped message used spoofed sender: %s", wrapped)
		}
		if strings.Contains(wrapped, `from_role=`) {
			t.Fatalf("wrapped message trusted sender-controlled role: %s", wrapped)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for wrapped agent message")
	}
}

// --- Ask/Reply Tests ---

// sendAsk is a helper that sends a message with expects_reply=true
// and returns the response body as a map. It creates the full broker
// request with auth fields.
func sendAsk(t *testing.T, addr, fromToken, fromRunID, toRunID, msgID, message string) map[string]any {
	t.Helper()
	payload := fmt.Sprintf(`{"id":%q,"from":%q,"from_run_id":%q,"to_run_id":%q,"message":%q,"expects_reply":true}`,
		msgID, fromRunID, fromRunID, toRunID, message)
	body := bytes.NewReader([]byte(fmt.Sprintf(`{
		"run_id": %q,
		"token": %q,
		"from_run_id": %q,
		"to_run_id": %q,
		"type": "agent_message",
		"payload": %s
	}`, fromRunID, fromToken, fromRunID, toRunID, payload)))
	resp, err := http.Post(fmt.Sprintf("http://%s/send", addr), "application/json", body)
	if err != nil {
		t.Fatalf("sendAsk: %v", err)
	}
	defer resp.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("sendAsk decode: %v", err)
	}
	result["status"] = resp.StatusCode
	return result
}

// sendReply is a helper that sends a reply (reply_to set) and returns
// the response body.
func sendReply(t *testing.T, addr, fromToken, fromRunID, toRunID, replyTo, message string) map[string]any {
	t.Helper()
	payload := fmt.Sprintf(`{"id":"reply-%s","from":%q,"from_run_id":%q,"to_run_id":%q,"message":%q,"reply_to":%q}`,
		replyTo, fromRunID, fromRunID, toRunID, message, replyTo)
	body := bytes.NewReader([]byte(fmt.Sprintf(`{
		"run_id": %q,
		"token": %q,
		"from_run_id": %q,
		"to_run_id": %q,
		"type": "agent_message",
		"payload": %s
	}`, fromRunID, fromToken, fromRunID, toRunID, payload)))
	resp, err := http.Post(fmt.Sprintf("http://%s/send", addr), "application/json", body)
	if err != nil {
		t.Fatalf("sendReply: %v", err)
	}
	defer resp.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("sendReply decode: %v", err)
	}
	result["status"] = resp.StatusCode
	return result
}

// fireAndForgetSend sends a plain fire-and-forget message (no expects_reply, no reply_to).
func fireAndForgetSend(t *testing.T, addr, fromToken, fromRunID, toRunID, message string) int {
	t.Helper()
	payload := fmt.Sprintf(`{"from":%q,"from_run_id":%q,"message":%q}`, fromRunID, fromRunID, message)
	body := bytes.NewReader([]byte(fmt.Sprintf(`{
		"run_id": %q,
		"token": %q,
		"from_run_id": %q,
		"to_run_id": %q,
		"type": "agent_message",
		"payload": %s
	}`, fromRunID, fromToken, fromRunID, toRunID, payload)))
	resp, err := http.Post(fmt.Sprintf("http://%s/send", addr), "application/json", body)
	if err != nil {
		t.Fatalf("fireAndForgetSend: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// waitReply calls /wait_reply and returns the result.
func waitReply(t *testing.T, addr, token, runID, waitingFor string) map[string]any {
	t.Helper()
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":%q,"token":%q,"waiting_for":%q}`, runID, token, waitingFor)))
	// Use a short timeout so tests don't hang for DefaultAskTimeout (10m) on failure.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://%s/wait_reply", addr), body)
	if err != nil {
		t.Fatalf("waitReply new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("waitReply: %v", err)
	}
	defer resp.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("waitReply decode: %v", err)
	}
	result["status"] = resp.StatusCode
	return result
}

func TestBrokerAskReplyHappyPath(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	// Register two runs
	senderToken, err := b.CreateRun("asker")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	replierToken, err := b.CreateRun("replier")
	if err != nil {
		t.Fatalf("create replier: %v", err)
	}

	addr := b.Addr()
	msgID := "ask-001"

	// Start a goroutine that polls for the ask to arrive and then sends a reply.
	// Poll in a loop so we don't miss the message if the poll returns before the
	// message is enqueued (race between sendAsk and poll-control).
	go func() {
		for i := 0; i < 50; i++ {
			pollBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"replier","token":%q}`, replierToken)))
			resp, err := http.Post(fmt.Sprintf("http://%s/poll-control", addr), "application/json", pollBody)
			if err != nil {
				t.Errorf("replier poll: %v", err)
				return
			}
			var msgs []ControlMessage
			if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
				resp.Body.Close()
				t.Errorf("replier decode: %v", err)
				return
			}
			resp.Body.Close()
			if len(msgs) > 0 {
				// Got the message, send the reply.
				result := sendReply(t, addr, replierToken, "replier", "asker", msgID, "here is your answer")
				if result["status"] != http.StatusOK {
					t.Errorf("sendReply status = %d", result["status"])
				}
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Error("replier: timed out waiting for ask message")
	}()

	// Send the ask
	askResult := sendAsk(t, addr, senderToken, "asker", "replier", msgID, "what is the answer?")
	if st := askResult["status"]; st != http.StatusOK {
		t.Fatalf("sendAsk status = %d", st)
	}
	if askResult["expects_reply"] != true {
		t.Fatal("sendAsk response missing expects_reply")
	}
	if askResult["message_id"] != msgID {
		t.Fatalf("sendAsk message_id = %q, want %q", askResult["message_id"], msgID)
	}

	// Wait for the reply via /wait_reply
	waitResult := waitReply(t, addr, senderToken, "asker", msgID)
	if st := waitResult["status"]; st != http.StatusOK {
		t.Fatalf("waitReply status = %d", st)
	}
	if waitResult["message_id"] != msgID {
		t.Errorf("waitReply message_id = %q, want %q", waitResult["message_id"], msgID)
	}
	if waitResult["from_run_id"] != "replier" {
		t.Errorf("waitReply from_run_id = %q, want replier", waitResult["from_run_id"])
	}

	// Verify the ask edge was cleaned up
	st := b.GetRun("asker")
	if st == nil {
		t.Fatal("asker run not found")
	}
	st.Mu.Lock()
	_, pendingOk := st.PendingAsks[msgID]
	_, waitOk := st.WaitingReply[msgID]
	st.Mu.Unlock()
	if pendingOk {
		t.Error("ask edge should have been removed after reply")
	}
	if waitOk {
		t.Error("waiting reply channel should have been removed after reply")
	}
}

func TestBrokerAskReplyFireAndForgetRemainsUnchanged(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("ff_sender")
	if err != nil {
		t.Fatalf("create ff_sender: %v", err)
	}
	targetToken, err := b.CreateRun("ff_target")
	if err != nil {
		t.Fatalf("create ff_target: %v", err)
	}

	// Fire-and-forget (no expects_reply, no reply_to)
	status := fireAndForgetSend(t, b.Addr(), senderToken, "ff_sender", "ff_target", "just a note")
	if status != http.StatusOK {
		t.Errorf("fire-and-forget status = %d, want 200", status)
	}

	// Verify no ask edge was created
	st := b.GetRun("ff_sender")
	if st == nil {
		t.Fatal("ff_sender run not found")
	}
	st.Mu.Lock()
	pendingCount := len(st.PendingAsks)
	st.Mu.Unlock()
	if pendingCount != 0 {
		t.Errorf("expected 0 pending asks for fire-and-forget, got %d", pendingCount)
	}

	// Verify target got the message
	pollBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"ff_target","token":%q}`, targetToken)))
	resp, err := http.Post(fmt.Sprintf("http://%s/poll-control", b.Addr()), "application/json", pollBody)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	defer resp.Body.Close()
	var msgs []ControlMessage
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected 1 message, got %d", len(msgs))
	}
}

func TestBrokerAskRequiresMessageID(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("asker_no_id")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	_, err = b.CreateRun("target_no_id")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	// Send an ask without a message ID — should fail with 400.
	payload := `{"from":"asker_no_id","from_run_id":"asker_no_id","message":"no id","expects_reply":true}`
	body := bytes.NewReader([]byte(fmt.Sprintf(`{
		"run_id": "asker_no_id",
		"token": %q,
		"from_run_id": "asker_no_id",
		"to_run_id": "target_no_id",
		"type": "agent_message",
		"payload": %s
	}`, senderToken, payload)))
	resp, err := http.Post(fmt.Sprintf("http://%s/send", b.Addr()), "application/json", body)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for ask without message ID, got %d", resp.StatusCode)
	}
}

func TestBrokerMutualAskGuard(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	aToken, err := b.CreateRun("agent_a")
	if err != nil {
		t.Fatalf("create agent_a: %v", err)
	}
	bToken, err := b.CreateRun("agent_b")
	if err != nil {
		t.Fatalf("create agent_b: %v", err)
	}

	addr := b.Addr()

	// Agent A asks Agent B — should succeed.
	result := sendAsk(t, addr, aToken, "agent_a", "agent_b", "ask-a-to-b", "question from A")
	if st := result["status"]; st != http.StatusOK {
		t.Fatalf("first ask should succeed, got %d", st)
	}

	// Agent B tries to ask Agent A — should fail with mutual-ask guard.
	result2 := sendAsk(t, addr, bToken, "agent_b", "agent_a", "ask-b-to-a", "question from B")
	if st := result2["status"]; st != http.StatusConflict {
		t.Errorf("mutual ask expected 409, got %d", st)
	}
}

func TestBrokerCancelMessage(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("canceller")
	if err != nil {
		t.Fatalf("create canceller: %v", err)
	}
	targetToken, err := b.CreateRun("cancel_target")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	addr := b.Addr()
	msgID := "cancel-me"

	// Send an ask
	result := sendAsk(t, addr, senderToken, "canceller", "cancel_target", msgID, "will be cancelled")
	if st := result["status"]; st != http.StatusOK {
		t.Fatalf("sendAsk status = %d", st)
	}

	// Verify the ask edge exists
	st := b.GetRun("canceller")
	if st == nil {
		t.Fatal("canceller run not found")
	}
	st.Mu.Lock()
	_, pendingOk := st.PendingAsks[msgID]
	st.Mu.Unlock()
	if !pendingOk {
		t.Fatal("ask edge should exist before cancel")
	}

	// First: other run trying to cancel someone else's ask should fail.
	// Use a different message ID so the test doesn't interfere with the
	// successful cancel below.
	otherMsgID := "other-ask"
	result2 := sendAsk(t, addr, senderToken, "canceller", "cancel_target", otherMsgID, "another ask")
	if st := result2["status"]; st != http.StatusOK {
		t.Fatalf("sendAsk for wrong-owner test status = %d", st)
	}
	// cancel_target tries to cancel a message sent by canceller.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reqBody := []byte(fmt.Sprintf(`{"run_id":"cancel_target","token":%q,"cancel_message_id":%q}`, targetToken, otherMsgID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://%s/cancel_message", addr), bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("cancel wrong owner request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp3, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("cancel wrong owner: %v", err)
	}
	defer resp3.Body.Close()
	// With namespaced keys, the wrong owner can't even find the edge (404)
	// instead of finding it but being denied (403). Both are acceptable.
	if resp3.StatusCode != http.StatusForbidden && resp3.StatusCode != http.StatusNotFound {
		t.Errorf("cancel wrong owner expected 403 or 404, got %d", resp3.StatusCode)
	}

	// Now test the successful cancel on the original message.
	cancelBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":%q,"token":%q,"cancel_message_id":%q}`, "canceller", senderToken, msgID)))
	resp, err := http.Post(fmt.Sprintf("http://%s/cancel_message", addr), "application/json", cancelBody)
	if err != nil {
		t.Fatalf("cancel_message: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel_message status = %d", resp.StatusCode)
	}
	var cancelResult map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&cancelResult); err != nil {
		t.Fatalf("cancel decode: %v", err)
	}
	if cancelResult["cancelled"] != true {
		t.Errorf("cancel response missing cancelled=true")
	}

	// Verify the ask edge was cleaned up
	st.Mu.Lock()
	_, pendingOk = st.PendingAsks[msgID]
	_, waitOk := st.WaitingReply[msgID]
	st.Mu.Unlock()
	if pendingOk {
		t.Error("ask edge should have been removed after cancel")
	}
	if waitOk {
		t.Error("waiting reply channel should have been removed after cancel")
	}

	// Cancelling a non-existent message should fail
	cancelBody2 := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":%q,"token":%q,"cancel_message_id":"nonexistent"}`, "canceller", senderToken)))
	resp2, err := http.Post(fmt.Sprintf("http://%s/cancel_message", addr), "application/json", cancelBody2)
	if err != nil {
		t.Fatalf("cancel nonexistent: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("cancel nonexistent expected 404, got %d", resp2.StatusCode)
	}
}

func TestBrokerWaitReplyForNonexistentAsk(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("nobody")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Wait for a message that was never asked
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"nobody","token":%q,"waiting_for":"ghost"}`, token)))
	resp, err := http.Post(fmt.Sprintf("http://%s/wait_reply", b.Addr()), "application/json", body)
	if err != nil {
		t.Fatalf("wait_reply: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for nonexistent ask, got %d", resp.StatusCode)
	}
}

func TestBrokerReplyAfterEdgeExpired(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("sender_expired")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	_, err = b.CreateRun("target_expired")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	addr := b.Addr()
	msgID := "expired-ask"

	// Send an ask
	result := sendAsk(t, addr, senderToken, "sender_expired", "target_expired", msgID, "will this expire?")
	if st := result["status"]; st != http.StatusOK {
		t.Fatalf("sendAsk status = %d", st)
	}

	// Manually remove the ask edge from both per-run and global registries
	// to simulate expiry.
	st := b.GetRun("sender_expired")
	if st == nil {
		t.Fatal("sender_expired run not found")
	}
	b.globalAskEdgesMu.Lock()
	delete(b.globalAskEdges, msgID)
	delete(b.globalWaitReplies, msgID)
	b.globalAskEdgesMu.Unlock()
	st.Mu.Lock()
	delete(st.PendingAsks, msgID)
	delete(st.WaitingReply, msgID)
	st.Mu.Unlock()

	// Send a reply — should fall back to fire-and-forget
	replyResult := sendReply(t, addr, senderToken, "sender_expired", "target_expired", msgID, "late reply")
	if st := replyResult["status"]; st != http.StatusOK {
		t.Fatalf("sendReply after expiry status = %d", st)
	}
	if replyResult["note"] == nil {
		t.Error("expected 'note' about fire-and-forget fallback in reply response")
	}

	// Verify the reply was delivered as a regular control message
	targetSt := b.GetRun("target_expired")
	if targetSt == nil {
		t.Fatal("target_expired run not found")
	}
	targetSt.Mu.Lock()
	queueLen := len(targetSt.ControlQueue)
	targetSt.Mu.Unlock()
	if queueLen == 0 {
		t.Error("expected the reply to be queued as fire-and-forget")
	}
}

func TestBrokerSessionsEndpoint(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	// No runs registered yet
	resp, err := http.Get(fmt.Sprintf("http://%s/sessions", b.Addr()))
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sessions status = %d", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	sessions, ok := body["sessions"].([]any)
	if !ok {
		t.Fatal("sessions response should have a sessions array")
	}
	if len(sessions) != 0 {
		t.Errorf("expected 0 sessions, got %d", len(sessions))
	}

	// Register a run and check again
	_, err = b.CreateRun("runner_1")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	b.UpdateSessionInfo("runner_1", &SessionInfo{
		Label:   "my-runner",
		Backend: "test-backend",
		Model:   "test-model",
		Dir:     "/tmp/test",
		Status:  "thinking",
	})

	resp2, err := http.Get(fmt.Sprintf("http://%s/sessions", b.Addr()))
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("sessions status = %d", resp2.StatusCode)
	}
	var body2 map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&body2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	sessions2, _ := body2["sessions"].([]any)
	if len(sessions2) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions2))
	}
	s0 := sessions2[0].(map[string]any)
	if s0["run_id"] != "runner_1" {
		t.Errorf("run_id = %q, want runner_1", s0["run_id"])
	}
	if s0["label"] != "my-runner" {
		t.Errorf("label = %q, want my-runner", s0["label"])
	}
	if s0["status"] != "thinking" {
		t.Errorf("status = %q, want thinking", s0["status"])
	}

	// Sessions should be accessible without auth
	resp3, err := http.Get(fmt.Sprintf("http://%s/sessions", b.Addr()))
	if err != nil {
		t.Fatalf("sessions no auth: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("sessions without auth expected 200, got %d", resp3.StatusCode)
	}
}

func TestBrokerUpdateSessionInfo(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("update_me")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	_ = token

	// Update info
	b.UpdateSessionInfo("update_me", &SessionInfo{
		Label:  "my-label",
		Status: "working",
	})

	st := b.GetRun("update_me")
	if st == nil {
		t.Fatal("run not found")
	}
	st.Mu.Lock()
	info := st.Info
	st.Mu.Unlock()
	if info == nil {
		t.Fatal("SessionInfo should not be nil after update")
	}
	if info.Label != "my-label" {
		t.Errorf("Label = %q, want my-label", info.Label)
	}
	if info.Status != "working" {
		t.Errorf("Status = %q, want working", info.Status)
	}
	if info.RunID != "update_me" {
		t.Errorf("RunID = %q, want update_me", info.RunID)
	}

	// Updating nonexistent run should not panic
	b.UpdateSessionInfo("nonexistent", &SessionInfo{Label: "ghost"})
}

func TestBrokerPruneExpiredAskEdges(t *testing.T) {
	// Test the pruning logic directly with an expired edge.
	// We use a very short expiration override by directly manipulating the map.
	b := New("")

	token, err := b.CreateRun("pruner")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	_ = token

	st := b.GetRun("pruner")
	if st == nil {
		t.Fatal("run not found")
	}

	// Add an ask edge that's already expired
	edge := &AskEdge{
		FromRunID: "pruner",
		ToRunID:   "other",
		MessageID: "stale-edge",
		CreatedAt: time.Now().Add(-DefaultAskTimeout - time.Second),
	}
	replyCh := make(chan AskReply, 1)
	st.Mu.Lock()
	st.PendingAsks["stale-edge"] = edge
	st.WaitingReply["stale-edge"] = replyCh
	st.Mu.Unlock()
	b.globalAskEdgesMu.Lock()
	b.globalAskEdges[edgeKey("pruner", "stale-edge")] = edge
	b.globalWaitReplies[edgeKey("pruner", "stale-edge")] = replyCh
	b.globalAskEdgesMu.Unlock()

	// Run pruning
	b.pruneExpiredAskEdges()

	// Pruning sends a timeout signal to the channel but does NOT delete
	// the registry entries — the wait_reply handler is responsible for
	// that after receiving.
	select {
	case sig := <-replyCh:
		if sig.FromRunID != "" {
			t.Errorf("timeout signal from_run_id = %q, want empty", sig.FromRunID)
		}
		var payload struct{ Timeout bool }
		if err := json.Unmarshal(sig.Payload, &payload); err != nil {
			t.Errorf("unmarshal timeout payload: %v", err)
		} else if !payload.Timeout {
			t.Errorf("expected timeout=true in signal payload")
		}
	case <-time.After(time.Second):
		t.Error("expected timeout signal on channel")
	}

	// Entries should still exist (pruning signals but doesn't delete).
	st.Mu.Lock()
	_, pendingOk := st.PendingAsks["stale-edge"]
	_, waitOk := st.WaitingReply["stale-edge"]
	st.Mu.Unlock()
	if !pendingOk {
		t.Error("stale edge should still exist after pruning (deletion is wait_reply's job)")
	}
	if !waitOk {
		t.Error("stale channel should still exist after pruning (deletion is wait_reply's job)")
	}
	// Global registry entries should also still exist.
	b.globalAskEdgesMu.RLock()
	_, gPendingOk := b.globalAskEdges[edgeKey("pruner", "stale-edge")]
	_, gWaitOk := b.globalWaitReplies[edgeKey("pruner", "stale-edge")]
	b.globalAskEdgesMu.RUnlock()
	if !gPendingOk {
		t.Error("global edge should still exist after pruning")
	}
	if !gWaitOk {
		t.Error("global wait channel should still exist after pruning")
	}

	// A fresh edge should NOT receive a timeout signal
	st.Mu.Lock()
	st.PendingAsks["fresh-edge"] = &AskEdge{
		FromRunID: "pruner",
		ToRunID:   "other",
		MessageID: "fresh-edge",
		CreatedAt: time.Now(),
	}
	st.WaitingReply["fresh-edge"] = make(chan AskReply, 1)
	st.Mu.Unlock()

	b.pruneExpiredAskEdges()

	st.Mu.Lock()
	_, freshOk := st.PendingAsks["fresh-edge"]
	st.Mu.Unlock()
	if !freshOk {
		t.Error("fresh ask edge should still exist after pruning")
	}
}

func TestBrokerAskReplyWaitTimeout(t *testing.T) {
	// Override the ask timeout to be very short so the test doesn't take 10 minutes.
	// We do this by calling pruneExpiredAskEdges after placing an edge with an
	// expired timestamp, avoiding the need to actually wait real wall time.
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("timeout_asker")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}

	addr := b.Addr()
	msgID := "will-timeout"

	// Place the ask edge directly (bypassing /send) with an expired timestamp.
	// Must register in both the per-run state and the global registry.
	st := b.GetRun("timeout_asker")
	if st == nil {
		t.Fatal("run not found")
	}
	edge := &AskEdge{
		FromRunID: "timeout_asker",
		ToRunID:   "somewhere",
		MessageID: msgID,
		CreatedAt: time.Now().Add(-DefaultAskTimeout - time.Second),
	}
	replyCh := make(chan AskReply, 1)

	st.Mu.Lock()
	st.PendingAsks[msgID] = edge
	st.WaitingReply[msgID] = replyCh
	st.Mu.Unlock()

	b.globalAskEdgesMu.Lock()
	b.globalAskEdges[edgeKey("timeout_asker", msgID)] = edge
	b.globalWaitReplies[edgeKey("timeout_asker", msgID)] = replyCh
	b.globalAskEdgesMu.Unlock()

	// Trigger pruning manually
	b.pruneExpiredAskEdges()

	// Now wait_reply should get the timeout signal from the channel
	result := waitReply(t, addr, senderToken, "timeout_asker", msgID)
	if st := result["status"]; st != http.StatusGatewayTimeout {
		t.Errorf("expected 504 timeout, got %d", st)
	}
	if result["timeout"] != true {
		t.Errorf("expected timeout=true in response")
	}
}

func TestBrokerSendAuthStillRequiredForAskReply(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("auth_test_1")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	_, err = b.CreateRun("auth_test_2")
	if err != nil {
		t.Fatalf("create run 2: %v", err)
	}

	// Attempt ask with wrong token
	payload := `{"id":"ask-noauth","from":"auth_test_1","from_run_id":"auth_test_1","message":"test","expects_reply":true}`
	body := bytes.NewReader([]byte(fmt.Sprintf(`{
		"run_id": "auth_test_1",
		"token": "wrong",
		"from_run_id": "auth_test_1",
		"to_run_id": "auth_test_2",
		"type": "agent_message",
		"payload": %s
	}`, payload)))
	resp, err := http.Post(fmt.Sprintf("http://%s/send", b.Addr()), "application/json", body)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong token, got %d", resp.StatusCode)
	}

	// wait_reply with wrong token
	waitBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"auth_test_1","token":"%s","waiting_for":"ask-noauth"}`, token)))
	resp2, err := http.Post(fmt.Sprintf("http://%s/wait_reply", b.Addr()), "application/json", waitBody)
	if err != nil {
		t.Fatalf("wait_reply: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for wait_reply with no ask, got %d", resp2.StatusCode)
	}

	// cancel_message with wrong token
	cancelBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"auth_test_1","token":"wrong","cancel_message_id":"ask-noauth"}`)))
	resp3, err := http.Post(fmt.Sprintf("http://%s/cancel_message", b.Addr()), "application/json", cancelBody)
	if err != nil {
		t.Fatalf("cancel_message: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong token on cancel, got %d", resp3.StatusCode)
	}
}
