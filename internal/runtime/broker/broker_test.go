package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
