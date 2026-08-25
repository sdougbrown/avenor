package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
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

// httpRegister registers a run over the HTTP /register endpoint and returns
// its token, exercising the real auth-credential path used by the endpoints.
func httpRegister(t *testing.T, addr, runID string) string {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("http://%s/register", addr),
		"application/json", bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":%q}`, runID))))
	if err != nil {
		t.Fatalf("register %s: %v", runID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register %s: %d", runID, resp.StatusCode)
	}
	var r map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode register %s: %v", runID, err)
	}
	if r["token"] == "" {
		t.Fatalf("register %s: empty token in response", runID)
	}
	return r["token"]
}

func TestBrokerRegisterBadJSON(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	resp, err := http.Post(fmt.Sprintf("http://%s/register", b.Addr()),
		"application/json", bytes.NewReader([]byte(`{not valid json`)))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad JSON register expected 400, got %d", resp.StatusCode)
	}
}

func TestBrokerRegisterEmptyRunID(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	resp, err := http.Post(fmt.Sprintf("http://%s/register", b.Addr()),
		"application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty run_id register expected 400, got %d", resp.StatusCode)
	}
}

func TestBrokerPushControlHTTP(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	addr := b.Addr()
	token := httpRegister(t, addr, "pc_http")

	body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"pc_http","token":%q,"id":"pc_1","type":"continue"}`, token)))
	resp, err := http.Post(fmt.Sprintf("http://%s/push-control", addr), "application/json", body)
	if err != nil {
		t.Fatalf("push-control: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("push-control status = %d", resp.StatusCode)
	}

	// Confirm the message reached the control queue over the HTTP poll path.
	pollBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"pc_http","token":%q}`, token)))
	presp, err := http.Post(fmt.Sprintf("http://%s/poll-control", addr), "application/json", pollBody)
	if err != nil {
		t.Fatalf("poll-control: %v", err)
	}
	defer presp.Body.Close()
	if presp.StatusCode != http.StatusOK {
		t.Fatalf("poll-control status = %d", presp.StatusCode)
	}
	var msgs []ControlMessage
	if err := json.NewDecoder(presp.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode poll: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != "pc_1" || msgs[0].Type != "continue" {
		t.Fatalf("expected queued pc_1/continue, got %+v", msgs)
	}
}

func TestBrokerReplyHTTP(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	addr := b.Addr()
	token := httpRegister(t, addr, "reply_src")

	body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"reply_src","token":%q,"to":"reply_dst","payload":%s}`, token, `{"text":"hi"}`)))
	resp, err := http.Post(fmt.Sprintf("http://%s/reply", addr), "application/json", body)
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reply status = %d", resp.StatusCode)
	}

	st := b.GetRun("reply_src")
	if st == nil {
		t.Fatal("run not found")
	}
	st.Mu.Lock()
	n := len(st.Replies)
	st.Mu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 reply ingested, got %d", n)
	}
}

func TestBrokerPermissionRequestHTTP(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	addr := b.Addr()
	token := httpRegister(t, addr, "perm_req_run")

	body := bytes.NewReader([]byte(fmt.Sprintf(
		`{"run_id":"perm_req_run","token":%q,"request_id":"pr1","tool_name":"bash","description":"run cmd","input_preview":"ls"}`, token)))
	resp, err := http.Post(fmt.Sprintf("http://%s/permission_request", addr), "application/json", body)
	if err != nil {
		t.Fatalf("permission_request: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("permission_request status = %d", resp.StatusCode)
	}

	st := b.GetRun("perm_req_run")
	if st == nil {
		t.Fatal("run not found")
	}
	st.Mu.Lock()
	pr := st.PermissionRequests["pr1"]
	st.Mu.Unlock()
	if pr == nil || pr.ToolName != "bash" || pr.Desc != "run cmd" {
		t.Errorf("permission request not stored correctly: %+v", pr)
	}
}

func TestBrokerPermissionHTTP(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	addr := b.Addr()
	token := httpRegister(t, addr, "perm_dec_run")

	body := bytes.NewReader([]byte(fmt.Sprintf(
		`{"run_id":"perm_dec_run","token":%q,"request_id":"pd1","behavior":"allow"}`, token)))
	resp, err := http.Post(fmt.Sprintf("http://%s/permission", addr), "application/json", body)
	if err != nil {
		t.Fatalf("permission: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("permission status = %d", resp.StatusCode)
	}

	st := b.GetRun("perm_dec_run")
	if st == nil {
		t.Fatal("run not found")
	}
	st.Mu.Lock()
	v := st.PermissionDecisions["pd1"]
	st.Mu.Unlock()
	if v != "allow" {
		t.Errorf("permission decision = %q, want allow", v)
	}
}

func TestBrokerSendHTTPToNonexistentTarget(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	addr := b.Addr()
	token := httpRegister(t, addr, "send_src")

	send := func(payload string) int {
		body := bytes.NewReader([]byte(fmt.Sprintf(
			`{"run_id":"send_src","token":%q,"from_run_id":"send_src","to_run_id":"ghost","type":"agent_message","payload":%s}`, token, payload)))
		resp, err := http.Post(fmt.Sprintf("http://%s/send", addr), "application/json", body)
		if err != nil {
			t.Fatalf("send: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}

	// Fire-and-forget to a nonexistent target -> 404.
	if st := send(`{"message":"fire"}`); st != http.StatusNotFound {
		t.Errorf("fire-and-forget to nonexistent target expected 404, got %d", st)
	}
	// Ask (expects_reply) to a nonexistent target -> 404.
	if st := send(`{"id":"ask-ghost","message":"ask","expects_reply":true}`); st != http.StatusNotFound {
		t.Errorf("ask to nonexistent target expected 404, got %d", st)
	}
}

func TestPollAgentMessagesStopsOnCancel(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	if _, err := b.CreateRun("poll_cancel"); err != nil {
		t.Fatalf("create run: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		b.PollAgentMessages(ctx, "poll_cancel", func(string) {})
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("PollAgentMessages did not stop after context cancellation")
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

// TestBrokerMutualAskGuardConcurrent verifies the mutual-ask guard is atomic
// under concurrent opposite-direction asks: exactly one of A→B / B→A may be
// accepted, the other must be rejected, and no ask edge may be left dangling.
func TestBrokerMutualAskGuardConcurrent(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	aToken, err := b.CreateRun("ca")
	if err != nil {
		t.Fatalf("create ca: %v", err)
	}
	bToken, err := b.CreateRun("cb")
	if err != nil {
		t.Fatalf("create cb: %v", err)
	}
	addr := b.Addr()

	ask := func(from, token, to, msgID string) int {
		payload := fmt.Sprintf(`{"id":%q,"from":%q,"from_run_id":%q,"to_run_id":%q,"message":"x","expects_reply":true}`,
			msgID, from, from, to)
		body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":%q,"token":%q,"from_run_id":%q,"to_run_id":%q,"type":"agent_message","payload":%s}`,
			from, token, from, to, payload)))
		resp, err := http.Post(fmt.Sprintf("http://%s/send", addr), "application/json", body)
		if err != nil {
			t.Errorf("send from %s: %v", from, err)
			return 0
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}

	statuses := make([]int, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); statuses[0] = ask("ca", aToken, "cb", "c-msg-a") }()
	go func() { defer wg.Done(); statuses[1] = ask("cb", bToken, "ca", "c-msg-b") }()
	wg.Wait()

	accepted, rejected := 0, 0
	for _, s := range statuses {
		switch s {
		case http.StatusOK:
			accepted++
		case http.StatusConflict:
			rejected++
		}
	}
	if accepted != 1 || rejected != 1 {
		t.Errorf("expected exactly one accepted (200) and one rejected (409), got %v", statuses)
	}

	// Exactly one edge should remain in the global registry.
	b.globalAskEdgesMu.RLock()
	n := len(b.globalAskEdges)
	b.globalAskEdgesMu.RUnlock()
	if n != 1 {
		t.Errorf("expected exactly 1 surviving ask edge, got %d", n)
	}
}

// TestBrokerAskReusesMessageIDConflict verifies a sender cannot register two
// overlapping pending asks with the same message ID (the second is refused).
func TestBrokerAskReusesMessageIDConflict(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	at, err := b.CreateRun("dup_a")
	if err != nil {
		t.Fatalf("create dup_a: %v", err)
	}
	if _, err := b.CreateRun("dup_b"); err != nil {
		t.Fatalf("create dup_b: %v", err)
	}
	addr := b.Addr()

	if r := sendAsk(t, addr, at, "dup_a", "dup_b", "dup-id", "first"); r["status"] != http.StatusOK {
		t.Fatalf("first ask should succeed, got %d", r["status"])
	}
	// Same sender, same message ID, still pending -> refused.
	r2 := sendAsk(t, addr, at, "dup_a", "dup_b", "dup-id", "second")
	if st := r2["status"]; st != http.StatusConflict {
		t.Errorf("duplicate pending ask expected 409, got %d", st)
	}
}

// TestBrokerDeleteRunCleansAskEdges verifies DeleteRun removes the run's
// pending ask edges from the global registries so they don't leak.
func TestBrokerDeleteRunCleansAskEdges(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	aToken, err := b.CreateRun("del_a")
	if err != nil {
		t.Fatalf("create del_a: %v", err)
	}
	if _, err := b.CreateRun("del_b"); err != nil {
		t.Fatalf("create del_b: %v", err)
	}
	addr := b.Addr()

	if r := sendAsk(t, addr, aToken, "del_a", "del_b", "del-ask-1", "hi"); r["status"] != http.StatusOK {
		t.Fatalf("ask should succeed, got %d", r["status"])
	}

	key := edgeKey("del_a", "del-ask-1")
	b.globalAskEdgesMu.RLock()
	_, existed := b.globalAskEdges[key]
	b.globalAskEdgesMu.RUnlock()
	if !existed {
		t.Fatal("ask edge should exist before DeleteRun")
	}

	b.DeleteRun("del_a")

	b.globalAskEdgesMu.RLock()
	_, gEdge := b.globalAskEdges[key]
	_, gCh := b.globalReplyChannels[key]
	b.globalAskEdgesMu.RUnlock()
	if gEdge || gCh {
		t.Error("DeleteRun should remove the run's ask edges from global registries")
	}
	if b.GetRun("del_a") != nil {
		t.Error("run should no longer exist after DeleteRun")
	}
}

// TestBrokerResetCleansAskEdges verifies Reset drops all run state including
// the global ask-edge registries.
func TestBrokerResetCleansAskEdges(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	aToken, err := b.CreateRun("res_a")
	if err != nil {
		t.Fatalf("create res_a: %v", err)
	}
	if _, err := b.CreateRun("res_b"); err != nil {
		t.Fatalf("create res_b: %v", err)
	}
	addr := b.Addr()

	if r := sendAsk(t, addr, aToken, "res_a", "res_b", "res-ask-1", "hi"); r["status"] != http.StatusOK {
		t.Fatalf("ask should succeed, got %d", r["status"])
	}

	b.Reset()

	b.globalAskEdgesMu.RLock()
	n := len(b.globalAskEdges)
	b.globalAskEdgesMu.RUnlock()
	if n != 0 {
		t.Errorf("Reset should clear global ask edges, got %d remaining", n)
	}
	if b.RunCount() != 0 {
		t.Errorf("Reset should clear runs, got %d", b.RunCount())
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
	targetToken, err := b.CreateRun("target_expired")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	addr := b.Addr()
	msgID := "expired-ask"

	// Send an ask from sender -> target
	result := sendAsk(t, addr, senderToken, "sender_expired", "target_expired", msgID, "will this expire?")
	if st := result["status"]; st != http.StatusOK {
		t.Fatalf("sendAsk status = %d", st)
	}

	// Manually remove the ask edge from both per-run and global registries
	// to simulate expiry. Use the namespaced key.
	st := b.GetRun("sender_expired")
	if st == nil {
		t.Fatal("sender_expired run not found")
	}
	key := edgeKey("sender_expired", msgID)
	b.globalAskEdgesMu.Lock()
	delete(b.globalAskEdges, key)
	delete(b.globalReplyChannels, key)
	b.globalAskEdgesMu.Unlock()
	st.Mu.Lock()
	delete(st.PendingAsks, msgID)
	delete(st.WaitingReply, msgID)
	st.Mu.Unlock()

	// Send a reply from target -> sender (correct direction).
	// Since the edge is gone, this should fall back to fire-and-forget.
	replyResult := sendReply(t, addr, targetToken, "target_expired", "sender_expired", msgID, "late reply")
	if st := replyResult["status"]; st != http.StatusOK {
		t.Fatalf("sendReply after expiry status = %d", st)
	}
	if replyResult["note"] == nil {
		t.Error("expected 'note' about fire-and-forget fallback in reply response")
	}

	// Verify the reply was delivered as a fire-and-forget message to the
	// original asker (sender_expired).
	senderSt := b.GetRun("sender_expired")
	if senderSt == nil {
		t.Fatal("sender_expired run not found")
	}
	senderSt.Mu.Lock()
	queueLen := len(senderSt.ControlQueue)
	senderSt.Mu.Unlock()
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

	// Register a run so we have auth credentials.
	token, err := b.CreateRun("sessions_user")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	// No sessions metadata set yet
	resp, err := http.Get(fmt.Sprintf("http://%s/sessions?run_id=sessions_user&token=%s", b.Addr(), token))
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
	// Our own run is registered, so we have 1 session.
	if len(sessions) != 1 {
		t.Errorf("expected 1 session (ourselves), got %d", len(sessions))
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

	resp2, err := http.Get(fmt.Sprintf("http://%s/sessions?run_id=sessions_user&token=%s", b.Addr(), token))
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
	// We have sessions_user + runner_1 = 2 sessions.
	if len(sessions2) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions2))
	}
	// Find runner_1 in the list (map iteration order is non-deterministic).
	var found bool
	for _, s := range sessions2 {
		entry, ok := s.(map[string]any)
		if !ok {
			continue
		}
		if entry["run_id"] == "runner_1" {
			found = true
			if entry["label"] != "my-runner" {
				t.Errorf("label = %q, want my-runner", entry["label"])
			}
			if entry["status"] != "thinking" {
				t.Errorf("status = %q, want thinking", entry["status"])
			}
			break
		}
	}
	if !found {
		t.Error("runner_1 not found in sessions list")
	}

	// Sessions should reject without auth
	resp3, err := http.Get(fmt.Sprintf("http://%s/sessions", b.Addr()))
	if err != nil {
		t.Fatalf("sessions no auth: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusUnauthorized {
		t.Errorf("sessions without auth expected 401, got %d", resp3.StatusCode)
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
	b.globalReplyChannels[edgeKey("pruner", "stale-edge")] = replyCh
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

	// Entries should be removed after pruning (leak fix: no /wait_reply needed).
	st.Mu.Lock()
	_, pendingOk := st.PendingAsks["stale-edge"]
	_, waitOk := st.WaitingReply["stale-edge"]
	st.Mu.Unlock()
	if pendingOk {
		t.Error("stale edge should be removed from run state after pruning")
	}
	if waitOk {
		t.Error("stale channel should be removed from run state after pruning")
	}
	// Global registry entries should also be removed.
	b.globalAskEdgesMu.RLock()
	_, gPendingOk := b.globalAskEdges[edgeKey("pruner", "stale-edge")]
	_, gWaitOk := b.globalReplyChannels[edgeKey("pruner", "stale-edge")]
	b.globalAskEdgesMu.RUnlock()
	if gPendingOk {
		t.Error("global edge should be removed after pruning")
	}
	if gWaitOk {
		t.Error("global wait channel should be removed after pruning")
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
	b.globalReplyChannels[edgeKey("timeout_asker", msgID)] = replyCh
	b.globalAskEdgesMu.Unlock()

	// Start the waiter in-flight so it looks up the reply channel before
	// pruning, then trigger pruning to deliver the timeout signal to the
	// blocked request. The /wait_reply handler captures the channel pointer
	// at request start, so it still receives the signal even though pruning
	// removes the registry entries.
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"timeout_asker","token":%q,"waiting_for":%q}`, senderToken, msgID)))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/wait_reply", body)
	if err != nil {
		t.Fatalf("wait_reply new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resultCh := make(chan map[string]any, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		defer resp.Body.Close()
		var result map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			errCh <- err
			return
		}
		result["status"] = resp.StatusCode
		resultCh <- result
	}()
	// Allow the wait_reply handler to register and block on the channel.
	time.Sleep(100 * time.Millisecond)
	b.pruneExpiredAskEdges()

	var result map[string]any
	select {
	case result = <-resultCh:
	case err := <-errCh:
		t.Fatalf("wait_reply: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("wait_reply did not return after pruning")
	}
	if st := result["status"]; st != http.StatusGatewayTimeout {
		t.Errorf("expected 504 timeout, got %d", st)
	}
	if result["timeout"] != true {
		t.Errorf("expected timeout=true in response")
	}

	// The expired edge should have been cleaned up from the global registry
	// and the run state by pruning (leak fix).
	b.globalAskEdgesMu.RLock()
	_, gOk := b.globalAskEdges[edgeKey("timeout_asker", msgID)]
	b.globalAskEdgesMu.RUnlock()
	if gOk {
		t.Error("expired ask edge should be removed from global registry after prune")
	}
	st.Mu.Lock()
	_, pendingOk := st.PendingAsks[msgID]
	_, waitOk := st.WaitingReply[msgID]
	st.Mu.Unlock()
	if pendingOk || waitOk {
		t.Error("expired ask edge should be removed from run state after prune")
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

	// wait_reply with nonexistent ask should return 404
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

func TestBrokerReplyByNonTargetReturns403(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	askerToken, err := b.CreateRun("asker")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	targetToken, err := b.CreateRun("target")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	thirdToken, err := b.CreateRun("third")
	if err != nil {
		t.Fatalf("create third: %v", err)
	}

	addr := b.Addr()
	msgID := "ask-for-403"

	// Asker sends an ask to target
	result := sendAsk(t, addr, askerToken, "asker", "target", msgID, "who can reply?")
	if st := result["status"]; st != http.StatusOK {
		t.Fatalf("sendAsk status = %d", st)
	}

	// Third party (not the target) tries to reply — should get 403
	replyResult := sendReply(t, addr, thirdToken, "third", "asker", msgID, "i should not be allowed")
	if st := replyResult["status"]; st != http.StatusForbidden {
		t.Errorf("expected 403, got %d", st)
	}

	// The legitimate target can still reply
	replyResult2 := sendReply(t, addr, targetToken, "target", "asker", msgID, "i am the target")
	if st := replyResult2["status"]; st != http.StatusOK {
		t.Errorf("legitimate reply expected 200, got %d", st)
	}
}

func TestBrokerSessionsNilInfo(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("no_info_run")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Query sessions without calling UpdateSessionInfo — st.Info should be nil.
	resp, err := http.Get(fmt.Sprintf("http://%s/sessions?run_id=no_info_run&token=%s", b.Addr(), token))
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
	sessions, _ := body["sessions"].([]any)
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
	s0 := sessions[0].(map[string]any)
	if s0["run_id"] != "no_info_run" {
		t.Errorf("run_id = %q, want no_info_run", s0["run_id"])
	}
	// Without UpdateSessionInfo, label/status should be empty
	if label, ok := s0["label"]; ok && label != "" {
		t.Errorf("expected empty label, got %q", label)
	}
	if status, ok := s0["status"]; ok && status != "" {
		t.Errorf("expected empty status, got %q", status)
	}
	// last_seen should be present
	lastSeen, ok := s0["last_seen"]
	if !ok {
		t.Error("last_seen should be present")
	}
	_ = lastSeen
}

func TestBrokerCancelSignalViaWaitReply(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("canceller_wait")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	_, err = b.CreateRun("victim")
	if err != nil {
		t.Fatalf("create victim: %v", err)
	}

	addr := b.Addr()
	msgID := "cancel-via-wait"

	// Send an ask
	result := sendAsk(t, addr, senderToken, "canceller_wait", "victim", msgID, "about to be cancelled")
	if st := result["status"]; st != http.StatusOK {
		t.Fatalf("sendAsk status = %d", st)
	}

	// Start waiting for the reply in a goroutine that will be cancelled.
	type waitResult struct {
		body map[string]any
		err  error
	}
	done := make(chan waitResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"canceller_wait","token":%q,"waiting_for":%q}`, senderToken, msgID)))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			fmt.Sprintf("http://%s/wait_reply", addr), body)
		if err != nil {
			done <- waitResult{err: err}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- waitResult{err: err}
			return
		}
		defer resp.Body.Close()
		result := map[string]any{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			done <- waitResult{err: err}
			return
		}
		result["status"] = resp.StatusCode
		done <- waitResult{body: result}
	}()

	// Give the goroutine time to start the long-poll
	time.Sleep(50 * time.Millisecond)

	// Cancel the message — the wait_reply should receive the cancel signal
	if st := b.GetRun("canceller_wait"); st == nil {
		t.Fatal("run not found")
	}
	cancelBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"canceller_wait","token":%q,"cancel_message_id":%q}`, senderToken, msgID)))
	resp, err := http.Post(fmt.Sprintf("http://%s/cancel_message", addr), "application/json", cancelBody)
	if err != nil {
		t.Fatalf("cancel_message: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel_message status = %d", resp.StatusCode)
	}

	// Read the wait_reply result
	select {
	case wr := <-done:
		if wr.err != nil {
			t.Fatalf("wait_reply error: %v", wr.err)
		}
		if wr.body["cancelled"] != true {
			t.Errorf("expected cancelled=true in wait_reply response, got %v", wr.body)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for wait_reply to return after cancel")
	}
}

func TestBrokerReplyDropOnFullChannel(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("drop_sender")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("drop_target")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	addr := b.Addr()
	msgID := "drop-test"

	// Send an ask
	result := sendAsk(t, addr, senderToken, "drop_sender", "drop_target", msgID, "test drop")
	if st := result["status"]; st != http.StatusOK {
		t.Fatalf("sendAsk status = %d", st)
	}

	// Fill the reply channel by sending a cancel signal first.
	cancelBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"drop_sender","token":%q,"cancel_message_id":%q}`, senderToken, msgID)))
	resp, err := http.Post(fmt.Sprintf("http://%s/cancel_message", addr), "application/json", cancelBody)
	if err != nil {
		t.Fatalf("cancel_message: %v", err)
	}
	resp.Body.Close()

	// Now try to send a reply — the channel is full so the reply should
	// hit the default case and be silently dropped.
	replyResult := sendReply(t, addr, targetToken, "drop_target", "drop_sender", msgID, "late reply after cancel")
	if st := replyResult["status"]; st != http.StatusOK {
		t.Fatalf("sendReply status = %d", st)
	}
}

// TestBrokerCancelDropOnFullChannel verifies the mirror case of the reply
// drop: when a cancel arrives but the reply channel is already full (a reply
// beat the cancel), the cancel signal is safely discarded and nothing panics
// or leaks.
func TestBrokerCancelDropOnFullChannel(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("cdrop_sender")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("cdrop_target")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	addr := b.Addr()
	msgID := "cdrop-test"

	if r := sendAsk(t, addr, senderToken, "cdrop_sender", "cdrop_target", msgID, "who replies first?"); r["status"] != http.StatusOK {
		t.Fatalf("sendAsk status = %d", r["status"])
	}

	// Deliver a real reply first — it fills the buffered channel and the
	// registries are kept alive (wait_reply cleans up after receiving).
	if r := sendReply(t, addr, targetToken, "cdrop_target", "cdrop_sender", msgID, "reply beats the cancel"); r["status"] != http.StatusOK {
		t.Fatalf("sendReply status = %d", r["status"])
	}

	// Now cancel: the channel is full, so the cancel signal hits the default
	// case and is dropped. The cancel still succeeds and cleans up the edge.
	cancelBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"cdrop_sender","token":%q,"cancel_message_id":%q}`, senderToken, msgID)))
	resp, err := http.Post(fmt.Sprintf("http://%s/cancel_message", addr), "application/json", cancelBody)
	if err != nil {
		t.Fatalf("cancel_message: %v", err)
	}
	defer resp.Body.Close()
	var cr map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		t.Fatalf("decode cancel: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel_message status = %d", resp.StatusCode)
	}
	if cr["cancelled"] != true {
		t.Errorf("expected cancelled=true, got %v", cr)
	}

	// The cancel also cleans up the registries even though the signal was
	// dropped, so nothing leaks.
	b.globalAskEdgesMu.RLock()
	_, gOk := b.globalAskEdges[edgeKey("cdrop_sender", msgID)]
	_, gCh := b.globalReplyChannels[edgeKey("cdrop_sender", msgID)]
	b.globalAskEdgesMu.RUnlock()
	if gOk || gCh {
		t.Error("ask edge should be removed after cancel even when the signal is dropped")
	}
}

func TestBrokerWaitReplyDisconnectCleanup(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("disco_sender")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	msgID := "disco-test"

	// Directly register an ask edge (no reply expected).
	key := edgeKey("disco_sender", msgID)
	edge := &AskEdge{
		FromRunID: "disco_sender",
		ToRunID:   "somewhere",
		MessageID: msgID,
		CreatedAt: time.Now(),
	}
	replyCh := make(chan AskReply, 1)

	st := b.GetRun("disco_sender")
	if st == nil {
		t.Fatal("run not found")
	}
	st.Mu.Lock()
	st.PendingAsks[msgID] = edge
	st.WaitingReply[msgID] = replyCh
	st.Mu.Unlock()
	b.globalAskEdgesMu.Lock()
	b.globalAskEdges[key] = edge
	b.globalReplyChannels[key] = replyCh
	b.globalAskEdgesMu.Unlock()

	// Start wait_reply, then cancel the context mid-wait to trigger cleanup.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"disco_sender","token":%q,"waiting_for":%q}`, senderToken, msgID)))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://%s/wait_reply", b.Addr()), body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}

	// Wait for the handler to process context cancellation.
	time.Sleep(10 * time.Millisecond)

	// Verify the edge was cleaned up.
	b.globalAskEdgesMu.RLock()
	_, edgeExists := b.globalAskEdges[key]
	_, chExists := b.globalReplyChannels[key]
	b.globalAskEdgesMu.RUnlock()
	if edgeExists || chExists {
		t.Error("expected edge and channel to be cleaned up on context cancellation")
	}

	st.Mu.Lock()
	_, pendingExists := st.PendingAsks[msgID]
	_, waitExists := st.WaitingReply[msgID]
	st.Mu.Unlock()
	if pendingExists || waitExists {
		t.Error("expected per-run state to be cleaned up on context cancellation")
	}
}

func TestBrokerDrainAgentMessages(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("receiver")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	_ = token
	senderToken, err := b.CreateRun("sender")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}

	// Send an ask targeting the receiver.
	sendAsk(t, b.Addr(), senderToken, "sender", "receiver", "drain-ask", "hello from subagent")

	// Also queue a non-agent control message to verify it is not drained.
	b.PushControl("receiver", ControlMessage{ID: "ctrl-keep", Type: "continue", RunID: "receiver"})

	// Drain agent messages for the receiver.
	msgs, err := b.DrainAgentMessages("receiver")
	if err != nil {
		t.Fatalf("DrainAgentMessages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 agent message, got %d", len(msgs))
	}
	if msgs[0].FromRunID != "sender" {
		t.Errorf("FromRunID = %q, want sender", msgs[0].FromRunID)
	}
	if msgs[0].Type != "agent_message" {
		t.Errorf("Type = %q, want agent_message", msgs[0].Type)
	}

	// The non-agent control message must remain.
	st := b.GetRun("receiver")
	st.Mu.Lock()
	remaining := len(st.ControlQueue)
	st.Mu.Unlock()
	if remaining != 1 {
		t.Errorf("expected 1 non-agent control message to remain, got %d", remaining)
	}

	// Draining again yields nothing.
	msgs2, err := b.DrainAgentMessages("receiver")
	if err != nil {
		t.Fatalf("drain again: %v", err)
	}
	if len(msgs2) != 0 {
		t.Errorf("expected 0 after second drain, got %d", len(msgs2))
	}
}

func TestBrokerSessionInfoContextFields(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("ctx_run")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	b.UpdateSessionInfo("ctx_run", &SessionInfo{
		Backend:       "acp",
		Model:         "test-model",
		Status:        "idle",
		ContextPct:    42,
		ContextTokens: 82000,
		ContextWindow: 200000,
	})

	// The stored metadata reflects the new fields.
	st := b.GetRun("ctx_run")
	if st == nil {
		t.Fatal("run not found")
	}
	st.Mu.Lock()
	info := st.Info
	st.Mu.Unlock()
	if info == nil {
		t.Fatal("SessionInfo should not be nil after update")
	}
	if info.ContextPct != 42 {
		t.Errorf("ContextPct = %d, want 42", info.ContextPct)
	}
	if info.ContextTokens != 82000 {
		t.Errorf("ContextTokens = %d, want 82000", info.ContextTokens)
	}
	if info.ContextWindow != 200000 {
		t.Errorf("ContextWindow = %d, want 200000", info.ContextWindow)
	}

	// /sessions must surface the context fields (with auth).
	resp, err := http.Get(fmt.Sprintf("http://%s/sessions?run_id=ctx_run&token=%s", b.Addr(), token))
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
	sessions, _ := body["sessions"].([]any)
	var found bool
	for _, s := range sessions {
		entry, ok := s.(map[string]any)
		if !ok {
			continue
		}
		if entry["run_id"] == "ctx_run" {
			found = true
			if pct, _ := entry["context_pct"].(float64); int(pct) != 42 {
				t.Errorf("context_pct = %v, want 42", entry["context_pct"])
			}
			if toks, _ := entry["context_tokens"].(float64); int(toks) != 82000 {
				t.Errorf("context_tokens = %v, want 82000", entry["context_tokens"])
			}
			if win, _ := entry["context_window"].(float64); int(win) != 200000 {
				t.Errorf("context_window = %v, want 200000", entry["context_window"])
			}
			break
		}
	}
	if !found {
		t.Error("ctx_run not found in sessions list")
	}
}
