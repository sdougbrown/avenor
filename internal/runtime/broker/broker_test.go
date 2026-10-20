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

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
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

	token, err := b.CreateRun("run_existing", "")
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
	if _, err := b.CreateRun("poll_cancel", ""); err != nil {
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

	token, err := b.CreateRun("run_poll", "")
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

	if _, err := b.CreateRun("run_to_delete", ""); err != nil {
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

	token, err := b.CreateRun("run_send", "")
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

	token, err := b.CreateRun("run_ingest_report", "")
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

	token, err := b.CreateRun("run_ingest_finish", "")
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
	senderToken, err := b.CreateRun("sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("target", "")
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
	if msgs[0].Sender != "sender" {
		t.Errorf("Sender = %q, want the registered name", msgs[0].Sender)
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

	token, err := b.CreateRun("sender", "")
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

	_, err := b.CreateRun("sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	_, err = b.CreateRun("target", "")
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

	senderToken, err := b.CreateRun("sender_auto", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	b.DeleteRun("target_auto")
	targetToken, err := b.CreateRun("target_auto", "")
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

// TestPollAgentMessagesKeepsNonAgentControl pins that a poll drains only agent
// messages: the control messages other consumers act on stay queued in order.
func TestPollAgentMessagesKeepsNonAgentControl(t *testing.T) {
	b := New("")
	status := &ControlMessage{Type: "request_status", Payload: json.RawMessage(`{"message":"status?"}`)}
	agent := &ControlMessage{Type: "agent_message", FromRunID: "peer", Payload: json.RawMessage(`{"message":"hello"}`)}
	shutdown := &ControlMessage{Type: "shutdown"}
	st := &RunState{ControlQueue: []*ControlMessage{status, agent, shutdown}}
	b.mu.Lock()
	b.runs["target"] = st
	b.mu.Unlock()

	var delivered []string
	b.pollAgentMessagesOnce("target", func(wrapped string) { delivered = append(delivered, wrapped) })

	if len(delivered) != 1 || !strings.Contains(delivered[0], "hello") {
		t.Fatalf("poll delivered %v, want only the agent message", delivered)
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	if len(st.ControlQueue) != 2 || st.ControlQueue[0] != status || st.ControlQueue[1] != shutdown {
		t.Fatalf("control queue = %v, want the non-agent messages in their original order", st.ControlQueue)
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
	// Use a short timeout so tests don't hang for the ask retention window on failure.
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
	senderToken, err := b.CreateRun("asker", "")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	replierToken, err := b.CreateRun("replier", "")
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
	if b.Store().Pending("asker", msgID) {
		t.Error("ask edge should have been removed after reply")
	}
}

func TestBrokerAskReplyFireAndForgetRemainsUnchanged(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("ff_sender", "")
	if err != nil {
		t.Fatalf("create ff_sender: %v", err)
	}
	targetToken, err := b.CreateRun("ff_target", "")
	if err != nil {
		t.Fatalf("create ff_target: %v", err)
	}

	// Fire-and-forget (no expects_reply, no reply_to)
	status := fireAndForgetSend(t, b.Addr(), senderToken, "ff_sender", "ff_target", "just a note")
	if status != http.StatusOK {
		t.Errorf("fire-and-forget status = %d, want 200", status)
	}

	// Verify no ask edge was created
	if pendingCount := b.Store().PendingCount("ff_sender"); pendingCount != 0 {
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

	senderToken, err := b.CreateRun("asker_no_id", "")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	_, err = b.CreateRun("target_no_id", "")
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

	aToken, err := b.CreateRun("agent_a", "")
	if err != nil {
		t.Fatalf("create agent_a: %v", err)
	}
	bToken, err := b.CreateRun("agent_b", "")
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

	aToken, err := b.CreateRun("ca", "")
	if err != nil {
		t.Fatalf("create ca: %v", err)
	}
	bToken, err := b.CreateRun("cb", "")
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

	// Exactly one edge should survive.
	if n := b.Store().ActiveCount(); n != 1 {
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

	at, err := b.CreateRun("dup_a", "")
	if err != nil {
		t.Fatalf("create dup_a: %v", err)
	}
	if _, err := b.CreateRun("dup_b", ""); err != nil {
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

	aToken, err := b.CreateRun("del_a", "")
	if err != nil {
		t.Fatalf("create del_a: %v", err)
	}
	if _, err := b.CreateRun("del_b", ""); err != nil {
		t.Fatalf("create del_b: %v", err)
	}
	addr := b.Addr()

	if r := sendAsk(t, addr, aToken, "del_a", "del_b", "del-ask-1", "hi"); r["status"] != http.StatusOK {
		t.Fatalf("ask should succeed, got %d", r["status"])
	}
	if !b.Store().Pending("del_a", "del-ask-1") {
		t.Fatal("ask edge should exist before DeleteRun")
	}

	b.DeleteRun("del_a")

	if b.Store().Pending("del_a", "del-ask-1") {
		t.Error("DeleteRun should remove the run's ask edges")
	}
	if b.GetRun("del_a") != nil {
		t.Error("run should no longer exist after DeleteRun")
	}
}

func TestBrokerResetCleansAskEdges(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	aToken, err := b.CreateRun("res_a", "")
	if err != nil {
		t.Fatalf("create res_a: %v", err)
	}
	if _, err := b.CreateRun("res_b", ""); err != nil {
		t.Fatalf("create res_b: %v", err)
	}
	addr := b.Addr()

	if r := sendAsk(t, addr, aToken, "res_a", "res_b", "res-ask-1", "hi"); r["status"] != http.StatusOK {
		t.Fatalf("ask should succeed, got %d", r["status"])
	}

	b.Reset()

	if n := b.Store().ActiveCount(); n != 0 {
		t.Errorf("Reset should clear ask edges, got %d remaining", n)
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

	senderToken, err := b.CreateRun("canceller", "")
	if err != nil {
		t.Fatalf("create canceller: %v", err)
	}
	targetToken, err := b.CreateRun("cancel_target", "")
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
	if !b.Store().Pending("canceller", msgID) {
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
	if b.Store().Pending("canceller", msgID) {
		t.Error("ask edge should have been removed after cancel")
	}
	// Withdrawal is the only terminal outcome that retracts the queued ask.
	target := b.GetRun("cancel_target")
	target.Mu.Lock()
	queued := len(target.ControlQueue)
	target.Mu.Unlock()
	if queued != 1 {
		t.Errorf("target queue = %d, want only the uncancelled ask", queued)
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

// TestBrokerRemovalTerminatesWaiters verifies that removing a participating
// run terminates an in-flight wait instead of leaving it until the deadline.
func TestBrokerRemovalTerminatesWaiters(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("removal_sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	if _, err := b.CreateRun("removal_target", ""); err != nil {
		t.Fatalf("create target: %v", err)
	}
	msgID := "removal-ask"
	if r := sendAsk(t, b.Addr(), senderToken, "removal_sender", "removal_target", msgID, "hi"); r["status"] != http.StatusOK {
		t.Fatalf("sendAsk status = %d", r["status"])
	}

	resultCh := make(chan map[string]any, 1)
	go func() {
		resultCh <- waitReply(t, b.Addr(), senderToken, "removal_sender", msgID)
	}()
	time.Sleep(50 * time.Millisecond)

	b.DeleteRun("removal_target")

	select {
	case result := <-resultCh:
		if result["status"] != http.StatusGone || result["lost"] != true {
			t.Fatalf("wait result = %+v, want 410 lost", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter outlived the removed run")
	}
}

// TestBrokerSessionsAdvertisesGeneration pins the generation the local router
// exposes to versioned clients.
func TestBrokerSessionsAdvertisesGeneration(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()
	token, err := b.CreateRun("gen_reader", "")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/sessions?run_id=gen_reader&token=%s", b.Addr(), token))
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	defer resp.Body.Close()
	var payload struct {
		Generation string `json:"generation"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload.Generation == "" || payload.Generation != b.Store().Generation() {
		t.Fatalf("generation = %q, want %q", payload.Generation, b.Store().Generation())
	}
}

// mintMsgID mints a versioned message ID with the given lifetime.
func mintMsgID(t *testing.T, ttl time.Duration) string {
	t.Helper()
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(ttl))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	return msgID
}

// askBody builds a /send request carrying an ask and an optional generation.
func askBody(token, fromRunID, toRunID, msgID, generation string) string {
	payload := fmt.Sprintf(`{"id":%q,"from":%q,"from_run_id":%q,"to_run_id":%q,"message":"ask","expects_reply":true}`,
		msgID, fromRunID, fromRunID, toRunID)
	body := fmt.Sprintf(`{"run_id":%q,"token":%q,"from_run_id":%q,"to_run_id":%q,"type":"agent_message","payload":%s`,
		fromRunID, token, fromRunID, toRunID, payload)
	if generation != "" {
		body += fmt.Sprintf(`,"generation":%q`, generation)
	}
	return body + "}"
}

// postBroker posts body to a broker endpoint and returns the status and body.
func postBroker(t *testing.T, addr, path, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("http://%s/%s", addr, path), "application/json", bytes.NewReader([]byte(body)))
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

// postBrokerTimeout posts body to a broker endpoint under a bounded context, so
// a long-poll that never completes fails the test instead of hanging it.
func postBrokerTimeout(t *testing.T, addr, path, body string, timeout time.Duration) (int, map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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

// TestBrokerReplyAnswersVersionedAsk pins the reply path for a versioned ask:
// the reply sender names no generation, so the broker must answer the edge
// admitted in the store's current generation and release the waiter.
func TestBrokerReplyAnswersVersionedAsk(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("versioned_asker", "")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	targetToken, err := b.CreateRun("versioned_replier", "")
	if err != nil {
		t.Fatalf("create replier: %v", err)
	}
	addr := b.Addr()
	gen := b.Store().Generation()
	msgID := mintMsgID(t, time.Minute)

	if status, body := postBroker(t, addr, "send", askBody(senderToken, "versioned_asker", "versioned_replier", msgID, gen)); status != http.StatusOK {
		t.Fatalf("send = (%d, %v)", status, body)
	}
	reply := sendReply(t, addr, targetToken, "versioned_replier", "versioned_asker", msgID, "the answer")
	if reply["status"] != http.StatusOK || reply["note"] != nil {
		t.Fatalf("reply = %+v, want the live edge answered", reply)
	}
	waitBody := fmt.Sprintf(`{"run_id":"versioned_asker","token":%q,"waiting_for":%q,"generation":%q}`, senderToken, msgID, gen)
	status, body := postBrokerTimeout(t, addr, "wait_reply", waitBody, 3*time.Second)
	if status != http.StatusOK || body["from_run_id"] != "versioned_replier" {
		t.Fatalf("wait_reply = (%d, %v)", status, body)
	}
	if b.Store().Pending("versioned_asker", msgID) {
		t.Fatal("ask edge survived the reply")
	}
}

// TestBrokerReplyAfterRotationFallsBackToDelivery pins the broker's
// fire-and-forget fallback: a rotation retires the edge, and the reply still
// reaches the asker as a plain delivery.
func TestBrokerReplyAfterRotationFallsBackToDelivery(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("rotated_asker", "")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	targetToken, err := b.CreateRun("rotated_replier", "")
	if err != nil {
		t.Fatalf("create replier: %v", err)
	}
	addr := b.Addr()
	msgID := mintMsgID(t, time.Minute)
	if status, body := postBroker(t, addr, "send", askBody(senderToken, "rotated_asker", "rotated_replier", msgID, b.Store().Generation())); status != http.StatusOK {
		t.Fatalf("send = (%d, %v)", status, body)
	}

	b.Store().Reset()

	reply := sendReply(t, addr, targetToken, "rotated_replier", "rotated_asker", msgID, "after the rotation")
	if reply["status"] != http.StatusOK || reply["note"] == nil {
		t.Fatalf("reply = %+v, want the fire-and-forget fallback", reply)
	}
	sender := b.GetRun("rotated_asker")
	sender.Mu.Lock()
	queued := len(sender.ControlQueue)
	sender.Mu.Unlock()
	if queued != 1 {
		t.Fatalf("asker queue = %d, want the reply delivered", queued)
	}
}

// TestBrokerVersionedIDRequiresGeneration pins that a versioned ID without a
// generation field is refused on every ask endpoint.
func TestBrokerVersionedIDRequiresGeneration(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("nogen_sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	if _, err := b.CreateRun("nogen_target", ""); err != nil {
		t.Fatalf("create target: %v", err)
	}
	addr := b.Addr()
	msgID := mintMsgID(t, time.Minute)

	if status, body := postBroker(t, addr, "send", askBody(senderToken, "nogen_sender", "nogen_target", msgID, "")); status != http.StatusConflict {
		t.Fatalf("send without generation = (%d, %v), want 409", status, body)
	}
	waitBody := fmt.Sprintf(`{"run_id":"nogen_sender","token":%q,"waiting_for":%q}`, senderToken, msgID)
	if status, body := postBroker(t, addr, "wait_reply", waitBody); status != http.StatusConflict {
		t.Fatalf("wait_reply without generation = (%d, %v), want 409", status, body)
	}
	cancelBody := fmt.Sprintf(`{"run_id":"nogen_sender","token":%q,"cancel_message_id":%q}`, senderToken, msgID)
	if status, body := postBroker(t, addr, "cancel_message", cancelBody); status != http.StatusConflict {
		t.Fatalf("cancel_message without generation = (%d, %v), want 409", status, body)
	}
	if n := b.Store().ActiveCount(); n != 0 {
		t.Fatalf("generation-less requests left %d live edges", n)
	}
}

// TestBrokerStaleGenerationRejected pins the 409 the local router returns for a
// stale generation on every ask endpoint.
func TestBrokerStaleGenerationRejected(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("stale_gen_sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	if _, err := b.CreateRun("stale_gen_target", ""); err != nil {
		t.Fatalf("create target: %v", err)
	}
	addr := b.Addr()
	msgID := mintMsgID(t, time.Minute)
	stale := "stale-generation"

	if status, body := postBroker(t, addr, "send", askBody(senderToken, "stale_gen_sender", "stale_gen_target", msgID, stale)); status != http.StatusConflict {
		t.Fatalf("send with stale generation = (%d, %v), want 409", status, body)
	}
	waitBody := fmt.Sprintf(`{"run_id":"stale_gen_sender","token":%q,"waiting_for":%q,"generation":%q}`, senderToken, msgID, stale)
	if status, body := postBroker(t, addr, "wait_reply", waitBody); status != http.StatusConflict {
		t.Fatalf("wait_reply with stale generation = (%d, %v), want 409", status, body)
	}
	cancelBody := fmt.Sprintf(`{"run_id":"stale_gen_sender","token":%q,"cancel_message_id":%q,"generation":%q}`, senderToken, msgID, stale)
	if status, body := postBroker(t, addr, "cancel_message", cancelBody); status != http.StatusConflict {
		t.Fatalf("cancel_message with stale generation = (%d, %v), want 409", status, body)
	}
	if n := b.Store().ActiveCount(); n != 0 {
		t.Fatalf("stale-generation requests left %d live edges", n)
	}
}

// TestBrokerWithdrawnVersionedIDRejectedOnResend pins the fence mapping: a
// versioned ID that was sent and then cancelled cannot be sent again.
func TestBrokerWithdrawnVersionedIDRejectedOnResend(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("fenced_sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	if _, err := b.CreateRun("fenced_target", ""); err != nil {
		t.Fatalf("create target: %v", err)
	}
	addr := b.Addr()
	gen := b.Store().Generation()
	msgID := mintMsgID(t, time.Minute)

	if status, body := postBroker(t, addr, "send", askBody(senderToken, "fenced_sender", "fenced_target", msgID, gen)); status != http.StatusOK {
		t.Fatalf("send = (%d, %v)", status, body)
	}
	cancelBody := fmt.Sprintf(`{"run_id":"fenced_sender","token":%q,"cancel_message_id":%q,"generation":%q}`, senderToken, msgID, gen)
	if status, body := postBroker(t, addr, "cancel_message", cancelBody); status != http.StatusOK || body["cancelled"] != true {
		t.Fatalf("cancel = (%d, %v)", status, body)
	}
	target := b.GetRun("fenced_target")
	target.Mu.Lock()
	queued := len(target.ControlQueue)
	target.Mu.Unlock()
	if queued != 0 {
		t.Fatalf("target queue = %d, want the withdrawn ask retracted", queued)
	}
	if status, body := postBroker(t, addr, "send", askBody(senderToken, "fenced_sender", "fenced_target", msgID, gen)); status != http.StatusConflict {
		t.Fatalf("re-send of a withdrawn id = (%d, %v), want 409", status, body)
	}
}

func TestBrokerWaitReplyForNonexistentAsk(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("nobody", "")
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

	senderToken, err := b.CreateRun("sender_expired", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("target_expired", "")
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

	// Expire the ask edge by pruning beyond its retention deadline.
	b.Store().Prune(time.Now().Add(askstore.DefaultAskTimeout + time.Second))
	if b.Store().Pending("sender_expired", msgID) {
		t.Fatal("ask edge should be gone after expiry")
	}

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
	token, err := b.CreateRun("sessions_user", "")
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
	_, err = b.CreateRun("runner_1", "")
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

	token, err := b.CreateRun("update_me", "")
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
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("pruner", "")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := b.CreateRun("pruner_target", ""); err != nil {
		t.Fatalf("create target: %v", err)
	}
	if r := sendAsk(t, b.Addr(), token, "pruner", "pruner_target", "stale-edge", "hi"); r["status"] != http.StatusOK {
		t.Fatalf("sendAsk status = %d", r["status"])
	}

	// Pruning past the retention deadline removes the edge; a fresh ask
	// survives a prune at the current time.
	b.Store().Prune(time.Now().Add(askstore.DefaultAskTimeout + time.Second))
	if b.Store().Pending("pruner", "stale-edge") {
		t.Error("stale edge should be removed after pruning")
	}
	if r := sendAsk(t, b.Addr(), token, "pruner", "pruner_target", "fresh-edge", "hi"); r["status"] != http.StatusOK {
		t.Fatalf("fresh sendAsk status = %d", r["status"])
	}
	b.Store().Prune(time.Now())
	if !b.Store().Pending("pruner", "fresh-edge") {
		t.Error("fresh ask edge should survive pruning")
	}
}

func TestBrokerAskReplyWaitTimeout(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("timeout_asker", "")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	if _, err := b.CreateRun("timeout_target", ""); err != nil {
		t.Fatalf("create target: %v", err)
	}

	addr := b.Addr()
	msgID := "will-timeout"
	if r := sendAsk(t, addr, senderToken, "timeout_asker", "timeout_target", msgID, "wait forever"); r["status"] != http.StatusOK {
		t.Fatalf("sendAsk status = %d", r["status"])
	}

	// Start the waiter in flight, then prune past the deadline so the prune
	// terminates the captured waiter.
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
	// Allow the wait_reply handler to capture the edge.
	time.Sleep(100 * time.Millisecond)
	b.Store().Prune(time.Now().Add(askstore.DefaultAskTimeout + time.Second))

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
	if b.Store().Pending("timeout_asker", msgID) {
		t.Error("expired ask edge should be removed after prune")
	}
}

func TestBrokerSendAuthStillRequiredForAskReply(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("auth_test_1", "")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	_, err = b.CreateRun("auth_test_2", "")
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

	askerToken, err := b.CreateRun("asker", "")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	targetToken, err := b.CreateRun("target", "")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	thirdToken, err := b.CreateRun("third", "")
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

func TestBrokerSessionsLaunchName(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	token, err := b.CreateRun("no_info_run", "")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	// A run registered without a label reports its run ID as its display name.
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
	if s0["label"] != "no_info_run" {
		t.Errorf("label = %v, want the run ID", s0["label"])
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

	senderToken, err := b.CreateRun("canceller_wait", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	_, err = b.CreateRun("victim", "")
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

	senderToken, err := b.CreateRun("drop_sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("drop_target", "")
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

	senderToken, err := b.CreateRun("cdrop_sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("cdrop_target", "")
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

	// The cancel also cleans up the edge even though the reply won the race,
	// so nothing leaks.
	if b.Store().Pending("cdrop_sender", msgID) {
		t.Error("ask edge should be removed after cancel")
	}
}

func TestBrokerWaitReplyDisconnectCleanup(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("disco_sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("disco_target", "")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	msgID := "disco-test"
	if r := sendAsk(t, b.Addr(), senderToken, "disco_sender", "disco_target", msgID, "hi"); r["status"] != http.StatusOK {
		t.Fatalf("sendAsk status = %d", r["status"])
	}

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
	deadline := time.Now().Add(2 * time.Second)
	for b.Store().Pending("disco_sender", msgID) {
		if time.Now().After(deadline) {
			t.Fatal("expected the ask edge to be cleaned up on context cancellation")
		}
		time.Sleep(time.Millisecond)
	}

	// The abandoned ask stays queued for the target: an unversioned ID records
	// no fence, exactly as the unextracted broker behaved.
	target := b.GetRun("disco_target")
	target.Mu.Lock()
	queued := len(target.ControlQueue)
	target.Mu.Unlock()
	if queued != 1 {
		t.Errorf("target queue = %d, want the abandoned ask retained", queued)
	}
	if ack, ok := b.Store().Fence(messaging.FenceKey{Sender: "disco_sender", MessageID: msgID}); ok {
		t.Errorf("legacy abandonment recorded a fence: %+v", ack)
	}

	// A later reply cannot answer the abandoned edge and is delivered to the
	// sender as fire-and-forget.
	if r := sendReply(t, b.Addr(), targetToken, "disco_target", "disco_sender", msgID, "late reply"); r["status"] != http.StatusOK || r["note"] == nil {
		t.Fatalf("late reply = %+v, want the fire-and-forget fallback", r)
	}
	sender := b.GetRun("disco_sender")
	sender.Mu.Lock()
	queued = len(sender.ControlQueue)
	sender.Mu.Unlock()
	if queued != 1 {
		t.Errorf("sender queue = %d, want the late reply delivered", queued)
	}

	// The unversioned ID carries no fence, so the same ID can be sent again.
	if r := sendAsk(t, b.Addr(), senderToken, "disco_sender", "disco_target", msgID, "again"); r["status"] != http.StatusOK {
		t.Fatalf("re-send of the abandoned legacy id = %d, want 200", r["status"])
	}
}

// TestBrokerVersionedWaitDisconnectCleanup pins the versioned counterpart: an
// abandoned versioned wait keeps the queued ask and leaves a terminal fence.
func TestBrokerVersionedWaitDisconnectCleanup(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("vdisco_sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	if _, err := b.CreateRun("vdisco_target", ""); err != nil {
		t.Fatalf("create target: %v", err)
	}
	addr := b.Addr()
	gen := b.Store().Generation()
	msgID := mintMsgID(t, time.Minute)
	if status, body := postBroker(t, addr, "send", askBody(senderToken, "vdisco_sender", "vdisco_target", msgID, gen)); status != http.StatusOK {
		t.Fatalf("send = (%d, %v)", status, body)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"vdisco_sender","token":%q,"waiting_for":%q,"generation":%q}`, senderToken, msgID, gen)))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/wait_reply", addr), body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}

	deadline := time.Now().Add(2 * time.Second)
	for b.Store().Pending("vdisco_sender", msgID) {
		if time.Now().After(deadline) {
			t.Fatal("expected the ask edge to be cleaned up on context cancellation")
		}
		time.Sleep(time.Millisecond)
	}

	target := b.GetRun("vdisco_target")
	target.Mu.Lock()
	queued := len(target.ControlQueue)
	target.Mu.Unlock()
	if queued != 1 {
		t.Fatalf("target queue = %d, want the abandoned ask retained", queued)
	}
	ack, ok := b.Store().Fence(messaging.FenceKey{Sender: "vdisco_sender", MessageID: msgID, Generation: gen})
	if !ok || !ack.Terminal {
		t.Fatalf("fence = (%+v, %v), want a terminal record for the abandoned ask", ack, ok)
	}
}

func TestBrokerDrainAgentMessages(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	if _, err := b.CreateRun("receiver", ""); err != nil {
		t.Fatalf("create run: %v", err)
	}
	senderToken, err := b.CreateRun("sender", "sender-display")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}

	askIDs := []string{mintMsgID(t, time.Minute), mintMsgID(t, time.Minute), mintMsgID(t, time.Minute)}
	for _, askID := range askIDs {
		if status, body := postBroker(t, b.Addr(), "send", askBody(senderToken, "sender", "receiver", askID, b.Store().Generation())); status != http.StatusOK {
			t.Fatalf("sendAsk %s = (%d, %v)", askID, status, body)
		}
	}

	// Also queue a non-agent control message to verify it is not drained.
	b.PushControl("receiver", ControlMessage{ID: "ctrl-keep", Type: "continue", RunID: "receiver"})

	// The backend caps the drain at the limit and reports the registered name.
	first, err := b.Store().Receive("receiver", 2)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(first) != 2 || first[0].MessageID != askIDs[0] || first[1].MessageID != askIDs[1] {
		t.Fatalf("Receive = %+v, want the first two agent messages", first)
	}
	for _, msg := range first {
		if msg.Sender != "sender-display" {
			t.Errorf("Sender = %q, want the registered display name", msg.Sender)
		}
		if msg.FromRunID != "sender" || msg.Kind != "agent_message" {
			t.Errorf("received message = %+v", msg)
		}
	}

	// The agent message the limit left behind and the control message stay
	// queued, in their original order.
	msgs, err := b.DrainAgentMessages("receiver", 0)
	if err != nil {
		t.Fatalf("DrainAgentMessages: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != askIDs[2] {
		t.Fatalf("unlimited drain = %+v, want the agent message the limit left queued", msgs)
	}

	st := b.GetRun("receiver")
	st.Mu.Lock()
	remaining := len(st.ControlQueue)
	remainingID := ""
	if remaining > 0 {
		remainingID = st.ControlQueue[0].ID
	}
	st.Mu.Unlock()
	if remaining != 1 || remainingID != "ctrl-keep" {
		t.Errorf("queue after the drains = (%d, %q), want only ctrl-keep", remaining, remainingID)
	}

	// Draining again yields nothing.
	msgs2, err := b.DrainAgentMessages("receiver", 0)
	if err != nil {
		t.Fatalf("drain again: %v", err)
	}
	if len(msgs2) != 0 {
		t.Errorf("expected 0 after second drain, got %d", len(msgs2))
	}
}

// TestBrokerWaitReplyDeliversReplyAfterWriteTimeout is the regression test for
// the #243 follow-up: the server-wide WriteTimeout must not truncate the
// wait_reply long-poll and drop a reply that arrives after the deadline.
func TestBrokerWaitReplyDeliversReplyAfterWriteTimeout(t *testing.T) {
	b := New("")
	// Shorten the write deadline before Start so the server is created with a
	// deadline the wait_reply long-poll can actually exceed in a test.
	b.writeTimeout = 100 * time.Millisecond
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	// Fresh connections per request so each wait_reply gets its own deadline.
	noKeepAlive := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	senderToken, err := b.CreateRun("asker", "")
	if err != nil {
		t.Fatalf("create asker: %v", err)
	}
	replierToken, err := b.CreateRun("replier", "")
	if err != nil {
		t.Fatalf("create replier: %v", err)
	}

	addr := b.Addr()
	msgID := "ask-late-reply"

	// Replier polls, sleeps past the write deadline, then replies.
	// Send the ask before starting the replier so the poll loop cannot die
	// on a transient error before the ask exists.
	askBody := bytes.NewReader([]byte(fmt.Sprintf(`{
		"run_id": "asker",
		"token": %q,
		"from_run_id": "asker",
		"to_run_id": "replier",
		"type": "agent_message",
		"payload": {"id":%q,"from":"asker","from_run_id":"asker","to_run_id":"replier","message":"late ask","expects_reply":true}
	}`, senderToken, msgID)))
	resp, err := noKeepAlive.Post(fmt.Sprintf("http://%s/send", addr), "application/json", askBody)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	resp.Body.Close()

	go func() {
		for i := 0; i < 50; i++ {
			pollBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"replier","token":%q}`, replierToken)))
			resp, err := noKeepAlive.Post(fmt.Sprintf("http://%s/poll-control", addr), "application/json", pollBody)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			var msgs []ControlMessage
			_ = json.NewDecoder(resp.Body).Decode(&msgs)
			resp.Body.Close()
			if len(msgs) > 0 {
				time.Sleep(400 * time.Millisecond)
				replyBody := bytes.NewReader([]byte(fmt.Sprintf(`{
					"run_id": "replier",
					"token": %q,
					"from_run_id": "replier",
					"to_run_id": "asker",
					"type": "agent_message",
					"payload": {"id":"reply-%s","from":"replier","from_run_id":"replier","to_run_id":"asker","message":"late answer","reply_to":%q}
				}`, replierToken, msgID, msgID)))
				_, _ = noKeepAlive.Post(fmt.Sprintf("http://%s/send", addr), "application/json", replyBody)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for the reply that arrives after the deadline.

	waitBody := bytes.NewReader([]byte(fmt.Sprintf(`{"run_id":"asker","token":%q,"waiting_for":%q}`, senderToken, msgID)))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/wait_reply", addr), waitBody)
	if err != nil {
		t.Fatalf("wait request: %v", err)
	}
	// Timing the wait makes the test self-verifying: a reply that arrives
	// before the wait_reply handler parks would return within the write
	// deadline and mean the extension path was never exercised.
	waitStarted := time.Now()
	waitResp, err := noKeepAlive.Do(req)
	if err != nil {
		t.Fatalf("wait_reply after write deadline: %v", err)
	}
	elapsed := time.Since(waitStarted)
	t.Logf("wait_reply returned after %v (write deadline %v)", elapsed, b.writeTimeout)
	if elapsed <= b.writeTimeout {
		t.Fatalf("wait_reply returned in %v, within the %v deadline; the long-poll extension was not exercised", elapsed, b.writeTimeout)
	}
	defer waitResp.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(waitResp.Body).Decode(&result); err != nil {
		t.Fatalf("wait_reply decode: %v", err)
	}
	if waitResp.StatusCode != http.StatusOK {
		t.Fatalf("wait_reply status = %d, want 200: %v", waitResp.StatusCode, result)
	}
	if result["from_run_id"] != "replier" {
		t.Fatalf("wait_reply from_run_id = %v, want replier", result["from_run_id"])
	}
}

// TestLocalRouterOutcomeFieldsAdditive pins that the ask handlers keep every
// existing field and status code and add the shared outcome alongside.
func TestLocalRouterOutcomeFieldsAdditive(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("additive_sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	if _, err := b.CreateRun("additive_target", ""); err != nil {
		t.Fatalf("create target: %v", err)
	}
	addr := b.Addr()
	gen := b.Store().Generation()

	// The legacy path keeps its fields and its semantics: an unversioned ask
	// records no terminal fence, so a later withdrawal reports absence.
	legacyID := "additive-ask"
	status, sent := postBroker(t, addr, "send", askBody(senderToken, "additive_sender", "additive_target", legacyID, ""))
	if status != http.StatusOK {
		t.Fatalf("send = (%d, %v)", status, sent)
	}
	if sent["queued"] != true || sent["message_id"] != legacyID || sent["expects_reply"] != true {
		t.Fatalf("send lost its legacy fields: %v", sent)
	}
	if sent["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("send outcome = %v", sent["outcome"])
	}
	status, waited := postBroker(t, addr, "wait_reply", fmt.Sprintf(
		`{"run_id":"additive_sender","token":%q,"waiting_for":%q,"timeout_ms":20}`, senderToken, legacyID))
	if status != http.StatusGatewayTimeout {
		t.Fatalf("wait = (%d, %v), want 504", status, waited)
	}
	if waited["timeout"] != true || waited["message_id"] != legacyID {
		t.Fatalf("wait lost its legacy fields: %v", waited)
	}
	if waited["outcome"] != string(messaging.OutcomeTimedOut) {
		t.Fatalf("wait outcome = %v", waited["outcome"])
	}
	status, absent := postBroker(t, addr, "cancel_message", fmt.Sprintf(
		`{"run_id":"additive_sender","token":%q,"cancel_message_id":%q}`, senderToken, legacyID))
	if status != http.StatusNotFound || absent["outcome"] != string(messaging.OutcomeEdgeAbsent) {
		t.Fatalf("legacy cancel = (%d, %v), want 404 edge-absent", status, absent)
	}

	// A versioned ask keeps a terminal fence, so its withdrawal is reported as
	// a terminal outcome next to the legacy fields.
	versionedID := mintMsgID(t, time.Minute)
	if status, body := postBroker(t, addr, "send", askBody(senderToken, "additive_sender", "additive_target", versionedID, gen)); status != http.StatusOK {
		t.Fatalf("versioned send = (%d, %v)", status, body)
	}
	status, cancelled := postBroker(t, addr, "cancel_message", fmt.Sprintf(
		`{"run_id":"additive_sender","token":%q,"cancel_message_id":%q,"generation":%q}`, senderToken, versionedID, gen))
	if status != http.StatusOK {
		t.Fatalf("cancel = (%d, %v)", status, cancelled)
	}
	if cancelled["cancelled"] != true || cancelled["message_id"] != versionedID {
		t.Fatalf("cancel lost its legacy fields: %v", cancelled)
	}
	if cancelled["outcome"] != string(messaging.OutcomeWithdrawn) {
		t.Fatalf("cancel outcome = %v", cancelled["outcome"])
	}

	// Re-sending a withdrawn ID keeps its existing 409 and now names the
	// outcome that refused it.
	if status, body := postBroker(t, addr, "send", askBody(senderToken, "additive_sender", "additive_target", versionedID, gen)); status != http.StatusConflict || body["outcome"] != string(messaging.OutcomeWithdrawn) {
		t.Fatalf("withdrawn resend = (%d, %v), want 409 withdrawn", status, body)
	}

	// A duplicate ID that meets a timed-out terminal fence keeps the existing
	// 409 with the protocol code and outcome alongside.
	expiredID := mintMsgID(t, time.Minute)
	if status, body := postBroker(t, addr, "send", askBody(senderToken, "additive_sender", "additive_target", expiredID, gen)); status != http.StatusOK {
		t.Fatalf("send = (%d, %v)", status, body)
	}
	if status, body := postBroker(t, addr, "wait_reply", fmt.Sprintf(
		`{"run_id":"additive_sender","token":%q,"waiting_for":%q,"generation":%q,"timeout_ms":20}`, senderToken, expiredID, gen)); status != http.StatusGatewayTimeout {
		t.Fatalf("wait = (%d, %v), want 504", status, body)
	}
	if status, body := postBroker(t, addr, "send", askBody(senderToken, "additive_sender", "additive_target", expiredID, gen)); status != http.StatusConflict {
		t.Fatalf("duplicate send = (%d, %v), want 409", status, body)
	} else {
		if body["error"] != "message id already in use" {
			t.Fatalf("duplicate send message = %v", body["error"])
		}
		if body["code"] != "duplicate-message-id" || body["outcome"] != string(messaging.OutcomeNotSent) {
			t.Fatalf("duplicate send envelope = %v", body)
		}
	}
}

// TestLocalRouterEnvelopeRouting pins that an ask can route on the envelope
// alone and still deliver its payload verbatim with the sender name intact.
func TestLocalRouterEnvelopeRouting(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("envelope_sender", "display-name")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("envelope_target", "")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	addr := b.Addr()
	gen := b.Store().Generation()
	msgID := mintMsgID(t, time.Minute)

	body := fmt.Sprintf(`{"run_id":"envelope_sender","token":%q,"from_run_id":"envelope_sender",
		"to_run_id":"envelope_target","type":"agent_message","generation":%q,"message_id":%q,
		"expects_reply":true,"sender":"display-name","payload":{"message":"ping"}}`,
		senderToken, gen, msgID)
	status, sent := postBroker(t, addr, "send", body)
	if status != http.StatusOK || sent["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("envelope send = (%d, %v)", status, sent)
	}

	polled := pollAgentMessages(t, addr, "envelope_target", targetToken)
	if len(polled) != 1 {
		t.Fatalf("polled %d messages, want 1", len(polled))
	}
	entry := polled[0]
	if entry["id"] != msgID || entry["type"] != "agent_message" {
		t.Fatalf("polled envelope = %v", entry)
	}
	if entry["sender"] != "display-name" {
		t.Fatalf("polled sender = %v, want the registered display name", entry["sender"])
	}
	raw, err := json.Marshal(entry["payload"])
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if string(raw) != `{"message":"ping"}` {
		t.Fatalf("polled payload = %s, want the caller's body verbatim", raw)
	}

	if queued := b.Store().Pending("envelope_sender", msgID); !queued {
		t.Fatal("envelope ask left no live edge")
	}

	// A payload that names the id and asks for a reply routes on the payload.
	payloadID := mintMsgID(t, time.Minute)
	payloadAsk := fmt.Sprintf(`{"run_id":"envelope_sender","token":%q,"from_run_id":"envelope_sender",
		"to_run_id":"envelope_target","type":"agent_message","generation":%q,
		"payload":{"id":%q,"from":"display-name","message":"payload-routed","expects_reply":true}}`,
		senderToken, gen, payloadID)
	if status, body := postBroker(t, addr, "send", payloadAsk); status != http.StatusOK {
		t.Fatalf("payload-routed send = (%d, %v)", status, body)
	}
	if queued := b.Store().Pending("envelope_sender", payloadID); !queued {
		t.Fatal("payload-routed ask did not land under the payload id")
	}
	if polled := pollAgentMessages(t, addr, "envelope_target", targetToken); len(polled) != 1 || polled[0]["id"] != payloadID {
		t.Fatalf("payload-routed poll = %v", polled)
	}

	// Two non-empty ids that disagree are refused instead of resolved by
	// precedence, and nothing is admitted under either id.
	conflictID := mintMsgID(t, time.Minute)
	otherID := mintMsgID(t, time.Minute)
	conflict := fmt.Sprintf(`{"run_id":"envelope_sender","token":%q,"from_run_id":"envelope_sender",
		"to_run_id":"envelope_target","type":"agent_message","generation":%q,"message_id":%q,
		"expects_reply":true,"payload":{"id":%q,"from":"display-name","message":"conflict"}}`,
		senderToken, gen, conflictID, otherID)
	status, refused := postBroker(t, addr, "send", conflict)
	if status != http.StatusBadRequest || refused["code"] != "invalid-request" {
		t.Fatalf("conflicting send = (%d, %v), want 400 invalid-request", status, refused)
	}
	if b.Store().Pending("envelope_sender", conflictID) || b.Store().Pending("envelope_sender", otherID) {
		t.Fatal("conflicting send admitted an ask")
	}

	// Equal ids are the same id, not a conflict: the ask is admitted and
	// delivered under it.
	equalID := mintMsgID(t, time.Minute)
	equal := fmt.Sprintf(`{"run_id":"envelope_sender","token":%q,"from_run_id":"envelope_sender",
		"to_run_id":"envelope_target","type":"agent_message","generation":%q,"message_id":%q,
		"expects_reply":true,"payload":{"id":%q,"from":"display-name","message":"equal"}}`,
		senderToken, gen, equalID, equalID)
	if status, body := postBroker(t, addr, "send", equal); status != http.StatusOK {
		t.Fatalf("equal-id send = (%d, %v), want 200", status, body)
	}
	if queued := b.Store().Pending("envelope_sender", equalID); !queued {
		t.Fatal("equal-id ask did not land under its id")
	}
	if polled := pollAgentMessages(t, addr, "envelope_target", targetToken); len(polled) != 1 || polled[0]["id"] != equalID {
		t.Fatalf("equal-id poll = %v", polled)
	}

	// The display name is bound to the token that authenticated the run: naming
	// someone else is refused, and naming nobody takes the registered name.
	mismatch := fmt.Sprintf(`{"run_id":"envelope_sender","token":%q,"from_run_id":"envelope_sender",
		"to_run_id":"envelope_target","type":"agent_message","generation":%q,"message_id":%q,
		"expects_reply":true,"sender":"other-name","payload":{"message":"impersonation"}}`,
		senderToken, gen, mintMsgID(t, time.Minute))
	status, refused = postBroker(t, addr, "send", mismatch)
	if status != http.StatusForbidden || refused["code"] != "sender-mismatch" {
		t.Fatalf("mismatched sender = (%d, %v), want 403 sender-mismatch", status, refused)
	}

	unnamed := fmt.Sprintf(`{"run_id":"envelope_sender","token":%q,"from_run_id":"envelope_sender",
		"to_run_id":"envelope_target","type":"agent_message","generation":%q,"message_id":%q,
		"expects_reply":true,"payload":{"message":"unnamed"}}`,
		senderToken, gen, mintMsgID(t, time.Minute))
	if status, body := postBroker(t, addr, "send", unnamed); status != http.StatusOK {
		t.Fatalf("unnamed send = (%d, %v)", status, body)
	}
	if polled := pollAgentMessages(t, addr, "envelope_target", targetToken); len(polled) != 1 || polled[0]["sender"] != "display-name" {
		t.Fatalf("unnamed send poll = %v, want the registered display name", polled)
	}

	// Fire-and-forget delivery reports the registered name too, not the raw
	// payload `from`.
	if status := fireAndForgetSend(t, addr, senderToken, "envelope_sender", "envelope_target", "note"); status != http.StatusOK {
		t.Fatalf("fire-and-forget status = %d", status)
	}
	if polled := pollAllMessages(t, addr, "envelope_target", targetToken); len(polled) != 1 || polled[0]["sender"] != "display-name" {
		t.Fatalf("fire-and-forget poll = %v, want the registered display name", polled)
	}

	// An unlabeled run reports its run ID as the display name.
	unlabeledToken, err := b.CreateRun("unlabeled_sender", "")
	if err != nil {
		t.Fatalf("create unlabeled sender: %v", err)
	}
	unlabeled := fmt.Sprintf(`{"run_id":"unlabeled_sender","token":%q,"from_run_id":"unlabeled_sender",
		"to_run_id":"envelope_target","type":"agent_message","generation":%q,"message_id":%q,
		"expects_reply":true,"payload":{"message":"unlabeled"}}`,
		unlabeledToken, gen, mintMsgID(t, time.Minute))
	if status, body := postBroker(t, addr, "send", unlabeled); status != http.StatusOK {
		t.Fatalf("unlabeled send = (%d, %v)", status, body)
	}
	if polled := pollAgentMessages(t, addr, "envelope_target", targetToken); len(polled) != 1 || polled[0]["sender"] != "unlabeled_sender" {
		t.Fatalf("unlabeled send poll = %v, want the run ID as the display name", polled)
	}

	// Renaming changes the name the recipient sees on the next delivery.
	if err := b.RenameRun("envelope_sender", "renamed-sender"); err != nil {
		t.Fatalf("RenameRun: %v", err)
	}
	renamed := fmt.Sprintf(`{"run_id":"envelope_sender","token":%q,"from_run_id":"envelope_sender",
		"to_run_id":"envelope_target","type":"agent_message","generation":%q,"message_id":%q,
		"expects_reply":true,"payload":{"message":"renamed"}}`,
		senderToken, gen, mintMsgID(t, time.Minute))
	if status, body := postBroker(t, addr, "send", renamed); status != http.StatusOK {
		t.Fatalf("renamed send = (%d, %v)", status, body)
	}
	if polled := pollAgentMessages(t, addr, "envelope_target", targetToken); len(polled) != 1 || polled[0]["sender"] != "renamed-sender" {
		t.Fatalf("renamed send poll = %v, want the new name", polled)
	}
}

// TestLocalPollControlAgentOnlyRespectsLimit pins that an agent-message-only
// drain leaves control messages queued, and that a limit caps the drained agent
// messages while the excess keeps its place in the queue.
func TestLocalPollControlAgentOnlyRespectsLimit(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	senderToken, err := b.CreateRun("drain_sender", "")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("drain_target", "")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	addr := b.Addr()

	if err := b.PushControl("drain_target", ControlMessage{ID: "ctrl-1", Type: "interrupt", RunID: "drain_target"}); err != nil {
		t.Fatalf("PushControl: %v", err)
	}
	askIDs := []string{mintMsgID(t, time.Minute), mintMsgID(t, time.Minute)}
	for _, askID := range askIDs {
		ask := fmt.Sprintf(`{"run_id":"drain_sender","token":%q,"from_run_id":"drain_sender",
			"to_run_id":"drain_target","type":"agent_message","generation":%q,"message_id":%q,
			"expects_reply":true,"payload":{"message":"ping"}}`, senderToken, b.Store().Generation(), askID)
		if status, body := postBroker(t, addr, "send", ask); status != http.StatusOK {
			t.Fatalf("send = (%d, %v)", status, body)
		}
	}

	polled := pollControl(t, addr, fmt.Sprintf(
		`{"run_id":"drain_target","token":%q,"agent_only":true,"limit":1}`, targetToken))
	if len(polled) != 1 || polled[0]["id"] != askIDs[0] {
		t.Fatalf("agent-only poll = %v, want the first agent message", polled)
	}
	// The control message was queued first, so it stays ahead of the agent
	// message the limit left behind.
	rest := pollAllMessages(t, addr, "drain_target", targetToken)
	if len(rest) != 2 || rest[0]["id"] != "ctrl-1" || rest[1]["id"] != askIDs[1] {
		t.Fatalf("queue after the limited drain = %v, want the original order", rest)
	}
}

func pollAgentMessages(t *testing.T, addr, runID, token string) []map[string]any {
	t.Helper()
	return pollControl(t, addr, fmt.Sprintf(`{"run_id":%q,"token":%q,"agent_only":true}`, runID, token))
}

func pollAllMessages(t *testing.T, addr, runID, token string) []map[string]any {
	t.Helper()
	return pollControl(t, addr, fmt.Sprintf(`{"run_id":%q,"token":%q}`, runID, token))
}

func pollControl(t *testing.T, addr, body string) []map[string]any {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("http://%s/poll-control", addr), "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("poll-control: %v", err)
	}
	defer resp.Body.Close()
	var msgs []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode poll-control: %v", err)
	}
	return msgs
}
