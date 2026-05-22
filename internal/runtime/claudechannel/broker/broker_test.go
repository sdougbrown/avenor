package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
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
