package broker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

// TestBodyReplacementPattern verifies that the io.ReadAll -> json.Unmarshal ->
// io.NopCloser(bytes.NewReader(bodyBytes)) pattern correctly restores the request
// body so downstream handlers can decode it.
func TestBodyReplacementPattern(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer b.Stop()

	baseURL := "http://" + b.Addr()

	// Register a run to get credentials
	regBody := bytes.NewReader([]byte(`{"run_id":"body_test"}`))
	resp, err := http.Post(baseURL+"/register", "application/json", regBody)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	var regResult map[string]string
	json.NewDecoder(resp.Body).Decode(&regResult)
	resp.Body.Close()
	token := regResult["token"]

	// Send a report through the authenticated /report endpoint.
	repBody := bytes.NewReader([]byte(`{"run_id":"body_test","token":"` + token + `","state":"working","payload":{"summary":"ok"}}`))
	resp, err = http.Post(baseURL+"/report", "application/json", repBody)
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var buf bytes.Buffer
		buf.ReadFrom(resp.Body)
		t.Fatalf("report: expected 200, got %d; body: %s", resp.StatusCode, buf.String())
	}

	st := b.GetRun("body_test")
	if st == nil {
		t.Fatal("run not found")
	}
	if len(st.Reports) != 1 {
		t.Fatalf("expected 1 report, got %d (state=%q)", len(st.Reports), func() string {
			if st != nil && len(st.Reports) > 0 {
				return st.Reports[0].State
			}
			return ""
		}())
	}
	if st.Reports[0].State != "working" {
		t.Fatalf("unexpected state: %s", st.Reports[0].State)
	}
}
