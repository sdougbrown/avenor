package modelapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCompleteSendsOpenAICompatibleRequest(t *testing.T) {
	var gotAuth, gotPath string
	var gotBody completionRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[{"message":{"content":"{\"decision\":\"defer\"}"}}]}`))
	}))
	defer srv.Close()

	client, err := New(srv.URL, "sk-test", "llama-3.3-70b", srv.Client())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	out, err := client.Complete(context.Background(), []Message{
		{Role: "system", Content: "sys"},
		{Role: "user", Content: "usr"},
	}, 256)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if !strings.Contains(out, "defer") {
		t.Fatalf("content = %q", out)
	}
	if gotAuth != "Bearer sk-test" {
		t.Fatalf("auth = %q", gotAuth)
	}
	if !strings.HasSuffix(gotPath, "/chat/completions") {
		t.Fatalf("path = %q", gotPath)
	}
	if gotBody.Model != "llama-3.3-70b" || len(gotBody.Messages) != 2 {
		t.Fatalf("body = %+v", gotBody)
	}
	if gotBody.Stream {
		t.Fatal("stream must be false")
	}
	if gotBody.Temperature != 0 {
		t.Fatalf("temperature = %v, want 0", gotBody.Temperature)
	}
	if gotBody.MaxTokens != 256 {
		t.Fatalf("max_tokens = %v", gotBody.MaxTokens)
	}
}

func TestNewAppendsV1WhenMissing(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer srv.Close()

	client, err := New(strings.TrimSuffix(srv.URL, "/v1"), "", "m", srv.Client())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := client.Complete(context.Background(), []Message{{Role: "user", Content: "x"}}, 8); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if !strings.HasSuffix(gotPath, "/v1/chat/completions") {
		t.Fatalf("path = %q", gotPath)
	}
}

func TestCompleteHTTPErrorIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	client, err := New(srv.URL, "k", "m", srv.Client())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := client.Complete(context.Background(), []Message{{Role: "user", Content: "x"}}, 8); err == nil {
		t.Fatal("500 response should be an error")
	}
}

func TestCompleteMalformedBodyIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"choices":[]}`))
	}))
	defer srv.Close()
	client, _ := New(srv.URL, "k", "m", srv.Client())
	if _, err := client.Complete(context.Background(), []Message{{Role: "user", Content: "x"}}, 8); err == nil {
		t.Fatal("no choices should be an error")
	}
}

func TestNewRejectsBadInput(t *testing.T) {
	if _, err := New("ftp://x", "k", "m", nil); err == nil {
		t.Fatal("non-http scheme should be rejected")
	}
	if _, err := New("http://x/v1", "k", "", nil); err == nil {
		t.Fatal("empty model should be rejected")
	}
}

func TestDefaultTimeoutIsApplied(t *testing.T) {
	client, err := New("http://x/v1", "k", "m", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if client.http.Timeout != DefaultTimeout {
		t.Fatalf("timeout = %v, want %v", client.http.Timeout, DefaultTimeout)
	}
	if DefaultTimeout <= time.Duration(0) {
		t.Fatal("DefaultTimeout must be positive")
	}
}
