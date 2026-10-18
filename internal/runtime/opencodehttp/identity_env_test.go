package opencodehttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// TestIdentityOptionsStayOutOfRemoteRequests pins the external-server
// limitation: attaching to an opencode server delivers no per-session child
// environment, so a launch with provenance neither mutates this process's
// environment nor sends identity to the server.
func TestIdentityOptionsStayOutOfRemoteRequests(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	record := func(r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		encoded, _ := json.Marshal(body)
		mu.Lock()
		bodies = append(bodies, r.Method+" "+r.URL.Path+" "+string(encoded))
		mu.Unlock()
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/global/health":
			w.WriteHeader(http.StatusOK)
		case "/session":
			record(r)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_identity"})
		case "/event":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
		default:
			record(r)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	t.Setenv(runtime.EnvRuntimeID, "parent-runtime")
	t.Setenv(runtime.EnvKind, "parent-kind")
	prov, err := NewWithOptions(runtime.StartOptions{})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	p := prov.(*Provider)
	defer p.Close()

	if _, err := p.Start(context.Background(), runtime.StartOptions{
		ServerURL: srv.URL,
		Provenance: &runtime.LaunchProvenance{
			RuntimeID:   "rt_child",
			ExecutionID: "rt_child",
			Kind:        "team",
			Phase:       "reviewer-secret-phase",
			Label:       "reviewer-secret-label",
			Attempt:     1,
		},
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if got := os.Getenv(runtime.EnvRuntimeID); got != "parent-runtime" {
		t.Fatalf("%s = %q, want the parent value untouched", runtime.EnvRuntimeID, got)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, body := range bodies {
		if strings.Contains(body, "AVENOR_") || strings.Contains(body, "reviewer-secret") || strings.Contains(body, "rt_child") {
			t.Fatalf("request carried launch identity: %s", body)
		}
	}
	if len(bodies) == 0 {
		t.Fatal("no request was recorded")
	}
}
