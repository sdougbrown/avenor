package childmessaging

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

func newTestProvider(t *testing.T, handler http.HandlerFunc) (*MessageProvider, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	return newTestProviderOn(t, srv, srv.Client())
}

// newTestProviderOn builds a provider over an already-started broker with the
// client the surface supplies.
func newTestProviderOn(t *testing.T, srv *httptest.Server, client *http.Client) (*MessageProvider, *httptest.Server) {
	t.Helper()
	t.Cleanup(srv.Close)
	provider, err := NewMessageProvider(messaging.SurfaceChanneltools, Options{
		RunID: "child_run", Token: "tok_1", BrokerURL: srv.URL, HTTPClient: client,
	})
	if err != nil {
		t.Fatalf("NewMessageProvider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	return provider, srv
}

func TestProviderPrepareBindsGeneration(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	provider, _ := newTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		generation := "gen-1"
		if calls > 1 {
			generation = "gen-2"
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"messaging_protocol": 1, "generation": generation})
	})
	if got := provider.Generation(); got != "" {
		t.Fatalf("generation before Prepare = %q", got)
	}
	if err := provider.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := provider.Generation(); got != "gen-1" {
		t.Fatalf("generation = %q, want gen-1", got)
	}
	if err := provider.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := provider.Generation(); got != "gen-1" {
		t.Fatalf("generation after a second Prepare = %q, want the pinned gen-1", got)
	}
	if provider.Caller().Kind != messaging.CallerBroker || provider.Caller().RunID != "child_run" {
		t.Fatalf("caller = %+v", provider.Caller())
	}
	if provider.Surface() != messaging.SurfaceChanneltools || provider.Role() != "agent" {
		t.Fatalf("surface/role = %q/%q", provider.Surface(), provider.Role())
	}
}

// TestProviderCloseRefusesRenameAndLast pins that both authenticated routes
// report the closed transport instead of reaching the broker again.
func TestProviderCloseRefusesRenameAndLast(t *testing.T) {
	provider, _ := newTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sessions" {
			t.Errorf("post-close request reached the broker: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"messaging_protocol": 1, "generation": "gen-1"})
	})
	ctx := context.Background()
	if err := provider.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := provider.Rename(ctx, messaging.RenameRequest{Name: "new"}); !errors.Is(err, messaging.ErrTransportClosed) {
		t.Fatalf("post-close Rename error = %v, want ErrTransportClosed", err)
	}
	if _, err := provider.Last(ctx, "rt_1"); !errors.Is(err, messaging.ErrTransportClosed) {
		t.Fatalf("post-close Last error = %v, want ErrTransportClosed", err)
	}
}

func TestProviderResolveAndRunState(t *testing.T) {
	provider, _ := newTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sessions" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messaging_protocol": 1, "generation": "gen-1",
			"sessions": []any{
				map[string]any{"run_id": "child_run", "label": "child", "status": "running"},
				map[string]any{"run_id": "rt_1", "label": "alpha", "status": "ended"},
				map[string]any{"run_id": "rt_2", "label": "mesh", "status": "running"},
				map[string]any{"run_id": "rt_3", "label": "mesh", "status": "running"},
			},
		})
	})
	ctx := context.Background()
	if err := provider.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	self, err := provider.Resolve(ctx, "child_run")
	if err != nil || self.RunID != "child_run" || self.Match != messaging.MatchUnique {
		t.Fatalf("self = %+v, %v", self, err)
	}
	byID, err := provider.Resolve(ctx, "rt_1")
	if err != nil || byID.RunID != "rt_1" || byID.Label != "alpha" {
		t.Fatalf("runtime id = %+v, %v", byID, err)
	}
	byLabel, err := provider.Resolve(ctx, "alpha")
	if err != nil || byLabel.RunID != "rt_1" {
		t.Fatalf("label = %+v, %v", byLabel, err)
	}
	ambiguous, err := provider.Resolve(ctx, "mesh")
	if err != nil || ambiguous.Match != messaging.MatchAmbiguous || len(ambiguous.Candidates) != 2 {
		t.Fatalf("ambiguous = %+v, %v", ambiguous, err)
	}
	none, err := provider.Resolve(ctx, "ghost")
	if err != nil || none.Match != messaging.MatchNone {
		t.Fatalf("none = %+v, %v", none, err)
	}

	facts, err := provider.RunState(ctx, "rt_1")
	if err != nil || facts.Status != "done" || facts.Live || facts.Resumable {
		t.Fatalf("ended facts = %+v, %v", facts, err)
	}
	live, err := provider.RunState(ctx, "rt_2")
	if err != nil || !live.Live {
		t.Fatalf("live facts = %+v, %v", live, err)
	}
	if _, err := provider.RunState(ctx, "ghost"); !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("unknown state error = %v", err)
	}
}

func TestProviderRefusesResolutionOnAnIncompleteListing(t *testing.T) {
	// The broker reports a partial listing beside the peers it read: the
	// provider refuses the whole lookup before consulting the partial set,
	// even for a peer it can see.
	provider, _ := newTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sessions" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messaging_protocol": 1, "generation": "gen-1",
			"sessions":    []any{map[string]any{"run_id": "rt_1", "label": "alpha", "status": "running"}},
			"peers_error": "the broker could not read every peer",
		})
	})
	ctx := context.Background()
	if err := provider.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Resolve(ctx, "rt_1"); !errors.Is(err, messaging.ErrIncompleteListing) {
		t.Fatalf("Resolve error = %v, want the incomplete-listing error", err)
	}
	if _, err := provider.RunState(ctx, "rt_1"); !errors.Is(err, messaging.ErrIncompleteListing) {
		t.Fatalf("RunState error = %v, want the incomplete-listing error", err)
	}
}

func TestRenameMapsWireCodes(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     map[string]any
		wantCode string
	}{
		{"success", http.StatusOK, map[string]any{"run_id": "rt_1", "label": "new", "previous_label": "old"}, ""},
		{"not permitted", http.StatusForbidden, map[string]any{"error": "not yours", "code": "not-permitted"}, "not_permitted"},
		{"invalid name", http.StatusBadRequest, map[string]any{"error": "bad name", "code": "invalid-name"}, "invalid_name"},
		{"unknown target", http.StatusNotFound, map[string]any{"error": "no run", "code": "unknown-target"}, "unknown_target"},
		{"ambiguous", http.StatusConflict, map[string]any{"error": "two runs", "code": "ambiguous-name"}, "ambiguous_name"},
		{"refused", http.StatusBadRequest, map[string]any{"error": "persist failed", "code": "invalid-request"}, "refused"},
		{"rejected token", http.StatusUnauthorized, map[string]any{"error": "bad run token", "code": "invalid-request"}, "not_permitted"},
		{"unrecognised forbidden", http.StatusForbidden, map[string]any{"error": "odd 403", "code": "some-new-code"}, "not_permitted"},
		{"unrecognised missing", http.StatusNotFound, map[string]any{"error": "odd 404", "code": "some-new-code"}, "unknown_target"},
		{"unrecognised conflict", http.StatusConflict, map[string]any{"error": "odd 409", "code": "some-new-code"}, "ambiguous_name"},
		{"unrecognised", http.StatusTeapot, map[string]any{"error": "odd"}, "refused"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body map[string]any
			provider, _ := newTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/sessions":
					_ = json.NewEncoder(w).Encode(map[string]any{"messaging_protocol": 1, "generation": "gen-1"})
				case "/rename":
					_ = json.NewDecoder(r.Body).Decode(&body)
					w.WriteHeader(tc.status)
					_ = json.NewEncoder(w).Encode(tc.body)
				default:
					http.NotFound(w, r)
				}
			})
			result, err := provider.Rename(context.Background(), messaging.RenameRequest{SessionID: "run_1", Name: "new"})
			if tc.wantCode == "" {
				if err != nil || result.Label != "new" || result.PreviousLabel != "old" {
					t.Fatalf("rename = %+v, %v", result, err)
				}
				if body["session_id"] != "run_1" || body["run_id"] != "child_run" || body["token"] != "tok_1" {
					t.Fatalf("rename body = %#v", body)
				}
				return
			}
			var toolErr *router.ToolError
			if !errors.As(err, &toolErr) || toolErr.Code != tc.wantCode {
				t.Fatalf("rename error = %v, want %s", err, tc.wantCode)
			}
		})
	}
}

func TestLastSeparatesUnknownTargetFromEmptyRecord(t *testing.T) {
	provider, _ := newTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{"messaging_protocol": 1, "generation": "gen-1"})
		case "/last_message":
			if r.URL.Query().Get("target") == "ghost" {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "run not found: ghost", "code": "target-unavailable"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"run_id": "rt_1", "label": "alpha", "has_record": false})
		default:
			http.NotFound(w, r)
		}
	})
	ctx := context.Background()
	empty, err := provider.Last(ctx, "rt_1")
	if err != nil || empty.HasRecord {
		t.Fatalf("empty record = %+v, %v", empty, err)
	}
	_, err = provider.Last(ctx, "ghost")
	var toolErr *router.ToolError
	if !errors.As(err, &toolErr) || toolErr.Code != "unknown_target" {
		t.Fatalf("unknown target error = %v", err)
	}
}

// pathFaultTransport delegates to a real transport except for the listed
// paths, which it fails with the given error. It shows which client a request
// was sent on.
type pathFaultTransport struct {
	base http.RoundTripper
	fail map[string]error
}

func (t pathFaultTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err, ok := t.fail[req.URL.Path]; ok {
		return nil, err
	}
	return t.base.RoundTrip(req)
}

// TestSurfaceClientCarriesRenameAndLast proves rename and last are sent on the
// client the child surface supplies, so a surface that configures a transport
// or a timeout gets it on those routes too instead of on the ask verbs only.
func TestSurfaceClientCarriesRenameAndLast(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{"messaging_protocol": 1, "generation": "gen-1"})
		case "/rename":
			_ = json.NewEncoder(w).Encode(map[string]any{"run_id": "child_run", "label": "new"})
		case "/last_message":
			_ = json.NewEncoder(w).Encode(map[string]any{"run_id": "rt_1", "has_record": true, "final_output": "OK"})
		default:
			http.NotFound(w, r)
		}
	}))
	fault := errors.New("surface client transport refused the request")
	client := &http.Client{Transport: pathFaultTransport{
		base: srv.Client().Transport,
		fail: map[string]error{"/rename": fault, "/last_message": fault},
	}}
	provider, _ := newTestProviderOn(t, srv, client)
	ctx := context.Background()
	if _, err := provider.Rename(ctx, messaging.RenameRequest{Name: "new"}); !errors.Is(err, fault) {
		t.Fatalf("rename error = %v, want the surface client's transport fault", err)
	}
	if _, err := provider.Last(ctx, "rt_1"); !errors.Is(err, fault) {
		t.Fatalf("last error = %v, want the surface client's transport fault", err)
	}
}

// TestSurfaceClientTimeoutBoundsRenameAndLast proves the surface's request
// timeout applies to rename and last, not the transport's own 30s default.
func TestSurfaceClientTimeoutBoundsRenameAndLast(t *testing.T) {
	var once sync.Once
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			_ = json.NewEncoder(w).Encode(map[string]any{"messaging_protocol": 1, "generation": "gen-1"})
			return
		}
		<-release
		_ = json.NewEncoder(w).Encode(map[string]any{"run_id": "child_run", "has_record": false})
	}))
	client := &http.Client{Timeout: 100 * time.Millisecond, Transport: srv.Client().Transport}
	provider, _ := newTestProviderOn(t, srv, client)
	// Registered after the server's own cleanup, so a blocked handler is
	// released before srv.Close waits for it.
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	ctx := context.Background()
	calls := []struct {
		name string
		call func() error
	}{
		{"rename", func() error {
			_, err := provider.Rename(ctx, messaging.RenameRequest{Name: "new"})
			return err
		}},
		{"last", func() error {
			_, err := provider.Last(ctx, "rt_1")
			return err
		}},
	}
	for _, call := range calls {
		done := make(chan error, 1)
		go func() { done <- call.call() }()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s error = %v, want the surface client's timeout", call.name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s outlived the surface client's timeout", call.name)
		}
	}
}

// TestRenameAndLastTakeTheProtocolGate proves the two authenticated routes
// refuse on a broker that does not speak the shared protocol, instead of
// succeeding where the ask verbs already refused.
func TestRenameAndLastTakeTheProtocolGate(t *testing.T) {
	provider, _ := newTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sessions" {
			t.Errorf("unexpected path %s on a broker without the protocol marker", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"generation": "gen-1"})
	})
	ctx := context.Background()
	if _, err := provider.Rename(ctx, messaging.RenameRequest{Name: "new"}); !errors.Is(err, messaging.ErrUnsupportedProtocol) {
		t.Fatalf("rename error = %v, want ErrUnsupportedProtocol", err)
	}
	if _, err := provider.Last(ctx, "rt_1"); !errors.Is(err, messaging.ErrUnsupportedProtocol) {
		t.Fatalf("last error = %v, want ErrUnsupportedProtocol", err)
	}
}
