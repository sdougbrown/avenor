package brokertools

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

func newTestProvider(t *testing.T, handler http.HandlerFunc) (*MessageProvider, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	provider, err := NewMessageProvider(messaging.SurfaceChanneltools, Options{
		RunID: "child_run", Token: "tok_1", BrokerURL: srv.URL, HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("NewMessageProvider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	return provider, srv
}

func TestProviderPrepareBindsGeneration(t *testing.T) {
	provider, _ := newTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"messaging_protocol": 1, "generation": "gen-1"})
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
	if provider.Caller().Kind != messaging.CallerBroker || provider.Caller().RunID != "child_run" {
		t.Fatalf("caller = %+v", provider.Caller())
	}
	if provider.Surface() != messaging.SurfaceChanneltools || provider.Role() != "agent" {
		t.Fatalf("surface/role = %q/%q", provider.Surface(), provider.Role())
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

func TestRenameSelfMapsWireCodes(t *testing.T) {
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
		{"unrecognised", http.StatusTeapot, map[string]any{"error": "odd"}, "refused"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/rename" {
					http.NotFound(w, r)
					return
				}
				_ = json.NewDecoder(r.Body).Decode(&body)
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(tc.body)
			}))
			defer srv.Close()
			result, err := RenameSelf(context.Background(), srv.Client(), srv.URL, messaging.LocalCredentials{RunID: "run_1", Token: "tok"}, "run_1", "new")
			if tc.wantCode == "" {
				if err != nil || result.Label != "new" || result.PreviousLabel != "old" {
					t.Fatalf("rename = %+v, %v", result, err)
				}
				if body["session_id"] != "run_1" || body["run_id"] != "run_1" || body["token"] != "tok" {
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

func TestLastMessageSeparatesUnknownTargetFromEmptyRecord(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/last_message" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("target") == "ghost" {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "run not found: ghost", "code": "target-unavailable"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"run_id": "rt_1", "label": "alpha", "has_record": false})
	}))
	defer srv.Close()
	creds := messaging.LocalCredentials{RunID: "run_1", Token: "tok"}
	empty, err := LastMessage(context.Background(), srv.Client(), srv.URL, creds, "rt_1")
	if err != nil || empty.HasRecord {
		t.Fatalf("empty record = %+v, %v", empty, err)
	}
	_, err = LastMessage(context.Background(), srv.Client(), srv.URL, creds, "ghost")
	var toolErr *router.ToolError
	if !errors.As(err, &toolErr) || toolErr.Code != "unknown_target" {
		t.Fatalf("unknown target error = %v", err)
	}
}
