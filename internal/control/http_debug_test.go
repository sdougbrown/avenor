package control

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

// --- helpers -----------------------------------------------------------------

// startDebugServer creates an HTTPDebugServer bound to a random loopback port
// and returns it along with the address and auth token.  The server is stopped
// automatically via t.Cleanup.
func startDebugServer(t *testing.T, ctrl *ControlServer, adapter StableAdapter) (*HTTPDebugServer, string, string) {
	t.Helper()
	h, err := NewHTTPDebugServer("127.0.0.1:0", ctrl)
	if err != nil {
		t.Fatalf("NewHTTPDebugServer: %v", err)
	}
	if adapter != nil {
		h.SetStableAdapter(adapter)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	go func() { _ = h.server.Serve(ln) }()
	t.Cleanup(func() { _ = h.Stop(context.Background()) })
	return h, addr, h.token
}

func authedGet(t *testing.T, client *http.Client, url, token string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, http.NoBody)
	req.Header.Set("X-Avenor-Token", token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

func authedPost(t *testing.T, client *http.Client, url, token string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, http.NoBody)
	req.Header.Set("X-Avenor-Token", token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

// --- mock StableAdapter ------------------------------------------------------

type mockAdapter struct {
	mu          sync.RWMutex
	runtimes    map[string]any
	cancelCalls []string
	cancelErr   error
}

func newMockAdapter(runtimes map[string]any) *mockAdapter {
	return &mockAdapter{runtimes: runtimes}
}

func (m *mockAdapter) HTTPRuntimeStatus(runtimeID string) (any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap, ok := m.runtimes[runtimeID]
	if !ok {
		return nil, ErrRuntimeNotFound
	}
	return snap, nil
}

func (m *mockAdapter) HTTPCancelRuntime(runtimeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancelErr != nil {
		return m.cancelErr
	}
	if _, ok := m.runtimes[runtimeID]; !ok {
		return ErrRuntimeNotFound
	}
	m.cancelCalls = append(m.cancelCalls, runtimeID)
	return nil
}

// SetRuntime is a concurrency-safe helper for tests that mutate adapter state
// after construction (e.g., while a concurrent SSE stream is active).
func (m *mockAdapter) SetRuntime(id string, snap any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runtimes[id] = snap
}

// SetCancelErr is a concurrency-safe helper to inject a cancel error.
func (m *mockAdapter) SetCancelErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cancelErr = err
}

// CancelCalls returns a copy of the cancel call log.
func (m *mockAdapter) CancelCalls() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, len(m.cancelCalls))
	copy(out, m.cancelCalls)
	return out
}

// --- resolveDebugAddr tests --------------------------------------------------

func TestResolveDebugAddr(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		wantErr bool
		wantOut string // empty means "any non-empty"
	}{
		// Bare :port rewrites to 127.0.0.1.
		{
			name:    "bare port",
			addr:    ":8080",
			wantOut: "127.0.0.1:8080",
		},
		// Numeric loopback fast paths.
		{
			name:    "127.0.0.1 explicit",
			addr:    "127.0.0.1:8080",
			wantOut: "127.0.0.1:8080",
		},
		{
			name:    "::1 explicit",
			addr:    "[::1]:8080",
			wantOut: "[::1]:8080",
		},
		// Non-loopback numeric IP must be rejected.
		{
			name:    "non-loopback IP",
			addr:    "192.168.1.1:8080",
			wantErr: true,
		},
		{
			name:    "0.0.0.0 bind-all",
			addr:    "0.0.0.0:8080",
			wantErr: true,
		},
		// Malformed address.
		{
			name:    "missing port",
			addr:    "127.0.0.1",
			wantErr: true,
		},
		{
			name:    "garbage",
			addr:    "not:an:address:8080",
			wantErr: true,
		},
		// "localhost" short-circuits to 127.0.0.1 without a DNS lookup.
		{
			name:    "localhost hostname",
			addr:    "localhost:8080",
			wantOut: "127.0.0.1:8080",
		},
		// "LOCALHOST" (uppercase) must also short-circuit.
		{
			name:    "localhost uppercase",
			addr:    "LOCALHOST:8080",
			wantOut: "127.0.0.1:8080",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveDebugAddr(tc.addr)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveDebugAddr(%q) = %q, nil; want error", tc.addr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveDebugAddr(%q): %v", tc.addr, err)
			}
			if tc.wantOut != "" && got != tc.wantOut {
				t.Fatalf("resolveDebugAddr(%q) = %q, want %q", tc.addr, got, tc.wantOut)
			}
			if got == "" {
				t.Fatalf("resolveDebugAddr(%q) returned empty string", tc.addr)
			}
		})
	}
}

// --- resolveDebugAddrWith injection-seam tests -------------------------------

func TestResolveDebugAddrWith(t *testing.T) {
	loopbackIPs := func(ips ...string) func(string) ([]net.IP, error) {
		return func(host string) ([]net.IP, error) {
			var result []net.IP
			for _, s := range ips {
				result = append(result, net.ParseIP(s))
			}
			return result, nil
		}
	}
	failLookup := func(t *testing.T) func(string) ([]net.IP, error) {
		return func(host string) ([]net.IP, error) {
			t.Errorf("lookup called for %q but should have been short-circuited", host)
			return nil, fmt.Errorf("unexpected lookup for %q", host)
		}
	}
	noIPs := func(string) ([]net.IP, error) { return nil, nil }
	lookupErr := func(string) ([]net.IP, error) { return nil, fmt.Errorf("no such host") }

	tests := []struct {
		name    string
		addr    string
		lookup  func(string) ([]net.IP, error)
		wantErr bool
		wantOut string
	}{
		{
			name:    "numeric loopback returns as-is",
			addr:    "127.0.0.1:9000",
			lookup:  failLookup(t),
			wantOut: "127.0.0.1:9000",
		},
		{
			name:    "localhost short-circuits — lookup not called",
			addr:    "localhost:9000",
			lookup:  failLookup(t),
			wantOut: "127.0.0.1:9000",
		},
		{
			name:    "hostname resolving to all loopback returns resolved IP",
			addr:    "myloopback.local:9000",
			lookup:  loopbackIPs("127.0.0.1"),
			wantOut: "127.0.0.1:9000",
		},
		{
			name:    "hostname resolving to loopback set prefers 127.0.0.1",
			addr:    "myloopback.local:9000",
			lookup:  loopbackIPs("::1", "127.0.0.1"),
			wantOut: "127.0.0.1:9000",
		},
		{
			name:    "hostname resolving to only ::1 returns ::1",
			addr:    "myloopback.local:9000",
			lookup:  loopbackIPs("::1"),
			wantOut: "[::1]:9000",
		},
		{
			name:    "hostname resolving to mixed loopback and public is rejected",
			addr:    "mixed.local:9000",
			lookup:  loopbackIPs("127.0.0.1", "93.184.216.34"),
			wantErr: true,
		},
		{
			name:    "hostname resolving to all public is rejected",
			addr:    "public.example:9000",
			lookup:  loopbackIPs("93.184.216.34"),
			wantErr: true,
		},
		{
			name:    "hostname resolving to nothing is rejected",
			addr:    "empty.local:9000",
			lookup:  noIPs,
			wantErr: true,
		},
		{
			name:    "lookup error is rejected",
			addr:    "bad.local:9000",
			lookup:  lookupErr,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveDebugAddrWith(tc.addr, tc.lookup)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveDebugAddrWith(%q) = %q, nil; want error", tc.addr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveDebugAddrWith(%q): %v", tc.addr, err)
			}
			if tc.wantOut != "" && got != tc.wantOut {
				t.Fatalf("resolveDebugAddrWith(%q) = %q, want %q", tc.addr, got, tc.wantOut)
			}
		})
	}
}

// --- Start() loopback-literal binding test -----------------------------------

func TestHTTPDebugServerStartBindsLoopbackLiteral(t *testing.T) {
	state := NewState("run_cli", "", 0)
	ctrl := NewServer(state)

	h, err := NewHTTPDebugServer("127.0.0.1:0", ctrl)
	if err != nil {
		t.Fatalf("NewHTTPDebugServer: %v", err)
	}
	if err := h.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	boundAddr := h.server.Addr
	host, _, err := net.SplitHostPort(boundAddr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", boundAddr, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		t.Fatalf("bound host %q is not a numeric IP address", host)
	}
	if !ip.IsLoopback() {
		t.Fatalf("bound host %q is not a loopback address", host)
	}
}

// --- /status/<runtime_id> tests ----------------------------------------------

func TestHTTPStatusRuntimeStableMode(t *testing.T) {
	state := NewState("run_stable", "", 0)
	ctrl := NewServer(state)
	adapter := newMockAdapter(map[string]any{
		"rt_1": map[string]any{
			"runtime_id": "rt_1",
			"status":     "running",
			"session_id": "ses_abc",
		},
	})
	_, addr, token := startDebugServer(t, ctrl, adapter)
	client := &http.Client{Timeout: 2 * time.Second}

	// Existing runtime returns 200 + snapshot.
	resp := authedGet(t, client, "http://"+addr+"/status/rt_1", token)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /status/rt_1: %d, want 200", resp.StatusCode)
	}
	var snap map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if v, _ := snap["runtime_id"].(string); v != "rt_1" {
		t.Errorf("runtime_id = %q, want rt_1", v)
	}
	if v, _ := snap["status"].(string); v != "running" {
		t.Errorf("status = %q, want running", v)
	}
}

func TestHTTPStatusRuntimeNotFound(t *testing.T) {
	state := NewState("run_stable", "", 0)
	ctrl := NewServer(state)
	adapter := newMockAdapter(map[string]any{})
	_, addr, token := startDebugServer(t, ctrl, adapter)
	client := &http.Client{Timeout: 2 * time.Second}

	resp := authedGet(t, client, "http://"+addr+"/status/rt_unknown", token)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /status/rt_unknown: %d, want 404", resp.StatusCode)
	}
}

func TestHTTPStatusRuntimeCLIMode(t *testing.T) {
	// No stable adapter — CLI mode. /status/<id> must return 404.
	state := NewState("run_cli", "", 0)
	ctrl := NewServer(state)
	_, addr, token := startDebugServer(t, ctrl, nil)
	client := &http.Client{Timeout: 2 * time.Second}

	resp := authedGet(t, client, "http://"+addr+"/status/rt_1", token)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /status/rt_1 (CLI mode): %d, want 404", resp.StatusCode)
	}
}

// --- /events?runtime_id= filter tests ----------------------------------------

func TestHTTPEventsRuntimeIDFilter(t *testing.T) {
	state := NewState("run_stable", "", 0)
	ctrl := NewServer(state)
	_, addr, token := startDebugServer(t, ctrl, nil)

	// Connect to SSE stream with runtime_id filter.
	// Use no client-level timeout — SSE is a long-lived streaming response.
	// Cancellation is handled via the request context.
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	req, _ := http.NewRequest(http.MethodGet,
		"http://"+addr+"/events?runtime_id=rt_1&token="+token, http.NoBody)
	req = req.WithContext(ctx)
	sseClient := &http.Client{} // no Timeout — context controls cancellation
	resp, err := sseClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /events: %d, want 200", resp.StatusCode)
	}

	// Publish one event for rt_1 and one for rt_2.
	go func() {
		time.Sleep(20 * time.Millisecond) // let the SSE loop start
		ctrl.PublishEvent(events.Event{
			Event: "agent.status",
			Fields: map[string]any{
				"runtime_id": "rt_2",
				"phase":      "running",
			},
		})
		ctrl.PublishEvent(events.Event{
			Event: "agent.status",
			Fields: map[string]any{
				"runtime_id": "rt_1",
				"phase":      "idle",
			},
		})
	}()

	// Read SSE data lines in a goroutine; collect runtime_id values seen.
	scanner := bufio.NewScanner(resp.Body)
	lines := make(chan string, 32)
	go func() {
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	var receivedRuntimeIDs []string
loop:
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				break loop
			}
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &payload); err != nil {
				continue
			}
			if rtID, _ := payload["runtime_id"].(string); rtID != "" {
				receivedRuntimeIDs = append(receivedRuntimeIDs, rtID)
				if rtID == "rt_1" {
					// Received the expected event; cancel the stream.
					cancel()
					break loop
				}
			}
		case <-ctx.Done():
			break loop
		}
	}

	for _, id := range receivedRuntimeIDs {
		if id == "rt_2" {
			t.Errorf("received event for rt_2 but filter was rt_1; ids received: %v", receivedRuntimeIDs)
			break
		}
	}
	if len(receivedRuntimeIDs) == 0 {
		t.Error("no runtime_id events received for rt_1")
	}
}

// --- /cancel stable-mode tests -----------------------------------------------

func TestHTTPCancelStableModeRequiresRuntimeID(t *testing.T) {
	state := NewState("run_stable", "", 0)
	ctrl := NewServer(state)
	adapter := newMockAdapter(map[string]any{
		"rt_1": map[string]any{"runtime_id": "rt_1"},
	})
	_, addr, token := startDebugServer(t, ctrl, adapter)
	client := &http.Client{Timeout: 2 * time.Second}

	// POST /cancel without runtime_id in stable mode must return 400.
	resp := authedPost(t, client, "http://"+addr+"/cancel", token)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /cancel (stable, no id): %d, want 400", resp.StatusCode)
	}
}

func TestHTTPCancelRuntimeStableMode(t *testing.T) {
	state := NewState("run_stable", "", 0)
	ctrl := NewServer(state)
	adapter := newMockAdapter(map[string]any{
		"rt_1": map[string]any{"runtime_id": "rt_1"},
	})
	_, addr, token := startDebugServer(t, ctrl, adapter)
	client := &http.Client{Timeout: 2 * time.Second}

	// POST /cancel/rt_1 — must return 200 and call adapter.
	resp := authedPost(t, client, "http://"+addr+"/cancel/rt_1", token)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /cancel/rt_1: %d, want 200", resp.StatusCode)
	}
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if v, _ := result["ok"].(bool); !v {
		t.Errorf("ok = %v, want true", result["ok"])
	}
	if calls := adapter.CancelCalls(); len(calls) != 1 || calls[0] != "rt_1" {
		t.Errorf("cancelCalls = %v, want [rt_1]", calls)
	}
}

func TestHTTPCancelRuntimeNotFound(t *testing.T) {
	state := NewState("run_stable", "", 0)
	ctrl := NewServer(state)
	adapter := newMockAdapter(map[string]any{})
	_, addr, token := startDebugServer(t, ctrl, adapter)
	client := &http.Client{Timeout: 2 * time.Second}

	resp := authedPost(t, client, "http://"+addr+"/cancel/rt_unknown", token)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST /cancel/rt_unknown: %d, want 404", resp.StatusCode)
	}
}

func TestHTTPCancelRuntimeCLIMode(t *testing.T) {
	// No stable adapter — CLI mode. /cancel/<id> must return 404.
	state := NewState("run_cli", "", 0)
	ctrl := NewServer(state)
	_, addr, token := startDebugServer(t, ctrl, nil)
	client := &http.Client{Timeout: 2 * time.Second}

	resp := authedPost(t, client, "http://"+addr+"/cancel/rt_1", token)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST /cancel/rt_1 (CLI mode): %d, want 404", resp.StatusCode)
	}
}

func TestHTTPCancelCLIModeFiresGlobalCancel(t *testing.T) {
	// CLI mode: POST /cancel (no id) should fire cancelFn.
	state := NewState("run_cli", "", 0)
	ctrl := NewServer(state)
	cancelled := false
	ctrl.SetCancelFunc(func() { cancelled = true })
	_, addr, token := startDebugServer(t, ctrl, nil)
	client := &http.Client{Timeout: 2 * time.Second}

	resp := authedPost(t, client, "http://"+addr+"/cancel", token)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /cancel (CLI mode): %d, want 200", resp.StatusCode)
	}
	if !cancelled {
		t.Error("cancelFn was not called")
	}
}

// --- Auth matrix tests for per-runtime and events endpoints ------------------
//
// Each sub-test checks: (1) missing token → 401, (2) wrong token → 401,
// (3) correct token → non-401 (auth passed; exact success/not-found code is
// route-specific and tested elsewhere).

// unauthGet sends a GET without any auth header or token query parameter.
func unauthGet(t *testing.T, client *http.Client, url string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, http.NoBody)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

// wrongTokenGet sends a GET with a deliberately incorrect X-Avenor-Token header.
func wrongTokenGet(t *testing.T, client *http.Client, url string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, http.NoBody)
	req.Header.Set("X-Avenor-Token", "00000000000000000000000000000000")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

// unauthPost sends a POST without any auth header.
func unauthPost(t *testing.T, client *http.Client, url string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, http.NoBody)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

// wrongTokenPost sends a POST with a deliberately incorrect X-Avenor-Token header.
func wrongTokenPost(t *testing.T, client *http.Client, url string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, http.NoBody)
	req.Header.Set("X-Avenor-Token", "00000000000000000000000000000000")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func TestAuthRequiredStatusRuntime(t *testing.T) {
	state := NewState("run_stable", "", 0)
	ctrl := NewServer(state)
	adapter := newMockAdapter(map[string]any{
		"rt_1": map[string]any{"runtime_id": "rt_1"},
	})
	_, addr, token := startDebugServer(t, ctrl, adapter)
	client := &http.Client{Timeout: 2 * time.Second}
	url := "http://" + addr + "/status/rt_1"

	t.Run("missing token returns 401", func(t *testing.T) {
		resp := unauthGet(t, client, url)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET /status/rt_1 (no token): %d, want 401", resp.StatusCode)
		}
	})

	t.Run("wrong token returns 401", func(t *testing.T) {
		resp := wrongTokenGet(t, client, url)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET /status/rt_1 (wrong token): %d, want 401", resp.StatusCode)
		}
	})

	t.Run("correct token passes auth", func(t *testing.T) {
		resp := authedGet(t, client, url, token)
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized {
			t.Fatalf("GET /status/rt_1 (correct token): got 401, want non-401")
		}
	})
}

func TestAuthRequiredCancelRuntime(t *testing.T) {
	state := NewState("run_stable", "", 0)
	ctrl := NewServer(state)
	adapter := newMockAdapter(map[string]any{
		"rt_1": map[string]any{"runtime_id": "rt_1"},
	})
	_, addr, token := startDebugServer(t, ctrl, adapter)
	client := &http.Client{Timeout: 2 * time.Second}
	url := "http://" + addr + "/cancel/rt_1"

	t.Run("missing token returns 401", func(t *testing.T) {
		resp := unauthPost(t, client, url)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("POST /cancel/rt_1 (no token): %d, want 401", resp.StatusCode)
		}
	})

	t.Run("wrong token returns 401", func(t *testing.T) {
		resp := wrongTokenPost(t, client, url)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("POST /cancel/rt_1 (wrong token): %d, want 401", resp.StatusCode)
		}
	})

	t.Run("correct token passes auth", func(t *testing.T) {
		resp := authedPost(t, client, url, token)
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized {
			t.Fatalf("POST /cancel/rt_1 (correct token): got 401, want non-401")
		}
	})
}

func TestAuthRequiredEventsRuntimeIDFilter(t *testing.T) {
	// /events uses checkAuthEventSource which accepts both header and ?token=
	// query parameter. Test both the header path and the query-param path.
	state := NewState("run_stable", "", 0)
	ctrl := NewServer(state)
	_, addr, token := startDebugServer(t, ctrl, nil)
	// Use a very short timeout: we only care about the HTTP status, not the
	// streaming body.
	client := &http.Client{Timeout: 2 * time.Second}
	baseURL := "http://" + addr + "/events?runtime_id=rt_1"

	t.Run("missing header and no token param returns 401", func(t *testing.T) {
		resp := unauthGet(t, client, baseURL)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET /events (no token): %d, want 401", resp.StatusCode)
		}
	})

	t.Run("wrong header token returns 401", func(t *testing.T) {
		resp := wrongTokenGet(t, client, baseURL)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET /events (wrong header token): %d, want 401", resp.StatusCode)
		}
	})

	t.Run("wrong query token returns 401", func(t *testing.T) {
		url := baseURL + "&token=00000000000000000000000000000000"
		resp := unauthGet(t, client, url)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET /events (wrong query token): %d, want 401", resp.StatusCode)
		}
	})

	t.Run("correct header token passes auth", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		req, _ := http.NewRequest(http.MethodGet, baseURL, http.NoBody)
		req.Header.Set("X-Avenor-Token", token)
		req = req.WithContext(ctx)
		sseClient := &http.Client{}
		resp, err := sseClient.Do(req)
		if err != nil && ctx.Err() == nil {
			t.Fatalf("GET /events (correct header token): %v", err)
		}
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized {
				t.Fatalf("GET /events (correct header token): got 401, want non-401")
			}
		}
	})

	t.Run("correct query token passes auth", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		url := baseURL + "&token=" + token
		req, _ := http.NewRequest(http.MethodGet, url, http.NoBody)
		req = req.WithContext(ctx)
		sseClient := &http.Client{}
		resp, err := sseClient.Do(req)
		if err != nil && ctx.Err() == nil {
			t.Fatalf("GET /events (correct query token): %v", err)
		}
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized {
				t.Fatalf("GET /events (correct query token): got 401, want non-401")
			}
		}
	})
}

// --- /answer-permission validation tests --------------------------------------

// authedPostBody sends a POST with a JSON body and the auth token.
func authedPostBody(t *testing.T, client *http.Client, url, token string, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("X-Avenor-Token", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func TestHTTPAnswerPermissionInvalidJSON(t *testing.T) {
	state := NewState("run_cli", "", 0)
	ctrl := NewServer(state)
	_, addr, token := startDebugServer(t, ctrl, nil)
	client := &http.Client{Timeout: 2 * time.Second}

	resp := authedPostBody(t, client, "http://"+addr+"/answer-permission", token, "{bad json")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /answer-permission (invalid json): %d, want 400", resp.StatusCode)
	}
}

func TestHTTPAnswerPermissionOversizedMessage(t *testing.T) {
	state := NewState("run_cli", "", 0)
	ctrl := NewServer(state)
	_, addr, token := startDebugServer(t, ctrl, nil)
	client := &http.Client{Timeout: 2 * time.Second}

	// Message larger than MaxPermissionMessageBytes (64 KiB).
	big := strings.Repeat("a", 64*1024+1)
	body := fmt.Sprintf(`{"request_id":"req_1","option_id":"allow","message":%q}`, big)
	resp := authedPostBody(t, client, "http://"+addr+"/answer-permission", token, body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /answer-permission (oversized message): %d, want 400", resp.StatusCode)
	}
}

func TestHTTPAnswerPermissionInvalidUTF8Message(t *testing.T) {
	state := NewState("run_cli", "", 0)
	ctrl := NewServer(state)
	_, addr, token := startDebugServer(t, ctrl, nil)
	client := &http.Client{Timeout: 2 * time.Second}

	// Invalid UTF-8 in the message field — embed raw 0xff/0xfe bytes inside
	// the JSON string value so the body is syntactically valid JSON but the
	// message contains invalid UTF-8. This exercises the UTF-8 validation path
	// in DecodePermissionMessageJSON, not just a JSON parse error.
	body := `{"request_id":"req_1","option_id":"allow","message":"` + string([]byte{0xff, 0xfe}) + `"}`
	resp := authedPostBody(t, client, "http://"+addr+"/answer-permission", token, body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /answer-permission (invalid UTF-8 message): %d, want 400", resp.StatusCode)
	}
}

func TestHTTPAnswerPermissionEmptyMessageOK(t *testing.T) {
	// An empty message is valid (ordinary option without write-in note).
	// The endpoint should not return 400 for an empty message — it will
	// return 409 because no pending permission exists, but that's past
	// validation.
	state := NewState("run_cli", "", 0)
	ctrl := NewServer(state)
	_, addr, token := startDebugServer(t, ctrl, nil)
	client := &http.Client{Timeout: 2 * time.Second}

	body := `{"request_id":"req_1","option_id":"allow"}`
	resp := authedPostBody(t, client, "http://"+addr+"/answer-permission", token, body)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("POST /answer-permission (empty message): got 400, want non-400 (empty message is valid)")
	}
}
