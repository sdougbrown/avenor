package control

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// errMockAdapter is a self-contained StableAdapter mock for error-path tests.
// It is intentionally separate from the mockAdapter in http_debug_test.go to
// avoid merge conflicts with parallel work on that file.
type errMockAdapter struct {
	statusFn func(runtimeID string) (any, error)
	cancelFn func(runtimeID string) error
}

func (m *errMockAdapter) HTTPRuntimeStatus(runtimeID string) (any, error) {
	return m.statusFn(runtimeID)
}

func (m *errMockAdapter) HTTPCancelRuntime(runtimeID string) error {
	return m.cancelFn(runtimeID)
}

// startErrTestServer binds an HTTPDebugServer with the given adapter and
// returns the bound address and auth token.
func startErrTestServer(t *testing.T, adapter StableAdapter) (string, string) {
	t.Helper()
	state := NewState("run_err_test", "", 0)
	ctrl, _ := NewTestServer(state)
	h, err := NewHTTPDebugServer("127.0.0.1:0", ctrl)
	if err != nil {
		t.Fatalf("NewHTTPDebugServer: %v", err)
	}
	if adapter != nil {
		h.SetStableAdapter(adapter)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := ln.Addr().String()
	go func() { _ = h.server.Serve(ln) }()
	t.Cleanup(func() { _ = h.Stop(context.Background()) })
	return addr, h.token
}

func errAuthedGet(t *testing.T, addr, path, token string) *http.Response {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	req, _ := http.NewRequest(http.MethodGet, "http://"+addr+path, http.NoBody)
	req.Header.Set("X-Avenor-Token", token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

func errAuthedPost(t *testing.T, addr, path, token string) *http.Response {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+path, http.NoBody)
	req.Header.Set("X-Avenor-Token", token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

// TestHandleStatusRuntime_NotFound verifies that ErrRuntimeNotFound from the
// adapter produces a 404 with body "runtime not found".
func TestHandleStatusRuntime_NotFound(t *testing.T) {
	adapter := &errMockAdapter{
		statusFn: func(_ string) (any, error) { return nil, ErrRuntimeNotFound },
		cancelFn: func(_ string) error { return nil },
	}
	addr, token := startErrTestServer(t, adapter)

	resp := errAuthedGet(t, addr, "/status/rt_missing", token)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(strings.TrimSpace(string(body)), "runtime not found") {
		t.Errorf("body = %q, want to contain \"runtime not found\"", string(body))
	}
}

// TestHandleStatusRuntime_OtherError verifies that a non-not-found error from
// the adapter produces a 500 with JSON body {"error":"<message>"}.
func TestHandleStatusRuntime_OtherError(t *testing.T) {
	adapter := &errMockAdapter{
		statusFn: func(_ string) (any, error) { return nil, errors.New("something broke") },
		cancelFn: func(_ string) error { return nil },
	}
	addr, token := startErrTestServer(t, adapter)

	resp := errAuthedGet(t, addr, "/status/rt_1", token)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	var payload map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode JSON body: %v", err)
	}
	if payload["error"] != "something broke" {
		t.Errorf("error = %q, want \"something broke\"", payload["error"])
	}
}

// TestHandleCancelRuntime_NotFound verifies that ErrRuntimeNotFound from the
// adapter produces a 404 with body "runtime not found".
func TestHandleCancelRuntime_NotFound(t *testing.T) {
	adapter := &errMockAdapter{
		statusFn: func(_ string) (any, error) { return nil, nil },
		cancelFn: func(_ string) error { return ErrRuntimeNotFound },
	}
	addr, token := startErrTestServer(t, adapter)

	resp := errAuthedPost(t, addr, "/cancel/rt_missing", token)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(strings.TrimSpace(string(body)), "runtime not found") {
		t.Errorf("body = %q, want to contain \"runtime not found\"", string(body))
	}
}

// TestHandleCancelRuntime_OtherError verifies that a non-not-found error from
// the adapter produces a 500 with JSON body {"error":"<message>"}.
func TestHandleCancelRuntime_OtherError(t *testing.T) {
	adapter := &errMockAdapter{
		statusFn: func(_ string) (any, error) { return nil, nil },
		cancelFn: func(_ string) error { return errors.New("something broke") },
	}
	addr, token := startErrTestServer(t, adapter)

	resp := errAuthedPost(t, addr, "/cancel/rt_1", token)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	var payload map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode JSON body: %v", err)
	}
	if payload["error"] != "something broke" {
		t.Errorf("error = %q, want \"something broke\"", payload["error"])
	}
}

// TestSetStableAdapter_Concurrent exercises SetStableAdapter and
// getStableAdapter from multiple goroutines to confirm there is no data race
// and the final stored value reads back correctly.
func TestSetStableAdapter_Concurrent(t *testing.T) {
	state := NewState("run_race", "", 0)
	ctrl, _ := NewTestServer(state)
	h, err := NewHTTPDebugServer("127.0.0.1:0", ctrl)
	if err != nil {
		t.Fatalf("NewHTTPDebugServer: %v", err)
	}

	final := &errMockAdapter{
		statusFn: func(_ string) (any, error) { return nil, ErrRuntimeNotFound },
		cancelFn: func(_ string) error { return ErrRuntimeNotFound },
	}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				a := &errMockAdapter{
					statusFn: func(_ string) (any, error) { return nil, ErrRuntimeNotFound },
					cancelFn: func(_ string) error { return ErrRuntimeNotFound },
				}
				h.SetStableAdapter(a)
			} else {
				_ = h.getStableAdapter()
			}
		}()
	}
	wg.Wait()

	h.SetStableAdapter(final)
	if got := h.getStableAdapter(); got != final {
		t.Errorf("getStableAdapter() = %v, want %v", got, final)
	}
}
