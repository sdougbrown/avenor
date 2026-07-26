package control

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"github.com/sdougbrown/avenor/internal/runtime"
)

var ErrRuntimeNotFound = errors.New("runtime not found")

// StableAdapter is the minimal interface that HTTPDebugServer needs to serve
// per-runtime endpoints in stable mode.  The supervisor implements this; CLI
// mode passes nil (no adapter wired).
//
// Method names are prefixed with HTTP to avoid shadowing the StableHandler
// methods on *Supervisor which carry the same logical names but different
// signatures (e.g. RuntimeStatus returns (any, error) there).
type StableAdapter interface {
	// HTTPRuntimeStatus returns the runtime snapshot for the given runtimeID.
	// Returns ErrRuntimeNotFound if the runtime ID does not exist.  Other
	// errors indicate operational failures and should map to 500.
	HTTPRuntimeStatus(runtimeID string) (any, error)
	// HTTPCancelRuntime cancels the named runtime.  Returns ErrRuntimeNotFound
	// if the runtime ID does not exist.  Other errors indicate operational
	// failures and should map to 500.
	HTTPCancelRuntime(runtimeID string) error
}

type HTTPDebugServer struct {
	control       *ControlServer
	stableAdapter atomic.Value // holds StableAdapter, nil if unset
	server        *http.Server
	token         string
}

func NewHTTPDebugServer(addr string, control *ControlServer) (*HTTPDebugServer, error) {
	resolved, err := resolveDebugAddr(addr)
	if err != nil {
		return nil, err
	}
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		panic("avenor: failed to generate debug token: " + err.Error())
	}
	token := hex.EncodeToString(tokenBytes)
	h := &HTTPDebugServer{control: control, token: token}
	mux := http.NewServeMux()
	mux.HandleFunc("/status/", h.handleStatusRuntime)
	mux.HandleFunc("/status", h.handleStatus)
	mux.HandleFunc("/events", h.handleEvents)
	mux.HandleFunc("/cancel/", h.handleCancelRuntime)
	mux.HandleFunc("/cancel", h.handleCancel)
	mux.HandleFunc("/answer-permission", h.handleAnswerPermission)
	h.server = &http.Server{Addr: resolved, Handler: mux}
	// Only print the token when stderr is an interactive terminal.  When stderr
	// is redirected to a file, pipe, or captured by a service manager (journald,
	// systemd, etc.) the token would persist in logs for the lifetime of the
	// process and could be read by anything with access to those logs.  Skipping
	// the print in non-TTY environments is intentional; operators that need the
	// token in non-interactive setups should inject it via another channel.
	if stat, serr := os.Stderr.Stat(); serr == nil && (stat.Mode()&os.ModeCharDevice) != 0 {
		fmt.Fprintf(os.Stderr, "avenor: http debug token: %s\n", token)
	}
	return h, nil
}

// SetStableAdapter wires a StableAdapter into the debug server so that
// per-runtime endpoints are active.  The supervisor calls this immediately
// after NewHTTPDebugServer; CLI mode never calls it.  Passing nil is a no-op.
func (h *HTTPDebugServer) SetStableAdapter(a StableAdapter) {
	if a == nil {
		return
	}
	h.stableAdapter.Store(a)
}

func (h *HTTPDebugServer) getStableAdapter() StableAdapter {
	v := h.stableAdapter.Load()
	if v == nil {
		return nil
	}
	return v.(StableAdapter)
}

// resolveDebugAddr normalises addr and rejects anything that would bind on a
// non-loopback interface.  Accepted forms: ":8080", "localhost:8080",
// "127.0.0.1:8080", "[::1]:8080".  Bare ":port" is rewritten to
// "127.0.0.1:port".
//
// Hostname inputs are resolved to a numeric IP before being returned so that
// the caller's net.Listen call binds to the same address that was verified.
func resolveDebugAddr(addr string) (string, error) {
	return resolveDebugAddrWith(addr, net.LookupIP)
}

// resolveDebugAddrWith is the testable implementation of resolveDebugAddr.
// lookup is called only for hostname inputs; numeric IPs bypass it.
func resolveDebugAddrWith(addr string, lookup func(string) ([]net.IP, error)) (string, error) {
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("avenor: --http-debug: invalid address %q: %w", addr, err)
	}
	if port == "" {
		return "", fmt.Errorf("avenor: --http-debug: address %q must include a port", addr)
	}

	// Fast path: numeric loopback IPs — no DNS needed.
	if ip := net.ParseIP(host); ip != nil {
		if !ip.IsLoopback() {
			return "", fmt.Errorf("avenor: --http-debug: address %q must bind to a loopback interface (localhost, 127.0.0.1, or ::1)", addr)
		}
		return net.JoinHostPort(host, port), nil
	}

	// "localhost" is a special name (RFC 6761) that must always mean the
	// loopback interface.  Short-circuit to 127.0.0.1 so we don't depend on
	// /etc/hosts or nsswitch ordering.
	if strings.EqualFold(host, "localhost") {
		return net.JoinHostPort("127.0.0.1", port), nil
	}

	// Hostname path: resolve to IPs and verify every result is loopback.
	// Return a numeric IP so the subsequent net.Listen binds to the same
	// address that was verified here.
	resolved, err := lookup(host)
	if err != nil {
		return "", fmt.Errorf("avenor: --http-debug: cannot resolve %q: %w", host, err)
	}
	if len(resolved) == 0 {
		return "", fmt.Errorf("avenor: --http-debug: %q resolved to no addresses", host)
	}
	for _, ip := range resolved {
		if !ip.IsLoopback() {
			return "", fmt.Errorf("avenor: --http-debug: hostname %q resolves to non-loopback address %s; use 127.0.0.1 or ::1 explicitly", host, ip)
		}
	}
	// Prefer 127.0.0.1 when available; fall back to the first result.
	chosen := resolved[0]
	for _, ip := range resolved {
		if ip4 := ip.To4(); ip4 != nil && ip4.Equal(net.ParseIP("127.0.0.1")) {
			chosen = ip
			break
		}
	}
	return net.JoinHostPort(chosen.String(), port), nil
}

// checkAuthHeader verifies the X-Avenor-Token request header only.
// Use this for all endpoints except /events (which must support EventSource).
func (h *HTTPDebugServer) checkAuthHeader(r *http.Request) bool {
	if h.token == "" {
		return false
	}
	hdr := r.Header.Get("X-Avenor-Token")
	return hdr != "" && subtle.ConstantTimeCompare([]byte(hdr), []byte(h.token)) == 1
}

// checkAuthEventSource verifies the X-Avenor-Token header or the ?token=
// query parameter.  The query-param fallback exists solely for EventSource
// clients (browser APIs that cannot set custom headers).  Do not use this on
// POST endpoints — query params leak into access logs, Referer headers, and
// shell history.
func (h *HTTPDebugServer) checkAuthEventSource(r *http.Request) bool {
	if h.checkAuthHeader(r) {
		return true
	}
	if h.token == "" {
		return false
	}
	q := r.URL.Query().Get("token")
	return q != "" && subtle.ConstantTimeCompare([]byte(q), []byte(h.token)) == 1
}

func (h *HTTPDebugServer) Start() error {
	ln, err := net.Listen("tcp", h.server.Addr)
	if err != nil {
		return err
	}
	h.server.Addr = ln.Addr().String()
	go func() { _ = h.server.Serve(ln) }()
	return nil
}

func (h *HTTPDebugServer) Stop(ctx context.Context) error {
	return h.server.Shutdown(ctx)
}

func (h *HTTPDebugServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.checkAuthHeader(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !isLocalhostRequest(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	writeJSON(w, h.control.state.Snapshot())
}

// handleStatusRuntime serves GET /status/<runtime_id>.
// Returns 404 when no stable adapter is wired (CLI mode).
func (h *HTTPDebugServer) handleStatusRuntime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.checkAuthHeader(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !isLocalhostRequest(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	adapter := h.getStableAdapter()
	if adapter == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	rtID := strings.TrimPrefix(r.URL.Path, "/status/")
	if rtID == "" {
		http.Error(w, "runtime_id required", http.StatusBadRequest)
		return
	}
	snap, err := adapter.HTTPRuntimeStatus(rtID)
	if err != nil {
		if errors.Is(err, ErrRuntimeNotFound) {
			http.Error(w, "runtime not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, snap)
}

func (h *HTTPDebugServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.checkAuthEventSource(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !isLocalhostRequest(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	// Flush headers immediately so the client's Do() returns without waiting
	// for the first event.
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Optional per-runtime filter: ?runtime_id=<id>
	filterID := r.URL.Query().Get("runtime_id")

	ch := h.control.SubscribeEvents(r.Context())
	for ev := range ch {
		if filterID != "" {
			evRuntimeID, _ := ev.Fields["runtime_id"].(string)
			if evRuntimeID != filterID {
				continue
			}
		}
		data, _ := json.Marshal(eventToMap(ev))
		_, _ = fmt.Fprintf(w, "event: event\ndata: %s\n\n", data)
		flusher.Flush()
	}
}

// handleCancel serves POST /cancel (no path suffix).
// In CLI mode (no stable adapter): fires the global cancelFn.
// In stable mode (stable adapter wired): returns 400 — a runtime_id is required.
func (h *HTTPDebugServer) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.checkAuthHeader(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !isLocalhostRequest(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if h.getStableAdapter() != nil {
		http.Error(w, "runtime_id required in stable mode; use POST /cancel/<runtime_id>", http.StatusBadRequest)
		return
	}
	if h.control.cancelFn != nil {
		h.control.cancelFn()
	}
	writeJSON(w, map[string]any{"ok": true})
}

// handleCancelRuntime serves POST /cancel/<runtime_id>.
// Returns 404 in CLI mode (no stable adapter wired).
func (h *HTTPDebugServer) handleCancelRuntime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.checkAuthHeader(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !isLocalhostRequest(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	adapter := h.getStableAdapter()
	if adapter == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	rtID := strings.TrimPrefix(r.URL.Path, "/cancel/")
	if rtID == "" {
		http.Error(w, "runtime_id required", http.StatusBadRequest)
		return
	}
	if err := adapter.HTTPCancelRuntime(rtID); err != nil {
		if errors.Is(err, ErrRuntimeNotFound) {
			http.Error(w, "runtime not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (h *HTTPDebugServer) handleAnswerPermission(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.checkAuthHeader(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !isLocalhostRequest(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var p PermissionAnswer
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := runtime.ValidatePermissionMessage(p.Message); err != nil {
		http.Error(w, "invalid permission message", http.StatusBadRequest)
		return
	}
	if !h.control.AnswerPendingPermission("", p.RequestID, p.OptionID, p.Message) {
		http.Error(w, "no pending permission", http.StatusConflict)
		return
	}
	writeJSON(w, map[string]any{"accepted": true})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// isLocalhostRequest returns true only when the TCP connection itself
// originated from a loopback address.  The Host header is intentionally
// ignored because it is caller-controlled and provides no security guarantee.
func isLocalhostRequest(r *http.Request) bool {
	remoteHost, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteHost = r.RemoteAddr
	}
	return remoteHost == "127.0.0.1" || remoteHost == "::1"
}
