package board

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/sdougbrown/avenor/client"
)

// Server is the board dashboard HTTP server.
type Server struct {
	cfg      Config
	server   *http.Server
	listener net.Listener
	token    string
}

// NewServer creates a board server. If cfg.Token is empty and cfg.NoAuth is
// false, the bind address must be loopback-only. NoAuth allows non-loopback
// without any auth — use on trusted networks only.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Token == "" && !cfg.NoAuth {
		if err := mustBeLoopback(cfg.Addr); err != nil {
			return nil, err
		}
	}

	resolved, err := resolveAddr(cfg.Addr)
	if err != nil {
		return nil, err
	}

	token := cfg.Token
	if token == "" {
		tokenBytes := make([]byte, 16)
		if _, err := rand.Read(tokenBytes); err != nil {
			return nil, fmt.Errorf("avenor board: failed to generate token: %w", err)
		}
		token = hex.EncodeToString(tokenBytes)
	}

	s := &Server{cfg: cfg, token: token}

	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/api/sockets", s.handleSockets)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/list", s.handleList)
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/cancel", s.handleCancel)
	mux.HandleFunc("/api/prompt", s.handlePrompt)

	// Static files under /board/
	boardFS, err := fs.Sub(EmbeddedFS, "dist")
	hasFrontend := err == nil
	if hasFrontend {
		// Verify the frontend actually exists by checking for index.html
		if f, ferr := boardFS.Open("index.html"); ferr != nil {
			hasFrontend = false
		} else {
			f.Close()
		}
	}
	if !hasFrontend {
		// No frontend built — serve a placeholder page.
		mux.HandleFunc("/board/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprint(w, `<!DOCTYPE html><html><body><h1>Avenor Board</h1><p>Frontend not built. Build with <code>mise run go-build-full</code> or <code>go build -tags board ./cmd/avenor</code>.</p></body></html>`)
		})
		mux.HandleFunc("/board", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/board/", http.StatusMovedPermanently)
		})
	} else {
	mux.HandleFunc("/board/", func(w http.ResponseWriter, r *http.Request) {
		// Trim the /board/ prefix to get the file path within the embedded FS
		name := strings.TrimPrefix(r.URL.Path, "/board/")
		if name == "" {
			name = "index.html"
		}
		// Try to serve the file directly
		f, err := boardFS.Open(name)
		if err != nil {
			// File not found — SPA fallback: serve index.html
			f, err = boardFS.Open("index.html")
			if err != nil {
				http.NotFound(w, r)
				return
			}
			name = "index.html"
		}
		defer f.Close()

		stat, err := f.Stat()
		if err != nil {
			http.NotFound(w, r)
			return
		}

		// Set content type based on file extension
		ext := strings.ToLower(filepath.Ext(name))
		switch ext {
		case ".html":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
		case ".js":
			w.Header().Set("Content-Type", "application/javascript")
		case ".css":
			w.Header().Set("Content-Type", "text/css")
		case ".json":
			w.Header().Set("Content-Type", "application/json")
		case ".svg":
			w.Header().Set("Content-Type", "image/svg+xml")
		case ".png":
			w.Header().Set("Content-Type", "image/png")
		case ".ico":
			w.Header().Set("Content-Type", "image/x-icon")
		default:
			w.Header().Set("Content-Type", "application/octet-stream")
		}

		http.ServeContent(w, r, name, stat.ModTime(), f.(io.ReadSeeker))
	})
	mux.HandleFunc("/board", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/board/", http.StatusMovedPermanently)
	})
	} // end else (boardFS available)

	// Redirect root to /board/
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/board/", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})

	s.server = &http.Server{Addr: resolved, Handler: mux}
	return s, nil
}

// Start binds and starts serving in a goroutine.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	s.listener = ln
	s.server.Addr = ln.Addr().String()
	go func() { _ = s.server.Serve(ln) }()
	return nil
}

// Addr returns the actual bound address (e.g. "127.0.0.1:43210").
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.server.Addr
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// Token returns the auth token for display.
func (s *Server) Token() string { return s.token }

// PrintStartupInfo prints the URL, token, and auth-protected URL.
func (s *Server) PrintStartupInfo() {
	if s.cfg.NoAuth {
		fmt.Fprintf(os.Stdout, "avenor board: http://%s/board/ (no auth)\n", s.Addr())
		return
	}
	fmt.Fprintf(os.Stdout, "avenor board: http://%s/board/\n", s.Addr())
	if s.cfg.Token != "" {
		// Explicit token — print both the token and a URL that auto-auths
		fmt.Fprintf(os.Stderr, "avenor board: token: %s\n", s.cfg.Token)
		fmt.Fprintf(os.Stdout, "avenor board: auth URL: http://%s/board/?token=%s\n", s.Addr(), s.cfg.Token)
	} else if isTTY() {
		fmt.Fprintf(os.Stderr, "avenor board: token: %s\n", s.token)
	}
}

func isTTY() bool {
	stat, err := os.Stderr.Stat()
	return err == nil && (stat.Mode()&os.ModeCharDevice) != 0
}

// ── Address resolution ──────────────────────────────────────────────────────

// resolveAddr normalises the address and resolves hostnames to IPs.
func resolveAddr(addr string) (string, error) {
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("avenor board: invalid address %q: %w", addr, err)
	}
	if port == "" {
		return "", fmt.Errorf("avenor board: address %q must include a port", addr)
	}

	// Numeric IP fast path
	if ip := net.ParseIP(host); ip != nil {
		return net.JoinHostPort(host, port), nil
	}

	// localhost short-circuit
	if strings.EqualFold(host, "localhost") {
		return net.JoinHostPort("127.0.0.1", port), nil
	}

	// Hostname resolution
	resolved, err := net.LookupIP(host)
	if err != nil {
		return "", fmt.Errorf("avenor board: cannot resolve %q: %w", host, err)
	}
	if len(resolved) == 0 {
		return "", fmt.Errorf("avenor board: %q resolved to no addresses", host)
	}
	// Prefer 127.0.0.1 when available
	chosen := resolved[0]
	for _, ip := range resolved {
		if ip4 := ip.To4(); ip4 != nil && ip4.Equal(net.ParseIP("127.0.0.1")) {
			chosen = ip
			break
		}
	}
	return net.JoinHostPort(chosen.String(), port), nil
}

// mustBeLoopback validates that the address binds to a loopback interface.
// Used when no token is provided.
func mustBeLoopback(addr string) error {
	normalised := addr
	if strings.HasPrefix(addr, ":") {
		normalised = "127.0.0.1" + addr
	}
	host, _, err := net.SplitHostPort(normalised)
	if err != nil {
		return fmt.Errorf("avenor board: invalid address %q: %w", addr, err)
	}
	if ip := net.ParseIP(host); ip != nil {
		if !ip.IsLoopback() {
			return fmt.Errorf("avenor board: address %q binds to a non-loopback interface; use --token to enable remote access", addr)
		}
		return nil
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	resolved, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("avenor board: cannot resolve %q: %w", host, err)
	}
	for _, ip := range resolved {
		if !ip.IsLoopback() {
			return fmt.Errorf("avenor board: hostname %q resolves to non-loopback %s; use --token to enable remote access", host, ip)
		}
	}
	return nil
}

// ── Auth helpers ────────────────────────────────────────────────────────────

// checkAuth verifies the Authorization: Bearer <token> header.
// Returns true when auth is not required (NoAuth or no token set).
func (s *Server) checkAuth(r *http.Request) bool {
	if s.cfg.NoAuth {
		return true
	}
	if s.cfg.Token == "" {
		return true
	}
	hdr := r.Header.Get("Authorization")
	if !strings.HasPrefix(hdr, "Bearer ") {
		return false
	}
	token := strings.TrimPrefix(hdr, "Bearer ")
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.Token)) == 1
}

// checkAuthEventSource verifies Authorization header or ?token= query param.
// The query-param fallback exists for EventSource clients that cannot set headers.
func (s *Server) checkAuthEventSource(r *http.Request) bool {
	if s.cfg.NoAuth {
		return true
	}
	if s.checkAuth(r) {
		return true
	}
	if s.cfg.Token == "" {
		return true
	}
	q := r.URL.Query().Get("token")
	return q != "" && subtle.ConstantTimeCompare([]byte(q), []byte(s.cfg.Token)) == 1
}

// ── API handlers ────────────────────────────────────────────────────────────

func (s *Server) handleSockets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var results []socketEntry

	if s.cfg.Socket != "" {
		entry := dialOne(s.cfg.Socket)
		if entry != nil {
			results = append(results, *entry)
		}
	} else {
		dirs := s.cfg.ScanDirs
		if len(dirs) == 0 {
			home, _ := os.UserHomeDir()
			dirs = []string{filepath.Join(home, ".avenor", "sockets")}
		}
		for _, dir := range dirs {
			dir = ResolveHome(dir)
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				if !strings.HasSuffix(e.Name(), ".sock") {
					continue
				}
				path := filepath.Join(dir, e.Name())
				entry := dialOne(path)
				if entry != nil {
					results = append(results, *entry)
				}
			}
		}
	}

	writeJSON(w, results)
}

type socketEntry struct {
	Path   string         `json:"path"`
	Type   string         `json:"type"` // "one-shot" or "stable"
	PID    int            `json:"pid"`
	Status map[string]any `json:"status,omitempty"`
}

// dialOne attempts to connect to a Unix socket, query its status, and determine
// if it's a one-shot or stable supervisor. Returns nil if the socket can't be
// reached (stale).
func dialOne(path string) *socketEntry {
	c, err := client.Dial(path)
	if err != nil {
		return nil // stale socket
	}

	// Determine type: try List() to see if it's a stable supervisor
	var sockType string
	list, listErr := c.List()
	if listErr == nil && len(list) > 0 {
		sockType = "stable"
	} else {
		sockType = "one-shot"
	}

	// Query status
	status, statusErr := c.Status("")

	_ = c.Close() // Connection close is best-effort; errors are not actionable.

	if statusErr != nil {
		// Socket is reachable but status failed — still report it
		return &socketEntry{
			Path: path,
			Type: sockType,
		}
	}

	// Extract pid if present in status
	pid := 0
	if pidVal, ok := status["pid"]; ok {
		switch v := pidVal.(type) {
		case float64:
			pid = int(v)
		case int:
			pid = v
		}
	}

	return &socketEntry{
		Path:   path,
		Type:   sockType,
		PID:    pid,
		Status: status,
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	socketPath := r.URL.Query().Get("socket")
	if socketPath == "" {
		http.Error(w, "socket query parameter required", http.StatusBadRequest)
		return
	}

	c, err := client.Dial(socketPath)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	defer c.Close()

	status, err := c.Status("")
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, status)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	socketPath := r.URL.Query().Get("socket")
	if socketPath == "" {
		http.Error(w, "socket query parameter required", http.StatusBadRequest)
		return
	}

	c, err := client.Dial(socketPath)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	defer c.Close()

	list, err := c.List()
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, list)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.checkAuthEventSource(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	socketPath := r.URL.Query().Get("socket")
	if socketPath == "" {
		http.Error(w, "socket query parameter required", http.StatusBadRequest)
		return
	}

	runtimeID := r.URL.Query().Get("runtime_id")

	c, err := client.Dial(socketPath)
	if err != nil {
		http.Error(w, "failed to connect to socket: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer c.Close()

	// Subscribe for events
	var subResult struct {
		Subscribed bool `json:"subscribed"`
	}
	if err := c.Call("subscribe", nil, &subResult); err != nil {
		http.Error(w, "subscribe failed: "+err.Error(), http.StatusBadGateway)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		select {
		case ev, ok := <-c.Events():
			if !ok {
				return
			}
			if runtimeID != "" {
				if ev.RuntimeID != runtimeID && ev.SessionID != runtimeID {
					continue
				}
			}
			data, err := json.Marshal(ev.Raw)
			if err != nil {
				continue
			}
			// SSE spec: data fields must not contain literal newlines.
			fmt.Fprintf(w, "event: event\ndata: %s\n\n", strings.ReplaceAll(string(data), "\n", "\\n"))
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// No auth required — the UI needs to know whether to show the token prompt
	writeJSON(w, map[string]any{
		"mutations_allowed": s.cfg.AllowMutations,
		"auth_required":     s.cfg.Token != "" && !s.cfg.NoAuth,
	})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.cfg.AllowMutations {
		http.Error(w, "mutations not allowed", http.StatusForbidden)
		return
	}
	if !s.checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var body struct {
		Socket    string `json:"socket"`
		RuntimeID string `json:"runtime_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.Socket == "" {
		http.Error(w, "socket is required", http.StatusBadRequest)
		return
	}

	c, err := client.Dial(body.Socket)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	defer c.Close()

	if err := c.Cancel(body.RuntimeID); err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func (s *Server) handlePrompt(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.cfg.AllowMutations {
		http.Error(w, "mutations not allowed", http.StatusForbidden)
		return
	}
	if !s.checkAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var body struct {
		Socket    string `json:"socket"`
		Text      string `json:"text"`
		RuntimeID string `json:"runtime_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.Socket == "" {
		http.Error(w, "socket is required", http.StatusBadRequest)
		return
	}
	if body.Text == "" {
		http.Error(w, "text is required", http.StatusBadRequest)
		return
	}

	c, err := client.Dial(body.Socket)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	defer c.Close()

	if err := c.Prompt(body.RuntimeID, body.Text); err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

// ── Helpers ─────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Encode errors on a response writer are rare (e.g. client disconnected).
		// Not worth surfacing — the caller already sent status code/headers above.
	}
}

func writeError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

// ResolveHome expands a leading "~/" to the user's home directory.
func ResolveHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, path[2:])
	}
	return path
}