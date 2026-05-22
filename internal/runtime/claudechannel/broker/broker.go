// Package broker implements the in-process HTTP broker for the claude-channel backend.
// It handles sidecar registration, control message queuing, report/finish/reply ingestion,
// and heartbeat tracking scoped by run ID.
package broker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ControlMessage is a message pushed from Avenor to the sidecar for delivery to Claude.
type ControlMessage struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	RunID     string          `json:"run_id"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// Report is the payload from an avenor_report tool call.
type Report struct {
	RunID   string          `json:"run_id"`
	State   string          `json:"state"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Finish is the payload from an avenor_finish tool call.
type Finish struct {
	RunID        string          `json:"run_id"`
	Status       string          `json:"status"`
	Summary      string          `json:"summary"`
	FilesChanged []string        `json:"files_changed,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
}

// Reply is the payload from an avenor_reply tool call.
type Reply struct {
	RunID   string          `json:"run_id"`
	To      string          `json:"to"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// PermissionState holds a pending permission relay request.
type PermissionState struct {
	RequestID string
	ToolName  string
	Desc      string
	Preview   string
	CreatedAt time.Time
}

// RunState holds all per-run broker state.
type RunState struct {
	RunID               string
	Token               string
	ControlQueue        []*ControlMessage
	LastSeen            time.Time
	Reports             []Report
	Finishes            []Finish
	Replies             []Reply
	PermissionRequests  map[string]*PermissionState
	PermissionDecisions map[string]string // requestID -> "allow" or "deny"
	Mu                  sync.Mutex
	Cond                *sync.Cond
}

func (st *RunState) Lock()   { st.Mu.Lock() }
func (st *RunState) Unlock() { st.Mu.Unlock() }

// PushControl queues a control message for the given run.
func (b *Broker) PushControl(runID string, msg ControlMessage) error {
	b.mu.RLock()
	st, ok := b.runs[runID]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("run not found: %s", runID)
	}
	st.Mu.Lock()
	msg.CreatedAt = time.Now()
	st.ControlQueue = append(st.ControlQueue, &msg)
	st.Cond.Broadcast()
	st.Mu.Unlock()
	return nil
}

// Broker is the in-process HTTP server for channel sidecar coordination.
type Broker struct {
	addr      string
	listener  net.Listener
	mu        sync.RWMutex
	runs      map[string]*RunState
	server    *http.Server
	httpToken string // optional global HTTP auth token for push-control endpoint
}

// New creates a broker on an ephemeral loopback port.
// The resulting addr is available after Start.
func New(globalToken string) *Broker {
	return &Broker{
		runs:      make(map[string]*RunState),
		httpToken: globalToken,
	}
}

func MakeToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Start binds the broker to an ephemeral loopback port.
func (b *Broker) Start() error {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	b.mu.Lock()
	b.listener = l
	b.mu.Unlock()

	router := http.NewServeMux()
	router.HandleFunc("/health", b.handleHealth)
	router.HandleFunc("/register", b.withMethod("POST", b.handleRegister))
	router.HandleFunc("/heartbeat", b.withMethod("POST", b.withAuth(b.handleHeartbeat)))
	router.HandleFunc("/push-control", b.withMethod("POST", b.withAuth(b.handlePushControl)))
	router.HandleFunc("/poll-control", b.withMethod("POST", b.withAuth(b.handlePollControl)))
	router.HandleFunc("/report", b.withMethod("POST", b.withAuth(b.handleReport)))
	router.HandleFunc("/finish", b.withMethod("POST", b.withAuth(b.handleFinish)))
	router.HandleFunc("/reply", b.withMethod("POST", b.withAuth(b.handleReply)))
	router.HandleFunc("/permission", b.withMethod("POST", b.withAuth(b.handlePermission)))

	b.server = &http.Server{
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	go func() {
		_ = b.server.Serve(l)
	}()
	return nil
}

func (b *Broker) Addr() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.listener == nil {
		return ""
	}
	return b.listener.Addr().String()
}

func (b *Broker) Stop() error {
	if b.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return b.server.Shutdown(ctx)
	}
	return nil
}

// --- HTTP helpers ---

func (b *Broker) withMethod(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		next(w, r)
	}
}

func (b *Broker) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check global HTTP token for push-control
		if b.httpToken != "" && r.URL.Path == "/push-control" {
			auth := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if !strings.HasPrefix(strings.ToLower(auth), strings.ToLower(prefix)) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			got := strings.TrimSpace(auth[len(prefix):])
			if subtle.ConstantTimeCompare([]byte(got), []byte(b.httpToken)) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next(w, r)
			return
		}
		// Sidecar auth: token + run_id in JSON body.
		// Read body into memory so it can be consumed again by the handler.
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var cred struct {
			RunID string `json:"run_id"`
			Token string `json:"token"`
		}
		if err := json.Unmarshal(bodyBytes, &cred); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if cred.RunID == "" || cred.Token == "" {
			http.Error(w, "unauthorized: missing run_id or token", http.StatusUnauthorized)
			return
		}
		b.mu.RLock()
		st, ok := b.runs[cred.RunID]
		b.mu.RUnlock()
		if !ok || subtle.ConstantTimeCompare([]byte(cred.Token), []byte(st.Token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// Replace request body with a fresh reader containing the same bytes.
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		// Update ContentLength to reflect the actual body length for downstream decoders.
		if r.ContentLength > 0 {
			r.ContentLength = int64(len(bodyBytes))
		}
		next(w, r)
	}
}

// --- handlers ---

func (b *Broker) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (b *Broker) handleRegister(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID string `json:"run_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if body.RunID == "" {
		http.Error(w, "missing run_id", http.StatusBadRequest)
		return
	}

	b.mu.Lock()
	if _, exists := b.runs[body.RunID]; exists {
		b.mu.Unlock()
		http.Error(w, "run already registered", http.StatusConflict)
		return
	}
	st := &RunState{
		RunID:               body.RunID,
		Token:               MakeToken(),
		LastSeen:            time.Now(),
		PermissionRequests:  make(map[string]*PermissionState),
		PermissionDecisions: make(map[string]string),
	}
	st.Cond = sync.NewCond(&st.Mu)
	b.runs[body.RunID] = st
	b.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"token": st.Token,
		"run_id": st.RunID,
	})
}

func (b *Broker) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID string `json:"run_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.runs[body.RunID]
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	st.Mu.Lock()
	st.LastSeen = time.Now()
	st.Mu.Unlock()
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (b *Broker) handlePushControl(w http.ResponseWriter, r *http.Request) {
	var msg ControlMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if msg.RunID == "" || msg.ID == "" || msg.Type == "" {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}

	b.mu.Lock()
	st, ok := b.runs[msg.RunID]
	b.mu.Unlock()
	if !ok {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	st.Mu.Lock()
	msg.CreatedAt = time.Now()
	st.ControlQueue = append(st.ControlQueue, &msg)
	st.Cond.Broadcast()
	st.Mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"queued": true})
}

func (b *Broker) handlePollControl(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID string `json:"run_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	b.mu.Lock()
	st, ok := b.runs[body.RunID]
	b.mu.Unlock()
	if !ok {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	st.Mu.Lock()
	if len(st.ControlQueue) == 0 {
		// Wait up to 2 seconds for new control messages
		waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		go func() {
			<-waitCtx.Done()
			st.Cond.Broadcast()
		}()
		st.Cond.Wait()
		cancel()
	}
	msgs := st.ControlQueue
	st.ControlQueue = nil
	st.Mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(msgs)
}

func (b *Broker) handleReport(w http.ResponseWriter, r *http.Request) {
	var rep Report
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	b.ingest(w, rep.RunID, func(st *RunState) { st.Reports = append(st.Reports, rep) })
}

func (b *Broker) handleFinish(w http.ResponseWriter, r *http.Request) {
	var fin Finish
	if err := json.NewDecoder(r.Body).Decode(&fin); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	b.ingest(w, fin.RunID, func(st *RunState) { st.Finishes = append(st.Finishes, fin) })
}

func (b *Broker) handleReply(w http.ResponseWriter, r *http.Request) {
	var rep Reply
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	b.ingest(w, rep.RunID, func(st *RunState) { st.Replies = append(st.Replies, rep) })
}

func (b *Broker) handlePermission(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID     string `json:"run_id"`
		RequestID string `json:"request_id"`
		Behavior  string `json:"behavior"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	b.ingest(w, body.RunID, func(st *RunState) {
		st.PermissionDecisions[body.RequestID] = body.Behavior
	})
}

func (b *Broker) ingest(w http.ResponseWriter, runID string, fn func(*RunState)) {
	b.mu.Lock()
	st, ok := b.runs[runID]
	b.mu.Unlock()
	if !ok {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	st.Mu.Lock()
	fn(st)
	st.LastSeen = time.Now()
	st.Mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// --- accessors ---

// GetRun returns the RunState for a given run ID, or nil if not found.
func (b *Broker) GetRun(runID string) *RunState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.runs[runID]
}

// CreateRun registers a new run and returns its token.
func (b *Broker) CreateRun(runID string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.runs[runID]; exists {
		return "", fmt.Errorf("run already registered")
	}
	st := &RunState{
		RunID:               runID,
		Token:               MakeToken(),
		LastSeen:            time.Now(),
		PermissionRequests:  make(map[string]*PermissionState),
		PermissionDecisions: make(map[string]string),
	}
	st.Cond = sync.NewCond(&st.Mu)
	b.runs[runID] = st
	return st.Token, nil
}

// Reset clears all run state.
func (b *Broker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.runs = make(map[string]*RunState)
}
