// Package broker provides a harness-agnostic run broker for agent communication.
// It handles run registration, control message queuing, report/finish/reply ingestion,
// and heartbeat tracking scoped by run ID.
//
// Harness-specific concerns (e.g., MCP sidecar registration, tmux bootstrap, .mcp.json entries)
// live in the individual harness adapter packages, not here.
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

// Envelope is the internal generic message format used by the broker.
// It is not exposed on the wire; HTTP endpoints continue to use the
// existing Report/Finish/Reply/ControlMessage types.
type Envelope struct {
	FromRunID     string          `json:"from_run_id"`
	ToRunID       string          `json:"to_run_id"`
	To            string          `json:"to,omitempty"`
	Kind          string          `json:"kind"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

func (r Report) ToEnvelope() Envelope {
	return Envelope{
		FromRunID: r.RunID,
		Kind:      r.State,
		Payload:   r.Payload,
	}
}

func (f Finish) ToEnvelope() Envelope {
	kind := f.Status
	if kind == "" {
		kind = "done"
	}
	return Envelope{
		FromRunID: f.RunID,
		Kind:      kind,
		Payload:   f.Payload,
	}
}

func (r Reply) ToEnvelope() Envelope {
	return Envelope{
		FromRunID: r.RunID,
		To:        r.To,
		Payload:   r.Payload,
	}
}

func (m ControlMessage) ToEnvelope() Envelope {
	return Envelope{
		ToRunID:       m.RunID,
		Kind:          m.Type,
		CorrelationID: m.ID,
		Payload:       m.Payload,
		CreatedAt:     m.CreatedAt,
	}
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
	RegisteredAt        time.Time
	LastSeen            time.Time
	Reports             []Report
	Finishes            []Finish
	Replies             []Reply
	PermissionRequests  map[string]*PermissionState
	PermissionDecisions map[string]string // requestID -> "allow" or "deny"
	Mu                  sync.Mutex
	Notify              chan struct{}
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
	st.signalLocked()
	st.Mu.Unlock()
	return nil
}

// Send enqueues a control message for the given run using raw fields.
// It is a convenience wrapper around PushControl that builds the
// ControlMessage internally. correlationID is optional; it is used as
// the ControlMessage ID when non-empty.
func (b *Broker) Send(runID string, kind string, payload json.RawMessage, correlationID string) error {
	if correlationID == "" {
		correlationID = MakeToken()
	}
	msg := ControlMessage{
		ID:      correlationID,
		Type:    kind,
		RunID:   runID,
		Payload: payload,
	}
	return b.PushControl(runID, msg)
}

// Ingest routes an incoming message from an agent into the appropriate
// RunState bucket based on kind.
//
// Known kinds:
//   - "done", "failed", "blocked" → stored in Finishes
//   - everything else            → stored in Reports
func (b *Broker) Ingest(runID string, kind string, payload json.RawMessage) error {
	if payload == nil {
		return fmt.Errorf("payload must not be nil")
	}
	b.mu.Lock()
	st, ok := b.runs[runID]
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("run not found: %s", runID)
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()

	switch kind {
	case "done", "failed", "blocked":
		st.Finishes = append(st.Finishes, Finish{
			RunID:   runID,
			Status:  kind,
			Payload: payload,
		})
	default:
		st.Reports = append(st.Reports, Report{
			RunID:   runID,
			State:   kind,
			Payload: payload,
		})
	}
	st.LastSeen = time.Now()
	return nil
}

// signalLocked sends a non-blocking signal on st.Notify to wake a
// waiting poller. Because the channel is buffered to 1, stale signals
// may sit unread — the consumer must drain Notify after each poll
// to avoid a spurious immediate return when the queue is actually empty.
func (st *RunState) signalLocked() {
	// Non-blocking send means stale signals can be lost if the consumer
	// doesn't drain Notify fast enough. This is intentional — it ensures
	// poll-control never blocks longer than necessary, but means a signal
	// arriving before the consumer drains the channel may be silently dropped.
	select {
	case st.Notify <- struct{}{}:
	default:
	}
}

// Broker is a harness-agnostic, in-process HTTP server that coordinates
// communication between agent harnesses and their orchestrators.
// It owns run-scoped state, message queuing, and lifecycle event ingestion.
type Broker struct {
	addr        string
	listener    net.Listener
	mu          sync.RWMutex
	runs        map[string]*RunState
	server      *http.Server
	httpToken   string        // optional global HTTP auth token for push-control endpoint
	pollTimeout time.Duration // max wait for poll-control before returning empty
}

// New creates a broker on an ephemeral loopback port.
// The resulting addr is available after Start.
func New(globalToken string) *Broker {
	return &Broker{
		runs:        make(map[string]*RunState),
		httpToken:   globalToken,
		pollTimeout: 2 * time.Second,
	}
}

func MakeToken() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
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
	router.HandleFunc("/permission_request", b.withMethod("POST", b.withAuth(b.handlePermissionRequest)))
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
			if !strings.HasPrefix(auth, prefix) {
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
		const maxBody = 1 << 20 // 1 MiB
		limited := io.LimitReader(r.Body, maxBody+1)
		bodyBytes, err := io.ReadAll(limited)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if len(bodyBytes) > maxBody {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
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
		Token string `json:"token"`
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
	if st, exists := b.runs[body.RunID]; exists {
		if body.Token == "" || subtle.ConstantTimeCompare([]byte(body.Token), []byte(st.Token)) != 1 {
			b.mu.Unlock()
			http.Error(w, "run already registered", http.StatusConflict)
			return
		}
		st.Mu.Lock()
		st.RegisteredAt = time.Now()
		st.LastSeen = time.Now()
		st.Mu.Unlock()
		b.mu.Unlock()
		resp := map[string]string{
			"token":  st.Token,
			"run_id": st.RunID,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	token := body.Token
	if token == "" {
		token = MakeToken()
	}
	st := &RunState{
		RunID:               body.RunID,
		Token:               token,
		RegisteredAt:        time.Now(),
		LastSeen:            time.Now(),
		Notify:              make(chan struct{}, 1),
		PermissionRequests:  make(map[string]*PermissionState),
		PermissionDecisions: make(map[string]string),
	}
	b.runs[body.RunID] = st
	b.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"token":  st.Token,
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
	st.signalLocked()
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
		notify := st.Notify
		st.Mu.Unlock()
		select {
		case <-notify:
		case <-time.After(b.pollTimeout):
		case <-r.Context().Done():
			return
		}
		st.Mu.Lock()
	}
	msgs := st.ControlQueue
	st.ControlQueue = nil
	if len(msgs) > 0 {
		select {
		case <-st.Notify:
		default:
		}
	}
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

func (b *Broker) handlePermissionRequest(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID     string `json:"run_id"`
		RequestID string `json:"request_id"`
		ToolName  string `json:"tool_name"`
		Desc      string `json:"description"`
		Preview   string `json:"input_preview"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	b.ingest(w, body.RunID, func(st *RunState) {
		st.PermissionRequests[body.RequestID] = &PermissionState{
			RequestID: body.RequestID,
			ToolName:  body.ToolName,
			Desc:      body.Desc,
			Preview:   body.Preview,
			CreatedAt: time.Now(),
		}
	})
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
		Notify:              make(chan struct{}, 1),
		PermissionRequests:  make(map[string]*PermissionState),
		PermissionDecisions: make(map[string]string),
	}
	b.runs[runID] = st
	return st.Token, nil
}

// DeleteRun removes a run from the broker. Safe to call even if the run
// does not exist.
func (b *Broker) DeleteRun(runID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.runs, runID)
}

// Reset clears all run state.
func (b *Broker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.runs = make(map[string]*RunState)
}
