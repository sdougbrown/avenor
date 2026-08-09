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

	"github.com/sdougbrown/avenor/internal/channelwrap"
)

// ControlMessage is a message pushed from Avenor to the sidecar for delivery to Claude.
type ControlMessage struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	RunID     string          `json:"run_id"`
	FromRunID string          `json:"from_run_id,omitempty"`
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

// AskEdge tracks a pending ask (blocking request for reply).
type AskEdge struct {
	FromRunID string    `json:"from_run_id"`
	ToRunID   string    `json:"to_run_id"`
	MessageID string    `json:"message_id"`
	CreatedAt time.Time `json:"created_at"`
}

// AskReply carries a reply routed to a waiting sender.
type AskReply struct {
	MessageID string          `json:"message_id"`
	FromRunID string          `json:"from_run_id"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// SessionInfo describes a registered run for peer discovery.
type SessionInfo struct {
	RunID    string `json:"run_id"`
	Label    string `json:"label,omitempty"`
	Backend  string `json:"backend,omitempty"`
	Model    string `json:"model,omitempty"`
	Dir      string `json:"dir,omitempty"`
	Status   string `json:"status,omitempty"`
	LastSeen int64  `json:"last_seen"`
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

	// Ask/reply tracking
	PendingAsks  map[string]*AskEdge      // messageID -> edge (sender is waiting)
	WaitingReply map[string]chan AskReply // messageID -> buffered(1) channel for /wait_reply

	// Session metadata for peer discovery
	Info *SessionInfo
}

const (
	// DefaultAskTimeout is how long a sender waits for a reply before
	// the ask edge is pruned and the waiter unblocks with a timeout.
	DefaultAskTimeout = 10 * time.Minute

	// askEdgePruneInterval is how often the broker checks for expired ask edges.
	askEdgePruneInterval = 30 * time.Second
)

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

// SendTo sends a message from one run to another run's control queue.
// The fromRunID must belong to a registered run whose token matches the
// authenticated caller's token.
func (b *Broker) SendTo(fromRunID, toRunID, kind string, payload json.RawMessage, correlationID string) error {
	b.mu.RLock()
	_, ok := b.runs[toRunID]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("run not found: %s", toRunID)
	}
	if correlationID == "" {
		correlationID = MakeToken()
	}
	msg := ControlMessage{
		ID:        correlationID,
		Type:      kind,
		RunID:     toRunID,
		FromRunID: fromRunID,
		Payload:   payload,
	}
	return b.PushControl(toRunID, msg)
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

	// Global ask-edge registry for cross-run lookups.
	// Keys are "senderRunID/messageID" to prevent cross-sender collisions.
	globalAskEdgesMu    sync.RWMutex
	globalAskEdges      map[string]*AskEdge      // "senderRunID/messageID" -> edge
	globalReplyChannels map[string]chan AskReply // "sender/messageID" -> reply channel

	// Shutdown signal for background goroutines.
	closeOnce sync.Once
	closeCh   chan struct{}
}

// New creates a broker on an ephemeral loopback port.
// The resulting addr is available after Start.
func New(globalToken string) *Broker {
	b := &Broker{
		runs:                make(map[string]*RunState),
		httpToken:           globalToken,
		pollTimeout:         2 * time.Second,
		globalAskEdges:      make(map[string]*AskEdge),
		globalReplyChannels: make(map[string]chan AskReply),
		closeCh:             make(chan struct{}),
	}
	return b
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
	router.HandleFunc("/send", b.withMethod("POST", b.withAuth(b.handleSend)))
	router.HandleFunc("/wait_reply", b.withMethod("POST", b.withAuth(b.handleWaitReply)))
	router.HandleFunc("/cancel_message", b.withMethod("POST", b.withAuth(b.handleCancelMessage)))
	router.HandleFunc("/sessions", b.withMethod("GET", b.withAuth(b.handleSessions)))
	router.HandleFunc("/permission_request", b.withMethod("POST", b.withAuth(b.handlePermissionRequest)))
	router.HandleFunc("/permission", b.withMethod("POST", b.withAuth(b.handlePermission)))

	b.server = &http.Server{
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 30 * time.Second, // /wait_reply long-poll handled by handler's time.After (DefaultAskTimeout)
	}
	go func() {
		_ = b.server.Serve(l)
	}()

	// Start periodic ask-edge cleanup
	go b.pruneAskEdgesLoop()

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
	b.closeOnce.Do(func() {
		close(b.closeCh)
	})
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
		// Auth: accept credentials in JSON body (POST) or query parameters (GET).
		var cred struct {
			RunID string `json:"run_id"`
			Token string `json:"token"`
		}
		if r.Method == http.MethodGet {
			cred.RunID = r.URL.Query().Get("run_id")
			cred.Token = r.URL.Query().Get("token")
		} else {
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
			if err := json.Unmarshal(bodyBytes, &cred); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			// Replace request body with a fresh reader containing the same bytes.
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			if r.ContentLength > 0 {
				r.ContentLength = int64(len(bodyBytes))
			}
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
		PendingAsks:         make(map[string]*AskEdge),
		WaitingReply:        make(map[string]chan AskReply),
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

// handleSend routes a message from one run to another. It supports
// fire-and-forget (no special fields), ask (expects_reply=true), and
// reply (reply_to is set).
func (b *Broker) handleSend(w http.ResponseWriter, r *http.Request) {
	// withAuth already read and replaced the body. Parse the full body
	// once to extract both credentials and send fields.
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var fullBody struct {
		RunID     string          `json:"run_id"`
		Token     string          `json:"token"`
		FromRunID string          `json:"from_run_id"`
		ToRunID   string          `json:"to_run_id"`
		Type      string          `json:"type"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(bodyBytes, &fullBody); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if fullBody.FromRunID == "" || fullBody.ToRunID == "" || fullBody.Type == "" {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}
	// Validate that the authenticated caller matches from_run_id's token.
	b.mu.RLock()
	fromSt, ok := b.runs[fullBody.FromRunID]
	b.mu.RUnlock()
	if !ok {
		http.Error(w, "from run not found", http.StatusNotFound)
		return
	}
	if subtle.ConstantTimeCompare([]byte(fullBody.Token), []byte(fromSt.Token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Parse the agent_message payload to inspect ask/reply fields.
	msgID := ""
	replyTo := ""
	expectsReply := false
	if fullBody.Type == "agent_message" && len(fullBody.Payload) > 0 {
		var am AgentMessage
		if err := json.Unmarshal(fullBody.Payload, &am); err == nil {
			msgID = am.ID
			replyTo = am.ReplyTo
			expectsReply = am.ExpectsReply
		}
	}

	if expectsReply {
		// Sender expects a reply -- register a pending ask edge.
		// The edge is stored on the SENDER's (fromSt) RunState so that
		// wait_reply and cancel_message (both called by the sender) can
		// find it without a global lookup.
		if msgID == "" {
			http.Error(w, "expects_reply requires a message id", http.StatusBadRequest)
			return
		}

		sender := fullBody.FromRunID
		target := fullBody.ToRunID

		b.mu.RLock()
		_, toOk := b.runs[target]
		b.mu.RUnlock()
		if !toOk {
			http.Error(w, "to run not found", http.StatusNotFound)
			return
		}

		// Mutual-ask guard and edge registration are performed atomically
		// under the global registry lock. This closes the TOCTOU window:
		// two concurrent asks in opposite directions (A→B and B→A) serialize
		// here, so whichever registers first is visible to the other, which
		// then refuses with a mutual-ask conflict.
		key := edgeKey(sender, msgID)
		b.globalAskEdgesMu.Lock()
		if _, dup := b.globalAskEdges[key]; dup {
			b.globalAskEdgesMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "message id already in use"})
			return
		}
		// Refuse this ask if the target is already waiting on a pending ask
		// from the sender (i.e. the target has an edge pointing back at us).
		for _, e := range b.globalAskEdges {
			if e.FromRunID == target && e.ToRunID == sender {
				b.globalAskEdgesMu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "mutual ask refused"})
				return
			}
		}
		edge := &AskEdge{
			FromRunID: sender,
			ToRunID:   target,
			MessageID: msgID,
			CreatedAt: time.Now(),
		}
		replyCh := make(chan AskReply, 1)
		b.globalAskEdges[key] = edge
		b.globalReplyChannels[key] = replyCh
		b.globalAskEdgesMu.Unlock()

		// Mirror the edge on the SENDER's RunState so wait_reply and
		// cancel_message can find it without a global lookup.
		if fromSt.PendingAsks == nil {
			fromSt.PendingAsks = make(map[string]*AskEdge)
		}
		if fromSt.WaitingReply == nil {
			fromSt.WaitingReply = make(map[string]chan AskReply)
		}
		fromSt.Mu.Lock()
		fromSt.PendingAsks[msgID] = edge
		fromSt.WaitingReply[msgID] = replyCh
		fromSt.Mu.Unlock()

		// Deliver the message to the target's control queue as usual.
		err = b.SendTo(fullBody.FromRunID, fullBody.ToRunID, fullBody.Type, fullBody.Payload, "")
		if err != nil {
			// Roll back ask edge on delivery failure.
			b.globalAskEdgesMu.Lock()
			delete(b.globalAskEdges, key)
			delete(b.globalReplyChannels, key)
			b.globalAskEdgesMu.Unlock()
			fromSt.Mu.Lock()
			delete(fromSt.PendingAsks, msgID)
			delete(fromSt.WaitingReply, msgID)
			fromSt.Mu.Unlock()
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"queued": true, "message_id": msgID, "expects_reply": true})
		return
	}

	if replyTo != "" {
		// This is a reply to a previous ask. The asker is the ToRunID
		// (the reply's destination). Look up the channel from the
		// global registry using the namespaced key.
		askerKey := edgeKey(fullBody.ToRunID, replyTo)
		b.globalAskEdgesMu.RLock()
		edge, edgeOk := b.globalAskEdges[askerKey]
		replyCh, chOk := b.globalReplyChannels[askerKey]
		b.globalAskEdgesMu.RUnlock()

		if !edgeOk || !chOk {
			// Edge may have timed out; deliver as fire-and-forget instead.
			err = b.SendTo(fullBody.FromRunID, fullBody.ToRunID, fullBody.Type, fullBody.Payload, "")
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": true, "note": "reply_to edge not found, delivered as fire-and-forget"})
			return
		}

		// Authorization: only the intended ask target may reply.
		if edge.ToRunID != fullBody.FromRunID {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "not the target of this ask"})
			return
		}

		// Route the reply to the waiting sender's channel.
		// Do NOT clean up registries here — the wait_reply handler
		// will do that after receiving.
		select {
		case replyCh <- AskReply{
			MessageID: replyTo,
			FromRunID: fullBody.FromRunID,
			Payload:   fullBody.Payload,
		}:
		default:
			// Channel already has a value (timeout or cancel signal).
			// The waiter will get that signal instead; this reply
			// is dropped. This is an edge case that indicates a race
			// between the reply arriving and a timeout/cancel.
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"queued": true, "reply_to": replyTo})
		return
	}

	// Fire-and-forget: deliver to the target's control queue.
	err = b.SendTo(fullBody.FromRunID, fullBody.ToRunID, fullBody.Type, fullBody.Payload, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"queued": true})
}

// handleWaitReply long-polls for a reply to a previously sent ask.
// The caller must have previously sent a message with expects_reply=true
// via /send. The response blocks until a matching reply arrives or
// the ask timeout expires.
func (b *Broker) handleWaitReply(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var fullBody struct {
		RunID      string `json:"run_id"`
		Token      string `json:"token"`
		WaitingFor string `json:"waiting_for"`
	}
	if err := json.Unmarshal(bodyBytes, &fullBody); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if fullBody.WaitingFor == "" {
		http.Error(w, "missing waiting_for", http.StatusBadRequest)
		return
	}

	b.mu.RLock()
	_, ok := b.runs[fullBody.RunID]
	b.mu.RUnlock()
	if !ok {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	// Only the original sender can wait for a reply. Namespace the key
	// by sender run ID to prevent cross-session ask ID collisions.
	key := edgeKey(fullBody.RunID, fullBody.WaitingFor)
	b.globalAskEdgesMu.RLock()
	replyCh, chOk := b.globalReplyChannels[key]
	edge, edgeOk := b.globalAskEdges[key]
	b.globalAskEdgesMu.RUnlock()

	if !chOk || !edgeOk {
		http.Error(w, "no pending ask for this message id", http.StatusNotFound)
		return
	}

	// Authorization: only the sender of the original ask may wait for its reply.
	if edge.FromRunID != fullBody.RunID {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "not the sender of this ask"})
		return
	}

	// Wait for the reply with a timeout.
	var reply AskReply
	select {
	case reply = <-replyCh:
		// Got the reply. Clean up both global and per-run registries.
		b.globalAskEdgesMu.Lock()
		delete(b.globalAskEdges, key)
		delete(b.globalReplyChannels, key)
		b.globalAskEdgesMu.Unlock()
		if ownerSt := b.GetRun(edge.FromRunID); ownerSt != nil {
			ownerSt.Mu.Lock()
			delete(ownerSt.PendingAsks, fullBody.WaitingFor)
			delete(ownerSt.WaitingReply, fullBody.WaitingFor)
			ownerSt.Mu.Unlock()
		}
		respondWaitReply(w, reply, fullBody.WaitingFor)
	case <-time.After(DefaultAskTimeout):
		b.globalAskEdgesMu.Lock()
		delete(b.globalAskEdges, key)
		delete(b.globalReplyChannels, key)
		b.globalAskEdgesMu.Unlock()
		if ownerSt := b.GetRun(edge.FromRunID); ownerSt != nil {
			ownerSt.Mu.Lock()
			delete(ownerSt.PendingAsks, fullBody.WaitingFor)
			delete(ownerSt.WaitingReply, fullBody.WaitingFor)
			ownerSt.Mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"timeout":    true,
			"message_id": fullBody.WaitingFor,
		})
	case <-r.Context().Done():
		b.globalAskEdgesMu.Lock()
		delete(b.globalAskEdges, key)
		delete(b.globalReplyChannels, key)
		b.globalAskEdgesMu.Unlock()
		if ownerSt := b.GetRun(edge.FromRunID); ownerSt != nil {
			ownerSt.Mu.Lock()
			delete(ownerSt.PendingAsks, fullBody.WaitingFor)
			delete(ownerSt.WaitingReply, fullBody.WaitingFor)
			ownerSt.Mu.Unlock()
		}
	}
}

// respondWaitReply writes the wait_reply response for a received AskReply.
// If from_run_id is empty, the reply is a system signal (timeout or cancel).
func respondWaitReply(w http.ResponseWriter, reply AskReply, waitingFor string) {
	if reply.FromRunID == "" {
		// System signal -- check payload for type.
		var signal struct {
			Timeout   *bool `json:"timeout,omitempty"`
			Cancelled *bool `json:"cancelled,omitempty"`
		}
		_ = json.Unmarshal(reply.Payload, &signal)
		if signal.Timeout != nil && *signal.Timeout {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusGatewayTimeout)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"timeout":    true,
				"message_id": waitingFor,
			})
			return
		}
		if signal.Cancelled != nil && *signal.Cancelled {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"cancelled":  true,
				"message_id": waitingFor,
			})
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message_id":  reply.MessageID,
		"from_run_id": reply.FromRunID,
		"payload":     reply.Payload,
	})
}

// edgeKey builds a namespaced key for the global ask registry.
func edgeKey(runID, messageID string) string {
	return runID + "/" + messageID
}

// handleCancelMessage cancels a pending ask by message ID.
func (b *Broker) handleCancelMessage(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var fullBody struct {
		RunID           string `json:"run_id"`
		Token           string `json:"token"`
		CancelMessageID string `json:"cancel_message_id"`
	}
	if err := json.Unmarshal(bodyBytes, &fullBody); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if fullBody.CancelMessageID == "" {
		http.Error(w, "missing cancel_message_id", http.StatusBadRequest)
		return
	}

	b.mu.RLock()
	_, ok := b.runs[fullBody.RunID]
	b.mu.RUnlock()
	if !ok {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	// Cancel only works for asks sent by this run, so the key is scoped to the sender.
	key := edgeKey(fullBody.RunID, fullBody.CancelMessageID)
	b.globalAskEdgesMu.RLock()
	edge, edgeOk := b.globalAskEdges[key]
	replyCh, chOk := b.globalReplyChannels[key]
	b.globalAskEdgesMu.RUnlock()

	if !edgeOk {
		http.Error(w, "no pending ask for this message id", http.StatusNotFound)
		return
	}

	// Verify the caller owns this ask.
	if edge.FromRunID != fullBody.RunID {
		http.Error(w, "not the sender of this ask", http.StatusForbidden)
		return
	}

	// Deliver a cancellation signal to the waiting channel.
	if chOk {
		select {
		case replyCh <- AskReply{
			MessageID: fullBody.CancelMessageID,
			FromRunID: "",
			Payload:   json.RawMessage(`{"cancelled": true}`),
		}:
		default:
			// Channel full: the waiter is no longer listening (already got a signal),
			// which is harmless -- the edge will be cleaned up below.
		}
	}

	// Clean up all registries.
	b.globalAskEdgesMu.Lock()
	delete(b.globalAskEdges, key)
	delete(b.globalReplyChannels, key)
	b.globalAskEdgesMu.Unlock()
	if ownerSt := b.GetRun(edge.FromRunID); ownerSt != nil {
		ownerSt.Mu.Lock()
		delete(ownerSt.PendingAsks, fullBody.CancelMessageID)
		delete(ownerSt.WaitingReply, fullBody.CancelMessageID)
		ownerSt.Mu.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"cancelled": true, "message_id": fullBody.CancelMessageID})
}

// handleSessions returns a list of all registered runs and their metadata.
func (b *Broker) handleSessions(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sessions := make([]SessionInfo, 0, len(b.runs))
	for _, st := range b.runs {
		st.Mu.Lock()
		info := SessionInfo{RunID: st.RunID}
		if st.Info != nil {
			info.Label = st.Info.Label
			info.Backend = st.Info.Backend
			info.Model = st.Info.Model
			info.Dir = st.Info.Dir
			info.Status = st.Info.Status
		}
		info.LastSeen = st.LastSeen.Unix()
		st.Mu.Unlock()
		sessions = append(sessions, info)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"sessions": sessions})
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

// PollAgentMessages polls the broker for agent_message control messages destined
// to the given runID every 2 seconds. Each received message is wrapped with
// channelwrap and passed to onMessage. The goroutine exits when ctx is done.
func (b *Broker) PollAgentMessages(ctx context.Context, runID string, onMessage func(wrappedContent string)) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		st := b.GetRun(runID)
		if st == nil {
			continue
		}
		st.Mu.Lock()
		msgs := make([]ControlMessage, len(st.ControlQueue))
		for i, m := range st.ControlQueue {
			msgs[i] = *m
		}
		st.ControlQueue = nil
		st.Mu.Unlock()
		for _, msg := range msgs {
			if msg.Type != "agent_message" {
				continue
			}
			var payload struct {
				Message string `json:"message"`
			}
			if err := json.Unmarshal(msg.Payload, &payload); err != nil || payload.Message == "" {
				continue
			}

			// Include from_run_id in the channel metadata so the
			// receiving agent knows which run sent the message.
			meta := map[string]string{"from_run_id": msg.FromRunID}
			if msg.FromRunID != "" {
				meta["from_run_id"] = msg.FromRunID
			}
			wrapped := channelwrap.ChannelWrap(payload.Message, channelwrap.AgentName("agent"), meta)
			onMessage(wrapped)
		}
	}
}

// pruneAskEdgesLoop periodically removes expired ask edges from all runs.
func (b *Broker) pruneAskEdgesLoop() {
	ticker := time.NewTicker(askEdgePruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.closeCh:
			return
		case <-ticker.C:
			b.pruneExpiredAskEdges()
		}
	}
}

// pruneExpiredAskEdges removes ask edges older than DefaultAskTimeout and
// unblocks any waiters with a timeout signal.
func (b *Broker) pruneExpiredAskEdges() {
	now := time.Now()
	b.mu.RLock()
	runIDs := make([]string, 0, len(b.runs))
	for id := range b.runs {
		runIDs = append(runIDs, id)
	}
	b.mu.RUnlock()

	for _, runID := range runIDs {
		st := b.GetRun(runID)
		if st == nil {
			continue
		}
		st.Mu.Lock()
		for msgID, edge := range st.PendingAsks {
			if now.Sub(edge.CreatedAt) > DefaultAskTimeout {
				key := edgeKey(edge.FromRunID, msgID)
				// Signal any waiter, then remove the edge from every registry.
				// Deleting here (rather than deferring to /wait_reply) prevents
				// these entries leaking when no one ever calls /wait_reply. A
				// concurrent /wait_reply that already captured the channel still
				// receives the timeout signal before the entry is removed.
				b.globalAskEdgesMu.Lock()
				if ch, chOk := b.globalReplyChannels[key]; chOk {
					select {
					case ch <- AskReply{
						MessageID: msgID,
						FromRunID: "",
						Payload:   json.RawMessage(`{"timeout": true}`),
					}:
					default:
					}
				}
				delete(b.globalAskEdges, key)
				delete(b.globalReplyChannels, key)
				b.globalAskEdgesMu.Unlock()
				delete(st.PendingAsks, msgID)
				delete(st.WaitingReply, msgID)
			}
		}
		st.Mu.Unlock()
	}
}

// UpdateSessionInfo sets or updates the metadata for a registered run.
func (b *Broker) UpdateSessionInfo(runID string, info *SessionInfo) {
	st := b.GetRun(runID)
	if st == nil {
		return
	}
	st.Mu.Lock()
	if info != nil {
		info.LastSeen = time.Now().Unix()
		info.RunID = runID
	}
	st.Info = info
	st.Mu.Unlock()
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
		PendingAsks:         make(map[string]*AskEdge),
		WaitingReply:        make(map[string]chan AskReply),
	}
	b.runs[runID] = st
	return st.Token, nil
}

// EnsureRun registers a new run if one does not already exist for the
// given runID. Returns the run's token (existing or newly created) and
// whether the run was newly created.
func (b *Broker) EnsureRun(runID string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st, exists := b.runs[runID]; exists {
		return st.Token, false
	}
	st := &RunState{
		RunID:               runID,
		Token:               MakeToken(),
		LastSeen:            time.Now(),
		Notify:              make(chan struct{}, 1),
		PermissionRequests:  make(map[string]*PermissionState),
		PermissionDecisions: make(map[string]string),
		PendingAsks:         make(map[string]*AskEdge),
		WaitingReply:        make(map[string]chan AskReply),
	}
	b.runs[runID] = st
	return st.Token, true
}

// DeleteRun removes a run from the broker. Safe to call even if the run
// does not exist.
func (b *Broker) DeleteRun(runID string) {
	b.mu.Lock()
	st, ok := b.runs[runID]
	if ok {
		delete(b.runs, runID)
	}
	b.mu.Unlock()
	if !ok {
		return
	}
	// Remove any pending ask edges this run registered as a sender so they
	// don't leak in the global registries (no owner is left to ever call
	// /wait_reply or /cancel_message).
	st.Mu.Lock()
	for _, edge := range st.PendingAsks {
		key := edgeKey(edge.FromRunID, edge.MessageID)
		b.globalAskEdgesMu.Lock()
		delete(b.globalAskEdges, key)
		delete(b.globalReplyChannels, key)
		b.globalAskEdgesMu.Unlock()
	}
	st.PendingAsks = nil
	st.WaitingReply = nil
	st.Mu.Unlock()
}

func (b *Broker) RunCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.runs)
}

func (b *Broker) FinishCount(runID string) int {
	st := b.GetRun(runID)
	if st == nil {
		return 0
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	return len(st.Finishes)
}

func (b *Broker) LastFinishStatus(runID string) string {
	st := b.GetRun(runID)
	if st == nil {
		return ""
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	if len(st.Finishes) == 0 {
		return ""
	}
	return st.Finishes[len(st.Finishes)-1].Status
}

// Reset clears all run state.
func (b *Broker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.runs = make(map[string]*RunState)
	// Drop orphaned ask/reply registrations so Reset leaves no stale state.
	b.globalAskEdgesMu.Lock()
	b.globalAskEdges = make(map[string]*AskEdge)
	b.globalReplyChannels = make(map[string]chan AskReply)
	b.globalAskEdgesMu.Unlock()
}
