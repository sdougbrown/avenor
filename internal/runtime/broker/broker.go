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
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/channelwrap"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
	"github.com/sdougbrown/avenor/internal/messaging/inprocess"
	"github.com/sdougbrown/avenor/internal/token"
)

// ControlMessage is a message pushed from Avenor to the sidecar for delivery to Claude.
type ControlMessage struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	RunID     string          `json:"run_id"`
	FromRunID string          `json:"from_run_id,omitempty"`
	Sender    string          `json:"sender,omitempty"`
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

// Reply is the payload from an avenor_message control_reply call.
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
	Kind     string `json:"kind,omitempty"`
	Backend  string `json:"backend,omitempty"`
	Model    string `json:"model,omitempty"`
	Dir      string `json:"dir,omitempty"`
	Status   string `json:"status,omitempty"`
	LastSeen int64  `json:"last_seen"`
}

// LastAgentMessage is one run's most recent completed turn. Text is bounded and
// Truncated reports a preview: the producer's flag, or the store's own bound
// having shortened the text.
type LastAgentMessage struct {
	RunID      string `json:"run_id"`
	Label      string `json:"label,omitempty"`
	Text       string `json:"final_output"`
	StopReason string `json:"stop_reason,omitempty"`
	Truncated  bool   `json:"final_output_truncated,omitempty"`
	At         int64  `json:"at,omitempty"`
}

// RunState holds all per-run broker state.
type RunState struct {
	RunID string
	// Token is the credential this run authenticates with. A replacement branch
	// claim rotates it, so it is guarded by the broker's own mutex, not by Mu:
	// read it through Broker.RunToken or Broker.runStateFor, or through
	// runStateLocked while holding b.mu.
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

	// HarnessName is the user-defined session name the harness process supplied
	// for this run: the human named the session in their tool. It is stored apart
	// from Info.Label so a model rename never erases it and it never blocks a
	// later rename. Like Info, it is guarded by Mu and read only through
	// displayName, so label resolution has one reader.
	HarnessName string
	// Session metadata for peer discovery. Info.Label is the name the run was
	// launched with or last renamed to; displayName resolves it against
	// HarnessName.
	Info *SessionInfo
	// LastMessage is the run's most recent completed turn, written by the
	// recorder and the host turn producer. Nil means no turn was recorded.
	LastMessage *LastAgentMessage
}

func (st *RunState) Lock()   { st.Mu.Lock() }
func (st *RunState) Unlock() { st.Mu.Unlock() }

// displayName reports the name peers see for this run.
func (st *RunState) displayName() string {
	st.Mu.Lock()
	defer st.Mu.Unlock()
	return st.labelLocked()
}

// labelLocked resolves the one display name peers see. A harness's own session
// name wins whenever the harness supplied one, because the human chose it; the
// model-set (or launch) name is the fallback, and a run with neither carries its
// run ID.
func (st *RunState) labelLocked() string {
	if st.HarnessName != "" {
		return st.HarnessName
	}
	if st.Info != nil && st.Info.Label != "" {
		return st.Info.Label
	}
	return st.RunID
}

// displayName reports the registered name of runID, or the run ID when no run
// is registered under it.
func (b *Broker) displayName(runID string) string {
	if runID == "" {
		return ""
	}
	st := b.GetRun(runID)
	if st == nil {
		return runID
	}
	return st.displayName()
}

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
	return b.sendToAs(fromRunID, toRunID, kind, payload, correlationID, b.displayName(fromRunID))
}

// sendToAs is SendTo with the sender named by the caller. The local seam always
// resolves the asker's name here; a relay instead carries the name the relaying
// branch resolved, because the receiving branch has no record of the asking run.
func (b *Broker) sendToAs(fromRunID, toRunID, kind string, payload json.RawMessage, correlationID, sender string) error {
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
		Sender:    sender,
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
	addr         string
	listener     net.Listener
	mu           sync.RWMutex
	runs         map[string]*RunState
	server       *http.Server
	httpToken    string        // optional global HTTP auth token for push-control endpoint
	pollTimeout  time.Duration // max wait for poll-control before returning empty
	writeTimeout time.Duration // per-connection write deadline (New sets 30s; /wait_reply extends its own deadline)

	// Ask lifecycle state and the delegate seam for local messaging.
	store     *askstore.Store
	messenger messaging.Messenger

	// parent is the broker-hierarchy link: the descendant registry its parent
	// routes write, and the ownership test the ask store's publication uses for
	// a run that has no queue here. nil leaves the broker standalone.
	parent      messaging.ParentLink
	parentToken string

	// runObserver is notified after the broker's set of owned runs changes, so
	// a host that publishes its runs to another broker re-publishes from here.
	runObserver func()

	// renameMu guards renameHandler, the supervisor-installed authority the
	// authenticated /rename endpoint delegates to.
	renameMu      sync.RWMutex
	renameHandler RenameHandler

	// harnessNameMu guards harnessNameObserver, the host-installed callback
	// SetHarnessName calls after it writes a harness session name, so a host
	// that keeps its own copy of the label follows the write.
	harnessNameMu       sync.RWMutex
	harnessNameObserver func(runID, name string)

	// parentRegisterMu serializes one registration's guard, register and apply:
	// the guard reads the run records and the link's ownership under two lock
	// domains, so a second registration whose own sequence interleaved with this
	// one could retire a record between this guard and this register and leave
	// the registry and the run records disagreeing about who owns a run. A
	// deregistration's retirement runs under the same mutex, so it cannot land
	// between one registration's register and its apply and leave that apply
	// holding records the link no longer owns.
	parentRegisterMu sync.Mutex

	pruneMu   sync.Mutex
	pruneStop context.CancelFunc
}

// New creates a broker on an ephemeral loopback port.
// The resulting addr is available after Start.
func New(globalToken string) *Broker {
	b := &Broker{
		runs:         make(map[string]*RunState),
		httpToken:    globalToken,
		pollTimeout:  2 * time.Second,
		writeTimeout: 30 * time.Second,
		store:        askstore.New(askstore.Options{}),
	}
	b.store.SetBackend(brokerBackend{broker: b})
	b.messenger = localMessenger{broker: b}
	return b
}

// Store returns the ask store the broker serves.
func (b *Broker) Store() *askstore.Store { return b.store }

// LocalMessenger returns the broker's own messaging implementation. A host
// that owns a parent link composes it for every target that is still local.
func (b *Broker) LocalMessenger() messaging.Messenger { return localMessenger{broker: b} }

// SetMessenger installs the delegate seam for local messaging. A nil messenger
// restores the broker's own implementation.
func (b *Broker) SetMessenger(m messaging.Messenger) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if m == nil {
		m = b.LocalMessenger()
	}
	b.messenger = m
}

// InstallParentLink installs everything a parent broker serves its parent-link
// routes with: the descendant registry those routes write, the bearer credential
// they require, and the callback that re-publishes this broker's owned runs when
// its run set changes. An unconfigured credential leaves the parent routes
// refused — the same-user trust the broker's local routes run on would let any
// local caller replace a live branch's claim wholesale; a nil link leaves them
// unserved, which is what a standalone broker reports.
func (b *Broker) InstallParentLink(link messaging.ParentLink, token string, observer func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.parent = link
	b.parentToken = token
	b.runObserver = observer
}

// parentLink reports the installed parent link and the credential its parent
// routes require. A nil link leaves those routes unserved, which is what a
// standalone broker reports.
func (b *Broker) parentLink() (messaging.ParentLink, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.parent, b.parentToken
}

// runSetChanged notifies the installed run observer.
func (b *Broker) runSetChanged() {
	b.mu.RLock()
	fn := b.runObserver
	b.mu.RUnlock()
	if fn != nil {
		fn()
	}
}

// RenameHandler is the authority a rename delegates to. It persists the name
// and returns the resolved run and the label before and after the write. The
// broker installs one only when a supervisor owns it; a broker without one
// falls back to the caller's own record.
type RenameHandler func(callerRunID, sessionID, name string) (messaging.RenameResult, error)

// SetRenameHandler installs the authority POST /rename delegates to.
func (b *Broker) SetRenameHandler(fn RenameHandler) {
	b.renameMu.Lock()
	defer b.renameMu.Unlock()
	b.renameHandler = fn
}

func (b *Broker) renameAuthority() RenameHandler {
	b.renameMu.RLock()
	defer b.renameMu.RUnlock()
	return b.renameHandler
}

func (b *Broker) seam() messaging.Messenger {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.messenger == nil {
		return localMessenger{broker: b}
	}
	return b.messenger
}

// MessengerTransport returns the in-process transport over the broker's ask
// store whose ask, reply and peer operations enter the messenger seam at call
// time. A messenger installed after this transport was built is still reached.
func (b *Broker) MessengerTransport() messaging.Transport {
	return inprocess.NewRouted(b.store, b.seam)
}

// MakeToken returns a random opaque credential via the shared hex generator.
// It panics if crypto/rand fails, matching its use as an infallible helper.
func MakeToken() string {
	credential, err := token.NewHex(16)
	if err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	return credential
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
	router.HandleFunc("/rename", b.withMethod("POST", b.withAuth(b.handleRename)))
	router.HandleFunc("/session_name", b.withMethod("POST", b.withAuth(b.handleSessionName)))
	router.HandleFunc("/last_message", b.withMethod("GET", b.withAuth(b.handleLastMessage)))
	router.HandleFunc("/permission_request", b.withMethod("POST", b.withAuth(b.handlePermissionRequest)))
	router.HandleFunc("/permission", b.withMethod("POST", b.withAuth(b.handlePermission)))
	router.HandleFunc("/parent/register", b.withMethod("POST", b.handleParentRegister))
	router.HandleFunc("/parent/deregister", b.withMethod("POST", b.handleParentDeregister))
	router.HandleFunc("/relay", b.withMethod("POST", b.withAuth(b.handleRelay)))

	b.server = &http.Server{
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: b.writeTimeout, // /wait_reply extends its own deadline via http.NewResponseController
	}
	go func() {
		_ = b.server.Serve(l)
	}()

	pruneCtx, pruneCancel := context.WithCancel(context.Background())
	b.pruneMu.Lock()
	b.pruneStop = pruneCancel
	b.pruneMu.Unlock()
	go b.store.RunPruneLoop(pruneCtx)

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
	b.pruneMu.Lock()
	if b.pruneStop != nil {
		b.pruneStop()
		b.pruneStop = nil
	}
	b.pruneMu.Unlock()
	var err error
	if b.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = b.server.Shutdown(ctx)
	}
	b.store.Reset()
	return err
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

// authenticatedCaller reports the run ID withAuth verified, which travels on
// the messaging leaf's caller-run key so a handler never has to trust the
// request body for the caller's identity and one key serves every reader.
func authenticatedCaller(r *http.Request) string {
	caller, _ := messaging.CallerRun(r.Context())
	return caller
}

// bearerAuthorized reports whether the request's Authorization header presents
// the expected Bearer credential.
func bearerAuthorized(r *http.Request, expected string) bool {
	auth := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(auth[len(prefix):])), []byte(expected)) == 1
}

func (b *Broker) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check global HTTP token for push-control
		if b.httpToken != "" && r.URL.Path == "/push-control" {
			if !bearerAuthorized(r, b.httpToken) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next(w, r)
			return
		}
		// Auth: credentials travel in the JSON body (POST) or, for GET, in
		// request headers, never in the URL.
		var cred struct {
			RunID string `json:"run_id"`
			Token string `json:"token"`
		}
		if r.Method == http.MethodGet {
			cred.RunID = r.Header.Get(messaging.HeaderRunID)
			cred.Token = r.Header.Get(messaging.HeaderRunToken)
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
		_, want, ok := b.runStateFor(cred.RunID)
		if !ok || subtle.ConstantTimeCompare([]byte(cred.Token), []byte(want)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// The authenticated run travels on the messaging leaf's own key, so the
		// router and a peer export a parent-aware messenger performs read the same
		// caller.
		next(w, r.WithContext(messaging.WithCallerRun(r.Context(), cred.RunID)))
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
	if st, token, exists := b.runStateLocked(body.RunID); exists {
		if body.Token == "" || subtle.ConstantTimeCompare([]byte(body.Token), []byte(token)) != 1 {
			b.mu.Unlock()
			http.Error(w, "run already registered", http.StatusConflict)
			return
		}
		st.Mu.Lock()
		st.RegisteredAt = time.Now()
		st.LastSeen = time.Now()
		st.Mu.Unlock()
		respToken := token
		b.mu.Unlock()
		resp := map[string]string{
			"token":  respToken,
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
	b.runSetChanged()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"token":  token,
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
		// AgentOnly drains the agent_message entries only. Control messages
		// stay queued for the consumers that act on them.
		AgentOnly bool `json:"agent_only"`
		// Limit caps the drained entries. The excess stays queued, so a
		// bounded poll loses nothing.
		Limit int `json:"limit"`
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
	msgs, queued := partitionDrained(st.ControlQueue, body.AgentOnly, body.Limit)
	st.ControlQueue = queued
	if len(st.ControlQueue) > 0 {
		st.signalLocked()
	}
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
		RunID        string          `json:"run_id"`
		Token        string          `json:"token"`
		FromRunID    string          `json:"from_run_id"`
		ToRunID      string          `json:"to_run_id"`
		Type         string          `json:"type"`
		Payload      json.RawMessage `json:"payload"`
		Generation   string          `json:"generation"`
		Sender       string          `json:"sender"`
		MessageID    string          `json:"message_id"`
		ReplyTo      string          `json:"reply_to"`
		ExpectsReply bool            `json:"expects_reply"`
	}
	if err := json.Unmarshal(bodyBytes, &fullBody); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if fullBody.FromRunID == "" || fullBody.ToRunID == "" || fullBody.Type == "" {
		writeCodedError(w, http.StatusBadRequest, "missing fields", "invalid-request")
		return
	}
	// Validate that the authenticated caller matches from_run_id's token.
	fromSt, fromToken, ok := b.runStateFor(fullBody.FromRunID)
	if !ok {
		writeCodedError(w, http.StatusNotFound, "from run not found", "invalid-request")
		return
	}
	if subtle.ConstantTimeCompare([]byte(fullBody.Token), []byte(fromToken)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// The registered name is bound to the token that authenticated the run, so
	// no caller can announce itself as a different participant. A request that
	// names nobody takes the registered name.
	senderName := fromSt.displayName()
	if fullBody.Sender != "" && fullBody.Sender != senderName {
		writeCodedError(w, http.StatusForbidden, "sender does not match this run", "sender-mismatch")
		return
	}

	// The payload carries the routing a legacy sender writes; the envelope
	// carries the same routing for clients that keep the payload opaque. Both
	// are read, and only a pair that disagrees is refused.
	msgID := fullBody.MessageID
	replyTo := fullBody.ReplyTo
	expectsReply := fullBody.ExpectsReply
	if fullBody.Type == "agent_message" && len(fullBody.Payload) > 0 {
		var am AgentMessage
		if err := json.Unmarshal(fullBody.Payload, &am); err != nil {
			writeCodedError(w, http.StatusBadRequest, "payload is not an agent message", "invalid-request")
			return
		}
		if msgID, ok = mergedField(am.ID, fullBody.MessageID); !ok {
			writeCodedError(w, http.StatusBadRequest, "message id in payload and envelope disagree", "invalid-request")
			return
		}
		if replyTo, ok = mergedField(am.ReplyTo, fullBody.ReplyTo); !ok {
			writeCodedError(w, http.StatusBadRequest, "reply_to in payload and envelope disagree", "invalid-request")
			return
		}
		expectsReply = expectsReply || am.ExpectsReply
	}

	if expectsReply {
		if msgID == "" {
			writeCodedError(w, http.StatusBadRequest, "expects_reply requires a message id", "invalid-request")
			return
		}

		target := fullBody.ToRunID

		if !b.seam().Reachable(target) {
			writeCodedError(w, http.StatusNotFound, "to run not found", "target-unavailable")
			return
		}

		res, err := b.seam().Send(r.Context(), messaging.SendRequest{
			From:       messaging.Identity{RunID: fullBody.FromRunID, Sender: senderName},
			To:         messaging.Identity{RunID: target, Sender: target},
			MessageID:  msgID,
			Generation: fullBody.Generation,
			Body:       string(fullBody.Payload),
			Legacy:     true,
		})
		if err != nil {
			writeSendError(w, err)
			return
		}
		if res.Outcome != messaging.OutcomeSent {
			writeJSONBody(w, http.StatusConflict, map[string]any{
				"error":   "message id withdrawn",
				"outcome": res.Outcome,
			})
			return
		}
		writeJSONBody(w, http.StatusOK, map[string]any{
			"queued":        true,
			"message_id":    msgID,
			"expects_reply": true,
			"outcome":       messaging.OutcomeSent,
		})
		return
	}

	if replyTo != "" {
		// A reply that names the generation its edge was admitted under refuses a
		// replacement broker instead of degrading into a delivery nobody waits
		// for. A reply that names none keeps the legacy delivery fallback below.
		if fullBody.Generation != "" && fullBody.Generation != b.store.Generation() {
			writeCodedError(w, http.StatusConflict, "generation mismatch", "generation-mismatch")
			return
		}
		// The asker is the reply's destination. A live edge is completed with
		// the reply; an absent edge falls back to fire-and-forget, which only
		// this layer can do because it owns the queues. The reply carries no
		// generation, so the messenger looks the edge up in the store's current
		// generation.
		outcome, err := b.seam().Reply(r.Context(), messaging.ReplyRequest{
			From:          messaging.Identity{RunID: fullBody.FromRunID},
			ToRunID:       fullBody.ToRunID,
			MessageID:     replyTo,
			CorrelationID: replyTo,
			Body:          string(fullBody.Payload),
		})
		if errors.Is(err, messaging.ErrNotAskTarget) {
			writeCodedError(w, http.StatusForbidden, "not the target of this ask", "not-ask-target")
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if outcome == messaging.OutcomeEdgeAbsent {
			// No live edge owns this ID, so the reply degrades to a delivery
			// rather than completing an edge the store already retired.
			err = b.seam().Deliver(r.Context(), messaging.DeliverRequest{
				FromRunID: fullBody.FromRunID,
				ToRunID:   fullBody.ToRunID,
				Kind:      fullBody.Type,
				Payload:   fullBody.Payload,
			})
			if err != nil {
				writeCodedError(w, http.StatusNotFound, err.Error(), "invalid-request")
				return
			}
			writeJSONBody(w, http.StatusOK, map[string]any{
				"queued":  true,
				"note":    "reply_to edge not found, delivered as fire-and-forget",
				"outcome": messaging.OutcomeEdgeAbsent,
			})
			return
		}
		writeJSONBody(w, http.StatusOK, map[string]any{
			"queued":     true,
			"reply_to":   replyTo,
			"message_id": replyTo,
			"outcome":    outcome,
		})
		return
	}

	// Fire-and-forget: deliver to the target's control queue.
	err = b.seam().Deliver(r.Context(), messaging.DeliverRequest{
		FromRunID: fullBody.FromRunID,
		ToRunID:   fullBody.ToRunID,
		Kind:      fullBody.Type,
		Payload:   fullBody.Payload,
	})
	if err != nil {
		writeCodedError(w, http.StatusNotFound, err.Error(), "target-unavailable")
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]any{
		"queued":  true,
		"outcome": messaging.OutcomeSent,
	})
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
		Generation string `json:"generation"`
		TimeoutMS  int64  `json:"timeout_ms"`
	}
	if err := json.Unmarshal(bodyBytes, &fullBody); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if fullBody.WaitingFor == "" {
		writeCodedError(w, http.StatusBadRequest, "missing waiting_for", "invalid-request")
		return
	}

	b.mu.RLock()
	_, ok := b.runs[fullBody.RunID]
	b.mu.RUnlock()
	if !ok {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	// The server-wide WriteTimeout would expire the socket's write deadline
	// before the bounded wait completes, dropping the reply after it was
	// consumed. Extend it to cover the wait plus response margin.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(askstore.DefaultAskTimeout + 30*time.Second))

	timeout := askstore.DefaultAskTimeout
	if fullBody.TimeoutMS > 0 {
		timeout = time.Duration(fullBody.TimeoutMS) * time.Millisecond
		if timeout > messaging.MaxWait {
			timeout = messaging.MaxWait
		}
	}

	res, err := b.seam().Wait(r.Context(), messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: fullBody.RunID, Sender: fullBody.RunID},
		MessageID:  fullBody.WaitingFor,
		Generation: fullBody.Generation,
		Timeout:    timeout,
	})
	if err != nil {
		if errors.Is(err, messaging.ErrGenerationMismatch) {
			writeCodedError(w, http.StatusConflict, "generation mismatch", "generation-mismatch")
			return
		}
		if r.Context().Err() != nil {
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch res.Outcome {
	case messaging.OutcomeReplied:
		if res.Reply == nil {
			writeCodedError(w, http.StatusNotFound, "no pending ask for this message id", "invalid-request")
			return
		}
		writeJSONBody(w, http.StatusOK, map[string]any{
			"message_id":  fullBody.WaitingFor,
			"from_run_id": res.Reply.FromRunID,
			"sender":      res.Reply.Sender,
			"kind":        res.Reply.Kind,
			"payload":     res.Reply.Payload,
			"outcome":     messaging.OutcomeReplied,
		})
	case messaging.OutcomeTimedOut:
		writeJSONBody(w, http.StatusGatewayTimeout, map[string]any{
			"timeout":    true,
			"message_id": fullBody.WaitingFor,
			"outcome":    messaging.OutcomeTimedOut,
		})
	case messaging.OutcomeWithdrawn:
		writeJSONBody(w, http.StatusOK, map[string]any{
			"cancelled":  true,
			"message_id": fullBody.WaitingFor,
			"outcome":    messaging.OutcomeWithdrawn,
		})
	case messaging.OutcomeLost:
		writeJSONBody(w, http.StatusGone, map[string]any{
			"lost":       true,
			"message_id": fullBody.WaitingFor,
			"outcome":    messaging.OutcomeLost,
		})
	case messaging.OutcomeEdgeAbsent:
		writeJSONBody(w, http.StatusNotFound, map[string]any{
			"error":   "no pending ask for this message id",
			"outcome": messaging.OutcomeEdgeAbsent,
		})
	case messaging.OutcomePossiblyPending:
		// The waiter's connection ended; nothing can be written.
	}
}

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
		Generation      string `json:"generation"`
	}
	if err := json.Unmarshal(bodyBytes, &fullBody); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if fullBody.CancelMessageID == "" {
		writeCodedError(w, http.StatusBadRequest, "missing cancel_message_id", "invalid-request")
		return
	}

	b.mu.RLock()
	_, ok := b.runs[fullBody.RunID]
	b.mu.RUnlock()
	if !ok {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	res, err := b.seam().Withdraw(r.Context(), messaging.WithdrawRequest{
		Sender:     messaging.Identity{RunID: fullBody.RunID, Sender: fullBody.RunID},
		MessageID:  fullBody.CancelMessageID,
		Generation: fullBody.Generation,
	})
	if err != nil {
		if errors.Is(err, messaging.ErrGenerationMismatch) {
			writeCodedError(w, http.StatusConflict, "generation mismatch", "generation-mismatch")
			return
		}
		if errors.Is(err, messaging.ErrFenceCapacity) {
			writeCodedError(w, http.StatusServiceUnavailable, "ask capacity exhausted", "fence-capacity")
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	switch res.Outcome {
	case messaging.OutcomeEdgeAbsent:
		writeJSONBody(w, http.StatusNotFound, map[string]any{
			"error":   "no pending ask for this message id",
			"outcome": messaging.OutcomeEdgeAbsent,
			"fence":   fenceBody(res.Fence),
		})
	default:
		// A live edge was consumed, or a reply already won the race; both
		// report the identifier as no longer pending.
		writeJSONBody(w, http.StatusOK, map[string]any{
			"cancelled":  true,
			"message_id": fullBody.CancelMessageID,
			"outcome":    res.Outcome,
			"fence":      fenceBody(res.Fence),
		})
	}
}

// handleSessions returns a list of all registered runs and their metadata.
// The generation and the messaging protocol marker let a shared client
// negotiate the local ask protocol before it sends anything. The peer list
// comes from the installed messenger, so a host that reaches peers the broker
// does not own lists them here too; a listing that messenger could not finish
// carries peers_error beside whatever it did return, so a partial list never
// reads as a complete one.
func (b *Broker) handleSessions(w http.ResponseWriter, r *http.Request) {
	generation := b.store.Generation()
	peers, listingErr := b.seam().Peers(r.Context())

	sessions := make([]SessionInfo, 0, len(peers))
	for _, peer := range peers {
		// Every run arrives as an encoded session record: the local messenger
		// encodes its own snapshot, so this single listing needs no second
		// snapshot to prefer local records over remote ones.
		info := SessionInfo{RunID: peer.RunID}
		if len(peer.Session) > 0 {
			if err := json.Unmarshal(peer.Session, &info); err != nil {
				listingErr = errors.Join(listingErr, fmt.Errorf("peer %s has an unreadable session record: %w", peer.RunID, err))
				continue
			}
		}
		if info.RunID == "" {
			info.RunID = peer.RunID
		}
		sessions = append(sessions, info)
	}

	body := map[string]any{
		"sessions":           sessions,
		"generation":         generation,
		"messaging_protocol": messaging.ProtocolVersion,
	}
	if listingErr != nil {
		body["peers_error"] = listingErr.Error()
	}
	writeJSONBody(w, http.StatusOK, body)
}

// --- parent link ---

// parentLinkAuthorized reports whether a parent-link request may act. A broker
// serves its parent routes only under the link credential it was configured
// with: an unconfigured credential would let any caller on the same machine
// replace a live branch's claim wholesale, so those routes refuse instead.
func parentLinkAuthorized(r *http.Request, token string) bool {
	if token == "" {
		return false
	}
	return bearerAuthorized(r, token)
}

// handleParentRegister adopts one child broker's claim on the runs it owns. The
// parent issues each descendant run's credential, so an ask forwarded upward
// arrives as the run it came from and a reply can complete the edge at the hop
// that admitted it. A registration replaces the child's previous claim, so the
// runs it stopped owning are retired with it.
func (b *Broker) handleParentRegister(w http.ResponseWriter, r *http.Request) {
	link, token := b.parentLink()
	if link == nil {
		writeCodedError(w, http.StatusNotFound, "this broker has no parent link", "unsupported-operation")
		return
	}
	if !parentLinkAuthorized(r, token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var body messaging.DescendantRegistration
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if body.ChildID == "" || body.Endpoint == "" || body.Generation == "" {
		writeCodedError(w, http.StatusBadRequest, "child_id, endpoint and generation are required", "invalid-request")
		return
	}
	// The guard, the register and the apply of one registration run as one
	// critical section, so a concurrent registration cannot slip a retirement
	// between this guard's read and this register, nor between this register
	// and the records it applies.
	b.parentRegisterMu.Lock()
	defer b.parentRegisterMu.Unlock()
	for _, claim := range body.Runs {
		if claim.RunID == "" {
			writeCodedError(w, http.StatusBadRequest, "every claimed run needs a run_id", "invalid-request")
			return
		}
		// A run this broker already owns itself is not a child's to claim, and a
		// claim never re-points a record it does not own: the registry names every
		// claim by the branch that makes it, so a collision here is a record this
		// broker minted itself.
		key := link.Key(body.ChildID, claim.RunID)
		if b.GetRun(key) != nil && !link.Owns(key) {
			writeCodedError(w, http.StatusConflict, "run already registered", "invalid-request")
			return
		}
	}
	claim, err := link.Register(body)
	if err != nil {
		writeCodedError(w, http.StatusConflict, err.Error(), "invalid-request")
		return
	}
	// The claim is applied: the runs the child owns now have records here and the
	// runs it stopped owning lose theirs. The run observer fires once per retired
	// run as they are deleted, plus once after the settled set, so a host that
	// publishes this broker's own runs re-publishes each step.
	for _, runID := range claim.Retired {
		b.DeleteRun(runID)
	}
	for _, run := range claim.Runs {
		b.ownRun(run)
	}
	b.runSetChanged()
	runKeys := make(map[string]string, len(claim.Runs))
	for _, run := range claim.Runs {
		// Every claim is namespaced: ChildID is required and ParentRunKey suffixes
		// it, so run.Key never equals run.RunID here.
		runKeys[run.RunID] = run.Key
	}
	response := map[string]any{
		"generation":         b.store.Generation(),
		"messaging_protocol": messaging.ProtocolVersion,
	}
	if len(runKeys) > 0 {
		response["run_keys"] = runKeys
	}
	writeJSONBody(w, http.StatusOK, response)
}

// handleParentDeregister retires one child broker's claim and the run records
// it owned, so a closed branch stops being a routing target. The branch sends
// the view of its registration its parent last accepted, and the retirement
// applies only while the registration still matches it: a same-identity
// replacement that re-registered while the deregistration was in flight keeps
// its claims and records.
func (b *Broker) handleParentDeregister(w http.ResponseWriter, r *http.Request) {
	link, token := b.parentLink()
	if link == nil {
		writeCodedError(w, http.StatusNotFound, "this broker has no parent link", "unsupported-operation")
		return
	}
	if !parentLinkAuthorized(r, token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var body messaging.DescendantRegistration
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if body.ChildID == "" {
		writeCodedError(w, http.StatusBadRequest, "child_id is required", "invalid-request")
		return
	}
	// The retirement runs under the registration critical section: retiring the
	// link's ownership between a concurrent registration's register and its
	// apply would leave that apply recreating records the link no longer owns,
	// and the next claim of those keys would be refused for good. A stale view
	// is still refused whole: the matching refusal below runs under this mutex
	// too, so a replacement that registered first keeps its claims and records.
	b.parentRegisterMu.Lock()
	defer b.parentRegisterMu.Unlock()
	retired, err := link.Deregister(body.ChildID, body)
	switch {
	case errors.Is(err, messaging.ErrGenerationMismatch):
		// The deregistration describes a registration this broker no longer
		// holds: a same-identity replacement registered while it was in flight,
		// and the replacement's claims and run records stay.
		writeCodedError(w, http.StatusConflict, err.Error(), "generation-mismatch")
		return
	case err != nil:
		writeCodedError(w, http.StatusNotFound, err.Error(), "target-unavailable")
		return
	}
	// Each retirement deletes the record only while the record still
	// authenticates with the credential its claim was registered with, so a
	// record a restarted run rotated is left to its owner.
	for runID, token := range retired {
		b.DeleteRunIfToken(runID, token)
	}
	writeJSONBody(w, http.StatusOK, map[string]any{
		"deregistered": true,
		"runs":         len(retired),
	})
}

// handleRelay publishes an ancestor's ask into the queue of the run that owns
// it. It allocates no edge: the ancestor's ask store owns the one edge the
// cross-branch ask has, and this is the hop that carries the ask into the next
// queue. The reply route is recorded before the ask is exposed, so a consumer
// can never answer an ask whose route is not bound yet.
func (b *Broker) handleRelay(w http.ResponseWriter, r *http.Request) {
	link, _ := b.parentLink()
	if link == nil {
		writeCodedError(w, http.StatusNotFound, "this broker has no parent link", "unsupported-operation")
		return
	}
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var body struct {
		RunID          string          `json:"run_id"`
		Token          string          `json:"token"`
		FromRunID      string          `json:"from_run_id"`
		FromSender     string          `json:"from_sender"`
		ToRunID        string          `json:"to_run_id"`
		Type           string          `json:"type"`
		Payload        json.RawMessage `json:"payload"`
		MessageID      string          `json:"message_id"`
		Generation     string          `json:"generation"`
		EdgeGeneration string          `json:"edge_generation"`
		ExpectsReply   bool            `json:"expects_reply"`
	}
	if err := json.Unmarshal(bodyBytes, &body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if authenticatedCaller(r) != body.ToRunID {
		writeCodedError(w, http.StatusForbidden, "relay target does not match the authenticated run", "not-ask-target")
		return
	}
	if body.FromRunID == "" || body.ToRunID == "" || body.Type == "" || body.MessageID == "" {
		writeCodedError(w, http.StatusBadRequest, "missing fields", "invalid-request")
		return
	}
	// A relay names the generation this broker pinned when it registered. A
	// different generation means the relay would land in a replacement broker, so
	// it is refused instead of replayed.
	if body.Generation != b.store.Generation() {
		writeCodedError(w, http.StatusConflict, "generation mismatch", "generation-mismatch")
		return
	}
	if body.ExpectsReply {
		if err := link.RecordRelay(messaging.RelayRoute{
			MessageID:      body.MessageID,
			AskerRunID:     body.FromRunID,
			EdgeGeneration: body.EdgeGeneration,
		}); err != nil {
			writeCodedError(w, http.StatusConflict, err.Error(), "invalid-request")
			return
		}
	}
	// The branch that owns the asker is the one that knows its display name,
	// so the relay carries the name it resolved; the receiving branch cannot
	// resolve a foreign run ID, and both branches compose through the same
	// sendTo path so they cannot diverge.
	sender := body.FromSender
	if sender == "" {
		sender = b.DisplayName(body.FromRunID)
	}
	if err := b.sendToAs(body.FromRunID, body.ToRunID, body.Type, body.Payload, body.MessageID, sender); err != nil {
		writeCodedError(w, http.StatusNotFound, "to run not found", "target-unavailable")
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]any{
		"queued":  true,
		"outcome": messaging.OutcomeSent,
	})
}

// handleRename renames one run's display name. It rejects a malformed request
// and a body whose run_id is not the run withAuth authenticated, then delegates
// the decision: with a handler installed the supervisor's SessionRename decides
// every row; without one the broker can only rename the authenticated caller's
// own record.
func (b *Broker) handleRename(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		writeCodedError(w, http.StatusBadRequest, "request body is unreadable", "invalid-request")
		return
	}
	var body struct {
		RunID     string `json:"run_id"`
		Token     string `json:"token"`
		Name      string `json:"name"`
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(bodyBytes, &body); err != nil {
		writeCodedError(w, http.StatusBadRequest, "request body is not a rename request", "invalid-request")
		return
	}
	if body.RunID == "" {
		writeCodedError(w, http.StatusBadRequest, "run_id is required", "invalid-request")
		return
	}
	if body.Name == "" {
		writeCodedError(w, http.StatusBadRequest, "name is required", "invalid-request")
		return
	}

	// Replacing this block would let a caller act as another run: the body
	// only repeats who authenticated, and it must agree.
	caller := authenticatedCaller(r)
	if body.RunID != caller {
		writeCodedError(w, http.StatusForbidden, "run_id does not match this run", "not-permitted")
		return
	}

	handler := b.renameAuthority()
	if handler == nil {
		// Broker-only fallback: the authority is the broker's own registry, so
		// this path can never grant more than the caller's own record. The name
		// rule is the same one the handler applies.
		if !messaging.ValidSessionName(body.Name) {
			writeCodedError(w, http.StatusBadRequest, messaging.SessionNameRuleMessage(body.Name), "invalid-name")
			return
		}
		previous := b.DisplayName(caller)
		if err := b.RenameRun(caller, body.Name); err != nil {
			writeCodedError(w, http.StatusBadRequest, err.Error(), "invalid-request")
			return
		}
		writeJSONBody(w, http.StatusOK, map[string]any{
			"run_id":         caller,
			"label":          b.DisplayName(caller),
			"previous_label": previous,
		})
		return
	}

	result, err := handler(caller, body.SessionID, body.Name)
	if err != nil {
		type toolCoder interface{ ToolCode() string }
		var coded toolCoder
		switch {
		case errors.As(err, &coded) && coded.ToolCode() == "not_permitted":
			writeCodedError(w, http.StatusForbidden, err.Error(), "not-permitted")
		case errors.As(err, &coded) && coded.ToolCode() == "invalid_name":
			writeCodedError(w, http.StatusBadRequest, err.Error(), "invalid-name")
		case errors.As(err, &coded) && coded.ToolCode() == "unknown_target":
			writeCodedError(w, http.StatusNotFound, err.Error(), "unknown-target")
		case errors.As(err, &coded) && coded.ToolCode() == "ambiguous_name":
			writeCodedError(w, http.StatusConflict, err.Error(), "ambiguous-name")
		default:
			writeCodedError(w, http.StatusBadRequest, err.Error(), "invalid-request")
		}
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]any{
		"run_id":         result.RunID,
		"label":          result.Label,
		"previous_label": result.PreviousLabel,
	})
}

// handleLastMessage reports any known run's recorded last turn. The target is
// not required to be the authenticated caller: this read is universally
// visible and side-effect free.
func (b *Broker) handleLastMessage(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Query().Get("target")
	if target == "" {
		writeCodedError(w, http.StatusBadRequest, "target is required", "invalid-request")
		return
	}
	if b.GetRun(target) == nil {
		writeCodedError(w, http.StatusNotFound, "run not found: "+target, "target-unavailable")
		return
	}
	record, hasRecord := b.LastAgentMessage(target)
	writeJSONBody(w, http.StatusOK, map[string]any{
		"run_id":                 record.RunID,
		"label":                  record.Label,
		"has_record":             hasRecord,
		"final_output":           record.Text,
		"stop_reason":            record.StopReason,
		"final_output_truncated": record.Truncated,
		"at":                     record.At,
	})
}

// sessions returns a snapshot of the registered runs.
func (b *Broker) sessions() []SessionInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sessions := make([]SessionInfo, 0, len(b.runs))
	for _, st := range b.runs {
		st.Mu.Lock()
		info := SessionInfo{RunID: st.RunID, Label: st.labelLocked()}
		if st.Info != nil {
			info.Kind = st.Info.Kind
			info.Backend = st.Info.Backend
			info.Model = st.Info.Model
			info.Dir = st.Info.Dir
			info.Status = st.Info.Status
		}
		info.LastSeen = st.LastSeen.Unix()
		st.Mu.Unlock()
		sessions = append(sessions, info)
	}
	return sessions
}

// runStateFor reports runID's record and the credential it authenticates with,
// both read under one lock. A replacement branch claim rotates the credential, so
// a reader that dropped the broker mutex between the lookup and the comparison
// would race the refresh and could authenticate against a credential that is
// already gone.
func (b *Broker) runStateFor(runID string) (*RunState, string, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.runStateLocked(runID)
}

// runStateLocked is runStateFor for a caller that already holds b.mu.
func (b *Broker) runStateLocked(runID string) (*RunState, string, bool) {
	st, ok := b.runs[runID]
	if !ok {
		return nil, "", false
	}
	return st, st.Token, true
}

// RunToken reports the credential this broker authenticates runID with.
func (b *Broker) RunToken(runID string) (string, bool) {
	_, token, ok := b.runStateFor(runID)
	return token, ok
}

// Sessions returns the runs this broker owns with the metadata its peers see.
func (b *Broker) Sessions() []SessionInfo { return b.sessions() }

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
		b.pollAgentMessagesOnce(runID, onMessage)
	}
}

// pollAgentMessagesOnce delivers runID's queued agent messages to onMessage and
// leaves every other control message queued for the consumers that act on it.
func (b *Broker) pollAgentMessagesOnce(runID string, onMessage func(wrappedContent string)) {
	st := b.GetRun(runID)
	if st == nil {
		return
	}
	st.Mu.Lock()
	agent, other := partitionAgentMessages(st.ControlQueue)
	msgs := make([]ControlMessage, 0, len(agent))
	for _, msg := range agent {
		msgs = append(msgs, *msg)
	}
	st.ControlQueue = other
	st.Mu.Unlock()
	for _, msg := range msgs {
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

// DrainAgentMessages returns and clears at most limit agent_message control
// messages queued for runID (asks / sends from peers). A non-positive limit
// drains every agent message. Other control messages, and the agent messages a
// limit leaves behind, stay queued in their original order.
func (b *Broker) DrainAgentMessages(runID string, limit int) ([]ControlMessage, error) {
	st := b.GetRun(runID)
	if st == nil {
		return nil, fmt.Errorf("run not found: %s", runID)
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	agent, rest := partitionDrained(st.ControlQueue, true, limit)
	st.ControlQueue = rest
	out := make([]ControlMessage, 0, len(agent))
	for _, msg := range agent {
		out = append(out, *msg)
	}
	return out, nil
}

// partitionDrained splits a control queue into the entries a consumer drains
// and the entries that stay queued in their original order. agentOnly limits
// the drain to agent messages; a non-positive limit drains every eligible
// entry.
func partitionDrained(queue []*ControlMessage, agentOnly bool, limit int) (drained, queued []*ControlMessage) {
	for _, msg := range queue {
		if (!agentOnly || msg.Type == "agent_message") && (limit <= 0 || len(drained) < limit) {
			drained = append(drained, msg)
			continue
		}
		queued = append(queued, msg)
	}
	return drained, queued
}

// partitionAgentMessages splits a control queue into the agent messages a drain
// consumes and the control messages it leaves to the consumers that act on
// them.
func partitionAgentMessages(queue []*ControlMessage) (agent, other []*ControlMessage) {
	for _, msg := range queue {
		if msg.Type == "agent_message" {
			agent = append(agent, msg)
			continue
		}
		other = append(other, msg)
	}
	return agent, other
}

// UpdateSessionInfo patches the metadata for a registered run field by field:
// every non-empty incoming field wins, an empty field never clears, and RunID
// is always stamped. Label is deliberately not patchable: it
// is owned by CreateRun/EnsureRun/RenameRun, so a later metadata write can
// never undo a rename. The harness-supplied name is a separate field for the
// same reason and is only reachable through SetHarnessName.
func (b *Broker) UpdateSessionInfo(runID string, info *SessionInfo) {
	st := b.GetRun(runID)
	if st == nil {
		return
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	if st.Info == nil {
		st.Info = &SessionInfo{RunID: runID}
	}
	if info != nil {
		if info.Kind != "" {
			st.Info.Kind = info.Kind
		}
		if info.Backend != "" {
			st.Info.Backend = info.Backend
		}
		if info.Model != "" {
			st.Info.Model = info.Model
		}
		if info.Dir != "" {
			st.Info.Dir = info.Dir
		}
		if info.Status != "" {
			st.Info.Status = info.Status
		}
	}
	st.Info.RunID = runID
}

// handleSessionName records a run's harness-supplied session name. The caller
// names itself, never another run: the body only repeats who withAuth
// authenticated, and it must agree. An empty name clears the harness name so
// the run falls back to its model-set name.
func (b *Broker) handleSessionName(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID string `json:"run_id"`
		Name  string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeCodedError(w, http.StatusBadRequest, "request body is not a session name", "invalid-request")
		return
	}
	caller := authenticatedCaller(r)
	if body.RunID != caller {
		writeCodedError(w, http.StatusForbidden, "run_id does not match this run", "not-permitted")
		return
	}
	if body.Name != "" && !messaging.ValidSessionName(body.Name) {
		writeCodedError(w, http.StatusBadRequest, messaging.SessionNameRuleMessage(body.Name), "invalid-name")
		return
	}
	if err := b.SetHarnessName(caller, body.Name); err != nil {
		writeCodedError(w, http.StatusNotFound, err.Error(), "invalid-request")
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]any{
		"run_id": caller,
		"label":  b.DisplayName(caller),
	})
}

// --- accessors ---

// GetRun returns the RunState for a given run ID, or nil if not found.
func (b *Broker) GetRun(runID string) *RunState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.runs[runID]
}

// CreateRun registers a new run under name, the display label its peers see.
// An empty name registers the run under its run ID. It returns the run token.
func (b *Broker) CreateRun(runID, name string) (string, error) {
	b.mu.Lock()
	if _, exists := b.runs[runID]; exists {
		b.mu.Unlock()
		return "", fmt.Errorf("run already registered")
	}
	st := newRunState(runID, name)
	b.runs[runID] = st
	token := st.Token
	b.mu.Unlock()
	b.runSetChanged()
	return token, nil
}

// EnsureRun registers a new run under name if one does not already exist for
// the given runID. An empty name registers the run under its run ID, and an
// existing run keeps the name it already has. Returns the run's token (existing
// or newly created) and whether the run was newly created.
func (b *Broker) EnsureRun(runID, name string) (string, bool) {
	b.mu.Lock()
	if st, exists := b.runs[runID]; exists {
		token := st.Token
		b.mu.Unlock()
		return token, false
	}
	st := newRunState(runID, name)
	b.runs[runID] = st
	token := st.Token
	b.mu.Unlock()
	b.runSetChanged()
	return token, true
}

// ownRun adopts a run a registered child broker owns, under the run ID this
// broker addresses it by. The record keeps the credential that branch
// authenticates the run with, so a forwarded ask and a relay into that branch
// use the one credential the run already has; a replacement branch presents a
// rotated credential, and the record follows the claim it accepted so both
// directions keep authenticating. A caller that adopts a whole set notifies the
// run observer once when the set is applied.
func (b *Broker) ownRun(claim messaging.ClaimedRun) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, exists := b.runs[claim.Key]
	if !exists {
		st = newRunState(claim.Key, claim.Label)
		if claim.Token != "" {
			st.Token = claim.Token
		}
		b.runs[claim.Key] = st
		return st.Token
	}
	if claim.Token != "" {
		st.Token = claim.Token
	}
	if claim.Label != "" {
		st.Mu.Lock()
		if st.Info == nil {
			st.Info = &SessionInfo{RunID: claim.Key}
		}
		st.Info.Label = claim.Label
		st.Mu.Unlock()
	}
	return st.Token
}

func newRunState(runID, name string) *RunState {
	if name == "" {
		name = runID
	}
	return &RunState{
		RunID:               runID,
		Token:               MakeToken(),
		LastSeen:            time.Now(),
		Notify:              make(chan struct{}, 1),
		PermissionRequests:  make(map[string]*PermissionState),
		PermissionDecisions: make(map[string]string),
		Info:                &SessionInfo{RunID: runID, Label: name},
	}
}

// DisplayName reports the name peers see for runID: the harness-supplied
// session name, else the model-set or launch name, else the run ID. This is the
// one reader of the resolved display name.
func (b *Broker) DisplayName(runID string) string { return b.displayName(runID) }

// MatchLabel returns every run whose display name is exactly label, sorted by
// run ID so the caller can tell an ambiguous label from a unique one instead of
// the broker guessing. A run a registered child broker owns is not this broker's
// to resolve: its display name belongs to the branch that owns it, so this
// broker's own labels never resolve to a descendant's record.
func (b *Broker) MatchLabel(label string) []string {
	b.mu.RLock()
	link := b.parent
	matches := make([]string, 0, 1)
	for runID, st := range b.runs {
		if link != nil && link.Owns(runID) {
			continue
		}
		if st.displayName() == label {
			matches = append(matches, runID)
		}
	}
	b.mu.RUnlock()
	sort.Strings(matches)
	return matches
}

// SetLastAgentMessage records runID's most recent completed turn. It always
// writes a record, even for empty text, so "no completed turn" stays distinct
// from "a turn with no final text". Truncated is the producer's fact, and the
// safety-net bound also sets it when it shortens the text: a cut record is a
// preview regardless of what the producer claimed.
func (b *Broker) SetLastAgentMessage(runID, text, stopReason string, truncated bool) error {
	st := b.GetRun(runID)
	if st == nil {
		return fmt.Errorf("run not found: %s", runID)
	}
	bounded := events.BoundedFinalOutput(text)
	if bounded != text {
		truncated = true
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	st.LastMessage = &LastAgentMessage{
		RunID:      runID,
		Text:       bounded,
		StopReason: stopReason,
		Truncated:  truncated,
		At:         time.Now().UnixMilli(),
	}
	return nil
}

// LastAgentMessage returns runID's recorded last turn. ok is false when no turn
// was ever recorded; the returned record still carries the run's real identity
// so a caller never renders an empty id. Label is the run's current display
// name, read here rather than snapshotted at write time, so a rename is always
// reflected.
func (b *Broker) LastAgentMessage(runID string) (LastAgentMessage, bool) {
	st := b.GetRun(runID)
	if st == nil {
		return LastAgentMessage{RunID: runID, Label: b.DisplayName(runID)}, false
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	label := st.labelLocked()
	if st.LastMessage == nil {
		return LastAgentMessage{RunID: runID, Label: label}, false
	}
	record := *st.LastMessage
	record.Label = label
	return record, true
}

// SetHarnessName records the user-defined session name the harness supplied for
// runID, or clears it when name is empty. It writes only the harness half of the
// label, so a model rename is neither clobbered by it nor able to hide it: the
// harness name is what peers see until the harness clears it. An observer set
// through SetHarnessNameObserver is called after the write with the run's
// resolved name, so a host that keeps its own copy of the label follows the
// write it did not perform.
func (b *Broker) SetHarnessName(runID, name string) error {
	st := b.GetRun(runID)
	if st == nil {
		return fmt.Errorf("run not found: %s", runID)
	}
	st.Mu.Lock()
	st.HarnessName = name
	st.Mu.Unlock()
	// The label is what a host publishes about this run, so a harness session
	// name is a run-set change the way a rename is.
	b.runSetChanged()
	b.harnessNameChanged(runID, name)
	// The label is part of what this broker publishes about the run, so a host
	// with a parent link re-publishes it now instead of at the next refresh.
	b.runSetChanged()
	return nil
}

// SetHarnessNameObserver sets the callback SetHarnessName calls after it writes
// a harness session name (or clears one). Nil leaves the write local to the
// broker.
func (b *Broker) SetHarnessNameObserver(fn func(runID, name string)) {
	b.harnessNameMu.Lock()
	defer b.harnessNameMu.Unlock()
	b.harnessNameObserver = fn
}

func (b *Broker) harnessNameChanged(runID, name string) {
	b.harnessNameMu.RLock()
	fn := b.harnessNameObserver
	b.harnessNameMu.RUnlock()
	if fn != nil {
		fn(runID, name)
	}
}

// RenameRun changes the model-set display name for runID. An empty name resets
// the run to its run ID. A harness-supplied name, if any, still wins. A run a
// registered child broker owns keeps the name that branch publishes for it, so
// a rename here is refused rather than silently reverted by the next claim.
func (b *Broker) RenameRun(runID, name string) error {
	if link, _ := b.parentLink(); link != nil && link.Owns(runID) {
		return fmt.Errorf("run %s is owned by a child broker and keeps the name that branch publishes", runID)
	}
	st := b.GetRun(runID)
	if st == nil {
		return fmt.Errorf("run not found: %s", runID)
	}
	if name == "" {
		name = runID
	}
	st.Mu.Lock()
	if st.Info == nil {
		st.Info = &SessionInfo{RunID: runID}
	}
	st.Info.Label = name
	st.Mu.Unlock()
	// The label is what a host publishes about this run, so a rename is a
	// run-set change even though the set itself did not change.
	b.runSetChanged()
	return nil
}

// DeleteRun removes a run from the broker. Safe to call even if the run
// does not exist.
func (b *Broker) DeleteRun(runID string) {
	b.mu.Lock()
	_, ok := b.runs[runID]
	if ok {
		delete(b.runs, runID)
	}
	b.mu.Unlock()
	if !ok {
		return
	}
	b.recordRemoved(runID)
}

// recordRemoved drops what a deleted record leaves behind: the run's ask edges,
// and the run-set change a host publishes. It runs after the record left the
// broker, outside the broker mutex.
func (b *Broker) recordRemoved(runID string) {
	// No owner is left to wait for or cancel this run's asks, and asks
	// addressed to it can never be answered.
	b.store.RemoveRun(runID)
	b.runSetChanged()
}

// DeleteRunIfToken removes a run from the broker only while it still
// authenticates with token, and reports whether it did. A relay that could not
// reach a branch retires the record it failed on, but a replacement branch may
// have registered with a fresh credential while the refusal was in flight; that
// record belongs to the healthy replacement and survives.
func (b *Broker) DeleteRunIfToken(runID, token string) bool {
	b.mu.Lock()
	st, ok := b.runs[runID]
	if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(st.Token)) != 1 {
		b.mu.Unlock()
		return false
	}
	delete(b.runs, runID)
	b.mu.Unlock()
	b.recordRemoved(runID)
	return true
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
	b.runs = make(map[string]*RunState)
	b.mu.Unlock()
	b.store.Reset()
	b.runSetChanged()
}

// brokerBackend adapts Broker queues and sessions to askstore.Backend.
type brokerBackend struct {
	broker *Broker
}

func (x brokerBackend) Publish(target string, msg messaging.InboundMessage) (func(), error) {
	// A run a registered child broker owns has no queue here. The parent's ask
	// store still admits the one edge the cross-branch ask owns; the relay that
	// reaches the child's queue runs after admission returns, so no publication
	// performs network I/O under the store's lock and nothing is queued locally
	// for a withdrawal to retract.
	if link, _ := x.broker.parentLink(); link != nil && link.Owns(target) {
		return func() {}, nil
	}
	st := x.broker.GetRun(target)
	if st == nil {
		return nil, messaging.ErrTargetUnavailable
	}
	queued := &ControlMessage{
		ID:        msg.MessageID,
		Type:      msg.Kind,
		RunID:     target,
		FromRunID: msg.FromRunID,
		// Stamp the sender's current display name here, server side, so the
		// in-process and HTTP transports agree and a rename always reaches the
		// peer. A surface sends an empty Sender by contract.
		Sender:    x.broker.DisplayName(msg.FromRunID),
		Payload:   msg.Payload,
		CreatedAt: time.Now(),
	}
	st.Mu.Lock()
	st.ControlQueue = append(st.ControlQueue, queued)
	st.signalLocked()
	st.Mu.Unlock()
	return func() {
		st.Mu.Lock()
		defer st.Mu.Unlock()
		for i, existing := range st.ControlQueue {
			if existing == queued {
				st.ControlQueue = append(st.ControlQueue[:i], st.ControlQueue[i+1:]...)
				return
			}
		}
	}, nil
}

func (x brokerBackend) Receive(sender string, limit int) ([]messaging.InboundMessage, error) {
	msgs, err := x.broker.DrainAgentMessages(sender, limit)
	if err != nil {
		return nil, err
	}
	out := make([]messaging.InboundMessage, 0, len(msgs))
	for _, msg := range msgs {
		var am AgentMessage
		_ = json.Unmarshal(msg.Payload, &am)
		sender := msg.Sender
		if sender == "" {
			sender = am.From
		}
		out = append(out, messaging.InboundMessage{
			MessageID: am.ID,
			FromRunID: msg.FromRunID,
			Sender:    sender,
			Kind:      msg.Type,
			Payload:   msg.Payload,
		})
	}
	return out, nil
}

func (x brokerBackend) Peers() ([]messaging.Peer, error) {
	// The store's backend carries no request context; peers are a snapshot.
	return x.broker.seam().Peers(context.Background())
}

// localMessenger serves local messaging from the broker's own queues and ask
// store. It owns no state of its own, so a parent-aware messenger composes it
// for every target that is still local.
type localMessenger struct {
	broker *Broker
}

func (m localMessenger) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	res, err := m.broker.store.Admit(ctx, askstore.AdmitRequest{
		Sender:     req.From,
		Target:     req.To.RunID,
		MessageID:  req.MessageID,
		Generation: req.Generation,
		Body:       req.Body,
		NoReply:    req.NoReply,
		Legacy:     req.Legacy,
	})
	return messaging.SendResult{Outcome: res.Outcome, MessageID: res.MessageID, Generation: res.Generation}, err
}

func (m localMessenger) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	return m.broker.store.Wait(ctx, m.key(req.Sender, req.MessageID, req.Generation), req.Timeout)
}

func (m localMessenger) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	return m.broker.store.Withdraw(m.key(req.Sender, req.MessageID, req.Generation))
}

func (m localMessenger) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.Outcome, error) {
	// An empty body is no payload at all, which keeps the reply's stored
	// payload valid JSON on the wire.
	var payload json.RawMessage
	if req.Body != "" {
		payload = json.RawMessage(req.Body)
	}
	key := m.key(messaging.Identity{RunID: req.ToRunID}, req.MessageID, m.broker.store.Generation())
	return m.broker.store.Reply(key, req.From.RunID, payload)
}

func (m localMessenger) Deliver(ctx context.Context, req messaging.DeliverRequest) error {
	return m.broker.SendTo(req.FromRunID, req.ToRunID, req.Kind, req.Payload, req.CorrelationID)
}

func (m localMessenger) Reachable(runID string) bool {
	return m.broker.GetRun(runID) != nil
}

func (m localMessenger) Peers(ctx context.Context) ([]messaging.Peer, error) {
	sessions := m.broker.sessions()
	peers := make([]messaging.Peer, 0, len(sessions))
	for _, info := range sessions {
		raw, err := json.Marshal(info)
		if err != nil {
			continue
		}
		peers = append(peers, messaging.Peer{RunID: info.RunID, Session: raw})
	}
	return peers, nil
}

func (m localMessenger) key(sender messaging.Identity, messageID, generation string) messaging.FenceKey {
	return messaging.FenceKey{Sender: sender.RunID, MessageID: messageID, Generation: generation}
}

// mergedField combines a payload field with its envelope counterpart: the
// payload is authoritative, the envelope fills a field the payload leaves
// empty, and two non-empty values that disagree report false.
func mergedField(payload, envelope string) (string, bool) {
	switch {
	case payload == "":
		return envelope, true
	case envelope == "" || payload == envelope:
		return payload, true
	}
	return "", false
}

// writeSendError maps a send failure to the local router's existing status
// codes and reports its protocol code plus outcome alongside.
func writeSendError(w http.ResponseWriter, err error) {
	code := messaging.WireCodeFor(err)
	switch {
	case errors.Is(err, messaging.ErrDuplicateMessageID):
		writeCodedError(w, http.StatusConflict, "message id already in use", code)
	case errors.Is(err, messaging.ErrMutualAsk):
		writeCodedError(w, http.StatusConflict, "mutual ask refused", code)
	case errors.Is(err, messaging.ErrGenerationMismatch):
		writeCodedError(w, http.StatusConflict, "generation mismatch", code)
	case errors.Is(err, messaging.ErrMessageIDExpired):
		writeCodedError(w, http.StatusConflict, "message id expired", code)
	case errors.Is(err, messaging.ErrFenceCapacity):
		writeCodedError(w, http.StatusServiceUnavailable, "ask capacity exhausted", code)
	case errors.Is(err, messaging.ErrTargetUnavailable):
		writeCodedError(w, http.StatusNotFound, "to run not found", code)
	case errors.Is(err, messaging.ErrInvalidRequest),
		errors.Is(err, messaging.ErrMalformedMessageID),
		errors.Is(err, messaging.ErrMessageIDHorizon):
		writeCodedError(w, http.StatusBadRequest, err.Error(), code)
	default:
		// An unclassified failure may still have admitted the ask, so report
		// the uncertainty instead of a code the client reads as not-sent.
		writeJSONBody(w, http.StatusInternalServerError, map[string]any{
			"error":   err.Error(),
			"outcome": messaging.OutcomePossiblyPending,
		})
	}
}

// writeCodedError reports a messaging failure with the legacy message text and
// the protocol code class that classifies it.
func writeCodedError(w http.ResponseWriter, status int, message, code string) {
	writeJSONBody(w, status, map[string]any{
		"error":   message,
		"code":    code,
		"outcome": messaging.WireOutcomeFor(code),
	})
}

// fenceBody renders a withdrawal's fence acknowledgement on the wire.
func fenceBody(ack messaging.FenceAck) map[string]any {
	return map[string]any{
		"sender":     ack.Sender,
		"message_id": ack.MessageID,
		"generation": ack.Generation,
		"deadline":   ack.Deadline,
		"terminal":   ack.Terminal,
	}
}

func writeJSONBody(w http.ResponseWriter, status int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
