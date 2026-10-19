// Package hostfixture is a test-only v1 host messaging server. It speaks the
// wire contract internal/messaging/hosthttp implements and is backed by a real
// askstore, so an ask admitted through it has the same edge, waiter and
// terminal-fence lifecycle as any other transport. Nothing outside tests may
// import it.
package hostfixture

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
)

const (
	// HeaderCapability and HeaderSecret name the capability the caller presents.
	HeaderCapability = "X-Avenor-Capability"
	HeaderSecret     = "X-Avenor-Secret"

	maxBodyBytes = 1 << 20
)

// Fixture is a v1 host endpoint bound to a fresh httptest server.
type Fixture struct {
	store   *askstore.Store
	backend *queues
	server  *httptest.Server
	paths   []string

	mu    sync.Mutex
	creds map[string]credential
}

type credential struct {
	secret string
	runID  string
}

// New starts a fixture and registers its cleanup.
func New(t *testing.T) *Fixture {
	t.Helper()
	backend := newQueues()
	store := askstore.New(askstore.Options{})
	store.SetBackend(backend)
	f := &Fixture{store: store, backend: backend, creds: make(map[string]credential)}

	routes := map[string]http.HandlerFunc{
		"/v1/preflight": f.preflight,
		"/v1/send":      f.send,
		"/v1/wait":      f.wait,
		"/v1/reply":     f.reply,
		"/v1/withdraw":  f.withdraw,
		"/v1/receive":   f.receive,
		"/v1/peers":     f.peers,
	}
	mux := http.NewServeMux()
	for path, handler := range routes {
		mux.HandleFunc(path, handler)
		f.paths = append(f.paths, path)
	}
	slices.Sort(f.paths)
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// Paths lists the v1 endpoints the fixture serves.
func (f *Fixture) Paths() []string { return append([]string(nil), f.paths...) }

// URL is the fixture's base URL.
func (f *Fixture) URL() string { return f.server.URL }

// Store exposes the ask state the fixture serves.
func (f *Fixture) Store() *askstore.Store { return f.store }

// Queued reports how many messages are waiting for target.
func (f *Fixture) Queued(target string) int { return f.backend.Queued(target) }

// Descriptor issues the endpoint and synthetic capability for id. Repeated
// calls for one identity keep the same capability so a transport built earlier
// stays authenticated, while ExpectedEpoch tracks the store's current
// generation.
func (f *Fixture) Descriptor(id messaging.Identity) messaging.EndpointDescriptor {
	f.mu.Lock()
	cred, ok := f.creds[id.RunID]
	if !ok {
		cred = credential{secret: "secret-" + messaging.NewGeneration(), runID: id.RunID}
		f.creds[id.RunID] = cred
	}
	f.mu.Unlock()
	f.backend.AddPeer(id.RunID)
	return messaging.EndpointDescriptor{
		BaseURL:          f.server.URL,
		CapabilityID:     id.RunID,
		CapabilitySecret: cred.secret,
		ExpectedEpoch:    f.store.Generation(),
		Timeout:          5 * time.Second,
	}
}

// PauseSend blocks the next admission of key until ResumeSend is called. It is
// a thin wrapper over the shared store seam, so the paused-send fence behaves
// exactly as it does in the in-process leg.
func (f *Fixture) PauseSend(sender messaging.Identity, messageID, generation string) {
	f.store.PauseAdmission(fenceKey(sender, messageID, generation))
}

// ResumeSend releases an admission paused by PauseSend.
func (f *Fixture) ResumeSend(sender messaging.Identity, messageID, generation string) {
	f.store.ResumeAdmission(fenceKey(sender, messageID, generation))
}

// Reached reports whether PauseSend's key waited at the admission seam.
func (f *Fixture) Reached(sender messaging.Identity, messageID, generation string) bool {
	return f.store.AdmissionReached(fenceKey(sender, messageID, generation))
}

func fenceKey(sender messaging.Identity, messageID, generation string) messaging.FenceKey {
	return messaging.FenceKey{Sender: sender.RunID, MessageID: messageID, Generation: generation}
}

func (f *Fixture) preflight(w http.ResponseWriter, r *http.Request) {
	if _, ok := f.authenticate(w, r); !ok {
		return
	}
	writeEnvelope(w, http.StatusOK, envelope{Protocol: messaging.ProtocolVersion, Epoch: f.store.Generation()})
}

func (f *Fixture) send(w http.ResponseWriter, r *http.Request) {
	cred, ok := f.authenticate(w, r)
	if !ok {
		return
	}
	var req struct {
		Epoch      string `json:"epoch"`
		MessageID  string `json:"message_id"`
		FromRunID  string `json:"from_run_id"`
		FromSender string `json:"from_sender"`
		ToRunID    string `json:"to_run_id"`
		Body       string `json:"body"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.FromRunID != cred.runID {
		writeError(w, "auth-rejected", messaging.OutcomeAuthRejected, "capability does not name the sender")
		return
	}
	if !f.epochOK(w, req.Epoch) {
		return
	}
	res, err := f.store.Admit(r.Context(), askstore.AdmitRequest{
		Sender:     messaging.Identity{RunID: req.FromRunID, Sender: req.FromSender},
		Target:     req.ToRunID,
		MessageID:  req.MessageID,
		Generation: req.Epoch,
	})
	if err != nil {
		writeError(w, wireCode(err), res.Outcome, err.Error())
		return
	}
	writeEnvelope(w, http.StatusOK, envelope{Outcome: res.Outcome, MessageID: res.MessageID, Epoch: res.Generation})
}

func (f *Fixture) wait(w http.ResponseWriter, r *http.Request) {
	cred, ok := f.authenticate(w, r)
	if !ok {
		return
	}
	var req struct {
		Epoch        string `json:"epoch"`
		SenderRunID  string `json:"sender_run_id"`
		MessageID    string `json:"message_id"`
		TimeoutMilli int64  `json:"timeout_ms"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SenderRunID != cred.runID {
		writeError(w, "auth-rejected", messaging.OutcomeAuthRejected, "capability does not name the sender")
		return
	}
	if !f.epochOK(w, req.Epoch) {
		return
	}
	res, err := f.store.Wait(r.Context(), fenceKey(messaging.Identity{RunID: req.SenderRunID}, req.MessageID, req.Epoch), time.Duration(req.TimeoutMilli)*time.Millisecond)
	if err != nil {
		writeError(w, wireCode(err), res.Outcome, err.Error())
		return
	}
	env := envelope{Outcome: res.Outcome}
	if res.Reply != nil {
		env.Reply = &message{
			MessageID: res.Reply.MessageID,
			FromRunID: res.Reply.FromRunID,
			Sender:    res.Reply.Sender,
			Kind:      res.Reply.Kind,
			Payload:   res.Reply.Payload,
		}
	}
	writeEnvelope(w, http.StatusOK, env)
}

func (f *Fixture) reply(w http.ResponseWriter, r *http.Request) {
	if _, ok := f.authenticate(w, r); !ok {
		return
	}
	var req struct {
		FromRunID     string `json:"from_run_id"`
		ToRunID       string `json:"to_run_id"`
		MessageID     string `json:"message_id"`
		CorrelationID string `json:"correlation_id"`
		Body          string `json:"body"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	outcome, err := f.store.Reply(
		fenceKey(messaging.Identity{RunID: req.ToRunID}, req.MessageID, f.store.Generation()),
		req.FromRunID,
		json.RawMessage(req.Body),
	)
	if err != nil {
		writeError(w, wireCode(err), outcome, err.Error())
		return
	}
	writeEnvelope(w, http.StatusOK, envelope{Outcome: outcome, MessageID: req.MessageID})
}

func (f *Fixture) withdraw(w http.ResponseWriter, r *http.Request) {
	cred, ok := f.authenticate(w, r)
	if !ok {
		return
	}
	var req struct {
		Epoch       string `json:"epoch"`
		SenderRunID string `json:"sender_run_id"`
		MessageID   string `json:"message_id"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SenderRunID != cred.runID {
		writeError(w, "auth-rejected", messaging.OutcomeAuthRejected, "capability does not name the sender")
		return
	}
	if !f.epochOK(w, req.Epoch) {
		return
	}
	res, err := f.store.Withdraw(fenceKey(messaging.Identity{RunID: req.SenderRunID}, req.MessageID, req.Epoch))
	if err != nil {
		writeError(w, wireCode(err), res.Outcome, err.Error())
		return
	}
	env := envelope{Outcome: res.Outcome}
	if res.Fence.Terminal {
		env.Fence = &fence{
			Sender:     res.Fence.Sender,
			MessageID:  res.Fence.MessageID,
			Generation: res.Fence.Generation,
			Deadline:   res.Fence.Deadline,
			Terminal:   res.Fence.Terminal,
		}
	}
	writeEnvelope(w, http.StatusOK, env)
}

func (f *Fixture) receive(w http.ResponseWriter, r *http.Request) {
	cred, ok := f.authenticate(w, r)
	if !ok {
		return
	}
	var req struct {
		Epoch       string `json:"epoch"`
		SenderRunID string `json:"sender_run_id"`
		Limit       int    `json:"limit"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SenderRunID != cred.runID {
		writeError(w, "auth-rejected", messaging.OutcomeAuthRejected, "capability does not name the sender")
		return
	}
	if !f.epochOK(w, req.Epoch) {
		return
	}
	if req.Limit <= 0 {
		req.Limit = 32
	}
	msgs, err := f.store.Receive(req.SenderRunID, req.Limit)
	if err != nil {
		writeError(w, wireCode(err), "", err.Error())
		return
	}
	env := envelope{Epoch: f.store.Generation()}
	for _, msg := range msgs {
		env.Messages = append(env.Messages, message{
			MessageID: msg.MessageID,
			FromRunID: msg.FromRunID,
			Sender:    msg.Sender,
			Kind:      msg.Kind,
			Payload:   msg.Payload,
		})
	}
	writeEnvelope(w, http.StatusOK, env)
}

func (f *Fixture) peers(w http.ResponseWriter, r *http.Request) {
	cred, ok := f.authenticate(w, r)
	if !ok {
		return
	}
	var req struct {
		Epoch       string `json:"epoch"`
		SenderRunID string `json:"sender_run_id"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SenderRunID != cred.runID {
		writeError(w, "auth-rejected", messaging.OutcomeAuthRejected, "capability does not name the sender")
		return
	}
	if !f.epochOK(w, req.Epoch) {
		return
	}
	all, err := f.store.Peers()
	if err != nil {
		writeError(w, wireCode(err), "", err.Error())
		return
	}
	env := envelope{Epoch: f.store.Generation()}
	for _, p := range all {
		env.Peers = append(env.Peers, peer{RunID: p.RunID, Session: p.Session})
	}
	writeEnvelope(w, http.StatusOK, env)
}

func (f *Fixture) authenticate(w http.ResponseWriter, r *http.Request) (credential, bool) {
	id := r.Header.Get(HeaderCapability)
	secret := r.Header.Get(HeaderSecret)
	f.mu.Lock()
	cred, ok := f.creds[id]
	f.mu.Unlock()
	if !ok || secret == "" || subtle.ConstantTimeCompare([]byte(cred.secret), []byte(secret)) != 1 {
		writeError(w, "auth-rejected", messaging.OutcomeAuthRejected, "capability rejected")
		return credential{}, false
	}
	return cred, true
}

func (f *Fixture) epochOK(w http.ResponseWriter, epoch string) bool {
	if epoch == "" || epoch != f.store.Generation() {
		writeError(w, "generation-mismatch", messaging.OutcomeNotSent, "request epoch does not match the served generation")
		return false
	}
	return true
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(dst); err != nil {
		writeError(w, "invalid-request", messaging.OutcomeNotSent, "malformed request body")
		return false
	}
	return true
}

// wireCode is the wire spelling of a typed messaging error.
func wireCode(err error) string {
	switch {
	case errors.Is(err, messaging.ErrAuthRejected):
		return "auth-rejected"
	case errors.Is(err, messaging.ErrGenerationMismatch):
		return "generation-mismatch"
	case errors.Is(err, messaging.ErrUnsupportedProtocol):
		return "unsupported-protocol"
	case errors.Is(err, messaging.ErrMalformedMessageID):
		return "malformed-message-id"
	case errors.Is(err, messaging.ErrMessageIDExpired):
		return "message-id-expired"
	case errors.Is(err, messaging.ErrMessageIDHorizon):
		return "message-id-horizon"
	case errors.Is(err, messaging.ErrDuplicateMessageID):
		return "duplicate-message-id"
	case errors.Is(err, messaging.ErrMutualAsk):
		return "mutual-ask"
	case errors.Is(err, messaging.ErrFenceCapacity):
		return "fence-capacity"
	case errors.Is(err, messaging.ErrNotAskSender):
		return "not-ask-sender"
	case errors.Is(err, messaging.ErrNotAskTarget):
		return "not-ask-target"
	case errors.Is(err, messaging.ErrTargetUnavailable):
		return "target-unavailable"
	case errors.Is(err, messaging.ErrTransportClosed):
		return "transport-closed"
	case errors.Is(err, messaging.ErrUnsupportedOperation):
		return "unsupported-operation"
	}
	return "invalid-request"
}

func statusFor(code string) int {
	switch code {
	case "auth-rejected":
		return http.StatusUnauthorized
	case "not-ask-target", "not-ask-sender":
		return http.StatusForbidden
	case "generation-mismatch", "duplicate-message-id":
		return http.StatusConflict
	case "fence-capacity", "target-unavailable":
		return http.StatusServiceUnavailable
	}
	return http.StatusBadRequest
}

func writeError(w http.ResponseWriter, code string, outcome messaging.Outcome, detail string) {
	writeEnvelope(w, statusFor(code), envelope{Outcome: outcome, Error: code, Message: detail})
}

func writeEnvelope(w http.ResponseWriter, status int, env envelope) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(env)
}

// envelope is one v1 response. Every field is optional so a response can carry
// only the operation's result plus, on failure, its typed error code.
type envelope struct {
	Outcome   messaging.Outcome `json:"outcome,omitempty"`
	Error     string            `json:"error,omitempty"`
	Message   string            `json:"message,omitempty"`
	Protocol  int               `json:"protocol,omitempty"`
	Epoch     string            `json:"epoch,omitempty"`
	MessageID string            `json:"message_id,omitempty"`
	Fence     *fence            `json:"fence,omitempty"`
	Reply     *message          `json:"reply,omitempty"`
	Messages  []message         `json:"messages,omitempty"`
	Peers     []peer            `json:"peers,omitempty"`
}

type fence struct {
	Sender     string    `json:"sender"`
	MessageID  string    `json:"message_id"`
	Generation string    `json:"generation"`
	Deadline   time.Time `json:"deadline"`
	Terminal   bool      `json:"terminal"`
}

type message struct {
	MessageID string          `json:"message_id"`
	FromRunID string          `json:"from_run_id,omitempty"`
	Sender    string          `json:"sender,omitempty"`
	Kind      string          `json:"kind,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type peer struct {
	RunID   string          `json:"run_id"`
	Session json.RawMessage `json:"session"`
}

// queues is the fixture's in-memory message and peer store.
type queues struct {
	mu     sync.Mutex
	queues map[string][]messaging.InboundMessage
	peers  []messaging.Peer
}

func newQueues() *queues {
	return &queues{queues: make(map[string][]messaging.InboundMessage)}
}

func (q *queues) Publish(target string, msg messaging.InboundMessage) (func(), error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queues[target] = append(q.queues[target], msg)
	removed := false
	return func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		if removed {
			return
		}
		removed = true
		queue := q.queues[target]
		for i, queued := range queue {
			if queued.MessageID == msg.MessageID {
				q.queues[target] = append(queue[:i], queue[i+1:]...)
				return
			}
		}
	}, nil
}

func (q *queues) Receive(sender string, limit int) ([]messaging.InboundMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue := q.queues[sender]
	if len(queue) == 0 {
		return nil, nil
	}
	if limit > 0 && len(queue) > limit {
		queue = queue[:limit]
	}
	out := append([]messaging.InboundMessage(nil), queue...)
	q.queues[sender] = q.queues[sender][len(queue):]
	return out, nil
}

func (q *queues) Peers() ([]messaging.Peer, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]messaging.Peer(nil), q.peers...), nil
}

func (q *queues) AddPeer(runID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, peer := range q.peers {
		if peer.RunID == runID {
			return
		}
	}
	q.peers = append(q.peers, messaging.Peer{RunID: runID})
}

func (q *queues) Queued(target string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queues[target])
}
