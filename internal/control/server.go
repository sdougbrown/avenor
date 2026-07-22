package control

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

const subscriberBuffer = 256
const maxSendToParentMessageBytes = 64 * 1024

type PermissionAnswer struct {
	RequestID string `json:"request_id"`
	OptionID  string `json:"option_id"`
}

type ControlServer struct {
	state *ControlState

	mu       sync.Mutex
	listener net.Listener
	path     string
	stopped  bool

	conns      map[*connState]struct{}
	subs       map[*subscriber]struct{}
	owner      *connState
	watch      map[chan events.Event]struct{}
	eventState map[string]*runtimeEventState

	cancelFn func()

	pendingMu     sync.Mutex
	pendingClaims map[permissionClaimKey]*permissionClaim

	promptMu        sync.Mutex
	promptQueue     []string
	interruptPrompt string

	interruptMu sync.Mutex
	interruptCh chan struct{}

	stableHandler StableHandler
}

type StableHandler interface {
	Spawn(params json.RawMessage) (any, error)
	List() any
	Shutdown(mode string) error
	RuntimeStatus(runtimeID string) (any, error)
	RuntimeCancel(runtimeID string) error
	RuntimePrompt(runtimeID, text, requestID string) error
	RuntimeAnswerPermission(runtimeID, requestID, optionID string) error
	RuntimeInterruptAndPrompt(runtimeID, text string, keepQueue bool) error
	RuntimeSendToParent(runtimeID, message string) error
}

type connState struct {
	id      uint64
	server  *ControlServer
	conn    net.Conn
	wmu     sync.Mutex
	closed  bool
	isOwner bool
}

type subscriber struct {
	conn      *connState
	ch        chan events.Event
	runtimeID string
	pending   []events.Event
	dropped   int
	seenSeq   map[string]int64
	ready     bool
	mu        sync.Mutex // guards subscriber buffers and channel lifecycle
	closed    bool
}

func NewServer(state *ControlState) *ControlServer {
	return &ControlServer{
		state:         state,
		conns:         map[*connState]struct{}{},
		subs:          map[*subscriber]struct{}{},
		watch:         map[chan events.Event]struct{}{},
		eventState:    map[string]*runtimeEventState{},
		pendingClaims: map[permissionClaimKey]*permissionClaim{},
	}
}

func (s *ControlServer) SetStableHandler(h StableHandler) { s.stableHandler = h }

func runtimeIDFromParams(params json.RawMessage) string {
	if len(params) == 0 {
		return ""
	}
	var p struct {
		RuntimeID string `json:"runtime_id"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return ""
	}
	return p.RuntimeID
}

func (s *ControlServer) SetCancelFunc(fn func()) { s.cancelFn = fn }

func (s *ControlServer) Start(socketPath string) error {
	if socketPath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return fmt.Errorf("create control socket dir: %w", err)
	}
	if _, err := os.Stat(socketPath); err == nil {
		if c, dialErr := net.DialTimeout("unix", socketPath, 250*time.Millisecond); dialErr == nil {
			_ = c.Close()
			return fmt.Errorf("control socket already in use: %s", socketPath)
		}
		_ = os.Remove(socketPath)
	}
	oldMask := syscall.Umask(0o077)
	l, err := net.Listen("unix", socketPath)
	syscall.Umask(oldMask)
	if err != nil {
		return err
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = l.Close()
		return fmt.Errorf("chmod control socket: %w", err)
	}

	s.mu.Lock()
	s.listener = l
	s.path = socketPath
	s.mu.Unlock()

	go s.acceptLoop()
	return nil
}

func (s *ControlServer) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	l := s.listener
	path := s.path
	conns := make([]*connState, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	if l != nil {
		_ = l.Close()
	}
	for _, c := range conns {
		_ = c.conn.Close()
	}
	if path != "" {
		_ = os.Remove(path)
	}
}

// HasClients reports whether the control plane has at least one connected
// client capable of answering control RPCs. Returns false when no socket is
// bound (e.g. --http-debug without --control-socket) or when no client has
// dialed in yet. Used to gate features that should defer to fallbacks
// (file-handler permissions) when nothing on the socket can answer.
func (s *ControlServer) HasClients() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.path != "" && len(s.conns) > 0
}

type permissionClaimKey struct {
	scope     string
	requestID string
}

type PermissionResolverState uint8

const (
	PermissionResolverUnknown PermissionResolverState = iota
	PermissionResolverReserved
	PermissionResolverControl
	PermissionResolverAutomatic
	PermissionResolverFile
	PermissionResolverNoResolver
	PermissionResolverDirectDelivery
	PermissionResolverResolved
)

type permissionClaim struct {
	state        PermissionResolverState
	answerCh     chan PermissionAnswer
	answerQueued bool
}

// PreparePermissionClaim records resolver ownership before permission.request
// is published. Terminal entries may be replaced when an ACP session reuses a
// request ID for a later request.
func (s *ControlServer) PreparePermissionClaim(scope, requestID string, state PermissionResolverState) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	key := permissionClaimKey{scope: scope, requestID: requestID}
	if existing := s.pendingClaims[key]; existing != nil &&
		existing.state != PermissionResolverResolved {
		return false
	}
	s.pendingClaims[key] = &permissionClaim{state: state, answerCh: make(chan PermissionAnswer, 1)}
	return true
}

func (s *ControlServer) SetPermissionResolverState(scope, requestID string, state PermissionResolverState) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	key := permissionClaimKey{scope: scope, requestID: requestID}
	if claim := s.pendingClaims[key]; claim != nil {
		claim.state = state
	}
}

// RetryDirectPermissionDelivery returns a failed direct delivery to the
// unowned state. It only changes the exact in-flight claim, so cleanup or a
// replacement claim cannot be overwritten by a late failure.
func (s *ControlServer) RetryDirectPermissionDelivery(scope, requestID string) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	key := permissionClaimKey{scope: scope, requestID: requestID}
	claim := s.pendingClaims[key]
	if claim == nil || claim.state != PermissionResolverDirectDelivery {
		return false
	}
	claim.state = PermissionResolverNoResolver
	return true
}

// HandoffPermissionClaim atomically consumes an answer already queued for the
// control resolver or closes control ownership by transitioning to next. Once
// it returns without an answer, later control deliveries are rejected.
func (s *ControlServer) HandoffPermissionClaim(scope, requestID string, next PermissionResolverState) (PermissionAnswer, bool) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	key := permissionClaimKey{scope: scope, requestID: requestID}
	claim := s.pendingClaims[key]
	if claim == nil {
		return PermissionAnswer{}, false
	}
	if claim.state != PermissionResolverReserved && claim.state != PermissionResolverControl {
		return PermissionAnswer{}, false
	}
	select {
	case answer := <-claim.answerCh:
		return answer, true
	default:
	}
	if next == PermissionResolverResolved {
		delete(s.pendingClaims, key)
	} else {
		claim.state = next
	}
	return PermissionAnswer{}, false
}

func (s *ControlServer) ClearPermissionClaims(scope string) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for key := range s.pendingClaims {
		if key.scope == scope {
			delete(s.pendingClaims, key)
		}
	}
}

func (s *ControlServer) PermissionResolverState(scope, requestID string) PermissionResolverState {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if claim := s.pendingClaims[permissionClaimKey{scope: scope, requestID: requestID}]; claim != nil {
		return claim.state
	}
	return PermissionResolverUnknown
}

// BeginPermissionClaim registers a permission claim within scope and returns
// its answer channel. Request IDs are only unique within an ACP session, so
// stable runtimes must use distinct scopes.
func (s *ControlServer) BeginPermissionClaim(scope, requestID string) (<-chan PermissionAnswer, bool) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	key := permissionClaimKey{scope: scope, requestID: requestID}
	claim := s.pendingClaims[key]
	if claim == nil {
		claim = &permissionClaim{answerCh: make(chan PermissionAnswer, 1)}
		s.pendingClaims[key] = claim
	} else if claim.state != PermissionResolverReserved {
		return nil, false
	}
	claim.state = PermissionResolverControl
	return claim.answerCh, true
}

func (s *ControlServer) EndPermissionClaim(scope, requestID string) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	delete(s.pendingClaims, permissionClaimKey{scope: scope, requestID: requestID})
}

// HasPendingPermission reports whether a permission claim is currently
// registered. Used by tests to verify claims are cleaned up after timeout.
func (s *ControlServer) HasPendingPermission() bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for _, claim := range s.pendingClaims {
		if claim.state == PermissionResolverReserved || claim.state == PermissionResolverControl {
			return true
		}
	}
	return false
}

// AnswerPendingPermission delivers an answer to the active permission claim.
// The bool reports whether the answer was actually delivered.
func (s *ControlServer) AnswerPendingPermission(scope, requestID, optionID string) bool {
	return s.DeliverPendingPermission(scope, requestID, optionID) == PermissionAnswerDelivered
}

type PermissionAnswerDelivery uint8

const (
	PermissionAnswerNotFound PermissionAnswerDelivery = iota
	PermissionAnswerDelivered
	PermissionAnswerChannelFull
	PermissionAnswerResolverOwned
	PermissionAnswerNoResolver
)

// DeliverPendingPermission distinguishes a missing claim from a claim that
// has already accepted an answer. answerQueued remains set after the resolver
// drains answerCh, making delivery single-use until the claim is ended.
func (s *ControlServer) DeliverPendingPermission(scope, requestID, optionID string) PermissionAnswerDelivery {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	claim := s.pendingClaims[permissionClaimKey{scope: scope, requestID: requestID}]
	if claim == nil {
		return PermissionAnswerNotFound
	}
	switch claim.state {
	case PermissionResolverReserved, PermissionResolverControl:
	case PermissionResolverNoResolver:
		claim.state = PermissionResolverDirectDelivery
		return PermissionAnswerNoResolver
	default:
		return PermissionAnswerResolverOwned
	}
	if claim.answerQueued {
		return PermissionAnswerChannelFull
	}
	select {
	case claim.answerCh <- PermissionAnswer{RequestID: requestID, OptionID: optionID}:
		claim.answerQueued = true
		return PermissionAnswerDelivered
	default:
		return PermissionAnswerChannelFull
	}
}

func (s *ControlServer) QueuePrompt(text string) {
	s.promptMu.Lock()
	s.promptQueue = append(s.promptQueue, text)
	s.promptMu.Unlock()
}

func (s *ControlServer) DequeuePrompt() string {
	s.promptMu.Lock()
	defer s.promptMu.Unlock()
	if len(s.promptQueue) == 0 {
		return ""
	}
	text := s.promptQueue[0]
	s.promptQueue = s.promptQueue[1:]
	return text
}

func (s *ControlServer) InterruptPrompt(text string, keepQueue bool) {
	s.promptMu.Lock()
	s.interruptPrompt = text
	if !keepQueue {
		s.promptQueue = nil
	}
	s.promptMu.Unlock()
	s.signalInterrupt()
}

func (s *ControlServer) ConsumeInterrupt() string {
	s.promptMu.Lock()
	defer s.promptMu.Unlock()
	text := s.interruptPrompt
	s.interruptPrompt = ""
	return text
}

// InterruptChan returns the channel that closes when an interrupt fires.
// Idempotent: returns the same channel until ResetInterrupt is called.
func (s *ControlServer) InterruptChan() <-chan struct{} {
	s.interruptMu.Lock()
	defer s.interruptMu.Unlock()
	if s.interruptCh == nil {
		s.interruptCh = make(chan struct{})
	}
	return s.interruptCh
}

// ResetInterrupt closes the current interrupt channel (if any) and creates a
// fresh one. Call this between turns to re-arm the interrupt signal.
func (s *ControlServer) ResetInterrupt() {
	s.interruptMu.Lock()
	defer s.interruptMu.Unlock()
	if s.interruptCh != nil {
		select {
		case <-s.interruptCh:
		default:
			close(s.interruptCh)
		}
	}
	s.interruptCh = make(chan struct{})
}

func (s *ControlServer) signalInterrupt() {
	s.interruptMu.Lock()
	ch := s.interruptCh
	s.interruptMu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

func (s *ControlServer) acceptLoop() {
	var seq uint64
	for {
		s.mu.Lock()
		l := s.listener
		stopped := s.stopped
		s.mu.Unlock()
		if stopped || l == nil {
			return
		}
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		seq++
		cs := &connState{id: seq, server: s, conn: conn}
		s.mu.Lock()
		s.conns[cs] = struct{}{}
		s.mu.Unlock()
		go s.handleConn(cs)
	}
}

func (s *ControlServer) handleConn(c *connState) {
	defer s.disconnect(c)
	scanner := bufio.NewScanner(c.conn)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 4*1024*1024)
	for scanner.Scan() {
		var req Request
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			_ = c.writeJSON(failure(nil, -32700, "parse error", nil))
			continue
		}
		if req.JSONRPC != "2.0" {
			_ = c.writeJSON(failure(req.ID, -32600, "invalid request", nil))
			continue
		}
		res := s.dispatch(c, req)
		if req.ID != nil {
			_ = c.writeJSON(res)
		}
	}
}

func (s *ControlServer) dispatch(c *connState, req Request) Response {
	switch req.Method {
	case "result":
		runtimeID := runtimeIDFromParams(req.Params)
		if runtimeID == "" {
			return success(req.ID, map[string]any{"final_output": s.state.FinalOutput()})
		}
		// Result is intentionally narrower than status: implementations may
		// retain a full terminal reply without putting it on status or event
		// presentation paths.
		handler, ok := s.stableHandler.(interface {
			RuntimeResult(string) (any, error)
		})
		if !ok {
			return failure(req.ID, -32601, "method not found", nil)
		}
		result, err := handler.RuntimeResult(runtimeID)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "status":
		if rtID := runtimeIDFromParams(req.Params); rtID != "" {
			if s.stableHandler == nil {
				return failure(req.ID, -32601, "method not found", nil)
			}
			result, err := s.stableHandler.RuntimeStatus(rtID)
			if err != nil {
				return failure(req.ID, -32000, err.Error(), nil)
			}
			return success(req.ID, result)
		}
		return success(req.ID, s.state.Snapshot())
	case "subscribe":
		var p subscribeParams
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
			}
		}
		if p.RuntimeID == "" && (p.Replay || p.AfterSeq > 0) {
			return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"runtime_id"}})
		}
		sub := newSubscriber(c, p.RuntimeID, p.AfterSeq)
		var replay []events.Event
		if p.Replay {
			s.mu.Lock()
			s.subs[sub] = struct{}{}
			replay, _ = s.runtimeHistoryLocked(p.RuntimeID, p.AfterSeq, p.Limit)
			s.mu.Unlock()
		} else {
			s.mu.Lock()
			s.subs[sub] = struct{}{}
			s.mu.Unlock()
		}
		go sub.start(replay)
		return success(req.ID, map[string]any{"subscribed": true})
	case "history":
		var p historyParams
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
			}
		}
		if p.RuntimeID == "" {
			return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"runtime_id"}})
		}
		history, latestSeq := s.runtimeHistory(p.RuntimeID, p.AfterSeq, p.Limit)
		result := make([]map[string]any, 0, len(history))
		for _, ev := range history {
			result = append(result, eventToMap(ev))
		}
		return success(req.ID, map[string]any{"runtime_id": p.RuntimeID, "events": result, "latest_seq": latestSeq})
	case "cancel":
		if rtID := runtimeIDFromParams(req.Params); rtID != "" {
			if s.stableHandler == nil {
				return failure(req.ID, -32601, "method not found", nil)
			}
			if !s.ensureOwner(c) {
				return failure(req.ID, -32010, "permission_denied", nil)
			}
			if err := s.stableHandler.RuntimeCancel(rtID); err != nil {
				return failure(req.ID, -32000, err.Error(), nil)
			}
			return success(req.ID, map[string]any{"ok": true})
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		if s.cancelFn != nil {
			s.cancelFn()
		}
		return success(req.ID, map[string]any{"ok": true})
	case "answer_permission":
		if rtID := runtimeIDFromParams(req.Params); rtID != "" {
			if s.stableHandler == nil {
				return failure(req.ID, -32601, "method not found", nil)
			}
			if !s.ensureOwner(c) {
				return failure(req.ID, -32010, "permission_denied", nil)
			}
			var p struct {
				RequestID string `json:"request_id"`
				OptionID  string `json:"option_id"`
			}
			if len(req.Params) > 0 {
				if err := json.Unmarshal(req.Params, &p); err != nil {
					return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
				}
			}
			if p.RequestID == "" || p.OptionID == "" {
				return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"request_id", "option_id"}})
			}
			if err := s.stableHandler.RuntimeAnswerPermission(rtID, p.RequestID, p.OptionID); err != nil {
				return failure(req.ID, -32000, err.Error(), nil)
			}
			return success(req.ID, map[string]any{"accepted": true})
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		var p PermissionAnswer
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return failure(req.ID, -32602, "invalid params", nil)
			}
		}
		if p.RequestID == "" || p.OptionID == "" {
			return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"request_id", "option_id"}})
		}
		if !s.AnswerPendingPermission("", p.RequestID, p.OptionID) {
			return failure(req.ID, -32001, "no_pending_permission", nil)
		}
		return success(req.ID, map[string]any{"accepted": true})
	case "prompt":
		if rtID := runtimeIDFromParams(req.Params); rtID != "" {
			if s.stableHandler == nil {
				return failure(req.ID, -32601, "method not found", nil)
			}
			if !s.ensureOwner(c) {
				return failure(req.ID, -32010, "permission_denied", nil)
			}
			var pr struct {
				Text      string `json:"text"`
				RequestID string `json:"request_id"`
			}
			if len(req.Params) > 0 {
				if err := json.Unmarshal(req.Params, &pr); err != nil {
					return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
				}
			}
			if pr.Text == "" {
				return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"text"}})
			}
			if err := s.stableHandler.RuntimePrompt(rtID, pr.Text, pr.RequestID); err != nil {
				return failure(req.ID, -32000, err.Error(), nil)
			}
			return success(req.ID, map[string]any{"accepted": true})
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		var pr struct {
			Text string `json:"text"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &pr); err != nil {
				return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
			}
		}
		if pr.Text == "" {
			return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"text"}})
		}
		s.QueuePrompt(pr.Text)
		s.state.Update(func(ss *Snapshot) { ss.TurnState = "idle" })
		return success(req.ID, map[string]any{"accepted": true})
	case "interrupt_and_prompt":
		if rtID := runtimeIDFromParams(req.Params); rtID != "" {
			if s.stableHandler == nil {
				return failure(req.ID, -32601, "method not found", nil)
			}
			if !s.ensureOwner(c) {
				return failure(req.ID, -32010, "permission_denied", nil)
			}
			var ip struct {
				Text      string `json:"text"`
				KeepQueue bool   `json:"keep_queue"`
			}
			if len(req.Params) > 0 {
				if err := json.Unmarshal(req.Params, &ip); err != nil {
					return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
				}
			}
			if ip.Text == "" {
				return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"text"}})
			}
			if err := s.stableHandler.RuntimeInterruptAndPrompt(rtID, ip.Text, ip.KeepQueue); err != nil {
				return failure(req.ID, -32000, err.Error(), nil)
			}
			return success(req.ID, map[string]any{"accepted": true})
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		var ip struct {
			Text      string `json:"text"`
			KeepQueue bool   `json:"keep_queue"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &ip); err != nil {
				return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
			}
		}
		if ip.Text == "" {
			return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"text"}})
		}
		s.InterruptPrompt(ip.Text, ip.KeepQueue)
		s.state.Update(func(ss *Snapshot) { ss.TurnState = "cancelling" })
		return success(req.ID, map[string]any{"accepted": true})
	case "spawn":
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		result, err := s.stableHandler.Spawn(req.Params)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "list":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		return success(req.ID, s.stableHandler.List())
	case "shutdown":
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		var sd struct {
			Mode string `json:"mode"`
		}
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params, &sd)
		}
		if sd.Mode == "" {
			sd.Mode = "graceful"
		}
		if err := s.stableHandler.Shutdown(sd.Mode); err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, map[string]any{"shutting_down": true})
	case "send_to_parent":
		if rtID := runtimeIDFromParams(req.Params); rtID != "" {
			if s.stableHandler == nil {
				return failure(req.ID, -32601, "method not found", nil)
			}
			var sp struct {
				Message string `json:"message"`
			}
			if len(req.Params) > 0 {
				if err := json.Unmarshal(req.Params, &sp); err != nil {
					return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
				}
			}
			if sp.Message == "" {
				return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"message"}})
			}
			if len(sp.Message) > maxSendToParentMessageBytes {
				return failure(req.ID, -32602, "invalid params", map[string]any{"message": fmt.Sprintf("message exceeds %d bytes", maxSendToParentMessageBytes)})
			}
			if err := s.stableHandler.RuntimeSendToParent(rtID, sp.Message); err != nil {
				return failure(req.ID, -32000, err.Error(), nil)
			}
			return success(req.ID, map[string]any{"accepted": true})
		}
		return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"runtime_id"}})
	default:
		return failure(req.ID, -32601, "method not found", nil)
	}
}

// ensureOwner implements first-come-first-served ownership: the first connected
// client to issue a mutating command becomes the owner. When the owner
// disconnects, ownership is released and the next client to call a mutating
// method inherits it. This relies on Unix socket permissions (0600) for
// process-level isolation.
func (s *ControlServer) ensureOwner(c *connState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owner == nil {
		s.owner = c
		c.isOwner = true
		return true
	}
	return s.owner == c
}

func (s *ControlServer) disconnect(c *connState) {
	s.mu.Lock()
	delete(s.conns, c)
	for sub := range s.subs {
		if sub.conn == c {
			delete(s.subs, sub)
			sub.mu.Lock()
			sub.closeLocked()
			sub.mu.Unlock()
		}
	}
	if s.owner == c {
		s.owner = nil
	}
	s.mu.Unlock()
	_ = c.conn.Close()
}

func (c *connState) writeJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	_, err = c.conn.Write(b)
	if err != nil {
		c.closed = true
	}
	return err
}

func newSubscriber(conn *connState, runtimeID string, afterSeq int64) *subscriber {
	sub := &subscriber{
		conn:      conn,
		ch:        make(chan events.Event, subscriberBuffer),
		runtimeID: runtimeID,
		seenSeq:   map[string]int64{},
	}
	if runtimeID != "" && afterSeq > 0 {
		sub.seenSeq[runtimeID] = afterSeq
	}
	return sub
}

func (s *subscriber) closeLocked() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

func (s *subscriber) matches(ev events.Event) bool {
	if s.runtimeID == "" {
		return true
	}
	return eventRuntimeID(ev) == s.runtimeID
}

func (s *subscriber) recordSeqLocked(ev events.Event) bool {
	seq, ok := int64Field(ev.Fields["seq"])
	if !ok {
		return true
	}
	key := eventScopeKey(ev)
	if last, ok := s.seenSeq[key]; ok && seq <= last {
		return false
	}
	s.seenSeq[key] = seq
	return true
}

func (s *subscriber) enqueueLocked(ev events.Event) {
	select {
	case s.ch <- ev:
	default:
		<-s.ch
		s.dropped++
		s.ch <- ev
	}
}

func (s *subscriber) enqueue(ev events.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seenSeq == nil {
		s.seenSeq = map[string]int64{}
		s.ready = true
	}
	if s.closed || !s.matches(ev) {
		return
	}
	if !s.ready {
		// Do not advance seenSeq until replay has been queued. Otherwise a live
		// event arriving during replay setup can make every older replay event
		// look like a duplicate and silently erase the requested history.
		if len(s.pending) >= subscriberBuffer {
			s.pending = append(s.pending[:0], s.pending[1:]...)
			s.dropped++
		}
		s.pending = append(s.pending, ev)
		return
	}
	if !s.recordSeqLocked(ev) {
		return
	}
	s.enqueueLocked(ev)
}

func (s *subscriber) flushPending() {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		if len(s.pending) == 0 {
			s.ready = true
			s.mu.Unlock()
			return
		}
		batch := append([]events.Event(nil), s.pending...)
		s.pending = nil
		s.mu.Unlock()
		for _, ev := range batch {
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				return
			}
			if s.recordSeqLocked(ev) {
				s.enqueueLocked(ev)
			}
			s.mu.Unlock()
		}
	}
}

func (s *subscriber) start(replay []events.Event) {
	go s.loop()
	for _, ev := range replay {
		s.mu.Lock()
		if s.closed || !s.matches(ev) || !s.recordSeqLocked(ev) {
			s.mu.Unlock()
			continue
		}
		s.enqueueLocked(ev)
		s.mu.Unlock()
	}
	s.flushPending()
}

func (s *subscriber) loop() {
	pendingLag := 0
	for ev := range s.ch {
		s.mu.Lock()
		if s.dropped > 0 {
			pendingLag += s.dropped
			s.dropped = 0
		}
		s.mu.Unlock()
		if pendingLag > 0 {
			_ = s.conn.writeJSON(Notification{JSONRPC: "2.0", Method: "event", Params: map[string]any{"event": "subscriber.lagged", "dropped_count": pendingLag, "ts": time.Now().UnixMilli()}})
			pendingLag = 0
		}
		_ = s.conn.writeJSON(Notification{JSONRPC: "2.0", Method: "event", Params: eventToMap(ev)})
	}
}

func eventToMap(ev events.Event) map[string]any {
	fields := map[string]any{"event": ev.Event}
	if ev.SessionID != "" {
		fields["session_id"] = ev.SessionID
	}
	for k, v := range ev.Fields {
		fields[k] = v
	}
	return fields
}
