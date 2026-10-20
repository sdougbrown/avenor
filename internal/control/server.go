package control

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/workflow"
)

const subscriberBuffer = 256

// subscriberFlushTimeout bounds how long Stop spends delivering events a
// subscriber already had queued, so a stuck reader cannot hold up shutdown.
const subscriberFlushTimeout = 250 * time.Millisecond

const maxSendToParentMessageBytes = 64 * 1024

const controlIdentityTokenEnv = "AVENOR_CONTROL_IDENTITY_TOKEN"

var dialControlSocket = net.DialTimeout

// ErrControlStopped reports that a control server refused work because it is
// shutting down. A request refused over the wire carries CodeControlStopped.
var ErrControlStopped = errors.New("control server stopped")

// These no-op hooks let lifecycle tests coordinate at lock and transition
// boundaries without changing production behavior.
var beforeControlSocketLock = func() {}
var beforeControlSocketCleanup = func() {}
var beforeDirectPermissionClaimWait = func() {}
var beforeSubscriberFlush = func() {}
var afterSubscriberFlushEvent = func() {}

type PermissionAnswer struct {
	RequestID string `json:"request_id"`
	OptionID  string `json:"option_id"`
	Message   string `json:"message,omitempty"`
}

func (p *PermissionAnswer) UnmarshalJSON(data []byte) error {
	var wire struct {
		RequestID string          `json:"request_id"`
		OptionID  string          `json:"option_id"`
		Message   json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	message, err := runtime.DecodePermissionMessageJSON(wire.Message)
	if err != nil {
		return err
	}
	p.RequestID, p.OptionID, p.Message = wire.RequestID, wire.OptionID, message
	return nil
}

type ControlServer struct {
	state *ControlState

	mu            sync.Mutex
	listener      net.Listener
	path          string
	socketInfo    os.FileInfo
	identityToken string
	// identityTokenOverride supplies the attempt token instead of the
	// environment, so tests can race two attempts with distinct tokens.
	identityTokenOverride string
	stopped               bool

	conns      map[*connState]struct{}
	subs       map[*subscriber]struct{}
	owner      OwnerCapability
	recordID   SocketIdentity
	watch      map[chan events.Event]struct{}
	eventState map[string]*runtimeEventState

	cancelFn                  func()
	afterClientDisconnectScan func() // deterministic lifecycle test hook

	pendingMu     sync.Mutex
	pendingClaims map[permissionClaimKey]*permissionClaim

	promptMu        sync.Mutex
	promptQueue     []string
	interruptPrompt string

	interruptMu sync.Mutex
	interruptCh chan struct{}

	stableHandler     StableHandler
	workflowHandler   atomic.Pointer[WorkflowHandler]
	controllerHandler atomic.Pointer[WorkflowControllerHandler]
}

type StableHandler interface {
	Spawn(params json.RawMessage) (any, error)
	SpawnMCP(params json.RawMessage) (any, error)
	List() any
	Shutdown(mode string) error
	RuntimeStatus(runtimeID string) (any, error)
	RuntimeWaitTurn(ctx context.Context, runtimeID string, timeout time.Duration) (any, error)
	RuntimeCancel(runtimeID string) error
	RuntimePrompt(runtimeID, text, requestID string) error
	RuntimeAnswerPermission(runtimeID, requestID, optionID, message string) error
	RuntimeInterruptAndPrompt(runtimeID, text string, keepQueue bool) error
	RuntimeSendToParent(runtimeID, message string) error
	BrokerSend(fromRunID, toRunID, message, role string) error
	BrokerAsk(ctx context.Context, toRunID, message, role string) (any, error)
	BrokerPeers() (any, error)
	BrokerCancel(messageID string) error
	BrokerReceive() (any, error)
	BrokerReply(toRunID, replyTo, message string) error

	// The typed messaging methods delegate to the supervisor's own transport.
	MessageTransport() (messaging.Transport, error)
	Resume(caller messaging.Caller, req messaging.ResumeRequest) (messaging.ResumeResult, error)
	SessionRename(caller messaging.Caller, sessionID, name string) (messaging.RenameResult, error)
	SessionLast(runtimeID string) (broker.LastAgentMessage, bool, error)
	SessionLastSet(caller messaging.Caller, runtimeID, finalOutput, stopReason string, truncated bool) error

	// TreeBudgetStatus reports tree-budget diagnostics. A supervisor without an
	// active budget returns a degraded status map with its reason.
	TreeBudgetStatus() any

	// WaitForCapacityMS blocks until a tree budget slot may be available or the
	// given timeout (milliseconds) elapses. Returns nil when capacity may be
	// available, or an error on timeout/shutdown. It does not reserve capacity.
	WaitForCapacityMS(timeoutMS int) error
}

// AskError carries the broker message ID of a failed ask plus whether the
// ask edge may still be pending. A failed wait is retracted by the ask client,
// so Pending is false once that cleanup confirmed a terminal fence; callers can
// withdraw a still-pending ask with broker_cancel instead of retrying blind.
type AskError struct {
	MessageID string
	Pending   bool
	Outcome   messaging.Outcome
	Err       error
}

func (e *AskError) Error() string { return e.Err.Error() }
func (e *AskError) Unwrap() error { return e.Err }

// askErrorData extracts cancel-friendly error data for RPC error responses.
func askErrorData(err error) any {
	var ae *AskError
	if errors.As(err, &ae) {
		data := map[string]any{"message_id": ae.MessageID, "pending": ae.Pending}
		if ae.Outcome != "" {
			data["outcome"] = ae.Outcome
		}
		return data
	}
	return nil
}

type connState struct {
	id      uint64
	server  *ControlServer
	conn    net.Conn
	wmu     sync.Mutex
	closed  bool
	isOwner bool

	// ctx is cancelled when the connection drops or the server stops, so
	// long-running handlers (broker asks, wait_turn) do not outlive their client.
	ctx    context.Context
	cancel context.CancelFunc

	// waitSlots bounds concurrent in-flight wait_turn requests per connection
	// so a single owner cannot accumulate unbounded parked goroutines.
	waitSlots chan struct{}

	// askSlots bounds concurrent in-flight broker asks on this connection.
	askSlots chan struct{}

	// messageSlots bounds concurrent in-flight blocking message verbs
	// (message_wait, message_receive) on this connection, matching the
	// broker_ask and wait_turn bounds.
	messageSlots chan struct{}
}

type subscriber struct {
	conn      *connState
	ch        chan events.Event
	done      chan struct{}
	runtimeID string
	pending   []events.Event
	dropped   int
	seenSeq   map[string]int64
	ready     bool
	mu        sync.Mutex // guards subscriber buffers and channel lifecycle
	closed    bool
}

func NewServer(state *ControlState, owner OwnerCapability) *ControlServer {
	return &ControlServer{
		state:         state,
		owner:         owner,
		conns:         map[*connState]struct{}{},
		subs:          map[*subscriber]struct{}{},
		watch:         map[chan events.Event]struct{}{},
		eventState:    map[string]*runtimeEventState{},
		pendingClaims: map[permissionClaimKey]*permissionClaim{},
	}
}

func (s *ControlServer) SetStableHandler(h StableHandler) { s.stableHandler = h }

// WorkflowControllerHandler is the optional handler for workflow-controller
// read and configuration methods. It is distinct from WorkflowHandler.
type WorkflowControllerHandler interface {
	WorkflowControllerCreate(json.RawMessage) (any, error)
	WorkflowControllerEnable(string) (any, error)
	WorkflowControllerDisable(string, json.RawMessage) (any, error)
	WorkflowControllerStatus(string) (any, error)
	WorkflowControllerList() (any, error)
	WorkflowReady(string, int) (any, error)
}

func (s *ControlServer) SetWorkflowHandler(h WorkflowHandler) {
	if h == nil {
		s.workflowHandler.Store(nil)
		return
	}
	s.workflowHandler.Store(&h)
}

func (s *ControlServer) SetWorkflowControllerHandler(h WorkflowControllerHandler) {
	if h == nil {
		s.controllerHandler.Store(nil)
		return
	}
	s.controllerHandler.Store(&h)
}

func workflowIDFromParams(params json.RawMessage) (string, error) {
	if len(params) == 0 {
		return "", errors.New("missing workflow_id")
	}
	var p struct {
		WorkflowID string `json:"workflow_id"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return "", err
	}
	if p.WorkflowID == "" {
		return "", errors.New("missing workflow_id")
	}
	return p.WorkflowID, nil
}

func controllerIDFromParams(req Request) (string, *Response) {
	var p struct {
		ControllerID string `json:"controller_id"`
	}
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &p); err != nil {
			resp := failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
			return "", &resp
		}
	}
	if p.ControllerID == "" {
		resp := failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"controller_id"}})
		return "", &resp
	}
	return p.ControllerID, nil
}

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

// lockControlSocket serializes every mutation of a socket path. Start holds it
// through stale-path removal and bind; Stop holds it through owned-path removal.
func lockControlSocket(socketPath string) (*os.File, error) {
	beforeControlSocketLock()
	lockFile, err := os.OpenFile(socketPath+".control.lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open control socket lock: %w", err)
	}
	if err := validateControlLock(lockFile); err != nil {
		_ = lockFile.Close()
		return nil, err
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("lock control socket: %w", err)
	}
	return lockFile, nil
}

// validateControlLock validates the opened lock inode rather than its path, and
// converges an inherited permissive mode without replacing the file.
func validateControlLock(lockFile *os.File) error {
	info, err := lockFile.Stat()
	if err != nil {
		return fmt.Errorf("stat control socket lock: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("control socket lock is not a regular file")
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("control socket lock is not owned by uid %d", os.Geteuid())
	}
	if info.Mode().Perm()&0o077 != 0 {
		if err := lockFile.Chmod(0o600); err != nil {
			return fmt.Errorf("chmod control socket lock: %w", err)
		}
	}
	return nil
}

func unlockControlSocket(lockFile *os.File) {
	_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
	_ = lockFile.Close()
}

// removeOwnedSocket removes path only while l still proves it is the active
// listener and path still names that listener's identity. The caller must hold
// the control socket lock. A path that no longer names this listener belongs to
// a replacement and is left alone.
func removeOwnedSocket(l net.Listener, path string, socketInfo os.FileInfo) error {
	unixListener, ok := l.(*net.UnixListener)
	if !ok || unixListener.SetDeadline(time.Now()) != nil {
		return nil
	}
	current, err := os.Stat(path)
	if err != nil {
		return nil
	}
	if socketInfo != nil && !os.SameFile(current, socketInfo) {
		return nil
	}
	if err := testOwnerSocketRemove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove control socket %s: %w", path, err)
	}
	return nil
}

func (s *ControlServer) Start(socketPath string) error {
	// startSupervisor passes this one-shot token to prove that the process
	// which acknowledged readiness is the process later reached by its parent.
	// Clear it immediately so spawned providers cannot inherit it.
	identityToken := s.identityTokenOverride
	if identityToken == "" {
		identityToken = os.Getenv(controlIdentityTokenEnv)
		_ = os.Unsetenv(controlIdentityTokenEnv)
	}

	if socketPath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return fmt.Errorf("create control socket dir: %w", err)
	}
	// Serialize stale-socket inspection, bind and publication across
	// independent starters. Only the holder may unlink and bind.
	lockFile, err := lockControlSocket(socketPath)
	if err != nil {
		return err
	}
	locked := true
	defer func() {
		if locked {
			unlockControlSocket(lockFile)
		}
	}()

	if _, err := os.Stat(socketPath); err == nil {
		if c, dialErr := dialControlSocket("unix", socketPath, 250*time.Millisecond); dialErr == nil {
			_ = c.Close()
			return fmt.Errorf("%w: %s", ErrSocketInUse, socketPath)
		} else if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			// A timeout or transient error does not prove the socket is stale.
			// Leave it intact rather than risking an active peer's path.
			return fmt.Errorf("probe existing control socket %s: %w", socketPath, dialErr)
		}
		if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale control socket: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat control socket: %w", err)
	}
	oldMask := syscall.Umask(0o077)
	l, err := net.Listen("unix", socketPath)
	syscall.Umask(oldMask)
	if err != nil {
		return err
	}
	// UnixListener.Close normally unlinks its path. Disable that implicit
	// cleanup so Stop can verify that the path still identifies this listener
	// before removing it; otherwise a repeated Close can unlink a replacement.
	if unixListener, ok := l.(*net.UnixListener); ok {
		unixListener.SetUnlinkOnClose(false)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = l.Close()
		_ = os.Remove(socketPath)
		return fmt.Errorf("chmod control socket: %w", err)
	}
	info, err := os.Stat(socketPath)
	if err != nil {
		_ = l.Close()
		_ = os.Remove(socketPath)
		return fmt.Errorf("stat control socket: %w", err)
	}
	identity, err := socketIdentity(info)
	if err != nil {
		_ = l.Close()
		_ = os.Remove(socketPath)
		return err
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		// Start and Stop raced. We still own the control lock and the live
		// listener, so clean up before returning without leaving a socket behind.
		_ = removeOwnedSocket(l, socketPath, info)
		_ = l.Close()
		return ErrControlStopped
	}
	s.mu.Unlock()

	// A test can force the connectable-before-published window so a launcher's
	// retry path is exercised against the real ordering instead of a race.
	if os.Getenv(runtime.EnvTestHooks) == "1" {
		if delay, err := time.ParseDuration(os.Getenv(runtime.EnvTestOwnerPublishDelay)); err == nil && delay > 0 {
			time.Sleep(delay)
		}
	}

	// Bound-but-unpublished is not a usable server. Publish this child's record
	// while the lock is still held and the listener is still live, and roll the
	// whole attempt back if publication cannot be completed.
	if err := s.publishOwnerRecord(socketPath, identity); err != nil {
		recordErr := s.removeOwnerRecord(socketPath, identity, s.owner.ID)
		socketErr := removeOwnedSocket(l, socketPath, info)
		unlockControlSocket(lockFile)
		locked = false
		closeErr := l.Close()
		return errors.Join(err, recordErr, socketErr, closeErr)
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		recordErr := s.removeOwnerRecord(socketPath, identity, s.owner.ID)
		socketErr := removeOwnedSocket(l, socketPath, info)
		unlockControlSocket(lockFile)
		locked = false
		return errors.Join(ErrControlStopped, recordErr, socketErr, l.Close())
	}
	s.listener = l
	s.path = socketPath
	s.socketInfo = info
	s.recordID = identity
	// The attempt token becomes observable only after this child's own record
	// is on disk, so a launcher that reads the token can attribute the record
	// to this process without ordering by time.
	s.identityToken = identityToken
	s.mu.Unlock()
	unlockControlSocket(lockFile)
	locked = false

	// startSupervisor supplies this one-shot inherited descriptor. Signalling
	// only after bind and publication lets its parent distinguish this child
	// from a competing stable process that bound the same path first.
	readyFDValue := os.Getenv("AVENOR_CONTROL_READY_FD")
	_ = os.Unsetenv("AVENOR_CONTROL_READY_FD")
	if readyFD, err := strconv.Atoi(readyFDValue); err == nil && readyFD >= 3 {
		ready := os.NewFile(uintptr(readyFD), "avenor-control-ready")
		if ready != nil {
			_, _ = ready.Write([]byte{1})
			_ = ready.Close()
		}
	}

	go s.acceptLoop()
	return nil
}

func (s *ControlServer) Stop() error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	l := s.listener
	path := s.path
	socketInfo := s.socketInfo
	identity := s.recordID
	// acceptLoop registers a connection under this same lock and refuses one
	// once stopped is set, so this snapshot already covers every connection the
	// server will ever close.
	conns := make([]*connState, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	var failures []error
	// Serialize cleanup with Start's stale-path inspection and bind. Remove
	// while the listener is still live; once it is closed, a replacement may
	// bind and reuse its inode before a parent can safely inspect the path.
	// The record removal takes the same lock, so a competing Start cannot rename
	// its own record into place between the identity check and the unlink.
	if l != nil && path != "" {
		lockFile, lockErr := lockControlSocket(path)
		if lockErr != nil {
			failures = append(failures, lockErr)
		} else {
			beforeControlSocketCleanup()
			if err := s.removeOwnerRecord(path, identity, s.owner.ID); err != nil {
				failures = append(failures, err)
			}
			if err := removeOwnedSocket(l, path, socketInfo); err != nil {
				failures = append(failures, err)
			}
			unlockControlSocket(lockFile)
		}
	}
	if l != nil {
		if err := l.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			failures = append(failures, fmt.Errorf("close control listener: %w", err))
		}
	}
	s.flushSubscribers()
	for _, c := range conns {
		c.cancel()
		if err := c.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			failures = append(failures, fmt.Errorf("close control connection: %w", err))
		}
	}
	return errors.Join(failures...)
}

func (s *ControlServer) flushSubscribers() {
	beforeSubscriberFlush()
	deadline := time.Now().Add(subscriberFlushTimeout)

	s.mu.Lock()
	subs := make([]*subscriber, 0, len(s.subs))
	for sub := range s.subs {
		subs = append(subs, sub)
	}
	s.mu.Unlock()

	// Bound each write first: a subscriber whose reader is gone must fail its
	// pending write instead of blocking the drain until then.
	for _, sub := range subs {
		_ = sub.conn.conn.SetWriteDeadline(deadline)
	}
	for _, sub := range subs {
		// flushPending skips a subscriber that a concurrent disconnect has
		// already closed, so the drain never sends on a closed channel.
		sub.flushPending(deadline)
		sub.mu.Lock()
		sub.closeLocked()
		sub.mu.Unlock()
	}
	for _, sub := range subs {
		sub.waitDrained(deadline)
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

func (s PermissionResolverState) String() string {
	switch s {
	case PermissionResolverUnknown:
		return "unknown"
	case PermissionResolverReserved:
		return "reserved"
	case PermissionResolverControl:
		return "control"
	case PermissionResolverAutomatic:
		return "automatic"
	case PermissionResolverFile:
		return "file"
	case PermissionResolverNoResolver:
		return "none"
	case PermissionResolverDirectDelivery:
		return "direct_delivery"
	case PermissionResolverResolved:
		return "resolved"
	default:
		return "unknown"
	}
}

type permissionClaim struct {
	state              PermissionResolverState
	resolutionSource   string
	resolutionOptionID string
	resolutionMessage  string
	answerCh           chan PermissionAnswer
	answerQueued       bool
	disconnectCh       chan struct{} // closed when all clients disconnect while in Reserved/Control state
	requiresMessage    map[string]bool
	directDone         chan struct{}
	directAbandoned    bool
}

// PreparePermissionClaim records resolver ownership before permission.request
// is published. Terminal entries may be replaced when an ACP session reuses a
// request ID for a later request.
func (s *ControlServer) PreparePermissionClaim(scope, requestID string, state PermissionResolverState, options []any) bool {
	// Serialize Reserved claim creation with the last-client disconnect path.
	// Otherwise signalClientDisconnect can observe no claim immediately before
	// this method creates one with an open disconnect channel.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	prepared := s.preparePermissionClaimLocked(scope, requestID, state, options)
	if prepared && state == PermissionResolverReserved && len(s.conns) == 0 {
		signalPermissionDisconnect(s.pendingClaims[permissionClaimKey{scope: scope, requestID: requestID}])
	}
	return prepared
}

// PreparePermissionClaimAfterDirectDelivery serializes request-ID reuse with a
// direct provider answer already in flight. It never holds pendingMu while it
// waits, so provider completion can take the normal writeMu -> controlMu ->
// pendingMu path before the replacement claim is prepared.
func (s *ControlServer) PreparePermissionClaimAfterDirectDelivery(ctx context.Context, scope, requestID string, state PermissionResolverState, options []any) bool {
	return s.PreparePermissionClaimAfterDirectDeliveryWith(ctx, scope, requestID, state, options, nil)
}

// PreparePermissionClaimAfterDirectDeliveryWith invokes onPrepared while the
// new claim is still protected by the server and pending-claim locks. The
// callback must not call ControlServer methods; stable runtimes use it only to
// publish an exact provider-generation binding before the claim is answerable.
func (s *ControlServer) PreparePermissionClaimAfterDirectDeliveryWith(ctx context.Context, scope, requestID string, state PermissionResolverState, options []any, onPrepared func()) bool {
	for {
		// Use the same s.mu -> pendingMu order as signalClientDisconnect so a
		// Reserved replacement is either created while a client is live or is
		// born with its disconnect signal already closed.
		s.mu.Lock()
		s.pendingMu.Lock()
		claim := s.pendingClaims[permissionClaimKey{scope: scope, requestID: requestID}]
		if claim == nil || claim.state != PermissionResolverDirectDelivery || claim.directDone == nil {
			prepared := s.preparePermissionClaimLocked(scope, requestID, state, options)
			if prepared && state == PermissionResolverReserved && len(s.conns) == 0 {
				signalPermissionDisconnect(s.pendingClaims[permissionClaimKey{scope: scope, requestID: requestID}])
			}
			if prepared && onPrepared != nil {
				onPrepared()
			}
			s.pendingMu.Unlock()
			s.mu.Unlock()
			return prepared
		}
		done := claim.directDone
		s.pendingMu.Unlock()
		s.mu.Unlock()

		beforeDirectPermissionClaimWait()
		select {
		case <-done:
			s.pendingMu.Lock()
			abandoned := claim.directAbandoned
			s.pendingMu.Unlock()
			if abandoned {
				return false
			}
		case <-ctx.Done():
			return false
		}
	}
}

func (s *ControlServer) preparePermissionClaimLocked(scope, requestID string, state PermissionResolverState, options []any) bool {
	key := permissionClaimKey{scope: scope, requestID: requestID}
	if existing := s.pendingClaims[key]; existing != nil && existing.state != PermissionResolverResolved {
		return false
	}
	claim := &permissionClaim{
		state:        state,
		answerCh:     make(chan PermissionAnswer, 1),
		disconnectCh: make(chan struct{}),
	}
	if options != nil {
		claim.requiresMessage = make(map[string]bool, len(options))
		for _, option := range options {
			m, ok := option.(map[string]any)
			if !ok {
				continue
			}
			optionID, _ := m["optionId"].(string)
			if optionID == "" {
				continue
			}
			requiresMessage, _ := m["requiresMessage"].(bool)
			claim.requiresMessage[optionID] = requiresMessage
		}
	}
	s.pendingClaims[key] = claim
	return true
}

// SetPermissionResolverState records resolver ownership on a claim that no
// answer owns yet. A claim a direct answer has taken (DirectDelivery) or one
// that already resolved is left alone: a resolver stamping its own fallback
// state after the answer landed must not erase the resolution it recorded.
func (s *ControlServer) SetPermissionResolverState(scope, requestID string, state PermissionResolverState) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	key := permissionClaimKey{scope: scope, requestID: requestID}
	claim := s.pendingClaims[key]
	if claim == nil || claim.state == PermissionResolverResolved || claim.state == PermissionResolverDirectDelivery {
		return
	}
	claim.state = state
}

// MarkPermissionClaimResolved changes claim.state to PermissionResolverResolved
// and records the resolving option ID and exact delivered message so later
// repeats can be compared. DeliverPendingPermission returns AlreadyResolvedSame
// for an identical repeat and AlreadyResolved for a conflicting one.
// Live-only channels and queued metadata are cleared so the resolved claim
// cannot be mistaken for a pending request. Channels are safely closed to
// unblock any lingering waiters before being set to nil.
func (s *ControlServer) MarkPermissionClaimResolved(scope, requestID, source, optionID, message string) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	claim := s.pendingClaims[permissionClaimKey{scope: scope, requestID: requestID}]
	if claim == nil {
		return false
	}
	claim.state = PermissionResolverResolved
	claim.resolutionSource = source
	claim.resolutionOptionID = optionID
	claim.resolutionMessage = message
	finishDirectPermissionClaim(claim)
	if claim.answerCh != nil {
		select {
		case <-claim.answerCh:
			// drain any queued value; claim is being resolved
		default:
		}
		close(claim.answerCh)
	}
	claim.answerCh = nil
	claim.answerQueued = false
	if claim.disconnectCh != nil {
		select {
		case <-claim.disconnectCh:
			// already closed by signalClientDisconnect
		default:
			close(claim.disconnectCh)
		}
	}
	claim.disconnectCh = nil
	claim.requiresMessage = nil
	return true
}

// resolvedMatchesLocked reports whether the recorded resolution on a resolved
// claim matches the offered answer exactly. pendingMu must be held. A claim
// recorded with an empty option ID never matches: the auto-approve path can
// record ("", "") when no allow option is found, and that must not be
// matchable by the universally guessable ("", "") without knowing the real
// answer.
func resolvedMatchesLocked(claim *permissionClaim, optionID, message string) bool {
	return claim.resolutionOptionID != "" && claim.resolutionOptionID == optionID && claim.resolutionMessage == message
}

// ResolvedPermissionMatches reports whether the request already has a recorded
// resolution and, if so, whether the offered answer matches it exactly. The
// standalone check is for early dispatch paths; callers that must compare and
// deliver atomically should rely on DeliverPendingPermission's locked section.
func (s *ControlServer) ResolvedPermissionMatches(scope, requestID, optionID, message string) (resolved, matches bool) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	claim := s.pendingClaims[permissionClaimKey{scope: scope, requestID: requestID}]
	if claim == nil || claim.state != PermissionResolverResolved {
		return false, false
	}
	return true, resolvedMatchesLocked(claim, optionID, message)
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
	finishDirectPermissionClaim(claim)
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
	for key, claim := range s.pendingClaims {
		if key.scope == scope {
			abandonDirectPermissionClaim(claim)
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
// its answer channel and a disconnect channel. The disconnect channel is
// closed when all control-socket clients disconnect while the claim is in the
// Reserved or Control state, allowing the resolver to fall through to a
// fallback handler. Request IDs are only unique within an ACP session, so
// stable runtimes must use distinct scopes.
func (s *ControlServer) BeginPermissionClaim(scope, requestID string) (<-chan PermissionAnswer, <-chan struct{}, bool) {
	// resolvePermission can call Begin directly for an unprepared claim. Keep
	// that creation atomic with disconnect handling too, using the established
	// s.mu -> pendingMu lock order.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	key := permissionClaimKey{scope: scope, requestID: requestID}
	claim := s.pendingClaims[key]
	if claim == nil {
		claim = &permissionClaim{answerCh: make(chan PermissionAnswer, 1), disconnectCh: make(chan struct{})}
		s.pendingClaims[key] = claim
	} else if claim.state != PermissionResolverReserved {
		return nil, nil, false
	}
	claim.state = PermissionResolverControl
	if len(s.conns) == 0 {
		signalPermissionDisconnect(claim)
	}
	return claim.answerCh, claim.disconnectCh, true
}

func (s *ControlServer) EndPermissionClaim(scope, requestID string) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	key := permissionClaimKey{scope: scope, requestID: requestID}
	abandonDirectPermissionClaim(s.pendingClaims[key])
	delete(s.pendingClaims, key)
}

func finishDirectPermissionClaim(claim *permissionClaim) {
	if claim == nil || claim.directDone == nil {
		return
	}
	close(claim.directDone)
	claim.directDone = nil
}

func abandonDirectPermissionClaim(claim *permissionClaim) {
	if claim == nil {
		return
	}
	claim.directAbandoned = true
	finishDirectPermissionClaim(claim)
}

func signalPermissionDisconnect(claim *permissionClaim) {
	if claim == nil || claim.disconnectCh == nil {
		return
	}
	select {
	case <-claim.disconnectCh:
		// already closed
	default:
		close(claim.disconnectCh)
	}
}

// signalClientDisconnect closes the disconnect channel for every pending
// claim in the Reserved or Control state. This notifies the resolver that no
// live control-socket client can answer the permission request, so it should
// fall through to a fallback handler (file handler or no-resolver). The claims
// themselves are not removed; they remain answerable via the direct-delivery
// path until resolved, cancelled, superseded, or the session ends.
//
// The disconnect signal is intentionally one-shot: after all clients
// disconnect, the claim permanently falls through to fallback. A reconnecting
// client cannot reclaim the claim; it must use the direct-delivery
// (NoResolver) path.
func (s *ControlServer) signalClientDisconnect() {
	// Hold s.mu across the connection check and the channel-close loop so
	// that no new client can connect (via acceptLoop) and register a claim
	// (via BeginPermissionClaim) in the gap between the check and closing
	// disconnect channels. Claim preparation and admission use the same
	// s.mu → pendingMu lock order.
	s.mu.Lock()
	if len(s.conns) > 0 {
		s.mu.Unlock()
		return
	}
	s.pendingMu.Lock()
	for _, claim := range s.pendingClaims {
		if claim.state == PermissionResolverReserved || claim.state == PermissionResolverControl {
			signalPermissionDisconnect(claim)
		}
	}
	s.pendingMu.Unlock()
	if s.afterClientDisconnectScan != nil {
		s.afterClientDisconnectScan()
	}
	s.mu.Unlock()
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

// AnswerPendingPermission accepts an answer for the active permission claim.
// A repeated identical answer for an already-resolved claim is accepted;
// a conflicting one is rejected.
func (s *ControlServer) AnswerPendingPermission(scope, requestID, optionID, message string) bool {
	switch s.DeliverPendingPermission(scope, requestID, optionID, message) {
	case PermissionAnswerDelivered, PermissionAnswerAlreadyResolvedSame:
		return true
	default:
		return false
	}
}

type PermissionAnswerDelivery uint8

const (
	PermissionAnswerNotFound PermissionAnswerDelivery = iota
	PermissionAnswerDelivered
	PermissionAnswerAlreadyResolved
	PermissionAnswerAlreadyResolvedSame
	PermissionAnswerChannelFull
	PermissionAnswerResolverOwned
	PermissionAnswerNoResolver
	PermissionAnswerInvalid
)

// DeliverPendingPermission distinguishes a missing claim from a claim that
// has already resolved. It checks claim.state before validating payloads.
// For PermissionResolverResolved, it compares the offered answer against the
// recorded resolution under the same lock: an exact match returns
// AlreadyResolvedSame and any difference returns AlreadyResolved, so the
// comparison and the delivery decision are atomic.
func (s *ControlServer) DeliverPendingPermission(scope, requestID, optionID, message string) PermissionAnswerDelivery {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	claim := s.pendingClaims[permissionClaimKey{scope: scope, requestID: requestID}]
	if claim == nil {
		return PermissionAnswerNotFound
	}
	if claim.state == PermissionResolverResolved {
		if resolvedMatchesLocked(claim, optionID, message) {
			return PermissionAnswerAlreadyResolvedSame
		}
		return PermissionAnswerAlreadyResolved
	}
	if claim.requiresMessage != nil {
		requiresMessage, offered := claim.requiresMessage[optionID]
		if !offered || (requiresMessage && message == "") {
			return PermissionAnswerInvalid
		}
	}
	switch claim.state {
	case PermissionResolverReserved, PermissionResolverControl:
	case PermissionResolverNoResolver:
		claim.state = PermissionResolverDirectDelivery
		claim.directDone = make(chan struct{})
		return PermissionAnswerNoResolver
	default:
		return PermissionAnswerResolverOwned
	}
	if claim.answerQueued {
		return PermissionAnswerChannelFull
	}
	select {
	case claim.answerCh <- PermissionAnswer{RequestID: requestID, OptionID: optionID, Message: message}:
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
		cs := &connState{
			id: seq, server: s, conn: conn,
			askSlots:         make(chan struct{}, maxInflightAsksPerConn),
			waitSlots:        make(chan struct{}, maxInflightAsksPerConn),
			messageSlots:     make(chan struct{}, maxInflightAsksPerConn),
		}
		cs.ctx, cs.cancel = context.WithCancel(context.Background())
		s.mu.Lock()
		// Stop sets stopped before it closes the listener, so registering under
		// the same lock either lands in Stop's close set or is refused here.
		if s.stopped {
			s.mu.Unlock()
			cs.cancel()
			_ = conn.Close()
			return
		}
		s.conns[cs] = struct{}{}
		s.mu.Unlock()
		go s.handleConn(cs)
	}
}

func (s *ControlServer) handleConn(c *connState) {
	defer s.disconnect(c)
	defer c.cancel()
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
		if res == asyncResponse {
			// The response is written by the background goroutine once the
			// long-running handler completes or is cancelled.
			continue
		}
		if req.ID != nil {
			_ = c.writeJSON(res)
		}
	}
}

// maxInflightAsksPerConn bounds concurrent in-flight broker asks per
// connection so a single owner cannot accumulate unbounded parked goroutines
// and broker edges.
const maxInflightAsksPerConn = 8

// maxAskTimeoutMS is the server-side cap on a request's timeout_ms. It sits
// below the ask store's DefaultAskTimeout so a clamped value expires on the
// server (with structured error data) instead of racing the broker's 504.
const maxAskTimeoutMS = int64(askstore.DefaultAskTimeout-5*time.Second) / int64(time.Millisecond)

// codeWaitTurnTimeout marks a wait_turn request whose timeout_ms elapsed
// before the runtime's turn settled. Clients match this code to a typed
// timeout error (client.ErrWaitTurnTimeout).
const codeWaitTurnTimeout = -32020

// ErrWaitTurnTimeout is returned by StableHandler.RuntimeWaitTurn when the
// requested timeout elapsed before the runtime's turn settled.
var ErrWaitTurnTimeout = errors.New("wait_turn: timeout before the runtime's turn settled")

// asyncResponse marks a request dispatched to a background goroutine; the
// response is written when the goroutine completes, so handleConn must not
// write it a second time.
var asyncResponse = Response{JSONRPC: "2.0"}

// messageTransport resolves the shared transport for one message_* request and
// maps a lookup failure onto the wire error the verb returns.
type messageTransport func() (messaging.Transport, Response, bool)

// messagePrelude applies the nil-handler gate, the optional owner gate and the
// params decode every message_* verb shares. The transport lookup is deferred
// so a blocking verb can take its slot first and release it if the lookup
// fails, exactly like the hand-written verbs it replaces.
func (s *ControlServer) messagePrelude(c *connState, req Request, params any, ownerOnly bool) (messageTransport, Response, bool) {
	if s.stableHandler == nil {
		return nil, failure(req.ID, -32601, "method not found", nil), false
	}
	if ownerOnly && !s.ensureOwner(c) {
		return nil, failure(req.ID, -32010, "permission_denied", nil), false
	}
	if params != nil {
		if err := json.Unmarshal(req.Params, params); err != nil {
			return nil, failure(req.ID, -32602, "invalid params", nil), false
		}
	}
	return func() (messaging.Transport, Response, bool) {
		tr, err := s.stableHandler.MessageTransport()
		if err != nil {
			return nil, failure(req.ID, -32000, err.Error(), messageErrorData(err, "", "")), false
		}
		return tr, Response{}, true
	}, Response{}, true
}

// messageAsyncResult is one blocking verb's outcome and wire result, so the
// shared runner can render either message_wait or message_receive.
type messageAsyncResult struct {
	outcome messaging.Outcome
	payload any
}

// runMessageAsync parks one blocking message verb on a background goroutine
// bound to the connection context, holding one per-connection slot for the
// call's lifetime.
func (c *connState) runMessageAsync(req Request, messageID string, transport messageTransport, call func(context.Context, messaging.Transport) (messageAsyncResult, error)) Response {
	select {
	case c.messageSlots <- struct{}{}:
	default:
		return failure(req.ID, -32000, "too many in-flight message waits or receives on this connection", nil)
	}
	tr, resp, ok := transport()
	if !ok {
		<-c.messageSlots
		return resp
	}
	go func() {
		defer func() { <-c.messageSlots }()
		result, err := call(c.ctx, tr)
		if req.ID == nil || c.ctx.Err() != nil {
			return
		}
		if err != nil {
			_ = c.writeJSON(failure(req.ID, -32000, err.Error(), messageErrorData(err, result.outcome, messageID)))
			return
		}
		_ = c.writeJSON(success(req.ID, result.payload))
	}()
	return asyncResponse
}

func (s *ControlServer) dispatch(c *connState, req Request) Response {
	switch req.Method {
	case "identity":
		return success(req.ID, map[string]any{"token": s.identityToken})
	case "authenticate_owner":
		return s.authenticateOwner(c, req)
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
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			return failure(req.ID, CodeControlStopped, ErrControlStopped.Error(), nil)
		}
		s.subs[sub] = struct{}{}
		var replay []events.Event
		if p.Replay {
			replay, _ = s.runtimeHistoryLocked(p.RuntimeID, p.AfterSeq, p.Limit)
		}
		s.mu.Unlock()
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
	case "wait_turn":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		var p struct {
			RuntimeID string `json:"runtime_id"`
			TimeoutMS int64  `json:"timeout_ms"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
			}
		}
		if p.RuntimeID == "" {
			return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"runtime_id"}})
		}
		if p.TimeoutMS < 0 {
			return failure(req.ID, -32602, "invalid params", map[string]any{"detail": "timeout_ms must be non-negative"})
		}
		// Bound concurrent in-flight waits per connection so one owner cannot
		// accumulate unbounded parked goroutines.
		select {
		case c.waitSlots <- struct{}{}:
		default:
			return failure(req.ID, -32000, "too many in-flight wait_turn requests on this connection", nil)
		}
		// Run the wait on a background goroutine bound to the connection
		// context: a blocked wait must not stall subsequent requests on this
		// connection, and a disconnect cancels the pending wait and frees its
		// slot.
		go func() {
			defer func() { <-c.waitSlots }()
			result, err := s.stableHandler.RuntimeWaitTurn(c.ctx, p.RuntimeID, time.Duration(p.TimeoutMS)*time.Millisecond)
			if req.ID == nil || c.ctx.Err() != nil {
				return
			}
			if err != nil {
				code := -32000
				if errors.Is(err, ErrWaitTurnTimeout) {
					code = codeWaitTurnTimeout
				}
				_ = c.writeJSON(failure(req.ID, code, err.Error(), nil))
				return
			}
			_ = c.writeJSON(success(req.ID, result))
		}()
		return asyncResponse
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
			var p PermissionAnswer
			if len(req.Params) > 0 {
				if err := json.Unmarshal(req.Params, &p); err != nil {
					return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
				}
			}
			if p.RequestID == "" || p.OptionID == "" {
				return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"request_id", "option_id"}})
			}
			if err := runtime.ValidatePermissionMessage(p.Message); err != nil {
				return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
			}
			if err := s.stableHandler.RuntimeAnswerPermission(rtID, p.RequestID, p.OptionID, p.Message); err != nil {
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
		if resolved, matches := s.ResolvedPermissionMatches("", p.RequestID, p.OptionID, p.Message); resolved {
			if matches {
				return success(req.ID, map[string]any{"accepted": true})
			}
			return failure(req.ID, -32001, "permission request already resolved", nil)
		}
		if err := runtime.ValidatePermissionMessage(p.Message); err != nil {
			return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
		}
		switch delivery := s.DeliverPendingPermission("", p.RequestID, p.OptionID, p.Message); delivery {
		case PermissionAnswerDelivered, PermissionAnswerAlreadyResolvedSame:
			return success(req.ID, map[string]any{"accepted": true})
		case PermissionAnswerAlreadyResolved:
			return failure(req.ID, -32001, "permission request already resolved", nil)
		case PermissionAnswerNotFound:
			return failure(req.ID, -32001, "no_pending_permission", nil)
		default:
			return failure(req.ID, -32001, "no_pending_permission", nil)
		}
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
			var ce *admission.CapacityError
			if errors.As(err, &ce) {
				data := map[string]any{
					"source":    ce.Source,
					"retryable": ce.Retryable(),
					"limit":     ce.Limit,
					"active":    ce.Active,
				}
				if ce.RootID != "" {
					data["root_id"] = ce.RootID
				}
				return failure(req.ID, -32050, ce.Error(), data)
			}
			var ice *IdempotencyConflictError
			if errors.As(err, &ice) {
				return failure(req.ID, -32031, ice.Error(), nil)
			}
			var icap *IdempotencyCapacityError
			if errors.As(err, &icap) {
				return failure(req.ID, -32030, icap.Error(), nil)
			}
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "spawn_mcp":
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		result, err := s.stableHandler.SpawnMCP(req.Params)
		if err != nil {
			var ce *admission.CapacityError
			if errors.As(err, &ce) {
				data := map[string]any{
					"source":    ce.Source,
					"retryable": ce.Retryable(),
					"limit":     ce.Limit,
					"active":    ce.Active,
				}
				if ce.RootID != "" {
					data["root_id"] = ce.RootID
				}
				return failure(req.ID, -32050, ce.Error(), data)
			}
			var ice *IdempotencyConflictError
			if errors.As(err, &ice) {
				return failure(req.ID, -32031, ice.Error(), nil)
			}
			var icap *IdempotencyCapacityError
			if errors.As(err, &icap) {
				return failure(req.ID, -32030, icap.Error(), nil)
			}
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "list":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		return success(req.ID, s.stableHandler.List())

	case "broker_send":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		var p struct {
			FromRunID string `json:"from_run_id"`
			ToRunID   string `json:"to_run_id"`
			Message   string `json:"message"`
			Role      string `json:"role,omitempty"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", nil)
		}
		if err := s.stableHandler.BrokerSend(p.FromRunID, p.ToRunID, p.Message, p.Role); err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, map[string]any{"ok": true})
	case "broker_ask":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		var p struct {
			ToRunID   string `json:"to_run_id"`
			Message   string `json:"message"`
			Role      string `json:"role,omitempty"`
			TimeoutMS int64  `json:"timeout_ms,omitempty"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", nil)
		}
		if p.TimeoutMS < 0 {
			return failure(req.ID, -32602, "invalid params: timeout_ms must be non-negative", nil)
		}
		// The broker long-poll itself expires at its 10-minute ask timeout, so
		// a larger server-side budget cannot be honored.
		if p.TimeoutMS > maxAskTimeoutMS {
			p.TimeoutMS = maxAskTimeoutMS
		}
		// Bound concurrent in-flight asks per connection so one owner cannot
		// accumulate unbounded parked goroutines and broker edges.
		select {
		case c.askSlots <- struct{}{}:
		default:
			return failure(req.ID, -32000, "too many in-flight broker asks on this connection", nil)
		}
		// Run the ask on a background goroutine bound to the connection
		// context: an unanswered ask must not block subsequent requests on
		// this connection, and a disconnect must cancel the pending wait.
		go func() {
			defer func() { <-c.askSlots }()
			ctx := c.ctx
			if p.TimeoutMS > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Duration(p.TimeoutMS)*time.Millisecond)
				defer cancel()
			}
			result, err := s.stableHandler.BrokerAsk(ctx, p.ToRunID, p.Message, p.Role)
			// The ask itself still runs fire-and-forget, but a JSON-RPC
			// notification (no id) must not get a response frame.
			if req.ID == nil || c.ctx.Err() != nil {
				return
			}
			if err != nil {
				_ = c.writeJSON(failure(req.ID, -32000, err.Error(), askErrorData(err)))
				return
			}
			_ = c.writeJSON(success(req.ID, result))
		}()
		return asyncResponse
	case "broker_peers":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		result, err := s.stableHandler.BrokerPeers()
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "broker_cancel":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		var p struct {
			MessageID string `json:"message_id"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", nil)
		}
		if err := s.stableHandler.BrokerCancel(p.MessageID); err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, map[string]any{"ok": true})
	case "broker_receive":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		result, err := s.stableHandler.BrokerReceive()
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "broker_reply":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		var p struct {
			ToRunID string `json:"to_run_id"`
			ReplyTo string `json:"reply_to"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", nil)
		}
		if err := s.stableHandler.BrokerReply(p.ToRunID, p.ReplyTo, p.Message); err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, map[string]any{"ok": true})
	case "message_send":
		var p MessageSendParams
		transport, resp, ok := s.messagePrelude(c, req, &p, true)
		if !ok {
			return resp
		}
		tr, resp, ok := transport()
		if !ok {
			return resp
		}
		res, err := tr.Send(c.ctx, p.request(messaging.Identity{RunID: "supervisor"}))
		if err != nil {
			return failure(req.ID, -32000, err.Error(), messageErrorData(err, res.Outcome, res.MessageID))
		}
		return success(req.ID, newMessageSendResult(res))
	case "message_wait":
		var p MessageWaitParams
		transport, resp, ok := s.messagePrelude(c, req, &p, true)
		if !ok {
			return resp
		}
		if p.TimeoutMS < 0 {
			return failure(req.ID, -32602, "invalid params: timeout_ms must be non-negative", nil)
		}
		// Match broker_ask's ceiling: an ask store wait must expire before the
		// broker's own ask timeout, not race it.
		if p.TimeoutMS > maxAskTimeoutMS {
			p.TimeoutMS = maxAskTimeoutMS
		}
		return c.runMessageAsync(req, p.MessageID, transport, func(ctx context.Context, tr messaging.Transport) (messageAsyncResult, error) {
			res, err := tr.Wait(ctx, p.request(messaging.Identity{RunID: "supervisor"}))
			return messageAsyncResult{outcome: res.Outcome, payload: newMessageWaitResult(res)}, err
		})
	case "message_reply":
		var p MessageReplyParams
		transport, resp, ok := s.messagePrelude(c, req, &p, true)
		if !ok {
			return resp
		}
		tr, resp, ok := transport()
		if !ok {
			return resp
		}
		res, err := tr.Reply(c.ctx, p.request(messaging.Identity{RunID: "supervisor"}))
		if err != nil {
			return failure(req.ID, -32000, err.Error(), messageErrorData(err, res.Outcome, res.MessageID))
		}
		return success(req.ID, newMessageSendResult(res))
	case "message_withdraw":
		var p MessageWithdrawParams
		transport, resp, ok := s.messagePrelude(c, req, &p, true)
		if !ok {
			return resp
		}
		tr, resp, ok := transport()
		if !ok {
			return resp
		}
		res, err := tr.Withdraw(c.ctx, p.request(messaging.Identity{RunID: "supervisor"}))
		if err != nil {
			return failure(req.ID, -32000, err.Error(), messageErrorData(err, res.Outcome, p.MessageID))
		}
		return success(req.ID, newMessageWithdrawResult(res))
	case "message_receive":
		var p MessageReceiveParams
		transport, resp, ok := s.messagePrelude(c, req, &p, false)
		if !ok {
			return resp
		}
		if p.TimeoutMS < 0 {
			return failure(req.ID, -32602, "invalid params: timeout_ms must be non-negative", nil)
		}
		// The receive shares message_wait's ceiling and slot budget: both are
		// blocking verbs whose transport may park until its own deadline.
		if p.TimeoutMS > maxAskTimeoutMS {
			p.TimeoutMS = maxAskTimeoutMS
		}
		return c.runMessageAsync(req, "", transport, func(ctx context.Context, tr messaging.Transport) (messageAsyncResult, error) {
			res, err := tr.Receive(ctx, p.request(messaging.Identity{RunID: "supervisor"}))
			return messageAsyncResult{payload: newMessageReceiveResult(res)}, err
		})
	case "message_peers":
		var p MessagePeersParams
		transport, resp, ok := s.messagePrelude(c, req, &p, false)
		if !ok {
			return resp
		}
		tr, resp, ok := transport()
		if !ok {
			return resp
		}
		res, err := tr.Peers(c.ctx, p.request(messaging.Identity{RunID: "supervisor"}))
		if err != nil {
			return failure(req.ID, -32000, err.Error(), messageErrorData(err, "", ""))
		}
		return success(req.ID, newMessagePeersResult(res))
	case "message_preflight":
		transport, resp, ok := s.messagePrelude(c, req, nil, false)
		if !ok {
			return resp
		}
		tr, resp, ok := transport()
		if !ok {
			return resp
		}
		res, err := tr.Preflight(c.ctx)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), messageErrorData(err, "", ""))
		}
		return success(req.ID, newMessagePreflightResult(res))
	case "resume":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		var p ResumeParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", nil)
		}
		res, err := s.stableHandler.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, p.request())
		if err != nil {
			return failure(req.ID, -32000, err.Error(), authorityErrorData(err))
		}
		return success(req.ID, newResumeResult(res))
	case "session_rename":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		var p SessionRenameParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", nil)
		}
		res, err := s.stableHandler.SessionRename(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, p.SessionID, p.Name)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), authorityErrorData(err))
		}
		return success(req.ID, newSessionRenameResult(res))
	case "session_last":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		var p SessionLastParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", nil)
		}
		record, ok, err := s.stableHandler.SessionLast(p.RuntimeID)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), authorityErrorData(err))
		}
		return success(req.ID, newSessionLastResult(record, ok))
	case "session_last_set":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		if !s.ensureOwner(c) {
			return failure(req.ID, -32010, "permission_denied", nil)
		}
		var p SessionLastSetParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", nil)
		}
		if err := s.stableHandler.SessionLastSet(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, p.RuntimeID, p.FinalOutput, p.StopReason, p.Truncated); err != nil {
			return failure(req.ID, -32000, err.Error(), authorityErrorData(err))
		}
		return success(req.ID, map[string]any{"ok": true})
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
	case "tree_budget":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		return success(req.ID, s.stableHandler.TreeBudgetStatus())
	case "wait_for_capacity":
		if s.stableHandler == nil {
			return failure(req.ID, -32601, "method not found", nil)
		}
		var p struct {
			TimeoutMS int `json:"timeout_ms"`
		}
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params, &p)
		}
		if p.TimeoutMS <= 0 {
			p.TimeoutMS = 5000
		}
		if err := s.stableHandler.WaitForCapacityMS(p.TimeoutMS); err != nil {
			return failure(req.ID, -32000, err.Error(), map[string]any{"retryable": true})
		}
		return success(req.ID, map[string]any{"capacity_available": true})
	default:
		if req.Method == "workflow.ready" || strings.HasPrefix(req.Method, "workflow.controller.") {
			return s.dispatchWorkflowController(c, req)
		}
		if strings.HasPrefix(req.Method, "workflow.") {
			return s.dispatchWorkflow(c, req)
		}
		return failure(req.ID, -32601, "method not found", nil)
	}
}

// dispatchWorkflow routes workflow.* methods to the optional WorkflowHandler.
// It is only reached from the dispatch default branch, so no method name
// collides with an existing stable handler method.
func (s *ControlServer) dispatchWorkflow(c *connState, req Request) Response {
	hp := s.workflowHandler.Load()
	if hp == nil {
		return failure(req.ID, -32601, "method not found", nil)
	}
	h := *hp
	switch req.Method {
	case "workflow.create":
		result, err := h.WorkflowCreate(req.Params)
		if err != nil {
			var tce *workflow.TemplateConflictError
			if errors.As(err, &tce) {
				return failure(req.ID, -32031, tce.Error(), nil)
			}
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.instantiate":
		result, err := h.WorkflowInstantiate(req.Params)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.status", "workflow.inspect":
		id, err := workflowIDFromParams(req.Params)
		if err != nil {
			return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
		}
		var result any
		if req.Method == "workflow.status" {
			result, err = h.WorkflowStatus(id)
		} else {
			result, err = h.WorkflowInspect(id)
		}
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.wait":
		var p struct {
			WorkflowID string `json:"workflow_id"`
			TimeoutMS  int64  `json:"timeout_ms"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
		}
		if p.WorkflowID == "" {
			return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"workflow_id"}})
		}
		if p.TimeoutMS <= 0 {
			p.TimeoutMS = 5000
		}
		result, err := h.WorkflowWait(p.WorkflowID, time.Duration(p.TimeoutMS)*time.Millisecond)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.events":
		var p struct {
			WorkflowID string `json:"workflow_id"`
			AfterSeq   int64  `json:"after_seq"`
			Limit      int    `json:"limit"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
		}
		if p.WorkflowID == "" {
			return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"workflow_id"}})
		}
		result, err := h.WorkflowEvents(p.WorkflowID, p.AfterSeq, p.Limit)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.cancel":
		var p struct {
			WorkflowID string `json:"workflow_id"`
			Actor      string `json:"actor"`
			Reason     string `json:"reason"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
		}
		if p.WorkflowID == "" {
			return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"workflow_id"}})
		}
		result, err := h.WorkflowCancel(p.WorkflowID, req.Params)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.command":
		var p struct {
			WorkflowID string          `json:"workflow_id"`
			Command    json.RawMessage `json:"command"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
		}
		if p.WorkflowID == "" {
			return failure(req.ID, -32602, "invalid params", map[string]any{"required": []string{"workflow_id"}})
		}
		payload := p.Command
		if len(payload) == 0 {
			payload = req.Params
		}
		result, err := h.WorkflowCommand(p.WorkflowID, payload)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	default:
		return failure(req.ID, -32601, "method not found", nil)
	}
}

// dispatchWorkflowController routes workflow.controller.* and workflow.ready
// methods to the optional WorkflowControllerHandler. It is only reached from
// the dispatch default branch, so no method name collides with an existing
// stable or workflow handler method. These methods, like workflow.*, do not
// use the run-scoped ownership model (ensureOwner).
func (s *ControlServer) dispatchWorkflowController(c *connState, req Request) Response {
	hp := s.controllerHandler.Load()
	if hp == nil {
		return failure(req.ID, -32601, "method not found", nil)
	}
	h := *hp
	switch req.Method {
	case "workflow.controller.create":
		result, err := h.WorkflowControllerCreate(req.Params)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.controller.enable":
		id, errResp := controllerIDFromParams(req)
		if errResp != nil {
			return *errResp
		}
		result, err := h.WorkflowControllerEnable(id)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.controller.disable":
		id, errResp := controllerIDFromParams(req)
		if errResp != nil {
			return *errResp
		}
		result, err := h.WorkflowControllerDisable(id, req.Params)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.controller.status":
		id, errResp := controllerIDFromParams(req)
		if errResp != nil {
			return *errResp
		}
		result, err := h.WorkflowControllerStatus(id)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.controller.list":
		result, err := h.WorkflowControllerList()
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	case "workflow.ready":
		id, errResp := controllerIDFromParams(req)
		if errResp != nil {
			return *errResp
		}
		var p struct {
			Limit int `json:"limit"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return failure(req.ID, -32602, "invalid params", map[string]any{"detail": err.Error()})
		}
		result, err := h.WorkflowReady(id, p.Limit)
		if err != nil {
			return failure(req.ID, -32000, err.Error(), nil)
		}
		return success(req.ID, result)
	default:
		return failure(req.ID, -32601, "method not found", nil)
	}
}

// authenticateOwner compares the presented capability against the immutable
// construction principal and sets only this connection's owner bit. A failed
// attempt clears that bit and never touches another connection, admitted work
// or the principal.
func (s *ControlServer) authenticateOwner(c *connState, req Request) Response {
	var params OwnerAuthRequest
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return ownerAuthFailure(req.ID, CodeUnsupportedOwnerAuth, "malformed-params")
		}
	}
	if params.Version != OwnerAuthVersion {
		reason := "version-" + strconv.Itoa(params.Version)
		s.setConnectionOwner(c, false)
		return ownerAuthFailure(req.ID, CodeUnsupportedOwnerAuth, reason)
	}
	if !s.matchesOwnerCapability(params.CapabilityID, params.Secret) {
		s.setConnectionOwner(c, false)
		return ownerAuthFailure(req.ID, CodeOwnerUnauthenticated, "capability-mismatch")
	}
	s.setConnectionOwner(c, true)
	return success(req.ID, OwnerAuthResult{Version: OwnerAuthVersion, Authenticated: true, CapabilityID: s.owner.ID})
}

// matchesOwnerCapability compares both halves in constant time. A length
// mismatch is a plain rejection; nothing reports which half matched.
func (s *ControlServer) matchesOwnerCapability(capabilityID, secret string) bool {
	if s.owner.ID == "" || s.owner.Secret == "" {
		return false
	}
	idOK := subtle.ConstantTimeCompare([]byte(capabilityID), []byte(s.owner.ID))
	secretOK := subtle.ConstantTimeCompare([]byte(secret), []byte(s.owner.Secret))
	return idOK&secretOK == 1
}

func (s *ControlServer) setConnectionOwner(c *connState, owner bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.isOwner = owner
}

func ownerAuthFailure(id any, code int, reason string) Response {
	return failure(id, code, ownerAuthMessage(code), OwnerAuthErrorData{Version: OwnerAuthVersion, Reason: reason})
}

func ownerAuthMessage(code int) string {
	switch code {
	case CodeOwnerUnauthenticated:
		return "unauthenticated"
	case CodeOwnerAuthTimeout:
		return "owner authentication timed out"
	default:
		return "unsupported owner authentication"
	}
}

// ensureOwner reports whether this connection authenticated as the
// construction owner. Disconnection discards only that connection's bit.
func (s *ControlServer) ensureOwner(c *connState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return c.isOwner
}

func (s *ControlServer) disconnect(c *connState) {
	s.mu.Lock()
	delete(s.conns, c)
	c.isOwner = false
	for sub := range s.subs {
		if sub.conn == c {
			delete(s.subs, sub)
			sub.mu.Lock()
			sub.closeLocked()
			sub.mu.Unlock()
		}
	}
	allGone := len(s.conns) == 0
	s.mu.Unlock()
	_ = c.conn.Close()
	if allGone {
		s.signalClientDisconnect()
	}
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
		done:      make(chan struct{}),
		runtimeID: runtimeID,
		seenSeq:   map[string]int64{},
	}
	if runtimeID != "" && afterSeq > 0 {
		sub.seenSeq[runtimeID] = afterSeq
	}
	return sub
}

// waitDrained blocks until the writer loop has written everything queued at
// close, or until deadline passes.
func (s *subscriber) waitDrained(deadline time.Time) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-s.done:
	case <-timer.C:
	}
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

// enqueueLocked offers ev to the subscriber channel while the caller holds the
// subscriber lock. Every send and the close in closeLocked happen under that
// lock, so a send can never follow a close, and only loop receives. Blocking on
// the channel here would deadlock against a loop that has already taken the
// buffered event and is waiting for the same lock.
func (s *subscriber) enqueueLocked(ev events.Event) {
	select {
	case s.ch <- ev:
		return
	default:
	}
	select {
	case <-s.ch:
		s.dropped++
	default:
	}
	s.ch <- ev
}

func (s *subscriber) enqueue(ev events.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
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

// flushPending drains the events buffered before replay setup finished onto the
// delivery channel and marks the subscriber ready. A non-zero deadline stops the
// drain once it passes, so a publisher that keeps writing cannot hold shutdown
// open.
func (s *subscriber) flushPending(deadline time.Time) {
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
		if !deadline.IsZero() && !time.Now().Before(deadline) {
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
			afterSubscriberFlushEvent()
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
	s.flushPending(time.Time{})
}

func (s *subscriber) loop() {
	defer close(s.done)
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
