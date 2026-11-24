package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/messaging"
)

// hostBindTimeout bounds the one conversation bind a launch performs. The bind
// is an owner RPC on a local socket, so an answer slower than this means the
// supervisor is in trouble and a launch must not wait with it.
const hostBindTimeout = 5 * time.Second

// hostHandoffTimeout bounds the one handoff write. The pipe belongs to this
// handoff alone, so a write that cannot land inside this window means the
// launching host stopped reading or went away: the launch fails instead of
// waiting on it. It is a variable so a test can narrow the window it proves.
var hostHandoffTimeout = 5 * time.Second

// hostBind is the native host conversation a trusted launcher declared for this
// server, plus the identity the launch's one bind resolved to. The declaration
// arrives with the launch, never as a tool argument, so the conversation a call
// speaks for cannot be chosen or spoofed by a model. The resolved identity —
// credentials included — is retained in memory only: it is never written to a
// log, argv, file, MCP tool result or child environment.
type hostBind struct {
	host      string
	sessionID string
	// handoff is the private pipe the launching host passed in, when it asked
	// for the identity on one: the one place the conversation's messaging
	// credentials are written. The server owns it and closes it once the
	// identity has been handed over, or as soon as the launch fails, so the
	// host's read never waits on this process. A launch without a pipe hands
	// the identity to nobody and keeps it in memory.
	handoff   io.WriteCloser
	closeOnce sync.Once
	// closeErr is the one result of closing the handoff pipe.
	closeErr error

	// mu guards the identity below, which the launch binds once before the
	// server serves and every later spawn reads.
	mu    sync.Mutex
	bound boundIdentity
}

// boundIdentity is one conversation's identity as the launch's bind resolved
// it: the supervisor that holds the run, the credentials the run was minted
// with, and the directory its inbox feed claims asks in.
type boundIdentity struct {
	supervisorPath string
	result         messaging.MessageBindResult
	asksDir        string
}

// newHostBind validates a launch declaration. Only a host the policy mints a
// run per conversation for is bindable from a launch: a reuse-only host's
// binding is the supervisor singleton, which a launch does not claim, and an
// undeclared binding is not an error. A shared transport serves many clients,
// so one process-wide conversation cannot name it.
func newHostBind(transport, host, sessionID string, handoff io.WriteCloser) (*hostBind, error) {
	if host == "" && sessionID == "" && handoff == nil {
		return nil, nil
	}
	if handoff != nil && (host == "" || sessionID == "") {
		return nil, fmt.Errorf("a handoff pipe requires a host and a session id")
	}
	if host == "" || sessionID == "" {
		return nil, fmt.Errorf("a host conversation declaration needs a host and a session id together")
	}
	if transport != "stdio" {
		return nil, fmt.Errorf("a host conversation is declared only on the stdio transport, which serves one client; a shared transport has no single conversation")
	}
	semantics, served := messaging.BindHostSemantics(host)
	if !served {
		return nil, fmt.Errorf("host %q is not a native host the conversation bind serves", host)
	}
	if semantics != messaging.BindMintsPerConversation {
		return nil, fmt.Errorf("host %q binds the supervisor's own run, which a launch declaration does not claim", host)
	}
	if !messaging.ValidSessionID(sessionID) {
		return nil, fmt.Errorf("the conversation session id must be 1-%d bytes forming one safe filesystem path component with no control or formatting characters", messaging.MaxSessionIDBytes)
	}
	return &hostBind{host: host, sessionID: sessionID, handoff: handoff}, nil
}

// hostBinder is the one control verb a declared conversation needs: a narrow
// view of the owner connection every mutating tool already holds.
type hostBinder interface {
	MessageBindContext(ctx context.Context, req messaging.MessageBindRequest) (messaging.MessageBindResult, error)
}

// identity reports the supervisor that holds this conversation's run and the
// run itself. Both are empty until the launch's bind has landed.
func (b *hostBind) identity() (supervisorPath, runID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bound.supervisorPath, b.bound.result.RunID
}

// boundState reports the identity the launch bound, credentials and inbox
// directory included. The bool is false until the launch's bind has landed.
func (b *hostBind) boundState() (boundIdentity, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bound, b.bound.result.RunID != ""
}

// setIdentity stores the launch's one identity snapshot atomically, so a reader
// never sees a run id beside another bind's credentials or inbox directory.
func (b *hostBind) setIdentity(supervisorPath string, res messaging.MessageBindResult, asksDir string) {
	b.mu.Lock()
	b.bound = boundIdentity{supervisorPath: supervisorPath, result: res, asksDir: asksDir}
	b.mu.Unlock()
}

// handOff writes the conversation's messaging identity to the launching host as
// one JSON line on the inherited pipe, then closes the pipe so the host reads a
// complete identity followed by EOF. The credentials leave this process only on
// that pipe: not to a log, not to argv, not to a file, not to an MCP tool result,
// and not into a child's environment. The write is bounded so a host that stopped
// reading fails the launch instead of stalling it.
func (b *hostBind) handOff(res messaging.MessageBindResult) error {
	err := b.writeLine(res)
	// Close whatever the write did: the host reads a complete line and then
	// EOF, or a truncated line that tells it the handoff failed.
	if closeErr := b.closeHandoff(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("conversation bind for host %q session %q: hand off to the launch pipe: %w", b.host, b.sessionID, err)
	}
	return nil
}

// writeLine puts the bind result on the pipe as one JSON line under a write
// deadline. The deadline is not optional: a blocking write into a pipe nobody
// drains has no bound otherwise, and the deadline is only available when the
// writer is registered with the runtime poller — which is why the launch makes
// the inherited descriptor non-blocking before it gets here.
func (b *hostBind) writeLine(res messaging.MessageBindResult) error {
	line, err := json.Marshal(control.NewMessageBindResult(res))
	if err != nil {
		return err
	}
	deadliner, ok := b.handoff.(interface{ SetWriteDeadline(time.Time) error })
	if !ok {
		return errors.New("the handoff pipe cannot bound its write")
	}
	if err := deadliner.SetWriteDeadline(time.Now().Add(hostHandoffTimeout)); err != nil {
		return fmt.Errorf("bound the handoff write: %w", err)
	}
	if _, err := b.handoff.Write(append(line, '\n')); err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return fmt.Errorf("the host is not reading the handoff pipe within %s: %w", hostHandoffTimeout, err)
		}
		return err
	}
	return nil
}

// closeHandoff closes the handoff pipe once, whoever gets there first: the
// handoff after its write, or the launch's failure path. A launch that declared
// no pipe has nothing to close.
func (b *hostBind) closeHandoff() error {
	if b.handoff == nil {
		return nil
	}
	b.closeOnce.Do(func() { b.closeErr = b.handoff.Close() })
	return b.closeErr
}

// reportUnboundParent writes one conversation-binding diagnostic, once per
// distinct line and then a single suppression note.
func (s *Server) reportUnboundParent(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	log, suppressed := s.unboundNoteGate.Admit(line)
	switch {
	case log:
		fmt.Fprintf(stderrOr(s.opts.Stderr), "avenor mcp: %s\n", line)
	case suppressed:
		fmt.Fprintln(stderrOr(s.opts.Stderr), "avenor mcp: further conversation-route notes suppressed")
	}
}

// bindHostConversation binds the launch's declared conversation on the
// supervisor this server owns, resolves the inbox directory its feed will
// serve, and hands the identity to the launching host when the launch asked for
// it on an inherited pipe. All of it happens before the first tool call is
// served: a conversation the host cannot reach has no inbox a child could report
// to, so a launch that cannot bind (or hand off) fails rather than serve a
// binding nobody holds.
func (s *Server) bindHostConversation(ctx context.Context) error {
	if s.hostBind == nil {
		return nil
	}
	cl, cleanup, supervisorPath, err := s.getClientForSupervisorWithPath("")
	if err != nil {
		return fmt.Errorf("conversation bind for host %q session %q: supervisor unavailable: %w", s.hostBind.host, s.hostBind.sessionID, err)
	}
	defer cleanup()
	binder, ok := cl.(hostBinder)
	if !ok {
		return fmt.Errorf("conversation bind for host %q session %q: the control connection cannot bind a conversation", s.hostBind.host, s.hostBind.sessionID)
	}
	bindCtx, cancel := context.WithTimeout(ctx, hostBindTimeout)
	defer cancel()
	res, err := binder.MessageBindContext(bindCtx, messaging.MessageBindRequest{Host: s.hostBind.host, SessionID: s.hostBind.sessionID})
	if err != nil {
		return fmt.Errorf("conversation bind for host %q session %q: %w", s.hostBind.host, s.hostBind.sessionID, err)
	}
	// Resolve the inbox directory before anything is handed over or stored: an
	// invalid broker URL must fail the launch cleanly before it serves, and the
	// error names only the bind, never the credentials.
	asksDir, err := askclaim.DefaultDir(appdir.StateHome(), res.BrokerURL, res.RunID)
	if err != nil {
		return fmt.Errorf("conversation bind for host %q session %q: resolve the inbox claim directory: %w", s.hostBind.host, s.hostBind.sessionID, err)
	}
	if s.hostBind.handoff != nil {
		if err := s.hostBind.handOff(res); err != nil {
			return err
		}
	}
	s.hostBind.setIdentity(supervisorPath, res, asksDir)
	return nil
}

// boundParentRun returns the broker run a spawn on supervisorPath must name as
// its parent, or "" when the child starts without a parent route.
//
// The run is the one the launch bound, and it belongs to the supervisor that
// minted it: a spawn aimed elsewhere is told nothing, because that supervisor
// holds no record of the conversation and nothing would consume from a run it
// minted. A supervisor that no longer holds the binding refuses the parent, so a
// conversation whose identity went stale is reported to the host instead of
// silently naming a parent nothing reads.
func (s *Server) boundParentRun(supervisorPath string) string {
	if s.hostBind == nil {
		return ""
	}
	boundPath, boundRun := s.hostBind.identity()
	if boundRun == "" {
		return ""
	}
	if boundPath == supervisorPath {
		return boundRun
	}
	s.reportUnboundParent("spawn targets supervisor %s, but the conversation's parent run %s lives on %s, so the child starts without a parent route", supervisorPath, boundRun, boundPath)
	return ""
}
