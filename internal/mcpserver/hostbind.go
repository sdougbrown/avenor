package mcpserver

import (
	"context"
	"fmt"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// hostBindTimeout bounds the one conversation bind a launch performs. The bind
// is an owner RPC on a local socket, so an answer slower than this means the
// supervisor is in trouble and a spawn must not wait with it.
const hostBindTimeout = 5 * time.Second

// hostBind is the native host conversation a trusted launcher declared for this
// server: a host the conversation bind policy serves and that host's native
// session id. It arrives with the launch, never as a tool argument, so the
// conversation a call speaks for cannot be chosen or spoofed by a model.
type hostBind struct {
	host      string
	sessionID string
}

// newHostBind validates a launch declaration. Only a host the policy mints a
// run per conversation for is bindable from a launch: a reuse-only host's
// binding is the supervisor singleton, which a launch does not claim, and an
// undeclared binding is not an error. A shared transport serves many clients,
// so one process-wide conversation cannot name it.
func newHostBind(transport, host, sessionID string) (*hostBind, error) {
	if host == "" && sessionID == "" {
		return nil, nil
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
	return &hostBind{host: host, sessionID: sessionID}, nil
}

// hostBinder is the one control verb a declared conversation needs: a narrow
// view of the owner connection every mutating tool already holds.
type hostBinder interface {
	MessageBindContext(ctx context.Context, req messaging.MessageBindRequest) (messaging.MessageBindResult, error)
}

// reportBindFailure writes one conversation-bind diagnostic, once per distinct
// line and then a single suppression note.
func (s *Server) reportBindFailure(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	log, suppressed := s.bindGate.Admit(line)
	switch {
	case log:
		fmt.Fprintf(stderrOr(s.opts.Stderr), "avenor mcp: %s\n", line)
	case suppressed:
		fmt.Fprintln(stderrOr(s.opts.Stderr), "avenor mcp: further conversation-bind failures suppressed")
	}
}

// bindHostConversation binds the declared conversation before the first call, so
// every later spawn names the run the supervisor already minted. A failure is
// reported and leaves the server serving unbound: execution does not depend on
// a messaging identity, and the next spawn tries again.
func (s *Server) bindHostConversation(ctx context.Context) {
	if s.hostBind == nil {
		return
	}
	cl, cleanup, supervisorPath, err := s.getClientForSupervisorWithPath("")
	if err != nil {
		s.reportBindFailure("supervisor unavailable: %v", err)
		return
	}
	defer cleanup()
	s.boundParentRun(ctx, cl, supervisorPath)
}

// boundParentRun returns the broker run a spawn on supervisorPath must name as
// its parent, or "" when the child starts without a parent route.
//
// A launch binds its conversation once: the run a bind returned is the parent
// for the whole life of this server, so a conversation is never silently
// reminted under a host that is still using the identity it was given. The
// binding belongs to the supervisor that minted it, so a spawn aimed elsewhere
// is told nothing rather than naming a run that supervisor would hold a record
// for and nothing would consume from.
func (s *Server) boundParentRun(ctx context.Context, cl ControlClient, supervisorPath string) string {
	if s.hostBind == nil {
		return ""
	}
	s.hostBindMu.Lock()
	boundPath, boundRun := s.hostBindPath, s.hostBindRunID
	s.hostBindMu.Unlock()
	if boundRun != "" {
		if boundPath == supervisorPath {
			return boundRun
		}
		s.reportBindFailure("spawn targets supervisor %s, but the conversation's parent run %s lives on %s, so the child starts without a parent route", supervisorPath, boundRun, boundPath)
		return ""
	}
	// The supervisor this server owns is the one the launch declaration
	// chooses. A call that names another one runs there without a parent
	// route, and that supervisor is never asked to bind the conversation.
	if defaultPath := s.getSupervisorPath(""); supervisorPath != defaultPath {
		s.reportBindFailure("spawn targets supervisor %s, but the conversation binds on this server's supervisor %s, so the child starts without a parent route", supervisorPath, defaultPath)
		return ""
	}
	binder, ok := cl.(hostBinder)
	if !ok {
		s.reportBindFailure("the control connection cannot bind a conversation")
		return ""
	}
	bindCtx, cancel := context.WithTimeout(ctx, hostBindTimeout)
	defer cancel()
	// Two spawns that arrive before the first bind both bind, outside any lock:
	// the supervisor's bind is idempotent per conversation, so they resolve to
	// one run and the second write stores the same identity.
	res, err := binder.MessageBindContext(bindCtx, messaging.MessageBindRequest{Host: s.hostBind.host, SessionID: s.hostBind.sessionID})
	if err != nil {
		s.reportBindFailure("conversation bind failed, so the child starts without a parent route: %v", err)
		return ""
	}
	s.hostBindMu.Lock()
	s.hostBindPath, s.hostBindRunID = supervisorPath, res.RunID
	s.hostBindMu.Unlock()
	return res.RunID
}
