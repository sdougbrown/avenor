package stable

import (
	"fmt"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// maxMessageBindings bounds the conversations one supervisor binds. A bound
// conversation holds a broker run and its credential, so the limit is what
// keeps an unbounded host from accumulating unbounded identities.
const maxMessageBindings = 256

// messageBindKey identifies one bound conversation.
type messageBindKey struct {
	host      string
	sessionID string
}

// bindReusesSupervisorRun reports whether host binds its one conversation to
// the supervisor's own run instead of minting per conversation.
func bindReusesSupervisorRun(host string) bool {
	semantics, _ := messaging.BindHostSemantics(host)
	return semantics == messaging.BindReusesSupervisorRun
}

func refuseBind(err error) (messaging.MessageBindResult, error) {
	return messaging.MessageBindResult{}, &router.ToolError{Code: "refused", Message: err.Error()}
}

// MessageBind implements control.StableHandler for the owner-only message_bind
// verb: it maps one (host, native session) pair to exactly one broker-owned
// run and returns that run's messaging credentials, under the bind policy in
// internal/messaging. A reuse-only host binds the supervisor's own run with a
// single conversation bound to it; every other served host mints a run per
// conversation. A record the supervisor did not mint is refused, never
// adopted. A minting host whose binding the broker has confirmed dead —
// record removed, peer expired or retired — is reclaimed and re-answered with
// a freshly minted identity; a reuse-only host never remints and fails closed
// instead.
func (s *Supervisor) MessageBind(caller messaging.Caller, req messaging.MessageBindRequest) (messaging.MessageBindResult, error) {
	if caller.Kind != messaging.CallerOwner {
		return messaging.MessageBindResult{}, &router.ToolError{
			Code:    "not_permitted",
			Message: "message_bind requires the owner connection",
		}
	}
	if _, served := messaging.BindHostSemantics(req.Host); !served {
		return messaging.MessageBindResult{}, fmt.Errorf("message_bind does not serve host %q", req.Host)
	}
	if !messaging.ValidSessionID(req.SessionID) {
		return messaging.MessageBindResult{}, fmt.Errorf("message_bind requires a native session id of 1-%d bytes forming one safe filesystem path component with no control or formatting characters", messaging.MaxSessionIDBytes)
	}
	s.messageBindMu.Lock()
	defer s.messageBindMu.Unlock()
	if s.broker == nil || s.messaging == nil {
		return refuseBind(fmt.Errorf("broker not available"))
	}
	key := messageBindKey{host: req.Host, sessionID: req.SessionID}
	reuse := bindReusesSupervisorRun(req.Host)
	if runID, ok := s.messageBinds[key]; ok {
		res, err := s.messageBindCredentials(runID, false)
		if err == nil {
			return res, nil
		}
		if reuse {
			return refuseBind(err)
		}
		// The broker has confirmed the mapped record removed, expired or
		// retired, so the stale binding is dropped and reminted below. The
		// record itself is left to the broker's own tombstone window: it stays
		// fenced, its credential stays dead, and nothing of it — token, queue,
		// ask edges — carries into the replacement.
		delete(s.messageBinds, key)
	}
	if reuse {
		for bound := range s.messageBinds {
			if bindReusesSupervisorRun(bound.host) {
				return refuseBind(fmt.Errorf("host %s already binds session %q; one conversation per supervisor singleton", bound.host, bound.sessionID))
			}
		}
	}
	if len(s.messageBinds) >= maxMessageBindings {
		s.reclaimUnavailableBinds()
		if len(s.messageBinds) >= maxMessageBindings {
			return refuseBind(fmt.Errorf("message bind capacity reached (%d bound conversations)", maxMessageBindings))
		}
	}
	var runID string
	if reuse {
		id, token := s.registerBrokerRun()
		if id == "" || token == "" {
			return refuseBind(fmt.Errorf("supervisor broker run unavailable; host %s cannot bind", req.Host))
		}
		runID = id
	} else {
		id, err := s.mintBoundRun()
		if err != nil {
			return refuseBind(err)
		}
		runID = id
	}
	s.messageBinds[key] = runID
	return s.messageBindCredentials(runID, !reuse)
}

// mintBoundRun registers a fresh leased broker run for one conversation: a
// peer-kind record whose expiry, tombstone and reap are the broker's own peer
// lifecycle, so a dead conversation's run does not live forever. The run id
// is opaque and unguessable; a collision with an existing record is refused,
// never adopted.
func (s *Supervisor) mintBoundRun() (string, error) {
	runID := s.mintBoundRunID()
	if _, created, err := s.broker.EnsurePeerRun(runID); err != nil {
		return "", fmt.Errorf("bound run %s not adopted: %w", runID, err)
	} else if !created {
		return "", fmt.Errorf("message bind refused: run %s already holds this supervisor's record", runID)
	}
	return runID, nil
}

// reclaimUnavailableBinds drops minting-host bindings whose runs the broker's
// owned+reachable lookup reports unavailable: record gone, peer expired or
// retired, or the id now holds a record this broker did not mint. Unavailable
// records cannot revive under the scan: a lapsed lease or tombstone refuses
// every renewal path, and an unowned record renews only its own credential.
// Live bindings and the reuse-only singleton are preserved. The caller holds
// messageBindMu.
func (s *Supervisor) reclaimUnavailableBinds() {
	for key, runID := range s.messageBinds {
		if bindReusesSupervisorRun(key.host) {
			continue
		}
		if _, ok := s.broker.OwnedReachableToken(runID); ok {
			continue
		}
		delete(s.messageBinds, key)
	}
}

// messageBindCredentials reports the messaging credentials of a bound run.
// The token comes from the broker's non-minting owned+reachable read, so a
// run whose record is gone, expired, retired, or unowned is refused. The
// caller decides whether that unavailability is a refusal (a reuse-only host)
// or a reclamation (minting hosts).
func (s *Supervisor) messageBindCredentials(runID string, created bool) (messaging.MessageBindResult, error) {
	token, ok := s.broker.OwnedReachableToken(runID)
	if !ok {
		return refuseBind(fmt.Errorf("bound run %s is unowned, expired or retired; the bind fails closed", runID))
	}
	return messaging.MessageBindResult{
		BrokerURL:  s.brokerURL(),
		RunID:      runID,
		RunToken:   token,
		Generation: s.broker.Store().Generation(),
		Created:    created,
	}, nil
}

// requireBoundParent refuses a parent that this supervisor no longer holds as a
// native conversation's live binding. The lookup is non-minting: a binding the
// supervisor does not map, or whose run the broker reports removed, expired or
// retired, is refused so the host can reconnect and bind again, never adopted
// into a fresh record that no consumer reads. The caller holds no lock; the bind
// authority's own lock covers the map and the run's liveness together.
func (s *Supervisor) requireBoundParent(runID string) error {
	s.messageBindMu.Lock()
	mapped := false
	for _, bound := range s.messageBinds {
		if bound == runID {
			mapped = true
			break
		}
	}
	var tokenOK bool
	if mapped {
		_, tokenOK = s.broker.OwnedReachableToken(runID)
	}
	s.messageBindMu.Unlock()
	switch {
	case !mapped:
		return fmt.Errorf("parent run %s is not a live conversation binding on this supervisor; reconnect the host conversation to bind it again", runID)
	case !tokenOK:
		return fmt.Errorf("bound parent run %s is expired or retired; reconnect the host conversation to bind it again", runID)
	}
	return nil
}
