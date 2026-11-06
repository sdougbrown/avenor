package stable

import (
	"fmt"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// maxMessageBindings bounds the conversations one supervisor binds. A bound
// conversation holds a broker run and its credential, so the limit is what
// keeps an unbounded host from accumulating unbounded identities.
const maxMessageBindings = 256

// messageBindHosts are the hosts message_bind serves. The pi host reuses the
// supervisor's own run under a reuse-only binding; every other host mints one
// run per native conversation. A host without defined bind semantics is
// refused rather than guessed at.
var messageBindHosts = map[string]bool{"pi": true, "opencode": true}

// messageBindKey identifies one bound conversation.
type messageBindKey struct {
	host      string
	sessionID string
}

// MessageBind implements control.StableHandler for the owner-only message_bind
// verb: it maps one (host, native session) pair to exactly one broker-owned
// run and returns that run's messaging credentials. The pi host reuses the
// supervisor's own run with a single conversation bound to it; every other
// served host mints a run per conversation. A record the supervisor did not
// mint is refused, never adopted. A minting host whose binding the broker has
// confirmed dead — record removed, peer expired or retired — is reclaimed and
// re-answered with a freshly minted identity; the pi host never remints and
// fails closed instead.
func (s *Supervisor) MessageBind(caller messaging.Caller, req messaging.MessageBindRequest) (messaging.MessageBindResult, error) {
	if caller.Kind != messaging.CallerOwner {
		return messaging.MessageBindResult{}, fmt.Errorf("message_bind requires the owner connection")
	}
	if !messageBindHosts[req.Host] {
		return messaging.MessageBindResult{}, fmt.Errorf("message_bind does not serve host %q", req.Host)
	}
	if !messaging.ValidSessionID(req.SessionID) {
		return messaging.MessageBindResult{}, fmt.Errorf("message_bind requires a native session id of 1-%d bytes forming one safe filesystem path component with no control or formatting characters", messaging.MaxSessionIDBytes)
	}
	s.messageBindMu.Lock()
	defer s.messageBindMu.Unlock()
	if s.broker == nil || s.messaging == nil {
		return messaging.MessageBindResult{}, fmt.Errorf("broker not available")
	}
	key := messageBindKey{host: req.Host, sessionID: req.SessionID}
	if runID, ok := s.messageBinds[key]; ok {
		res, err := s.messageBindCredentials(runID, false)
		if err == nil {
			return res, nil
		}
		if req.Host == "pi" {
			return messaging.MessageBindResult{}, err
		}
		// The broker has confirmed the mapped record removed, expired or
		// retired, so the stale binding is dropped and reminted below. The
		// record itself is left to the broker's own tombstone window: it stays
		// fenced, its credential stays dead, and nothing of it — token, queue,
		// ask edges — carries into the replacement.
		delete(s.messageBinds, key)
	}
	if req.Host == "pi" {
		for bound := range s.messageBinds {
			if bound.host == "pi" {
				return messaging.MessageBindResult{}, fmt.Errorf("host pi already binds session %q; one conversation per supervisor singleton", bound.sessionID)
			}
		}
	}
	if len(s.messageBinds) >= maxMessageBindings {
		s.reclaimUnavailableBinds()
		if len(s.messageBinds) >= maxMessageBindings {
			return messaging.MessageBindResult{}, fmt.Errorf("message bind capacity reached (%d bound conversations)", maxMessageBindings)
		}
	}
	var runID string
	if req.Host == "pi" {
		id, token := s.registerBrokerRun()
		if id == "" || token == "" {
			return messaging.MessageBindResult{}, fmt.Errorf("supervisor broker run unavailable; host pi cannot bind")
		}
		runID = id
	} else {
		id, err := s.mintBoundRun()
		if err != nil {
			return messaging.MessageBindResult{}, err
		}
		runID = id
	}
	s.messageBinds[key] = runID
	return s.messageBindCredentials(runID, req.Host != "pi")
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
// Live bindings and the pi singleton are preserved. The caller holds
// messageBindMu.
func (s *Supervisor) reclaimUnavailableBinds() {
	for key, runID := range s.messageBinds {
		if key.host == "pi" {
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
// caller decides whether that unavailability is a refusal (pi) or a
// reclamation (minting hosts).
func (s *Supervisor) messageBindCredentials(runID string, created bool) (messaging.MessageBindResult, error) {
	token, ok := s.broker.OwnedReachableToken(runID)
	if !ok {
		return messaging.MessageBindResult{}, fmt.Errorf("bound run %s is unowned, expired or retired; the bind fails closed", runID)
	}
	return messaging.MessageBindResult{
		BrokerURL:  s.brokerURL(),
		RunID:      runID,
		RunToken:   token,
		Generation: s.broker.Store().Generation(),
		Created:    created,
	}, nil
}
