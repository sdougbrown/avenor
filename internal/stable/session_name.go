package stable

import (
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// Static permission reasons. A rename denial is a fixed sentence, not a
// template: the two levels are the whole rule.
const (
	renameOwnerDenied  = "the control owner may rename only the supervisor session"
	renameBrokerDenied = "a broker-authenticated run may rename only itself"
)

// SessionRename is the only place a rename is authorized. It validates the
// name, resolves the target, applies the caller's level, persists through
// Broker.RenameRun (the one display-name writer) together with the runtime
// registry's copy, and publishes session.renamed after the write.
func (s *Supervisor) SessionRename(caller messaging.Caller, sessionID, name string) (messaging.RenameResult, error) {
	if !messaging.ValidSessionName(name) {
		return messaging.RenameResult{}, &router.ToolError{
			Code:    "invalid_name",
			Message: messaging.SessionNameRuleMessage(name),
			Facts:   map[string]string{"name": messaging.Quote(name)},
		}
	}

	runID, err := s.resolveRenameTarget(caller, sessionID)
	if err != nil {
		return messaging.RenameResult{}, err
	}

	actor := "self"
	switch caller.Kind {
	case messaging.CallerOwner:
		if runID != "supervisor" {
			return messaging.RenameResult{}, &router.ToolError{Code: "not_permitted", Message: renameOwnerDenied}
		}
		actor = "owner"
	case messaging.CallerBroker:
		if caller.RunID != runID {
			return messaging.RenameResult{}, &router.ToolError{Code: "not_permitted", Message: renameBrokerDenied}
		}
	default:
		return messaging.RenameResult{}, &router.ToolError{Code: "not_permitted", Message: renameBrokerDenied}
	}

	previous := s.broker.DisplayName(runID)
	renamed, err := s.applySessionName(runID, name)
	if err != nil {
		return messaging.RenameResult{}, err
	}
	// Persist then event: an event failure never rolls the name back, and a
	// persist failure above publishes nothing.
	s.publishSessionRenamed(runID, renamed, previous, actor)
	return messaging.RenameResult{RunID: runID, Label: renamed, PreviousLabel: previous}, nil
}

// applySessionName writes one display name to both stores that carry it: the
// broker's SessionInfo.Label and, for a locally owned run, the child runtime's
// displayName. Both writes happen under s.controlMu, which labelHolderLocked,
// List and RuntimeStatus also hold while they read displayName, so a reader
// never sees the new broker name beside the old runtime name and a concurrent
// keyed spawn sees the old name held or the new name held, never both free. The
// supervisor session is broker-only: it has no child runtime to update.
func (s *Supervisor) applySessionName(runID, name string) (string, error) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if err := s.broker.RenameRun(runID, name); err != nil {
		return "", err
	}
	renamed := s.broker.DisplayName(runID)
	if child := s.runtimes[runID]; child != nil {
		child.mu.Lock()
		child.displayName = renamed
		child.mu.Unlock()
	}
	return renamed, nil
}

// resolveRenameTarget resolves a rename target through the one resolution
// plane: an empty session_id means the caller itself (the supervisor session
// for the owner), "supervisor" names that session, then an exact broker run id,
// then the one display name. A public run id never resolves here.
func (s *Supervisor) resolveRenameTarget(caller messaging.Caller, sessionID string) (string, error) {
	switch {
	case sessionID == "":
		if caller.Kind == messaging.CallerBroker {
			if caller.RunID == "" {
				return "", &router.ToolError{Code: "not_permitted", Message: renameBrokerDenied}
			}
			return caller.RunID, nil
		}
		return "supervisor", nil
	case sessionID == "supervisor":
		return "supervisor", nil
	}
	return s.resolveRunReference(sessionID)
}

// publishSessionRenamed publishes the rename to the control stream. runtime_id
// is set so ControlServer.PublishCanonicalEvent indexes the event into that
// runtime's history. The target's on_event log is deliberately not written.
func (s *Supervisor) publishSessionRenamed(runID, label, previous, actor string) {
	if s.control == nil {
		return
	}
	s.control.PublishEvent(events.Event{
		Event: "session.renamed",
		Fields: map[string]any{
			"run_id":         runID,
			"runtime_id":     runID,
			"label":          label,
			"previous_label": previous,
			"actor":          actor,
			"ts":             time.Now().UnixMilli(),
		},
	})
}
