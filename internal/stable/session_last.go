package stable

import (
	"context"
	"errors"
	"fmt"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// SessionLast reads one run's recorded last turn. It resolves the run before
// reading, so an unknown runtime is a typed unknown_target refusal while a
// known run with no recorded turn is a synthesized identity plus ok=false.
//
// A tombstone still answers here: this surface reads history by an id the caller
// already holds, where a routing boundary asks whether a target can take part.
// A run the broker does not hold is the parent's to answer when this broker has
// one, so the read follows the same seam a tell does.
func (s *Supervisor) SessionLast(rtID string) (broker.LastAgentMessage, bool, error) {
	if s.broker == nil {
		return broker.LastAgentMessage{}, false, fmt.Errorf("broker not available")
	}
	last, err := s.broker.Last(context.Background(), rtID)
	if errors.Is(err, messaging.ErrTargetUnavailable) {
		return broker.LastAgentMessage{}, false, unknownTargetError(rtID)
	}
	if err != nil {
		return broker.LastAgentMessage{}, false, errors.New("the parent broker could not answer the read")
	}
	return broker.LastAgentMessage{
		RunID:      last.RunID,
		Label:      last.Label,
		Text:       last.Text,
		StopReason: last.StopReason,
		Truncated:  last.Truncated,
		At:         last.At,
	}, last.HasRecord, nil
}

// SessionLastSet publishes the supervisor session's last turn. It is the host
// turn producer's only write path and accepts nothing but the supervisor
// session from an owner caller.
func (s *Supervisor) SessionLastSet(caller messaging.Caller, rtID, finalOutput, stopReason string, truncated bool) error {
	if caller.Kind != messaging.CallerOwner || rtID != messaging.SupervisorRunID {
		return &router.ToolError{
			Code:    "not_permitted",
			Message: "the control owner may publish only the supervisor session's last turn",
		}
	}
	if s.broker == nil {
		return fmt.Errorf("broker not available")
	}
	return s.broker.SetLastAgentMessage(rtID, finalOutput, stopReason, truncated)
}
