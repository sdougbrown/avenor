package mcpserver

import (
	"context"
	"fmt"
	"time"
)

type waitCondition string

const (
	waitTerminal     waitCondition = "terminal"
	waitPhaseChange  waitCondition = "phase_change"
	waitTurnComplete waitCondition = "turn_complete"
	waitPermission   waitCondition = "permission"
)

const waitCadence = time.Second

func parseWaitCondition(value string) (waitCondition, error) {
	if value == "" {
		return "", nil
	}
	condition := waitCondition(value)
	if !condition.valid() {
		return "", fmt.Errorf("wait_for must be one of terminal, phase_change, turn_complete, or permission")
	}
	return condition, nil
}

func (c waitCondition) valid() bool {
	switch c {
	case waitTerminal, waitPhaseChange, waitTurnComplete, waitPermission:
		return true
	default:
		return false
	}
}

func isTerminalStatus(status map[string]any) bool {
	state, _ := status["status"].(string)
	switch state {
	case "done", "failed", "timeout", "killed":
		return true
	default:
		return false
	}
}

func hasPendingPermission(status map[string]any) bool {
	value, ok := status["pending_permission"]
	if !ok || value == nil {
		return status["status"] == "waiting"
	}
	if pending, ok := value.(bool); ok {
		return pending
	}
	// Treat any non-nil object from older supervisors as pending, including
	// empty objects from sparse responses.
	return true
}

func waitConditionSatisfied(condition waitCondition, status map[string]any, initialPhase string) bool {
	// A pending permission interrupts every wait condition.
	if hasPendingPermission(status) {
		return true
	}

	switch condition {
	case waitTerminal, waitTurnComplete:
		return isTerminalStatus(status)
	case waitPhaseChange:
		phase, _ := status["phase"].(string)
		if phase != initialPhase {
			return true
		}
		// A terminal status always satisfies phase_change, even when the
		// phase string did not change (e.g. running/"" -> done/"").
		return isTerminalStatus(status)
	case waitPermission:
		return false
	default:
		return false
	}
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *Server) waitForRun(ctx context.Context, cl ControlClient, runID string, condition waitCondition, deadline time.Time) (map[string]any, bool, error) {
	if !condition.valid() {
		return nil, false, fmt.Errorf("invalid wait condition: %q", condition)
	}

	var initialPhase string
	firstSnapshot := true
	for {
		status, err := s.queryRunStatus(cl, runID)
		if err != nil {
			return nil, false, err
		}
		if firstSnapshot {
			initialPhase, _ = status["phase"].(string)
			firstSnapshot = false
		}
		if waitConditionSatisfied(condition, status, initialPhase) {
			return status, false, nil
		}

		if !deadline.IsZero() {
			remaining := deadline.Sub(s.clock())
			if remaining <= 0 {
				return status, true, nil
			}
			if remaining < waitCadence {
				if err := s.waitSleep(ctx, remaining); err != nil {
					return nil, false, err
				}
				// Re-query after sleeping to the deadline so conditions observed
				// there take precedence over timeout results.
				continue
			}
		}

		if err := s.waitSleep(ctx, waitCadence); err != nil {
			return nil, false, err
		}
	}
}

func (s *Server) waitSleep(ctx context.Context, delay time.Duration) error {
	if s.sleep != nil {
		return s.sleep(ctx, delay)
	}
	return sleepWithContext(ctx, delay)
}
