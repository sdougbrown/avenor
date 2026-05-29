package phaseconfig

import (
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

type EventWriter interface {
	Write(event events.Event) error
}

func EmitRetry(w EventWriter, runID string, attempt, maxRetries int) {
	fields := map[string]any{
		"attempt":     attempt,
		"max_retries": maxRetries,
		"ts":          time.Now().UnixMilli(),
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	_ = w.Write(events.Event{
		Event:  "avenor.retry",
		Fields: fields,
	})
}

func EmitPhaseStart(w EventWriter, runID, phase string, iteration int, kind string) error {
	fields := map[string]any{
		"ts":        time.Now().UnixMilli(),
		"phase":     phase,
		"iteration": iteration,
		"kind":      kind,
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	return w.Write(events.Event{
		Event:  "avenor.phase.start",
		Fields: fields,
	})
}

func EmitPhaseEnd(w EventWriter, runID, phase string, iteration int, stopReason string, marker *LoopMarker) error {
	fields := map[string]any{
		"ts":          time.Now().UnixMilli(),
		"phase":       phase,
		"iteration":   iteration,
		"stop_reason": stopReason,
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	if marker != nil {
		if marker.Directive == "abort" {
			fields["abort_marker"] = true
			if marker.Label != "" {
				fields["abort_marker_label"] = marker.Label
			}
		} else if marker.Directive == "exit" {
			fields["exit_marker"] = true
			if marker.Label != "" {
				fields["exit_marker_label"] = marker.Label
			}
		}
	}
	return w.Write(events.Event{
		Event:  "avenor.phase.end",
		Fields: fields,
	})
}