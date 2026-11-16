package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

// EmitErrorEvent logs message to stderr and writes an avenor.error event to
// the event log. If the event write fails, that failure is also logged.
func EmitErrorEvent(writer EventSink, sessionID, runID, source, message string, stderr io.Writer, runLabel ...string) {
	fmt.Fprintf(stderr, "avenor: %s\n", message)
	if writer == nil {
		return
	}
	fields := map[string]any{
		"message": message,
		"source":  source,
		"ts":      time.Now().UnixMilli(),
	}
	if runID != "" {
		fields["run_id"] = runID
	}
	if len(runLabel) > 0 && runLabel[0] != "" {
		fields["run_label"] = runLabel[0]
	}
	if err := writer.Write(events.Event{
		Event:     "avenor.error",
		SessionID: sessionID,
		Fields:    fields,
	}); err != nil {
		fmt.Fprintf(stderr, "avenor: write error event: %v\n", err)
	}
}
