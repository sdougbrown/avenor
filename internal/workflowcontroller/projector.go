package workflowcontroller

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// WriteProjection renders the controller's diagnostic Markdown projection to
// controller.md under dir. The projection is a derived, human-facing artifact:
// callers treat a write failure as non-fatal and never let it block an
// already-committed state transition.
func WriteProjection(dir string, rec ControllerRecord) error {
	var b []byte
	b = fmt.Appendf(b, "# Controller %s\n\n", rec.ControllerID)
	b = fmt.Appendf(b, "- Desired state: %s\n", rec.DesiredState)
	if rec.DesiredState == DesiredDisabled && rec.DisabledReason != "" {
		b = fmt.Appendf(b, "- Disabled reason: %s\n", rec.DisabledReason)
	}
	b = fmt.Appendf(b, "- Max inflight: %d\n", rec.MaxInflight)
	b = fmt.Appendf(b, "- Revision: %d\n", rec.Revision)
	b = fmt.Appendf(b, "- Owner epoch: %d\n", rec.OwnerEpoch)
	if rec.Leader != nil {
		b = fmt.Appendf(b, "- Leader: %s (owner %s, epoch %d, expires %s)\n",
			rec.Leader.LeaseID, rec.Leader.OwnerID, rec.Leader.OwnerEpoch,
			rec.Leader.ExpiresAt.UTC().Format(time.RFC3339))
	} else {
		b = append(b, "- Leader: none\n"...)
	}
	b = fmt.Appendf(b, "- Updated: %s\n", rec.UpdatedAt.UTC().Format(time.RFC3339))
	return os.WriteFile(filepath.Join(dir, "controller.md"), b, 0o644)
}
