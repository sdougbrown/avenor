package stable

// workflow_poll.go owns the adapter-poll staging area under the workflow
// root. Before staging evidence, the leader writes the adapter's bounded raw
// stdout to a temp file here; the kernel's evidence stager hard-links it into
// the instance's immutable evidence directory and the gate command follows. A
// crash between staging and the command leaves the temp file behind; the
// startup sweep removes those orphans.

import (
	"log"
	"os"
	"path/filepath"
)

// adapterStagingDir returns the workflow root's adapter-poll staging
// directory. It is created lazily by the first staged poll.
func adapterStagingDir(root string) string {
	return filepath.Join(root, "adapter-poll")
}

// sweepOrphanedAdapterFiles removes leftover staged adapter temp files from a
// crash between evidence staging and the gate command. The evidence copies
// themselves are immutable and stay; only the staging files are removed. A
// missing directory is not an error.
func sweepOrphanedAdapterFiles(root string) {
	dir := adapterStagingDir(root)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			log.Printf("workflow: adapter sweep: remove %s: %v", entry.Name(), err)
			continue
		}
		removed++
	}
	if removed > 0 {
		log.Printf("workflow: startup sweep removed %d orphaned adapter staging file(s)", removed)
	}
}
