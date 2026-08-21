package workflow

import (
	"os"
	"path/filepath"
)

// CatalogedInstance pairs a workflow ID with its recovered snapshot.
type CatalogedInstance struct {
	WorkflowID WorkflowID
	Snapshot   Snapshot
}

// Catalog enumerates every recoverable instance under the root, replaying each
// instance's events beyond its snapshot revision and expiring stale leases.
// The per-instance restart-recovery work lives in recovery.go.
func (s *Store) Catalog() ([]CatalogedInstance, error) {
	if err := s.CreateRoot(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "instances"))
	if err != nil {
		return nil, err
	}
	var list []CatalogedInstance
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		wf := WorkflowID(entry.Name())
		snap, ok, err := s.recoverInstance(wf)
		if err != nil {
			return nil, err
		}
		if ok {
			list = append(list, CatalogedInstance{WorkflowID: wf, Snapshot: snap})
		}
	}
	return list, nil
}
