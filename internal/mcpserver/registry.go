package mcpserver

import (
	"fmt"
	"sync"
	"time"
)

type RunInfo struct {
	RunID            string
	Label            string
	RuntimeID        string
	SessionID        string
	SupervisorID     string
	SentinelPath     string
	EventLogPath     string
	Agent            string
	AgentProfile     string
	Model            string
	Backend          string
	RosterFile       string
	RosterEntry      string
	EffectiveAgent   string
	EffectiveModel   string
	EffectiveBackend string
	Thinking         string
	Dir              string
	AutoApprove      bool
	CreatedAt        time.Time
}

// RunRegistry stores MCP run metadata scoped by supervisor. The ID map is
// keyed by (SupervisorID, RunID) so identical run or runtime IDs on different
// supervisors cannot collide. Labels remain globally unique across the
// registry: a label always maps to exactly one run.
type RunRegistry struct {
	mu      sync.RWMutex
	byID    map[string]map[string]*RunInfo
	byLabel map[string]*RunInfo
}

func NewRunRegistry() *RunRegistry {
	return &RunRegistry{
		byID:    make(map[string]map[string]*RunInfo),
		byLabel: make(map[string]*RunInfo),
	}
}

// Store upserts info scoped by (SupervisorID, RunID). Re-storing the same run
// on the same supervisor with the same runtime ID is benign reuse. A
// different runtime ID for the same key, or a label that would map to a
// different run, is a collision and is rejected rather than silently
// overwriting a live mapping. All collision conditions are validated before
// either index is mutated, so a rejected store never disturbs live mappings.
// Staleness is owned by the discovery layer: a supervisor restart orphans its
// pre-restart entries, and the live list re-points a colliding label before a
// store so a restarted supervisor's run can reclaim a label.
func (r *RunRegistry) Store(info *RunInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var existing *RunInfo
	if e := r.byID[info.SupervisorID][info.RunID]; e != nil {
		existing = e
		if e.RuntimeID != info.RuntimeID {
			return fmt.Errorf("run %s already registered on %s with runtime %s, not %s",
				info.RunID, info.SupervisorID, e.RuntimeID, info.RuntimeID)
		}
	}
	if info.Label != "" {
		if e := r.byLabel[info.Label]; e != nil &&
			(e.SupervisorID != info.SupervisorID || e.RunID != info.RunID) {
			return fmt.Errorf("label %q already maps to run %s on %s", info.Label, e.RunID, e.SupervisorID)
		}
	}
	if existing != nil && existing.Label != "" && existing.Label != info.Label && r.byLabel[existing.Label] == existing {
		delete(r.byLabel, existing.Label)
	}
	sup := r.byID[info.SupervisorID]
	if sup == nil {
		sup = make(map[string]*RunInfo)
		r.byID[info.SupervisorID] = sup
	}
	sup[info.RunID] = info
	if info.Label != "" {
		r.byLabel[info.Label] = info
	}
	return nil
}

// Lookup returns the entry for (supervisorID, runID), or nil. Labels are not
// resolved here because they are not scoped per supervisor.
func (r *RunRegistry) Lookup(supervisorID, runID string) *RunInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byID[supervisorID][runID]
}

// LookupLabel returns the entry for (supervisorID, label), or nil. Labels are
// globally unique across the registry, so a label maps to at most one entry;
// this scoped lookup returns it only when it belongs to the given supervisor.
func (r *RunRegistry) LookupLabel(supervisorID, label string) *RunInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if info, ok := r.byLabel[label]; ok && info.SupervisorID == supervisorID {
		return info
	}
	return nil
}

// LookupUnique resolves key across all supervisors and returns an entry only
// when the key identifies exactly one run: by globally unique label, or by a
// run ID registered under exactly one supervisor. Ambiguous or unknown keys
// return nil.
func (r *RunRegistry) LookupUnique(key string) *RunInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if info, ok := r.byLabel[key]; ok {
		return info
	}
	var found *RunInfo
	for _, sup := range r.byID {
		if info, ok := sup[key]; ok {
			if found != nil {
				return nil
			}
			found = info
		}
	}
	return found
}

// Remove deletes the entry for (supervisorID, runID) and returns it, or nil
// when no such entry exists.
func (r *RunRegistry) Remove(supervisorID, runID string) *RunInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	sup := r.byID[supervisorID]
	info, ok := sup[runID]
	if !ok {
		return nil
	}
	delete(sup, runID)
	if len(sup) == 0 {
		delete(r.byID, supervisorID)
	}
	if info.Label != "" && r.byLabel[info.Label] == info {
		delete(r.byLabel, info.Label)
	}
	return info
}

// All returns every entry across all supervisors.
func (r *RunRegistry) All() []*RunInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, sup := range r.byID {
		count += len(sup)
	}
	result := make([]*RunInfo, 0, count)
	for _, sup := range r.byID {
		for _, info := range sup {
			result = append(result, info)
		}
	}
	return result
}
