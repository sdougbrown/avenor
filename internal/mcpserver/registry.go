package mcpserver

import (
	"fmt"
	"sync"
	"time"
)

type RunInfo struct {
	RunID        string
	Label        string
	RuntimeID    string
	SessionID    string
	SupervisorID string
	SentinelPath string
	EventLogPath string
	Agent        string
	Backend      string
	Dir          string
	CreatedAt    time.Time
}

type RunRegistry struct {
	mu      sync.RWMutex
	byID    map[string]*RunInfo
	byLabel map[string]*RunInfo
}

func NewRunRegistry() *RunRegistry {
	return &RunRegistry{
		byID:    make(map[string]*RunInfo),
		byLabel: make(map[string]*RunInfo),
	}
}

func (r *RunRegistry) Store(info *RunInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[info.RunID]; ok {
		return fmt.Errorf("run_id already exists: %s", info.RunID)
	}
	r.byID[info.RunID] = info
	r.byLabel[info.Label] = info
	return nil
}

func (r *RunRegistry) Lookup(key string) *RunInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if info, ok := r.byID[key]; ok {
		return info
	}
	if info, ok := r.byLabel[key]; ok {
		return info
	}
	return nil
}

func (r *RunRegistry) Remove(runID string) *RunInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.byID[runID]
	if !ok {
		return nil
	}
	delete(r.byID, runID)
	if existing, ok := r.byLabel[info.Label]; ok && existing == info {
		delete(r.byLabel, info.Label)
	}
	return info
}

func (r *RunRegistry) All() []*RunInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*RunInfo, 0, len(r.byID))
	for _, info := range r.byID {
		result = append(result, info)
	}
	return result
}
