package control

import (
	"sync"
	"time"
)

type Snapshot struct {
	SessionID         string         `json:"session_id,omitempty"`
	RunID             string         `json:"run_id,omitempty"`
	RunLabel          string         `json:"run_label,omitempty"`
	Phase             string         `json:"phase,omitempty"`
	PhaseLabel        string         `json:"phase_label,omitempty"`
	LastEvent         string         `json:"last_event,omitempty"`
	RetryAttempt      int            `json:"retry_attempt,omitempty"`
	MaxRetries        int            `json:"max_retries,omitempty"`
	PendingPermission bool           `json:"pending_permission"`
	Permission        map[string]any `json:"permission,omitempty"`
	StartedAt         int64          `json:"started_at"`
	UpdatedAt         int64          `json:"updated_at"`
	TurnState         string         `json:"turn_state,omitempty"`
	LatestSeq         int64          `json:"latest_seq,omitempty"`
	FinalOutput       string         `json:"final_output,omitempty"`
}

type ControlState struct {
	mu sync.RWMutex
	s  Snapshot
}

func NewState(runID, runLabel string, maxRetries int) *ControlState {
	now := time.Now().UnixMilli()
	return &ControlState{s: Snapshot{RunID: runID, RunLabel: runLabel, MaxRetries: maxRetries, StartedAt: now, UpdatedAt: now}}
}

func (s *ControlState) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := s.s
	if s.s.Permission != nil {
		out.Permission = make(map[string]any, len(s.s.Permission))
		for k, v := range s.s.Permission {
			out.Permission[k] = v
		}
	}
	return out
}

func (s *ControlState) Update(fn func(*Snapshot)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.s)
	s.s.UpdatedAt = time.Now().UnixMilli()
}
