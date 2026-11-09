package control

import (
	"context"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runstate"
	"github.com/sdougbrown/avenor/internal/runtime"
)

const (
	eventHistoryLimit   = 256
	historyDefaultLimit = 64
	historyMaxLimit     = eventHistoryLimit
)

type runtimeEventState struct {
	latestSeq int64
	history   []events.Event
}

type subscribeParams struct {
	RuntimeID string `json:"runtime_id"`
	Replay    bool   `json:"replay,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	AfterSeq  int64  `json:"after_seq,omitempty"`
}

type historyParams struct {
	RuntimeID string `json:"runtime_id"`
	Limit     int    `json:"limit,omitempty"`
	AfterSeq  int64  `json:"after_seq,omitempty"`
}

func normalizeHistoryLimit(limit int) int {
	if limit <= 0 {
		return historyDefaultLimit
	}
	if limit > historyMaxLimit {
		return historyMaxLimit
	}
	return limit
}

func stringField(fields map[string]any, key string) string {
	if fields == nil {
		return ""
	}
	v, _ := fields[key].(string)
	return v
}

func int64Field(value any) (int64, bool) {
	return events.Int64(value)
}

func eventRuntimeID(ev events.Event) string {
	return stringField(ev.Fields, "runtime_id")
}

// singleRunID names the one run a server owns when no supervisor supplies
// runtimes. A supervisor server has no ambient run, so it returns empty.
func (s *ControlServer) singleRunID() string {
	if s.stableHandler != nil {
		return ""
	}
	return s.state.Snapshot().RunID
}

// oneShotSnapshot returns the owned run's snapshot with the status a caller
// expects even before the first event arrives.
func (s *ControlServer) oneShotSnapshot() Snapshot {
	snap := s.state.Snapshot()
	if snap.Status == "" {
		snap.Status = string(runstate.StatusRunning)
	}
	return snap
}

// oneShotList presents the owned run in the shape a supervisor's list uses, so
// a caller can resolve it by run id or label the same way.
func (s *ControlServer) oneShotList() []map[string]any {
	snap := s.oneShotSnapshot()
	if snap.RunID == "" {
		return []map[string]any{}
	}
	return []map[string]any{s.oneShotEntry(snap)}
}

// oneShotEntry renders the owned run with the field names a supervisor's list
// and runtime-scoped status use, so a caller reads one shape from either socket.
func (s *ControlServer) oneShotEntry(snap Snapshot) map[string]any {
	exitCode := 0
	if snap.ExitCode != nil {
		exitCode = *snap.ExitCode
	}
	return map[string]any{
		"runtime_id":         snap.RunID,
		"run_id":             snap.RunID,
		"label":              snap.RunLabel,
		"session_id":         snap.SessionID,
		"status":             snap.Status,
		"stop_reason":        snap.StopReason,
		"exit_code":          exitCode,
		"phase":              string(snap.Phase),
		"phase_label":        snap.PhaseLabel,
		"started_at":         snap.StartedAt,
		"pending_permission": snap.PendingPermission,
		"latest_seq":         snap.LatestSeq,
	}
}

// eventScope indexes one event under the runtime that owns it. A one-shot
// server's events carry no runtime_id, so they fall back to the run it owns;
// this keeps its history and subscriptions addressable by that id.
func (s *ControlServer) eventScope(ev events.Event) string {
	if id := eventRuntimeID(ev); id != "" {
		return id
	}
	return s.singleRunID()
}

func (s *ControlServer) CanonicalizeEvent(event events.Event) events.Event {
	// Canonical events are written to durable NDJSON by the caller, so retain
	// the complete final_output here. Presentation boundaries are applied only
	// when publishing to status/history/subscribers below.
	out := events.Clone(event)
	if out.Fields == nil {
		out.Fields = map[string]any{}
	}
	if out.SessionID == "" {
		out.SessionID = stringField(out.Fields, "session_id")
	}

	snap := s.state.Snapshot()
	if _, ok := out.Fields["run_id"]; !ok && snap.RunID != "" {
		out.Fields["run_id"] = snap.RunID
	}
	if _, ok := out.Fields["run_label"]; !ok && snap.RunLabel != "" {
		out.Fields["run_label"] = snap.RunLabel
	}
	if _, ok := int64Field(out.Fields["ts"]); !ok {
		out.Fields["ts"] = time.Now().UnixMilli()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.eventScope(out)
	st := s.eventState[key]
	if st == nil {
		st = &runtimeEventState{}
		s.eventState[key] = st
	}
	if seq, ok := int64Field(out.Fields["seq"]); ok {
		if seq > st.latestSeq {
			st.latestSeq = seq
		}
	} else {
		st.latestSeq++
		out.Fields["seq"] = st.latestSeq
	}
	return out
}

func (s *ControlServer) PublishEvent(event events.Event) events.Event {
	stamped := s.CanonicalizeEvent(event)
	s.PublishCanonicalEvent(stamped)
	return stamped
}

func (s *ControlServer) PublishCanonicalEvent(event events.Event) {
	// Keep the terminal answer privately for the explicit result RPC, while
	// all status, replay, and subscription surfaces receive a bounded preview.
	fullFinalOutput, _ := event.Fields["final_output"].(string)
	event = events.BoundFinalOutput(event)
	s.state.Update(func(ss *Snapshot) {
		ss.LastEvent = event.Event
		if event.SessionID != "" {
			ss.SessionID = event.SessionID
		}
		if seq, ok := int64Field(event.Fields["seq"]); ok && seq > ss.LatestSeq {
			ss.LatestSeq = seq
		}
		if event.Event == "agent.status" {
			if phase, _ := event.Fields["phase"].(string); phase != "" {
				ss.Phase, _ = runstate.ParsePhase(phase)
			}
			if label, _ := event.Fields["label"].(string); label != "" {
				ss.PhaseLabel = label
			}
		}
		if event.Event == "permission.request" {
			ss.PendingPermission = true
			ss.Permission = map[string]any{}
			for k, v := range event.Fields {
				ss.Permission[k] = v
			}
		}
		if event.Event == "permission.response" {
			ss.PendingPermission = false
			ss.Permission = nil
		}
		if event.Event == "session.end" {
			if finalOutput, _ := event.Fields["final_output"].(string); finalOutput != "" {
				ss.FinalOutput = finalOutput
			}
			if fullFinalOutput != "" {
				ss.FullFinalOutput = fullFinalOutput
				ss.FinalOutputTruncated = events.FinalOutputTruncated(fullFinalOutput)
			}
		}
		switch event.Event {
		case "session.start":
			ss.TurnState = "starting"
			if s.stableHandler == nil && (ss.Status == "" || ss.Status == string(runstate.StatusEnded)) {
				ss.Status = string(runstate.StatusRunning)
			}
		case "tool.call", "agent.thought_chunk", "agent.message_chunk":
			if ss.TurnState == "starting" || ss.TurnState == "" {
				ss.TurnState = "running"
			}
		case "session.end":
			if ss.TurnState != "idle" && ss.TurnState != "cancelling" {
				ss.TurnState = "ended"
			}
			if s.stableHandler != nil {
				break
			}
			reason, _ := event.Fields["stop_reason"].(string)
			exitCode, hasExit := int64Field(event.Fields["exit_code"])
			// A retryable attempt failure is not the run's outcome: the loop
			// publishes another session.start, so report running until the last
			// allowed attempt settles.
			if hasExit && runtime.IsRetryableFailure(int(exitCode), reason) && ss.RetryAttempt <= ss.MaxRetries {
				break
			}
			ss.Status = string(runstate.StatusEnded)
			if reason != "" {
				ss.StopReason = reason
			}
			if hasExit {
				code := int(exitCode)
				ss.ExitCode = &code
			}
		}
	})

	s.mu.Lock()
	if runtimeID := s.eventScope(event); runtimeID != "" {
		st := s.eventState[runtimeID]
		if st == nil {
			st = &runtimeEventState{}
			s.eventState[runtimeID] = st
		}
		if seq, ok := int64Field(event.Fields["seq"]); ok && seq > st.latestSeq {
			st.latestSeq = seq
		}
		st.history = append(st.history, events.Clone(event))
		if len(st.history) > eventHistoryLimit {
			st.history = append([]events.Event(nil), st.history[len(st.history)-eventHistoryLimit:]...)
		}
	}
	subs := make([]*subscriber, 0, len(s.subs))
	for sub := range s.subs {
		subs = append(subs, sub)
	}
	// Fan out to the watch channels while still holding s.mu: SubscribeEvents
	// removes and closes a watch channel under the same lock, so a send can
	// never follow a close. The sends are non-blocking — the retry follows a
	// drain that made room — so a full watcher never holds the lock.
	for ch := range s.watch {
		select {
		case ch <- event:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- event:
			default:
			}
		}
	}
	s.mu.Unlock()

	for _, sub := range subs {
		sub.enqueue(event)
	}
}

func (s *ControlServer) runtimeHistory(runtimeID string, afterSeq int64, limit int) ([]events.Event, int64) {
	limit = normalizeHistoryLimit(limit)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runtimeHistoryLocked(runtimeID, afterSeq, limit)
}

func (s *ControlServer) runtimeHistoryLocked(runtimeID string, afterSeq int64, limit int) ([]events.Event, int64) {
	st := s.eventState[runtimeID]
	if st == nil {
		return nil, 0
	}
	latestSeq := st.latestSeq
	if len(st.history) == 0 {
		return nil, latestSeq
	}
	var filtered []events.Event
	for _, ev := range st.history {
		seq, _ := int64Field(ev.Fields["seq"])
		if seq <= afterSeq {
			continue
		}
		filtered = append(filtered, events.Clone(ev))
	}
	if len(filtered) > limit {
		filtered = filtered[len(filtered)-limit:]
	}
	return filtered, latestSeq
}

func (s *ControlServer) SubscribeEvents(ctx context.Context) <-chan events.Event {
	ch := make(chan events.Event, subscriberBuffer)
	s.mu.Lock()
	s.watch[ch] = struct{}{}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		// Close under s.mu so the fanout cannot send between the removal and the
		// close.
		s.mu.Lock()
		delete(s.watch, ch)
		close(ch)
		s.mu.Unlock()
	}()
	return ch
}
