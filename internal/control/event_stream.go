package control

import (
	"context"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
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

func eventScopeKey(ev events.Event) string {
	return eventRuntimeID(ev)
}

func (s *ControlServer) CanonicalizeEvent(event events.Event) events.Event {
	out := events.BoundFinalOutput(events.Clone(event))
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
	key := eventRuntimeID(out)
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
				ss.Phase = phase
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
		}
		switch event.Event {
		case "session.start":
			ss.TurnState = "starting"
		case "tool.call", "agent.thought_chunk", "agent.message_chunk":
			if ss.TurnState == "starting" || ss.TurnState == "" {
				ss.TurnState = "running"
			}
		case "session.end":
			if ss.TurnState != "idle" && ss.TurnState != "cancelling" {
				ss.TurnState = "ended"
			}
		}
	})

	s.mu.Lock()
	if runtimeID := eventRuntimeID(event); runtimeID != "" {
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
	watch := make([]chan events.Event, 0, len(s.watch))
	for ch := range s.watch {
		watch = append(watch, ch)
	}
	s.mu.Unlock()

	for _, sub := range subs {
		sub.enqueue(event)
	}
	for _, ch := range watch {
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
		s.mu.Lock()
		delete(s.watch, ch)
		s.mu.Unlock()
		close(ch)
	}()
	return ch
}
