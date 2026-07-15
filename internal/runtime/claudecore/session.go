package claudecore

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

type Session struct {
	SessionID string
	RunID     string
	Dir       string
	TmuxName  string

	Term       terminal.Session
	Transcript *TranscriptReader

	Events    chan events.Event
	Done      chan struct{}
	Ctx       context.Context
	CancelFn  context.CancelFunc
	StartedAt time.Time

	Mu                  sync.Mutex
	Prompted            bool
	Active              bool
	Finished            bool
	PendingTerminalPerm *TerminalPermission
}

type SessionOptions struct {
	SessionID  string
	RunID      string
	Dir        string
	TmuxName   string
	Term       terminal.Session
	Transcript *TranscriptReader
	EventsBuf  int
}

func NewSession(ctx context.Context, opts SessionOptions) *Session {
	sessCtx, cancel := context.WithCancel(ctx)
	return &Session{
		SessionID:  opts.SessionID,
		RunID:      opts.RunID,
		Dir:        opts.Dir,
		TmuxName:   opts.TmuxName,
		Term:       opts.Term,
		Transcript: opts.Transcript,
		Events:     make(chan events.Event, opts.EventsBuf),
		Done:       make(chan struct{}),
		Ctx:        sessCtx,
		CancelFn:   cancel,
		StartedAt:  time.Now(),
	}
}

func (s *Session) MarkFinished() bool {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.Finished {
		return false
	}
	s.Finished = true
	return true
}

func (s *Session) MarkActive() {
	s.Mu.Lock()
	s.Active = true
	s.Mu.Unlock()
}

func (s *Session) Emit(ev events.Event) {
	select {
	case s.Events <- ev:
	case <-s.Ctx.Done():
	}
}

func (s *Session) SendCancelKeys(ctx context.Context) error {
	return s.Term.SendKeys(ctx, terminal.KeyEsc, terminal.KeyEsc)
}

func (s *Session) ScanTerminalTick() {
	state, err := ScanTerminal(s.Ctx, s.Term)
	if err != nil {
		return
	}

	s.Mu.Lock()
	prompted := s.Prompted
	finished := s.Finished
	currentPerm := s.PendingTerminalPerm
	s.Mu.Unlock()

	if finished || !prompted {
		return
	}

	switch state {
	case PaneStatePermission:
		s.MarkActive()

		// If we already brokered this permission, wait for resolution.
		if currentPerm != nil {
			return
		}

		// Parse the permission prompt and broker it.
		out, err := s.Term.Capture(s.Ctx)
		if err != nil {
			return
		}
		tmuxPerm := ParseTerminalPermission(string(out))
		if tmuxPerm == nil {
			return
		}
		tmuxPerm.CreatedAt = time.Now()
		tmuxPerm.RequestID = uuid.New().String()

		s.Mu.Lock()
		s.PendingTerminalPerm = tmuxPerm
		s.Mu.Unlock()

		// Emit the brokered permission request.
		opts := make([]map[string]any, len(tmuxPerm.Options))
		for i, o := range tmuxPerm.Options {
			opts[i] = map[string]any{"id": o.ID, "label": o.Label}
		}
		s.Events <- events.Event{
			Event:     "permission.request",
			SessionID: s.SessionID,
			Fields: map[string]any{
				"request_id":  tmuxPerm.RequestID,
				"source":      s.Term.Kind(),
				"description": tmuxPerm.Prompt,
				"options":     opts,
			},
		}

	case PaneStateActive:
		// Mark the session as active but don't emit a working event from the
		// pane: scanTranscriptTick is the authoritative working signal (it
		// fires only when Claude actually writes new JSONL records, where
		// pane-scrape "active" is a 500ms heartbeat that floods consumers).
		s.MarkActive()
	case PaneStateIdle:
		// Idle means Claude is at the prompt, waiting for next input.
		// Don't end the session — the channel supports multi-turn.
		// Clear any stale permission.
		if currentPerm != nil {
			s.Mu.Lock()
			s.PendingTerminalPerm = nil
			s.Mu.Unlock()
		}
		s.Emit(events.Event{Event: "agent.status", SessionID: s.SessionID, Fields: map[string]any{"phase": "waiting", "source": s.Term.Kind()}})
	}
}

func (s *Session) ScanTranscriptTick() {
	if s.Transcript == nil {
		return
	}
	recs, _, err := s.Transcript.Tick()
	if err != nil || len(recs) == 0 {
		return
	}

	s.Mu.Lock()
	prompted := s.Prompted
	finished := s.Finished
	s.Mu.Unlock()
	if finished || !prompted {
		return
	}

	for _, rec := range recs {
		for _, ev := range rec.ContentEvents {
			ev.SessionID = s.SessionID
			s.Emit(ev)
		}
		if rec.StopReason == "end_turn" {
			if !s.MarkFinished() {
				return
			}
			s.Emit(events.Event{
				Event:     "session.end",
				SessionID: s.SessionID,
				Fields: map[string]any{
					"status":      "done",
					"stop_reason": "end_turn",
				},
			})
			return
		}
	}

	s.MarkActive()
	s.Emit(events.Event{
		Event:     "agent.status",
		SessionID: s.SessionID,
		Fields: map[string]any{
			"phase":   "working",
			"source":  "transcript",
			"records": len(recs),
		},
	})
}
