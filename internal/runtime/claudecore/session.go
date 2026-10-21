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

	// StderrLog is the file claude's stderr is redirected into by LaunchCommand.
	// It outlives the terminal, which is the only way to read a fatal launch
	// banner — see LaunchCommand for why the pane cannot be used.
	StderrLog string

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

	// progStatus is the OSC 7501 source set by AttachProgStatus. Without one,
	// or before its first root report, the pane and transcript rules alone
	// drive the session.
	progStatus  terminal.ProgStatusSession
	progDrainMu sync.Mutex
	progNext    uint64 // guarded by progDrainMu
	osc         progTurn
}

type SessionOptions struct {
	SessionID  string
	RunID      string
	Dir        string
	TmuxName   string
	Term       terminal.Session
	Transcript *TranscriptReader
	StderrLog  string
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
		StderrLog:  opts.StderrLog,
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
	s.drainProgStatus()
	state, err := ScanTerminal(s.Ctx, s.Term)
	if err != nil {
		return
	}

	s.Mu.Lock()
	prompted := s.Prompted
	finished := s.Finished
	currentPerm := s.PendingTerminalPerm
	oscActive := s.osc.active
	rootBlocked := s.osc.root.State == terminal.ProgBlocked
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

		// Parse the permission prompt and broker it. With OSC 7501 active the
		// dialog paints just after the blocked report, so it is brokered only
		// once two captures in a row show the same options, as in the chase.
		out, err := s.Term.Capture(s.Ctx)
		if err != nil {
			return
		}
		if oscActive {
			s.Mu.Lock()
			stable := s.permissionCaptureStableLocked(out)
			s.Mu.Unlock()
			if !stable {
				return
			}
		}
		s.brokerTerminalPermission(out)

	case PaneStateActive:
		// Mark the session as active but don't emit a working event from the
		// pane: scanTranscriptTick is the authoritative working signal (it
		// fires only when Claude actually writes new JSONL records, where
		// pane-scrape "active" is a 500ms heartbeat that floods consumers).
		s.MarkActive()
	case PaneStateIdle:
		// Idle means Claude is at the prompt, waiting for next input.
		// Don't end the session — the channel supports multi-turn.
		// Clear any stale permission, unless Claude still reports itself
		// blocked: the screen can show the input box while the dialog is
		// being repainted.
		if currentPerm != nil && !rootBlocked {
			s.Mu.Lock()
			s.PendingTerminalPerm = nil
			s.Mu.Unlock()
		}
		// With OSC 7501 active, status events come from report transitions
		// rather than this heartbeat.
		if !oscActive {
			s.Emit(events.Event{Event: "agent.status", SessionID: s.SessionID, Fields: map[string]any{"phase": "waiting", "source": s.Term.Kind()}})
		}
	}
}

// brokerTerminalPermission parses a permission dialog from out and emits a
// permission.request for it, unless one is already pending or Claude still
// reports the blocked state whose dialog was already brokered. It reports
// whether a request is pending afterwards.
func (s *Session) brokerTerminalPermission(out string) bool {
	tmuxPerm := ParseTerminalPermission(out)
	if tmuxPerm == nil {
		return false
	}
	tmuxPerm.CreatedAt = time.Now()
	tmuxPerm.RequestID = uuid.New().String()

	s.Mu.Lock()
	if s.PendingTerminalPerm != nil {
		s.Mu.Unlock()
		return true
	}
	if s.permissionBrokeredLocked() {
		s.Mu.Unlock()
		return false
	}
	s.PendingTerminalPerm = tmuxPerm
	s.notePermissionBrokeredLocked()
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
	return true
}

// endTurnSettle is how long an end_turn without OSC reports waits for the
// transcript's trailing records. Claude Code writes a final reply as two
// end_turn records about a millisecond apart (thinking, then text), so a
// tick between the writes sees only the first.
var endTurnSettle = 250 * time.Millisecond

// endTurnSettleWait waits out endTurnSettle and reports whether the session's
// context is still live.
var endTurnSettleWait = func(ctx context.Context) bool {
	select {
	case <-time.After(endTurnSettle):
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Session) emitContent(rec TranscriptRecord) {
	for _, ev := range rec.ContentEvents {
		ev.SessionID = s.SessionID
		s.Emit(ev)
	}
}

// settleEndTurn waits endTurnSettle, then emits whatever the transcript
// gained meanwhile, unless the session finished during the wait (a cancel).
func (s *Session) settleEndTurn() {
	if !endTurnSettleWait(s.Ctx) {
		return
	}
	recs, _, err := s.Transcript.Tick()
	if err != nil {
		return
	}
	s.Mu.Lock()
	finished := s.Finished
	s.Mu.Unlock()
	if finished {
		return
	}
	for _, rec := range recs {
		s.emitContent(rec)
	}
}

func (s *Session) ScanTranscriptTick() {
	if s.Transcript == nil {
		return
	}
	// Apply pending reports first so an end_turn is judged against the
	// current root record.
	s.drainProgStatus()
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

	heldEndTurn := false
	for i, rec := range recs {
		s.emitContent(rec)
		if rec.StopReason == "end_turn" {
			if s.holdEndTurn() {
				// ProgStatusTick finishes the turn; keep emitting what follows.
				heldEndTurn = true
				continue
			}
			for _, rest := range recs[i+1:] {
				s.emitContent(rest)
			}
			s.settleEndTurn()
			s.finishEndTurn()
			return
		}
	}
	if heldEndTurn {
		return
	}

	s.MarkActive()
	// With OSC 7501 active, status events come from report transitions. A
	// record written before a blocked report can be read after it, and a
	// working event then would hide the wait.
	s.Mu.Lock()
	oscActive := s.osc.active
	s.Mu.Unlock()
	if oscActive {
		return
	}
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
