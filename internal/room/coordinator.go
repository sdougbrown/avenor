package room

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/client"
)

// HeadSpec describes one persistent head at spawn time.
type HeadSpec struct {
	Name     string
	Backend  string
	Model    string
	Thinking string
}

// Head is a live participant backed by one parked avenor runtime.
type Head struct {
	Name          string
	RuntimeID     string
	Model         string
	Backend       string
	LastOutputSeq int64 // room Seq of the head's most recent head_output
}

// Options bounds the room.
type Options struct {
	Dir          string
	ExcerptLimit int           // characters of any single output shown to a peer
	MaxDepth     int           // peer hops per operator turn
	MaxAuto      int           // automatic (non-fan-out) activations per operator turn
	TurnTimeout  time.Duration // per-activation deadline; 0 uses the default
	Governor     Governor
}

// Room owns participants, the room log, and the turn loop.
type Room struct {
	client *client.Client
	log    *Log
	opts   Options

	heads     []*Head
	byRuntime map[string]*Head

	mu      sync.Mutex
	pending map[string]*pendingTurn
}

type pendingTurn struct {
	head       *Head
	done       chan TurnResult
	depth      int
	mode       string
	parents    []string
	sawRunning bool // status poll observed the queued prompt actually executing
}

// TurnResult is what the pump extracts from a session.end event.
type TurnResult struct {
	RuntimeID   string
	StopReason  string
	FinalOutput string
}

func New(c *client.Client, opts Options) (*Room, error) {
	// The control server only pushes events to connections that subscribed;
	// the room subscribes globally and lets the client filter per runtime.
	if err := c.Call("subscribe", nil, nil); err != nil {
		return nil, fmt.Errorf("subscribe to control events: %w", err)
	}
	logPath := filepath.Join(opts.Dir, ".room", "log.ndjson")
	log, err := OpenLog(logPath)
	if err != nil {
		return nil, err
	}
	if opts.ExcerptLimit <= 0 {
		opts.ExcerptLimit = 1200
	}
	if opts.MaxDepth <= 0 {
		opts.MaxDepth = 2
	}
	if opts.MaxAuto <= 0 {
		opts.MaxAuto = 8
	}
	if opts.Governor == nil {
		opts.Governor = MarkerGovernor{}
	}
	return &Room{
		client:    c,
		log:       log,
		opts:      opts,
		byRuntime: map[string]*Head{},
		pending:   map[string]*pendingTurn{},
	}, nil
}

// LogPath is where the room record lives inside the workspace.
func (r *Room) LogPath() string { return filepath.Join(r.opts.Dir, ".room", "log.ndjson") }

// Start spawns every head, subscribes to its event stream, and waits out the
// bootstrap orientation turn so each head enters the room with the room
// contract already in its native history.
func (r *Room) Start(ctx context.Context, specs []HeadSpec) error {
	for _, spec := range specs {
		params := map[string]any{
			"dir":     r.opts.Dir,
			"backend": spec.Backend,
			"label":   "head-" + spec.Name,
			"prompt": fmt.Sprintf(
				"You are head %q in a shared room with an operator and peer head(s). "+
					"Wait for the operator's first instruction; acknowledge in one line.",
				spec.Name),
		}
		if spec.Model != "" {
			params["model"] = spec.Model
		}
		if spec.Thinking != "" {
			params["thinking"] = spec.Thinking
		}
		res, err := r.client.Spawn(params)
		if err != nil {
			return fmt.Errorf("spawn head %s: %w", spec.Name, err)
		}
		h := &Head{
			Name:      spec.Name,
			RuntimeID: res["runtime_id"].(string),
			Model:     str(res["effective_model"]),
			Backend:   str(res["effective_backend"]),
		}
		r.heads = append(r.heads, h)
		r.byRuntime[h.RuntimeID] = h
	}
	r.pump(ctx)
	for _, h := range r.heads {
		out, err := r.awaitBootstrap(ctx, h)
		if err != nil {
			return err
		}
		ev, err := r.log.Append(Participant(h.Name), HeadOutput, out, VisibilityRoom, nil, 0)
		if err != nil {
			return err
		}
		h.LastOutputSeq = ev.Seq
	}
	_, err := r.log.Append("room", System, "room started with heads: "+headNames(r.heads), VisibilityRoom, nil, 0)
	return err
}

// awaitBootstrap polls runtime status until the spawn turn finishes. The pump
// is not yet guaranteed to have seen session.end for a turn that may have
// completed before SubscribeRuntime attached, so status is authoritative here.
func (r *Room) awaitBootstrap(ctx context.Context, h *Head) (string, error) {
	for {
		st, err := r.client.Status(h.RuntimeID)
		if err != nil {
			return "", err
		}
		if st["status"] == "idle" || st["status"] == "ended" {
			out, _ := st["final_output"].(string)
			return out, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// HumanTurn runs one operator turn: record, fan out to targets, wait for the
// join, then let the governor drive bounded peer rounds.
func (r *Room) HumanTurn(ctx context.Context, text string, targets []string) (string, error) {
	if len(targets) == 0 {
		for _, h := range r.heads {
			targets = append(targets, h.Name)
		}
	}
	ev, err := r.log.Append("human", HumanInput, text, VisibilityRoom, nil, 0)
	if err != nil {
		return "", err
	}

	// Fan-out: same pre-turn snapshot for every target.
	type waited struct {
		name string
		p    *pendingTurn
	}
	var waits []waited
	for _, name := range targets {
		h, ok := r.head(name)
		if !ok {
			return "", fmt.Errorf("no head named %q", name)
		}
		prompt := FanoutPrompt(r.log, h, ev, r.LogPath(), r.opts.ExcerptLimit)
		p, err := r.startTurn(h, "answer", []string{ev.ID}, 0)
		if err != nil {
			return "", err
		}
		debugf("prompt %s fanout", h.Name)
		if err := r.client.Prompt(h.RuntimeID, prompt); err != nil {
			return "", fmt.Errorf("prompt %s: %w", h.Name, err)
		}
		waits = append(waits, waited{name, p})
	}

	autoBudget := r.opts.MaxAuto
	depth := 0
	var settled []Activation
	for _, w := range waits {
		act, err := r.await(ctx, w.p)
		if err != nil {
			return "", err
		}
		settled = append(settled, act)
	}

	// Guard: any blocked head returns control to the operator immediately.
	for _, a := range settled {
		if a.Blocked {
			_, _ = r.log.Append("room", GovernorDecision,
				"return to human: head "+a.Participant+" ended "+a.StopReason, VisibilityRoom, nil, depth)
			return r.digest(settled), nil
		}
	}

	// Governor rounds: bounded by depth and budget, never by model patience.
	for {
		dec := r.opts.Governor.Decide(State{
			HumanInput:      text,
			Participants:    headNameList(r.heads),
			Settled:         settled,
			Depth:           depth,
			BudgetRemaining: autoBudget,
			MaxDepth:        r.opts.MaxDepth,
		})
		if len(dec.Activate) == 0 {
			_, _ = r.log.Append("room", GovernorDecision, "return to human: "+dec.Reason, VisibilityRoom, nil, depth)
			break
		}
		_, _ = r.log.Append("room", GovernorDecision,
			fmt.Sprintf("activate %v mode=%s: %s", dec.Activate, dec.Mode, dec.Reason), VisibilityRoom, nil, depth)
		speaker, ok := r.eventByID(dec.SpeakerEventID)
		if !ok {
			return "", fmt.Errorf("governor referenced unknown event %s", dec.SpeakerEventID)
		}
		autoBudget -= len(dec.Activate)
		depth++
		for _, name := range dec.Activate {
			h, ok := r.head(name)
			if !ok {
				return "", fmt.Errorf("governor activated unknown head %q", name)
			}
			prompt := PeerPrompt(r.log, h, speaker, r.LogPath(), r.opts.ExcerptLimit)
			p, err := r.startTurn(h, dec.Mode, []string{dec.SpeakerEventID, ev.ID}, depth)
			if err != nil {
				return "", err
			}
			debugf("prompt %s react depth=%d", h.Name, depth)
			if err := r.client.Prompt(h.RuntimeID, prompt); err != nil {
				return "", fmt.Errorf("prompt %s: %w", h.Name, err)
			}
			waits = append(waits, waited{name, p})
		}
		for _, w := range waits[len(waits)-len(dec.Activate):] {
			act, err := r.await(ctx, w.p)
			if err != nil {
				return "", err
			}
			settled = append(settled, act)
		}
	}
	return r.digest(settled), nil
}

// startTurn registers a pending activation before the prompt is sent so the
// pump cannot miss the session.end that closes it.
func (r *Room) startTurn(h *Head, mode string, parents []string, depth int) (*pendingTurn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, busy := r.pending[h.RuntimeID]; busy {
		return nil, fmt.Errorf("head %s already has a pending turn", h.Name)
	}
	p := &pendingTurn{
		head:    h,
		done:    make(chan TurnResult, 1),
		depth:   depth,
		mode:    mode,
		parents: parents,
	}
	r.pending[h.RuntimeID] = p
	return p, nil
}

// await finalizes one pending activation into the room log. Event delivery
// from the supervisor is best-effort (events may be deduped or dropped), so a
// status poll acts as the fallback completion signal: a runtime that was seen
// running and is idle again has finished its turn.
func (r *Room) await(ctx context.Context, p *pendingTurn) (Activation, error) {
	timeout := r.opts.TurnTimeout
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	deadline := time.After(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var res TurnResult
	haveRes := false
	for !haveRes {
		select {
		case <-ctx.Done():
			return Activation{}, ctx.Err()
		case <-deadline:
			return Activation{}, fmt.Errorf("turn timeout for head %s (%s)", p.head.Name, timeout)
		case res = <-p.done:
			haveRes = true
		case <-ticker.C:
			st, err := r.client.Status(p.head.RuntimeID)
			if err != nil {
				continue
			}
			switch st["status"] {
			case "running":
				p.sawRunning = true
			case "idle", "ended":
				if p.sawRunning || st["status"] == "ended" {
					res = TurnResult{
						RuntimeID:   p.head.RuntimeID,
						StopReason:  str(st["stop_reason"]),
						FinalOutput: str(st["final_output"]),
					}
					haveRes = true
					r.mu.Lock()
					delete(r.pending, p.head.RuntimeID)
					r.mu.Unlock()
				}
			}
		}
	}
	debugf("await %s mode=%s depth=%d stop=%s", p.head.Name, p.mode, p.depth, res.StopReason)
	ev, err := r.log.Append(Participant(p.head.Name), HeadOutput, res.FinalOutput, VisibilityRoom, p.parents, p.depth)
	if err != nil {
		return Activation{}, err
	}
	p.head.LastOutputSeq = ev.Seq
	return Activation{
		Participant: p.head.Name,
		EventID:     ev.ID,
		Mode:        p.mode,
		Depth:       p.depth,
		StopReason:  res.StopReason,
		FinalOutput: res.FinalOutput,
		Blocked:     res.StopReason != "" && res.StopReason != "end_turn",
	}, nil
}

// pump subscribes to every head's event stream and closes pending turns on
// session.end.
func (r *Room) pump(ctx context.Context) {
	for _, h := range r.heads {
		go func(h *Head) {
			ch := r.client.SubscribeRuntime(ctx, h.RuntimeID)
			for {
				select {
				case <-ctx.Done():
					return
				case ev := <-ch:
					debugf("pump %s event=%s", h.Name, ev.Event)
					if ev.Event != "session.end" {
						continue
					}
					res := TurnResult{RuntimeID: h.RuntimeID}
					if v, ok := ev.Raw["stop_reason"].(string); ok {
						res.StopReason = v
					}
					if v, ok := ev.Raw["final_output"].(string); ok {
						res.FinalOutput = v
					}
					r.mu.Lock()
					p := r.pending[h.RuntimeID]
					delete(r.pending, h.RuntimeID)
					r.mu.Unlock()
					if p != nil {
						p.done <- res
					}
				}
			}
		}(h)
	}
}

func (r *Room) head(name string) (*Head, bool) {
	for _, h := range r.heads {
		if h.Name == name {
			return h, true
		}
	}
	return nil, false
}

func (r *Room) eventByID(id string) (RoomEvent, bool) {
	for _, ev := range r.log.Snapshot() {
		if ev.ID == id {
			return ev, true
		}
	}
	return RoomEvent{}, false
}

// digest renders the settled activations in causal order for the operator.
func (r *Room) digest(settled []Activation) string {
	var b strings.Builder
	for _, a := range settled {
		fmt.Fprintf(&b, "\n[%s | %s]%s\n%s\n", a.Participant, a.EventID,
			blockNote(a), strings.TrimSpace(a.FinalOutput))
	}
	return b.String()
}

func blockNote(a Activation) string {
	if !a.Blocked {
		return ""
	}
	return fmt.Sprintf(" (stop_reason=%s)", a.StopReason)
}

func debugf(format string, args ...any) {
	if os.Getenv("ROOM_DEBUG") != "" {
		fmt.Fprintf(os.Stderr, "room-debug: "+format+"\n", args...)
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func headNameList(heads []*Head) []string {
	names := make([]string, len(heads))
	for i, h := range heads {
		names[i] = h.Name
	}
	return names
}

func headNames(heads []*Head) string {
	names := make([]string, len(heads))
	for i, h := range heads {
		names[i] = h.Name
	}
	return strings.Join(names, ", ")
}
