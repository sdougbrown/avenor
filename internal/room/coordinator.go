package room

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
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
	// Mutation-arbiter state for the current operator turn. The gate is armed
	// for the fan-out window: every head prompted from the same pre-turn
	// snapshot works against the same revision, so a write that lands after a
	// peer's turn finished is still a lost update. The gate lifts at the join,
	// before governor-driven react turns (those heads saw the mutation notice).
	writeHolder   string
	deniedOnce    map[string]bool
	arbiterActive bool
	revision      int64
	// workspaceFingerprint is the last observed git working-tree state; a
	// change between turn boundaries counts as a mutation regardless of which
	// tool (write tool or bash) caused it.
	workspaceFingerprint string
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
		client:     c,
		log:        log,
		opts:       opts,
		byRuntime:  map[string]*Head{},
		pending:    map[string]*pendingTurn{},
		deniedOnce: map[string]bool{},
	}, nil
}

// LogPath is where the room record lives inside the workspace.
func (r *Room) LogPath() string { return filepath.Join(r.opts.Dir, ".room", "log.ndjson") }

// Start spawns every head, subscribes to its event stream, and waits out the
// bootstrap orientation turn so each head enters the room with the room
// contract already in its native history.
func (r *Room) Start(ctx context.Context, specs []HeadSpec) error {
	if _, err := InstallArbiterExtension(r.opts.Dir); err != nil {
		return fmt.Errorf("install arbiter extension: %w", err)
	}
	if err := EnsurePiTrust(r.opts.Dir); err != nil {
		return fmt.Errorf("grant project trust: %w", err)
	}
	_, _ = r.log.Append("room", System, "workspace .pi extension room-arbiter.ts installed and project trust granted", VisibilityRoom, nil, 0)
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
	r.workspaceFingerprint = workspaceFingerprint(r.opts.Dir)
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
	r.resetArbiter()
	r.mu.Lock()
	r.arbiterActive = true
	r.mu.Unlock()
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

	// Join: the fan-out window closes; react turns are single-writer by
	// construction (their prompts carry the staleness notice).
	r.mu.Lock()
	r.arbiterActive = false
	r.mu.Unlock()

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

// startTurn launches the turn-completion wait before the prompt is sent so
// the settle can never be missed; wait_turn is state-machine-based (#264), so
// delivery loss cannot strand it (the per-activation deadline is the backstop).
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
	timeout := r.opts.TurnTimeout
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	go func() {
		res, err := r.client.WaitTurn(h.RuntimeID, timeout)
		if err != nil {
			res = map[string]any{}
			res["stop_reason"] = err.Error()
		}
		p.done <- TurnResult{
			RuntimeID:   h.RuntimeID,
			StopReason:  str(res["stop_reason"]),
			FinalOutput: str(res["final_output"]),
		}
	}()
	return p, nil
}

// workspaceFingerprint hashes the git working-tree state so mutations made by
// any tool (including bash) are detected at turn boundaries.
func workspaceFingerprint(dir string) string {
	out, err := exec.Command("git", "-C", dir, "status", "--porcelain", "-b").CombinedOutput()
	if err != nil {
		return ""
	}
	// Untracked files show as "??" regardless of content, so hash their
	// contents too or overwrites of untracked files are invisible.
	var sb strings.Builder
	sb.Write(out)
	for _, line := range strings.Split(string(out), "\n") {
		if len(line) < 4 || line[3] != '?' && line[2] != '?' {
			continue
		}
		name := strings.TrimPrefix(line, line[:3])
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || len(content) > 4<<20 {
			continue
		}
		sum := sha256.Sum256(content)
		sb.WriteString(hex.EncodeToString(sum[:8]))
	}
	sum := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(sum[:8])
}

// detectMutations logs a Mutation event when the workspace changed since the
// last observation. Returns true when a change was recorded.
func (r *Room) detectMutations(author Participant) bool {
	fp := workspaceFingerprint(r.opts.Dir)
	if fp == "" || fp == r.workspaceFingerprint {
		return false
	}
	r.mu.Lock()
	r.workspaceFingerprint = fp
	r.revision++
	r.mu.Unlock()
	_, _ = r.log.Append(author, Mutation, "workspace changed during "+string(author)+"'s turn", VisibilityRoom, nil, 0)
	return true
}

// await finalizes one pending activation into the room log. The completion
// signal is wait_turn (#264) — delivery-guarantee-independent — so no status
// polling or event-loss fallback is needed.
func (r *Room) await(ctx context.Context, p *pendingTurn) (Activation, error) {
	var res TurnResult
	select {
	case <-ctx.Done():
		return Activation{}, ctx.Err()
	case res = <-p.done:
	}
	r.mu.Lock()
	delete(r.pending, p.head.RuntimeID)
	r.mu.Unlock()
	debugf("await %s mode=%s depth=%d stop=%s", p.head.Name, p.mode, p.depth, res.StopReason)
	ev, err := r.log.Append(Participant(p.head.Name), HeadOutput, res.FinalOutput, VisibilityRoom, p.parents, p.depth)
	if err != nil {
		return Activation{}, err
	}
	p.head.LastOutputSeq = ev.Seq
	r.detectMutations(Participant(p.head.Name))
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

// pump subscribes to every head's event stream to resolve permission
// requests. Turn completion no longer rides the event stream (#264).
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
					if ev.Event == "permission.request" {
						r.gatePermission(h, ev)
					}
				}
			}
		}(h)
	}
}

// gatePermission resolves one head tool-permission request. The room is the
// resolver for its heads: allow unless the arbiter denies (parallel window,
// second writer). Denials carry a re-read instruction as the write-in message.
func (r *Room) gatePermission(h *Head, ev client.Event) {
	requestID, _ := ev.Raw["request_id"].(string)
	if requestID == "" {
		return
	}
	// The permission event's kind field is the dialog kind ("confirm") and
	// its tool field is best-effort and often empty for confirm dialogs, so
	// correlate with the runtime's most recent tool.call title.
	// The permission event's kind field is the dialog kind ("confirm");
	// avenor stamps the in-flight tool name into the tool field (#263).
	kind, _ := ev.Raw["tool"].(string)
	options, _ := ev.Raw["options"].([]any)
	r.mu.Lock()
	holder := r.writeHolder
	dec := Gate(h.Name, holder, kind, r.arbiterActive)
	if dec.Allow && r.arbiterActive && WriteKinds[strings.ToLower(kind)] && holder == "" {
		r.writeHolder = h.Name
	}
	// One denial per head per window: the first block tells the head to
	// re-read and reconcile; its reconciled retry is allowed, otherwise a
	// persistent model turns the gate into a retry livelock.
	if !dec.Allow {
		if r.deniedOnce[h.Name] {
			dec = ArbiterDecision{Allow: true}
		} else {
			r.deniedOnce[h.Name] = true
			_, _ = r.log.Append("room", System, fmt.Sprintf("arbiter denied %s write: %s", h.Name, dec.Message), VisibilityRoom, nil, 0)
		}
	}
	r.mu.Unlock()
	if dec.Allow {
		_ = r.client.AnswerPermission(h.RuntimeID, requestID, optionID(options, "allow"))
		return
	}
	_ = r.client.AnswerPermissionWithMessage(h.RuntimeID, requestID, optionID(options, "reject"), dec.Message)
}

// resetArbiter clears the write holder at the start of each operator turn.
func (r *Room) resetArbiter() {
	r.mu.Lock()
	r.writeHolder = ""
	r.deniedOnce = map[string]bool{}
	r.arbiterActive = false
	r.mu.Unlock()
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
