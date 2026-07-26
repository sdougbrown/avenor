package agy

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"google.golang.org/protobuf/proto"
)

const (
	maxTrajectoryToolArguments = 64 << 10
	maxTrajectoryUpdateSteps   = 4096
	maxTrajectoryTrackedSteps  = 16384
	maxTrajectoryStepText      = 8 << 20
	maxTrajectoryRetainedText  = 32 << 20
	maxTrajectoryOpaqueID      = 4 << 10
	maxTrajectoryToolName      = 256
)

type trajectoryStepKey struct {
	trajectoryID string
	index        uint32
}

type trajectoryUsage struct {
	inputTokens          uint64
	outputTokens         uint64
	cacheWriteTokens     uint64
	cacheReadTokens      uint64
	thinkingOutputTokens uint64
	responseOutputTokens uint64
}

type trajectoryStepState struct {
	hash             [sha256.Size]byte
	text             string
	textFullyEmitted bool
	usage            trajectoryUsage
	usagePresent     bool
	toolStarted      bool
	toolVisible      bool
	toolTerminal     agyv115.CortexStepStatus
	toolID           string
	toolEventID      string
	unknownEmitted   bool
}

type trajectorySnapshotMode uint8

const (
	trajectorySnapshotSeed trajectorySnapshotMode = iota
	trajectorySnapshotRecovery
)

// trajectoryMapper is deliberately transport-free. It retains replacement
// state for one opaque conversation trajectory and emits only evidence-backed
// canonical events.
type trajectoryMapper struct {
	sessionID            string
	expectedConversation string
	expectedTrajectory   string
	streamConversation   string
	trajectoryID         string
	steps                map[trajectoryStepKey]*trajectoryStepState
	turnUsage            map[trajectoryStepKey]trajectoryUsage
	turnUsagePresent     map[trajectoryStepKey]bool
	turnBegun            bool
	turnProgress         bool
	terminal             bool
	turnUnreconciled     bool
	retainedTextBytes    int
	lastPlanner          trajectoryStepKey
	hasLastPlanner       bool
	interactionObserver  func(trajectoryStepKey, *agyv115.Step, bool) []events.Event
	interactionClear     func()
}

type trajectoryMapResult struct {
	events   []events.Event
	recover  bool
	terminal bool
}

func newTrajectoryMapper(sessionID, conversationID, trajectoryID string) *trajectoryMapper {
	return &trajectoryMapper{
		sessionID:            sessionID,
		expectedConversation: conversationID,
		expectedTrajectory:   trajectoryID,
		steps:                make(map[trajectoryStepKey]*trajectoryStepState),
		turnUsage:            make(map[trajectoryStepKey]trajectoryUsage),
		turnUsagePresent:     make(map[trajectoryStepKey]bool),
	}
}

// BeginTurn is the explicit lifecycle boundary. Replacement state is retained
// so historical snapshot/stream replay remains silent, while terminal and
// usage accounting are reset for the new turn.
func (m *trajectoryMapper) BeginTurn() {
	m.turnBegun = true
	m.turnProgress = false
	m.terminal = false
	m.turnUnreconciled = false
	m.turnUsage = make(map[trajectoryStepKey]trajectoryUsage)
	m.turnUsagePresent = make(map[trajectoryStepKey]bool)
	m.hasLastPlanner = false
}

func (m *trajectoryMapper) Terminal() bool       { return m.terminal }
func (m *trajectoryMapper) TrajectoryID() string { return m.trajectoryID }

// SetInteractionObserver extends the accepted sequential mapper seam. It is
// called only while the mapper applies a stream or recovered snapshot step.
// The seed flag is true when the observation comes from a held startup
// snapshot: the bridge still records the pending identity so subsequent
// stream replays dedupe through it, but it does not surface a fresh
// permission.request event from a stale wire representation.
func (m *trajectoryMapper) SetInteractionObserver(observe func(trajectoryStepKey, *agyv115.Step, bool) []events.Event, clear func()) {
	m.interactionObserver = observe
	m.interactionClear = clear
}

// bindIdentity binds only a stream-supplied trajectory ID. A conversation ID is
// never used as a trajectory substitute. Diagnostics intentionally omit opaque
// identities.
func (m *trajectoryMapper) bindIdentity(conversationID, trajectoryID string) trajectoryMapResult {
	if conversationID == "" || trajectoryID == "" {
		return m.protocol("missing_stream_identity", true)
	}
	if len(conversationID) > maxTrajectoryOpaqueID || len(trajectoryID) > maxTrajectoryOpaqueID {
		return m.protocol("identity_too_large", true)
	}
	if m.expectedConversation != "" && conversationID != m.expectedConversation {
		return m.protocol("identity_mismatch", true)
	}
	if m.streamConversation != "" && conversationID != m.streamConversation {
		return m.protocol("identity_mismatch", true)
	}
	if m.expectedTrajectory != "" && trajectoryID != m.expectedTrajectory {
		return m.protocol("identity_mismatch", true)
	}
	if m.trajectoryID != "" && trajectoryID != m.trajectoryID {
		return m.protocol("identity_mismatch", true)
	}
	m.streamConversation = conversationID
	m.trajectoryID = trajectoryID
	return trajectoryMapResult{}
}

// BindStreamIdentity lets recovery seed a held startup snapshot before merging
// the first matching stream update.
func (m *trajectoryMapper) BindStreamIdentity(update *agyv115.AgentStateUpdate) trajectoryMapResult {
	if update == nil {
		return m.protocol("missing_update", true)
	}
	return m.bindIdentity(update.GetConversationId(), update.GetTrajectoryId())
}

func (m *trajectoryMapper) ApplyStream(update *agyv115.AgentStateUpdate) trajectoryMapResult {
	bound := m.BindStreamIdentity(update)
	if bound.recover || m.terminal {
		return bound
	}
	result := m.applySteps(update.GetMainTrajectoryUpdate().GetStepsUpdate(), false)
	if result.recover || !m.turnBegun || m.terminal {
		return result
	}
	if update.GetStatus() == agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING {
		m.turnProgress = true
	}
	if m.turnProgress && update.GetStatus() == agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE && update.GetFullyIdle() {
		m.terminal = true
		if m.interactionClear != nil {
			m.interactionClear()
		}
		result.terminal = true
		result.events = append(result.events, m.terminalEvent())
	}
	return result
}

// MergeSnapshot assigns wire-less response elements mechanically from the
// caller-supplied offset. Seed mode records historical state without emitting;
// recovery mode emits only missing transitions; identical DONE replacements remain silent.
func (m *trajectoryMapper) MergeSnapshot(trajectoryID string, offset uint32, steps []*agyv115.Step, mode trajectorySnapshotMode) trajectoryMapResult {
	if len(steps) > maxTrajectoryUpdateSteps {
		return m.protocol("snapshot_too_many_steps", true)
	}
	if m.terminal && mode != trajectorySnapshotSeed {
		return trajectoryMapResult{}
	}
	if trajectoryID == "" || m.trajectoryID == "" || trajectoryID != m.trajectoryID {
		return m.protocol("snapshot_identity_mismatch", true)
	}
	for index := range steps {
		if uint64(offset)+uint64(index) > uint64(^uint32(0)) {
			return m.protocol("snapshot_index_overflow", true)
		}
	}
	var result trajectoryMapResult
	for index, step := range steps {
		key := trajectoryStepKey{trajectoryID: trajectoryID, index: offset + uint32(index)}
		one := m.applyStep(key, step, mode == trajectorySnapshotSeed)
		result.events = append(result.events, one.events...)
		if one.recover {
			result.recover = true
			return result
		}
	}
	return result
}

func (m *trajectoryMapper) applySteps(update *agyv115.StepsUpdate, seed bool) trajectoryMapResult {
	if update == nil {
		return trajectoryMapResult{}
	}
	if len(update.GetSteps()) > maxTrajectoryUpdateSteps {
		return m.protocol("steps_update_too_large", true)
	}
	if len(update.GetIndices()) != len(update.GetSteps()) {
		return m.protocol("steps_indices_length_mismatch", true)
	}
	var result trajectoryMapResult
	for index, step := range update.GetSteps() {
		key := trajectoryStepKey{trajectoryID: m.trajectoryID, index: update.GetIndices()[index]}
		one := m.applyStep(key, step, seed)
		result.events = append(result.events, one.events...)
		if one.recover {
			result.recover = true
			return result
		}
	}
	return result
}

func (m *trajectoryMapper) applyStep(key trajectoryStepKey, step *agyv115.Step, seed bool) trajectoryMapResult {
	if step == nil {
		return m.protocol("nil_step", true)
	}
	if planner := step.GetPlannerResponse(); planner != nil && len(planner.GetResponse()) > maxTrajectoryStepText {
		return m.protocol("planner_response_too_large", true)
	}
	if tool := step.GetMetadata().GetToolCall(); tool != nil && (len(tool.GetId()) > maxTrajectoryOpaqueID || len(tool.GetName()) > maxTrajectoryToolName) {
		return m.protocol("tool_identity_too_large", true)
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(step)
	if err != nil {
		return m.protocol("step_hash_failed", true)
	}
	hash := sha256.Sum256(wire)
	state := m.steps[key]
	if state != nil && state.hash == hash {
		if !seed && m.interactionObserver != nil {
			return trajectoryMapResult{events: m.interactionObserver(key, step, false)}
		}
		return trajectoryMapResult{}
	}
	if planner := step.GetPlannerResponse(); planner != nil {
		previousBytes := 0
		if state != nil {
			previousBytes = len(state.text)
		}
		retained := m.retainedTextBytes - previousBytes + len(planner.GetResponse())
		if retained < 0 || retained > maxTrajectoryRetainedText {
			return m.protocol("retained_text_limit_exceeded", true)
		}
	}
	if state == nil {
		if len(m.steps) >= maxTrajectoryTrackedSteps {
			return m.protocol("too_many_tracked_steps", true)
		}
		state = &trajectoryStepState{}
		m.steps[key] = state
	}
	if !seed && m.turnBegun && !m.terminal {
		m.turnProgress = true
	}
	if usage, ok := usageFromStep(step); ok {
		state.usage, state.usagePresent = usage, true
		if !seed && m.turnBegun {
			m.turnUsage[key] = usage
			m.turnUsagePresent[key] = true
		}
	} else {
		state.usage, state.usagePresent = trajectoryUsage{}, false
		if !seed && m.turnBegun {
			delete(m.turnUsage, key)
			delete(m.turnUsagePresent, key)
		}
	}

	var result trajectoryMapResult
	if m.interactionObserver != nil {
		result.events = append(result.events, m.interactionObserver(key, step, seed)...)
	} else if step.GetStatus() == agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING && step.GetRequestedInteraction() != nil {
		// Without an observer, a WAITING step with a valid interaction cannot
		// be handled; emit one diagnostic and rely on the suppressed
		// unsupported_step_type below to keep the event surface quiet.
		result.events = append(result.events, m.event("avenor.agy.protocol", map[string]any{"code": "unsupported_requested_interaction"}))
	}
	switch step.GetType() {
	case agyv115.CortexStepType_CORTEX_STEP_TYPE_PLANNER_RESPONSE:
		result.events = append(result.events, m.mapPlanner(key, state, step.GetPlannerResponse(), seed)...)
	case agyv115.CortexStepType_CORTEX_STEP_TYPE_LIST_DIRECTORY:
		toolResult := m.mapTool(key, state, step, seed)
		result.events = append(result.events, toolResult.events...)
		if toolResult.recover {
			result.recover = true
			return result
		}
	default:
		// A WAITING step with a valid requested interaction is a bridge-owned
		// surface, not an unsupported step type. Suppress the diagnostic in
		// either mode (with or without an observer) so the bridge can speak
		// the typed shape without an extra noise event.
		waitingWithInteraction := step.GetStatus() == agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING && step.GetRequestedInteraction() != nil
		if !seed && !state.unknownEmitted && !waitingWithInteraction {
			state.unknownEmitted = true
			result.events = append(result.events, m.event("avenor.agy.protocol", map[string]any{"code": "unsupported_step_type", "step_index": int64(key.index), "step_type": int32(step.GetType())}))
		}
	}
	state.hash = hash
	return result
}

func (m *trajectoryMapper) mapPlanner(key trajectoryStepKey, state *trajectoryStepState, planner *agyv115.CortexStepPlannerResponse, suppress bool) []events.Event {
	if planner == nil {
		return nil
	}
	text := planner.GetResponse()
	if text == state.text {
		return nil
	}
	previous := state.text
	m.retainedTextBytes = m.retainedTextBytes - len(previous) + len(text)
	state.text = text
	if m.turnBegun {
		m.lastPlanner, m.hasLastPlanner = key, true
	}
	if suppress {
		state.textFullyEmitted = false
		return nil
	}
	if previous == "" && text != "" {
		state.textFullyEmitted = true
		return []events.Event{m.event("agent.message_chunk", map[string]any{"delta": text, "step_index": int64(key.index)})}
	}
	if len(text) >= len(previous) && text[:len(previous)] == previous {
		if state.textFullyEmitted {
			return []events.Event{m.event("agent.message_chunk", map[string]any{"delta": text[len(previous):], "step_index": int64(key.index)})}
		}
		return nil
	}
	m.turnUnreconciled = true
	state.textFullyEmitted = false
	return []events.Event{m.event("avenor.agy.reconciliation", map[string]any{"code": "planner_response_non_prefix_replacement", "step_index": int64(key.index), "action": "invalidate"})}
}

func (m *trajectoryMapper) mapTool(key trajectoryStepKey, state *trajectoryStepState, step *agyv115.Step, seed bool) trajectoryMapResult {
	tool := step.GetMetadata().GetToolCall()
	if tool == nil || tool.GetId() == "" || tool.GetName() == "" {
		return trajectoryMapResult{}
	}
	if state.toolID != "" && state.toolID != tool.GetId() {
		return m.protocol("tool_identity_changed", true)
	}
	if state.toolID == "" {
		state.toolID = tool.GetId()
		state.toolEventID = stableTrajectoryEventID("agy-tool-", key, tool.GetId())
	}
	fields := map[string]any{
		"item_id":    state.toolEventID,
		"tool_id":    state.toolEventID,
		"title":      tool.GetName(),
		"step_index": int64(key.index),
	}
	if arguments, ok := boundedJSONArguments(tool.GetArgumentsJson()); ok {
		fields["arguments"] = arguments
	}
	var out []events.Event
	if !state.toolStarted {
		state.toolStarted = true
		if !seed {
			state.toolVisible = true
			start := cloneFields(fields)
			start["status"] = "running"
			out = append(out, m.event("tool.call", start))
		}
	}
	status, terminal := toolTerminalStatus(step.GetStatus())
	if terminal && state.toolTerminal != step.GetStatus() {
		state.toolTerminal = step.GetStatus()
		if !seed && state.toolVisible {
			update := cloneFields(fields)
			update["status"] = status
			out = append(out, m.event("tool.call_update", update))
		}
	}
	return trajectoryMapResult{events: out}
}

func toolTerminalStatus(status agyv115.CortexStepStatus) (string, bool) {
	switch status {
	case agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE:
		return "completed", true
	case agyv115.CortexStepStatus_CORTEX_STEP_STATUS_ERROR:
		return "error", true
	case agyv115.CortexStepStatus_CORTEX_STEP_STATUS_CANCELED:
		return "cancelled", true
	default:
		return "", false
	}
}

func stableTrajectoryEventID(prefix string, key trajectoryStepKey, opaque string) string {
	hash := sha256.New()
	var word [4]byte
	binary.BigEndian.PutUint32(word[:], uint32(len(key.trajectoryID)))
	_, _ = hash.Write(word[:])
	_, _ = hash.Write([]byte(key.trajectoryID))
	binary.BigEndian.PutUint32(word[:], key.index)
	_, _ = hash.Write(word[:])
	binary.BigEndian.PutUint32(word[:], uint32(len(opaque)))
	_, _ = hash.Write(word[:])
	_, _ = hash.Write([]byte(opaque))
	return prefix + hex.EncodeToString(hash.Sum(nil)[:12])
}

func boundedJSONArguments(raw string) (any, bool) {
	if raw == "" || len(raw) > maxTrajectoryToolArguments || !json.Valid([]byte(raw)) {
		return nil, false
	}
	var value any
	if json.Unmarshal([]byte(raw), &value) != nil {
		return nil, false
	}
	return value, true
}

func usageFromStep(step *agyv115.Step) (trajectoryUsage, bool) {
	usage := step.GetMetadata().GetModelUsage()
	if usage == nil {
		return trajectoryUsage{}, false
	}
	return trajectoryUsage{
		inputTokens: usage.GetInputTokens(), outputTokens: usage.GetOutputTokens(), cacheWriteTokens: usage.GetCacheWriteTokens(), cacheReadTokens: usage.GetCacheReadTokens(), thinkingOutputTokens: usage.GetThinkingOutputTokens(), responseOutputTokens: usage.GetResponseOutputTokens(),
	}, true
}

func (m *trajectoryMapper) terminalEvent() events.Event {
	fields := map[string]any{"stop_reason": "end_turn"}
	if m.turnUnreconciled {
		fields["stop_reason"] = "error"
		fields["error"] = "agy response could not be reconciled safely"
		// Keep the key present so the CLI cannot promote stale streamed chunks.
		fields["final_output"] = ""
		return m.event("session.end", fields)
	}
	if usage, ok, overflow := m.aggregateUsage(); ok && !overflow {
		fields["usage"] = map[string]any{
			"input_tokens": usage.inputTokens, "output_tokens": usage.outputTokens, "cache_write_tokens": usage.cacheWriteTokens, "cache_read_tokens": usage.cacheReadTokens, "thinking_output_tokens": usage.thinkingOutputTokens, "response_output_tokens": usage.responseOutputTokens,
		}
	} else if overflow {
		fields["usage_invalid"] = true
	}
	if m.hasLastPlanner {
		state := m.steps[m.lastPlanner]
		if state != nil && state.text != "" && state.textFullyEmitted {
			fields["final_output"] = state.text
		}
	}
	return m.event("session.end", fields)
}

func (m *trajectoryMapper) aggregateUsage() (trajectoryUsage, bool, bool) {
	var total trajectoryUsage
	if len(m.turnUsagePresent) == 0 {
		return total, false, false
	}
	for key := range m.turnUsagePresent {
		usage := m.turnUsage[key]
		if addUsageChecked(&total, usage) {
			return trajectoryUsage{}, true, true
		}
	}
	return total, true, false
}

func addUsageChecked(total *trajectoryUsage, usage trajectoryUsage) bool {
	values := [][2]*uint64{
		{&total.inputTokens, &usage.inputTokens}, {&total.outputTokens, &usage.outputTokens},
		{&total.cacheWriteTokens, &usage.cacheWriteTokens}, {&total.cacheReadTokens, &usage.cacheReadTokens},
		{&total.thinkingOutputTokens, &usage.thinkingOutputTokens}, {&total.responseOutputTokens, &usage.responseOutputTokens},
	}
	for _, pair := range values {
		if math.MaxUint64-*pair[0] < *pair[1] {
			return true
		}
		*pair[0] += *pair[1]
	}
	return false
}

func (m *trajectoryMapper) protocol(code string, recover bool) trajectoryMapResult {
	return trajectoryMapResult{events: []events.Event{m.event("avenor.agy.protocol", map[string]any{"code": code})}, recover: recover}
}
func (m *trajectoryMapper) event(name string, fields map[string]any) events.Event {
	return events.Event{Event: name, SessionID: m.sessionID, Fields: fields}
}
func cloneFields(fields map[string]any) map[string]any {
	return events.Clone(events.Event{Fields: fields}).Fields
}

// Explicit markers are reserved for a later provider lifecycle integration.
func (m *trajectoryMapper) MarkCancelled() {
	m.terminal = true
	if m.interactionClear != nil {
		m.interactionClear()
	}
}
func (m *trajectoryMapper) MarkErrored() {
	m.terminal = true
	if m.interactionClear != nil {
		m.interactionClear()
	}
}

// trajectoryRecoveryTransport is deliberately small so recovery tests do not
// need an HTTP server. rpcTrajectoryRecoveryTransport is the production bridge
// to the Stage 10/11 typed APIs.
type trajectoryRecoveryStream interface {
	Recv() (*agyv115.StreamAgentStateUpdatesResponse, error)
	Close() error
}
type trajectoryRecoveryTransport interface {
	getSteps(context.Context, string, uint32, agyv115.SnapshotTrajectoryVerbosity, agyv115.StreamTrajectoryVerbosity) (*agyv115.GetCascadeTrajectoryStepsResponse, error)
	openStream(context.Context, *agyv115.StreamAgentStateUpdatesRequest) (trajectoryRecoveryStream, error)
}
type rpcTrajectoryRecoveryTransport struct{ client *rpcClient }

func (t rpcTrajectoryRecoveryTransport) getSteps(ctx context.Context, cascadeID string, offset uint32, verbosity agyv115.SnapshotTrajectoryVerbosity, trajectoryVerbosity agyv115.StreamTrajectoryVerbosity) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
	return t.client.getCascadeTrajectorySteps(ctx, cascadeID, offset, verbosity, trajectoryVerbosity)
}
func (t rpcTrajectoryRecoveryTransport) openStream(ctx context.Context, request *agyv115.StreamAgentStateUpdatesRequest) (trajectoryRecoveryStream, error) {
	return t.client.streamAgentStateUpdates(ctx, request)
}

type trajectoryRecoveryOptions struct {
	cascadeID           string
	conversationID      string
	snapshotOffset      uint32
	snapshotVerbosity   agyv115.SnapshotTrajectoryVerbosity
	streamVerbosity     agyv115.StreamTrajectoryVerbosity
	subscriberID        string
	maxRecoveryAttempts int
	backoff             func(int) time.Duration
	onEvent             func(events.Event)
	holdAfterReady      bool
}

type heldTrajectorySnapshot struct {
	offset uint32
	steps  []*agyv115.Step
}

var errTrajectoryRecoveryClosed = errors.New("agy trajectory recovery is closed")

// trajectoryRecoveryCoordinator owns one sequential stream/recovery loop. It
// does not start goroutines; Close closes the active Stage 11 stream to unblock
// Recv, and Run always closes every opened stream before returning.
type trajectoryRecoveryCoordinator struct {
	transport    trajectoryRecoveryTransport
	mapper       *trajectoryMapper
	options      trajectoryRecoveryOptions
	closed       chan struct{}
	closeOnce    sync.Once
	ready        chan struct{}
	readyOnce    sync.Once
	finished     chan struct{}
	finishOnce   sync.Once
	terminal     chan struct{}
	terminalOnce sync.Once
	release      chan struct{}
	releaseOnce  sync.Once
	released     atomic.Bool
	mu           sync.Mutex
	stream       trajectoryRecoveryStream
	runCancel    context.CancelFunc
	ran          bool
	resultErr    error
	sleep        func(context.Context, time.Duration) error
}

func newTrajectoryRecoveryCoordinator(client *rpcClient, mapper *trajectoryMapper, options trajectoryRecoveryOptions) *trajectoryRecoveryCoordinator {
	return newTrajectoryRecoveryCoordinatorWithTransport(rpcTrajectoryRecoveryTransport{client: client}, mapper, options)
}
func newTrajectoryRecoveryCoordinatorWithTransport(transport trajectoryRecoveryTransport, mapper *trajectoryMapper, options trajectoryRecoveryOptions) *trajectoryRecoveryCoordinator {
	if options.maxRecoveryAttempts <= 0 {
		options.maxRecoveryAttempts = 3
	}
	if options.backoff == nil {
		options.backoff = func(attempt int) time.Duration { return time.Duration(attempt) * 50 * time.Millisecond }
	}
	return &trajectoryRecoveryCoordinator{
		transport: transport,
		mapper:    mapper,
		options:   options,
		closed:    make(chan struct{}),
		ready:     make(chan struct{}),
		finished:  make(chan struct{}),
		terminal:  make(chan struct{}),
		release:   make(chan struct{}),
		sleep:     sleepTrajectoryRecovery,
	}
}

func (c *trajectoryRecoveryCoordinator) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.mu.Lock()
		if c.runCancel != nil {
			c.runCancel()
		}
		if c.stream != nil {
			_ = c.stream.Close()
		}
		if !c.ran {
			c.resultErr = errTrajectoryRecoveryClosed
			c.finishOnce.Do(func() { close(c.finished) })
		}
		c.mu.Unlock()
	})
	return nil
}

func (c *trajectoryRecoveryCoordinator) WaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		select {
		case <-c.closed:
			return errTrajectoryRecoveryClosed
		default:
			return nil
		}
	case <-c.finished:
		return c.result()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReleaseAfterReady lets a turn owner begin its mutation only after the
// stream has reported a quiescent IDLE baseline. It is a no-op for recovery
// coordinators that were not configured to hold after readiness.
func (c *trajectoryRecoveryCoordinator) ReleaseAfterReady() {
	c.releaseOnce.Do(func() {
		c.released.Store(true)
		close(c.release)
	})
}

func (c *trajectoryRecoveryCoordinator) Wait(ctx context.Context) error {
	select {
	case <-c.terminal:
		<-c.finished
		return c.result()
	case <-c.finished:
		return c.result()
	case <-ctx.Done():
		select {
		case <-c.terminal:
			<-c.finished
			return c.result()
		case <-c.finished:
			return c.result()
		default:
			return ctx.Err()
		}
	}
}

func (c *trajectoryRecoveryCoordinator) result() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resultErr
}

func (c *trajectoryRecoveryCoordinator) Run(ctx context.Context) (resultErr error) {
	if c.transport == nil || c.mapper == nil || c.options.cascadeID == "" || c.options.conversationID == "" {
		return errors.New("agy trajectory recovery requires transport, mapper, cascade, and conversation")
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	if c.runCancel != nil {
		c.mu.Unlock()
		cancel()
		return errors.New("agy trajectory recovery is already running")
	}
	if c.ran {
		c.mu.Unlock()
		cancel()
		return errors.New("agy trajectory recovery cannot be reused")
	}
	select {
	case <-c.closed:
		c.mu.Unlock()
		cancel()
		return errTrajectoryRecoveryClosed
	default:
	}
	c.runCancel = cancel
	c.ran = true
	c.mu.Unlock()
	defer func() {
		cancel()
		c.mu.Lock()
		c.runCancel = nil
		c.resultErr = resultErr
		c.mu.Unlock()
		c.finishOnce.Do(func() { close(c.finished) })
	}()
	ctx = runCtx
	attempts := 0
	held, attempts, err := c.fetchSnapshotWithRetry(ctx, attempts)
	if err != nil {
		return err
	}
	for {
		if err := c.stopped(ctx); err != nil {
			return err
		}
		stream, err := c.transport.openStream(ctx, &agyv115.StreamAgentStateUpdatesRequest{ConversationId: c.options.conversationID, SubscriberId: c.options.subscriberID, TrajectoryVerbosity: c.options.streamVerbosity})
		if err != nil {
			attempts++
			var recoverErr error
			attempts, recoverErr = c.recover(ctx, attempts, &held)
			if recoverErr != nil {
				return recoverErr
			}
			continue
		}
		c.setStream(stream)
		if err := c.stopped(ctx); err != nil {
			c.clearStream(stream)
			_ = stream.Close()
			return err
		}
		reopen := false
		for {
			response, recvErr := stream.Recv()
			if recvErr != nil {
				reopen = true
				break
			}
			update := response.GetUpdate()
			if c.mapper.TrajectoryID() == "" {
				bound := c.mapper.BindStreamIdentity(update)
				c.emit(bound.events)
				if bound.recover {
					reopen = true
					break
				}
				if held != nil {
					seed := c.mapper.MergeSnapshot(c.mapper.TrajectoryID(), held.offset, held.steps, trajectorySnapshotSeed)
					c.emit(seed.events)
					held = nil
					if seed.recover {
						reopen = true
						break
					}
				}
			}
			result := c.mapper.ApplyStream(update)
			if result.terminal {
				c.terminalOnce.Do(func() { close(c.terminal) })
			}
			c.emit(result.events)
			if !result.recover && update.GetStatus() == agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE && update.GetFullyIdle() {
				becameReady := false
				c.readyOnce.Do(func() {
					becameReady = true
					close(c.ready)
				})
				if becameReady && c.options.holdAfterReady {
					select {
					case <-c.release:
					case <-ctx.Done():
						return ctx.Err()
					case <-c.closed:
						return errTrajectoryRecoveryClosed
					}
				}
			}
			if result.terminal {
				c.clearStream(stream)
				_ = stream.Close()
				return nil
			}
			if result.recover {
				reopen = true
				break
			}
		}
		c.clearStream(stream)
		_ = stream.Close()
		if !reopen {
			return nil
		}
		attempts++
		var recoverErr error
		attempts, recoverErr = c.recover(ctx, attempts, &held)
		if recoverErr != nil {
			return recoverErr
		}
	}
}

func (c *trajectoryRecoveryCoordinator) recover(ctx context.Context, attempts int, held **heldTrajectorySnapshot) (int, error) {
	if err := c.stopped(ctx); err != nil {
		return attempts, err
	}
	if attempts > c.options.maxRecoveryAttempts {
		return attempts, errors.New("agy trajectory recovery exceeded retry limit")
	}
	snapshot, nextAttempts, err := c.fetchSnapshotWithRetry(ctx, attempts)
	if err != nil {
		return nextAttempts, err
	}
	attempts = nextAttempts
	if c.mapper.TrajectoryID() == "" {
		*held = snapshot
	} else {
		result := c.mapper.MergeSnapshot(c.mapper.TrajectoryID(), snapshot.offset, snapshot.steps, trajectorySnapshotRecovery)
		c.emit(result.events)
		if result.recover {
			return attempts, errors.New("agy trajectory recovery snapshot merge failed")
		}
	}
	if err := c.sleep(ctx, c.options.backoff(attempts)); err != nil {
		return attempts, err
	}
	return attempts, c.stopped(ctx)
}

func (c *trajectoryRecoveryCoordinator) fetchSnapshotWithRetry(ctx context.Context, attempts int) (*heldTrajectorySnapshot, int, error) {
	for {
		if err := c.stopped(ctx); err != nil {
			return nil, attempts, err
		}
		snapshot, err := c.fetchSnapshot(ctx)
		if err == nil {
			return snapshot, attempts, nil
		}
		attempts++
		if attempts > c.options.maxRecoveryAttempts {
			return nil, attempts, errors.New("agy trajectory recovery exceeded retry limit")
		}
		if err := c.sleep(ctx, c.options.backoff(attempts)); err != nil {
			return nil, attempts, err
		}
	}
}

func (c *trajectoryRecoveryCoordinator) fetchSnapshot(ctx context.Context) (*heldTrajectorySnapshot, error) {
	response, err := c.transport.getSteps(ctx, c.options.cascadeID, c.options.snapshotOffset, c.options.snapshotVerbosity, c.options.streamVerbosity)
	if err != nil {
		return nil, err
	}
	return &heldTrajectorySnapshot{offset: c.options.snapshotOffset, steps: response.GetSteps()}, nil
}
func (c *trajectoryRecoveryCoordinator) stopped(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.closed:
		return errTrajectoryRecoveryClosed
	default:
		return nil
	}
}
func (c *trajectoryRecoveryCoordinator) emit(events []events.Event) {
	if c.options.holdAfterReady && !c.released.Load() {
		return
	}
	for _, event := range events {
		if c.options.onEvent != nil {
			c.options.onEvent(event)
		}
	}
}
func (c *trajectoryRecoveryCoordinator) setStream(stream trajectoryRecoveryStream) {
	c.mu.Lock()
	c.stream = stream
	c.mu.Unlock()
}
func (c *trajectoryRecoveryCoordinator) clearStream(stream trajectoryRecoveryStream) {
	c.mu.Lock()
	if c.stream == stream {
		c.stream = nil
	}
	c.mu.Unlock()
}
func sleepTrajectoryRecovery(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ io.Closer = (*agentStateUpdateStream)(nil)
