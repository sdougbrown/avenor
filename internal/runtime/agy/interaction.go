package agy

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"google.golang.org/protobuf/proto"
)

const (
	maxInteractionText       = 4096
	maxInteractionOptions    = 32
	maxInteractionProtoBytes = 256 << 10 // conservative, well below the 32 MiB transport ceiling.
)

var (
	errInteractionRejected = errors.New("agy interaction answer rejected")
	errInteractionStale    = errors.New("agy interaction answer is stale")
)

type pendingInteraction struct {
	requestID         string
	key               trajectoryStepKey
	generationVersion uint32
	request           *agyv115.RequestedInteraction
	question          map[string]string // local option ID -> raw option ID; never emitted.
	writeInID         string
	kind              string
}

// interactionBridge owns the one current, opaque interaction for an RPC host.
// It is fed synchronously by the existing sequential trajectory mapper; it
// never opens a second receiver or performs a mutation while observing state.
type interactionBridge struct {
	sessionID string
	cascadeID string
	getSteps  func(context.Context, uint32) (*agyv115.GetCascadeTrajectoryStepsResponse, error)
	handle    func(context.Context, *agyv115.CascadeUserInteraction) error

	mu           sync.Mutex
	pending      *pendingInteraction
	claimed      *pendingInteraction
	trajectoryID string // latest observed stream trajectory; never exposed.

	// inFlight is the cancellation context of claimed. Clear, Close, or an
	// observed replacement cancels it so stale work cannot reach Handle. The
	// done channel lets host teardown join the claimed answer before RPC close.
	inFlight     context.CancelFunc
	inFlightDone chan struct{}

	// dispatch serializes the final Handle boundary with host cancellation.
	// An answer that acquired it is considered already dispatching; cancellation
	// waits it out before marking the bridge stale, so no later answer can send.
	dispatch chan struct{}
}

func newInteractionBridge(sessionID, cascadeID string, getSteps func(context.Context, uint32) (*agyv115.GetCascadeTrajectoryStepsResponse, error), handle func(context.Context, *agyv115.CascadeUserInteraction) error) *interactionBridge {
	dispatch := make(chan struct{}, 1)
	dispatch <- struct{}{}
	return &interactionBridge{sessionID: sessionID, cascadeID: cascadeID, getSteps: getSteps, handle: handle, dispatch: dispatch}
}

// Observe consumes one exact stream/snapshot step. Duplicate representations of
// the same WAITING request retain their local token. A changed request or a new
// WAITING step is deliberately a new request; no normalized equivalence exists.
// Seed observations remain silent and do not cache actionable state. The first
// matching stream representation is authoritative and creates the local token.
func (b *interactionBridge) Observe(key trajectoryStepKey, step *agyv115.Step, seed bool) []events.Event {
	if b == nil || step == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if key.trajectoryID != "" {
		b.trajectoryID = key.trajectoryID
	}

	generation := step.GetMetadata().GetStepGenerationVersion()
	request := step.GetRequestedInteraction()
	waiting := step.GetStatus() == agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING && len(step.GetCompletedInteractions()) == 0
	if b.claimed != nil {
		same := b.claimed.key == key && b.claimed.generationVersion == generation && proto.Equal(b.claimed.request, request)
		if waiting && same {
			return nil
		}
		// Only the claimed step leaving WAITING, or a newly observed WAITING
		// identity, retires the in-flight answer. Unrelated step updates do not.
		if b.claimed.key == key || waiting {
			// Do not cancel here: the stream may report completion before the
			// successful Handle response arrives. Clearing the claim makes the
			// final pre-dispatch check fail for a true replacement, while host
			// shutdown remains the sole bridge-owned cancellation boundary.
			b.claimed = nil
		} else {
			return nil
		}
	}
	if b.pending != nil {
		same := b.pending.key == key && b.pending.generationVersion == generation && proto.Equal(b.pending.request, request)
		if waiting && same {
			return nil
		}
		// As above, unrelated non-WAITING steps cannot evict the one current
		// interaction. A different WAITING identity deliberately replaces it.
		if b.pending.key == key || waiting {
			b.pending = nil
		} else {
			return nil
		}
	}
	if !waiting {
		return nil
	}
	if request == nil {
		if seed {
			return nil
		}
		return []events.Event{b.protocol("waiting_interaction_missing_request")}
	}
	if b.pending != nil {
		return nil
	}
	// A changed key, generation, or content is a different current request even
	// when it is unsupported or malformed. Never transfer an older answer.
	if seed {
		return nil
	}

	pending, event, code := b.makePending(key, generation, request)
	if code != "" {
		if seed {
			return nil
		}
		return []events.Event{b.protocol(code)}
	}
	// There is one active interaction per session. A later observed WAITING
	// identity replaces the old local request without transferring any answer.
	b.pending = pending
	return []events.Event{event}
}

func (b *interactionBridge) makePending(key trajectoryStepKey, generation uint32, request *agyv115.RequestedInteraction) (*pendingInteraction, events.Event, string) {
	if key.trajectoryID == "" || len(key.trajectoryID) > maxTrajectoryOpaqueID {
		return nil, events.Event{}, "interaction_identity_invalid"
	}
	// Bound total serialized size before clone, so an adversarial or
	// pathological request cannot allocate unbounded memory and so the
	// generated event stays well below the gRPC-Web transport ceiling.
	if proto.Size(request) > maxInteractionProtoBytes {
		return nil, events.Event{}, "interaction_too_large"
	}
	// An unknown top-level field is an unsupported interaction oneof arm. Nested
	// unknown fields remain bounded by proto.Size and survive in the clone.
	if hasUnknownInteractionFields(request) {
		return nil, events.Event{}, "unsupported_requested_interaction"
	}
	requestID := "agy-request-" + uuid.NewString()
	pending := &pendingInteraction{requestID: requestID, key: key, generationVersion: generation, request: proto.Clone(request).(*agyv115.RequestedInteraction)}
	fields := map[string]any{"request_id": requestID}

	switch interaction := request.GetInteraction().(type) {
	case *agyv115.RequestedInteraction_Permission:
		spec := interaction.Permission
		resource := spec.GetResource()
		if spec == nil || resource == nil || !boundedInteractionText(resource.GetAction()) || !boundedInteractionText(resource.GetTarget()) || (spec.GetReason() != "" && !boundedInteractionText(spec.GetReason())) || (spec.GetActionDescription() != "" && !boundedInteractionText(spec.GetActionDescription())) {
			return nil, events.Event{}, "permission_interaction_malformed"
		}
		question := strings.TrimSpace(strings.Join([]string{resource.GetAction(), resource.GetTarget(), spec.GetActionDescription(), spec.GetReason()}, " "))
		if !boundedInteractionText(question) {
			return nil, events.Event{}, "permission_interaction_malformed"
		}
		pending.kind = "permission"
		fields["question"] = question
		fields["options"] = []any{
			map[string]any{"optionId": "allow_once", "kind": "allow", "name": "Allow once"},
			map[string]any{"optionId": "deny", "kind": "reject", "name": "Deny"},
		}
	case *agyv115.RequestedInteraction_AskQuestion:
		spec := interaction.AskQuestion
		if spec == nil || len(spec.GetQuestions()) != 1 {
			return nil, events.Event{}, "ask_question_unsupported_shape"
		}
		entry := spec.GetQuestions()[0]
		if entry.GetIsMultiSelect() || entry.GetSkipped() || len(entry.GetSelectedOptionIds()) != 0 || entry.GetWriteInResponse() != "" || !boundedInteractionText(entry.GetQuestion()) || len(entry.GetOptions()) == 0 || len(entry.GetOptions()) > maxInteractionOptions {
			return nil, events.Event{}, "ask_question_unsupported_shape"
		}
		pending.kind = "question"
		// Questions require a semantic user choice and must never be consumed by
		// the generic permission auto-approver.
		fields["requires_user_input"] = true
		pending.question = make(map[string]string, len(entry.GetOptions()))
		options := make([]any, 0, len(entry.GetOptions())+1)
		seenRaw := make(map[string]struct{}, len(entry.GetOptions()))
		for _, option := range entry.GetOptions() {
			if option == nil || !boundedInteractionText(option.GetId()) || !boundedInteractionText(option.GetText()) {
				return nil, events.Event{}, "ask_question_unsupported_shape"
			}
			if _, dup := seenRaw[option.GetId()]; dup {
				return nil, events.Event{}, "ask_question_unsupported_shape"
			}
			seenRaw[option.GetId()] = struct{}{}
			localID := "agy-option-" + uuid.NewString()
			pending.question[localID] = option.GetId()
			options = append(options, map[string]any{"optionId": localID, "kind": "allow", "name": option.GetText()})
		}
		pending.writeInID = "agy-write-in-" + uuid.NewString()
		options = append(options, map[string]any{
			"optionId":        pending.writeInID,
			"kind":            "allow",
			"name":            "Other",
			"requiresMessage": true,
		})
		fields["question"] = entry.GetQuestion()
		fields["options"] = options
	default:
		return nil, events.Event{}, "unsupported_requested_interaction"
	}
	return pending, events.Event{Event: "permission.request", SessionID: b.sessionID, Fields: fields}, ""
}

func boundedInteractionText(value string) bool {
	return value != "" && len(value) <= maxInteractionText
}

// hasUnknownInteractionFields reports whether the request carries any raw
// protobuf fields outside the local view. The diagnostics emitted on
// rejection are intentionally content-free; Avenor never stores or echoes
// unknown bytes into events.
func hasUnknownInteractionFields(request *agyv115.RequestedInteraction) bool {
	if request == nil {
		return true
	}
	return len(request.ProtoReflect().GetUnknown()) > 0
}

// Answer builds and validates the typed Handle payload before any pending
// request is evicted or any RPC is issued. Unknown/empty options, mismatched
// Allow/OptionID, and empty/oversized write-ins reject without consuming the
// pending request or sending a mutation. A successful build then re-reads the
// exact cached wire index with per-answer cancellation, sends exactly one
// Handle mutation, and never retries or restores state on failure.
func (b *interactionBridge) Answer(ctx context.Context, requestID string, response runtime.PermissionResponse) error {
	if b == nil || requestID == "" {
		return errInteractionRejected
	}
	if b.cascadeID == "" || b.getSteps == nil || b.handle == nil {
		return errInteractionRejected
	}
	if err := ctx.Err(); err != nil {
		return errInteractionStale
	}
	b.mu.Lock()
	pending := b.pending
	if pending == nil || pending.requestID != requestID || b.inFlightDone != nil {
		b.mu.Unlock()
		return errInteractionRejected
	}
	// Build the typed payload against the still-pending identity. If the build
	// is invalid, the pending request is left intact so the caller can retry or
	// the stream can replace it. Only a successful build claims and evicts.
	interaction, ok := pending.response(response)
	if !ok {
		b.mu.Unlock()
		return errInteractionRejected
	}
	// Claim and evict before I/O: concurrent/duplicate callers cannot send a
	// second mutation even if the first caller blocks or fails.
	b.pending = nil
	answerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	b.claimed, b.inFlight, b.inFlightDone = pending, cancel, done
	pendingTrajectory := pending.key.trajectoryID
	b.mu.Unlock()
	defer func() {
		cancel()
		b.mu.Lock()
		if b.claimed == pending {
			b.claimed, b.inFlight = nil, nil
		}
		if b.inFlightDone == done {
			b.inFlight, b.inFlightDone = nil, nil
		}
		b.mu.Unlock()
		close(done)
	}()

	snapshot, err := b.getSteps(answerCtx, pending.key.index)
	if err != nil || snapshot == nil || len(snapshot.GetSteps()) == 0 {
		return errInteractionStale
	}
	current := snapshot.GetSteps()[0]
	if current == nil || current.GetStatus() != agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING || !proto.Equal(current.GetRequestedInteraction(), pending.request) || current.GetMetadata().GetStepGenerationVersion() != pending.generationVersion {
		return errInteractionStale
	}
	// Trajectory race check: the stream may have moved on while the snapshot
	// was in flight. Without it a concurrent Observe could retire the old
	// trajectory and the Handle mutation would arrive against a new one.
	b.mu.Lock()
	if b.claimed != pending || b.trajectoryID != pendingTrajectory {
		b.mu.Unlock()
		return errInteractionStale
	}
	b.mu.Unlock()

	// Serialize the irreversible Handle mutation with cancellation. A host
	// cancellation that arrives first drains this token and clears the claim;
	// an already-dispatching answer completes before cancellation is marked.
	select {
	case <-b.dispatch:
		defer func() { b.dispatch <- struct{}{} }()
	case <-answerCtx.Done():
		return errInteractionStale
	}
	b.mu.Lock()
	valid := b.claimed == pending && b.trajectoryID == pendingTrajectory
	b.mu.Unlock()
	if !valid || answerCtx.Err() != nil {
		return errInteractionStale
	}
	if err := b.handle(answerCtx, interaction); err != nil {
		return errInteractionRejected
	}
	return nil
}

func (p *pendingInteraction) response(response runtime.PermissionResponse) (*agyv115.CascadeUserInteraction, bool) {
	out := &agyv115.CascadeUserInteraction{TrajectoryId: p.key.trajectoryID, StepIndex: p.key.index}
	switch p.kind {
	case "permission":
		// Permission accepts exactly allow_once+Allow true or deny+Allow false.
		// No OptionID, no Message, no other scopes through the typed surface.
		if response.Allow {
			if response.OptionID != "allow_once" {
				return nil, false
			}
			if response.Message != "" {
				return nil, false
			}
			out.Interaction = &agyv115.CascadeUserInteraction_Permission{Permission: &agyv115.PermissionInteraction{Allow: true, Scope: agyv115.PermissionScope_PERMISSION_SCOPE_ONCE}}
			return out, true
		}
		if response.OptionID != "deny" {
			return nil, false
		}
		if response.Message != "" {
			return nil, false
		}
		out.Interaction = &agyv115.CascadeUserInteraction_Permission{Permission: &agyv115.PermissionInteraction{Allow: false, Scope: agyv115.PermissionScope_PERMISSION_SCOPE_UNSPECIFIED}}
		return out, true
	case "question":
		// Question requires Allow true and a bounded Message for write-ins, or
		// an OptionID that maps to a real raw option. Option selection must not
		// carry a Message; write-in requires a bounded Message.
		if !response.Allow {
			return nil, false
		}
		entry := &agyv115.AskQuestionEntry{}
		if response.OptionID == "" {
			return nil, false
		}
		if rawID, ok := p.question[response.OptionID]; ok {
			if response.Message != "" {
				return nil, false
			}
			entry.SelectedOptionIds = []string{rawID}
		} else if response.OptionID == p.writeInID {
			if response.Message == "" || runtime.ValidatePermissionMessage(response.Message) != nil {
				return nil, false
			}
			entry.WriteInResponse = response.Message
		} else {
			return nil, false
		}
		out.Interaction = &agyv115.CascadeUserInteraction_AskQuestion{AskQuestion: &agyv115.AskQuestionInteraction{Responses: []*agyv115.AskQuestionEntry{entry}}}
		return out, true
	default:
		return nil, false
	}
}

// Evict retires actionable identity without canceling a Handle response that
// may already have produced the observed completion update.
func (b *interactionBridge) Evict() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.pending, b.claimed = nil, nil
	b.mu.Unlock()
}

// Clear is the host-shutdown/cancellation boundary. It evicts and interrupts
// any claimed answer; callers that tear down transport must follow with Wait.
func (b *interactionBridge) Clear() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.pending, b.claimed = nil, nil
	cancel := b.inFlight
	b.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// CancelAndWait is the host cancellation boundary. It first clears and
// cancels all answer work, then drains a Handle already at the mutation
// boundary and joins its goroutine. An answer rechecks its claim after taking
// dispatch, so one waiting to send cannot cross this boundary.
func (b *interactionBridge) CancelAndWait(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.Clear()
	select {
	case <-b.dispatch:
		// Clear happened before this fence. Release immediately so an answer
		// that was waiting at dispatch can recheck the cleared claim and exit.
		b.dispatch <- struct{}{}
	case <-ctx.Done():
		return ctx.Err()
	}
	return b.Wait(ctx)
}

func (b *interactionBridge) Wait(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	done := b.inFlightDone
	b.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *interactionBridge) protocol(code string) events.Event {
	return events.Event{Event: "avenor.agy.protocol", SessionID: b.sessionID, Fields: map[string]any{"code": code}}
}
