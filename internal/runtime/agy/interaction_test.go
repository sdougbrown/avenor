package agy

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"google.golang.org/protobuf/proto"
)

const (
	testInteractionGeneration = 7
	pendingGenerationVersion  = testInteractionGeneration // compatibility name for shared test fixtures.
)

func permissionWaiting(action, target, reason string) *agyv115.Step {
	return &agyv115.Step{Type: agyv115.CortexStepType_CORTEX_STEP_TYPE_PLANNER_RESPONSE, Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING, RequestedInteraction: &agyv115.RequestedInteraction{Interaction: &agyv115.RequestedInteraction_Permission{Permission: &agyv115.PermissionInteractionSpec{Resource: &agyv115.PermissionResource{Action: action, Target: target}, Reason: proto.String(reason)}}}, Metadata: &agyv115.CortexStepMetadata{StepGenerationVersion: testInteractionGeneration}}
}

func questionWaiting(question, id, text string) *agyv115.Step {
	return &agyv115.Step{Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING, RequestedInteraction: &agyv115.RequestedInteraction{Interaction: &agyv115.RequestedInteraction_AskQuestion{AskQuestion: &agyv115.AskQuestionInteractionSpec{Questions: []*agyv115.AskQuestionEntry{{Question: question, Options: []*agyv115.AskQuestionOption{{Id: id, Text: text}}}}}}}, Metadata: &agyv115.CortexStepMetadata{StepGenerationVersion: testInteractionGeneration}}
}

func newInteractionTestBridge(t *testing.T, current **agyv115.Step, sent *[]*agyv115.CascadeUserInteraction) *interactionBridge {
	t.Helper()
	return newInteractionBridge("session", "cascade", func(context.Context, uint32) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
		return &agyv115.GetCascadeTrajectoryStepsResponse{Steps: []*agyv115.Step{*current}}, nil
	}, func(_ context.Context, interaction *agyv115.CascadeUserInteraction) error {
		*sent = append(*sent, proto.Clone(interaction).(*agyv115.CascadeUserInteraction))
		return nil
	})
}

func interactionWriteInID(t *testing.T, bridge *interactionBridge) string {
	t.Helper()
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if bridge.pending == nil || bridge.pending.writeInID == "" {
		t.Fatal("missing provider-direct write-in ID")
	}
	return bridge.pending.writeInID
}

func TestInteractionBridgePermissionAllowDenyTypedShape(t *testing.T) {
	for _, allow := range []bool{true, false} {
		t.Run(map[bool]string{true: "allow", false: "deny"}[allow], func(t *testing.T) {
			current := permissionWaiting("act", "target", "reason")
			var sent []*agyv115.CascadeUserInteraction
			bridge := newInteractionTestBridge(t, &current, &sent)
			events := bridge.Observe(trajectoryStepKey{trajectoryID: "raw-trajectory", index: 7}, current, false)
			if len(events) != 1 || events[0].Event != "permission.request" {
				t.Fatalf("events = %#v", events)
			}
			requestID, _ := events[0].Fields["request_id"].(string)
			if requestID == "" || requestID == "raw-trajectory" {
				t.Fatalf("request id leaked raw identity: %q", requestID)
			}
			optionID := map[bool]string{true: "allow_once", false: "deny"}[allow]
			if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: allow, OptionID: optionID}); err != nil {
				t.Fatal(err)
			}
			if len(sent) != 1 || sent[0].GetTrajectoryId() != "raw-trajectory" || sent[0].GetStepIndex() != 7 {
				t.Fatalf("sent = %#v", sent)
			}
			permission := sent[0].GetPermission()
			if permission == nil || permission.GetAllow() != allow || permission.GetSandboxOverride() || permission.GetScope() != map[bool]agyv115.PermissionScope{true: agyv115.PermissionScope_PERMISSION_SCOPE_ONCE, false: agyv115.PermissionScope_PERMISSION_SCOPE_UNSPECIFIED}[allow] || sent[0].GetTimedOut() {
				t.Fatalf("permission response = %#v", permission)
			}
		})
	}
}

func TestInteractionBridgeFetchesExactCachedOffset(t *testing.T) {
	current := permissionWaiting("act", "target", "reason")
	var offset uint32
	bridge := newInteractionBridge("session", "cascade", func(_ context.Context, got uint32) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
		offset = got
		return &agyv115.GetCascadeTrajectoryStepsResponse{Steps: []*agyv115.Step{current}}, nil
	}, func(context.Context, *agyv115.CascadeUserInteraction) error { return nil })
	event := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 47}, current, false)[0]
	if err := bridge.Answer(context.Background(), event.Fields["request_id"].(string), runtime.PermissionResponse{Allow: true, OptionID: "allow_once"}); err != nil {
		t.Fatal(err)
	}
	if offset != 47 {
		t.Fatalf("snapshot offset = %d, want exact wire index 47", offset)
	}
}

func TestInteractionBridgeQuestionOptionAndWriteIn(t *testing.T) {
	for _, writeIn := range []bool{false, true} {
		t.Run(map[bool]string{false: "option", true: "write-in"}[writeIn], func(t *testing.T) {
			current := questionWaiting("choose", "raw-option-id", "Visible option")
			var sent []*agyv115.CascadeUserInteraction
			bridge := newInteractionTestBridge(t, &current, &sent)
			event := bridge.Observe(trajectoryStepKey{trajectoryID: "raw-trajectory", index: 9}, current, false)[0]
			options := event.Fields["options"].([]any)
			if len(options) != 2 || event.Fields["requires_user_input"] != true {
				t.Fatalf("question event shape = %#v", event.Fields)
			}
			writeInOption := options[1].(map[string]any)
			if writeInOption["name"] != "Other" || writeInOption["requiresMessage"] != true || writeInOption["optionId"] != interactionWriteInID(t, bridge) {
				t.Fatalf("write-in option = %#v", writeInOption)
			}
			optionID := options[0].(map[string]any)["optionId"].(string)
			if optionID == "raw-option-id" {
				t.Fatal("raw option ID exposed")
			}
			response := runtime.PermissionResponse{Allow: true, OptionID: optionID}
			if writeIn {
				response.OptionID = interactionWriteInID(t, bridge)
				response.Message = "typed answer"
			}
			if err := bridge.Answer(context.Background(), event.Fields["request_id"].(string), response); err != nil {
				t.Fatal(err)
			}
			answer := sent[0].GetAskQuestion().GetResponses()
			if len(answer) != 1 {
				t.Fatalf("responses = %#v", answer)
			}
			if writeIn {
				if answer[0].GetWriteInResponse() != "typed answer" || len(answer[0].GetSelectedOptionIds()) != 0 {
					t.Fatalf("write-in = %#v", answer[0])
				}
			} else if got := answer[0].GetSelectedOptionIds(); len(got) != 1 || got[0] != "raw-option-id" {
				t.Fatalf("selected IDs = %v", got)
			}
		})
	}
}

func TestInteractionBridgeReplayReplacementAndStaleValidation(t *testing.T) {
	current := permissionWaiting("act", "target", "reason")
	var sent []*agyv115.CascadeUserInteraction
	bridge := newInteractionTestBridge(t, &current, &sent)
	key := trajectoryStepKey{trajectoryID: "trajectory", index: 2}
	first := bridge.Observe(key, current, false)[0]
	if again := bridge.Observe(key, proto.Clone(current).(*agyv115.Step), false); len(again) != 0 {
		t.Fatalf("duplicate emitted %#v", again)
	}
	changed := permissionWaiting("act", "other", "reason")
	replacement := bridge.Observe(key, changed, false)[0]
	if replacement.Fields["request_id"] == first.Fields["request_id"] {
		t.Fatal("changed WAITING request reused local ID")
	}
	current = changed
	if err := bridge.Answer(context.Background(), first.Fields["request_id"].(string), runtime.PermissionResponse{Allow: true}); err != errInteractionRejected {
		t.Fatalf("old answer error = %v", err)
	}
	current = permissionWaiting("act", "changed-again", "reason")
	if err := bridge.Answer(context.Background(), replacement.Fields["request_id"].(string), runtime.PermissionResponse{Allow: true, OptionID: "allow_once"}); err != errInteractionStale {
		t.Fatalf("stale answer error = %v", err)
	}
	if len(sent) != 0 {
		t.Fatalf("stale answer mutated: %#v", sent)
	}
}

func TestInteractionBridgeRejectsTrajectoryMismatch(t *testing.T) {
	current := permissionWaiting("act", "target", "reason")
	var sent []*agyv115.CascadeUserInteraction
	bridge := newInteractionTestBridge(t, &current, &sent)
	event := bridge.Observe(trajectoryStepKey{trajectoryID: "original", index: 3}, current, false)[0]
	bridge.Observe(trajectoryStepKey{trajectoryID: "replacement", index: 4}, &agyv115.Step{Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE}, false)
	if err := bridge.Answer(context.Background(), event.Fields["request_id"].(string), runtime.PermissionResponse{Allow: true, OptionID: "allow_once"}); err != errInteractionStale {
		t.Fatalf("trajectory mismatch error = %v", err)
	}
	if len(sent) != 0 {
		t.Fatal("trajectory mismatch sent a mutation")
	}
}

func TestInteractionBridgeCompletionBeforeHandleResponseDoesNotCancelAcceptedAnswer(t *testing.T) {
	current := permissionWaiting("act", "target", "reason")
	entered := make(chan struct{})
	release := make(chan struct{})
	bridge := newInteractionBridge("session", "cascade", func(context.Context, uint32) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
		return &agyv115.GetCascadeTrajectoryStepsResponse{Steps: []*agyv115.Step{current}}, nil
	}, func(ctx context.Context, _ *agyv115.CascadeUserInteraction) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	key := trajectoryStepKey{trajectoryID: "trajectory", index: 3}
	event := bridge.Observe(key, current, false)[0]
	done := make(chan error, 1)
	go func() {
		done <- bridge.Answer(context.Background(), event.Fields["request_id"].(string), runtime.PermissionResponse{Allow: true, OptionID: "allow_once"})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Handle did not start")
	}
	bridge.Observe(key, &agyv115.Step{Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, Metadata: &agyv115.CortexStepMetadata{StepGenerationVersion: testInteractionGeneration}}, false)
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("accepted answer was canceled by completion update: %v", err)
	}
}

func TestInteractionBridgeConcurrentAnswerAndEviction(t *testing.T) {
	current := questionWaiting("q", "raw", "option")
	var sent []*agyv115.CascadeUserInteraction
	bridge := newInteractionTestBridge(t, &current, &sent)
	event := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 3}, current, false)[0]
	optionID := event.Fields["options"].([]any)[0].(map[string]any)["optionId"].(string)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- bridge.Answer(context.Background(), event.Fields["request_id"].(string), runtime.PermissionResponse{Allow: true, OptionID: optionID})
		}()
	}
	wg.Wait()
	close(errs)
	ok, rejected := 0, 0
	for err := range errs {
		if err == nil {
			ok++
		} else if err == errInteractionRejected {
			rejected++
		}
	}
	if ok != 1 || rejected != 1 || len(sent) != 1 {
		t.Fatalf("answers success=%d rejected=%d sent=%d", ok, rejected, len(sent))
	}
	bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 3}, &agyv115.Step{Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE}, false)
	if err := bridge.Answer(context.Background(), event.Fields["request_id"].(string), runtime.PermissionResponse{}); err != errInteractionRejected {
		t.Fatalf("evicted answer error = %v", err)
	}
}

func TestInteractionMapperReplayAndTerminalEviction(t *testing.T) {
	current := permissionWaiting("act", "target", "reason")
	var sent []*agyv115.CascadeUserInteraction
	bridge := newInteractionTestBridge(t, &current, &sent)
	mapper := newTrajectoryMapper("local-session", testConversation, "")
	mapper.SetInteractionObserver(bridge.Observe, bridge.Clear)
	mapper.BeginTurn()
	stream := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{6}, current)
	first := mapper.ApplyStream(stream)
	if len(first.events) != 1 || first.events[0].Event != "permission.request" {
		t.Fatalf("first events = %#v", first.events)
	}
	if replay := mapper.ApplyStream(stream); len(replay.events) != 0 {
		t.Fatalf("stream replay = %#v", replay.events)
	}
	if snapshot := mapper.MergeSnapshot("trajectory", 6, []*agyv115.Step{current}, trajectorySnapshotRecovery); len(snapshot.events) != 0 {
		t.Fatalf("snapshot replay = %#v", snapshot.events)
	}
	requestID := first.events[0].Fields["request_id"].(string)
	_ = mapper.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{6}, &agyv115.Step{Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE}))
	if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true}); err != errInteractionRejected {
		t.Fatalf("terminal interaction was not evicted: %v", err)
	}
}

func TestInteractionBridgeRejectsMalformedWithoutContentDiagnostic(t *testing.T) {
	current := questionWaiting("q", "raw", "option")
	var sent []*agyv115.CascadeUserInteraction
	bridge := newInteractionTestBridge(t, &current, &sent)
	step := questionWaiting("q", "raw", "option")
	step.GetRequestedInteraction().GetAskQuestion().Questions = append(step.GetRequestedInteraction().GetAskQuestion().Questions, &agyv115.AskQuestionEntry{Question: "secret question"})
	events := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 4}, step, false)
	if len(events) != 1 || events[0].Event != "avenor.agy.protocol" || events[0].Fields["code"] != "ask_question_unsupported_shape" {
		t.Fatalf("diagnostic = %#v", events)
	}
	if err := bridge.Answer(context.Background(), "no-such-request", runtime.PermissionResponse{}); err != errInteractionRejected || len(sent) != 0 {
		t.Fatalf("answer=%v sent=%d", err, len(sent))
	}
}

// TestInteractionBridgeRejectsInvalidAnswerBeforeEviction proves that
// unknown/empty options, mismatched Allow/OptionID, empty/oversized write-ins,
// and Allow=false on a question reject without consuming the pending request
// or sending a mutation. The pending identity remains answerable afterwards.
func TestInteractionBridgeRejectsInvalidAnswerBeforeEviction(t *testing.T) {
	t.Run("question allow false", func(t *testing.T) {
		current := questionWaiting("q", "raw", "option")
		var sent []*agyv115.CascadeUserInteraction
		bridge := newInteractionTestBridge(t, &current, &sent)
		event := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 1}, current, false)[0]
		requestID := event.Fields["request_id"].(string)
		if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: false, OptionID: "raw"}); err != errInteractionRejected {
			t.Fatalf("answer error = %v, want rejected", err)
		}
		if len(sent) != 0 {
			t.Fatalf("invalid answer mutated: %#v", sent)
		}
		if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: event.Fields["options"].([]any)[0].(map[string]any)["optionId"].(string)}); err != nil {
			t.Fatalf("retry after rejection = %v", err)
		}
		if len(sent) != 1 {
			t.Fatalf("retry did not send one mutation, got %d", len(sent))
		}
	})

	t.Run("option with message", func(t *testing.T) {
		current := questionWaiting("q", "raw", "option")
		var sent []*agyv115.CascadeUserInteraction
		bridge := newInteractionTestBridge(t, &current, &sent)
		event := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 1}, current, false)[0]
		requestID := event.Fields["request_id"].(string)
		optionID := event.Fields["options"].([]any)[0].(map[string]any)["optionId"].(string)
		if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: optionID, Message: "extra"}); err != errInteractionRejected {
			t.Fatalf("answer error = %v, want rejected", err)
		}
		if len(sent) != 0 {
			t.Fatalf("option+message mutated: %#v", sent)
		}
		if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: optionID}); err != nil {
			t.Fatalf("retry after rejection = %v", err)
		}
	})

	t.Run("write-in oversized", func(t *testing.T) {
		current := questionWaiting("q", "raw", "option")
		var sent []*agyv115.CascadeUserInteraction
		bridge := newInteractionTestBridge(t, &current, &sent)
		event := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 1}, current, false)[0]
		requestID := event.Fields["request_id"].(string)
		writeInID := interactionWriteInID(t, bridge)
		if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: writeInID, Message: ""}); err != errInteractionRejected {
			t.Fatalf("empty write-in error = %v, want rejected", err)
		}
		if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: writeInID, Message: strings.Repeat("x", runtime.MaxPermissionMessageBytes+1)}); err != errInteractionRejected {
			t.Fatalf("oversized write-in error = %v, want rejected", err)
		}
		if len(sent) != 0 {
			t.Fatalf("rejected write-in mutated: %#v", sent)
		}
		if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: writeInID, Message: "ok"}); err != nil {
			t.Fatalf("retry after rejection = %v", err)
		}
	})

	t.Run("permission wrong option", func(t *testing.T) {
		current := permissionWaiting("act", "target", "reason")
		var sent []*agyv115.CascadeUserInteraction
		bridge := newInteractionTestBridge(t, &current, &sent)
		event := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 1}, current, false)[0]
		requestID := event.Fields["request_id"].(string)
		if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: "deny"}); err != errInteractionRejected {
			t.Fatalf("mismatched option error = %v, want rejected", err)
		}
		if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, Message: "msg"}); err != errInteractionRejected {
			t.Fatalf("permission with message error = %v, want rejected", err)
		}
		if len(sent) != 0 {
			t.Fatalf("rejected permission mutated: %#v", sent)
		}
		if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: "allow_once"}); err != nil {
			t.Fatalf("retry after rejection = %v", err)
		}
	})
}

// TestInteractionBridgeRejectsGenerationVersionMismatch proves the pre-answer
// snapshot requires the exact step_generation_version that was bound to the
// pending identity. A different generation on the wire rejects without
// sending a mutation.
func TestInteractionBridgeGenerationReplacementGetsNewToken(t *testing.T) {
	current := permissionWaiting("act", "target", "reason")
	var sent []*agyv115.CascadeUserInteraction
	bridge := newInteractionTestBridge(t, &current, &sent)
	key := trajectoryStepKey{trajectoryID: "trajectory", index: 11}
	first := bridge.Observe(key, current, false)[0]
	replacement := proto.Clone(current).(*agyv115.Step)
	replacement.Metadata.StepGenerationVersion++
	second := bridge.Observe(key, replacement, false)
	if len(second) != 1 || second[0].Event != "permission.request" || second[0].Fields["request_id"] == first.Fields["request_id"] {
		t.Fatalf("generation replacement events = %#v", second)
	}
	if err := bridge.Answer(context.Background(), first.Fields["request_id"].(string), runtime.PermissionResponse{Allow: true, OptionID: "allow_once"}); err != errInteractionRejected {
		t.Fatalf("old generation answer = %v", err)
	}
}

func TestInteractionBridgeRejectsGenerationVersionMismatch(t *testing.T) {
	current := permissionWaiting("act", "target", "reason")
	var sent []*agyv115.CascadeUserInteraction
	bridge := newInteractionTestBridge(t, &current, &sent)
	event := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 11}, current, false)[0]
	requestID := event.Fields["request_id"].(string)
	// Replace the current stub with a step whose generation differs.
	other := proto.Clone(current).(*agyv115.Step)
	other.Metadata = &agyv115.CortexStepMetadata{StepGenerationVersion: testInteractionGeneration + 1}
	current = other
	if err := bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: "allow_once"}); err != errInteractionStale {
		t.Fatalf("generation mismatch error = %v, want stale", err)
	}
	if len(sent) != 0 {
		t.Fatalf("generation mismatch sent a mutation")
	}
}

// TestInteractionBridgeQuestionWithBoundedMessageAndOptionPayload verifies the
// typed Handle payload for both question option selection and write-in,
// through the bridge directly, against a recording handle.
func TestInteractionBridgeQuestionWithBoundedMessageAndOptionPayload(t *testing.T) {
	for _, tc := range []struct {
		name       string
		allow      bool
		optionKind string
		message    string
		wantWrite  string
		wantSelect []string
	}{
		{"option", true, "option", "", "", []string{"raw-option-id"}},
		{"write-in", true, "write-in", "typed answer", "typed answer", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := questionWaiting("choose", "raw-option-id", "Visible option")
			var sent []*agyv115.CascadeUserInteraction
			bridge := newInteractionTestBridge(t, &current, &sent)
			event := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 9}, current, false)[0]
			options := event.Fields["options"].([]any)
			optionID := options[0].(map[string]any)["optionId"].(string)
			response := runtime.PermissionResponse{Allow: tc.allow, OptionID: optionID, Message: tc.message}
			if tc.optionKind == "write-in" {
				response.OptionID = interactionWriteInID(t, bridge)
			}
			if err := bridge.Answer(context.Background(), event.Fields["request_id"].(string), response); err != nil {
				t.Fatal(err)
			}
			if len(sent) != 1 {
				t.Fatalf("sent = %d", len(sent))
			}
			if sent[0].GetTrajectoryId() != "trajectory" || sent[0].GetStepIndex() != 9 {
				t.Fatalf("handle trajectory/step = %s/%d", sent[0].GetTrajectoryId(), sent[0].GetStepIndex())
			}
			answer := sent[0].GetAskQuestion().GetResponses()
			if len(answer) != 1 {
				t.Fatalf("responses = %#v", answer)
			}
			if answer[0].GetWriteInResponse() != tc.wantWrite {
				t.Fatalf("write-in = %q, want %q", answer[0].GetWriteInResponse(), tc.wantWrite)
			}
			gotSelect := answer[0].GetSelectedOptionIds()
			if len(gotSelect) != len(tc.wantSelect) {
				t.Fatalf("selected = %v, want %v", gotSelect, tc.wantSelect)
			}
			for i, want := range tc.wantSelect {
				if gotSelect[i] != want {
					t.Fatalf("selected[%d] = %q, want %q", i, gotSelect[i], want)
				}
			}
		})
	}
}

// TestInteractionBridgeRejectsDuplicateRawOptionIDs proves that two raw option
// IDs colliding on the wire are rejected as unsupported shape, with a
// content-free diagnostic and no pending identity recorded.
func TestInteractionBridgeRejectsDuplicateRawOptionIDs(t *testing.T) {
	current := questionWaiting("q", "raw", "option")
	current.GetRequestedInteraction().GetAskQuestion().Questions[0].Options = append(current.GetRequestedInteraction().GetAskQuestion().Questions[0].Options, &agyv115.AskQuestionOption{Id: "raw", Text: "duplicate raw id"})
	events := bridgeObserveEvents(t, current, false)
	if len(events) != 1 || events[0].Event != "avenor.agy.protocol" || events[0].Fields["code"] != "ask_question_unsupported_shape" {
		t.Fatalf("diagnostic = %#v", events)
	}
}

func bridgeObserveEvents(t *testing.T, step *agyv115.Step, seed bool) []events.Event {
	t.Helper()
	current := step
	var sent []*agyv115.CascadeUserInteraction
	bridge := newInteractionTestBridge(t, &current, &sent)
	return bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 1}, step, seed)
}

// TestInteractionBridgeRejectsOversizedAndExcessOptions proves both bounds:
// 33 options and option text over the limit reject as unsupported shape.
func TestInteractionBridgeRejectsOversizedAndExcessOptions(t *testing.T) {
	t.Run("excess options", func(t *testing.T) {
		step := &agyv115.Step{Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING, RequestedInteraction: &agyv115.RequestedInteraction{Interaction: &agyv115.RequestedInteraction_AskQuestion{AskQuestion: &agyv115.AskQuestionInteractionSpec{Questions: []*agyv115.AskQuestionEntry{{Question: "q", Options: make([]*agyv115.AskQuestionOption, 0, 33)}}}}}}
		for i := 0; i < 33; i++ {
			step.GetRequestedInteraction().GetAskQuestion().Questions[0].Options = append(step.GetRequestedInteraction().GetAskQuestion().Questions[0].Options, &agyv115.AskQuestionOption{Id: "id-" + strings.Repeat("x", 1), Text: "t"})
		}
		events := bridgeObserveEvents(t, step, false)
		if len(events) != 1 || events[0].Fields["code"] != "ask_question_unsupported_shape" {
			t.Fatalf("diagnostic = %#v", events)
		}
	})
	t.Run("oversized option text", func(t *testing.T) {
		step := questionWaiting("q", "raw", "ok")
		step.GetRequestedInteraction().GetAskQuestion().Questions[0].Options[0].Text = strings.Repeat("x", maxInteractionText+1)
		events := bridgeObserveEvents(t, step, false)
		if len(events) != 1 || events[0].Fields["code"] != "ask_question_unsupported_shape" {
			t.Fatalf("diagnostic = %#v", events)
		}
	})
}

// TestInteractionBridgeRejectsOversizedRequest proves the proto.Size bound and
// unknown-field bound reject without exposing content in the diagnostic.
func TestInteractionBridgeRejectsOversizedRequest(t *testing.T) {
	step := permissionWaiting("act", "target", "reason")
	step.GetRequestedInteraction().GetPermission().Reason = proto.String(strings.Repeat("a", maxInteractionText))
	events := bridgeObserveEvents(t, step, false)
	if len(events) != 1 || events[0].Event != "avenor.agy.protocol" {
		t.Fatalf("oversized rejected = %#v", events)
	}
	// Diagnostic is content-free.
	if s, ok := events[0].Fields["code"].(string); !ok || s == "" {
		t.Fatalf("diagnostic code = %#v", events[0].Fields)
	}
	for _, v := range events[0].Fields {
		if str, ok := v.(string); ok && len(str) > 64 {
			t.Fatalf("diagnostic leaked oversized content: %q", str)
		}
	}
}

func TestInteractionBridgeCancelAtDispatchPreventsHandle(t *testing.T) {
	current := permissionWaiting("act", "target", "reason")
	fetchDone := make(chan struct{})
	var handleCalls atomic.Int32
	bridge := newInteractionBridge("session", "cascade", func(context.Context, uint32) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
		close(fetchDone)
		return &agyv115.GetCascadeTrajectoryStepsResponse{Steps: []*agyv115.Step{current}}, nil
	}, func(context.Context, *agyv115.CascadeUserInteraction) error {
		handleCalls.Add(1)
		return nil
	})
	event := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 4}, current, false)[0]
	<-bridge.dispatch
	answerDone := make(chan error, 1)
	go func() {
		answerDone <- bridge.Answer(context.Background(), event.Fields["request_id"].(string), runtime.PermissionResponse{Allow: true, OptionID: "allow_once"})
	}()
	select {
	case <-fetchDone:
	case <-time.After(time.Second):
		t.Fatal("answer did not reach dispatch boundary")
	}
	// Establish cancellation before releasing the gate; CancelAndWait repeats
	// Clear and then fences with the dispatch token.
	bridge.Clear()
	cancelDone := make(chan error, 1)
	go func() { cancelDone <- bridge.CancelAndWait(context.Background()) }()
	bridge.dispatch <- struct{}{}
	select {
	case err := <-answerDone:
		if err == nil {
			t.Fatal("answer crossed cancellation boundary")
		}
	case <-time.After(time.Second):
		t.Fatal("answer did not exit")
	}
	select {
	case err := <-cancelDone:
		if err != nil {
			t.Fatalf("CancelAndWait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CancelAndWait did not exit")
	}
	if handleCalls.Load() != 0 {
		t.Fatalf("Handle calls = %d", handleCalls.Load())
	}
}

// TestInteractionBridgeCloseCancelsInFlightPreAnswerFetch proves that Close
// cancels a blocked pre-answer snapshot RPC and yields zero Handle mutations.
func TestInteractionBridgeCloseCancelsInFlightPreAnswerFetch(t *testing.T) {
	current := permissionWaiting("act", "target", "reason")
	var sent []*agyv115.CascadeUserInteraction
	var getCalls atomic.Int32
	getRelease := make(chan struct{})
	getCancelled := make(chan struct{})
	bridge := newInteractionBridge("session", "cascade",
		func(ctx context.Context, offset uint32) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
			getCalls.Add(1)
			select {
			case <-getRelease:
			case <-ctx.Done():
				close(getCancelled)
				return nil, ctx.Err()
			}
			return &agyv115.GetCascadeTrajectoryStepsResponse{Steps: []*agyv115.Step{current}}, nil
		},
		func(context.Context, *agyv115.CascadeUserInteraction) error { return nil },
	)
	event := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 5}, current, false)[0]
	requestID := event.Fields["request_id"].(string)
	answerDone := make(chan error, 1)
	go func() {
		answerDone <- bridge.Answer(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: "allow_once"})
	}()
	// Wait for the bridge to enter the snapshot RPC.
	for getCalls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	bridge.Clear()
	select {
	case <-getCancelled:
	case <-time.After(time.Second):
		t.Fatal("Clear did not cancel the pre-answer snapshot")
	}
	// Ensure the in-flight answer returns without sending a mutation.
	select {
	case err := <-answerDone:
		if err == nil {
			t.Fatal("answer succeeded after Clear")
		}
	case <-time.After(time.Second):
		t.Fatal("answer goroutine leaked after Clear")
	}
	if len(sent) != 0 {
		t.Fatalf("Handle sent %d mutations after Close", len(sent))
	}
	// A second Clear must be a no-op.
	bridge.Clear()
}
