package agy

import (
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
)

const (
	testConversation = "conversation"
	testTrajectory   = "trajectory"
)

func plannerStep(status agyv115.CortexStepStatus, text string, usage *agyv115.ModelUsageStats) *agyv115.Step {
	return &agyv115.Step{Type: agyv115.CortexStepType_CORTEX_STEP_TYPE_PLANNER_RESPONSE, Status: status, Metadata: &agyv115.CortexStepMetadata{ModelUsage: usage}, Step: &agyv115.Step_PlannerResponse{PlannerResponse: &agyv115.CortexStepPlannerResponse{Response: text}}}
}
func listDirectoryStep(status agyv115.CortexStepStatus, id, name, arguments string) *agyv115.Step {
	return &agyv115.Step{Type: agyv115.CortexStepType_CORTEX_STEP_TYPE_LIST_DIRECTORY, Status: status, Metadata: &agyv115.CortexStepMetadata{ToolCall: &agyv115.ChatToolCall{Id: id, Name: name, ArgumentsJson: arguments}}, Step: &agyv115.Step_ListDirectory{ListDirectory: &agyv115.CortexStepListDirectory{}}}
}
func update(status agyv115.CascadeRunStatus, idle bool, indices []uint32, steps ...*agyv115.Step) *agyv115.AgentStateUpdate {
	return &agyv115.AgentStateUpdate{ConversationId: testConversation, TrajectoryId: testTrajectory, Status: status, FullyIdle: idle, MainTrajectoryUpdate: &agyv115.TrajectoryUpdate{StepsUpdate: &agyv115.StepsUpdate{Indices: indices, Steps: steps}}}
}
func eventNames(in []events.Event) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = in[i].Event
	}
	return out
}
func requireNames(t *testing.T, got []events.Event, want ...string) {
	t.Helper()
	names := eventNames(got)
	if len(names) != len(want) {
		t.Fatalf("events = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("events = %v, want %v", names, want)
		}
	}
}

func TestTrajectoryMapperFixtures(t *testing.T) {
	usage := &agyv115.ModelUsageStats{InputTokens: 2, OutputTokens: 3, CacheReadTokens: 4}
	cases := []struct {
		name string
		run  func(*testing.T, *trajectoryMapper)
	}{
		{"cumulative suffix and equal replay", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			requireNames(t, m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{2}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "hel", nil))).events, "agent.message_chunk")
			requireNames(t, m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{2}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "hello", nil))).events, "agent.message_chunk")
			if got := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{2}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "hello", nil))).events; len(got) != 0 {
				t.Fatalf("replay events = %v", got)
			}
		}},
		{"non-prefix replacement fails terminal safely", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			_ = m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{2}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "before", nil)))
			requireNames(t, m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{2}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "after", nil))).events, "avenor.agy.reconciliation")
			terminal := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil))
			requireNames(t, terminal.events, "session.end")
			if terminal.events[0].Fields["stop_reason"] != "error" || terminal.events[0].Fields["final_output"] != "" {
				t.Fatalf("unsafe reconciliation terminal = %#v", terminal.events[0].Fields)
			}
		}},
		{"done replay never replays text", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			_ = m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "done", nil)))
			if got := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "done", nil))).events; len(got) != 0 {
				t.Fatalf("DONE replay = %v", got)
			}
		}},
		{"unknown type is bounded", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			step := &agyv115.Step{Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_RUNNING}
			requireNames(t, m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{9}, step)).events, "avenor.agy.protocol")
			if got := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{9}, step)).events; len(got) != 0 {
				t.Fatalf("unknown replay = %v", got)
			}
		}},
		{"malformed index pairing", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			result := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1, 2}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "x", nil)))
			requireNames(t, result.events, "avenor.agy.protocol")
			if !result.recover {
				t.Fatal("malformed pairing did not request recovery")
			}
		}},
		{"identity mismatch", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			first := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, nil)
			_ = m.ApplyStream(first)
			second := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, nil)
			second.TrajectoryId = "other"
			result := m.ApplyStream(second)
			requireNames(t, result.events, "avenor.agy.protocol")
			if !result.recover {
				t.Fatal("mismatch did not request recovery")
			}
		}},
		{"usage dedupe aggregation", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			_ = m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "answer", usage)))
			_ = m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "answer", usage)))
			result := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "answer", usage)))
			requireNames(t, result.events, "session.end")
			got := result.events[0].Fields["usage"].(map[string]any)
			if got["input_tokens"] != uint64(2) || got["output_tokens"] != uint64(3) || got["cache_read_tokens"] != uint64(4) {
				t.Fatalf("usage = %#v", got)
			}
		}},
		{"initial idle does not terminate", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			if got := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil)).events; len(got) != 0 || m.Terminal() {
				t.Fatalf("initial idle = %v terminal=%v", got, m.Terminal())
			}
		}},
		{"one terminal after turn progress", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			_ = m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "answer", nil)))
			requireNames(t, m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "answer", nil))).events, "session.end")
			if got := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil)).events; len(got) != 0 {
				t.Fatalf("terminal replay = %v", got)
			}
		}},
		{"tool metadata lifecycle and replay", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			requireNames(t, m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{4}, listDirectoryStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_RUNNING, "call", "ListDirectory", `{"path":"."}`))).events, "tool.call")
			requireNames(t, m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{4}, listDirectoryStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "call", "ListDirectory", `not json`))).events, "tool.call_update")
			if got := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{4}, listDirectoryStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "call", "ListDirectory", `not json`))).events; len(got) != 0 {
				t.Fatalf("tool replay = %v", got)
			}
		}},
		{"unsupported waiting interaction is diagnostic", func(t *testing.T, m *trajectoryMapper) {
			m.BeginTurn()
			step := plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING, "", nil)
			step.RequestedInteraction = &agyv115.RequestedInteraction{Interaction: &agyv115.RequestedInteraction_Deploy{Deploy: &agyv115.CascadeDeployInteractionSpec{}}}
			requireNames(t, m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{5}, step)).events, "avenor.agy.protocol")
		}},
		{"valid waiting interaction does not emit unsupported_step_type", func(t *testing.T, m *trajectoryMapper) {
			current := permissionWaiting("act", "target", "reason")
			var sent []*agyv115.CascadeUserInteraction
			bridge := newInteractionTestBridge(t, &current, &sent)
			m.SetInteractionObserver(bridge.Observe, bridge.Clear)
			m.BeginTurn()
			step := plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING, "", nil)
			step.RequestedInteraction = &agyv115.RequestedInteraction{Interaction: &agyv115.RequestedInteraction_Permission{Permission: &agyv115.PermissionInteractionSpec{Resource: &agyv115.PermissionResource{Action: "act", Target: "target"}}}}
			if got := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{6}, step)).events; len(got) != 1 || got[0].Event != "permission.request" {
				t.Fatalf("permission events = %v, want exactly one permission.request", got)
			}
			question := plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING, "", nil)
			question.RequestedInteraction = &agyv115.RequestedInteraction{Interaction: &agyv115.RequestedInteraction_AskQuestion{AskQuestion: &agyv115.AskQuestionInteractionSpec{Questions: []*agyv115.AskQuestionEntry{{Question: "q", Options: []*agyv115.AskQuestionOption{{Id: "raw", Text: "visible"}}}}}}}
			if got := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{7}, question)).events; len(got) != 1 || got[0].Event != "permission.request" {
				t.Fatalf("question events = %v, want exactly one permission.request", got)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, newTrajectoryMapper("session", testConversation, "")) })
	}
}

func TestTrajectoryMapperToolFirstObservedDone(t *testing.T) {
	m := newTrajectoryMapper("session", testConversation, "")
	m.BeginTurn()
	result := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{4}, listDirectoryStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "call", "ListDirectory", `{"path":"."}`)))
	requireNames(t, result.events, "tool.call", "tool.call_update")
	if result.events[0].Fields["tool_id"] != result.events[1].Fields["tool_id"] || result.events[0].Fields["item_id"] != result.events[1].Fields["item_id"] {
		t.Fatalf("tool identity changed: %#v", result.events)
	}
}

func TestTrajectoryMapperSnapshots(t *testing.T) {
	m := newTrajectoryMapper("session", testConversation, "")
	if result := m.BindStreamIdentity(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil)); result.recover {
		t.Fatal("bind failed")
	}
	m.BeginTurn()
	if got := m.MergeSnapshot(testTrajectory, 7, []*agyv115.Step{plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "old", nil)}, trajectorySnapshotSeed).events; len(got) != 0 {
		t.Fatalf("baseline emitted %v", got)
	}
	if got := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{7}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "old", nil))).events; len(got) != 0 {
		t.Fatalf("baseline replay = %v", got)
	}
	result := m.MergeSnapshot(testTrajectory, 10, []*agyv115.Step{plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "new", nil)}, trajectorySnapshotRecovery)
	requireNames(t, result.events, "agent.message_chunk")
	if result.events[0].Fields["step_index"] != int64(10) {
		t.Fatalf("snapshot offset identity = %#v", result.events[0].Fields)
	}
	done := m.MergeSnapshot(testTrajectory, 11, []*agyv115.Step{plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "done snapshot", nil)}, trajectorySnapshotRecovery)
	requireNames(t, done.events, "agent.message_chunk")
	if replay := m.MergeSnapshot(testTrajectory, 11, []*agyv115.Step{plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "done snapshot", nil)}, trajectorySnapshotRecovery).events; len(replay) != 0 {
		t.Fatalf("DONE snapshot replay = %v", replay)
	}
}

func TestTrajectoryMapperPinsIndependentStreamConversation(t *testing.T) {
	m := newTrajectoryMapper("logical-session", "", "")
	first := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil)
	if result := m.ApplyStream(first); result.recover {
		t.Fatal("first stream identity did not bind")
	}
	changed := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, nil)
	changed.ConversationId = "different-stream-conversation"
	result := m.ApplyStream(changed)
	requireNames(t, result.events, "avenor.agy.protocol")
	if !result.recover {
		t.Fatal("changed stream conversation did not request recovery")
	}
}

// TestTrajectorySeedThenStreamDedupesThroughBridge verifies that a held
// startup snapshot does not surface a stale permission.request event, and
// that the first matching stream step is the one the bridge finally speaks
// to. Identical step hashes between seed and stream must dedupe through the
// bridge.
func TestTrajectorySeedThenStreamDedupesThroughBridge(t *testing.T) {
	current := permissionWaiting("act", "target", "reason")
	var sent []*agyv115.CascadeUserInteraction
	bridge := newInteractionTestBridge(t, &current, &sent)
	mapper := newTrajectoryMapper("local-session", testConversation, "")
	mapper.SetInteractionObserver(bridge.Observe, bridge.Clear)
	if result := mapper.BindStreamIdentity(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil)); result.recover {
		t.Fatal("bind failed")
	}
	m := mapper
	_ = m
	mapper.BeginTurn()
	seeded := mapper.MergeSnapshot(testTrajectory, 3, []*agyv115.Step{current}, trajectorySnapshotSeed)
	if len(seeded.events) != 0 {
		t.Fatalf("seed emitted stale interaction: %#v", seeded.events)
	}
	// The seed is only a baseline. The first matching stream step remains the
	// authoritative actionable observation and emits exactly one request even
	// though its step hash equals the seeded state.
	first := mapper.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{3}, current))
	if len(first.events) != 1 || first.events[0].Event != "permission.request" {
		t.Fatalf("first stream emit = %#v, want one permission request", first.events)
	}
	// A materially different step at the same index must replace the pending.
	changed := permissionWaiting("act", "other", "reason")
	second := mapper.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{3}, changed))
	if len(second.events) != 1 || second.events[0].Event != "permission.request" {
		t.Fatalf("replacement events = %#v", second.events)
	}
}

// TestTrajectoryDisconnectSnapshotThenStreamReplaysThroughBridge covers the
// disconnect → snapshot → stream cycle. The recovered snapshot must dedupe
// through the bridge, and a later stream replacement of the same step must
// re-emit because the recovery/snapshot path is the only place the bridge
// is asked to re-derive the current identity.
func TestTrajectoryDisconnectSnapshotThenStreamReplaysThroughBridge(t *testing.T) {
	first := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{
		streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{4}, permissionWaiting("act", "target", "reason"))),
	}, errs: []error{io.EOF}}
	replacement := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{
		streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{4}, permissionWaiting("act", "other", "reason"))),
		streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{4}, permissionWaiting("act", "other", "reason"))),
	}}
	recovered := &agyv115.GetCascadeTrajectoryStepsResponse{Steps: []*agyv115.Step{permissionWaiting("act", "target", "reason")}}
	transport := &fakeRecoveryTransport{snapshots: []*agyv115.GetCascadeTrajectoryStepsResponse{recovered}, streams: []trajectoryRecoveryStream{first, replacement}}
	var got []events.Event
	mapper := newTrajectoryMapper("session", testConversation, "")
	coordinator := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation, maxRecoveryAttempts: 2, backoff: func(int) time.Duration { return 0 }, onEvent: func(event events.Event) { got = append(got, event) }})
	bridge := newInteractionTestBridge(t, &recovered.Steps[0], nil)
	mapper.SetInteractionObserver(bridge.Observe, bridge.Clear)
	mapper.BeginTurn()
	if err := coordinator.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// We expect: first stream emits a permission.request for the original,
	// recovery snapshot deduplicates (no event), replacement stream emits a
	// fresh permission.request because the step is materially different, then
	// a final IDLE without a session.end because the bridge is not wired to a
	// terminal event in this minimal scenario.
	if len(got) < 2 {
		t.Fatalf("events = %v", eventNames(got))
	}
	if got[0].Event != "permission.request" {
		t.Fatalf("first event = %q", got[0].Event)
	}
	replays := 0
	for _, evt := range got {
		if evt.Event == "permission.request" {
			replays++
		}
	}
	if replays != 2 {
		t.Fatalf("replays = %d, want 2", replays)
	}
	transport.mu.Lock()
	calls := append([]string(nil), transport.calls...)
	transport.mu.Unlock()
	want := []string{"snapshot", "stream", "snapshot", "stream"}
	if len(calls) != len(want) {
		t.Fatalf("call ordering = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("call ordering = %v, want %v", calls, want)
		}
	}
}

func TestTrajectoryMapperBoundsAndPrivateIDs(t *testing.T) {
	t.Run("usage overflow is omitted", func(t *testing.T) {
		m := newTrajectoryMapper("session", testConversation, "")
		m.BeginTurn()
		_ = m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1, 2},
			plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "a", &agyv115.ModelUsageStats{InputTokens: math.MaxUint64}),
			plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "b", &agyv115.ModelUsageStats{InputTokens: 1})))
		terminal := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil))
		requireNames(t, terminal.events, "session.end")
		if terminal.events[0].Fields["usage_invalid"] != true {
			t.Fatalf("overflow terminal = %#v", terminal.events[0].Fields)
		}
		if _, ok := terminal.events[0].Fields["usage"]; ok {
			t.Fatal("overflowed usage was emitted")
		}
	})

	t.Run("content and state are bounded", func(t *testing.T) {
		m := newTrajectoryMapper("session", testConversation, "")
		m.BeginTurn()
		tooLarge := strings.Repeat("x", maxTrajectoryStepText+1)
		result := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, tooLarge, nil)))
		requireNames(t, result.events, "avenor.agy.protocol")
		if !result.recover {
			t.Fatal("oversized text did not request recovery")
		}
		steps := make([]*agyv115.Step, maxTrajectoryUpdateSteps+1)
		indices := make([]uint32, len(steps))
		result = m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, indices, steps...))
		requireNames(t, result.events, "avenor.agy.protocol")
		if !result.recover {
			t.Fatal("oversized step update did not request recovery")
		}
	})

	t.Run("opaque tool identity is derived", func(t *testing.T) {
		m := newTrajectoryMapper("session", testConversation, "")
		m.BeginTurn()
		result := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{4}, listDirectoryStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_RUNNING, "private-call-id", "ListDirectory", `{}`)))
		requireNames(t, result.events, "tool.call")
		fields := result.events[0].Fields
		if fields["item_id"] == "private-call-id" || fields["tool_id"] == "private-call-id" {
			t.Fatal("raw tool identity leaked")
		}
		if _, ok := fields["trajectory_id"]; ok {
			t.Fatal("raw trajectory identity leaked")
		}
		changed := m.ApplyStream(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{4}, listDirectoryStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_RUNNING, "different-private-id", "ListDirectory", `{}`)))
		requireNames(t, changed.events, "avenor.agy.protocol")
		if !changed.recover {
			t.Fatal("changed tool identity did not request recovery")
		}
	})
}

type fakeRecoveryStream struct {
	mu        sync.Mutex
	responses []*agyv115.StreamAgentStateUpdatesResponse
	errs      []error
	closed    bool
	wait      context.Context
}

func (s *fakeRecoveryStream) Recv() (*agyv115.StreamAgentStateUpdatesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.responses) > 0 {
		r := s.responses[0]
		s.responses = s.responses[1:]
		return r, nil
	}
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return nil, err
	}
	if s.wait != nil {
		s.mu.Unlock()
		<-s.wait.Done()
		s.mu.Lock()
		return nil, s.wait.Err()
	}
	return nil, io.EOF
}
func (s *fakeRecoveryStream) Close() error { s.mu.Lock(); s.closed = true; s.mu.Unlock(); return nil }

type closeBlockingRecoveryStream struct {
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (s *closeBlockingRecoveryStream) Recv() (*agyv115.StreamAgentStateUpdatesResponse, error) {
	s.once.Do(func() { close(s.started) })
	<-s.closed
	return nil, io.ErrClosedPipe
}
func (s *closeBlockingRecoveryStream) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

type fakeRecoveryTransport struct {
	mu           sync.Mutex
	snapshots    []*agyv115.GetCascadeTrajectoryStepsResponse
	snapshotErrs []error
	streams      []trajectoryRecoveryStream
	calls        []string
	openStarted  chan struct{}
	openRelease  chan struct{}
	blockOpen    bool
}

func (t *fakeRecoveryTransport) getSteps(context.Context, string, uint32, agyv115.SnapshotTrajectoryVerbosity, agyv115.StreamTrajectoryVerbosity) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, "snapshot")
	if len(t.snapshotErrs) > 0 {
		err := t.snapshotErrs[0]
		t.snapshotErrs = t.snapshotErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if len(t.snapshots) == 0 {
		return &agyv115.GetCascadeTrajectoryStepsResponse{}, nil
	}
	s := t.snapshots[0]
	t.snapshots = t.snapshots[1:]
	return s, nil
}
func (t *fakeRecoveryTransport) openStream(ctx context.Context, _ *agyv115.StreamAgentStateUpdatesRequest) (trajectoryRecoveryStream, error) {
	t.mu.Lock()
	t.calls = append(t.calls, "stream")
	if t.blockOpen {
		started := t.openStarted
		t.mu.Unlock()
		if started != nil {
			select {
			case <-started:
			default:
				close(started)
			}
		}
		if t.openRelease != nil {
			select {
			case <-t.openRelease:
				return &fakeRecoveryStream{
					responses: []*agyv115.StreamAgentStateUpdatesResponse{streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil))},
					wait:      ctx,
				}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	defer t.mu.Unlock()
	if len(t.streams) == 0 {
		return nil, errors.New("no stream")
	}
	s := t.streams[0]
	t.streams = t.streams[1:]
	return s, nil
}

func streamResponse(u *agyv115.AgentStateUpdate) *agyv115.StreamAgentStateUpdatesResponse {
	return &agyv115.StreamAgentStateUpdatesResponse{Update: u}
}

func TestTrajectoryRecoveryDisconnectSnapshotThenReconnect(t *testing.T) {
	first := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, nil))}, errs: []error{errors.New("transport")}}
	second := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{
		streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{2}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "answer", nil))),
		streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{2}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "answer", nil))),
	}}
	transport := &fakeRecoveryTransport{snapshots: []*agyv115.GetCascadeTrajectoryStepsResponse{{}, {}}, streams: []trajectoryRecoveryStream{first, second}}
	var got []events.Event
	mapper := newTrajectoryMapper("session", testConversation, "")
	mapper.BeginTurn()
	coordinator := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation, maxRecoveryAttempts: 2, backoff: func(int) time.Duration { return 0 }, onEvent: func(event events.Event) { got = append(got, event) }})
	if err := coordinator.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireNames(t, got, "agent.message_chunk", "session.end")
	transport.mu.Lock()
	calls := append([]string(nil), transport.calls...)
	transport.mu.Unlock()
	want := []string{"snapshot", "stream", "snapshot", "stream"}
	if len(calls) != len(want) {
		t.Fatalf("call ordering = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("call ordering = %v, want %v", calls, want)
		}
	}
	if !first.closed || !second.closed {
		t.Fatal("streams were not closed")
	}
}

func TestTrajectoryRecoveryDoneSnapshotRestoresMissingReply(t *testing.T) {
	first := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{
		streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, nil)),
	}, errs: []error{errors.New("transport")}}
	doneStep := plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "recovered answer", nil)
	second := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{
		streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{0}, doneStep)),
	}}
	transport := &fakeRecoveryTransport{
		snapshots: []*agyv115.GetCascadeTrajectoryStepsResponse{{}, {Steps: []*agyv115.Step{doneStep}}},
		streams:   []trajectoryRecoveryStream{first, second},
	}
	mapper := newTrajectoryMapper("session", testConversation, "")
	mapper.BeginTurn()
	var got []events.Event
	c := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation, maxRecoveryAttempts: 2, backoff: func(int) time.Duration { return 0 }, onEvent: func(event events.Event) { got = append(got, event) }})
	if err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireNames(t, got, "agent.message_chunk", "session.end")
	if got[0].Fields["delta"] != "recovered answer" || got[1].Fields["final_output"] != "recovered answer" {
		t.Fatalf("recovered events = %#v", got)
	}
}

func TestTrajectoryRecoveryCleanTrailerAndIdentityMismatch(t *testing.T) {
	clean := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, nil))}, errs: []error{io.EOF}}
	wrong := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, nil)
	wrong.TrajectoryId = "wrong"
	mismatch := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{streamResponse(wrong)}}
	terminal := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{3}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "answer", nil))), streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{3}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "answer", nil)))}}
	transport := &fakeRecoveryTransport{snapshots: []*agyv115.GetCascadeTrajectoryStepsResponse{{}, {}, {}}, streams: []trajectoryRecoveryStream{clean, mismatch, terminal}}
	mapper := newTrajectoryMapper("session", testConversation, "")
	mapper.BeginTurn()
	var got []events.Event
	c := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation, maxRecoveryAttempts: 3, backoff: func(int) time.Duration { return 0 }, onEvent: func(event events.Event) { got = append(got, event) }})
	if err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireNames(t, got, "avenor.agy.protocol", "agent.message_chunk", "session.end")
}

func TestTrajectoryRecoveryReadiness(t *testing.T) {
	t.Run("signals only after stream install", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		transport := &fakeRecoveryTransport{snapshots: []*agyv115.GetCascadeTrajectoryStepsResponse{{}}, blockOpen: true, openStarted: started, openRelease: release}
		mapper := newTrajectoryMapper("session", testConversation, "")
		mapper.BeginTurn()
		c := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation})
		go func() { _ = c.Run(ctx) }()
		<-started
		short, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer shortCancel()
		if err := c.WaitReady(short); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("premature readiness = %v", err)
		}
		close(release)
		readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
		defer readyCancel()
		if err := c.WaitReady(readyCtx); err != nil {
			t.Fatalf("readiness = %v", err)
		}
		cancel()
		if err := c.Wait(readyCtx); !errors.Is(err, context.Canceled) {
			t.Fatalf("run result = %v", err)
		}
	})

	t.Run("quiescent baseline holds updates until release", func(t *testing.T) {
		stream := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{
			streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil)),
			streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "answer", nil))),
			streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "answer", nil))),
		}}
		transport := &fakeRecoveryTransport{snapshots: []*agyv115.GetCascadeTrajectoryStepsResponse{{}}, streams: []trajectoryRecoveryStream{stream}}
		mapper := newTrajectoryMapper("session", testConversation, "")
		var got []events.Event
		c := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation, holdAfterReady: true, onEvent: func(event events.Event) { got = append(got, event) }})
		go func() { _ = c.Run(context.Background()) }()
		readyCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.WaitReady(readyCtx); err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("baseline emitted before release: %v", eventNames(got))
		}
		mapper.BeginTurn()
		c.ReleaseAfterReady()
		if err := c.Wait(readyCtx); err != nil {
			t.Fatal(err)
		}
		requireNames(t, got, "agent.message_chunk", "session.end")
	})

	t.Run("close wakes readiness before run", func(t *testing.T) {
		c := newTrajectoryRecoveryCoordinatorWithTransport(&fakeRecoveryTransport{}, newTrajectoryMapper("session", testConversation, ""), trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation})
		_ = c.Close()
		if err := c.WaitReady(context.Background()); !errors.Is(err, errTrajectoryRecoveryClosed) {
			t.Fatalf("closed readiness = %v", err)
		}
		if err := c.Wait(context.Background()); !errors.Is(err, errTrajectoryRecoveryClosed) {
			t.Fatalf("closed result = %v", err)
		}
	})
}

func TestTrajectoryRecoveryBoundsAndCancellation(t *testing.T) {
	t.Run("snapshot failure is retried before stream", func(t *testing.T) {
		terminal := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{
			streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "answer", nil))),
			streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "answer", nil))),
		}}
		transport := &fakeRecoveryTransport{snapshotErrs: []error{errors.New("temporary")}, snapshots: []*agyv115.GetCascadeTrajectoryStepsResponse{{}}, streams: []trajectoryRecoveryStream{terminal}}
		mapper := newTrajectoryMapper("session", testConversation, "")
		mapper.BeginTurn()
		c := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation, maxRecoveryAttempts: 2, backoff: func(int) time.Duration { return 0 }})
		if err := c.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		transport.mu.Lock()
		calls := append([]string(nil), transport.calls...)
		transport.mu.Unlock()
		want := []string{"snapshot", "snapshot", "stream"}
		if strings.Join(calls, ",") != strings.Join(want, ",") {
			t.Fatalf("snapshot-first calls = %v, want %v", calls, want)
		}
	})

	t.Run("recovery snapshot failure is retried before reopen", func(t *testing.T) {
		first := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, nil))}, errs: []error{io.EOF}}
		terminal := &fakeRecoveryStream{responses: []*agyv115.StreamAgentStateUpdatesResponse{
			streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "answer", nil))),
			streamResponse(update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "answer", nil))),
		}}
		transport := &fakeRecoveryTransport{snapshotErrs: []error{nil, errors.New("temporary"), nil}, snapshots: []*agyv115.GetCascadeTrajectoryStepsResponse{{}, {}}, streams: []trajectoryRecoveryStream{first, terminal}}
		mapper := newTrajectoryMapper("session", testConversation, "")
		mapper.BeginTurn()
		c := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation, maxRecoveryAttempts: 3, backoff: func(int) time.Duration { return 0 }})
		if err := c.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		transport.mu.Lock()
		calls := append([]string(nil), transport.calls...)
		transport.mu.Unlock()
		want := []string{"snapshot", "stream", "snapshot", "snapshot", "stream"}
		if strings.Join(calls, ",") != strings.Join(want, ",") {
			t.Fatalf("recovery snapshot-first calls = %v, want %v", calls, want)
		}
	})

	t.Run("bounded retry", func(t *testing.T) {
		transport := &fakeRecoveryTransport{streams: []trajectoryRecoveryStream{&fakeRecoveryStream{errs: []error{io.EOF}}, &fakeRecoveryStream{errs: []error{io.EOF}}, &fakeRecoveryStream{errs: []error{io.EOF}}}}
		mapper := newTrajectoryMapper("session", testConversation, "")
		mapper.BeginTurn()
		c := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation, maxRecoveryAttempts: 2, backoff: func(int) time.Duration { return 0 }})
		if err := c.Run(context.Background()); err == nil {
			t.Fatal("unbounded recovery succeeded")
		}
	})
	t.Run("explicit close cancels stream open", func(t *testing.T) {
		started := make(chan struct{})
		transport := &fakeRecoveryTransport{snapshots: []*agyv115.GetCascadeTrajectoryStepsResponse{{}}, blockOpen: true, openStarted: started}
		mapper := newTrajectoryMapper("session", testConversation, "")
		mapper.BeginTurn()
		c := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation})
		done := make(chan error, 1)
		go func() { done <- c.Run(context.Background()) }()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("stream open did not start")
		}
		_ = c.Close()
		select {
		case err := <-done:
			if !errors.Is(err, errTrajectoryRecoveryClosed) && !errors.Is(err, context.Canceled) {
				t.Fatalf("close during open = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close did not cancel stream open")
		}
	})

	t.Run("explicit close unblocks receive", func(t *testing.T) {
		stream := &closeBlockingRecoveryStream{started: make(chan struct{}), closed: make(chan struct{})}
		transport := &fakeRecoveryTransport{snapshots: []*agyv115.GetCascadeTrajectoryStepsResponse{{}}, streams: []trajectoryRecoveryStream{stream}}
		mapper := newTrajectoryMapper("session", testConversation, "")
		mapper.BeginTurn()
		c := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation})
		done := make(chan error, 1)
		go func() { done <- c.Run(context.Background()) }()
		select {
		case <-stream.started:
		case <-time.After(time.Second):
			t.Fatal("receive did not start")
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if !errors.Is(err, errTrajectoryRecoveryClosed) && !errors.Is(err, context.Canceled) {
				t.Fatalf("close result = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close did not unblock Run")
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		blocked := &fakeRecoveryStream{wait: ctx}
		transport := &fakeRecoveryTransport{streams: []trajectoryRecoveryStream{blocked}}
		mapper := newTrajectoryMapper("session", testConversation, "")
		mapper.BeginTurn()
		c := newTrajectoryRecoveryCoordinatorWithTransport(transport, mapper, trajectoryRecoveryOptions{cascadeID: "cascade", conversationID: testConversation, backoff: func(int) time.Duration { return 0 }})
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()
		time.Sleep(time.Millisecond)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation = %v", err)
		}
	})
}
