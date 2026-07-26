package agy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"google.golang.org/protobuf/proto"
)

func TestResolveRPCModel(t *testing.T) {
	models := &agyv115.FetchAvailableModelsResponse{Models: map[string]*agyv115.ModelDetails{
		"known": {Model: agyv115.Model_MODEL_GOOGLE_GEMINI_2_5_FLASH},
		"zero":  {},
		"nil":   nil,
	}}
	if got, err := resolveRPCModel(models, "known"); err != nil || got != agyv115.Model_MODEL_GOOGLE_GEMINI_2_5_FLASH {
		t.Fatalf("known = %v, %v", got, err)
	}
	if got, err := resolveRPCModel(nil, ""); !errors.Is(err, errRPCModelRequired) || got != agyv115.Model_MODEL_UNSPECIFIED {
		t.Fatalf("empty = %v, %v", got, err)
	}
	for _, slug := range []string{"Known", "missing", "zero", "nil", strings.Repeat("x", maxRPCModelSlugBytes+1)} {
		if _, err := resolveRPCModel(models, slug); !errors.Is(err, errRPCModelUnavailable) || strings.Contains(err.Error(), slug) {
			t.Fatalf("slug resolution leaked/fell back for length %d: %v", len(slug), err)
		}
	}
	unknown := agyv115.Model(987654)
	models.Models["future"] = &agyv115.ModelDetails{Model: unknown}
	if got, err := resolveRPCModel(models, "future"); err != nil || got != unknown {
		t.Fatalf("unknown enum = %v, %v", got, err)
	}
}

type turnTestServer struct {
	t      *testing.T
	server *httptest.Server

	mu              sync.Mutex
	streamNumber    uint32
	currentStream   chan struct{}
	streamOpened    chan struct{}
	streamOpenOnce  sync.Once
	sends           []*agyv115.SendUserCascadeMessageRequest
	blockAfterSend  bool
	failNextSend    bool
	blockRelease    chan struct{}
	initialRelease  chan struct{}
	cancelRelease   chan struct{}
	cancelOnce      sync.Once
	cancelCalls     int
	cancelStatus    int
	cancelDelay     time.Duration
	cancelNoRelease bool
	sendNotify      chan struct{}
	sendRelease     chan struct{}
	notifyOnce      sync.Once
	activeStep      *agyv115.Step
	activeNotify    chan struct{}
	activeOnce      sync.Once
}

func newTurnTestServer(t *testing.T) *turnTestServer {
	t.Helper()
	s := &turnTestServer{t: t, blockRelease: make(chan struct{}), cancelRelease: make(chan struct{})}
	s.server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

func (s *turnTestServer) close() {
	for _, ch := range []chan struct{}{s.blockRelease, s.initialRelease, s.sendRelease} {
		if ch != nil {
			func() {
				defer func() { _ = recover() }()
				close(ch)
			}()
		}
	}
	s.server.Close()
}

func (s *turnTestServer) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", grpcWebContentType)
	switch {
	case strings.HasSuffix(r.URL.Path, "/GetCascadeTrajectorySteps"):
		_, _ = w.Write(grpcReply(s.t, &agyv115.GetCascadeTrajectoryStepsResponse{}))
	case strings.HasSuffix(r.URL.Path, "/StreamAgentStateUpdates"):
		s.mu.Lock()
		s.streamNumber++
		index := s.streamNumber
		signal := make(chan struct{})
		s.currentStream = signal
		streamOpened := s.streamOpened
		s.mu.Unlock()
		if streamOpened != nil {
			s.streamOpenOnce.Do(func() { close(streamOpened) })
		}
		w.WriteHeader(http.StatusOK)
		s.mu.Lock()
		initialRelease := s.initialRelease
		s.mu.Unlock()
		if initialRelease != nil {
			select {
			case <-initialRelease:
			case <-r.Context().Done():
				return
			}
		}
		initial := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil)
		if index > 1 {
			initial = update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, []uint32{1}, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, "turn", nil))
		}
		initialPayload, err := proto.Marshal(streamResponse(initial))
		if err != nil {
			s.t.Errorf("marshal initial stream update: %v", err)
			return
		}
		_, _ = w.Write(grpcFrame(grpcWebDataFlag, initialPayload))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-signal:
		case <-s.blockRelease:
			return
		case <-r.Context().Done():
			return
		}
		s.mu.Lock()
		block := s.blockAfterSend
		activeStep := s.activeStep
		activeNotify := s.activeNotify
		s.mu.Unlock()
		if activeStep != nil {
			active := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, []uint32{index}, activeStep)
			payload, err := proto.Marshal(streamResponse(active))
			if err != nil {
				s.t.Errorf("marshal active stream update: %v", err)
				return
			}
			_, _ = w.Write(grpcFrame(grpcWebDataFlag, payload))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			if activeNotify != nil {
				s.activeOnce.Do(func() { close(activeNotify) })
			}
		}
		if block {
			select {
			case <-s.blockRelease:
				return
			case <-s.cancelRelease:
			case <-r.Context().Done():
				return
			}
		}
		idle := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, nil)
		if activeStep == nil {
			text := "turn"
			steps := []uint32{index}
			running := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, steps, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, text, nil))
			idle = update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, steps, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, text, nil))
			_, _ = w.Write(grpcStreamReply(s.t, streamResponse(running), streamResponse(idle)))
			return
		}
		_, _ = w.Write(grpcStreamReply(s.t, streamResponse(idle)))
	case strings.HasSuffix(r.URL.Path, "/CancelCascadeSteps"):
		s.mu.Lock()
		s.cancelCalls++
		status := s.cancelStatus
		delay := s.cancelDelay
		noRelease := s.cancelNoRelease
		s.mu.Unlock()
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		if status != 0 {
			w.Header().Set("Grpc-Status", strconv.Itoa(status))
			return
		}
		if !noRelease {
			s.cancelOnce.Do(func() { close(s.cancelRelease) })
		}
		_, _ = w.Write(grpcReply(s.t, &agyv115.CancelCascadeStepsResponse{}))
	case strings.HasSuffix(r.URL.Path, "/SendUserCascadeMessage"):
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) < 5 {
			s.t.Errorf("read send: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		request := new(agyv115.SendUserCascadeMessageRequest)
		if err := proto.Unmarshal(body[5:], request); err != nil {
			s.t.Errorf("decode send: %v", err)
		}
		s.mu.Lock()
		s.sends = append(s.sends, request)
		fail := s.failNextSend
		s.failNextSend = false
		signal := s.currentStream
		s.currentStream = nil
		notify := s.sendNotify
		sendRelease := s.sendRelease
		s.mu.Unlock()
		if notify != nil {
			s.notifyOnce.Do(func() { close(notify) })
		}
		if sendRelease != nil {
			select {
			case <-sendRelease:
			case <-r.Context().Done():
				return
			}
		}
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if signal == nil {
			s.t.Error("send arrived before stream")
		} else {
			close(signal)
		}
		_, _ = w.Write(grpcReply(s.t, &agyv115.SendUserCascadeMessageResponse{}))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *turnTestServer) requests() []*agyv115.SendUserCascadeMessageRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agyv115.SendUserCascadeMessageRequest(nil), s.sends...)
}

func newTurnTestHost(t *testing.T, serverURL string) *ptyRPCHost {
	t.Helper()
	client, err := newRPCEndpointClient(rpcEndpoint{address: strings.TrimPrefix(serverURL, "http://")})
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &ptyRPCHost{
		rpc:            &rpcHost{client: client, sessionID: testConversation},
		conversationID: testConversation,
		models: &agyv115.FetchAvailableModelsResponse{Models: map[string]*agyv115.ModelDetails{
			"known": {Model: agyv115.Model_MODEL_GOOGLE_GEMINI_2_5_FLASH},
		}},
		mapper:      newTrajectoryMapper(testConversation, "", ""),
		turnGate:    gate,
		processDone: make(chan struct{}),
		closed:      make(chan struct{}),
	}
}

func TestPTYRPCHostRunTurnUsesRecoveryMapperAndExactModel(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	host := newTurnTestHost(t, server.server.URL)
	defer host.rpc.close()

	for turn := 0; turn < 2; turn++ {
		var got []events.Event
		if err := host.RunTurn(context.Background(), "private prompt", "known", func(event events.Event) { got = append(got, event) }); err != nil {
			t.Fatalf("turn %d: %v", turn, err)
		}
		requireNames(t, got, "agent.message_chunk", "session.end")
	}
	before := len(server.requests())
	if err := host.RunTurn(context.Background(), "must not send", "missing", nil); !errors.Is(err, errRPCModelUnavailable) {
		t.Fatalf("missing model = %v", err)
	}
	if len(server.requests()) != before {
		t.Fatal("unsupported model sent a mutation")
	}
	if err := host.RunTurn(context.Background(), "must not send", "", nil); !errors.Is(err, errRPCModelRequired) {
		t.Fatalf("empty model turn: %v", err)
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("send count = %d", len(requests))
	}
	for index, request := range requests {
		if request.GetCascadeId() != testConversation || len(request.GetItems()) != 1 || request.GetItems()[0].GetText() == "" {
			t.Fatalf("request %d shape = %#v", index, request)
		}
		want := agyv115.Model_MODEL_GOOGLE_GEMINI_2_5_FLASH
		if request.GetCascadeConfig().GetPlannerConfig().GetPlanModel() != want {
			t.Fatalf("request %d model = %v, want %v", index, request.GetCascadeConfig().GetPlannerConfig().GetPlanModel(), want)
		}
	}
}

func TestPTYRPCHostNormalTerminalWinsCancelRace(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	host := newTurnTestHost(t, server.server.URL)
	defer host.rpc.close()
	var ends int
	var cancelErr error
	err := host.RunTurn(context.Background(), "private", "known", func(event events.Event) {
		if event.Event == "session.end" {
			ends++
			if event.Fields["stop_reason"] != "end_turn" {
				t.Fatalf("normal terminal = %#v", event)
			}
			cancelErr = host.Cancel(context.Background())
		}
	})
	if err != nil || cancelErr != nil {
		t.Fatalf("turn/cancel errors = %v/%v", err, cancelErr)
	}
	server.mu.Lock()
	cancelCalls := server.cancelCalls
	server.mu.Unlock()
	if ends != 1 || cancelCalls != 0 {
		t.Fatalf("normal race ends/cancels = %d/%d", ends, cancelCalls)
	}
}

func TestPTYRPCHostTerminalWinsProcessExit(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	host := newTurnTestHost(t, server.server.URL)
	defer host.rpc.close()
	processDone := make(chan struct{})
	host.processDone = processDone
	var once sync.Once
	err := host.RunTurn(context.Background(), "private", "known", func(event events.Event) {
		if event.Event == "session.end" {
			once.Do(func() { close(processDone) })
		}
	})
	if err != nil {
		t.Fatalf("process exit after terminal = %v", err)
	}
}

func TestPTYRPCHostSendFailureIsRedactedAndNotRetried(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	server.mu.Lock()
	server.failNextSend = true
	server.mu.Unlock()
	host := newTurnTestHost(t, server.server.URL)
	defer host.rpc.close()
	if err := host.RunTurn(context.Background(), "private prompt", "known", nil); err == nil || strings.Contains(err.Error(), "private prompt") {
		t.Fatalf("send failure = %v", err)
	}
	if len(server.requests()) != 1 {
		t.Fatalf("failed send calls = %d", len(server.requests()))
	}
	if err := host.RunTurn(context.Background(), "next", "known", nil); err != nil {
		t.Fatalf("host unusable after send failure: %v", err)
	}
	if len(server.requests()) != 2 {
		t.Fatalf("total send calls = %d", len(server.requests()))
	}
}

func TestPTYRPCHostRejectsConcurrentTurn(t *testing.T) {
	host := &ptyRPCHost{turnGate: make(chan struct{}), closed: make(chan struct{})}
	if err := host.RunTurn(context.Background(), "", "", nil); !errors.Is(err, errRPCTurnActive) {
		t.Fatalf("concurrent turn = %v", err)
	}
}

func TestPTYRPCHostCancelRPCEmitsOneTerminalAndHostRemainsReusable(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	server.mu.Lock()
	server.blockAfterSend = true
	server.sendNotify = make(chan struct{})
	notify := server.sendNotify
	server.mu.Unlock()
	host := newTurnTestHost(t, server.server.URL)
	defer host.rpc.close()

	var mu sync.Mutex
	var got []events.Event
	turnDone := make(chan error, 1)
	go func() {
		turnDone <- host.RunTurn(context.Background(), "private", "known", func(event events.Event) {
			mu.Lock()
			got = append(got, event)
			mu.Unlock()
		})
	}()
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}

	var cancels sync.WaitGroup
	for range 2 {
		cancels.Add(1)
		go func() {
			defer cancels.Done()
			if err := host.Cancel(context.Background()); err != nil {
				t.Errorf("Cancel: %v", err)
			}
		}()
	}
	cancels.Wait()
	select {
	case err := <-turnDone:
		if !errors.Is(err, errRPCTurnCancelled) {
			t.Fatalf("turn error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled turn did not stop")
	}
	server.mu.Lock()
	cancelCalls := server.cancelCalls
	server.mu.Unlock()
	if cancelCalls != 1 {
		t.Fatalf("CancelCascadeSteps calls = %d", cancelCalls)
	}
	mu.Lock()
	ends := 0
	for _, event := range got {
		if event.Event == "session.end" {
			ends++
			if event.Fields["stop_reason"] != "cancelled" || event.Fields["final_output"] != "" {
				t.Fatalf("cancel terminal = %#v", event)
			}
		}
	}
	mu.Unlock()
	if ends != 1 {
		t.Fatalf("session.end count = %d", ends)
	}
	if err := host.RunTurn(context.Background(), "next", "known", nil); err != nil {
		t.Fatalf("host was not reusable after RPC cancellation: %v", err)
	}
}

func TestPTYRPCHostPendingProviderCancelPreventsTurnStart(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	host := newTurnTestHost(t, server.server.URL)
	defer host.rpc.close()
	host.mu.Lock()
	host.pendingCancel = true
	host.mu.Unlock()
	if err := host.RunTurn(context.Background(), "private", "known", nil); !errors.Is(err, errRPCTurnCancelled) {
		t.Fatalf("pending cancel turn = %v", err)
	}
	if len(server.requests()) != 0 {
		t.Fatal("pending provider cancel sent a mutation")
	}
}

func TestPTYRPCHostDisarmClearsUnconsumedProviderCancel(t *testing.T) {
	host := &ptyRPCHost{}
	host.ArmCancellation()
	host.DisarmCancellation()
	host.mu.Lock()
	pending := host.pendingCancel
	host.mu.Unlock()
	if pending {
		t.Fatal("unconsumed cancellation remained armed past its prompt")
	}
}

func TestPTYRPCHostCancelBeforeSendIssuesNoSend(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	server.mu.Lock()
	server.initialRelease = make(chan struct{})
	server.streamOpened = make(chan struct{})
	initialRelease := server.initialRelease
	streamOpened := server.streamOpened
	server.mu.Unlock()
	host := newTurnTestHost(t, server.server.URL)
	defer host.rpc.close()

	turnDone := make(chan error, 1)
	go func() { turnDone <- host.RunTurn(context.Background(), "private", "known", nil) }()
	select {
	case <-streamOpened:
	case <-time.After(time.Second):
		t.Fatal("stream did not reach held readiness boundary")
	}
	if err := host.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := <-turnDone; !errors.Is(err, errRPCTurnCancelled) {
		t.Fatalf("turn error = %v", err)
	}
	server.mu.Lock()
	if len(server.sends) != 0 || server.cancelCalls != 1 {
		t.Fatalf("send/cancel calls = %d/%d", len(server.sends), server.cancelCalls)
	}
	close(initialRelease)
	server.initialRelease = nil
	server.mu.Unlock()
	if err := host.RunTurn(context.Background(), "next", "known", nil); err != nil {
		t.Fatalf("host not reusable after pre-send cancel: %v", err)
	}
}

func TestPTYRPCHostCancelActiveStreamStates(t *testing.T) {
	cases := []struct {
		name      string
		step      func() *agyv115.Step
		wantEvent string
	}{
		{"generation", func() *agyv115.Step {
			return plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, "partial", nil)
		}, "agent.message_chunk"},
		{"tool", func() *agyv115.Step {
			return &agyv115.Step{Type: agyv115.CortexStepType_CORTEX_STEP_TYPE_LIST_DIRECTORY, Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, Metadata: &agyv115.CortexStepMetadata{ToolCall: &agyv115.ChatToolCall{Id: "opaque-tool", Name: "list_directory"}}, Step: &agyv115.Step_ListDirectory{ListDirectory: &agyv115.CortexStepListDirectory{}}}
		}, "tool.call"},
		{"waiting", func() *agyv115.Step {
			return permissionWaiting("read", "resource", "reason")
		}, "permission.request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newTurnTestServer(t)
			defer server.close()
			server.mu.Lock()
			server.blockAfterSend = true
			server.activeStep = tc.step()
			server.activeNotify = make(chan struct{})
			activeNotify := server.activeNotify
			server.mu.Unlock()
			host := newTurnTestHost(t, server.server.URL)
			defer host.rpc.close()
			host.installInteractionBridge()

			var mu sync.Mutex
			var got []events.Event
			activeSeen := make(chan struct{})
			var activeOnce sync.Once
			turnDone := make(chan error, 1)
			go func() {
				turnDone <- host.RunTurn(context.Background(), "private", "known", func(event events.Event) {
					mu.Lock()
					got = append(got, event)
					mu.Unlock()
					if event.Event == tc.wantEvent {
						activeOnce.Do(func() { close(activeSeen) })
					}
				})
			}()
			select {
			case <-activeNotify:
			case <-time.After(time.Second):
				t.Fatal("active state was not streamed")
			}
			select {
			case <-activeSeen:
			case <-time.After(time.Second):
				t.Fatalf("missing active %s event", tc.wantEvent)
			}
			mu.Lock()
			if tc.name == "tool" {
				for _, event := range got {
					if event.Event == "tool.call" && (event.Fields["title"] != "list_directory" || event.Fields["status"] != "running" || event.Fields["tool_id"] == "") {
						mu.Unlock()
						t.Fatalf("visible tool fields = %#v", event.Fields)
					}
				}
			}
			beforeCancel := len(got)
			mu.Unlock()
			if err := host.Cancel(context.Background()); err != nil {
				t.Fatalf("Cancel: %v", err)
			}
			if err := <-turnDone; !errors.Is(err, errRPCTurnCancelled) {
				t.Fatalf("turn error = %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			ends := 0
			for index, event := range got {
				if event.Event == "session.end" {
					ends++
					if event.Fields["stop_reason"] != "cancelled" || event.Fields["final_output"] != "" {
						t.Fatalf("cancel terminal = %#v", event)
					}
				} else if index >= beforeCancel {
					t.Fatalf("post-cancel event = %#v", event)
				}
			}
			if ends != 1 {
				t.Fatalf("session.end count = %d", ends)
			}
		})
	}
}

func TestPTYRPCHostCancelEvictsWaitingInteractionBeforeCascadeMutation(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	server.mu.Lock()
	server.blockAfterSend = true
	server.sendNotify = make(chan struct{})
	notify := server.sendNotify
	server.mu.Unlock()
	host := newTurnTestHost(t, server.server.URL)
	defer host.rpc.close()

	fetchStarted := make(chan struct{})
	var handleCalls int
	bridge := newInteractionBridge(testConversation, testConversation,
		func(ctx context.Context, _ uint32) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
			close(fetchStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
		func(context.Context, *agyv115.CascadeUserInteraction) error {
			handleCalls++
			return nil
		},
	)
	step := &agyv115.Step{Type: agyv115.CortexStepType_CORTEX_STEP_TYPE_PLANNER_RESPONSE, Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING, Metadata: &agyv115.CortexStepMetadata{StepGenerationVersion: pendingGenerationVersion}, RequestedInteraction: &agyv115.RequestedInteraction{Interaction: &agyv115.RequestedInteraction_Permission{Permission: &agyv115.PermissionInteractionSpec{Resource: &agyv115.PermissionResource{Action: "act", Target: "target"}}}}}
	observed := bridge.Observe(trajectoryStepKey{trajectoryID: "trajectory", index: 1}, step, false)
	if len(observed) != 1 {
		t.Fatalf("waiting interaction = %#v", observed)
	}
	host.interactions = bridge
	requestID := observed[0].Fields["request_id"].(string)

	turnDone := make(chan error, 1)
	go func() { turnDone <- host.RunTurn(context.Background(), "private", "known", nil) }()
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}
	answerDone := make(chan error, 1)
	go func() {
		answerDone <- host.AnswerPermission(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: "allow_once"})
	}()
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("interaction fetch did not start")
	}
	if err := host.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := <-answerDone; err == nil {
		t.Fatal("waiting answer completed after cancellation")
	}
	if handleCalls != 0 {
		t.Fatalf("Handle mutations after cancellation = %d", handleCalls)
	}
	if err := <-turnDone; !errors.Is(err, errRPCTurnCancelled) {
		t.Fatalf("turn error = %v", err)
	}
}

// TestPTYRPCHostCancelProtocolFailureFallsBack proves that any
// CancelCascadeSteps protocol error (not just unavailable/transport) forces
// exact-process termination. No false local-cancel + reuse path is allowed.
func TestPTYRPCHostCancelProtocolFailureFallsBack(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	server.mu.Lock()
	server.blockAfterSend = true
	server.cancelStatus = 9
	server.sendNotify = make(chan struct{})
	notify := server.sendNotify
	server.mu.Unlock()
	host := newTurnTestHost(t, server.server.URL)
	session := terminal.NewFakeSession("agy", 42, "")
	session.SetInterruptCompletes(true)
	host.terminal = session
	host.cancel = func() {}
	processDone := make(chan struct{})
	host.processDone = processDone
	go func() {
		_ = session.Wait(context.Background())
		close(processDone)
	}()
	turnDone := make(chan error, 1)
	go func() { turnDone <- host.RunTurn(context.Background(), "private", "known", nil) }()
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}
	if err := host.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := <-turnDone; !errors.Is(err, errRPCTurnCancelled) {
		t.Fatalf("turn error = %v", err)
	}
	if !host.closing {
		t.Fatal("protocol failure must fall back to process termination")
	}
	if session.InterruptCalls() != 1 || session.KillCalls() != 0 || session.WaitCalls() < 1 {
		t.Fatalf("graceful fallback interrupt=%d kill=%d wait=%d", session.InterruptCalls(), session.KillCalls(), session.WaitCalls())
	}
	if err := host.RunTurn(context.Background(), "next", "known", nil); !errors.Is(err, errRPCHostClosed) {
		t.Fatalf("host was reused after protocol-failure fallback: %v", err)
	}
}

func TestPTYRPCHostCancelTimeoutAndMissingTerminalFallback(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cancelDelay time.Duration
		noRelease   bool
	}{
		{"rpc timeout", 200 * time.Millisecond, false},
		{"missing terminal", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previousCancel, previousGrace := ptyRPCCancelOverride, ptyRPCGraceOverride
			ptyRPCCancelOverride = func() time.Duration { return 30 * time.Millisecond }
			ptyRPCGraceOverride = func() time.Duration { return 10 * time.Millisecond }
			defer func() { ptyRPCCancelOverride, ptyRPCGraceOverride = previousCancel, previousGrace }()

			server := newTurnTestServer(t)
			defer server.close()
			server.mu.Lock()
			server.blockAfterSend = true
			server.cancelDelay = tc.cancelDelay
			server.cancelNoRelease = tc.noRelease
			server.sendNotify = make(chan struct{})
			notify := server.sendNotify
			server.mu.Unlock()
			host := newTurnTestHost(t, server.server.URL)
			session := terminal.NewFakeSession("agy", 43, "")
			host.terminal = session
			host.cancel = func() {}
			processDone := make(chan struct{})
			host.processDone = processDone
			go func() { _ = session.Wait(context.Background()); close(processDone) }()

			turnDone := make(chan error, 1)
			go func() { turnDone <- host.RunTurn(context.Background(), "private", "known", nil) }()
			select {
			case <-notify:
			case <-time.After(time.Second):
				t.Fatal("send did not start")
			}
			if err := host.Cancel(context.Background()); err != nil {
				t.Fatalf("Cancel: %v", err)
			}
			if err := <-turnDone; !errors.Is(err, errRPCTurnCancelled) {
				t.Fatalf("turn error = %v", err)
			}
			server.mu.Lock()
			cancelCalls := server.cancelCalls
			server.mu.Unlock()
			if cancelCalls != 1 || !host.closing || session.InterruptCalls() != 1 || session.KillCalls() != 1 || session.WaitCalls() < 1 {
				t.Fatalf("fallback calls cancel=%d closing=%v interrupt=%d kill=%d wait=%d", cancelCalls, host.closing, session.InterruptCalls(), session.KillCalls(), session.WaitCalls())
			}
			if err := host.RunTurn(context.Background(), "next", "known", nil); !errors.Is(err, errRPCHostClosed) {
				t.Fatalf("fallback host remained reusable: %v", err)
			}
		})
	}
}

func TestPTYRPCHostCancelUnavailableFallsBackToExactProcessTermination(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	server.mu.Lock()
	server.blockAfterSend = true
	server.cancelStatus = 14
	server.sendNotify = make(chan struct{})
	notify := server.sendNotify
	server.mu.Unlock()
	host := newTurnTestHost(t, server.server.URL)
	defer host.rpc.close()
	session := terminal.NewFakeSession("agy", 42, "")
	host.terminal = session
	host.cancel = func() {}
	processDone := make(chan struct{})
	host.processDone = processDone
	go func() {
		_ = session.Wait(context.Background())
		close(processDone)
	}()
	turnDone := make(chan error, 1)
	go func() { turnDone <- host.RunTurn(context.Background(), "private", "known", nil) }()
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}
	if err := host.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := <-turnDone; !errors.Is(err, errRPCTurnCancelled) {
		t.Fatalf("turn error = %v", err)
	}
	if session.InterruptCalls() != 1 || session.KillCalls() != 1 || session.WaitCalls() < 2 {
		t.Fatalf("fallback interrupt=%d kill=%d wait=%d", session.InterruptCalls(), session.KillCalls(), session.WaitCalls())
	}
	if err := host.RunTurn(context.Background(), "next", "known", nil); !errors.Is(err, errRPCHostClosed) {
		t.Fatalf("fallback host remained reusable: %v", err)
	}
}

func TestPTYRPCHostRunTurnCancellationAndProcessDeath(t *testing.T) {
	for _, processDeath := range []bool{false, true} {
		name := "caller cancellation"
		if processDeath {
			name = "process death"
		}
		t.Run(name, func(t *testing.T) {
			server := newTurnTestServer(t)
			defer server.close()
			server.mu.Lock()
			server.blockAfterSend = true
			server.sendNotify = make(chan struct{})
			notify := server.sendNotify
			server.mu.Unlock()
			host := newTurnTestHost(t, server.server.URL)
			defer host.rpc.close()
			processDone := make(chan struct{})
			host.processDone = processDone
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- host.RunTurn(ctx, "private", "known", nil) }()
			select {
			case <-notify:
			case <-time.After(time.Second):
				t.Fatal("send did not start")
			}
			if processDeath {
				close(processDone)
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if err == nil || strings.Contains(err.Error(), "private") {
					t.Fatalf("turn result = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("turn did not stop")
			}
			if !processDeath {
				if err := host.RunTurn(context.Background(), "no send", "missing", nil); !errors.Is(err, errRPCModelUnavailable) {
					t.Fatalf("turn gate was not released: %v", err)
				}
			}
		})
	}
}

func TestPTYRPCHostCloseStopsActiveTurn(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	server.mu.Lock()
	server.blockAfterSend = true
	server.sendNotify = make(chan struct{})
	notify := server.sendNotify
	server.mu.Unlock()
	host := newTurnTestHost(t, server.server.URL)
	session := terminal.NewFakeSession("agy", 42, "")
	processDone := make(chan struct{})
	host.terminal = session
	host.cancel = func() {}
	host.processDone = processDone
	go func() {
		_ = session.Wait(context.Background())
		close(processDone)
	}()

	turnDone := make(chan error, 1)
	go func() { turnDone <- host.RunTurn(context.Background(), "private", "known", nil) }()
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}
	if err := host.RunTurn(context.Background(), "second", "known", nil); !errors.Is(err, errRPCTurnActive) {
		t.Fatalf("concurrent turn = %v", err)
	}
	if err := host.Close(context.Background()); err != nil {
		t.Fatalf("host close: %v", err)
	}
	select {
	case err := <-turnDone:
		if err == nil || strings.Contains(err.Error(), "private") {
			t.Fatalf("active turn result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active turn did not stop")
	}
	if session.KillCalls() != 1 || session.WaitCalls() != 1 {
		t.Fatalf("cleanup kill=%d wait=%d", session.KillCalls(), session.WaitCalls())
	}
}

func TestPTYRPCHostCancelRacesCloseWithoutDoubleTeardown(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	server.mu.Lock()
	server.blockAfterSend = true
	server.sendNotify = make(chan struct{})
	notify := server.sendNotify
	server.mu.Unlock()
	host := newTurnTestHost(t, server.server.URL)
	session := terminal.NewFakeSession("agy", 44, "")
	host.terminal = session
	host.cancel = func() {}
	processDone := make(chan struct{})
	host.processDone = processDone
	go func() { _ = session.Wait(context.Background()); close(processDone) }()

	var mu sync.Mutex
	var got []events.Event
	turnDone := make(chan error, 1)
	go func() {
		turnDone <- host.RunTurn(context.Background(), "private", "known", func(event events.Event) {
			mu.Lock()
			got = append(got, event)
			mu.Unlock()
		})
	}()
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() { <-start; results <- host.Cancel(context.Background()) }()
	go func() { <-start; results <- host.Close(context.Background()) }()
	close(start)
	for range 2 {
		select {
		case <-results:
		case <-time.After(2 * time.Second):
			t.Fatal("Cancel/Close race did not converge")
		}
	}
	select {
	case <-turnDone:
	case <-time.After(2 * time.Second):
		t.Fatal("turn did not converge")
	}
	if session.KillCalls()+session.InterruptCalls() != 1 || session.WaitCalls() < 1 {
		t.Fatalf("teardown interrupt=%d kill=%d wait=%d", session.InterruptCalls(), session.KillCalls(), session.WaitCalls())
	}
	server.mu.Lock()
	cancelCalls := server.cancelCalls
	server.mu.Unlock()
	if cancelCalls > 1 {
		t.Fatalf("CancelCascadeSteps calls = %d", cancelCalls)
	}
	mu.Lock()
	defer mu.Unlock()
	ends := 0
	for _, event := range got {
		if event.Event == "session.end" {
			ends++
		}
	}
	if ends > 1 {
		t.Fatalf("session.end count = %d", ends)
	}
}

func TestPTYRPCHostCancelRacesProcessExit(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	server.mu.Lock()
	server.blockAfterSend = true
	server.sendNotify = make(chan struct{})
	notify := server.sendNotify
	server.mu.Unlock()
	host := newTurnTestHost(t, server.server.URL)
	session := terminal.NewFakeSession("agy", 45, "")
	session.SetInterruptCompletes(true)
	host.terminal = session
	host.cancel = func() {}
	processDone := make(chan struct{})
	host.processDone = processDone
	go func() { _ = session.Wait(context.Background()); close(processDone) }()

	turnDone := make(chan error, 1)
	go func() { turnDone <- host.RunTurn(context.Background(), "private", "known", nil) }()
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}
	start := make(chan struct{})
	cancelDone := make(chan error, 1)
	go func() { <-start; cancelDone <- host.Cancel(context.Background()) }()
	go func() { <-start; _ = session.Interrupt(context.Background()) }()
	close(start)
	select {
	case <-cancelDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel/process race did not converge")
	}
	select {
	case <-turnDone:
	case <-time.After(2 * time.Second):
		t.Fatal("turn did not converge after process exit")
	}
	server.mu.Lock()
	cancelCalls := server.cancelCalls
	server.mu.Unlock()
	if cancelCalls > 1 {
		t.Fatalf("CancelCascadeSteps calls = %d", cancelCalls)
	}
	if err := host.RunTurn(context.Background(), "next", "known", nil); err == nil {
		t.Fatal("process-dead host remained reusable")
	}
}

func TestPTYRPCHostRunTurnProcessDeath(t *testing.T) {
	server := newTurnTestServer(t)
	defer server.close()
	host := newTurnTestHost(t, server.server.URL)
	defer host.rpc.close()
	processDone := make(chan struct{})
	host.processDone = processDone
	close(processDone)
	if err := host.RunTurn(context.Background(), "private", "known", nil); err == nil || strings.Contains(err.Error(), "private") {
		t.Fatalf("process death = %v", err)
	}
}
