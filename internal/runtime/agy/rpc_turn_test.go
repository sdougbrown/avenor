package agy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"

	"github.com/sdougbrown/avenor/internal/events"
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

	mu             sync.Mutex
	streamNumber   uint32
	currentStream  chan struct{}
	sends          []*agyv115.SendUserCascadeMessageRequest
	blockAfterSend bool
	failNextSend   bool
	blockRelease   chan struct{}
	sendNotify     chan struct{}
	notifyOnce     sync.Once
}

func newTurnTestServer(t *testing.T) *turnTestServer {
	t.Helper()
	s := &turnTestServer{t: t, blockRelease: make(chan struct{})}
	s.server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

func (s *turnTestServer) close() {
	close(s.blockRelease)
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
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
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
		s.mu.Unlock()
		if block {
			select {
			case <-s.blockRelease:
			case <-r.Context().Done():
			}
			return
		}
		text := "turn"
		steps := []uint32{index}
		running := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, false, steps, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_GENERATING, text, nil))
		idle := update(agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_IDLE, true, steps, plannerStep(agyv115.CortexStepStatus_CORTEX_STEP_STATUS_DONE, text, nil))
		_, _ = w.Write(grpcStreamReply(s.t, streamResponse(running), streamResponse(idle)))
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
		s.mu.Unlock()
		if notify != nil {
			s.notifyOnce.Do(func() { close(notify) })
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
