package agy

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type fakeRPCSessionHost struct {
	mu sync.Mutex

	id          string
	runs        []fakeRPCRun
	closeCalls  int
	closeErr    error
	cancelCalls int
	armed       bool
	running     bool
	answers     []fakeRPCAnswer
	run         func(context.Context, string, string, func(events.Event)) error
	cancel      func()
}

type fakeRPCRun struct{ prompt, model string }
type fakeRPCAnswer struct {
	requestID string
	response  runtime.PermissionResponse
}

func (h *fakeRPCSessionHost) ConversationID() string { return h.id }
func (h *fakeRPCSessionHost) RunTurn(ctx context.Context, prompt, model string, emit func(events.Event)) error {
	h.mu.Lock()
	if h.armed {
		h.armed = false
		h.mu.Unlock()
		return errRPCTurnCancelled
	}
	h.runs = append(h.runs, fakeRPCRun{prompt, model})
	h.running = true
	run := h.run
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.running = false
		h.mu.Unlock()
	}()
	if run != nil {
		return run(ctx, prompt, model, emit)
	}
	emit(events.Event{Event: "session.end", SessionID: h.id, Fields: map[string]any{"stop_reason": "end_turn"}})
	return nil
}
func (h *fakeRPCSessionHost) ArmCancellation() {
	h.mu.Lock()
	if !h.running {
		h.armed = true
	}
	h.mu.Unlock()
}
func (h *fakeRPCSessionHost) DisarmCancellation() {
	h.mu.Lock()
	h.armed = false
	h.mu.Unlock()
}
func (h *fakeRPCSessionHost) Cancel(context.Context) error {
	h.mu.Lock()
	h.cancelCalls++
	cancel := h.cancel
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}
func (h *fakeRPCSessionHost) AnswerPermission(_ context.Context, requestID string, response runtime.PermissionResponse) error {
	h.mu.Lock()
	h.answers = append(h.answers, fakeRPCAnswer{requestID: requestID, response: response})
	h.mu.Unlock()
	return nil
}
func (h *fakeRPCSessionHost) Close(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closeCalls++
	return h.closeErr
}

func TestSelectTransportPolicies(t *testing.T) {
	opts := runtime.StartOptions{Dir: "/work", Model: "model"}
	for _, tc := range []struct {
		name      string
		mode      string
		wantKind  transportKind
		wantCalls int
		wantErr   bool
	}{
		{"unset remains headless", "", transportHeadless, 0, false},
		{"headless does not probe", "headless", transportHeadless, 0, false},
		{"rpc probes", "rpc", transportRPC, 1, false},
		{"auto probes", "auto", transportRPC, 1, false},
		{"invalid rejected", "wat", transportHeadless, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			host := &fakeRPCSessionHost{id: "conversation"}
			selection, diagnostic, err := selectTransport(context.Background(), func(string) string { return tc.mode }, opts, "resume-id", "1.1.7", func(_ context.Context, got runtime.StartOptions, resumeID, version string) (rpcSessionHost, error) {
				calls++
				if got != opts || resumeID != "resume-id" || version != "1.1.7" {
					t.Fatalf("factory args = %#v, %q, %q", got, resumeID, version)
				}
				return host, nil
			})
			if calls != tc.wantCalls {
				t.Fatalf("factory calls = %d, want %d", calls, tc.wantCalls)
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("selectTransport succeeded")
				}
				return
			}
			if err != nil || diagnostic != "" || selection.kind != tc.wantKind {
				t.Fatalf("selection = %#v diagnostic=%q err=%v", selection, diagnostic, err)
			}
		})
	}
}

func TestSelectTransportAutoClosesFailedProbeWithoutLeakingError(t *testing.T) {
	host := &fakeRPCSessionHost{id: "sensitive-conversation"}
	selection, diagnostic, err := selectTransport(context.Background(), func(string) string { return "auto" }, runtime.StartOptions{}, "resume", "1.1.5", func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) {
		return host, errors.New("https://127.0.0.1:9999/private/path sensitive-conversation")
	})
	if err != nil || selection.kind != transportHeadless || diagnostic != autoRPCFallbackDiagnostic {
		t.Fatalf("selection = %#v diagnostic=%q err=%v", selection, diagnostic, err)
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.closeCalls != 1 {
		t.Fatalf("failed probe close calls = %d", host.closeCalls)
	}
}

func TestSelectTransportRejectsTypedNilHostWithoutCallingClose(t *testing.T) {
	probeErr := errors.New("probe failed")
	for _, tc := range []struct {
		mode           string
		wantErr        bool
		wantDiagnostic string
	}{
		{mode: "rpc", wantErr: true},
		{mode: "auto", wantDiagnostic: autoRPCFallbackDiagnostic},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			selection, diagnostic, err := selectTransport(context.Background(), func(string) string { return tc.mode }, runtime.StartOptions{}, "", "1.1.7", func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) {
				var host *ptyRPCHost
				return host, probeErr
			})
			if (err != nil) != tc.wantErr || diagnostic != tc.wantDiagnostic {
				t.Fatalf("selection=%#v diagnostic=%q err=%v", selection, diagnostic, err)
			}
			if err != nil && !errors.Is(err, probeErr) {
				t.Fatalf("error = %v, want probe error", err)
			}
			if selection.rpc != nil {
				t.Fatalf("retained typed nil host: %#v", selection.rpc)
			}
		})
	}
}

func TestSelectTransportDoesNotFallbackWhenProbeCleanupFails(t *testing.T) {
	host := &fakeRPCSessionHost{id: "conversation", closeErr: errors.New("private cleanup detail")}
	selection, diagnostic, err := selectTransport(context.Background(), func(string) string { return "auto" }, runtime.StartOptions{}, "", "1.1.7", func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) {
		return host, errors.New("private probe detail")
	})
	if err == nil || selection.kind != transportHeadless || selection.rpc != host || diagnostic != "" {
		t.Fatalf("selection=%#v diagnostic=%q err=%v", selection, diagnostic, err)
	}
	if strings.Contains(err.Error(), "private") {
		t.Fatalf("cleanup error leaked details: %v", err)
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.closeCalls != 2 {
		t.Fatalf("cleanup attempts = %d", host.closeCalls)
	}
}

func TestProviderExplicitRPCFailureRegistersNoSession(t *testing.T) {
	for _, resume := range []bool{false, true} {
		t.Run(map[bool]string{false: "start", true: "resume"}[resume], func(t *testing.T) {
			p := testProvider(runtime.StartOptions{})
			p.getenv = func(string) string { return "rpc" }
			host := &fakeRPCSessionHost{id: map[bool]string{false: "start-id", true: "resume-id"}[resume]}
			p.rpcHostFactory = func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) {
				return host, errors.New("rpc unavailable")
			}
			var err error
			if resume {
				_, err = p.Resume(context.Background(), "resume-id")
			} else {
				_, err = p.Start(context.Background(), runtime.StartOptions{})
			}
			if err == nil {
				t.Fatal("RPC selection unexpectedly succeeded")
			}
			p.mu.Lock()
			count := len(p.sessions)
			p.mu.Unlock()
			host.mu.Lock()
			closeCalls := host.closeCalls
			host.mu.Unlock()
			if count != 0 || closeCalls != 1 {
				t.Fatalf("registered sessions=%d closeCalls=%d", count, closeCalls)
			}
		})
	}
}

func TestProviderRetainsFailedUnregisteredHostUntilClose(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	p.getenv = func(string) string { return "auto" }
	host := &fakeRPCSessionHost{id: "cleanup-owner", closeErr: errors.New("still running")}
	p.rpcHostFactory = func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) {
		return host, errors.New("probe failed")
	}
	if _, err := p.Start(context.Background(), runtime.StartOptions{}); err == nil {
		t.Fatal("Start unexpectedly fell back after failed cleanup")
	}
	p.mu.Lock()
	retained := len(p.cleanupHosts)
	p.mu.Unlock()
	if retained != 1 {
		t.Fatalf("retained cleanup hosts = %d", retained)
	}
	host.mu.Lock()
	host.closeErr = nil
	host.mu.Unlock()
	if err := p.Close(); err != nil {
		t.Fatalf("Close retained host: %v", err)
	}
	p.mu.Lock()
	remaining := len(p.cleanupHosts)
	p.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("remaining cleanup hosts = %d", remaining)
	}
}

func TestProviderRPCDispatchKeepsValidatedHost(t *testing.T) {
	p := testProvider(runtime.StartOptions{Dir: "/default", Model: "configured-model"})
	p.getenv = func(string) string { return "rpc" }
	host := &fakeRPCSessionHost{id: "validated-conversation"}
	var factoryCalls int
	p.rpcHostFactory = func(_ context.Context, opts runtime.StartOptions, resumeID, version string) (rpcSessionHost, error) {
		factoryCalls++
		if opts.Dir != "/work" || resumeID != "" || version != "1.1.5" {
			t.Fatalf("factory args = %#v, %q, %q", opts, resumeID, version)
		}
		return host, nil
	}

	session, err := p.Start(context.Background(), runtime.StartOptions{Dir: "/work"})
	if err != nil || session.SessionID != host.id || factoryCalls != 1 {
		t.Fatalf("Start = %#v, %v; calls=%d", session, err, factoryCalls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := p.Events(ctx, session.SessionID)
	for _, prompt := range []string{"first", "second"} {
		if err := p.Prompt(context.Background(), session.SessionID, prompt); err != nil {
			t.Fatalf("Prompt(%q): %v", prompt, err)
		}
	}
	var starts, ends int
	for i := 0; i < 3; i++ {
		event := eventWithin(t, ch)
		if event.SessionID != host.id {
			t.Fatalf("event session = %q", event.SessionID)
		}
		if event.Event == "session.start" {
			starts++
		}
		if event.Event == "session.end" {
			ends++
		}
	}
	if starts != 1 || ends != 2 {
		t.Fatalf("starts=%d ends=%d", starts, ends)
	}
	host.mu.Lock()
	if len(host.runs) != 2 || host.runs[0].model != "configured-model" || host.runs[1].model != "configured-model" {
		t.Fatalf("runs = %#v", host.runs)
	}
	host.mu.Unlock()
	writeIn := runtime.PermissionResponse{Allow: true, OptionID: "other", Message: "write-in"}
	if err := p.AnswerPermission(context.Background(), host.id, "request", writeIn); err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if len(host.answers) != 1 || host.answers[0].requestID != "request" || host.answers[0].response != writeIn || host.closeCalls != 1 {
		t.Fatalf("answers=%#v closes=%d", host.answers, host.closeCalls)
	}
}

func TestProviderRPCCancelUsesSingleFallbackTerminal(t *testing.T) {
	p := testProvider(runtime.StartOptions{Model: "model"})
	p.getenv = func(string) string { return "rpc" }
	host := &fakeRPCSessionHost{id: "cancel-conversation"}
	release := make(chan struct{})
	host.run = func(ctx context.Context, _ string, _ string, _ func(events.Event)) error {
		select {
		case <-release:
			return context.Canceled
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.rpcHostFactory = func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) { return host, nil }

	session, err := p.Start(context.Background(), runtime.StartOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := p.Events(ctx, session.SessionID)
	done := make(chan error, 1)
	go func() { done <- p.Prompt(context.Background(), session.SessionID, "wait") }()
	_ = eventWithin(t, ch) // logical session.start
	if err := p.Cancel(context.Background(), session.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	end := eventWithin(t, ch)
	if end.Event != "session.end" || end.Fields["stop_reason"] != "cancelled" {
		t.Fatalf("terminal = %#v", end)
	}
	close(release)
	<-done
	select {
	case event := <-ch:
		if event.Event == "session.end" {
			t.Fatal("duplicate terminal")
		}
	default:
	}
}

func TestProviderRPCCancelForwardsHostTerminalWithoutDuplicate(t *testing.T) {
	p := testProvider(runtime.StartOptions{Model: "model"})
	p.getenv = func(string) string { return "rpc" }
	host := &fakeRPCSessionHost{id: "cancel-host-terminal"}
	emitReady := make(chan func(events.Event), 1)
	release := make(chan struct{})
	host.run = func(_ context.Context, _ string, _ string, emit func(events.Event)) error {
		emitReady <- emit
		<-release
		return errRPCTurnCancelled
	}
	host.cancel = func() {
		emit := <-emitReady
		emit(events.Event{Event: "session.end", SessionID: host.id, Fields: map[string]any{"stop_reason": "cancelled", "final_output": ""}})
		close(release)
	}
	p.rpcHostFactory = func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) { return host, nil }

	session, err := p.Start(context.Background(), runtime.StartOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := p.Events(ctx, session.SessionID)
	done := make(chan error, 1)
	go func() { done <- p.Prompt(context.Background(), session.SessionID, "wait") }()
	_ = eventWithin(t, ch)
	if err := p.Cancel(context.Background(), session.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if end := eventWithin(t, ch); end.Event != "session.end" || end.Fields["stop_reason"] != "cancelled" {
		t.Fatalf("terminal = %#v", end)
	}
	if err := <-done; !errors.Is(err, errRPCTurnCancelled) {
		t.Fatalf("Prompt = %v", err)
	}
	select {
	case event := <-ch:
		if event.Event == "session.end" {
			t.Fatal("duplicate terminal")
		}
	default:
	}
}

func TestProviderRPCResumeUsesExistingSessionBeforeProbe(t *testing.T) {
	p := testProvider(runtime.StartOptions{Model: "model"})
	p.getenv = func(string) string { return "rpc" }
	calls := 0
	host := &fakeRPCSessionHost{id: "resume-id"}
	p.rpcHostFactory = func(_ context.Context, opts runtime.StartOptions, resumeID, version string) (rpcSessionHost, error) {
		calls++
		if resumeID != "resume-id" || opts.Model != "model" || version != "1.1.5" {
			t.Fatalf("resume factory args = %#v, %q, %q", opts, resumeID, version)
		}
		return host, nil
	}
	session, err := p.Resume(context.Background(), "resume-id")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := p.Events(ctx, session.SessionID)
	for _, prompt := range []string{"resumed first", "resumed second"} {
		if err := p.Prompt(context.Background(), session.SessionID, prompt); err != nil {
			t.Fatal(err)
		}
	}
	if first, second, third := eventWithin(t, ch), eventWithin(t, ch), eventWithin(t, ch); first.Event != "session.start" || second.Event != "session.end" || third.Event != "session.end" {
		t.Fatalf("resumed events = %#v, %#v, %#v", first, second, third)
	}
	if _, err := p.Resume(context.Background(), "resume-id"); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("resume probes = %d", calls)
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if len(host.runs) != 2 || host.runs[0] != (fakeRPCRun{prompt: "resumed first", model: "model"}) || host.runs[1] != (fakeRPCRun{prompt: "resumed second", model: "model"}) {
		t.Fatalf("resumed runs = %#v", host.runs)
	}
}

func TestProviderCapabilityPolicy(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want bool
	}{
		{"", false},
		{"headless", false},
		{"auto", false},
		{"rpc", true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			p := testProvider(runtime.StartOptions{})
			p.getenv = func(string) string { return tc.mode }
			caps, err := p.Capabilities(context.Background())
			if err != nil || caps.Permissions != tc.want {
				t.Fatalf("Capabilities = %#v, %v", caps, err)
			}
		})
	}
}

func TestProviderAutoFallbackEmitsOneStaticDiagnostic(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	p.getenv = func(string) string { return "auto" }
	p.rpcHostFactory = func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) {
		return &fakeRPCSessionHost{id: "sensitive-id"}, errors.New("/private/path/sensitive-id")
	}
	c, out := fakeClient()
	p.clientFactory = func() *client { return c }
	session, err := p.Start(context.Background(), runtime.StartOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := p.Events(ctx, session.SessionID)
	go sendEvents(out, newInit("headless-id", "", ""), newResult("headless-id", "", nil))
	if err := p.Prompt(context.Background(), session.SessionID, "private prompt"); err != nil {
		t.Fatal(err)
	}
	var diagnostics int
	for i := 0; i < 3; i++ {
		event := eventWithin(t, ch)
		if event.Event == "avenor.agy.transport" {
			diagnostics++
			if len(event.Fields) != 1 || event.Fields["code"] != autoRPCFallbackDiagnostic {
				t.Fatalf("diagnostic = %#v", event)
			}
		}
	}
	if diagnostics != 1 {
		t.Fatalf("diagnostics = %d", diagnostics)
	}
}

func TestProviderCloseRetainsFailedRPCHostForRetry(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	p.getenv = func(string) string { return "rpc" }
	host := &fakeRPCSessionHost{id: "close-retry", closeErr: errors.New("first close failed")}
	p.rpcHostFactory = func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) { return host, nil }
	if _, err := p.Start(context.Background(), runtime.StartOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err == nil || strings.Contains(err.Error(), "first close failed") {
		t.Fatalf("first Close = %v", err)
	}
	p.mu.Lock()
	retained := len(p.sessions)
	p.mu.Unlock()
	if retained != 1 {
		t.Fatalf("retained sessions = %d", retained)
	}
	host.mu.Lock()
	host.closeErr = nil
	host.mu.Unlock()
	if err := p.Close(); err != nil {
		t.Fatalf("retry Close: %v", err)
	}
	p.mu.Lock()
	remaining := len(p.sessions)
	p.mu.Unlock()
	host.mu.Lock()
	closeCalls := host.closeCalls
	host.mu.Unlock()
	if remaining != 0 || closeCalls != 2 {
		t.Fatalf("remaining=%d closeCalls=%d", remaining, closeCalls)
	}
}

func TestProviderCloseWinsInFlightRPCRegistration(t *testing.T) {
	for _, resume := range []bool{false, true} {
		name := map[bool]string{false: "start", true: "resume"}[resume]
		t.Run(name, func(t *testing.T) {
			p := testProvider(runtime.StartOptions{})
			p.getenv = func(string) string { return "rpc" }
			host := &fakeRPCSessionHost{id: "race-id"}
			entered := make(chan struct{})
			release := make(chan struct{})
			p.rpcHostFactory = func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) {
				close(entered)
				<-release
				return host, nil
			}
			done := make(chan error, 1)
			go func() {
				if resume {
					_, err := p.Resume(context.Background(), "race-id")
					done <- err
					return
				}
				_, err := p.Start(context.Background(), runtime.StartOptions{})
				done <- err
			}()
			<-entered
			closeDone := make(chan error, 1)
			go func() { closeDone <- p.Close() }()
			select {
			case err := <-closeDone:
				t.Fatalf("Close returned before selection cleanup: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			if err := <-closeDone; err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := <-done; err == nil || err.Error() != "agy provider is closed" {
				t.Fatalf("registration after Close = %v", err)
			}
			host.mu.Lock()
			closeCalls := host.closeCalls
			host.mu.Unlock()
			p.mu.Lock()
			sessionCount := len(p.sessions)
			p.mu.Unlock()
			if closeCalls != 1 || sessionCount != 0 {
				t.Fatalf("closeCalls=%d sessions=%d", closeCalls, sessionCount)
			}
		})
	}
}

func TestProviderAutoFallbackResumeEmitsStartAndDiagnostic(t *testing.T) {
	p := testProvider(runtime.StartOptions{Model: "model"})
	p.getenv = func(string) string { return "auto" }
	failedHost := &fakeRPCSessionHost{id: "resume-id"}
	p.rpcHostFactory = func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) {
		return failedHost, errors.New("probe detail")
	}
	cl, output := fakeClient()
	p.clientFactory = func() *client { return cl }
	session, err := p.Resume(context.Background(), "resume-id")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := p.Events(ctx, session.SessionID)
	go sendEvents(output, newInit("resume-id", "", ""), newResult("resume-id", "", nil))
	if err := p.Prompt(context.Background(), session.SessionID, "continue"); err != nil {
		t.Fatal(err)
	}
	first, second, third := eventWithin(t, ch), eventWithin(t, ch), eventWithin(t, ch)
	if first.Event != "session.start" || second.Event != "avenor.agy.transport" || second.Fields["code"] != autoRPCFallbackDiagnostic || third.Event != "session.end" {
		t.Fatalf("fallback resume events = %#v, %#v, %#v", first, second, third)
	}
	failedHost.mu.Lock()
	defer failedHost.mu.Unlock()
	if failedHost.closeCalls != 1 {
		t.Fatalf("failed probe closes = %d", failedHost.closeCalls)
	}
}

func TestProviderResumeRaceClosesUnusedHost(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	p.getenv = func(string) string { return "rpc" }
	host := &fakeRPCSessionHost{id: "race-id"}
	entered := make(chan struct{})
	release := make(chan struct{})
	p.rpcHostFactory = func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) {
		close(entered)
		<-release
		return host, nil
	}
	done := make(chan error, 1)
	go func() { _, err := p.Resume(context.Background(), "race-id"); done <- err }()
	<-entered
	p.mu.Lock()
	p.sessions["race-id"] = &sessionState{sessionID: "race-id", startOpts: runtime.StartOptions{Dir: "/existing"}}
	p.mu.Unlock()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Resume: %v", err)
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.closeCalls != 1 {
		t.Fatalf("unused race host closes = %d", host.closeCalls)
	}
}

func TestProviderStartRPCCollisionClosesLosingHost(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	p.getenv = func(string) string { return "rpc" }
	ready := make(chan struct{})
	release := make(chan struct{})
	var factoryMu sync.Mutex
	var hosts []*fakeRPCSessionHost
	p.rpcHostFactory = func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error) {
		host := &fakeRPCSessionHost{id: "same-conversation"}
		factoryMu.Lock()
		hosts = append(hosts, host)
		if len(hosts) == 2 {
			close(ready)
		}
		factoryMu.Unlock()
		<-release
		return host, nil
	}
	errs := make(chan error, 2)
	for range 2 {
		go func() { _, err := p.Start(context.Background(), runtime.StartOptions{}); errs <- err }()
	}
	<-ready
	close(release)
	var successes, failures int
	for range 2 {
		if err := <-errs; err == nil {
			successes++
		} else {
			failures++
		}
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("successes=%d failures=%d", successes, failures)
	}
	factoryMu.Lock()
	defer factoryMu.Unlock()
	closes := 0
	for _, host := range hosts {
		host.mu.Lock()
		closes += host.closeCalls
		host.mu.Unlock()
	}
	if closes != 1 {
		t.Fatalf("losing host closes = %d", closes)
	}
}
