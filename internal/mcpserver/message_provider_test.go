package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runstate"
)

const testSupervisorPath = "/tmp/avenor-test-supervisor.sock"

var _ ControlClient = (*client.Client)(nil)

// fakeMessageControlClient drives the message verbs. The embedded fakeClient
// supplies the ControlClient half, so a test configures only the verbs it
// exercises.
type fakeMessageControlClient struct {
	*fakeClient

	sendFunc      func(context.Context, messaging.SendRequest) (messaging.SendResult, error)
	waitFunc      func(context.Context, messaging.WaitRequest) (messaging.WaitResult, error)
	replyFunc     func(context.Context, messaging.ReplyRequest) (messaging.SendResult, error)
	withdrawFunc  func(context.Context, messaging.WithdrawRequest) (messaging.WithdrawResult, error)
	receiveFunc   func(context.Context, messaging.ReceiveRequest) (messaging.ReceiveResult, error)
	peersFunc     func(context.Context, messaging.PeersRequest) (messaging.PeersResult, error)
	preflightFunc func(context.Context) (messaging.PreflightResult, error)
	resumeFunc    func(messaging.ResumeRequest) (messaging.ResumeResult, error)
	renameFunc    func(context.Context, string, string) (messaging.RenameResult, error)

	waitGenerations  []string
	peersGenerations []string
	renameCalls      []renameCall
}

type renameCall struct {
	sessionID string
	name      string
}

func (f *fakeMessageControlClient) MessageSendContext(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	if f.sendFunc != nil {
		return f.sendFunc(ctx, req)
	}
	return messaging.SendResult{}, nil
}

func (f *fakeMessageControlClient) MessageWaitContext(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	f.waitGenerations = append(f.waitGenerations, req.Generation)
	if f.waitFunc != nil {
		return f.waitFunc(ctx, req)
	}
	return messaging.WaitResult{}, nil
}

func (f *fakeMessageControlClient) MessageReplyContext(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	if f.replyFunc != nil {
		return f.replyFunc(ctx, req)
	}
	return messaging.SendResult{}, nil
}

func (f *fakeMessageControlClient) MessageWithdrawContext(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	if f.withdrawFunc != nil {
		return f.withdrawFunc(ctx, req)
	}
	return messaging.WithdrawResult{}, nil
}

func (f *fakeMessageControlClient) MessageReceiveContext(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	if f.receiveFunc != nil {
		return f.receiveFunc(ctx, req)
	}
	return messaging.ReceiveResult{}, nil
}

func (f *fakeMessageControlClient) MessagePeersContext(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	f.peersGenerations = append(f.peersGenerations, req.Generation)
	if f.peersFunc != nil {
		return f.peersFunc(ctx, req)
	}
	return messaging.PeersResult{Generation: "gen-1"}, nil
}

func (f *fakeMessageControlClient) MessagePreflightContext(ctx context.Context) (messaging.PreflightResult, error) {
	if f.preflightFunc != nil {
		return f.preflightFunc(ctx)
	}
	return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-1"}, nil
}

func (f *fakeMessageControlClient) ResumeContext(_ context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	if f.resumeFunc != nil {
		return f.resumeFunc(req)
	}
	return messaging.ResumeResult{}, nil
}

func (f *fakeMessageControlClient) SessionRenameContext(ctx context.Context, sessionID, name string) (messaging.RenameResult, error) {
	f.renameCalls = append(f.renameCalls, renameCall{sessionID: sessionID, name: name})
	if f.renameFunc != nil {
		return f.renameFunc(ctx, sessionID, name)
	}
	return messaging.RenameResult{}, nil
}

func (f *fakeMessageControlClient) SessionLastContext(context.Context, string) (messaging.PeerLast, error) {
	return messaging.PeerLast{}, nil
}

func newTestMessageProvider(t *testing.T, cl ControlClient) *mcpMessageProvider {
	t.Helper()
	p, err := newMCPMessageProvider(context.Background(), &Server{registry: NewRunRegistry()}, cl, testSupervisorPath)
	if err != nil {
		t.Fatalf("newMCPMessageProvider: %v", err)
	}
	return p
}

func testPeer(runID, label, status string) messaging.Peer {
	session, err := json.Marshal(map[string]string{
		"run_id": runID,
		"label":  label,
		"status": status,
	})
	if err != nil {
		panic(err)
	}
	return messaging.Peer{RunID: runID, Session: session}
}

// TestMCPMessageProviderGenerationRefreshesAfterMismatch proves a stale
// cached generation is recovered instead of failing the provider for the rest
// of the process. The store answers gen-1 once, rejects the versioned wait
// under it, then reports gen-2; the wait is retried under gen-2 and the cache
// follows.
func TestMCPMessageProviderGenerationRefreshesAfterMismatch(t *testing.T) {
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{}}
	preflights := 0
	cl.preflightFunc = func(context.Context) (messaging.PreflightResult, error) {
		preflights++
		generation := "gen-1"
		if preflights > 1 {
			generation = "gen-2"
		}
		return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: generation}, nil
	}
	cl.waitFunc = func(_ context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
		if req.Generation != "gen-2" {
			return messaging.WaitResult{Outcome: messaging.OutcomeNotSent}, messaging.ErrGenerationMismatch
		}
		return messaging.WaitResult{Outcome: messaging.OutcomeReplied}, nil
	}

	p := newTestMessageProvider(t, cl)
	if got := p.Generation(); got != "gen-1" {
		t.Fatalf("construction generation = %q, want gen-1", got)
	}

	res, err := p.Wait(context.Background(), messaging.WaitRequest{MessageID: "msg-1"})
	if err != nil {
		t.Fatalf("Wait error = %v, want retried success", err)
	}
	if res.Outcome != messaging.OutcomeReplied {
		t.Fatalf("Wait outcome = %q, want %q", res.Outcome, messaging.OutcomeReplied)
	}
	if got := p.Generation(); got != "gen-2" {
		t.Fatalf("Generation() = %q after recovery, want gen-2", got)
	}
	if want := []string{"gen-1", "gen-2"}; !slices.Equal(cl.waitGenerations, want) {
		t.Fatalf("wait generations = %v, want %v", cl.waitGenerations, want)
	}
}

func TestMCPMessageProviderGenerationKeepsPinWhenRefreshFails(t *testing.T) {
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{}}
	preflights := 0
	refreshErr := errors.New("preflight unavailable")
	cl.preflightFunc = func(context.Context) (messaging.PreflightResult, error) {
		preflights++
		if preflights > 1 {
			return messaging.PreflightResult{}, refreshErr
		}
		return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-1"}, nil
	}
	cl.waitFunc = func(context.Context, messaging.WaitRequest) (messaging.WaitResult, error) {
		return messaging.WaitResult{Outcome: messaging.OutcomeNotSent}, messaging.ErrGenerationMismatch
	}

	p := newTestMessageProvider(t, cl)
	_, err := p.Wait(context.Background(), messaging.WaitRequest{MessageID: "msg-1"})
	// The retry preflight failed, so the original mismatch is what the caller
	// sees and the cached generation is not replaced with anything.
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("Wait error = %v, want the original generation mismatch", err)
	}
	if errors.Is(err, refreshErr) {
		t.Fatalf("Wait error = %v, want the preflight error swallowed", err)
	}
	if got := p.Generation(); got != "gen-1" {
		t.Fatalf("Generation() = %q after a failed refresh, want gen-1", got)
	}
	if preflights != 2 {
		t.Fatalf("preflights = %d, want 2 (construction plus one retry)", preflights)
	}
}

func TestMCPMessageProviderListedPeersReturnsDecodeError(t *testing.T) {
	cl := &fakeMessageControlClient{
		fakeClient: &fakeClient{},
		peersFunc: func(context.Context, messaging.PeersRequest) (messaging.PeersResult, error) {
			return messaging.PeersResult{
				Peers:      []messaging.Peer{{RunID: "rt-bad", Session: json.RawMessage(`{"run_id":`)}},
				Generation: "gen-1",
			}, nil
		},
	}
	p := newTestMessageProvider(t, cl)
	if _, err := p.Resolve(context.Background(), "rt-bad"); err == nil {
		t.Fatal("Resolve error = nil, want the peer session decode error")
	}
}

func TestMCPMessageProviderPeerRunStateMissIsTargetUnavailable(t *testing.T) {
	cl := &fakeMessageControlClient{
		fakeClient: &fakeClient{},
		peersFunc: func(context.Context, messaging.PeersRequest) (messaging.PeersResult, error) {
			return messaging.PeersResult{
				Peers:      []messaging.Peer{testPeer("rt-other", "other", "running")},
				Generation: "gen-1",
			}, nil
		},
	}
	p := newTestMessageProvider(t, cl)
	if _, err := p.peerRunState(context.Background(), "rt-missing"); !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("peerRunState miss error = %v, want ErrTargetUnavailable", err)
	}
}

func TestAuthorityToolErrorCodeGate(t *testing.T) {
	tests := []struct {
		name        string
		data        json.RawMessage
		wantCode    string
		wantMessage string
	}{
		{
			name:        "allowlisted typed code passes through",
			data:        json.RawMessage(`{"code":"unknown_target"}`),
			wantCode:    "unknown_target",
			wantMessage: "target gone",
		},
		{
			name:        "hyphenated wire class is refused",
			data:        json.RawMessage(`{"code":"generation-mismatch"}`),
			wantCode:    "refused",
			wantMessage: "rpc error [-32000]: target gone",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := authorityToolError(&client.RPCError{
				Code:    -32000,
				Message: "target gone",
				Data:    tc.data,
			})
			var toolErr *router.ToolError
			if !errors.As(err, &toolErr) {
				t.Fatalf("authorityToolError = %T %v, want *router.ToolError", err, err)
			}
			if toolErr.Code != tc.wantCode {
				t.Fatalf("tool error code = %q, want %q", toolErr.Code, tc.wantCode)
			}
			if toolErr.Message != tc.wantMessage {
				t.Fatalf("tool error message = %q, want %q", toolErr.Message, tc.wantMessage)
			}
		})
	}
}

// TestMCPMessageProviderResumeRegistrationFailureIsUnclassified proves a
// resume that ran but could not be registered is reported as a server-side
// failure, not as a typed refusal that would deny an action already performed.
// A label already held by another run makes the registry store reject the new
// entry, which is the realistic collision.
func TestMCPMessageProviderResumeRegistrationFailureIsUnclassified(t *testing.T) {
	const (
		heldLabel = "held"
		publicID  = "3f6a9d1c-2b44-4f0e-9d31-5a7c8e2b4d61"
	)
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{}}
	cl.resumeFunc = func(messaging.ResumeRequest) (messaging.ResumeResult, error) {
		return messaging.ResumeResult{
			RunID:        publicID,
			RuntimeID:    "rt-new",
			Label:        heldLabel,
			SentinelFile: filepath.Join(t.TempDir(), "avenor-run-"+publicID+".done"),
			OnEvent:      filepath.Join(t.TempDir(), "avenor-run-"+publicID+".log"),
		}, nil
	}
	p := newTestMessageProvider(t, cl)
	if err := p.server.registry.Store(&RunInfo{
		RunID:        "pub-holder",
		RuntimeID:    "rt-holder",
		Label:        heldLabel,
		SupervisorID: testSupervisorPath,
	}); err != nil {
		t.Fatalf("registry.Store: %v", err)
	}

	_, err := p.Resume(context.Background(), messaging.ResumeRequest{Target: "rt-holder", Message: "keep going", Label: heldLabel})
	if err == nil {
		t.Fatal("Resume error = nil, want the registration failure")
	}
	var toolErr *router.ToolError
	if errors.As(err, &toolErr) {
		t.Fatalf("Resume error = %T %v, want a plain error, not a router refusal", err, err)
	}
	for _, want := range []string{"resume succeeded", "registration failed"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Resume error = %q, want it to name %q", err, want)
		}
	}
}

// TestMCPMessageProviderResumeRegistersRun proves the resume happy path stores
// the run the control verb reports, so a later call resolves the resumed
// runtime by the public run id, and the result is passed through unchanged.
func TestMCPMessageProviderResumeRegistersRun(t *testing.T) {
	const (
		publicID  = "0b7c4f2a-93d8-4e51-8c6a-1f2e3d4c5b6a"
		runtimeID = "rt-resumed"
		label     = "resumed-label"
	)
	want := messaging.ResumeResult{
		RunID:        publicID,
		RuntimeID:    runtimeID,
		Label:        label,
		SessionID:    "sess-resumed",
		SentinelFile: filepath.Join(t.TempDir(), "avenor-run-"+publicID+".done"),
		OnEvent:      filepath.Join(t.TempDir(), "avenor-run-"+publicID+".log"),
	}
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{}}
	cl.resumeFunc = func(messaging.ResumeRequest) (messaging.ResumeResult, error) {
		return want, nil
	}
	p := newTestMessageProvider(t, cl)

	got, err := p.Resume(context.Background(), messaging.ResumeRequest{
		Target:  "rt-old",
		Message: "keep going",
		Label:   label,
	})
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Resume result = %#v, want %#v passed through unchanged", got, want)
	}

	stored := p.server.registry.LookupLabel(testSupervisorPath, label)
	if stored == nil {
		t.Fatalf("registry has no entry for label %q after resume", label)
	}
	if stored.RunID != publicID || stored.RuntimeID != runtimeID {
		t.Fatalf("stored entry = %#v, want run id %q runtime %q", stored, publicID, runtimeID)
	}

	resolution, err := p.Resolve(context.Background(), publicID)
	if err != nil {
		t.Fatalf("Resolve(%q) error = %v", publicID, err)
	}
	wantResolution := messaging.Resolution{
		RunID:       runtimeID,
		PublicRunID: publicID,
		Label:       label,
		Match:       messaging.MatchUnique,
	}
	if !reflect.DeepEqual(resolution, wantResolution) {
		t.Fatalf("Resolve(%q) = %#v, want %#v", publicID, resolution, wantResolution)
	}
}

// TestMCPMessageProviderResumeRegistersRunWhenStatusFails proves a status miss
// after a successful resume leaves the identity empty instead of failing the
// registration: the resumed run stays in the registry with the ids the control
// verb reported, and the result comes back unchanged.
func TestMCPMessageProviderResumeRegistersRunWhenStatusFails(t *testing.T) {
	const (
		publicID  = "7d1c9a52-4b8e-4f60-9c37-2a5e8b1d3f47"
		runtimeID = "rt-resumed-unstatusable"
		label     = "resumed-unstatusable-label"
	)
	want := messaging.ResumeResult{
		RunID:        publicID,
		RuntimeID:    runtimeID,
		Label:        label,
		SessionID:    "sess-resumed-unstatusable",
		SentinelFile: filepath.Join(t.TempDir(), "avenor-run-"+publicID+".done"),
		OnEvent:      filepath.Join(t.TempDir(), "avenor-run-"+publicID+".log"),
	}
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{
		statusErr: errors.New("runtime not found"),
	}}
	cl.resumeFunc = func(messaging.ResumeRequest) (messaging.ResumeResult, error) {
		return want, nil
	}
	p := newTestMessageProvider(t, cl)

	got, err := p.Resume(context.Background(), messaging.ResumeRequest{
		Target:  "rt-old",
		Message: "keep going",
		Label:   label,
	})
	if err != nil {
		t.Fatalf("Resume error = %v, want the status failure to leave registration intact", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Resume result = %#v, want %#v passed through unchanged", got, want)
	}
	if !slices.Contains(cl.statusCapturedRuntimeIDs, runtimeID) {
		t.Fatalf("Status calls = %v, want the resumed runtime %q queried", cl.statusCapturedRuntimeIDs, runtimeID)
	}

	stored := p.server.registry.LookupLabel(testSupervisorPath, label)
	if stored == nil {
		t.Fatalf("registry has no entry for label %q after a failed status lookup", label)
	}
	if stored.RunID != publicID || stored.RuntimeID != runtimeID {
		t.Fatalf("stored entry = %#v, want run id %q runtime %q", stored, publicID, runtimeID)
	}
	if stored.SessionID != want.SessionID || stored.SentinelPath != want.SentinelFile || stored.EventLogPath != want.OnEvent {
		t.Fatalf("stored entry = %#v, want session %q sentinel %q event log %q", stored, want.SessionID, want.SentinelFile, want.OnEvent)
	}
	if stored.Agent != "" || stored.Model != "" || stored.Backend != "" {
		t.Fatalf("stored identity = agent %q model %q backend %q, want it empty on a status miss", stored.Agent, stored.Model, stored.Backend)
	}
}

// TestMCPMessageProviderResolveByPublicIDReapsStaleLabel proves the public-id
// rung reaps a pre-restart label mapping before it stores, so a supervisor
// restart that re-points a label does not leave the run resolvable by label
// but not by its public id.
func TestMCPMessageProviderResolveByPublicIDReapsStaleLabel(t *testing.T) {
	const (
		publicID = "9c1f0e77-58ad-4c3a-8b25-0d6e4f7a2b93"
		label    = "reused"
	)
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{listResult: []map[string]any{{
		"runtime_id":    "rt-live",
		"label":         label,
		"session_id":    "sess-live",
		"sentinel_file": filepath.Join(t.TempDir(), "avenor-run-"+publicID+".done"),
	}}}}
	p := newTestMessageProvider(t, cl)
	if err := p.server.registry.Store(&RunInfo{
		RunID:        "pub-stale",
		RuntimeID:    "rt-stale",
		Label:        label,
		SupervisorID: testSupervisorPath,
	}); err != nil {
		t.Fatalf("registry.Store: %v", err)
	}

	got, err := p.Resolve(context.Background(), publicID)
	if err != nil {
		t.Fatalf("Resolve(%q) error = %v", publicID, err)
	}
	want := messaging.Resolution{
		RunID:       "rt-live",
		PublicRunID: publicID,
		Label:       label,
		Match:       messaging.MatchUnique,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Resolve(%q) = %#v, want %#v", publicID, got, want)
	}
}

// TestMCPMessageProviderLookupRunByPublicIDRehydrates proves the id-only rung
// rehydrates an entry from the supervisor's list on a registry miss, reading
// the public run id out of the sentinel basename.
func TestMCPMessageProviderLookupRunByPublicIDRehydrates(t *testing.T) {
	const publicID = "5b8e2c40-7a19-4d63-9f02-1c3d5e7a9b48"
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{listResult: []map[string]any{{
		"runtime_id":    "rt-listed",
		"label":         "listed-label",
		"session_id":    "sess-listed",
		"sentinel_file": filepath.Join(t.TempDir(), "avenor-run-"+publicID+".done"),
		"on_event":      "/tmp/avenor-run-listed.log",
	}}}}
	p := newTestMessageProvider(t, cl)

	info, err := p.server.lookupRunByPublicID(cl, testSupervisorPath, publicID)
	if err != nil {
		t.Fatalf("lookupRunByPublicID(%q) error = %v", publicID, err)
	}
	if info == nil {
		t.Fatalf("lookupRunByPublicID(%q) = nil, want the rehydrated entry", publicID)
	}
	if info.RunID != publicID || info.RuntimeID != "rt-listed" || info.Label != "listed-label" {
		t.Fatalf("rehydrated entry = %#v, want run id %q runtime rt-listed label listed-label", info, publicID)
	}
	if info.SessionID != "sess-listed" || info.SupervisorID != testSupervisorPath {
		t.Fatalf("rehydrated entry = %#v, want session sess-listed on %q", info, testSupervisorPath)
	}
}

// TestMCPMessageProviderRunStateTranslatesStatus proves RunState reports the
// translated status, an in-flight turn as live, and a raw session id as
// resumable.
func TestMCPMessageProviderRunStateTranslatesStatus(t *testing.T) {
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{
		statusResult: map[string]any{
			"status":     "running",
			"phase":      "active",
			"session_id": "sess-1",
		},
	}}
	p := newTestMessageProvider(t, cl)

	got, err := p.RunState(context.Background(), "rt-running")
	if err != nil {
		t.Fatalf("RunState error = %v", err)
	}
	want := messaging.RunFacts{
		RunID:  "rt-running",
		Status: runstate.StatusRunning,
		// A non-terminal raw phase passes through Translate unchanged.
		Phase:     runstate.Phase("active"),
		Live:      true,
		Resumable: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RunState = %#v, want %#v", got, want)
	}
}

// TestMCPMessageProviderRunStateFallsBackToTerminalSentinel proves a run that
// finished before a supervisor restart is still reported as done. Status fails
// because the runtime is gone, and the registry's terminal sentinel answers;
// without a registry entry the original status error survives.
func TestMCPMessageProviderRunStateFallsBackToTerminalSentinel(t *testing.T) {
	const publicID = "3f6a9d1c-2b44-4f0e-9d31-5a7c8e2b4d61"
	sentinelPath := filepath.Join(t.TempDir(), "avenor-run-"+publicID+".done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_prior\n"), 0644); err != nil {
		t.Fatal(err)
	}
	statusErr := errors.New("no such runtime")
	cl := &fakeMessageControlClient{fakeClient: &fakeClient{statusErr: statusErr}}
	p := newTestMessageProvider(t, cl)
	if err := p.server.registry.Store(&RunInfo{
		SupervisorID: testSupervisorPath,
		RunID:        publicID,
		RuntimeID:    "rt_1",
		Label:        "finished",
		SentinelPath: sentinelPath,
	}); err != nil {
		t.Fatalf("registry.Store: %v", err)
	}

	got, err := p.RunState(context.Background(), publicID)
	if err != nil {
		t.Fatalf("RunState error = %v", err)
	}
	want := messaging.RunFacts{
		RunID:     publicID,
		Status:    runstate.StatusDone,
		Live:      false,
		Resumable: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RunState = %#v, want %#v", got, want)
	}

	// The fallback is narrow: an unknown run id keeps the status failure.
	if _, err := p.RunState(context.Background(), "run-unknown"); !errors.Is(err, statusErr) {
		t.Fatalf("RunState of an unregistered run error = %v, want %v", err, statusErr)
	}
}

// TestNewMCPMessageProviderReturnsPreflightError proves the constructor
// surfaces the preflight failure instead of returning a provider.
func TestNewMCPMessageProviderReturnsPreflightError(t *testing.T) {
	preflightErr := errors.New("control store unavailable")
	cl := &fakeMessageControlClient{
		fakeClient: &fakeClient{},
		preflightFunc: func(context.Context) (messaging.PreflightResult, error) {
			return messaging.PreflightResult{}, preflightErr
		},
	}

	p, err := newMCPMessageProvider(context.Background(), &Server{registry: NewRunRegistry()}, cl, testSupervisorPath)
	if !errors.Is(err, preflightErr) {
		t.Fatalf("newMCPMessageProvider error = %v, want %v", err, preflightErr)
	}
	if p != nil {
		t.Fatalf("newMCPMessageProvider returned a provider on failure: %#v", p)
	}
}

// TestNewMCPMessageProviderSurfacesCancelledPreflight proves a cancelled
// constructor context fails the preflight rather than returning a provider
// bound to an empty generation.
func TestNewMCPMessageProviderSurfacesCancelledPreflight(t *testing.T) {
	cl := &fakeMessageControlClient{
		fakeClient: &fakeClient{},
		preflightFunc: func(ctx context.Context) (messaging.PreflightResult, error) {
			return messaging.PreflightResult{}, ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p, err := newMCPMessageProvider(ctx, &Server{registry: NewRunRegistry()}, cl, testSupervisorPath)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("newMCPMessageProvider error = %v, want context.Canceled", err)
	}
	if p != nil {
		t.Fatalf("newMCPMessageProvider returned a provider on a cancelled context: %#v", p)
	}
}
