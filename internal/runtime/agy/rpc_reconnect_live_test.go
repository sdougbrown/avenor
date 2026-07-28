package agy

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

const liveRPCReconnectEnv = "AVENOR_AGY_LIVE_RECONNECT"

// liveStreamDropper is an opt-in test transport. It closes only the currently
// active StreamAgentStateUpdates response body; unary snapshot and mutation
// calls continue through the production loopback transport unchanged.
type liveStreamDropper struct {
	base http.RoundTripper

	mu          sync.Mutex
	active      *liveDropBody
	streamOpens int
	snapshots   int
	sends       int
	handles     int
	drops       int
}

type liveDropBody struct {
	inner httpBody
	owner *liveStreamDropper
	once  sync.Once
}

type httpBody interface {
	Read([]byte) (int, error)
	Close() error
}

func (d *liveStreamDropper) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := d.base.RoundTrip(request)
	if err != nil {
		return response, err
	}

	d.mu.Lock()
	switch {
	case strings.HasSuffix(request.URL.Path, "/StreamAgentStateUpdates"):
		d.streamOpens++
		body := &liveDropBody{inner: response.Body, owner: d}
		d.active = body
		response.Body = body
	case strings.HasSuffix(request.URL.Path, "/GetCascadeTrajectorySteps"):
		d.snapshots++
	case strings.HasSuffix(request.URL.Path, "/SendUserCascadeMessage"):
		d.sends++
	case strings.HasSuffix(request.URL.Path, "/HandleCascadeUserInteraction"):
		d.handles++
	}
	d.mu.Unlock()
	return response, nil
}

func (b *liveDropBody) Read(payload []byte) (int, error) { return b.inner.Read(payload) }

func (b *liveDropBody) Close() error {
	var err error
	b.once.Do(func() {
		b.owner.mu.Lock()
		if b.owner.active == b {
			b.owner.active = nil
		}
		b.owner.mu.Unlock()
		err = b.inner.Close()
	})
	return err
}

func (d *liveStreamDropper) dropActive() bool {
	d.mu.Lock()
	body := d.active
	if body == nil {
		d.mu.Unlock()
		return false
	}
	d.drops++
	d.mu.Unlock()
	_ = body.Close()
	return true
}

func (d *liveStreamDropper) counts() (streams, snapshots, sends, handles, drops int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.streamOpens, d.snapshots, d.sends, d.handles, d.drops
}

type liveTurnRecorder struct {
	mu sync.Mutex

	deltaHash  hash.Hash
	deltaBytes int
	finalHash  [sha256.Size]byte
	finalBytes int
	finalSeen  bool

	chunks        int
	toolCalls     int
	toolUpdates   int
	permissions   int
	terminals     int
	stopReason    string
	eventCounts   map[string]int
	protocolCodes map[string]int

	dropOn  string
	drop    *liveStreamDropper
	dropped sync.Once

	host       *ptyRPCHost
	answerOnce sync.Once
	answerDone chan error
}

func newLiveTurnRecorder(dropper *liveStreamDropper, dropOn string, host *ptyRPCHost) *liveTurnRecorder {
	return &liveTurnRecorder{
		deltaHash:     sha256.New(),
		dropOn:        dropOn,
		drop:          dropper,
		host:          host,
		answerDone:    make(chan error, 1),
		eventCounts:   make(map[string]int),
		protocolCodes: make(map[string]int),
	}
}

func (r *liveTurnRecorder) onEvent(event events.Event) {
	r.mu.Lock()
	r.eventCounts[event.Event]++
	switch event.Event {
	case "agent.message_chunk":
		r.chunks++
		if delta, ok := event.Fields["delta"].(string); ok {
			_, _ = r.deltaHash.Write([]byte(delta))
			r.deltaBytes += len(delta)
		}
	case "tool.call":
		r.toolCalls++
	case "tool.call_update":
		r.toolUpdates++
	case "permission.request":
		r.permissions++
	case "avenor.agy.protocol":
		if code, ok := event.Fields["code"].(string); ok {
			r.protocolCodes[code]++
		}
	case "session.end":
		r.terminals++
		r.stopReason, _ = event.Fields["stop_reason"].(string)
		if final, ok := event.Fields["final_output"].(string); ok {
			r.finalHash = sha256.Sum256([]byte(final))
			r.finalBytes = len(final)
			r.finalSeen = true
		}
	}
	shouldDrop := event.Event == r.dropOn
	r.mu.Unlock()

	if shouldDrop {
		r.dropped.Do(func() { r.drop.dropActive() })
	}
	if event.Event == "permission.request" {
		requestID, _ := event.Fields["request_id"].(string)
		if requestID != "" {
			r.answerOnce.Do(func() {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					r.answerDone <- r.host.AnswerPermission(ctx, requestID, runtime.PermissionResponse{Allow: true, OptionID: "allow_once"})
				}()
			})
		}
	}
}

func (r *liveTurnRecorder) permissionCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.permissions
}

func (r *liveTurnRecorder) toolCounts() (calls, updates int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.toolCalls, r.toolUpdates
}

func (r *liveTurnRecorder) diagnosticCounts() (eventsByName, protocolByCode map[string]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	eventsByName = make(map[string]int, len(r.eventCounts))
	for name, count := range r.eventCounts {
		eventsByName[name] = count
	}
	protocolByCode = make(map[string]int, len(r.protocolCodes))
	for code, count := range r.protocolCodes {
		protocolByCode[code] = count
	}
	return eventsByName, protocolByCode
}

func (r *liveTurnRecorder) assertCommon(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminals != 1 || r.stopReason != "end_turn" {
		t.Fatalf("terminal count=%d stop_reason=%q", r.terminals, r.stopReason)
	}
	if !r.finalSeen || r.deltaBytes != r.finalBytes {
		t.Fatalf("canonical output lengths: deltas=%d final=%d seen=%v", r.deltaBytes, r.finalBytes, r.finalSeen)
	}
	if got := r.deltaHash.Sum(nil); !equalDigest(got, r.finalHash[:]) {
		t.Fatal("canonical deltas did not reconstruct the terminal output")
	}
}

func equalDigest(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var different byte
	for i := range left {
		different |= left[i] ^ right[i]
	}
	return different == 0
}

func requireLiveRPCReconnect(t *testing.T) {
	t.Helper()
	if os.Getenv(liveRPCReconnectEnv) != "1" {
		t.Skip("set " + liveRPCReconnectEnv + "=1 to run against installed agy")
	}
}

func startLiveReconnectHost(t *testing.T) (*Provider, *ptyRPCHost, *liveStreamDropper, <-chan struct{}) {
	t.Helper()
	workspace, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal("resolve live RPC workspace")
	}
	provider := NewWithOptions(runtime.StartOptions{Dir: workspace, Model: "gemini-2.5-flash"})
	provider.getenv = func(name string) string {
		if name == "AVENOR_AGY_TRANSPORT" {
			return "rpc"
		}
		return os.Getenv(name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	session, err := provider.Start(ctx, runtime.StartOptions{})
	if err != nil {
		_ = provider.Close()
		t.Fatalf("start live RPC provider: %v", err)
	}
	state := provider.getSession(session.SessionID)
	if state == nil {
		_ = provider.Close()
		t.Fatal("live RPC session was not registered")
	}
	state.mu.Lock()
	host, ok := state.rpcHost.(*ptyRPCHost)
	state.mu.Unlock()
	if !ok || host == nil || host.rpc == nil || host.rpc.client == nil || host.rpc.client.http == nil {
		_ = provider.Close()
		t.Fatal("live RPC host was not retained")
	}
	base := host.rpc.client.http.Transport
	if base == nil {
		_ = provider.Close()
		t.Fatal("live RPC client has no transport")
	}
	dropper := &liveStreamDropper{base: base}
	host.rpc.client.http.Transport = dropper
	return provider, host, dropper, host.processDone
}

func closeLiveReconnectHost(t *testing.T, provider *Provider, processDone <-chan struct{}) {
	t.Helper()
	if err := provider.Close(); err != nil {
		t.Fatalf("close live RPC provider: %v", err)
	}
	select {
	case <-processDone:
	case <-time.After(10 * time.Second):
		t.Fatal("live RPC child was not reaped")
	}
}

func runLiveReconnectTurn(t *testing.T, host *ptyRPCHost, recorder *liveTurnRecorder, prompt string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := host.RunTurn(ctx, prompt, "gemini-2.5-flash", recorder.onEvent); err != nil {
		t.Fatalf("live RPC turn: %v", err)
	}
}

func TestLiveRPCForcedReconnectDuringText(t *testing.T) {
	requireLiveRPCReconnect(t)
	provider, host, dropper, processDone := startLiveReconnectHost(t)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = provider.Close()
		}
	})

	recorder := newLiveTurnRecorder(dropper, "agent.message_chunk", host)
	runLiveReconnectTurn(t, host, recorder, "Produce sixty short numbered lines, then finish normally.")
	recorder.assertCommon(t)
	streams, snapshots, sends, _, drops := dropper.counts()
	if streams < 2 || snapshots < 2 || sends != 1 || drops != 1 {
		t.Fatalf("recovery counts: streams=%d snapshots=%d sends=%d drops=%d", streams, snapshots, sends, drops)
	}

	// A clean second turn proves that the recovered host remains reusable. The
	// recorder stores only digests and bounded counters, never reply content.
	reuse := newLiveTurnRecorder(dropper, "", host)
	runLiveReconnectTurn(t, host, reuse, "Reply with one short word.")
	reuse.assertCommon(t)
	_, _, sendsAfterReuse, _, _ := dropper.counts()
	if sendsAfterReuse != 2 {
		t.Fatalf("send mutations after reuse=%d, want 2", sendsAfterReuse)
	}

	closeLiveReconnectHost(t, provider, processDone)
	closed = true
}

func TestLiveRPCForcedReconnectDuringPermission(t *testing.T) {
	requireLiveRPCReconnect(t)
	provider, host, dropper, processDone := startLiveReconnectHost(t)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = provider.Close()
		}
	})

	recorder := newLiveTurnRecorder(dropper, "permission.request", host)
	runLiveReconnectTurn(t, host, recorder, "Use a terminal tool to run pwd exactly once. Wait for permission if needed, then reply briefly.")
	permissions := recorder.permissionCount()
	if permissions == 0 {
		t.Skip("installed agy policy did not request permission")
	}
	select {
	case err := <-recorder.answerDone:
		if err != nil {
			t.Fatalf("answer live permission: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("live permission answer did not finish")
	}
	recorder.assertCommon(t)
	if permissions != 1 {
		t.Fatalf("permission requests=%d, want 1", permissions)
	}
	streams, snapshots, sends, handles, drops := dropper.counts()
	if streams < 2 || snapshots < 2 || sends != 1 || handles != 1 || drops != 1 {
		t.Fatalf("recovery counts: streams=%d snapshots=%d sends=%d handles=%d drops=%d", streams, snapshots, sends, handles, drops)
	}

	closeLiveReconnectHost(t, provider, processDone)
	closed = true
}

func TestLiveRPCForcedReconnectDuringToolExecution(t *testing.T) {
	requireLiveRPCReconnect(t)
	provider, host, dropper, processDone := startLiveReconnectHost(t)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = provider.Close()
		}
	})

	recorder := newLiveTurnRecorder(dropper, "tool.call", host)
	runLiveReconnectTurn(t, host, recorder, "Use the directory-listing tool exactly once to inspect the current workspace, then reply briefly.")
	permissions := recorder.permissionCount()
	if permissions > 0 {
		select {
		case err := <-recorder.answerDone:
			if err != nil {
				t.Fatalf("answer live permission: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("live permission answer did not finish")
		}
	}
	recorder.assertCommon(t)
	toolCalls, toolUpdates := recorder.toolCounts()
	if toolCalls != 1 || toolUpdates != 1 {
		counts, codes := recorder.diagnosticCounts()
		t.Fatalf("tool lifecycle calls=%d updates=%d events=%v protocol=%v", toolCalls, toolUpdates, counts, codes)
	}
	streams, snapshots, sends, handles, drops := dropper.counts()
	if streams < 2 || snapshots < 2 || sends != 1 || drops != 1 {
		t.Fatalf("recovery counts: streams=%d snapshots=%d sends=%d handles=%d drops=%d", streams, snapshots, sends, handles, drops)
	}
	if permissions > 0 && handles != 1 {
		t.Fatalf("interaction mutations=%d, want 1", handles)
	}

	closeLiveReconnectHost(t, provider, processDone)
	closed = true
}

func TestLiveStreamDropperNoActiveStream(t *testing.T) {
	dropper := &liveStreamDropper{}
	if dropper.dropActive() {
		t.Fatal("drop succeeded without an active stream")
	}
	if _, _, _, _, drops := dropper.counts(); drops != 0 {
		t.Fatalf("drops=%d without active stream", drops)
	}
}

func TestLiveTurnRecorderRejectsDuplicateOutput(t *testing.T) {
	recorder := newLiveTurnRecorder(&liveStreamDropper{}, "", nil)
	recorder.onEvent(events.Event{Event: "agent.message_chunk", Fields: map[string]any{"delta": "same"}})
	recorder.onEvent(events.Event{Event: "agent.message_chunk", Fields: map[string]any{"delta": "same"}})
	recorder.onEvent(events.Event{Event: "session.end", Fields: map[string]any{"stop_reason": "end_turn", "final_output": "same"}})
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.deltaBytes == recorder.finalBytes || equalDigest(recorder.deltaHash.Sum(nil), recorder.finalHash[:]) {
		t.Fatal("duplicate canonical output was not distinguishable")
	}
}

func TestLiveDropBodyCloseIsIdempotent(t *testing.T) {
	body := &countingHTTPBody{}
	dropper := &liveStreamDropper{}
	wrapped := &liveDropBody{inner: body, owner: dropper}
	dropper.active = wrapped
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if body.closes != 1 {
		t.Fatalf("inner closes=%d, want 1", body.closes)
	}
}

type countingHTTPBody struct{ closes int }

func (*countingHTTPBody) Read([]byte) (int, error) { return 0, errors.New("closed") }
func (b *countingHTTPBody) Close() error {
	b.closes++
	return nil
}
