package agy

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

const liveRPCConcurrencyEnv = "AVENOR_AGY_LIVE_CONCURRENCY"

func requireLiveRPCConcurrency(t *testing.T) {
	t.Helper()
	if os.Getenv(liveRPCConcurrencyEnv) != "1" {
		t.Skip("set " + liveRPCConcurrencyEnv + "=1 to run against installed agy")
	}
}

// liveConcurrentSendGate holds each session's sole prompt mutation until both
// independent hosts reach the mutation boundary. This proves the turns are
// simultaneously active without inspecting or retaining either request body.
type liveConcurrentSendGate struct {
	arrivals atomic.Int32
	release  chan struct{}
	once     sync.Once
}

func newLiveConcurrentSendGate() *liveConcurrentSendGate {
	return &liveConcurrentSendGate{release: make(chan struct{})}
}

func (g *liveConcurrentSendGate) arrive(ctx context.Context) error {
	if g.arrivals.Add(1) == 2 {
		g.once.Do(func() { close(g.release) })
	}
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type liveConcurrentSendTransport struct {
	base http.RoundTripper
	gate *liveConcurrentSendGate
}

func (t *liveConcurrentSendTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if strings.HasSuffix(request.URL.Path, "/SendUserCascadeMessage") {
		if err := t.gate.arrive(request.Context()); err != nil {
			return nil, err
		}
	}
	return t.base.RoundTrip(request)
}

func startConcurrentLiveSession(t *testing.T, provider *Provider) (*ptyRPCHost, *liveStreamDropper, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	session, err := provider.Start(ctx, runtime.StartOptions{})
	if err != nil {
		t.Fatalf("start live RPC session: %v", err)
	}
	state := provider.getSession(session.SessionID)
	if state == nil {
		t.Fatal("live RPC session was not registered")
	}
	state.mu.Lock()
	host, ok := state.rpcHost.(*ptyRPCHost)
	state.mu.Unlock()
	if !ok || host == nil || host.rpc == nil || host.rpc.client == nil || host.rpc.client.http == nil {
		t.Fatal("live RPC host was not retained")
	}
	base := host.rpc.client.http.Transport
	if base == nil {
		t.Fatal("live RPC client has no transport")
	}
	dropper := &liveStreamDropper{base: base}
	host.rpc.client.http.Transport = dropper
	return host, dropper, host.processDone
}

type liveMarkerMatcher struct {
	pattern []byte
	prefix  []int
	matched int
	found   bool
}

func newLiveMarkerMatcher(pattern string) *liveMarkerMatcher {
	matcher := &liveMarkerMatcher{pattern: []byte(pattern), prefix: make([]int, len(pattern))}
	for index, length := 1, 0; index < len(matcher.pattern); {
		if matcher.pattern[index] == matcher.pattern[length] {
			length++
			matcher.prefix[index] = length
			index++
		} else if length > 0 {
			length = matcher.prefix[length-1]
		} else {
			index++
		}
	}
	return matcher
}

func (m *liveMarkerMatcher) write(text string) {
	for index := 0; index < len(text) && !m.found; index++ {
		for m.matched > 0 && text[index] != m.pattern[m.matched] {
			m.matched = m.prefix[m.matched-1]
		}
		if text[index] == m.pattern[m.matched] {
			m.matched++
			if m.matched == len(m.pattern) {
				m.found = true
			}
		}
	}
}

func concurrentRecorderCallback(recorder *liveTurnRecorder, expectedSession string, mismatch *atomic.Bool, own, other *liveMarkerMatcher) func(events.Event) {
	return func(event events.Event) {
		if event.SessionID != expectedSession {
			mismatch.Store(true)
		}
		if event.Event == "agent.message_chunk" {
			if delta, ok := event.Fields["delta"].(string); ok {
				own.write(delta)
				other.write(delta)
			}
		}
		recorder.onEvent(event)
	}
}

func TestLiveMarkerMatcherAcrossChunks(t *testing.T) {
	matcher := newLiveMarkerMatcher("ALPHA_OK")
	matcher.write("prefix AL")
	matcher.write("PHA_OK suffix")
	if !matcher.found {
		t.Fatal("split marker was not detected")
	}
	other := newLiveMarkerMatcher("BETA_OK")
	other.write("prefix ALPHA_OK suffix")
	if other.found {
		t.Fatal("different marker was detected")
	}
}

func TestLiveRPCConcurrentSessions(t *testing.T) {
	requireLiveRPCConcurrency(t)

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
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = provider.Close()
		}
	})

	host1, dropper1, processDone1 := startConcurrentLiveSession(t, provider)
	host2, dropper2, processDone2 := startConcurrentLiveSession(t, provider)
	if host1 == host2 || processDone1 == processDone2 || host1.terminal.PID() == host2.terminal.PID() {
		t.Fatal("concurrent sessions shared a host or process lifetime")
	}

	// Wrap the already-counting transports with one shared prompt-mutation
	// barrier so both real Agy sessions are active before either send proceeds.
	gate := newLiveConcurrentSendGate()
	host1.rpc.client.http.Transport = &liveConcurrentSendTransport{base: dropper1, gate: gate}
	host2.rpc.client.http.Transport = &liveConcurrentSendTransport{base: dropper2, gate: gate}

	recorder1 := newLiveTurnRecorder(dropper1, "", host1)
	recorder2 := newLiveTurnRecorder(dropper2, "", host2)
	var mismatch1, mismatch2 atomic.Bool
	own1, other1 := newLiveMarkerMatcher("CONCURRENT_ALPHA_OK"), newLiveMarkerMatcher("CONCURRENT_BETA_OK")
	own2, other2 := newLiveMarkerMatcher("CONCURRENT_BETA_OK"), newLiveMarkerMatcher("CONCURRENT_ALPHA_OK")
	callback1 := concurrentRecorderCallback(recorder1, host1.mapper.sessionID, &mismatch1, own1, other1)
	callback2 := concurrentRecorderCallback(recorder2, host2.mapper.sessionID, &mismatch2, own2, other2)

	start := make(chan struct{})
	var turns sync.WaitGroup
	var turnErr1, turnErr2 error
	turns.Add(2)
	go func() {
		defer turns.Done()
		<-start
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		turnErr1 = host1.RunTurn(ctx, "Reply with exactly CONCURRENT_ALPHA_OK.", "gemini-2.5-flash", callback1)
	}()
	go func() {
		defer turns.Done()
		<-start
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		turnErr2 = host2.RunTurn(ctx, "Reply with exactly CONCURRENT_BETA_OK.", "gemini-2.5-flash", callback2)
	}()
	close(start)
	turns.Wait()

	if turnErr1 != nil || turnErr2 != nil {
		t.Fatalf("concurrent turns failed: first=%v second=%v", turnErr1, turnErr2)
	}
	if gate.arrivals.Load() != 2 {
		t.Fatalf("concurrent prompt arrivals=%d, want 2", gate.arrivals.Load())
	}
	recorder1.assertCommon(t)
	recorder2.assertCommon(t)
	if mismatch1.Load() || mismatch2.Load() {
		t.Fatal("event session identity crossed between concurrent hosts")
	}

	if !own1.found || other1.found || !own2.found || other2.found {
		t.Fatal("concurrent session output marker crossed or was missing")
	}
	_, _, sends1, _, _ := dropper1.counts()
	_, _, sends2, _, _ := dropper2.counts()
	if sends1 != 1 || sends2 != 1 {
		t.Fatalf("concurrent prompt mutations: first=%d second=%d", sends1, sends2)
	}

	if err := provider.Close(); err != nil {
		t.Fatalf("close live RPC provider: %v", err)
	}
	closed = true
	for index, done := range []<-chan struct{}{processDone1, processDone2} {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("concurrent child %d was not reaped", index+1)
		}
	}
}
