package parentlink

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// gateProxy stands in front of a parent broker so a test can keep registration
// failing, swap which parent broker answers, or drop one response, all on the
// one URL the branch registered with.
type gateProxy struct {
	server *httptest.Server

	mu     sync.Mutex
	target string
	closed bool
	drop   string
}

func newGateProxy(t *testing.T, target string) *gateProxy {
	t.Helper()
	gate := &gateProxy{target: target, closed: true}
	gate.server = httptest.NewServer(http.HandlerFunc(gate.serve))
	t.Cleanup(gate.server.Close)
	return gate
}

func (g *gateProxy) url() string { return g.server.URL }

// Open lets requests through to the current target.
func (g *gateProxy) Open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = false
}

// SetTarget points the gate at another parent broker.
func (g *gateProxy) SetTarget(target string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.target = target
}

// DropResponse makes the next request to path reach its target, then lose the
// answer on the way back: the transmission happened, the caller never learns
// the outcome.
func (g *gateProxy) DropResponse(path string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.drop = path
}

func (g *gateProxy) serve(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	closed, target, drop := g.closed, g.target, g.drop
	if drop != "" && drop == r.URL.Path {
		g.drop = ""
	}
	g.mu.Unlock()
	if closed {
		http.Error(w, "parent not up yet", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target+r.URL.Path, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	for name, values := range r.Header {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		http.Error(w, "parent unreachable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	answer, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "parent answer unreadable", http.StatusBadGateway)
		return
	}
	if drop != "" {
		// The target handled the request; the answer is lost in transit.
		if hijacker, ok := w.(http.Hijacker); ok {
			if conn, _, err := hijacker.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		return
	}
	for name, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(answer)
}

// TestPublicationLoopStartsWithoutAnInitialRegistration pins that the loop is
// independent of the first registration: a branch whose parent was not up yet
// still re-registers when its run set changes, without any upward operation.
func TestPublicationLoopStartsWithoutAnInitialRegistration(t *testing.T) {
	parent := startBranch(t, "")
	gate := newGateProxy(t, parent.url())
	branch := startBranchTolerant(t, gate.url(), "asker")
	if _, registered := branch.pinned(); registered {
		t.Fatal("the branch registered with a parent that is not answering")
	}

	gate.Open()
	// A run change is the only trigger: nothing is forwarded upward here.
	if _, err := branch.broker.CreateRun("late", "late"); err != nil {
		t.Fatalf("create run after the failed registration: %v", err)
	}
	awaitRegistered(t, parent, "asker", "late")
}

// TestIdleBranchReRegistersWithAReplacedParent pins the periodic refresh: a
// parent that is replaced while the branch is idle is reached by the refresh
// interval, with no run change and no upward operation to prompt it.
func TestIdleBranchReRegistersWithAReplacedParent(t *testing.T) {
	first := startBranch(t, "")
	gate := newGateProxy(t, first.url())
	gate.Open()
	branch := newBranchWith(t, gate.url(), newTestChildID(), 20*time.Millisecond, true, []string{"asker"})
	awaitRegistered(t, first, "asker")

	replacement := startBranch(t, "")
	gate.SetTarget(replacement.url())
	if err := first.broker.Stop(); err != nil {
		t.Fatalf("stop the replaced parent: %v", err)
	}

	awaitUntil(t, "the idle branch to register with the replacement parent", func() bool {
		return replacement.link.Registry().Owns("asker")
	})
	generation, pinned := branch.pinned()
	if !pinned || generation != replacement.broker.Store().Generation() {
		t.Fatalf("pinned generation = (%q, %v), want the replacement parent's %q",
			generation, pinned, replacement.broker.Store().Generation())
	}
}
