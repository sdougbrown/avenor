package parentlink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestDelayedRefusalKeepsTheReplacementRouted pins the claim half of the
// retirement rule: a relay posted to a branch's old endpoint can be refused after
// that branch came back and registered a new endpoint and generation. The claim
// it failed on no longer resolves to the registered hop, so nothing is retired and
// the replacement keeps its routing.
func TestDelayedRefusalKeepsTheReplacementRouted(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	stale := newBlockingEndpoint(t, http.StatusConflict, "generation-mismatch")
	relayed := make(chan struct{}, 1)
	replacement := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case relayed <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"outcome":"sent"}`))
	}))
	t.Cleanup(replacement.Close)

	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-old", "target", "token-old")
	if !parent.link.Registry().Owns("target") {
		t.Fatal("the parent never adopted the run the fake branch claimed")
	}

	// One ask is relayed to the endpoint the branch registered first, and that
	// endpoint holds the refusal until the replacement has registered.
	refusal := relayAsk(t, parent, "target", stale)
	registerDescendant(t, parent, "branch-delta", replacement.URL, "gen-new", "target", "token-new")
	stale.release()
	expectRefusedAsk(t, <-refusal)

	// The refusal described the endpoint it was posted to, not the branch that
	// registered while it was in flight.
	if !parent.link.Registry().Owns("target") {
		t.Fatal("the delayed refusal retired the replacement branch's claim")
	}
	if st := parent.broker.GetRun("target"); st == nil {
		t.Fatal("the delayed refusal deleted the replacement branch's run record")
	}
	if token, ok := parent.broker.RunToken("target"); !ok || token != "token-new" {
		t.Fatalf("run record credential = (%q, %v), want the replacement's", token, ok)
	}

	// The replacement stays a routing target: the next ask reaches it.
	secondID := newMsgID(t)
	answer := sendAsk(parent.broker.Addr(), "orchestrator", parent.token(t, "orchestrator"), "target", secondID, parent.broker.Store().Generation())
	if answer.err != nil {
		t.Fatalf("ask the replacement branch: %v", answer.err)
	}
	if answer.status != http.StatusOK || answer.body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("ask the replacement branch = (%d, %v), want 200 sent", answer.status, answer.body)
	}
	select {
	case <-relayed:
	case <-time.After(awaitDeadline):
		t.Fatal("the replacement branch never received an ask")
	}
}

// TestDelayedRefusalKeepsARestartedRunsCredential pins the credential half of the
// retirement rule: a run restarted inside a live branch keeps the branch's
// endpoint and generation but presents a fresh credential. The refusal describes
// the credential it authenticated with, so it leaves both the record and the claim
// in place: the restarted run keeps its ask edges and stays a routing target,
// where a dropped claim would make the branch's re-registration a permanent 409.
func TestDelayedRefusalKeepsARestartedRunsCredential(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	stale := newBlockingEndpoint(t, http.StatusConflict, "generation-mismatch")

	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-old", "target", "token-old")
	refusal := relayAsk(t, parent, "target", stale)
	// The same branch instance, the same endpoint and generation, a new credential:
	// this is one run restarting inside a live branch.
	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-old", "target", "token-new")
	stale.release()
	expectRefusedAsk(t, <-refusal)

	if token, ok := parent.broker.RunToken("target"); !ok || token != "token-new" {
		t.Fatalf("run record credential = (%q, %v), want the credential the branch refreshed", token, ok)
	}
	if st := parent.broker.GetRun("target"); st == nil {
		t.Fatal("the delayed refusal deleted the restarted run's record")
	}
	if !parent.link.Registry().Owns("target") {
		t.Fatal("the delayed refusal dropped the restarted run's claim")
	}

	// The restarted run stays a routing target: the next ask reaches the branch
	// instead of being admitted into the parent's own store with no consumer.
	secondID := newMsgID(t)
	answer := sendAsk(parent.broker.Addr(), "orchestrator", parent.token(t, "orchestrator"), "target", secondID, parent.broker.Store().Generation())
	if answer.err != nil {
		t.Fatalf("ask the restarted run: %v", answer.err)
	}
	if answer.status != http.StatusOK || answer.body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("ask the restarted run = (%d, %v), want 200 sent", answer.status, answer.body)
	}
	select {
	case <-stale.relayed:
	case <-time.After(awaitDeadline):
		t.Fatal("the restarted run's branch never received an ask")
	}
}

// TestOneGenerationRefusalRetiresTheWholeClaim pins the branch half of the
// retirement rule: a generation refusal is a verdict about the registration, not
// about one run, so one refusal retires every claim the registration carried —
// without costing one failing hop per run the branch owns. The refusal still
// names the registration it failed on: a claim the relay no longer resolves to is
// left alone, which the replacement and restarted-run tests above pin.
func TestOneGenerationRefusalRetiresTheWholeClaim(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	stale := newBlockingEndpoint(t, http.StatusConflict, "generation-mismatch")

	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-old",
		"target", "token-a", "second", "token-b")
	if !parent.link.Registry().Owns("second") {
		t.Fatal("the parent never adopted the second run the branch claimed")
	}

	refusal := relayAsk(t, parent, "target", stale)
	stale.release()
	expectRefusedAsk(t, <-refusal)

	for _, runID := range []string{"target", "second"} {
		if parent.link.Registry().Owns(runID) {
			t.Fatalf("the generation refusal left run %q claimed by a registration it refused", runID)
		}
		if st := parent.broker.GetRun(runID); st != nil {
			t.Fatalf("the generation refusal left run %q served locally", runID)
		}
	}

	// The runs are claimable again once the branch re-registers.
	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-new",
		"target", "token-a2", "second", "token-b2")
	for _, runID := range []string{"target", "second"} {
		if !parent.link.Registry().Owns(runID) {
			t.Fatalf("run %q was not claimable after the branch re-registered", runID)
		}
	}
}

// TestDelayedRefusalKeepsTheClaimARefreshedGenerationCarries pins the generation
// half of the per-run retirement rule: a run whose claim the refusal is retired
// against is compared claim by claim, so a re-registration that moved the
// generation but kept the endpoint and credential survives a refusal that
// described the generation the relay was posted with.
func TestDelayedRefusalKeepsTheClaimARefreshedGenerationCarries(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	stale := newBlockingEndpoint(t, http.StatusNotFound, "target-unavailable")

	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-old", "target", "token-old")
	if !parent.link.Registry().Owns("target") {
		t.Fatal("the parent never adopted the run the fake branch claimed")
	}

	refusal := relayAsk(t, parent, "target", stale)
	// The same child, the same endpoint and credential, a new generation: the
	// branch's ask store moved on without touching anything else the claim
	// carries.
	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-new", "target", "token-old")
	stale.release()
	expectRefusedAsk(t, <-refusal)

	if !parent.link.Registry().Owns("target") {
		t.Fatal("the delayed refusal dropped the claim the refreshed generation carries")
	}
	if st := parent.broker.GetRun("target"); st == nil {
		t.Fatal("the delayed refusal deleted the re-registered run's record")
	}
	if token, ok := parent.broker.RunToken("target"); !ok || token != "token-old" {
		t.Fatalf("run record credential = (%q, %v), want the credential the registration kept", token, ok)
	}

	// The refreshed claim stays a routing target: the next ask reaches it.
	secondID := newMsgID(t)
	answer := sendAsk(parent.broker.Addr(), "orchestrator", parent.token(t, "orchestrator"), "target", secondID, parent.broker.Store().Generation())
	if answer.err != nil {
		t.Fatalf("ask the run with the refreshed generation: %v", answer.err)
	}
	if answer.status != http.StatusOK || answer.body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("ask the run with the refreshed generation = (%d, %v), want 200 sent", answer.status, answer.body)
	}
	select {
	case <-stale.relayed:
	case <-time.After(awaitDeadline):
		t.Fatal("the branch with the refreshed generation never received an ask")
	}
}

// TestDelayedRefusalKeepsTheClaimAnotherChildTookOver pins the owner half of the
// per-run retirement rule: a refusal carries the child it was relayed to, so it
// retires nothing when the run is owned by a different child by the time it
// lands. Child A's claim is removed while its refusal is held, child B claims the
// run, and the refusal leaves B's claim, record and route alone.
func TestDelayedRefusalKeepsTheClaimAnotherChildTookOver(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	stale := newBlockingEndpoint(t, http.StatusNotFound, "target-unavailable")
	relayed := make(chan struct{}, 1)
	replacement := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case relayed <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"outcome":"sent"}`))
	}))
	t.Cleanup(replacement.Close)

	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-old", "target", "token-old")
	refusal := relayAsk(t, parent, "target", stale)

	// While the refusal is in flight, branch-delta drops its claim and
	// branch-echo takes the run over.
	deregisterDescendant(t, parent, "branch-delta")
	registerDescendant(t, parent, "branch-echo", replacement.URL, "gen-old", "target", "token-echo")
	stale.release()
	expectRefusedAsk(t, <-refusal)

	if owner, ok := parent.link.Registry().Owner("target"); !ok || owner != "branch-echo" {
		t.Fatalf("run owner = (%q, %v), want branch-echo: the refusal retired the child that took the run over", owner, ok)
	}
	if token, ok := parent.broker.RunToken("target"); !ok || token != "token-echo" {
		t.Fatalf("run record credential = (%q, %v), want the taking-over child's", token, ok)
	}
	if st := parent.broker.GetRun("target"); st == nil {
		t.Fatal("the delayed refusal deleted the taking-over child's run record")
	}

	// The taking-over child stays a routing target: the next ask reaches it.
	secondID := newMsgID(t)
	answer := sendAsk(parent.broker.Addr(), "orchestrator", parent.token(t, "orchestrator"), "target", secondID, parent.broker.Store().Generation())
	if answer.err != nil {
		t.Fatalf("ask the run the other child took over: %v", answer.err)
	}
	if answer.status != http.StatusOK || answer.body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("ask the run the other child took over = (%d, %v), want 200 sent", answer.status, answer.body)
	}
	select {
	case <-relayed:
	case <-time.After(awaitDeadline):
		t.Fatal("the child that took the run over never received an ask")
	}
}

// relayAsk posts one ask to target in the background, so the test can register a
// refreshed claim while the relay is in flight. It waits until the endpoint that
// owns the target has received the relay.
func relayAsk(t *testing.T, parent *branch, target string, endpoint *blockingEndpoint) <-chan sendAnswer {
	t.Helper()
	msgID := newMsgID(t)
	asker := parent.token(t, "orchestrator")
	generation := parent.broker.Store().Generation()
	answers := make(chan sendAnswer, 1)
	go func() {
		answers <- sendAsk(parent.broker.Addr(), "orchestrator", asker, target, msgID, generation)
	}()
	select {
	case <-endpoint.entered:
	case <-time.After(awaitDeadline):
		t.Fatal("the target's endpoint never received the relay")
	}
	return answers
}

// expectRefusedAsk reports whether the answer is the refusal a replaced branch's
// relay produces, so a test knows the retirement path ran at all.
func expectRefusedAsk(t *testing.T, answer sendAnswer) {
	t.Helper()
	if answer.err != nil {
		t.Fatalf("ask the refusing endpoint: %v", answer.err)
	}
	if answer.status != http.StatusNotFound || answer.body["code"] != "target-unavailable" {
		t.Fatalf("ask refused by a replaced branch = (%d, %v), want 404 target-unavailable", answer.status, answer.body)
	}
}

// blockingEndpoint is a descendant broker's /relay route that holds its first
// answer until the test releases it, so a claim can be refreshed while the relay
// is in flight. It refuses that first relay with the status and code a branch
// answers a refused relay with, and accepts the relays after it, so a test can
// ask again once the claim settled.
type blockingEndpoint struct {
	server   *httptest.Server
	entered  chan struct{}
	released chan struct{}
	relayed  chan struct{}
	once     sync.Once
	mu       sync.Mutex
	requests int
}

func newBlockingEndpoint(t *testing.T, status int, code string) *blockingEndpoint {
	t.Helper()
	endpoint := &blockingEndpoint{
		entered:  make(chan struct{}),
		released: make(chan struct{}),
		relayed:  make(chan struct{}, 1),
	}
	endpoint.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		endpoint.mu.Lock()
		endpoint.requests++
		first := endpoint.requests == 1
		endpoint.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if !first {
			select {
			case endpoint.relayed <- struct{}{}:
			default:
			}
			_, _ = w.Write([]byte(`{"outcome":"sent"}`))
			return
		}
		endpoint.once.Do(func() { close(endpoint.entered) })
		<-endpoint.released
		w.WriteHeader(status)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"the relay was refused","code":%q}`, code)))
	}))
	t.Cleanup(endpoint.server.Close)
	return endpoint
}

func (e *blockingEndpoint) url() string { return e.server.URL }

func (e *blockingEndpoint) release() { close(e.released) }

// registerDescendant claims runs for a child broker the way a branch's link
// publishes its run set, so a test can register an endpoint it controls. Each
// run is a run ID followed by the credential the branch hands the parent for it.
func registerDescendant(t *testing.T, parent *branch, childID, endpoint, generation string, runs ...string) {
	t.Helper()
	if len(runs)%2 != 0 {
		t.Fatalf("register descendant needs a run ID and token per run, got %d arguments", len(runs))
	}
	claims := make([]map[string]string, 0, len(runs)/2)
	for i := 0; i < len(runs); i += 2 {
		claims = append(claims, map[string]string{"run_id": runs[i], "label": runs[i], "token": runs[i+1]})
	}
	raw, err := json.Marshal(map[string]any{
		"child_id":   childID,
		"endpoint":   endpoint,
		"generation": generation,
		"runs":       claims,
	})
	if err != nil {
		t.Fatalf("marshal registration: %v", err)
	}
	resp, err := http.Post(parent.url()+pathRegister, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("register descendant: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var decoded map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&decoded)
		t.Fatalf("register descendant = (%d, %v), want 200", resp.StatusCode, decoded)
	}
}

// deregisterDescendant retracts a child broker's claim the way a closing branch
// does, so a test can hand a run from one child to another while a refusal is in
// flight.
func deregisterDescendant(t *testing.T, parent *branch, childID string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"child_id": childID})
	if err != nil {
		t.Fatalf("marshal deregistration: %v", err)
	}
	resp, err := http.Post(parent.url()+pathDeregister, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("deregister descendant: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("deregister descendant = %d, want 200", resp.StatusCode)
	}
}

// sendAnswer is one /send response, decoded so a goroutine can report it without
// calling into the test.
type sendAnswer struct {
	status int
	body   map[string]any
	err    error
}

// sendAsk posts one ask over a broker's HTTP surface and reports the answer.
func sendAsk(addr, fromRunID, token, toRunID, msgID, generation string) sendAnswer {
	raw, err := json.Marshal(sendBody(fromRunID, token, toRunID, msgID, generation))
	if err != nil {
		return sendAnswer{err: err}
	}
	resp, err := http.Post("http://"+addr+"/send", "application/json", bytes.NewReader(raw))
	if err != nil {
		return sendAnswer{err: err}
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return sendAnswer{status: resp.StatusCode, body: decoded}
}
