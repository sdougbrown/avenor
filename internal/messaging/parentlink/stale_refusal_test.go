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
	relayed, replacement := sentRelayServer(t)

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
	askRoutedTarget(t, parent, "target", "the replacement branch", "the replacement branch", relayed)
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
	askRoutedTarget(t, parent, "target", "the restarted run", "the restarted run's branch", stale.relayed)
}

// TestDelayedDeregisterKeepsTheReplacementRouted pins the deregistration half
// of the retirement rule: a deregistration is posted fire-and-forget with a
// client-side timeout, so a same-identity replacement can re-register while it
// is in flight. The deregistration carries the view of the registration the
// closing branch last published, and a registration that no longer matches it is
// refused: the replacement's claim and run record stay, where an unconditional
// retirement would take the healthy branch out of routing.
func TestDelayedDeregisterKeepsTheReplacementRouted(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")

	registerDescendant(t, parent, "branch-delta", "http://stale-delta", "gen-old", "target", "token-old")
	// The replacement re-registered under the same durable identity before the
	// closing branch's deregistration landed.
	registerDescendant(t, parent, "branch-delta", "http://replacement-delta", "gen-new", "target", "token-new")

	deregisterWith(t, parent, "branch-delta", "http://stale-delta", "gen-old",
		http.StatusConflict, "generation-mismatch", "target", "token-old")

	if owner, ok := parent.link.Registry().Owner("target"); !ok || owner != "branch-delta" {
		t.Fatalf("run owner = (%q, %v), want the replacement branch-delta: the delayed deregistration retired its claim", owner, ok)
	}
	if st := parent.broker.GetRun("target"); st == nil {
		t.Fatal("the delayed deregistration deleted the replacement branch's run record")
	}
	if token, ok := parent.broker.RunToken("target"); !ok || token != "token-new" {
		t.Fatalf("run record credential = (%q, %v), want the replacement's", token, ok)
	}
}

// TestDeregisterRetiresTheClaimsItNames pins the matching half of the
// deregistration rule: a deregistration whose view still matches the
// registration retires every claim it holds and deletes the run records the
// claims carried, so a closed branch stops being served locally. The records
// are deleted through the credential they were registered with, the way a relay
// refusal retires its failed claim.
func TestDeregisterRetiresTheClaimsItNames(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")

	registerDescendant(t, parent, "branch-delta", "http://delta", "gen-old",
		"target", "token-a", "second", "token-b")
	for _, runID := range []string{"target", "second"} {
		if !parent.link.Registry().Owns(runID) || parent.broker.GetRun(runID) == nil {
			t.Fatalf("run %q was never claimed with a record", runID)
		}
	}

	deregisterDescendant(t, parent, "branch-delta", "http://delta", "gen-old",
		"target", "token-a", "second", "token-b")

	for _, runID := range []string{"target", "second"} {
		if parent.link.Registry().Owns(runID) {
			t.Fatalf("the deregistration left run %q claimed", runID)
		}
		if st := parent.broker.GetRun(runID); st != nil {
			t.Fatalf("the deregistration left run %q served locally", runID)
		}
	}

	// The claims are terminal: the runs are claimable again once a branch
	// re-registers.
	registerDescendant(t, parent, "branch-delta", "http://delta", "gen-new", "target", "token-a2")
	if !parent.link.Registry().Owns("target") {
		t.Fatal("run target was not claimable after the branch re-registered")
	}
}

// TestDeregisterAcceptsAViewThatNamesARetiredRun pins the claim-set half of the
// deregistration rule: the parent is a second writer of a registration's claim
// set, and a per-run relay refusal retires one claim between the branch's
// registrations, so the view a closing branch holds can name a run the
// registration no longer carries. The close is still the branch's own: it
// retires every claim the registration still holds, where refusing it over the
// retired run would strand the surviving claims on a branch that is gone.
func TestDeregisterAcceptsAViewThatNamesARetiredRun(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	stale := newBlockingEndpoint(t, http.StatusNotFound, "target-unavailable")

	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-old",
		"target", "token-a", "second", "token-b")

	// A relay for target finds the run gone: the branch itself answers that the
	// run is no longer there, so the parent retires target's claim alone.
	refusal := relayAsk(t, parent, "target", stale)
	stale.release()
	expectRefusedAsk(t, <-refusal)
	if parent.link.Registry().Owns("target") {
		t.Fatal("the per-run refusal left the retired run claimed")
	}

	// The branch closes with the view of its last accepted registration, which
	// still names target: the close retires the claim the registration holds.
	deregisterDescendant(t, parent, "branch-delta", stale.url(), "gen-old",
		"target", "token-a", "second", "token-b")

	if parent.link.Registry().Owns("second") {
		t.Fatal("the deregistration left the surviving run claimed")
	}
	if st := parent.broker.GetRun("second"); st != nil {
		t.Fatal("the deregistration left the surviving run served locally")
	}
}

// postDeregistration posts a deregistration that carries a view of a
// registration and returns the status and decoded body the parent answers it
// with, so the caller asserts the response the view earns. Each run is a run ID
// followed by the credential the registration carried.
func postDeregistration(t *testing.T, parent *branch, childID, endpoint, generation string, runs ...string) (int, map[string]any) {
	t.Helper()
	if len(runs)%2 != 0 {
		t.Fatalf("deregister needs a run ID and token per run, got %d arguments", len(runs))
	}
	claims := make([]map[string]string, 0, len(runs)/2)
	for i := 0; i < len(runs); i += 2 {
		claims = append(claims, map[string]string{"run_id": runs[i], "token": runs[i+1]})
	}
	raw, err := json.Marshal(map[string]any{
		"child_id":   childID,
		"endpoint":   endpoint,
		"generation": generation,
		"runs":       claims,
	})
	if err != nil {
		t.Fatalf("marshal deregistration: %v", err)
	}
	resp, err := http.Post(parent.url()+pathDeregister, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("post deregistration: %v", err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

// deregisterWith posts a deregistration that carries a view of the registration
// and expects the status and code the parent answers a mismatch with: the view
// no longer matches the registration the parent holds.
func deregisterWith(t *testing.T, parent *branch, childID, endpoint, generation string, status int, code string, runs ...string) {
	t.Helper()
	got, decoded := postDeregistration(t, parent, childID, endpoint, generation, runs...)
	if got != status || decoded["code"] != code {
		t.Fatalf("deregister with a stale view = (%d, %v), want %d %s", got, decoded, status, code)
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
	askRoutedTarget(t, parent, "target", "the run with the refreshed generation", "the branch with the refreshed generation", stale.relayed)
}

// TestDelayedRefusalKeepsTheClaimAnotherChildTookOver pins the owner half of the
// per-run retirement rule: a refusal carries the child it was relayed to, so it
// retires nothing when the run is owned by a different child by the time it
// lands. Child A's claim is removed while its refusal is held, child B claims the
// run, and the refusal leaves B's claim, record and route alone.
func TestDelayedRefusalKeepsTheClaimAnotherChildTookOver(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	stale := newBlockingEndpoint(t, http.StatusNotFound, "target-unavailable")
	relayed, replacement := sentRelayServer(t)

	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-old", "target", "token-old")
	refusal := relayAsk(t, parent, "target", stale)

	// While the refusal is in flight, branch-delta drops its claim and
	// branch-echo takes the run over.
	deregisterDescendant(t, parent, "branch-delta", stale.url(), "gen-old", "target", "token-old")
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
	askRoutedTarget(t, parent, "target", "the run the other child took over", "the child that took the run over", relayed)
}

// TestDelayedRefusalKeepsTheClaimTheMovedEndpointCarries pins the endpoint half
// of the per-run retirement rule: a branch that re-registered from a new
// endpoint keeping its generation and credentials is compared claim by claim,
// so a refusal that described the old endpoint leaves the moved claim, the run
// record and the route alone.
func TestDelayedRefusalKeepsTheClaimTheMovedEndpointCarries(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	stale := newBlockingEndpoint(t, http.StatusNotFound, "target-unavailable")
	relayed, moved := sentRelayServer(t)

	registerDescendant(t, parent, "branch-delta", stale.url(), "gen-old", "target", "token-old")
	refusal := relayAsk(t, parent, "target", stale)
	// The same child, the same generation and credential, a new endpoint: the
	// branch moved to an address the parent has not been told about yet.
	registerDescendant(t, parent, "branch-delta", moved.URL, "gen-old", "target", "token-old")
	stale.release()
	expectRefusedAsk(t, <-refusal)

	if !parent.link.Registry().Owns("target") {
		t.Fatal("the delayed refusal dropped the claim the moved endpoint carries")
	}
	if st := parent.broker.GetRun("target"); st == nil {
		t.Fatal("the delayed refusal deleted the re-registered run's record")
	}
	if token, ok := parent.broker.RunToken("target"); !ok || token != "token-old" {
		t.Fatalf("run record credential = (%q, %v), want the credential the registration kept", token, ok)
	}

	// The moved claim stays a routing target: the next ask reaches the endpoint
	// the registration now carries.
	askRoutedTarget(t, parent, "target", "the run at the moved endpoint", "the branch at the moved endpoint", relayed)
}

// sentRelayServer stands in for a branch endpoint that accepts every relay: it
// notes the first ask on relayed and answers sent, so a test can prove an ask
// reached the endpoint a claim now carries.
func sentRelayServer(t *testing.T) (<-chan struct{}, *httptest.Server) {
	t.Helper()
	relayed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case relayed <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"outcome":"sent"}`))
	}))
	t.Cleanup(server.Close)
	return relayed, server
}

// askRoutedTarget posts one fresh ask to target and asserts it is admitted (200
// sent) and reaches relayed within the await deadline, so a test can prove a
// claim stayed a routing target. asked and who name the target in the ask and
// channel messages.
func askRoutedTarget(t *testing.T, parent *branch, target, asked, who string, relayed <-chan struct{}) {
	t.Helper()
	answer := sendAsk(parent.broker.Addr(), "orchestrator", parent.token(t, "orchestrator"), target, newMsgID(t), parent.broker.Store().Generation())
	if answer.err != nil {
		t.Fatalf("ask %s: %v", asked, answer.err)
	}
	if answer.status != http.StatusOK || answer.body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("ask %s = (%d, %v), want 200 sent", asked, answer.status, answer.body)
	}
	select {
	case <-relayed:
	case <-time.After(awaitDeadline):
		t.Fatalf("%s never received an ask", who)
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
// does, sending the view of its registration the parent last accepted, so a test
// can hand a run from one child to another while a refusal is in flight.
func deregisterDescendant(t *testing.T, parent *branch, childID, endpoint, generation string, runs ...string) {
	t.Helper()
	got, _ := postDeregistration(t, parent, childID, endpoint, generation, runs...)
	if got != http.StatusOK {
		t.Fatalf("deregister descendant = %d, want 200", got)
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
