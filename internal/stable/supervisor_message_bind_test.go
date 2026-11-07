package stable

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

func bindCaller() messaging.Caller {
	return messaging.Caller{Kind: messaging.CallerOwner, RunID: messaging.SupervisorRunID}
}

// TestMessageBindPiReusesTheSupervisorRun pins the reuse-only host contract:
// the pi host conversation binds to the supervisor's own broker run without
// minting a record, a repeated bind for the same native session returns the
// same credentials, and a second pi session is refused rather than retargeting
// the singleton.
func TestMessageBindPiReusesTheSupervisorRun(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	runID, token := sup.registerBrokerRun()
	if runID == "" || token == "" {
		t.Fatal("supervisor broker run unavailable")
	}

	res, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "pi", SessionID: "ses_pi"})
	if err != nil {
		t.Fatalf("message bind: %v", err)
	}
	if res.RunID != messaging.SupervisorRunID {
		t.Fatalf("pi bind run id = %q, want the supervisor run", res.RunID)
	}
	if res.Created {
		t.Fatal("pi bind must not mint a run")
	}
	if res.RunToken != token {
		t.Fatal("pi bind returned a credential other than the supervisor run's own")
	}
	if res.BrokerURL != sup.broker.URL() {
		t.Fatalf("pi bind broker url = %q, want %q", res.BrokerURL, sup.broker.URL())
	}
	if res.Generation != sup.broker.Store().Generation() {
		t.Fatalf("pi bind generation = %q, want %q", res.Generation, sup.broker.Store().Generation())
	}

	repeat, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "pi", SessionID: "ses_pi"})
	if err != nil {
		t.Fatalf("repeated pi bind: %v", err)
	}
	if repeat != res {
		t.Fatalf("repeated pi bind = %+v, want %+v", repeat, res)
	}

	if _, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "pi", SessionID: "ses_second"}); err == nil || bindRefusalCode(t, err) != "refused" {
		t.Fatalf("a second pi session must be refused, not bound to the supervisor singleton (err %v)", err)
	}

	again, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "pi", SessionID: "ses_pi"})
	if err != nil {
		t.Fatalf("pi bind after a refused second session: %v", err)
	}
	if again != res {
		t.Fatalf("pi bind after refusal = %+v, want %+v", again, res)
	}
}

// TestMessageBindMintsOneRunPerConversation pins the minting-host contract:
// each native conversation of a minting host gets its own broker-owned run,
// a repeated bind reuses that run, and the minted run carries no display
// label of its own — the peers list resolves it to the run id until a native
// rename names the work.
func TestMessageBindMintsOneRunPerConversation(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	first, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_1"})
	if err != nil {
		t.Fatalf("message bind: %v", err)
	}
	if !first.Created {
		t.Fatal("a first conversation bind must mint its run")
	}
	if first.RunID == "" || first.RunID == messaging.SupervisorRunID {
		t.Fatalf("minted run id = %q, want a fresh broker-owned run", first.RunID)
	}
	if first.RunToken == "" {
		t.Fatal("minted run carries no credential")
	}
	if first.BrokerURL != sup.broker.URL() || first.Generation != sup.broker.Store().Generation() {
		t.Fatalf("minted bind = %+v, want the local broker endpoint and generation", first)
	}
	record := sup.broker.GetRun(first.RunID)
	if record == nil {
		t.Fatalf("broker holds no run %q for the bound conversation", first.RunID)
	}
	if record.Token != first.RunToken {
		t.Fatal("bound credential does not match the broker's run record")
	}
	if sup.broker.DisplayName(first.RunID) != first.RunID {
		t.Fatalf("minted run display name = %q, want the run id", sup.broker.DisplayName(first.RunID))
	}

	repeat, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_1"})
	if err != nil {
		t.Fatalf("repeated bind: %v", err)
	}
	if repeat.RunID != first.RunID || repeat.RunToken != first.RunToken || repeat.Created {
		t.Fatalf("repeated bind = %+v, want the first run reused without minting", repeat)
	}

	second, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_2"})
	if err != nil {
		t.Fatalf("second conversation bind: %v", err)
	}
	if second.RunID == first.RunID {
		t.Fatal("two conversations must not share one bound run")
	}
}

// TestMessageBindRefusesAnExternallyRegisteredRun pins the ownership rule: a
// bind whose minted run id collides with a record an external writer
// registered is refused, never adopted with that writer's credential.
func TestMessageBindRefusesAnExternallyRegisteredRun(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	resp, err := http.Post(fmt.Sprintf("http://%s/register", sup.broker.Addr()), "application/json",
		strings.NewReader(`{"run_id":"external-run","token":"caller-chosen-token"}`))
	if err != nil {
		t.Fatalf("external registration: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("external registration status = %d: %s", resp.StatusCode, body)
	}

	sup.mintBoundRunID = func() string { return "external-run" }
	if _, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_x"}); err == nil || bindRefusalCode(t, err) != "refused" {
		t.Fatalf("a bind colliding with an externally registered run must be refused, not adopted (err %v)", err)
	}
	if sup.broker.GetRun("external-run").Token != "caller-chosen-token" {
		t.Fatal("the refused bind must leave the external record's credential untouched")
	}
}

// TestMessageBindCapsBoundConversations pins the bound-conversation limit: a
// supervisor refuses conversation binds beyond the cap whichever host asks, and
// an already bound conversation keeps reusing its run at capacity.
func TestMessageBindCapsBoundConversations(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	for i := 0; i < maxMessageBindings; i++ {
		req := messaging.MessageBindRequest{Host: "opencode", SessionID: fmt.Sprintf("ses_%d", i)}
		if _, err := sup.MessageBind(bindCaller(), req); err != nil {
			t.Fatalf("bind %d: %v", i, err)
		}
	}
	if _, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_over"}); err == nil || bindRefusalCode(t, err) != "refused" {
		t.Fatalf("a bind past the conversation cap must be refused (err %v)", err)
	}
	if _, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "pi", SessionID: "ses_pi"}); err == nil || bindRefusalCode(t, err) != "refused" {
		t.Fatalf("a pi bind past the conversation cap must be refused like any other host (err %v)", err)
	}
	repeat, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_0"})
	if err != nil {
		t.Fatalf("bound conversation at capacity: %v", err)
	}
	if repeat.Created {
		t.Fatal("a reused bind at capacity must not mint")
	}
}

// TestMessageBindConcurrentBindsShareOneRun pins the idempotence contract under
// contention: concurrent binds of the same native session all receive one run's
// credentials and exactly one of them mints.
func TestMessageBindConcurrentBindsShareOneRun(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	const binders = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	first := messaging.MessageBindResult{}
	for i := 0; i < binders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_concurrent"})
			if err != nil {
				t.Errorf("concurrent bind: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if first.RunID == "" {
				first = res
				created = 1
				return
			}
			if res.RunID != first.RunID || res.RunToken != first.RunToken {
				t.Errorf("concurrent bind = %+v, want the first bind's credentials %+v", res, first)
			}
			if res.Created {
				created++
			}
		}()
	}
	wg.Wait()
	if created != 1 {
		t.Fatalf("concurrent binds minted %d runs, want exactly one", created)
	}
	if sup.broker.GetRun(first.RunID) == nil {
		t.Fatalf("broker holds no record for the concurrently bound run %q", first.RunID)
	}
}

// TestMessageBindReclaimsUnavailableBindsAtCapacity pins capacity hygiene: a
// bind at the cap reclaims only the bound conversations the broker itself
// declares unavailable, keeps every live binding reusable, and only then
// admits the new conversation.
func TestMessageBindReclaimsUnavailableBindsAtCapacity(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	bound := make([]messaging.MessageBindResult, maxMessageBindings)
	for i := range bound {
		req := messaging.MessageBindRequest{Host: "opencode", SessionID: fmt.Sprintf("ses_%d", i)}
		res, err := sup.MessageBind(bindCaller(), req)
		if err != nil {
			t.Fatalf("bind %d: %v", i, err)
		}
		bound[i] = res
	}
	retireBoundPeer(t, sup, bound[3])
	retireBoundPeer(t, sup, bound[7])

	fresh, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_fresh"})
	if err != nil {
		t.Fatalf("bind after reclaiming unavailable peers: %v", err)
	}
	if !fresh.Created {
		t.Fatal("a bind admitted past reclaimed unavailable peers must mint")
	}
	if len(sup.messageBinds) != maxMessageBindings-1 {
		t.Fatalf("bound conversations after reclaim = %d, want %d", len(sup.messageBinds), maxMessageBindings-1)
	}
	for _, session := range []string{"ses_3", "ses_7"} {
		if _, ok := sup.messageBinds[messageBindKey{host: "opencode", sessionID: session}]; ok {
			t.Fatalf("retired bind %q must be reclaimed", session)
		}
	}
	for _, session := range []string{"ses_0", "ses_128", "ses_255"} {
		if _, ok := sup.messageBinds[messageBindKey{host: "opencode", sessionID: session}]; !ok {
			t.Fatalf("live bind %q must be preserved", session)
		}
	}
	repeat, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_128"})
	if err != nil {
		t.Fatalf("live bind at capacity: %v", err)
	}
	if repeat.Created || repeat.RunID != bound[128].RunID || repeat.RunToken != bound[128].RunToken {
		t.Fatalf("live bind at capacity = %+v, want %+v reused without minting", repeat, bound[128])
	}
}

// TestMessageBindConcurrentRebindsShareOneRun pins reclamation under
// contention: concurrent rebinds of one retired session all receive one fresh
// identity and exactly one of them mints.
func TestMessageBindConcurrentRebindsShareOneRun(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	retired, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_rebind"})
	if err != nil {
		t.Fatalf("initial bind: %v", err)
	}
	retireBoundPeer(t, sup, retired)

	const binders = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	first := messaging.MessageBindResult{}
	for i := 0; i < binders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_rebind"})
			if err != nil {
				t.Errorf("concurrent rebind: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if first.RunID == "" {
				first = res
				created = 1
				return
			}
			if res.RunID != first.RunID || res.RunToken != first.RunToken {
				t.Errorf("concurrent rebind = %+v, want the first rebind's credentials %+v", res, first)
			}
			if res.Created {
				created++
			}
		}()
	}
	wg.Wait()
	if created != 1 {
		t.Fatalf("concurrent rebinds minted %d runs, want exactly one", created)
	}
	if first.RunID == retired.RunID || first.RunToken == retired.RunToken {
		t.Fatalf("concurrent rebinds revived %+v, want a fresh identity", retired)
	}
	if sup.broker.GetRun(first.RunID) == nil {
		t.Fatalf("broker holds no record for the concurrently rebound run %q", first.RunID)
	}
}

// TestMessageBindRefusesANonOwnerCaller pins the authority boundary the bind
// handler itself enforces: only the owner caller binds, and a refused caller
// binds nothing.
func TestMessageBindRefusesANonOwnerCaller(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	broker := messaging.Caller{Kind: messaging.CallerBroker, RunID: "rt_child"}
	if _, err := sup.MessageBind(broker, messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_1"}); err == nil || bindRefusalCode(t, err) != "not_permitted" {
		t.Fatalf("a broker caller must not bind a conversation (err %v)", err)
	}
	res, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_1"})
	if err != nil || !res.Created {
		t.Fatalf("owner bind after a refused caller = (%+v, %v), want a fresh mint", res, err)
	}
}

// TestMessageBindBoundsAndRejectsHostileSessionIDs pins the session id rule:
// bounded in bytes and free of control, format and separator runes, so a
// session id stays a safe identifier and a safe record path component.
func TestMessageBindBoundsAndRejectsHostileSessionIDs(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	hostile := []string{
		"ses\x00",
		"ses\n",
		"ses\u200b", // zero-width space: a formatting rune
		"ses/../../etc",
		"ses\\win",
		".",
		"..",
		"ses\xfe", // invalid UTF-8
		strings.Repeat("s", messaging.MaxSessionIDBytes+1),
	}
	for _, session := range hostile {
		if _, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: session}); err == nil {
			t.Fatalf("session id %q must be refused", session)
		}
	}
	boundary := strings.Repeat("s", messaging.MaxSessionIDBytes)
	if _, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: boundary}); err != nil {
		t.Fatalf("a %d-byte session id must bind: %v", messaging.MaxSessionIDBytes, err)
	}
}

// TestMessageBindRemintsAfterBoundRunRemoval pins the reclamation answer to a
// stale mapping: when the bound run's record is removed through the broker's
// production retirement path, the next bind remints a fresh identity visibly,
// and the reclaimed binding then reuses that identity like any other.
func TestMessageBindRemintsAfterBoundRunRemoval(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	req := messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_gone"}
	res, err := sup.MessageBind(bindCaller(), req)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	if !sup.broker.DeleteRunIfToken(res.RunID, res.RunToken) {
		t.Fatal("the production retirement path must remove the bound record")
	}

	rebound, err := sup.MessageBind(bindCaller(), req)
	if err != nil {
		t.Fatalf("rebind after removal: %v", err)
	}
	if !rebound.Created || rebound.RunID == res.RunID || rebound.RunToken == res.RunToken {
		t.Fatalf("rebind after removal = %+v, want a fresh minted identity beside %+v", rebound, res)
	}
	repeat, err := sup.MessageBind(bindCaller(), req)
	if err != nil {
		t.Fatalf("repeated rebind after removal: %v", err)
	}
	if repeat.Created || repeat.RunID != rebound.RunID || repeat.RunToken != rebound.RunToken {
		t.Fatalf("repeated rebind = %+v, want %+v reused without minting", repeat, rebound)
	}
}

// TestMessageBindRefusesUnknownHostsAndEmptySessions pins the admission
// validation: only hosts with defined bind semantics are served, and a bind
// without a native session id is refused rather than bound to an unnamed
// conversation.
func TestMessageBindRefusesUnknownHostsAndEmptySessions(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)

	for host, session := range map[string]string{
		"claude":   "ses_c",
		"codex":    "ses_c",
		"opencode": "",
		"":         "ses_c",
	} {
		_, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: host, SessionID: session})
		if err == nil {
			t.Fatalf("bind host=%q session=%q must be refused", host, session)
		}
		if code := bindRefusalCode(t, err); code != "" {
			t.Fatalf("bind host=%q session=%q must stay untyped for the wire's invalid-request class, got code %q", host, session, code)
		}
	}
}

// TestMessageBindRequiresBrokerMessaging pins the availability boundary: a
// supervisor whose broker messaging never came up binds no conversation.
func TestMessageBindRequiresBrokerMessaging(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 2)
	sup.broker = nil
	sup.messaging = nil

	if _, err := sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "opencode", SessionID: "ses_1"}); err == nil || bindRefusalCode(t, err) != "refused" {
		t.Fatalf("a bind without broker messaging must be refused (err %v)", err)
	}
}

// bindRefusalCode reports the typed tool code a bind refusal carries, or ""
// when the error is untyped and falls back to the wire's invalid-request class.
func bindRefusalCode(t *testing.T, err error) string {
	t.Helper()
	var toolErr *router.ToolError
	if !errors.As(err, &toolErr) {
		return ""
	}
	return toolErr.Code
}
