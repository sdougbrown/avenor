package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// boundHandler serves the supervisor side of a configured session: the
// non-minting read a registered command makes, the feed's bind, and the three
// run verbs. It records every call so a test can tell which path a command took.
type boundHandler struct {
	control.StableHandler

	mu       sync.Mutex
	lookups  []messaging.MessageBindRequest
	binds    []messaging.MessageBindRequest
	spawned  []map[string]any
	statuses []string
	waits    []string
	results  []string

	bound     messaging.MessageBindResult
	bindErr   error
	lookupErr error
}

func (h *boundHandler) MessageBind(_ messaging.Caller, req messaging.MessageBindRequest) (messaging.MessageBindResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.binds = append(h.binds, req)
	if h.bindErr != nil {
		return messaging.MessageBindResult{}, h.bindErr
	}
	return h.bound, nil
}

func (h *boundHandler) MessageBindLookup(_ messaging.Caller, req messaging.MessageBindRequest) (messaging.MessageBindResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lookups = append(h.lookups, req)
	if h.lookupErr != nil {
		return messaging.MessageBindResult{}, h.lookupErr
	}
	return h.bound, nil
}

func (h *boundHandler) SpawnMCP(raw json.RawMessage) (any, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var params map[string]any
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, err
	}
	h.spawned = append(h.spawned, params)
	return map[string]any{"runtime_id": "rt_child", "session_id": "ses_child"}, nil
}

func (h *boundHandler) RuntimeStatus(runtimeID string) (any, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.statuses = append(h.statuses, runtimeID)
	return map[string]any{"runtime_id": runtimeID, "status": "running"}, nil
}

func (h *boundHandler) RuntimeWaitTurn(_ context.Context, runtimeID string, _ time.Duration) (any, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.waits = append(h.waits, runtimeID)
	return map[string]any{"runtime_id": runtimeID}, nil
}

func (h *boundHandler) RuntimeResult(runtimeID string) (any, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.results = append(h.results, runtimeID)
	return map[string]any{"final_output": "done"}, nil
}

// calls reports how many times each verb reached the handler.
func (h *boundHandler) calls() (lookups, binds, spawned int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.lookups), len(h.binds), len(h.spawned)
}

// binding is the answer the configured conversation's live binding gives.
const boundRunID = "rt_bound"

func liveBinding() messaging.MessageBindResult {
	return messaging.MessageBindResult{BrokerURL: "http://127.0.0.1:1", RunID: boundRunID, RunToken: "tok_bound", Generation: "g1"}
}

// boundCommandArgs is the static part of a configured command's argv and env: the
// socket, host and session the operator fixed, never a model argument.
func boundCommandArgs(verb, socket string, extra ...string) []string {
	return append([]string{verb, "--socket", socket, "--bind-host", "poo", "--bind-session", "ses_configured"}, extra...)
}

// TestBoundSpawnNamesTheBoundConversation pins the spawn a configured session
// makes: it reads the conversation's live binding and starts the child as that
// run's child, with no parent, host or supervisor named by the caller.
func TestBoundSpawnNamesTheBoundConversation(t *testing.T) {
	handler := &boundHandler{bound: liveBinding()}
	socket := startOwnerControlServer(t, handler)

	var out, stderr bytes.Buffer
	args := boundCommandArgs("spawn", socket, "--prompt", "work", "--dir", t.TempDir(), "--agent", "worker")
	if code := runDeclared(args, &out, &stderr); code != 0 {
		t.Fatalf("spawn exit = %d, stderr = %s", code, &stderr)
	}
	lookups, binds, spawned := handler.calls()
	if lookups != 1 || binds != 0 || spawned != 1 {
		t.Fatalf("lookups/binds/spawns = %d/%d/%d, want the read then one gated spawn", lookups, binds, spawned)
	}
	if got := handler.lookups[0]; got.Host != "poo" || got.SessionID != "ses_configured" {
		t.Fatalf("lookup = %+v, want the configured conversation", got)
	}
	params := handler.spawned[0]
	if params["parent_run_id"] != boundRunID {
		t.Fatalf("spawn parent_run_id = %v, want the bound run %q", params["parent_run_id"], boundRunID)
	}
	if params["agent"] != "worker" || params["prompt"] != "work" {
		t.Fatalf("spawn params = %+v", params)
	}
	if _, named := params["run_id"]; named {
		t.Fatalf("spawn params carry a caller-chosen run id: %+v", params)
	}
	if !strings.Contains(out.String(), `"runtime_id": "rt_child"`) {
		t.Fatalf("spawn stdout = %s, want the supervisor's spawn result", &out)
	}
}

// TestBoundSpawnRefusesWithoutALiveBinding pins the closed path: a conversation
// whose feed has not bound starts no child at all, and the refusal says what to
// do about it.
func TestBoundSpawnRefusesWithoutALiveBinding(t *testing.T) {
	handler := &boundHandler{
		lookupErr: fmt.Errorf("no live conversation binding for host %q session %q on this supervisor; the conversation's inbox feed must bind it first", "poo", "ses_configured"),
	}
	socket := startOwnerControlServer(t, handler)

	var out, stderr bytes.Buffer
	if code := runDeclared(boundCommandArgs("spawn", socket, "--prompt", "work", "--dir", t.TempDir()), &out, &stderr); code != 1 {
		t.Fatalf("spawn exit = %d, stderr = %s", code, &stderr)
	}
	if _, _, spawned := handler.calls(); spawned != 0 {
		t.Fatalf("spawns = %d, want none without a live binding", spawned)
	}
	if !strings.Contains(stderr.String(), "must bind it first") {
		t.Fatalf("stderr = %s, want the missing-binding refusal", &stderr)
	}
}

// TestBoundStatusAndResultReachTheRun pins the read commands: both resolve the
// configured conversation first, and --wait turn_complete waits through the
// supervisor's own bounded wait before the status is read.
func TestBoundStatusAndResultReachTheRun(t *testing.T) {
	handler := &boundHandler{bound: liveBinding()}
	socket := startOwnerControlServer(t, handler)

	var out, stderr bytes.Buffer
	if code := runDeclared(boundCommandArgs("status", socket, "--runtime", "rt_child"), &out, &stderr); code != 0 {
		t.Fatalf("status exit = %d, stderr = %s", code, &stderr)
	}
	if len(handler.statuses) != 1 || handler.statuses[0] != "rt_child" || len(handler.waits) != 0 {
		t.Fatalf("status reads = %v waits = %v, want one status read and no wait", handler.statuses, handler.waits)
	}

	out.Reset()
	if code := runDeclared(boundCommandArgs("status", socket, "--runtime", "rt_child", "--wait", "turn_complete", "--timeout-ms", "5000"), &out, &stderr); code != 0 {
		t.Fatalf("status --wait exit = %d, stderr = %s", code, &stderr)
	}
	if len(handler.waits) != 1 || handler.waits[0] != "rt_child" {
		t.Fatalf("waits = %v, want one turn_complete wait", handler.waits)
	}

	out.Reset()
	if code := runDeclared(boundCommandArgs("result", socket, "--runtime", "rt_child"), &out, &stderr); code != 0 {
		t.Fatalf("result exit = %d, stderr = %s", code, &stderr)
	}
	if len(handler.results) != 1 || !strings.Contains(out.String(), `"final_output": "done"`) {
		t.Fatalf("result = %s, want the run's final output", &out)
	}
	if lookups, _, _ := handler.calls(); lookups != 3 {
		t.Fatalf("lookups = %d, want one per command", lookups)
	}
}

// TestBoundCommandsRefusePartialCoordinates pins the configuration rule: a
// command that names only part of the attachment refuses before it dials, because
// half the coordinates name no conversation.
func TestBoundCommandsRefusePartialCoordinates(t *testing.T) {
	handler := &boundHandler{bound: liveBinding()}
	socket := startOwnerControlServer(t, handler)
	for _, args := range [][]string{
		{"spawn", "--socket", socket, "--prompt", "work", "--dir", t.TempDir()},
		{"status", "--socket", socket, "--runtime", "rt_child"},
		{"result", "--bind-host", "poo", "--runtime", "rt_child"},
	} {
		var out, stderr bytes.Buffer
		if code := runDeclared(args, &out, &stderr); code != 2 {
			t.Fatalf("%v exit = %d, stderr = %s, want a usage refusal", args, code, &stderr)
		}
		if !strings.Contains(stderr.String(), "required together") {
			t.Fatalf("%v stderr = %s", args, &stderr)
		}
	}
	if lookups, _, spawned := handler.calls(); lookups != 0 || spawned != 0 {
		t.Fatalf("lookups/spawns = %d/%d, want a refusal before any call", lookups, spawned)
	}
}

// TestBoundMessageResolvesCredentialsFromTheBinding pins the reply path a plain
// host session uses: configured coordinates and no credential, and the message
// still reaches the broker as the conversation's bound run.
func TestBoundMessageResolvesCredentialsFromTheBinding(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	token, _, err := b.EnsureRun(boundRunID, "bound")
	if err != nil {
		t.Fatal(err)
	}
	supervisorToken, _, err := b.EnsureRun("supervisor", "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	tr, err := localhttp.New(b.Addr(), messaging.LocalCredentials{RunID: "supervisor", Token: supervisorToken})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	pre, err := tr.Preflight(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	// The call is configured with the attachment's coordinates alone, so no
	// broker credential is in its environment.
	for _, key := range []string{"AVENOR_BROKER_URL", "AVENOR_RUN_ID", "AVENOR_BROKER_TOKEN"} {
		t.Setenv(key, "")
	}

	handler := &boundHandler{bound: messaging.MessageBindResult{
		BrokerURL: "http://" + b.Addr(), RunID: boundRunID, RunToken: token, Generation: pre.Generation,
	}}
	socket := startOwnerControlServer(t, handler)

	var out, stderr bytes.Buffer
	if code := runDeclared(boundCommandArgs("message", socket, "--action=tell", "--target=supervisor", "--message=hello"), &out, &stderr); code != 0 {
		t.Fatalf("message exit = %d, stderr = %s", code, &stderr)
	}
	lookups, binds, _ := handler.calls()
	if lookups != 1 || binds != 0 {
		t.Fatalf("lookups/binds = %d/%d, want the read and no bind", lookups, binds)
	}
	msg := receiveInbound(t, tr, pre.Generation)
	if msg.FromRunID != boundRunID || !strings.Contains(string(msg.Payload), "hello") {
		t.Fatalf("delivered message = %+v, want it from the bound run", msg)
	}
}

// TestBoundMessageKeepsTheCredentialPathWithoutCoordinates pins the legacy rule:
// a call that names no coordinates never reaches the supervisor's binding read,
// so a missing credential still fails exactly as it did.
func TestBoundMessageKeepsTheCredentialPathWithoutCoordinates(t *testing.T) {
	handler := &boundHandler{bound: liveBinding()}
	socket := startOwnerControlServer(t, handler)
	for _, key := range []string{"AVENOR_BROKER_URL", "AVENOR_RUN_ID", "AVENOR_BROKER_TOKEN", envControlSocket, envBindHost, envBindSession} {
		t.Setenv(key, "")
	}

	var out, stderr bytes.Buffer
	if code := runDeclared([]string{"message", "--action=tell", "--target=x", "--message=x"}, &out, &stderr); code != 2 {
		t.Fatalf("message exit = %d, stderr = %s, want the existing usage refusal", code, &stderr)
	}
	if !strings.Contains(stderr.String(), "--broker-url, --token and --run-id") {
		t.Fatalf("stderr = %s, want the credential usage refusal", &stderr)
	}
	if lookups, _, _ := handler.calls(); lookups != 0 {
		t.Fatalf("lookups = %d, want none without configured coordinates", lookups)
	}

	// The same missing credentials alongside configured coordinates refuse as a
	// conflict instead of quietly choosing one path.
	t.Setenv(envBindHost, "poo")
	t.Setenv(envBindSession, "ses_configured")
	if code := runDeclared([]string{"message", "--socket", socket, "--action=tell", "--target=x", "--message=x", "--broker-url=http://127.0.0.1:1", "--run-id=r", "--token=t"}, &out, &stderr); code != 2 {
		t.Fatalf("conflict exit = %d, stderr = %s", code, &stderr)
	}
	if !strings.Contains(stderr.String(), "mutually exclusive") {
		t.Fatalf("stderr = %s, want the conflict refusal", &stderr)
	}
}

// TestBoundMessagePrefersCoordinatesOverInheritedCredentials pins the rule a
// session that already holds a broker credential needs: only a credential the
// caller supplies conflicts with the coordinates. An inherited one stays put,
// and the call reaches the conversation through its binding.
func TestBoundMessagePrefersCoordinatesOverInheritedCredentials(t *testing.T) {
	handler := &boundHandler{bound: liveBinding()}
	socket := startOwnerControlServer(t, handler)
	t.Setenv("AVENOR_BROKER_URL", "http://127.0.0.1:1")
	t.Setenv("AVENOR_RUN_ID", "rt_inherited")
	t.Setenv("AVENOR_BROKER_TOKEN", "tok_inherited")
	t.Setenv(envBindHost, "poo")
	t.Setenv(envBindSession, "ses_configured")

	var out, stderr bytes.Buffer
	if code := runDeclared([]string{"message", "--socket", socket, "--action=tell", "--target=x", "--message=x"}, &out, &stderr); code == 2 {
		t.Fatalf("message exit = %d, stderr = %s, want the binding path", code, &stderr)
	}
	if lookups, _, _ := handler.calls(); lookups != 1 {
		t.Fatalf("lookups = %d, want the conversation's binding read", lookups)
	}
}

// TestBoundStatusRefusesANonPositiveWait pins the wait bound: an explicit
// timeout of zero would otherwise mean an unbounded wait.
func TestBoundStatusRefusesANonPositiveWait(t *testing.T) {
	handler := &boundHandler{bound: liveBinding()}
	socket := startOwnerControlServer(t, handler)

	var out, stderr bytes.Buffer
	if code := runDeclared([]string{"status", "--socket", socket, "--bind-host", "poo", "--bind-session", "ses_configured", "--runtime", "rt_1", "--timeout-ms=0"}, &out, &stderr); code != 2 {
		t.Fatalf("status exit = %d, stderr = %s, want the usage refusal", code, &stderr)
	}
	if !strings.Contains(stderr.String(), "--timeout-ms must be at least 1") {
		t.Fatalf("stderr = %s, want the timeout refusal", &stderr)
	}
	if lookups, _, _ := handler.calls(); lookups != 0 {
		t.Fatalf("lookups = %d, want none before the refusal", lookups)
	}
}

// TestFeedBrokerBindsTheConfiguredConversation pins the feed's side: the feed is
// the one consumer that binds, and it binds the configured conversation on the
// configured supervisor before it serves anything.
func TestFeedBrokerBindsTheConfiguredConversation(t *testing.T) {
	handler := &boundHandler{bound: liveBinding()}
	socket := startOwnerControlServer(t, handler)
	for _, key := range []string{"AVENOR_BROKER_URL", "AVENOR_RUN_ID", "AVENOR_BROKER_TOKEN"} {
		t.Setenv(key, "")
	}

	bound, err := bindConversation(t.Context(), boundCoordinates{socket: socket, host: "poo", session: "ses_configured"})
	if err != nil {
		t.Fatalf("bind conversation: %v", err)
	}
	if bound.RunID != boundRunID || bound.RunToken != "tok_bound" {
		t.Fatalf("bind = %+v, want the supervisor's answer", bound)
	}
	lookups, binds, _ := handler.calls()
	if binds != 1 || lookups != 0 {
		t.Fatalf("lookups/binds = %d/%d, want the feed to bind and never read", lookups, binds)
	}
	if got := handler.binds[0]; got.Host != "poo" || got.SessionID != "ses_configured" {
		t.Fatalf("bind request = %+v, want the configured conversation", got)
	}
}

// TestFeedBrokerRefusesCredentialsAndCoordinatesTogether pins the one rule the
// feed shares with the read commands: a call names its attachment one way.
func TestFeedBrokerRefusesCredentialsAndCoordinatesTogether(t *testing.T) {
	socket := startOwnerControlServer(t, &boundHandler{bound: liveBinding()})
	args := []string{"feed", "broker", "--socket", socket, "--bind-host", "poo", "--bind-session", "ses_configured",
		"--broker-url=http://127.0.0.1:1", "--run-id=r", "--token=t"}
	var out, stderr bytes.Buffer
	if code := runDeclared(args, &out, &stderr); code != 2 {
		t.Fatalf("feed exit = %d, stderr = %s", code, &stderr)
	}
	if !strings.Contains(stderr.String(), "mutually exclusive") {
		t.Fatalf("stderr = %s, want the conflict refusal", &stderr)
	}
}
