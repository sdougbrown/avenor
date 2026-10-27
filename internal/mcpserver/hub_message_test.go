package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/stable"
)

const hubLinkToken = "hub-message-test-link-token"

// liveSupervisor is one supervisor process a test drives, with its parts.
type liveSupervisor struct {
	socket string
	url    string
	sup    *stable.Supervisor
	done   chan int
}

// startLiveSupervisor runs a supervisor in the background and waits for the
// broker URL it publishes beside its control socket, so a branch joins it the
// way an operator's branch does.
func startLiveSupervisor(t *testing.T, name string, cfg stable.Config) *liveSupervisor {
	t.Helper()
	cfg.ControlSocket = filepath.Join(t.TempDir(), name+".sock")
	cfg.MaxRuntimes = 4
	cfg.Stderr = io.Discard
	sup := stable.NewSupervisor(cfg)
	hub := &liveSupervisor{socket: cfg.ControlSocket, sup: sup, done: make(chan int, 1)}
	go func() { hub.done <- sup.Run() }()
	t.Cleanup(func() {
		_ = sup.Shutdown("kill")
		select {
		case <-hub.done:
		case <-time.After(15 * time.Second):
			t.Errorf("%s supervisor did not shut down", name)
		}
	})
	hub.url = awaitBrokerURLRecord(t, cfg.ControlSocket)
	return hub
}

// awaitBrokerURLRecord waits for the broker URL a supervisor publishes beside
// its control socket and returns it.
func awaitBrokerURLRecord(t *testing.T, socket string) string {
	t.Helper()
	path := socket + ".broker.url"
	deadline := time.Now().Add(15 * time.Second)
	for {
		if data, err := os.ReadFile(path); err == nil {
			if url := strings.TrimSpace(string(data)); url != "" {
				return url
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never appeared", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// branchPost posts one request to a branch broker, authenticating as runID when
// a credential is supplied, and returns the raw answer. Broker credentials
// travel in the JSON body for POST, which is how a run authenticates itself.
func branchPost(t *testing.T, url, runID, token string, request map[string]any) []byte {
	t.Helper()
	body := map[string]any{}
	for key, value := range request {
		body[key] = value
	}
	if runID != "" {
		body["run_id"] = runID
		body["token"] = token
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	httpClient := &http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post %s = %d %s", url, resp.StatusCode, raw)
	}
	return raw
}

// registerBranchRun registers a run on a branch broker the way one of that
// branch's own runs registers itself, and returns its credential.
func registerBranchRun(t *testing.T, brokerURL, runID string) string {
	t.Helper()
	var answer struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(branchPost(t, brokerURL+"/register", "", "", map[string]any{"run_id": runID}), &answer); err != nil {
		t.Fatal(err)
	}
	if answer.Token == "" {
		t.Fatalf("registering %s on the branch returned no credential", runID)
	}
	return answer.Token
}

// awaitHubPeer waits until the hub's own peer listing carries a run whose id
// starts with prefix, and returns that id.
func awaitHubPeer(t *testing.T, cl ControlClient, prefix string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		pre, err := cl.MessagePreflightContext(context.Background())
		if err == nil {
			peers, err := cl.MessagePeersContext(context.Background(), messaging.PeersRequest{Generation: pre.Generation})
			if err == nil {
				for _, peer := range peers.Peers {
					if strings.HasPrefix(peer.RunID, prefix) {
						return peer.RunID
					}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the hub never listed a run starting with %q", prefix)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// awaitBranchMessage drains a branch run's control queue until a message
// arrives, and returns it.
func awaitBranchMessage(t *testing.T, brokerURL, runID, token string) broker.ControlMessage {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var queued []broker.ControlMessage
		if err := json.Unmarshal(branchPost(t, brokerURL+"/poll-control", runID, token, map[string]any{
			"run_id": runID, "limit": 1,
		}), &queued); err != nil {
			t.Fatal(err)
		}
		if len(queued) > 0 {
			return queued[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no message reached %s", runID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestAwaitBranchMessageBacksOffBetweenPolls pins the cadence of the wait that
// drains a branch run's queue: a tight loop would hammer the branch broker and
// outrun the goroutine meant to fill it for the whole deadline.
func TestAwaitBranchMessageBacksOffBetweenPolls(t *testing.T) {
	started := time.Now()
	var polls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		polls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		// The queue fills 50ms in, so the wait has to poll without a message
		// to return for that window.
		if time.Since(started) < 50*time.Millisecond {
			_, _ = w.Write([]byte("[]"))
			return
		}
		_, _ = w.Write([]byte(`[{"type":"agent_message","run_id":"rt_branch"}]`))
	}))
	defer srv.Close()

	if got := awaitBranchMessage(t, srv.URL, "rt_branch", "token"); got.Type != "agent_message" {
		t.Fatalf("awaitBranchMessage = %+v, want the queued message", got)
	}
	if got := polls.Load(); got > 20 {
		t.Fatalf("awaitBranchMessage polled %d times in 50ms, want a bounded cadence", got)
	}
}

// callToolResult runs one merged-tool call and returns its result object, its
// error flag and any transport failure, so a caller that runs the tool off the
// test goroutine never fails the test from there.
func callToolResult(s *Server, args map[string]any) (map[string]any, bool, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, false, err
	}
	res, err := s.handleAvenorMessage(context.Background(), &mcpsdk.CallToolRequest{
		Params: &mcpsdk.CallToolParamsRaw{Arguments: raw},
	})
	if err != nil {
		return nil, false, err
	}
	object, _ := res.StructuredContent.(map[string]any)
	return object, res.IsError, nil
}

// callTool runs one merged-tool call on the test goroutine.
func callTool(t *testing.T, s *Server, args map[string]any) (map[string]any, bool) {
	t.Helper()
	object, isError, err := callToolResult(s, args)
	if err != nil {
		t.Fatalf("avenor_message %v: %v", args, err)
	}
	return object, isError
}

// TestHubHostMessageAddressesABranchRun pins the hub host's message tool
// against a real cross-broker delivery: a run a registered branch registered is
// addressable through the hub's tool, the tell and the ask both arrive in that
// run's own queue, the ask's reply comes back through the hub's store, and a
// target neither the hub nor a registered branch holds stays refused.
func TestHubHostMessageAddressesABranchRun(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("TMPDIR", home)

	hub := startLiveSupervisor(t, "hub", stable.Config{BrokerParentToken: hubLinkToken})
	branch := startLiveSupervisor(t, "branch", stable.Config{
		BrokerParentURL:   hub.url,
		BrokerParentToken: hubLinkToken,
	})
	const branchRunID = "rt_branch"
	token := registerBranchRun(t, branch.url, branchRunID)

	cl, err := client.AttachOwner(hub.socket)
	if err != nil {
		t.Fatalf("attach hub owner: %v", err)
	}
	defer cl.Close()
	server, err := NewServer(Options{
		Transport:        "stdio",
		NoAutostart:      true,
		ControlClient:    cl,
		SupervisorSocket: hub.socket,
		Stderr:           io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	// The hub addresses the branch's run under the key it adopted the claim
	// with, which is the id its own peer listing publishes.
	target := awaitHubPeer(t, cl, branchRunID+"@")

	// A tell is addressed to the adopted run and relayed into its queue.
	object, isError := callTool(t, server, map[string]any{
		"action": "tell", "target": target, "message": "TELL-FROM-HUB",
	})
	if isError {
		t.Fatalf("tell to the branch run was refused: %#v", object)
	}
	if object["outcome"] != string(messaging.OutcomeSent) || object["label"] != branchRunID {
		t.Fatalf("tell result = %#v, want sent to the run labelled %q", object, branchRunID)
	}
	told := awaitBranchMessage(t, branch.url, branchRunID, token)
	if told.Type != "agent_message" || !strings.Contains(string(told.Payload), "TELL-FROM-HUB") {
		t.Fatalf("relayed message = %+v, want the tell body", told)
	}

	// An ask is addressed the same way; its edge is the hub's, and the reply
	// the branch sends back reaches the hub's wait.
	type askOutcome struct {
		object map[string]any
		err    bool
		failed error
	}
	asked := make(chan askOutcome, 1)
	go func() {
		object, isError, err := callToolResult(server, map[string]any{
			"action": "ask", "target": target, "message": "ASK-FROM-HUB", "timeout_ms": 20000,
		})
		asked <- askOutcome{object: object, err: isError, failed: err}
	}()
	relayedAsk := awaitBranchMessage(t, branch.url, branchRunID, token)
	if relayedAsk.ID == "" || !strings.Contains(string(relayedAsk.Payload), "ASK-FROM-HUB") {
		t.Fatalf("relayed ask = %+v, want the ask body and its message id", relayedAsk)
	}
	branchPost(t, branch.url+"/send", branchRunID, token, map[string]any{
		"from_run_id": branchRunID,
		"to_run_id":   messaging.SupervisorRunID,
		"type":        "agent_message",
		"reply_to":    relayedAsk.ID,
		"payload":     map[string]any{"message": "PONG-FROM-BRANCH", "role": "agent"},
	})
	answer := <-asked
	if answer.failed != nil {
		t.Fatalf("ask to the branch run failed: %v", answer.failed)
	}
	if answer.err {
		t.Fatalf("ask to the branch run was refused: %#v", answer.object)
	}
	if answer.object["outcome"] != string(messaging.OutcomeReplied) || answer.object["reply"] != "PONG-FROM-BRANCH" {
		t.Fatalf("ask result = %#v, want the branch's reply", answer.object)
	}

	// A target neither the hub nor a registered branch holds is still refused.
	object, isError = callTool(t, server, map[string]any{
		"action": "tell", "target": "rt_ghost", "message": "NOBODY",
	})
	if !isError || object["code"] != "unknown_target" {
		t.Fatalf("unknown target = (%#v, error=%v), want the unknown_target refusal", object, isError)
	}
}
