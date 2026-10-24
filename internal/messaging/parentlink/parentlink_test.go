package parentlink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// This file drives real brokers over their real HTTP surfaces: one parent and
// the branches that register with it. Every wait is on durable evidence (a
// registry entry, a queued message, an ask-store edge) polled to a deadline, so
// no test sleeps for a fixed time.

const awaitDeadline = 5 * time.Second

// branch is one broker wired to its parent exactly the way the supervisor wires
// it.
type branch struct {
	broker *broker.Broker
	link   *Messenger
	tokens map[string]string
}

// branchSeq names each test branch with a distinct durable identity, the way a
// supervisor derives one from its control socket path.
var branchSeq atomic.Uint64

func newTestChildID() string { return fmt.Sprintf("branch-%d", branchSeq.Add(1)) }

func (b *branch) url() string { return "http://" + b.broker.Addr() }

func (b *branch) token(t *testing.T, runID string) string {
	t.Helper()
	token, ok := b.tokens[runID]
	if !ok {
		t.Fatalf("run %q has no credential in this branch", runID)
	}
	return token
}

// pinned reports the parent generation this branch last registered under.
func (b *branch) pinned() (string, bool) {
	if b.link == nil || b.link.up == nil {
		return "", false
	}
	return b.link.up.pinnedGeneration()
}

// startBranch starts one broker, registers the given runs and installs the
// parent-aware messenger. An empty parentURL starts a root branch (a parent that
// has no parent of its own).
func startBranch(t *testing.T, parentURL string, runs ...string) *branch {
	t.Helper()
	return newBranch(t, parentURL, true, runs)
}

// startBranchTolerant is startBranch for a branch whose parent may not be
// listening: a failed registration leaves the branch serving locally, which is
// the state a supervisor started before its parent is up runs in.
func startBranchTolerant(t *testing.T, parentURL string, runs ...string) *branch {
	t.Helper()
	return newBranch(t, parentURL, false, runs)
}

func newBranch(t *testing.T, parentURL string, strict bool, runs []string) *branch {
	t.Helper()
	return newBranchWith(t, parentURL, newTestChildID(), DefaultRefreshInterval, strict, runs)
}

// newBranchWith is newBranch with the branch identity and refresh interval a
// test controls: the identity is what a restart reclaims its runs under, and the
// interval is how long an idle branch stays absent from a replaced parent.
func newBranchWith(t *testing.T, parentURL, childID string, refresh time.Duration, strict bool, runs []string) *branch {
	t.Helper()
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	branch := &branch{broker: b, tokens: map[string]string{}}
	for _, runID := range runs {
		token, err := b.CreateRun(runID, runID)
		if err != nil {
			t.Fatalf("create run %q: %v", runID, err)
		}
		branch.tokens[runID] = token
	}
	var (
		link *Messenger
		err  error
	)
	link, err = Install(Options{
		ParentURL:       parentURL,
		ChildID:         childID,
		RefreshInterval: refresh,
		Endpoint:        branch.url(),
		Generation:      b.Store().Generation,
		Runs: func() []messaging.RunClaim {
			return RunClaims(b.Sessions(),
				func(info broker.SessionInfo) (string, string) { return info.RunID, info.Label },
				link.Owns, b.RunToken)
		},
		// A run this branch stops owning loses its broker record too, so a
		// refused descendant stops being served locally.
		// A run this branch stops owning loses its broker record too, so a
		// refused descendant stops being served locally.
		RetireRun: b.DeleteRunIfToken,
	}, b)
	if err != nil {
		t.Fatalf("build parent link: %v", err)
	}
	branch.link = link
	t.Cleanup(func() { _ = link.Close() })
	if parentURL != "" {
		if err := link.Start(context.Background()); err != nil {
			if strict {
				t.Fatalf("register with parent: %v", err)
			}
			t.Logf("branch registration failed, retrying on the next run change: %v", err)
		}
	}
	return branch
}

// awaitRegistered waits until the parent broker has adopted every named run.
func awaitRegistered(t *testing.T, parent *branch, runs ...string) {
	t.Helper()
	deadline := time.Now().Add(awaitDeadline)
	for _, runID := range runs {
		for !parent.link.Registry().Owns(runID) {
			if time.Now().After(deadline) {
				t.Fatalf("parent never adopted run %q", runID)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
}

func postJSON(t *testing.T, addr, path, runID, token string, body map[string]any) (int, map[string]any) {
	t.Helper()
	body["run_id"] = runID
	body["token"] = token
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	resp, err := http.Post("http://"+addr+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

func getSessions(t *testing.T, addr, runID, token string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/sessions", nil)
	if err != nil {
		t.Fatalf("build sessions request: %v", err)
	}
	req.Header.Set(messaging.HeaderRunID, runID)
	req.Header.Set(messaging.HeaderRunToken, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get /sessions: %v", err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

func newMsgID(t *testing.T) string {
	t.Helper()
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("mint message id: %v", err)
	}
	return msgID
}

// ask sends one ask over a branch's HTTP surface, the way an agent does.
func ask(t *testing.T, from *branch, fromRunID, toRunID, msgID string) (int, map[string]any) {
	t.Helper()
	// An agent pins the generation of the broker it talks to; the link translates
	// that pin into the generation of the hop the ask is admitted on.
	return postJSON(t, from.broker.Addr(), "/send", fromRunID, from.token(t, fromRunID), map[string]any{
		"from_run_id":   fromRunID,
		"to_run_id":     toRunID,
		"type":          "agent_message",
		"payload":       map[string]any{"message": "ping"},
		"message_id":    msgID,
		"generation":    from.broker.Store().Generation(),
		"expects_reply": true,
	})
}

// reply answers a queued ask over the target branch's HTTP surface.
func reply(t *testing.T, from *branch, fromRunID, toRunID, msgID, text string) (int, map[string]any) {
	t.Helper()
	return postJSON(t, from.broker.Addr(), "/send", fromRunID, from.token(t, fromRunID), map[string]any{
		"from_run_id": fromRunID,
		"to_run_id":   toRunID,
		"type":        "agent_message",
		"reply_to":    msgID,
		"payload":     map[string]any{"message": text},
	})
}

func waitReply(t *testing.T, from *branch, runID, msgID string, timeoutMS int64) (int, map[string]any) {
	t.Helper()
	return postJSON(t, from.broker.Addr(), "/wait_reply", runID, from.token(t, runID), map[string]any{
		"waiting_for": msgID,
		"generation":  from.broker.Store().Generation(),
		"timeout_ms":  timeoutMS,
	})
}

func withdraw(t *testing.T, from *branch, runID, msgID string) (int, map[string]any) {
	t.Helper()
	return postJSON(t, from.broker.Addr(), "/cancel_message", runID, from.token(t, runID), map[string]any{
		"cancel_message_id": msgID,
		"generation":        from.broker.Store().Generation(),
	})
}

// queuedAgentMessages reports the agent messages waiting in one run's queue.
func queuedAgentMessages(t *testing.T, b *broker.Broker, runID string) []*broker.ControlMessage {
	t.Helper()
	st := b.GetRun(runID)
	if st == nil {
		t.Fatalf("run %q is not in this broker", runID)
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	var queued []*broker.ControlMessage
	for _, msg := range st.ControlQueue {
		if msg.Type == "agent_message" {
			queued = append(queued, msg)
		}
	}
	return queued
}

// TestLocalAskNeverCrossesTheParentLink pins that a target this branch owns is
// answered here even when the parent broker is gone: no lookup and no
// publication crosses the link.
func TestLocalAskNeverCrossesTheParentLink(t *testing.T) {
	parent := startBranch(t, "")
	local := startBranch(t, parent.url(), "asker", "target")
	awaitRegistered(t, parent, "asker", "target")

	// The parent stops serving. A local ask must be unaffected.
	if err := parent.broker.Stop(); err != nil {
		t.Fatalf("stop parent broker: %v", err)
	}

	msgID := newMsgID(t)
	status, body := ask(t, local, "asker", "target", msgID)
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("local ask = (%d, %v), want 200 sent", status, body)
	}
	if edges := parent.broker.Store().ActiveCount(); edges != 0 {
		t.Fatalf("parent holds %d ask edges for a local ask, want 0", edges)
	}
	if queued := queuedAgentMessages(t, local.broker, "target"); len(queued) != 1 {
		t.Fatalf("target queue = %d messages, want the local ask", len(queued))
	}

	if status, body = reply(t, local, "target", "asker", msgID, "pong"); status != http.StatusOK {
		t.Fatalf("local reply = (%d, %v), want 200", status, body)
	}
	status, body = waitReply(t, local, "asker", msgID, 5000)
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("local wait = (%d, %v), want 200 replied", status, body)
	}
	payload, ok := body["payload"].(map[string]any)
	if !ok || payload["message"] != "pong" {
		t.Fatalf("local wait payload = %v, want the reply", body["payload"])
	}
}
