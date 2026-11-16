package stable

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/permission"
)

func TestPooInboxWakePreservesParkedSupervisor(t *testing.T) {
	testPooInboxWake(t, wakePlain)
}

func TestPooInboxWakeRetainsPermissionAsk(t *testing.T) {
	testPooInboxWake(t, wakeControlAnswer)
}

func TestPooInboxWakeAnswersThroughPermissionFile(t *testing.T) {
	testPooInboxWake(t, wakeFileAnswer)
}

func TestPooInboxWakeHonorsAutoApprove(t *testing.T) {
	testPooInboxWake(t, wakeAutoApprove)
}

// answerPermissionFile replies to a parked child's ask through the permission
// file the launch derives from its sentinel, and checks the request carries the
// id the durable event log published.
func answerPermissionFile(t *testing.T, base, wantRequestID string) {
	t.Helper()
	requestPath, responsePath := permission.FilePaths(base)
	var request permission.FileRequest
	waitFor(t, "parked permission file request", func() bool {
		data, err := os.ReadFile(requestPath)
		return err == nil && json.Unmarshal(data, &request) == nil
	})
	if request.RequestID != wantRequestID {
		t.Fatalf("permission file request id = %q, want %q", request.RequestID, wantRequestID)
	}
	optionID := ""
	options, _ := request.Options.([]any)
	for _, option := range options {
		m, _ := option.(map[string]any)
		if m["kind"] == "allow" {
			optionID, _ = m["optionId"].(string)
			break
		}
	}
	if optionID == "" {
		t.Fatalf("permission file request offers no allow option: %+v", request.Options)
	}
	response, err := json.Marshal(map[string]string{"outcome": "selected", "option_id": optionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(responsePath, response, 0o600); err != nil {
		t.Fatal(err)
	}
}

// wakeMode is how a parked child's inbox wake may ask: whether its profile asks
// at all, and which resolver answers the ask.
type wakeMode int

const (
	// wakePlain runs a profile that never asks.
	wakePlain wakeMode = iota
	// wakeControlAnswer attaches a control client and answers with
	// RuntimeAnswerPermission.
	wakeControlAnswer
	// wakeFileAnswer answers through the launch's derived permission file.
	wakeFileAnswer
	// wakeAutoApprove lets the launch's own auto-approve answer.
	wakeAutoApprove
)

// asks reports whether this mode's profile puts a permission ask on the wire.
func (m wakeMode) asks() bool { return m != wakePlain }

// resolver is the resolver hint the launch's policy must publish with the ask.
func (m wakeMode) resolver() string {
	return map[wakeMode]string{
		wakeControlAnswer: "reserved",
		wakeFileAnswer:    "file",
		wakeAutoApprove:   "automatic",
	}[m]
}

// responseSource is the answer's source in the permission.response event: the
// resolver that actually answered.
func (m wakeMode) responseSource() string {
	return map[wakeMode]string{
		wakeControlAnswer: "control",
		wakeFileAnswer:    "file",
		wakeAutoApprove:   "avenor",
	}[m]
}

func testPooInboxWake(t *testing.T, mode wakeMode) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	for _, build := range []struct{ out, pkg string }{{filepath.Join(home, "avenor-test-bin"), "../../cmd/avenor"}, {filepath.Join(root, "poo"), "github.com/sdougbrown/poo/cmd/poo"}} {
		cmd := exec.Command("go", "build", "-o", build.out, build.pkg)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", build.pkg, err, output)
		}
	}
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", filepath.Join(root, "state"))
	t.Setenv("POO_FEEDS_PATH", "")
	t.Setenv("POO_CLI_REGISTRY_PATH", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if mode.asks() && n == 2 {
			fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"bash\",\"arguments\":\"{\\\"command\\\":\\\"echo wake\\\"}\"}}]}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
			return
		}
		fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"answer-%d\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", n)
	}))
	t.Cleanup(server.Close)
	settings := map[string]any{"version": 1, "endpoints": map[string]any{"fake": map[string]any{"endpoint": server.URL + "/v1"}}, "models": map[string]any{"test-model": map[string]any{"endpoint": "fake", "model_id": "test-model", "api": "chat_completions", "tools_mode": "native", "ops": []string{"chat"}, "context_limit": 200000}}, "roles": map[string]any{"chat": "test-model"}, "session": map[string]any{"mode": "logged", "dir": filepath.Join(root, "sessions")}}
	if mode.asks() {
		settings["permission"] = map[string]any{"unmatched": "ask", "fallback": "deny", "askers": []string{"wire"}, "chain_timeout_s": 10}
	}
	data, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	settingsPath := filepath.Join(root, "settings.json")
	if err := os.WriteFile(settingsPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	roster := map[string]any{"test": map[string]any{"backend": "poo", "command": filepath.Join(root, "poo"), "args": []string{"serve"}, "settings": settingsPath, "agent": "code", "model": "test-model"}}
	data, _ = json.Marshal(roster)
	rosterPath := filepath.Join(root, "roster.json")
	if err := os.WriteFile(rosterPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	socket := newStableSocketPath(t, "poo-wake")
	sup := newTestSupervisor(t, Config{ControlSocket: socket, MaxRuntimes: 2})
	t.Cleanup(func() { _ = sup.Shutdown("kill"); _ = sup.broker.Stop() })
	if mode == wakeControlAnswer {
		if err := sup.control.Start(socket); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { sup.control.Stop() })
		conn, err := client.Dial(socket)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if _, err := conn.Identity(); err != nil {
			t.Fatal(err)
		}
		attachOwnerForTest(t, conn, socket)
	}
	sentinel := filepath.Join(root, "done.env")
	eventPath := filepath.Join(root, "events.ndjson")
	on := true
	spawned, err := sup.spawn(SpawnParams{Prompt: "initial", Dir: root, RosterFile: rosterPath, RosterEntry: "test", SentinelFile: sentinel, OnEvent: eventPath, Messaging: &on, AutoApprove: mode == wakeAutoApprove})
	if err != nil {
		t.Fatal(err)
	}
	child := sup.runtimeByID(spawned.RuntimeID)
	waitForStableChild(t, child, func(active, completed bool, phase, label string) bool { return !active && phase == "done" })
	before, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	statusBefore, err := sup.RuntimeStatus(spawned.RuntimeID)
	if err != nil {
		t.Fatal(err)
	}
	resultBefore, err := sup.RuntimeResult(spawned.RuntimeID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(before), "DONE") {
		t.Fatalf("sentinel: %s", before)
	}
	// Observe the durable stream; another provider subscription would duplicate the watcher.
	if err := sup.broker.SendTo("supervisor", spawned.RuntimeID, "agent_message", json.RawMessage(`{"id":"wake-id","message":"wake","expects_reply":false}`), "wake-id"); err != nil {
		t.Fatal(err)
	}
	sessionID := statusBefore.(map[string]any)["session_id"].(string)
	sessionLog := filepath.Join(root, "sessions", sessionID+".jsonl")
	if mode.asks() {
		waitFor(t, "parked permission ask", func() bool {
			data, _ := os.ReadFile(sessionLog)
			return strings.Contains(string(data), `"record":"permission.asked"`)
		})
		var requestID, resolver string
		waitFor(t, "idle permission request", func() bool {
			data, _ := os.ReadFile(eventPath)
			for _, line := range strings.Split(string(data), "\n") {
				var event map[string]any
				if json.Unmarshal([]byte(line), &event) == nil && event["event"] == "permission.request" {
					requestID, _ = event["request_id"].(string)
					resolver, _ = event["resolver"].(string)
				}
			}
			return requestID != ""
		})
		if want := mode.resolver(); resolver != want {
			t.Fatalf("permission.request resolver = %q, want %q", resolver, want)
		}
		switch mode {
		case wakeControlAnswer:
			if err := sup.RuntimeAnswerPermission(spawned.RuntimeID, requestID, "allow", ""); err != nil {
				t.Fatal(err)
			}
		case wakeFileAnswer:
			answerPermissionFile(t, cli.DerivePermBase(sentinel), requestID)
		}
		waitFor(t, "wake tool result and settlement", func() bool {
			data, _ := os.ReadFile(sessionLog)
			return calls.Load() >= 3 && strings.Contains(string(data), "wake\\n") && strings.Count(string(data), `"record":"run.settled"`) >= 2
		})
		// The resolver's own record has to reach the log: the backend settles
		// whether or not Avenor recorded the answer it gave.
		waitFor(t, "permission response", func() bool {
			data, _ := os.ReadFile(eventPath)
			for _, line := range strings.Split(string(data), "\n") {
				var event map[string]any
				if json.Unmarshal([]byte(line), &event) == nil && event["event"] == "permission.response" {
					return event["source"] == mode.responseSource()
				}
			}
			return false
		})
		data, err := os.ReadFile(eventPath)
		if err != nil {
			t.Fatal(err)
		}
		if count := strings.Count(string(data), `"event":"permission.request"`); count != 1 {
			t.Fatalf("got %d permission requests, want one: %s", count, data)
		}
		// Answering an ask from a run nobody prompted must leave the parked
		// turn as it was: the ask belongs to the unattended run, not to the
		// turn the supervisor is still reporting.
		assertParkedPreserved(t, sup, spawned.RuntimeID, sentinel, before, statusBefore, resultBefore)
		return
	}
	waitFor(t, "wake settlement", func() bool {
		data, _ := os.ReadFile(sessionLog)
		eventData, _ := os.ReadFile(eventPath)
		return calls.Load() >= 2 && strings.Count(string(data), `"record":"run.settled"`) >= 2 && strings.Count(string(eventData), `"event":"session.end"`) >= 2
	})
	assertParkedPreserved(t, sup, spawned.RuntimeID, sentinel, before, statusBefore, resultBefore)
	resumed, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{RunID: resumeUUID1, Target: spawned.RuntimeID, Message: "continue", SentinelFile: filepath.Join(root, "resume.env"), EventLog: filepath.Join(root, "resume.log")})
	if err != nil {
		t.Fatal(err)
	}
	next := sup.runtimeByID(resumed.RuntimeID)
	waitForStableChild(t, next, func(active, completed bool, phase, label string) bool {
		return !active && phase == "done" && calls.Load() >= 3
	})
	result, err := sup.RuntimeResult(resumed.RuntimeID)
	if err != nil {
		t.Fatal(err)
	}
	if result.(map[string]any)["final_output"] != "answer-3" {
		t.Fatalf("resume result: %v", result)
	}
	resumeSentinel, err := os.ReadFile(filepath.Join(root, "resume.env"))
	if err != nil || !strings.Contains(string(resumeSentinel), "STOP_REASON=end_turn") {
		t.Fatalf("resume sentinel: %s %v", resumeSentinel, err)
	}
}

// assertParkedPreserved proves a wake left the parked turn untouched: the same
// sentinel, the same reported status and final output, and a child still idle.
func assertParkedPreserved(t *testing.T, sup *Supervisor, runtimeID, sentinel string, before []byte, statusBefore, resultBefore any) {
	t.Helper()
	after, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	statusAfter, _ := sup.RuntimeStatus(runtimeID)
	resultAfter, _ := sup.RuntimeResult(runtimeID)
	if resultBefore.(map[string]any)["final_output"] != "answer-1" || resultAfter.(map[string]any)["final_output"] != "answer-1" {
		t.Fatalf("parked result changed: %v -> %v", resultBefore, resultAfter)
	}
	if statusBefore.(map[string]any)["final_output"] != statusAfter.(map[string]any)["final_output"] {
		t.Fatalf("parked status changed: %v -> %v", statusBefore, statusAfter)
	}
	if string(before) != string(after) {
		t.Fatalf("sentinel changed: %s -> %s", before, after)
	}
	if status := statusAfter.(map[string]any); status["status"] != "idle" || status["exit_code"] != 0 {
		t.Fatalf("status after wake: %v", status)
	}
}
