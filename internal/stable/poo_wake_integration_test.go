package stable

import (
	"context"
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
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
)

func TestPooInboxWakePreservesParkedSupervisor(t *testing.T) {
	testPooInboxWake(t, false)
}

func TestPooInboxWakeRetainsPermissionAsk(t *testing.T) {
	testPooInboxWake(t, true)
}

func testPooInboxWake(t *testing.T, ask bool) {
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
		if ask && n == 2 {
			fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"bash\",\"arguments\":\"{\\\"command\\\":\\\"echo wake\\\"}\"}}]}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
			return
		}
		fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"answer-%d\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", n)
	}))
	t.Cleanup(server.Close)
	settings := map[string]any{"version": 1, "endpoints": map[string]any{"fake": map[string]any{"endpoint": server.URL + "/v1"}}, "models": map[string]any{"test-model": map[string]any{"endpoint": "fake", "model_id": "test-model", "api": "chat_completions", "tools_mode": "native", "ops": []string{"chat"}, "context_limit": 200000}}, "roles": map[string]any{"chat": "test-model"}, "session": map[string]any{"mode": "logged", "dir": filepath.Join(root, "sessions")}}
	if ask {
		settings["permission"] = map[string]any{"unmatched": "ask", "fallback": "deny", "askers": []string{"wire"}, "chain_timeout_s": 60}
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
	if ask {
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
	spawned, err := sup.spawn(SpawnParams{Prompt: "initial", Dir: root, RosterFile: rosterPath, RosterEntry: "test", SentinelFile: sentinel, OnEvent: eventPath, Messaging: &on})
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
	var wakeEvents <-chan events.Event
	if !ask {
		child.mu.Lock()
		provider := child.provider
		child.mu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		wakeEvents, err = provider.Events(ctx, statusBefore.(map[string]any)["session_id"].(string))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := sup.broker.SendTo("supervisor", spawned.RuntimeID, "agent_message", json.RawMessage(`{"id":"wake-id","message":"wake","expects_reply":false}`), "wake-id"); err != nil {
		t.Fatal(err)
	}
	sessionID := statusBefore.(map[string]any)["session_id"].(string)
	sessionLog := filepath.Join(root, "sessions", sessionID+".jsonl")
	if ask {
		waitFor(t, "parked permission ask", func() bool {
			data, _ := os.ReadFile(sessionLog)
			return strings.Contains(string(data), `"record":"permission.asked"`)
		})
		if err := sup.RuntimePrompt(spawned.RuntimeID, "continue", ""); err != nil {
			t.Fatal(err)
		}
		var requestID string
		waitFor(t, "permission request replay", func() bool {
			data, _ := os.ReadFile(eventPath)
			for _, line := range strings.Split(string(data), "\n") {
				var event map[string]any
				if json.Unmarshal([]byte(line), &event) == nil && event["event"] == "permission.request" {
					requestID, _ = event["request_id"].(string)
				}
			}
			return requestID != ""
		})
		if err := sup.RuntimeAnswerPermission(spawned.RuntimeID, requestID, "allow", ""); err != nil {
			t.Fatal(err)
		}
		waitForStableChild(t, child, func(active, completed bool, phase, label string) bool {
			return !active && phase == "done" && calls.Load() >= 3
		})
		return
	}
	waitFor(t, "wake settlement", func() bool {
		data, _ := os.ReadFile(sessionLog)
		return calls.Load() >= 2 && strings.Count(string(data), `"record":"run.settled"`) >= 2
	})
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
settlement:
	for {
		select {
		case ev, ok := <-wakeEvents:
			if !ok {
				t.Fatal("wake event stream closed")
			}
			if ev.Event == "session.end" {
				break settlement
			}
		case <-timer.C:
			t.Fatal("wake settlement did not reach provider")
		}
	}
	after, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	statusAfter, _ := sup.RuntimeStatus(spawned.RuntimeID)
	resultAfter, _ := sup.RuntimeResult(spawned.RuntimeID)
	if resultBefore.(map[string]any)["final_output"] != "answer-1" || resultAfter.(map[string]any)["final_output"] != "answer-1" {
		t.Fatalf("parked result changed: %v -> %v", resultBefore, resultAfter)
	}
	if statusBefore.(map[string]any)["final_output"] != statusAfter.(map[string]any)["final_output"] {
		t.Fatalf("parked status changed: %v -> %v", statusBefore, statusAfter)
	}
	if string(before) != string(after) {
		t.Fatalf("sentinel changed: %s -> %s", before, after)
	}
	status := statusAfter.(map[string]any)
	if status["status"] != "idle" || status["exit_code"] != 0 {
		t.Fatalf("status after wake: %v", status)
	}
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
