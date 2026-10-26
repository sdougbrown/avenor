package stable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runtime/broker/brokertest"
)

func TestSessionRenameLevelsAndResolution(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newItestEventLog(sup.control.SubscribeEvents(ctx))

	// The owner may rename the supervisor session, and the event is published
	// after the write with the label before and after.
	result, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, "", "main")
	if err != nil {
		t.Fatalf("owner rename: %v", err)
	}
	if result.RunID != "supervisor" || result.Label != "main" || result.PreviousLabel != "supervisor" {
		t.Fatalf("rename result = %+v", result)
	}
	ev := log.waitFor(t, "session.renamed", func(ev events.Event) bool { return ev.Event == "session.renamed" })
	if ev.Fields["run_id"] != "supervisor" || ev.Fields["runtime_id"] != "supervisor" ||
		ev.Fields["label"] != "main" || ev.Fields["previous_label"] != "supervisor" || ev.Fields["actor"] != "owner" {
		t.Fatalf("session.renamed fields = %v", ev.Fields)
	}

	// A broker caller may rename itself, and the display name is the only field
	// the resolution ladder reads afterwards.
	if _, err := sup.broker.CreateRun("rt_a", "label-a"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	renamed, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerBroker, RunID: "rt_a"}, "", "label-b")
	if err != nil {
		t.Fatalf("broker self rename: %v", err)
	}
	if renamed.PreviousLabel != "label-a" || renamed.Label != "label-b" {
		t.Fatalf("self rename = %+v", renamed)
	}
	if got := sup.broker.MatchLabel("label-b"); len(got) != 1 || got[0] != "rt_a" {
		t.Fatalf("new label resolves to %v", got)
	}
	if got := sup.broker.MatchLabel("label-a"); len(got) != 0 {
		t.Fatalf("old label still resolves to %v", got)
	}

	// Neither level may reach another run.
	if _, err := sup.broker.CreateRun("rt_b", "label-other"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	_, err = sup.SessionRename(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, "rt_a", "hijack")
	assertToolCode(t, err, "not_permitted")
	if !strings.Contains(err.Error(), "the control owner may rename only the supervisor session") {
		t.Fatalf("owner denial = %q", err)
	}
	_, err = sup.SessionRename(messaging.Caller{Kind: messaging.CallerBroker, RunID: "rt_b"}, "rt_a", "hijack")
	assertToolCode(t, err, "not_permitted")
	if !strings.Contains(err.Error(), "a broker-authenticated run may rename only itself") {
		t.Fatalf("broker denial = %q", err)
	}

	// Resolution comes before permission and never resolves a public run id.
	_, err = sup.SessionRename(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, "ghost", "x")
	assertToolCode(t, err, "unknown_target")
	if err.Error() != `no run matches "ghost"` {
		t.Fatalf("unknown target text = %q", err)
	}
}

func TestSessionRenameAmbiguousLabel(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	for _, id := range []string{"rt_b", "rt_a"} {
		if _, err := sup.broker.CreateRun(id, "shared"); err != nil {
			t.Fatalf("create run: %v", err)
		}
	}
	_, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, "shared", "x")
	assertToolCode(t, err, "ambiguous_name")
	if err.Error() != `"shared" matches 2 runs: "rt_a", "rt_b"` {
		t.Fatalf("ambiguous text = %q", err)
	}
}

func TestSessionRenameNameRules(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	cases := map[string]string{
		"empty":      "",
		"too long":   strings.Repeat("a", 65),
		"newline":    "two\nlines",
		"bidi":       "a\u202Eb",
		"zero width": "a\u200Bb",
		"carriage":   "a\rb",
		"paragraph":  "a\u2029b",
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, "", value)
			assertToolCode(t, err, "invalid_name")
		})
	}
	// 64 runes, 128 bytes: the rule counts runes, as the schema does.
	if _, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, "", strings.Repeat("é", 64)); err != nil {
		t.Fatalf("64-rune name rejected: %v", err)
	}
}

func TestSessionRenameThenSendDelivers(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	token, err := sup.broker.CreateRun("rt_a", "old-name")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := sup.broker.CreateRun("rt_b", "peer-b"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := sup.broker.CreateRun("rt_c", "peer-c"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerBroker, RunID: "rt_a"}, "", "new-name"); err != nil {
		t.Fatalf("rename: %v", err)
	}

	tr, err := localhttp.New(sup.broker.Addr(), messaging.LocalCredentials{RunID: "rt_a", Token: token})
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	defer tr.Close()
	ctx := context.Background()
	pre, err := tr.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	body, err := messaging.SendBody("rt_a", "rt_b", "hello", "agent")
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	msgID, err := messaging.NewMsgID(time.Now(), time.Time{})
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	sent, err := tr.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "rt_a"},
		To:         messaging.Identity{RunID: "rt_b"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       string(body),
		NoReply:    true,
	})
	if err != nil {
		t.Fatalf("send after rename: %v (result %+v)", err, sent)
	}
	queued := brokertest.WaitForAgentMessage(t, sup.broker, "rt_b")
	if queued.Sender != "new-name" {
		t.Fatalf("sender = %q, want the new display name", queued.Sender)
	}

	// The control message_send path uses the supervisor's in-process transport,
	// so the same rename must reach the peer through it too.
	if _, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, "", "main"); err != nil {
		t.Fatalf("rename supervisor: %v", err)
	}
	inproc, err := sup.MessageTransport()
	if err != nil {
		t.Fatalf("in-process transport: %v", err)
	}
	pre, err = inproc.Preflight(ctx)
	if err != nil {
		t.Fatalf("in-process preflight: %v", err)
	}
	msgID, err = messaging.NewMsgID(time.Now(), time.Time{})
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	if _, err := inproc.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: "rt_c"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       string(body),
		NoReply:    true,
	}); err != nil {
		t.Fatalf("in-process send after rename: %v", err)
	}
	if queued := brokertest.WaitForAgentMessage(t, sup.broker, "rt_c"); queued.Sender != "main" {
		t.Fatalf("in-process sender = %q, want the new display name", queued.Sender)
	}
}

// TestSessionRenameUpdatesRuntimeLabel pins that a rename reaches every surface
// that reports or reserves a run's name: status, list, MCP label resolution, the
// messaging plane, and the label reservation a keyed spawn checks.
func TestSessionRenameUpdatesRuntimeLabel(t *testing.T) {
	const spawnLabel, renamedLabel = "spawn-name", "renamed-name"
	sup, _ := newStage2Supervisor(t, 8)
	dir := t.TempDir()
	child, err := renameTestSpawn(t, sup, dir, spawnLabel, "rename-child")
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if _, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerBroker, RunID: child.RuntimeID}, "", renamedLabel); err != nil {
		t.Fatalf("rename: %v", err)
	}

	status, err := sup.RuntimeStatus(child.RuntimeID)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	if got := status.(map[string]any)["label"]; got != renamedLabel {
		t.Fatalf("status label = %v, want %q", got, renamedLabel)
	}

	list := sup.List()
	entries, ok := list.([]map[string]any)
	if !ok {
		t.Fatalf("List = %T, want entries", list)
	}
	listed := ""
	for _, entry := range entries {
		if entry["runtime_id"] == child.RuntimeID {
			listed, _ = entry["label"].(string)
		}
	}
	if listed != renamedLabel {
		t.Fatalf("list label = %q, want %q", listed, renamedLabel)
	}
	// The control list's label field is what an MCP tool call by name resolves
	// against, and the messaging plane resolves the same name.
	if got := listLabelMatch(entries, renamedLabel); got != child.RuntimeID {
		t.Fatalf("label resolution for %q = %q, want %q", renamedLabel, got, child.RuntimeID)
	}
	if got := listLabelMatch(entries, spawnLabel); got != "" {
		t.Fatalf("label resolution for the old name %q = %q, want none", spawnLabel, got)
	}
	if got := sup.broker.MatchLabel(renamedLabel); len(got) != 1 || got[0] != child.RuntimeID {
		t.Fatalf("broker match for %q = %v", renamedLabel, got)
	}
	if got := sup.broker.MatchLabel(spawnLabel); len(got) != 0 {
		t.Fatalf("broker match for the old name %q = %v, want none", spawnLabel, got)
	}

	// The rename releases the old name and reserves the new one: a keyed spawn
	// may take the old name, and one asking for the new name is rejected.
	if _, err := renameTestSpawn(t, sup, dir, spawnLabel, "rename-reuse-old"); err != nil {
		t.Fatalf("spawn under the released name: %v", err)
	}
	_, err = renameTestSpawn(t, sup, dir, renamedLabel, "rename-reuse-new")
	if err == nil || !strings.Contains(err.Error(), "label already in use: "+renamedLabel) {
		t.Fatalf("spawn under the renamed runtime's name = %v, want label already in use", err)
	}
}

// renameTestSpawn starts one keyed runtime, the path that reserves a label.
func renameTestSpawn(t *testing.T, sup *Supervisor, dir, label, key string) (SpawnResult, error) {
	t.Helper()
	raw, err := json.Marshal(SpawnParams{Prompt: "go", Dir: dir, Agent: "claude", Label: label, IdempotencyKey: key})
	if err != nil {
		t.Fatalf("marshal spawn params: %v", err)
	}
	out, err := sup.Spawn(raw)
	if err != nil {
		return SpawnResult{}, err
	}
	res, ok := out.(SpawnResult)
	if !ok {
		t.Fatalf("Spawn result = %#v, want SpawnResult", out)
	}
	return res, nil
}

// listLabelMatch mirrors the label rung of the MCP run lookup: a control list
// entry's label field is what a tool call by name resolves against, and only a
// unique match resolves.
func listLabelMatch(entries []map[string]any, label string) string {
	matched := ""
	for _, entry := range entries {
		if entry["label"] != label {
			continue
		}
		if matched != "" {
			return ""
		}
		matched, _ = entry["runtime_id"].(string)
	}
	return matched
}

func TestSessionLastRecordsSupervisorOnly(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	if _, err := sup.broker.CreateRun("rt_a", "peer-a"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	record, ok, err := sup.SessionLast("rt_a")
	if err != nil {
		t.Fatalf("session last: %v", err)
	}
	if ok || record.RunID != "rt_a" || record.Label != "peer-a" || record.Text != "" {
		t.Fatalf("no-record answer = %+v ok=%v", record, ok)
	}
	_, _, err = sup.SessionLast("rt_missing")
	assertToolCode(t, err, "unknown_target")

	owner := messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}
	if err := sup.SessionLastSet(owner, "supervisor", "the answer", "done", true); err != nil {
		t.Fatalf("set last: %v", err)
	}
	record, ok, err = sup.SessionLast("supervisor")
	if err != nil || !ok || record.Text != "the answer" || record.StopReason != "done" || !record.Truncated {
		t.Fatalf("record = %+v ok=%v err=%v", record, ok, err)
	}
	assertToolCode(t, sup.SessionLastSet(messaging.Caller{Kind: messaging.CallerBroker, RunID: "rt_a"}, "supervisor", "x", "", false), "not_permitted")
	assertToolCode(t, sup.SessionLastSet(owner, "rt_a", "x", "", false), "not_permitted")
}

func assertToolCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("err = nil, want tool code %s", want)
	}
	var coded *router.ToolError
	if !errors.As(err, &coded) {
		t.Fatalf("err = %T %v, want *router.ToolError", err, err)
	}
	if coded.Code != want {
		t.Fatalf("tool code = %q, want %q (%v)", coded.Code, want, err)
	}
}

// TestSessionRenameIndexesRuntimeHistory pins the runtime_id the event carries:
// without it a control history or tail read for the renamed runtime cannot see
// the rename.
func TestSessionRenameIndexesRuntimeHistory(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	socket := newStableSocketPath(t, "s2-history")
	if err := sup.control.Start(socket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer sup.control.Stop()
	c, err := client.Dial(socket)
	if err != nil {
		t.Fatalf("dial control socket: %v", err)
	}
	defer c.Close()

	if _, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, "", "main"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	var history struct {
		Events []map[string]any `json:"events"`
	}
	if err := c.Call("history", map[string]any{"runtime_id": "supervisor"}, &history); err != nil {
		t.Fatalf("history: %v", err)
	}
	for _, ev := range history.Events {
		if ev["event"] != "session.renamed" {
			continue
		}
		if ev["label"] != "main" || ev["previous_label"] != "supervisor" {
			t.Fatalf("session.renamed history fields = %v", ev)
		}
		return
	}
	t.Fatalf("session.renamed is absent from the runtime history: %v", history.Events)
}

// TestHarnessNameObserverMirrorsIntoTheRuntimeLabel pins the supervisor-level
// wiring of the broker's harness-name observer: SetHarnessName writes the
// broker's label half, and the observer wired at construction copies the
// broker's resolved name into the runtime's display name, the copy status and
// label reservation read.
func TestHarnessNameObserverMirrorsIntoTheRuntimeLabel(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "harnessname"), MaxRuntimes: 2})
	if sup.broker == nil {
		t.Fatal("the test supervisor has no broker")
	}
	const runID = "rt_harness"
	if _, ok := sup.broker.EnsureRun(runID, "launch-label"); !ok {
		t.Fatal("ensure the run record: not created")
	}
	sup.controlMu.Lock()
	sup.runtimes[runID] = &childRuntime{id: runID, label: "launch-label", displayName: "launch-label", done: make(chan struct{})}
	sup.controlMu.Unlock()
	// The synthetic runtime leaves the map before the lifecycle cleanup walks
	// runtimes it does not model.
	t.Cleanup(func() {
		sup.controlMu.Lock()
		delete(sup.runtimes, runID)
		sup.controlMu.Unlock()
	})

	if err := sup.broker.SetHarnessName(runID, "harness-name"); err != nil {
		t.Fatalf("set harness name: %v", err)
	}
	if got := runtimeDisplayName(sup, runID); got != "harness-name" {
		t.Fatalf("runtime display name after harness name = %q, want harness-name", got)
	}

	// Clearing the harness name rewrites the mirror to the resolved fallback.
	if err := sup.broker.SetHarnessName(runID, ""); err != nil {
		t.Fatalf("clear harness name: %v", err)
	}
	if got := runtimeDisplayName(sup, runID); got != "launch-label" {
		t.Fatalf("runtime display name after clear = %q, want launch-label", got)
	}
}

// runtimeDisplayName reads one runtime's display name under its lock.
func runtimeDisplayName(sup *Supervisor, runID string) string {
	sup.controlMu.Lock()
	child := sup.runtimes[runID]
	sup.controlMu.Unlock()
	if child == nil {
		return ""
	}
	child.mu.Lock()
	defer child.mu.Unlock()
	return child.displayName
}

// TestSessionRenameUnderAHarnessName pins the primary rename path against the
// harness overlay: SessionRename writes the model-set label underneath while
// the resolved display name keeps winning with the harness name, and clearing
// the harness name falls back to the renamed model-set label.
func TestSessionRenameUnderAHarnessName(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "harnessrename"), MaxRuntimes: 2})
	if sup.broker == nil {
		t.Fatal("the test supervisor has no broker")
	}
	const runID = "rt_hrn"
	if _, ok := sup.broker.EnsureRun(runID, "launch-label"); !ok {
		t.Fatal("ensure the run record: not created")
	}
	sup.controlMu.Lock()
	sup.runtimes[runID] = &childRuntime{id: runID, label: "launch-label", displayName: "launch-label", done: make(chan struct{})}
	sup.controlMu.Unlock()
	t.Cleanup(func() {
		sup.controlMu.Lock()
		delete(sup.runtimes, runID)
		sup.controlMu.Unlock()
	})

	if err := sup.broker.SetHarnessName(runID, "harness-name"); err != nil {
		t.Fatalf("set harness name: %v", err)
	}
	renamed, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerBroker, RunID: runID}, "", "model-name")
	if err != nil {
		t.Fatalf("rename under a harness name: %v", err)
	}
	// The result carries the resolved display name (the harness overlay wins),
	// while the model-set label is written underneath — pinned by the fallback
	// below, which surfaces it when the harness name clears. The previous label
	// is the resolved name before the write (the harness overlay), not the raw
	// model label underneath it.
	if renamed.Label != "harness-name" {
		t.Fatalf("rename result label = %q, want the resolved harness name", renamed.Label)
	}
	if renamed.PreviousLabel != "harness-name" {
		t.Fatalf("rename result previous label = %q, want the resolved harness name", renamed.PreviousLabel)
	}
	if got := runtimeDisplayName(sup, runID); got != "harness-name" {
		t.Fatalf("runtime display name after rename = %q, want the harness name to keep winning", got)
	}

	if err := sup.broker.SetHarnessName(runID, ""); err != nil {
		t.Fatalf("clear harness name: %v", err)
	}
	if got := runtimeDisplayName(sup, runID); got != "model-name" {
		t.Fatalf("runtime display name after clear = %q, want the renamed model-set label", got)
	}
}
