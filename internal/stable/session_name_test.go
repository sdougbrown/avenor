package stable

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runtime/broker/brokertest"
)

func lastEventNamed(t *testing.T, log *itestEventLog, name string) (events.Event, bool) {
	t.Helper()
	return log.find(func(ev events.Event) bool { return ev.Event == name })
}

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
}

func TestSessionLastRecordsSupervisorOnly(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	if _, err := sup.broker.CreateRun("rt_a", "peer-a"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newItestEventLog(sup.control.SubscribeEvents(ctx))

	record, ok, err := sup.SessionLast("rt_a")
	if err != nil {
		t.Fatalf("session last: %v", err)
	}
	if ok || record.RunID != "rt_a" || record.Label != "peer-a" || record.Text != "" {
		t.Fatalf("no-record answer = %+v ok=%v", record, ok)
	}
	if _, _, err := sup.SessionLast("rt_missing"); err == nil || !strings.Contains(err.Error(), `runtime "rt_missing" not found`) {
		t.Fatalf("unknown runtime error = %v", err)
	}

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

	time.Sleep(50 * time.Millisecond)
	if _, found := lastEventNamed(t, log, "session.last_message"); found {
		t.Fatal("session_last_set published an event")
	}
}

func assertToolCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("err = nil, want tool code %s", want)
	}
	coded, ok := err.(*router.ToolError)
	if !ok {
		t.Fatalf("err = %T %v, want *router.ToolError", err, err)
	}
	if coded.Code != want {
		t.Fatalf("tool code = %q, want %q (%v)", coded.Code, want, err)
	}
}
