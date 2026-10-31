package stable

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker/brokertest"
)

// ownerControlClient starts the supervisor's control server and attaches the
// owner capability, so the message verbs that require the owner can be driven
// the way the Pi host drives them.
func ownerControlClient(t *testing.T, sup *Supervisor, name string) *client.Client {
	t.Helper()
	socket := newStableSocketPath(t, name)
	if err := sup.control.Start(socket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(func() { sup.control.Stop() })
	c, err := client.AttachOwner(socket)
	if err != nil {
		t.Fatalf("attach owner: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// TestChildAskReachesSupervisorThroughMessageVerbs pins the child → supervisor
// leg over the surviving control verbs: an unversioned ask from a child's
// broker run is queued for the supervisor, drained by message_receive, and
// answered by message_reply to the child's waiting message id.
func TestChildAskReachesSupervisorThroughMessageVerbs(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 4)
	c := ownerControlClient(t, sup, "s4-child-ask")

	token, err := sup.broker.CreateRun("rt_child", "worker")
	if err != nil {
		t.Fatalf("create child run: %v", err)
	}
	child, err := localhttp.New(sup.broker.Addr(), messaging.LocalCredentials{RunID: "rt_child", Token: token})
	if err != nil {
		t.Fatalf("child transport: %v", err)
	}
	defer child.Close()

	ctx := context.Background()
	pre, err := child.Preflight(ctx)
	if err != nil {
		t.Fatalf("child preflight: %v", err)
	}
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	body, err := messaging.AskBody(msgID, "rt_child", "supervisor", "status?", "agent")
	if err != nil {
		t.Fatalf("ask body: %v", err)
	}
	sent, err := child.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "rt_child"},
		To:         messaging.Identity{RunID: "supervisor"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       string(body),
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("child ask = (%+v, %v), want sent", sent, err)
	}

	controlPre, err := c.MessagePreflight()
	if err != nil {
		t.Fatalf("control preflight: %v", err)
	}
	received, err := c.MessageReceive(messaging.ReceiveRequest{Generation: controlPre.Generation})
	if err != nil {
		t.Fatalf("message_receive: %v", err)
	}
	if len(received.Messages) != 1 {
		t.Fatalf("message_receive returned %d messages, want the child's ask", len(received.Messages))
	}
	ask := received.Messages[0]
	if ask.MessageID != msgID || ask.FromRunID != "rt_child" {
		t.Fatalf("received ask = %+v, want rt_child's %s", ask, msgID)
	}
	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(ask.Payload, &payload); err != nil || payload.Message != "status?" {
		t.Fatalf("ask payload = %s (%v), want status?", ask.Payload, err)
	}

	replied, err := c.MessageReply(messaging.ReplyRequest{
		From:          messaging.Identity{RunID: "supervisor"},
		ToRunID:       "rt_child",
		MessageID:     msgID,
		CorrelationID: msgID,
		Body:          `{"message":"all good","role":"host"}`,
	})
	if err != nil || replied.Outcome != messaging.OutcomeReplied {
		t.Fatalf("message_reply = (%+v, %v), want replied", replied, err)
	}

	waited, err := child.Wait(ctx, messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: "rt_child"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Timeout:    2 * time.Second,
	})
	if err != nil || waited.Outcome != messaging.OutcomeReplied || waited.Reply == nil {
		t.Fatalf("child wait = (%+v, %v), want the supervisor's reply", waited, err)
	}
	var reply struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(waited.Reply.Payload, &reply); err != nil || reply.Message != "all good" {
		t.Fatalf("reply payload = %s (%v), want all good", waited.Reply.Payload, err)
	}
}

// TestSupervisorAskReachesChildThroughMessageVerbs pins the host → child leg
// over the surviving control verbs: message_send + message_wait ask a child's
// broker run, the child drains the ask, and a broker-side reply resolves the
// host's wait.
func TestSupervisorAskReachesChildThroughMessageVerbs(t *testing.T) {
	t.Parallel()
	sup, _ := newStage2Supervisor(t, 4)
	c := ownerControlClient(t, sup, "s4-host-ask")

	token, err := sup.broker.CreateRun("rt_child", "worker")
	if err != nil {
		t.Fatalf("create child run: %v", err)
	}
	child, err := localhttp.New(sup.broker.Addr(), messaging.LocalCredentials{RunID: "rt_child", Token: token})
	if err != nil {
		t.Fatalf("child transport: %v", err)
	}
	defer child.Close()

	pre, err := c.MessagePreflight()
	if err != nil {
		t.Fatalf("control preflight: %v", err)
	}
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	body, err := messaging.AskBody(msgID, "supervisor", "rt_child", "status?", "host")
	if err != nil {
		t.Fatalf("ask body: %v", err)
	}
	sent, err := c.MessageSend(messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: "rt_child"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       string(body),
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("host ask = (%+v, %v), want sent", sent, err)
	}

	queued := brokertest.WaitForAgentMessage(t, sup.broker, "rt_child")
	if queued.Payload == nil || queued.ID != msgID {
		t.Fatalf("queued ask = %+v, want the host's %s", queued, msgID)
	}

	ctx := context.Background()
	replied, err := child.Reply(ctx, messaging.ReplyRequest{
		From:          messaging.Identity{RunID: "rt_child"},
		ToRunID:       "supervisor",
		MessageID:     msgID,
		CorrelationID: msgID,
		Body:          `{"message":"all good","role":"agent"}`,
	})
	if err != nil || replied.Outcome != messaging.OutcomeReplied {
		t.Fatalf("child reply = (%+v, %v), want replied", replied, err)
	}

	waited, err := c.MessageWait(messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: "supervisor"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Timeout:    2 * time.Second,
	})
	if err != nil || waited.Outcome != messaging.OutcomeReplied || waited.Reply == nil {
		t.Fatalf("message_wait = (%+v, %v), want the child's reply", waited, err)
	}
	var reply struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(waited.Reply.Payload, &reply); err != nil || reply.Message != "all good" {
		t.Fatalf("reply payload = %s (%v), want all good", waited.Reply.Payload, err)
	}
}

// TestAskToCompletedRunChildIsQueuedAndTimesOut pins what a runChild teardown
// leaves behind: a completed direct child's broker run stays registered, so an
// ask to its rt_N is admitted and queued, and the asker waits out its own
// bounded timeout instead of being told the target is unavailable.
func TestAskToCompletedRunChildIsQueuedAndTimesOut(t *testing.T) {
	t.Parallel()
	provider := &stableScriptedProvider{
		attempt: 0,
		scripts: []stableScriptedAttempt{{
			sessionID: "ses_ended_ask",
			events: []stableScriptedEvent{{event: events.Event{
				Event:     "session.end",
				SessionID: "ses_ended_ask",
				Fields:    map[string]any{"stop_reason": "end_turn"},
			}}},
		}},
	}
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "ended-ask"), MaxRuntimes: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	child := &childRuntime{
		id:          "rt_ended_ask",
		provider:    provider,
		session:     runtime.Session{SessionID: "ses_ended_ask"},
		eventWriter: stableTestSink{},
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		cancelFn:    cancel,
	}
	registerSyntheticChild(t, sup, child, testLifecycleOptions{exec: execStarting, class: directAttemptClass})
	go sup.runChild(ctx, child, "ask me", 0, 0)

	// The child is run by runChild itself, so the broker run under test is the
	// one the spawn path installs, and the parked done phase is what its teardown
	// leaves: no active runtime and no broker run removed with it.
	waitForStableChild(t, child, func(active bool, completed bool, phase, phaseLabel string) bool {
		return !active && !completed && phase == "done"
	})
	if sup.broker.GetRun(child.id) == nil {
		t.Fatal("the completed child's broker run went away with its runtime")
	}

	askerToken, err := sup.broker.CreateRun("asker", "")
	if err != nil {
		t.Fatalf("create asker run: %v", err)
	}
	asker, err := localhttp.New(sup.broker.Addr(), messaging.LocalCredentials{RunID: "asker", Token: askerToken})
	if err != nil {
		t.Fatalf("asker transport: %v", err)
	}
	defer asker.Close()
	pre, err := asker.Preflight(ctx)
	if err != nil {
		t.Fatalf("asker preflight: %v", err)
	}
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	body, err := messaging.AskBody(msgID, "asker", child.id, "status?", "agent")
	if err != nil {
		t.Fatalf("ask body: %v", err)
	}
	sent, err := asker.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "asker"},
		To:         messaging.Identity{RunID: child.id},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       string(body),
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("ask to a completed child = (%+v, %v), want sent", sent, err)
	}
	queued := brokertest.WaitForAgentMessage(t, sup.broker, child.id)
	if queued.Payload == nil || queued.ID != msgID {
		t.Fatalf("queued ask = %+v, want the asker's %s", queued, msgID)
	}

	waited, err := asker.Wait(ctx, messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: "asker"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Timeout:    300 * time.Millisecond,
	})
	if err != nil || waited.Outcome != messaging.OutcomeTimedOut || waited.Reply != nil {
		t.Fatalf("wait on a completed child = (%+v, %v), want a bounded timeout with no reply", waited, err)
	}

	cancel()
	waitForStableDone(t, child)
}
