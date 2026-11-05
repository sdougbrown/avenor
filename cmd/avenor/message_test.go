package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// TestMessageRoundTrips exercises the commands with real broker credentials.
func TestMessageRoundTrips(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	child, _, err := b.EnsureRun("child", "child")
	if err != nil {
		t.Fatal(err)
	}
	parent, _, err := b.EnsureRun("supervisor", "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AVENOR_BROKER_URL", "http://"+b.Addr())
	t.Setenv("AVENOR_RUN_ID", "child")
	t.Setenv("AVENOR_BROKER_TOKEN", child)
	tr, err := localhttp.New(b.Addr(), messaging.LocalCredentials{RunID: "supervisor", Token: parent})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	pre, err := tr.Preflight(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	receive := func() messaging.InboundMessage {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			res, err := tr.Receive(t.Context(), messaging.ReceiveRequest{Sender: messaging.Identity{RunID: "supervisor"}, Generation: pre.Generation})
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Messages) > 0 {
				return res.Messages[0]
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("message not received")
		return messaging.InboundMessage{}
	}
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	dir, err := askclaim.DefaultDir(filepath.Join(stateHome, "avenor"), "http://"+b.Addr(), "child")
	if err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if code := runDeclared([]string{"message", "--action=tell", "--target=supervisor", "--message=hello"}, &out, &stderr); code != 0 {
		t.Fatalf("tell: %d %s", code, &stderr)
	}
	msg := receive()
	if msg.FromRunID != "child" || !strings.Contains(string(msg.Payload), "hello") {
		t.Fatalf("tell message: %+v", msg)
	}
	type result struct {
		code     int
		out, err string
	}
	done := make(chan result, 1)
	go func() {
		var o, e bytes.Buffer
		code := runDeclared([]string{"message", "--action=ask", "--target=supervisor", "--message=question", "--timeout-ms=5000"}, &o, &e)
		done <- result{code, o.String(), e.String()}
	}()
	msg = receive()
	if msg.FromRunID != "child" || !strings.Contains(string(msg.Payload), "question") {
		t.Fatalf("ask message: %+v", msg)
	}
	reply, err := tr.Reply(context.Background(), messaging.ReplyRequest{From: messaging.Identity{RunID: "supervisor"}, ToRunID: "child", MessageID: msg.MessageID, Body: `{"message":"answer","role":"agent"}`})
	if err != nil || reply.Outcome != messaging.OutcomeReplied {
		t.Fatalf("reply: %+v %v", reply, err)
	}
	got := <-done
	if got.code != 0 || !strings.Contains(got.out, "answer") {
		t.Fatalf("ask: %+v", got)
	}
	// A supervisor ask is answered by the child CLI using the broker's route.
	id, err := messaging.NewMsgID(time.Now(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	sent, err := tr.Send(t.Context(), messaging.SendRequest{From: messaging.Identity{RunID: "supervisor"}, To: messaging.Identity{RunID: "child"}, MessageID: id, Generation: pre.Generation, Body: `{"message":"question","role":"agent"}`})
	if err != nil {
		t.Fatalf("send: %+v %v", sent, err)
	}
	if err := askclaim.Record(dir, id, "supervisor"); err != nil {
		t.Fatal(err)
	}
	type waitResult struct {
		result messaging.WaitResult
		err    error
	}
	waiting := make(chan waitResult, 1)
	go func() {
		result, err := tr.Wait(t.Context(), messaging.WaitRequest{Sender: messaging.Identity{RunID: "supervisor"}, MessageID: id, Generation: pre.Generation, Timeout: 5 * time.Second})
		waiting <- waitResult{result, err}
	}()
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "--action=reply", "--reply-to=" + id, "--message=child answer"}, &out, &stderr); code != 0 || !strings.Contains(out.String(), `"outcome": "replied"`) {
		t.Fatalf("reply: %d %s %s", code, &out, &stderr)
	}
	waited := <-waiting
	if waited.err != nil || waited.result.Outcome != messaging.OutcomeReplied || !strings.Contains(string(waited.result.Reply.Payload), "child answer") {
		t.Fatalf("wait: %+v", waited)
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "--action=reply", "--reply-to=" + id, "--asks-dir=" + dir, "--message=again"}, &out, &stderr); code != 0 || !strings.Contains(out.String(), `"outcome": "edge-absent"`) {
		t.Fatalf("second reply: %d %s %s", code, &out, &stderr)
	}
	tellID, err := messaging.NewMsgID(time.Now(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Send(t.Context(), messaging.SendRequest{From: messaging.Identity{RunID: "supervisor"}, To: messaging.Identity{RunID: "child"}, MessageID: tellID, Generation: pre.Generation, Body: `{"message":"notice","role":"agent"}`, NoReply: true}); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "--action=reply", "--reply-to=" + tellID, "--asks-dir=" + dir, "--message=notice answer"}, &out, &stderr); code != 0 || !strings.Contains(out.String(), `"outcome": "sent"`) {
		t.Fatalf("tell reply: exit %d, stdout %s, stderr %s", code, &out, &stderr)
	}
	msg = receive()
	if msg.FromRunID != "child" || !strings.Contains(string(msg.Payload), "notice answer") {
		t.Fatalf("tell reply delivery: %+v", msg)
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "--action=ask", "--target=supervisor", "--message=timeout", "--timeout-ms=10"}, &out, &stderr); code != 0 {
		t.Fatalf("timeout: %d %s", code, &stderr)
	}
	var v map[string]any
	if err := json.Unmarshal(out.Bytes(), &v); err != nil || v["outcome"] != "timed-out" {
		t.Fatalf("timeout output: %s %v", &out, err)
	}
	timeoutID, ok := v["message_id"].(string)
	if !ok || timeoutID == "" {
		t.Fatalf("timeout missing message id: %s", &out)
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "--action=withdraw", "--message-id=" + timeoutID}, &out, &stderr); code != 0 || !strings.Contains(out.String(), `"outcome": "withdrawn"`) {
		t.Fatalf("withdraw: %d %s %s", code, &out, &stderr)
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "--action=tell", "--target=missing", "--message=x"}, &out, &stderr); code != 1 || !strings.Contains(out.String(), `"code": "unknown_target"`) {
		t.Fatalf("unknown target: %d %s %s", code, &out, &stderr)
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "--action=ask", "--target=missing", "--message=x", "--timeout-ms=600001"}, &out, &stderr); code != 1 || !strings.Contains(out.String(), `"code": "invalid_request"`) {
		t.Fatalf("over-limit timeout: %d %s %s", code, &out, &stderr)
	}
	// The credential flags override the environment.
	t.Setenv("AVENOR_BROKER_URL", "invalid-url")
	t.Setenv("AVENOR_RUN_ID", "nobody")
	t.Setenv("AVENOR_BROKER_TOKEN", "wrong")
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "--action=tell", "--target=supervisor", "--message=by flags", "--broker-url=http://" + b.Addr(), "--run-id=child", "--token=" + child}, &out, &stderr); code != 0 {
		t.Fatalf("tell by flags: %d %s %s", code, &out, &stderr)
	}
	msg = receive()
	if msg.FromRunID != "child" || !strings.Contains(string(msg.Payload), "by flags") {
		t.Fatalf("tell by flags delivery: %+v", msg)
	}
}

// TestMessageRejectsInvalidBrokerURL checks that a bad --broker-url fails the command.
func TestMessageRejectsInvalidBrokerURL(t *testing.T) {
	var out, stderr bytes.Buffer
	if code := runDeclared([]string{"message", "--action=tell", "--target=x", "--message=x", "--broker-url=invalid-url", "--run-id=r", "--token=t"}, &out, &stderr); code != 1 || !strings.Contains(stderr.String(), "avenor message:") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
	}
}

func TestMessageUsage(t *testing.T) {
	for _, missing := range []string{"", "AVENOR_BROKER_URL", "AVENOR_BROKER_TOKEN", "AVENOR_RUN_ID"} {
		t.Run(missing, func(t *testing.T) {
			for _, k := range []string{"AVENOR_BROKER_URL", "AVENOR_BROKER_TOKEN", "AVENOR_RUN_ID"} {
				t.Setenv(k, "valid")
			}
			args := []string{"message", "--action=tell", "--target=x", "--message=x"}
			if missing == "" {
				args = []string{"message"}
			} else {
				t.Setenv(missing, "")
			}
			var out, stderr bytes.Buffer
			if code := runDeclared(args, &out, &stderr); code != 2 {
				t.Errorf("%v exit %d: %s", args, code, &stderr)
			}
		})
	}
}
