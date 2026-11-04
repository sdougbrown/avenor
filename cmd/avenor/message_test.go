package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

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
	var out, stderr bytes.Buffer
	if code := runDeclared([]string{"message", "tell", "--text=hello"}, &out, &stderr); code != 0 {
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
		code := runDeclared([]string{"message", "ask", "--text=question", "--timeout-ms=5000"}, &o, &e)
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
	other, _, err := b.EnsureRun("other", "other")
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "reply", "--reply-to=" + id, "--to=other", "--text=wrong recipient"}, &out, &stderr); code != 0 || !strings.Contains(out.String(), `"outcome": "sent"`) {
		t.Fatalf("wrong recipient: exit %d, stdout %s, stderr %s", code, &out, &stderr)
	}
	otherTr, err := localhttp.New(b.Addr(), messaging.LocalCredentials{RunID: "other", Token: other})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = otherTr.Close() })
	otherMsgs, err := otherTr.Receive(t.Context(), messaging.ReceiveRequest{Sender: messaging.Identity{RunID: "other"}, Generation: pre.Generation})
	if err != nil || len(otherMsgs.Messages) != 1 || !strings.Contains(string(otherMsgs.Messages[0].Payload), "wrong recipient") {
		t.Fatalf("fallback delivery: %+v %v", otherMsgs, err)
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
	select {
	case pending := <-waiting:
		t.Fatalf("wrong recipient completed ask: %+v", pending)
	case <-time.After(50 * time.Millisecond):
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "reply", "--reply-to=" + id, "--to=supervisor", "--text=child answer"}, &out, &stderr); code != 0 || !strings.Contains(out.String(), `"outcome": "replied"`) {
		t.Fatalf("reply: %d %s %s", code, &out, &stderr)
	}
	waited := <-waiting
	if waited.err != nil || waited.result.Outcome != messaging.OutcomeReplied || !strings.Contains(string(waited.result.Reply.Payload), "child answer") {
		t.Fatalf("wait: %+v", waited)
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
	if code := runDeclared([]string{"message", "reply", "--reply-to=" + tellID, "--text=notice answer"}, &out, &stderr); code != 0 || !strings.Contains(out.String(), `"outcome": "sent"`) {
		t.Fatalf("tell reply: exit %d, stdout %s, stderr %s", code, &out, &stderr)
	}
	msg = receive()
	if msg.FromRunID != "child" || !strings.Contains(string(msg.Payload), "notice answer") {
		t.Fatalf("tell reply delivery: %+v", msg)
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "ask", "--text=timeout", "--timeout-ms=10"}, &out, &stderr); code != 0 {
		t.Fatalf("timeout: %d %s", code, &stderr)
	}
	var v map[string]any
	if err := json.Unmarshal(out.Bytes(), &v); err != nil || v["outcome"] != "timed-out" {
		t.Fatalf("timeout output: %s %v", &out, err)
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "tell", "--target=missing", "--text=x"}, &out, &stderr); code != 1 {
		t.Fatalf("unknown target: %d %s %s", code, &out, &stderr)
	}
	out.Reset()
	stderr.Reset()
	if code := runDeclared([]string{"message", "ask", "--text=x", "--timeout-ms=-1"}, &out, &stderr); code != 2 {
		t.Fatalf("negative timeout: %d %s", code, &stderr)
	}
}

// TestMessageUsage checks that missing credentials and invalid inputs are usage errors.
func TestMessageUsage(t *testing.T) {
	for _, k := range []string{"AVENOR_BROKER_URL", "AVENOR_BROKER_TOKEN", "AVENOR_RUN_ID"} {
		t.Setenv(k, "")
	}
	for _, args := range [][]string{{"message", "tell", "--text=x"}, {"message", "ask", "--text=x"}, {"message", "reply", "--text=x", "--reply-to=id"}, {"message", "ask", "--text=x", "--timeout-ms=-1"}} {
		var out, stderr bytes.Buffer
		if code := runDeclared(args, &out, &stderr); code != 2 {
			t.Errorf("%v exit %d: %s", args, code, &stderr)
		}
	}
}
