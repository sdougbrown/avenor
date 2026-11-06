package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// receiveInbound polls the supervisor transport until a message arrives or the
// deadline lapses.
func receiveInbound(t *testing.T, tr *localhttp.Transport, generation string) messaging.InboundMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		res, err := tr.Receive(t.Context(), messaging.ReceiveRequest{Sender: messaging.Identity{RunID: "supervisor"}, Generation: generation})
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
	msg := receiveInbound(t, tr, pre.Generation)
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
	msg = receiveInbound(t, tr, pre.Generation)
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
	msg = receiveInbound(t, tr, pre.Generation)
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
	msg = receiveInbound(t, tr, pre.Generation)
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
	for _, missing := range []string{"", "--action=", "AVENOR_BROKER_URL", "AVENOR_BROKER_TOKEN", "AVENOR_RUN_ID"} {
		t.Run(missing, func(t *testing.T) {
			for _, k := range []string{"AVENOR_BROKER_URL", "AVENOR_BROKER_TOKEN", "AVENOR_RUN_ID"} {
				t.Setenv(k, "valid")
			}
			args := []string{"message", "--action=tell", "--target=x", "--message=x"}
			switch missing {
			case "":
				args = []string{"message"}
			case "--action=":
				args = []string{"message", "--action=", "--target=x", "--message=x"}
			default:
				t.Setenv(missing, "")
			}
			var out, stderr bytes.Buffer
			if code := runDeclared(args, &out, &stderr); code != 2 {
				t.Errorf("%v exit %d: %s", args, code, &stderr)
			}
		})
	}
}

// errReader is a stdin double that fails the read so a read error can be
// exercised without a real pipe.
type errReader struct{ err error }

func (e errReader) Read(p []byte) (int, error) { return 0, e.err }

// TestReadMessageStdin pins the read helper's refusal paths that a real pipe
// cannot produce: a read error surfaces without sending.
func TestReadMessageStdin(t *testing.T) {
	if _, err := readMessageStdin(errReader{errors.New("read failed")}); err == nil || !strings.Contains(err.Error(), "read failed") {
		t.Fatalf("read error = %v, want it surfaced", err)
	}
}

// TestMessageStdin exercises --message-stdin through the built binary against a
// real broker, feeding each case's text over the process's own stdin.
func TestMessageStdin(t *testing.T) {
	bin := buildVerifyBinary(t)
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
	tr, err := localhttp.New(b.Addr(), messaging.LocalCredentials{RunID: "supervisor", Token: parent})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	pre, err := tr.Preflight(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	// noDelivery long-polls the supervisor once; the broker's poll waits for a
	// message before returning empty, so an empty result proves nothing landed.
	noDelivery := func() bool {
		t.Helper()
		res, err := tr.Receive(t.Context(), messaging.ReceiveRequest{Sender: messaging.Identity{RunID: "supervisor"}, Generation: pre.Generation})
		if err != nil {
			t.Fatal(err)
		}
		return len(res.Messages) == 0
	}
	// runStdin runs the built binary with stdin fed from data and returns the
	// exit code and captured output.
	runStdin := func(data string, args ...string) (int, string, string) {
		t.Helper()
		cmd := exec.Command(bin, append([]string{"message"}, args...)...)
		cmd.Env = append(os.Environ(),
			"AVENOR_BROKER_URL=http://"+b.Addr(),
			"AVENOR_RUN_ID=child",
			"AVENOR_BROKER_TOKEN="+child,
		)
		cmd.Stdin = strings.NewReader(data)
		var out, errb bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &errb
		code := 0
		if err := cmd.Run(); err != nil {
			var ee *exec.ExitError
			if !errors.As(err, &ee) {
				t.Fatalf("run: %v\nstdout=%s\nstderr=%s", err, &out, &errb)
			}
			code = ee.ExitCode()
		}
		return code, out.String(), errb.String()
	}
	// payloadText decodes the wire payload's message field.
	payloadText := func(p json.RawMessage) string {
		t.Helper()
		var body struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(p, &body); err != nil {
			t.Fatalf("payload: %v", err)
		}
		return body.Message
	}

	// A multiline tell delivers the exact bytes, including the trailing newline.
	code, out, stderr := runStdin("line one\nline two\n", "--action=tell", "--target=supervisor", "--message-stdin")
	if code != 0 {
		t.Fatalf("stdin tell: %d %s", code, stderr)
	}
	msg := receiveInbound(t, tr, pre.Generation)
	if msg.FromRunID != "child" || payloadText(msg.Payload) != "line one\nline two\n" {
		t.Fatalf("stdin tell: %+v", msg)
	}

	// A multiline reply delivers the exact bytes.
	id, err := messaging.NewMsgID(time.Now(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Send(t.Context(), messaging.SendRequest{From: messaging.Identity{RunID: "supervisor"}, To: messaging.Identity{RunID: "child"}, MessageID: id, Generation: pre.Generation, Body: `{"message":"question","role":"agent"}`}); err != nil {
		t.Fatal(err)
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
	code, out, stderr = runStdin("reply one\nreply two\n", "--action=reply", "--reply-to="+id, "--asks-dir="+dir, "--message-stdin")
	if code != 0 || !strings.Contains(out, `"outcome": "replied"`) {
		t.Fatalf("stdin reply: %d %s %s", code, out, stderr)
	}
	waited := <-waiting
	if waited.err != nil || waited.result.Outcome != messaging.OutcomeReplied || payloadText(waited.result.Reply.Payload) != "reply one\nreply two\n" {
		t.Fatalf("stdin reply wait: %+v", waited)
	}

	// An explicit empty stdin follows the existing empty-message rule: it
	// delivers an empty message.
	code, _, stderr = runStdin("", "--action=tell", "--target=supervisor", "--message-stdin")
	if code != 0 {
		t.Fatalf("empty stdin: %d %s", code, stderr)
	}
	msg = receiveInbound(t, tr, pre.Generation)
	if msg.FromRunID != "child" || payloadText(msg.Payload) != "" {
		t.Fatalf("empty stdin: %+v", msg)
	}

	// --message and an enabled --message-stdin are mutually exclusive, even for
	// an explicit empty --message; the refusal delivers nothing and never reads
	// stdin.
	code, _, stderr = runStdin("unread", "--action=tell", "--target=supervisor", "--message=x", "--message-stdin")
	if code != 2 || !strings.Contains(stderr, "mutually exclusive") {
		t.Fatalf("conflict: %d %s", code, stderr)
	}
	if !noDelivery() {
		t.Fatal("conflict delivered a message")
	}
	code, _, stderr = runStdin("unread", "--action=tell", "--target=supervisor", "--message=", "--message-stdin")
	if code != 2 {
		t.Fatalf("empty conflict: %d %s", code, stderr)
	}
	if !noDelivery() {
		t.Fatal("empty conflict delivered a message")
	}

	// --message-stdin=false preserves the inline behavior without reading stdin.
	code, _, stderr = runStdin("unread", "--action=tell", "--target=supervisor", "--message=inline", "--message-stdin=false")
	if code != 0 {
		t.Fatalf("stdin=false: %d %s", code, stderr)
	}
	msg = receiveInbound(t, tr, pre.Generation)
	if msg.FromRunID != "child" || payloadText(msg.Payload) != "inline" {
		t.Fatalf("stdin=false: %+v", msg)
	}

	// Oversized input refuses without truncation and without sending.
	code, _, stderr = runStdin(strings.Repeat("a", 1<<20+100), "--action=tell", "--target=supervisor", "--message-stdin")
	if code != 1 || !strings.Contains(stderr, "exceeds 1 MiB") {
		t.Fatalf("oversized: %d %s", code, stderr)
	}
	if !noDelivery() {
		t.Fatal("oversized delivered a message")
	}

	// Invalid UTF-8 input is refused without sending, so the message is never
	// altered by a replacement-character conversion and reported as sent.
	code, _, stderr = runStdin("\xff\xfe", "--action=tell", "--target=supervisor", "--message-stdin")
	if code != 1 || !strings.Contains(stderr, "not valid UTF-8") {
		t.Fatalf("invalid utf-8: %d %s", code, stderr)
	}
	if !noDelivery() {
		t.Fatal("invalid utf-8 delivered a message")
	}

	// Invalid UTF-8 in --message is refused before transport, just like
	// stdin: nothing is sent and the raw bytes never reach stderr.
	code, _, stderr = runStdin("", "--action=tell", "--target=supervisor", "--message=\xff\xfe")
	if code != 1 || !strings.Contains(stderr, "not valid UTF-8") {
		t.Fatalf("invalid utf-8 inline: %d %s", code, stderr)
	}
	if strings.Contains(stderr, "\xff\xfe") {
		t.Fatalf("invalid utf-8 inline leaked raw bytes: %s", stderr)
	}
	if !noDelivery() {
		t.Fatal("invalid utf-8 inline delivered a message")
	}

	// The refusal holds with --message-stdin=false too.
	code, _, stderr = runStdin("", "--action=tell", "--target=supervisor", "--message=\xff\xfe", "--message-stdin=false")
	if code != 1 || !strings.Contains(stderr, "not valid UTF-8") {
		t.Fatalf("invalid utf-8 inline, stdin=false: %d %s", code, stderr)
	}
	if !noDelivery() {
		t.Fatal("invalid utf-8 inline, stdin=false delivered a message")
	}

	// An explicit empty --message with stdin disabled delivers an empty
	// message, as before.
	code, _, stderr = runStdin("unread", "--action=tell", "--target=supervisor", "--message=", "--message-stdin=false")
	if code != 0 {
		t.Fatalf("empty inline: %d %s", code, stderr)
	}
	msg = receiveInbound(t, tr, pre.Generation)
	if msg.FromRunID != "child" || payloadText(msg.Payload) != "" {
		t.Fatalf("empty inline: %+v", msg)
	}
}

// TestMessageConcurrentDuplicateReplies pins the claim store's promise at the
// CLI boundary: two replies racing on the same delivered ask — one claim in
// flight while the other arrives — land exactly one reply. The loser reports
// edge-absent and exits 0, because the ask it names has no open edge left.
func TestMessageConcurrentDuplicateReplies(t *testing.T) {
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
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	tr, err := localhttp.New(b.Addr(), messaging.LocalCredentials{RunID: "supervisor", Token: parent})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	pre, err := tr.Preflight(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := messaging.NewMsgID(time.Now(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Send(t.Context(), messaging.SendRequest{From: messaging.Identity{RunID: "supervisor"}, To: messaging.Identity{RunID: "child"}, MessageID: id, Generation: pre.Generation, Body: `{"message":"question","expects_reply":true}`}); err != nil {
		t.Fatal(err)
	}
	stateHome := os.Getenv("XDG_STATE_HOME")
	dir, err := askclaim.DefaultDir(filepath.Join(stateHome, "avenor"), "http://"+b.Addr(), "child")
	if err != nil {
		t.Fatal(err)
	}
	if err := askclaim.Record(dir, id, "supervisor"); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		code    int
		outcome string
	}
	done := make(chan outcome, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			var out, stderr bytes.Buffer
			code := runDeclared([]string{"message", "--action=reply", "--reply-to=" + id, "--asks-dir=" + dir, "--message=answer"}, &out, &stderr)
			var parsed struct {
				Outcome string `json:"outcome"`
			}
			_ = json.Unmarshal(out.Bytes(), &parsed)
			done <- outcome{code: code, outcome: parsed.Outcome}
		}()
	}
	close(start)

	replied, edgeAbsent := 0, 0
	for i := 0; i < 2; i++ {
		got := <-done
		if got.code != 0 {
			t.Fatalf("reply exited %d", got.code)
		}
		switch got.outcome {
		case "replied":
			replied++
		case "edge-absent":
			edgeAbsent++
		default:
			t.Fatalf("unexpected outcome %q", got.outcome)
		}
	}
	if replied != 1 || edgeAbsent != 1 {
		t.Fatalf("outcomes: %d replied, %d edge-absent; want exactly one of each", replied, edgeAbsent)
	}
}
