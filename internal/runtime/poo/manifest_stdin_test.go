package poo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/poo"
	"github.com/sdougbrown/poo/tool/bash"
	"github.com/sdougbrown/poo/tool/cliregistry"
)

var (
	manifestBuildOnce sync.Once
	manifestBinPath   string
	manifestBuildErr  error
)

// buildMessageBinary builds the avenor binary once so the manifest-driven
// calls run the real command the way a poo child does.
func buildMessageBinary(t *testing.T) string {
	t.Helper()
	manifestBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "avenor-manifest-stdin-")
		if err != nil {
			manifestBuildErr = err
			return
		}
		manifestBinPath = filepath.Join(dir, "avenor")
		cmd := exec.Command("go", "build", "-o", manifestBinPath, ".")
		cmd.Dir = "../../../cmd/avenor"
		out, err := cmd.CombinedOutput()
		if err != nil {
			manifestBuildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if manifestBuildErr != nil {
		t.Fatal(manifestBuildErr)
	}
	return manifestBinPath
}

// TestMessageManifestStdinDelivery runs the published poo manifest through
// poo's call_cli tool against a real broker: the model-facing arguments keep
// their shape, the message text rides stdin, and every action delivers its
// exact bytes.
func TestMessageManifestStdinDelivery(t *testing.T) {
	bin := buildMessageBinary(t)
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

	root := t.TempDir()
	origExecutable := avenorExecutable
	avenorExecutable = func() (string, error) { return bin, nil }
	t.Cleanup(func() { avenorExecutable = origExecutable })

	content, err := renderManifests(root, "http://"+b.Addr(), "child")
	if err != nil {
		t.Fatal(err)
	}
	paths, err := publishManifests(root, content)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := cliregistry.Load(paths.registry)
	if err != nil {
		t.Fatal(err)
	}
	tool := cliregistry.NewCall(catalog)

	env := append(os.Environ(),
		"AVENOR_BROKER_URL=http://"+b.Addr(),
		"AVENOR_RUN_ID=child",
		"AVENOR_BROKER_TOKEN="+child,
	)
	callTool := func(t *testing.T, args map[string]any) (string, bool) {
		t.Helper()
		encoded, err := json.Marshal(map[string]any{"name": "avenor_message", "args": args})
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := tool.Prepare(encoded, poo.PrepareEnv{})
		if err != nil {
			t.Fatalf("prepare %v: %v", args, err)
		}
		res, err := tool.Call(context.Background(), prepared, poo.ToolEnv{Env: env})
		if err != nil {
			t.Fatalf("call %v: %v", args, err)
		}
		return res.Text, res.IsError
	}
	// stdout decodes the call's observation envelope and returns the CLI's
	// own JSON result.
	stdout := func(t *testing.T, text string) map[string]any {
		t.Helper()
		var obs map[string]any
		if err := json.Unmarshal([]byte(text), &obs); err != nil {
			t.Fatalf("observation %q: %v", text, err)
		}
		out, _ := obs[bash.EnvelopeOutput].(string)
		var result map[string]any
		if err := json.Unmarshal([]byte(out), &result); err != nil {
			t.Fatalf("cli output %q: %v", out, err)
		}
		return result
	}
	receiveInbound := func() messaging.InboundMessage {
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

	// A tell through the manifest delivers the exact bytes, newlines
	// included, and nothing about the text exists in the plan's argv.
	tellText := "line one\nline two\n雪 ❄️\n"
	text, isErr := callTool(t, map[string]any{"action": "tell", "target": "supervisor", "message": tellText})
	if isErr {
		t.Fatalf("tell failed: %s", text)
	}
	msg := receiveInbound()
	if msg.FromRunID != "child" {
		t.Fatalf("tell sender: %+v", msg)
	}
	if got := payloadText(msg.Payload); got != tellText {
		t.Fatalf("tell text = %q, want %q", got, tellText)
	}

	// An ask through the manifest carries its multiline question over stdin,
	// the supervisor answers through the broker, and the pending call sees
	// the answer.
	askText := "question one\nquestion two\n"
	type callResult struct {
		text  string
		isErr bool
	}
	done := make(chan callResult, 1)
	go func() {
		text, isErr := callTool(t, map[string]any{"action": "ask", "target": "supervisor", "message": askText, "timeout_ms": 30000})
		done <- callResult{text, isErr}
	}()
	msg = receiveInbound()
	if got := payloadText(msg.Payload); got != askText {
		t.Fatalf("ask text = %q, want %q", got, askText)
	}
	reply, err := tr.Reply(t.Context(), messaging.ReplyRequest{From: messaging.Identity{RunID: "supervisor"}, ToRunID: "child", MessageID: msg.MessageID, Body: `{"message":"answer text","role":"agent"}`})
	if err != nil || reply.Outcome != messaging.OutcomeReplied {
		t.Fatalf("reply: %+v %v", reply, err)
	}
	got := <-done
	if got.isErr || !strings.Contains(got.text, "answer text") {
		t.Fatalf("ask: %+v", got)
	}

	// A reply to a delivered tell goes back through the manifest and lands
	// with its exact bytes.
	replyID, err := messaging.NewMsgID(time.Now(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Send(t.Context(), messaging.SendRequest{From: messaging.Identity{RunID: "supervisor"}, To: messaging.Identity{RunID: "child"}, MessageID: replyID, Generation: pre.Generation, Body: `{"message":"notice","role":"agent"}`, NoReply: true}); err != nil {
		t.Fatal(err)
	}
	asks, err := askclaim.DefaultDir(root, "http://"+b.Addr(), "child")
	if err != nil {
		t.Fatal(err)
	}
	if err := askclaim.Record(asks, replyID, "supervisor"); err != nil {
		t.Fatal(err)
	}
	replyText := "reply one\nreply two\n"
	text, isErr = callTool(t, map[string]any{"action": "reply", "reply_to": replyID, "message": replyText})
	if isErr {
		t.Fatalf("reply failed: %s", text)
	}
	msg = receiveInbound()
	if got := payloadText(msg.Payload); got != replyText {
		t.Fatalf("reply text = %q, want %q", got, replyText)
	}

	// A timed-out ask closes through a manifest-driven withdraw without any
	// message text at all: the plan emits no stdin flag for the action.
	text, isErr = callTool(t, map[string]any{"action": "ask", "target": "supervisor", "message": "timeout ask", "timeout_ms": 10})
	if isErr {
		t.Fatalf("timeout ask failed: %s", text)
	}
	result := stdout(t, text)
	if result["outcome"] != "timed-out" {
		t.Fatalf("timeout ask outcome: %v", result)
	}
	timeoutID, ok := result["message_id"].(string)
	if !ok || timeoutID == "" {
		t.Fatalf("timeout missing message id: %v", result)
	}
	text, isErr = callTool(t, map[string]any{"action": "withdraw", "message_id": timeoutID})
	if isErr {
		t.Fatalf("withdraw failed: %s", text)
	}
	if result = stdout(t, text); result["outcome"] != "withdrawn" {
		t.Fatalf("withdraw outcome: %v", result)
	}

	// The manifest's own plan keeps the text off argv for every bound call:
	// each plan carries --message-stdin and never the bytes themselves.
	for _, args := range []map[string]any{
		{"action": "tell", "target": "supervisor", "message": tellText},
		{"action": "ask", "target": "supervisor", "message": askText, "timeout_ms": 30000},
		{"action": "reply", "reply_to": "id", "message": replyText},
		{"action": "withdraw", "message_id": "id"},
	} {
		plan, err := catalog.Plan("avenor_message", args)
		if err != nil {
			t.Fatal(err)
		}
		for _, arg := range plan.Argv {
			if strings.Contains(arg, "line one") || strings.Contains(arg, "question one") || strings.Contains(arg, "reply one") {
				t.Errorf("argv %q carries message text", arg)
			}
		}
		if args["action"] == "withdraw" && plan.Stdin != nil {
			t.Errorf("withdraw plan carries stdin %q", *plan.Stdin)
		}
		if args["action"] != "withdraw" && (plan.Stdin == nil || !strings.Contains(*plan.Stdin, "\n")) {
			t.Errorf("%v: plan stdin = %v, want the multiline text", args, plan.Stdin)
		}
		if args["action"] != "withdraw" {
			flagged := false
			for _, arg := range plan.Argv {
				if arg == "--message-stdin" {
					flagged = true
				}
			}
			if !flagged {
				t.Errorf("%v: plan argv %v lacks --message-stdin", args, plan.Argv)
			}
		}
	}
}
