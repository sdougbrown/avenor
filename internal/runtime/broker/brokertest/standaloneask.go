package brokertest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/tooltest"
)

// StandaloneAskRun is the broker connection and stdio pipes a tool server gets
// for one standalone ask.
type StandaloneAskRun struct {
	BrokerURL string
	RunID     string
	Token     string
	Sender    string
	Stdin     io.Reader
	Stdout    io.Writer
}

// StandaloneAsk describes one standalone ask against a real local broker.
type StandaloneAsk struct {
	Sender       string
	ReplyMessage string
	WantText     string
	Run          func(ctx context.Context, in StandaloneAskRun) error
}

// RunStandaloneLocalAsk starts a real broker and the caller's tool server, asks
// parent_run under cfg.Sender, answers over the broker, and checks the rendered
// reply text.
func RunStandaloneLocalAsk(t *testing.T, cfg StandaloneAsk) {
	t.Helper()
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer func() { _ = b.Stop() }()

	childToken, err := b.CreateRun("child_run", cfg.Sender)
	if err != nil {
		t.Fatalf("CreateRun child: %v", err)
	}
	parentToken, err := b.CreateRun("parent_run", "parent")
	if err != nil {
		t.Fatalf("CreateRun parent: %v", err)
	}

	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	t.Cleanup(func() {
		_ = stdinW.Close()
		_ = stdoutW.Close()
	})
	done := make(chan error, 1)
	go func() {
		done <- cfg.Run(context.Background(), StandaloneAskRun{
			BrokerURL: "http://" + b.Addr(),
			RunID:     "child_run",
			Token:     childToken,
			Sender:    cfg.Sender,
			Stdin:     stdinR,
			Stdout:    stdoutW,
		})
	}()

	if _, err := fmt.Fprintln(stdinW, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"avenor_ask","arguments":{"to_run_id":"parent_run","message":"status?"}}}`); err != nil {
		t.Fatalf("write ask: %v", err)
	}

	ask := WaitForAgentMessage(t, b, "parent_run")
	if _, err := messaging.ParseMsgID(ask.ID, time.Now()); err != nil {
		t.Fatalf("ask message id %q is not a versioned id: %v", ask.ID, err)
	}
	if ask.FromRunID != "child_run" || ask.Sender != cfg.Sender {
		t.Fatalf("ask identity = %q/%q, want child_run/%s", ask.FromRunID, ask.Sender, cfg.Sender)
	}

	replyBody, err := json.Marshal(map[string]any{
		"run_id":      "parent_run",
		"token":       parentToken,
		"from_run_id": "parent_run",
		"to_run_id":   "child_run",
		"type":        "agent_message",
		"reply_to":    ask.ID,
		"payload":     map[string]any{"message": cfg.ReplyMessage},
	})
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	resp, err := http.Post("http://"+b.Addr()+"/send", "application/json", bytes.NewReader(replyBody))
	if err != nil {
		t.Fatalf("post reply: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reply status = %d, want 200", resp.StatusCode)
	}

	var response map[string]any
	if err := json.Unmarshal(tooltest.ReadResponse(t, stdoutR), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	result, _ := response["result"].(map[string]any)
	content, _ := result["content"].([]any)
	first, _ := content[0].(map[string]any)
	if text, _ := first["text"].(string); text != cfg.WantText {
		t.Fatalf("ask result text = %q, want %q", text, cfg.WantText)
	}

	_ = stdinW.Close()
	_ = stdoutW.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after stdin closed")
	}
}
